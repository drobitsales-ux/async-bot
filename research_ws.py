"""research_ws.py — H-WS («широкий стоп»): пересимуляция ВЫХОДА на уже записанных shadow-сигналах (BOT_SPEC §2.33).

ТОЛЬКО ИССЛЕДОВАНИЕ: новых сигналов нет, торговых таблиц модуль не меняет (читает shadow_signals, пишет только кэш ws_bars и htf_reports),
async_bot не импортирует (все параметры — аргументами). Правило остановки §2.32.8 действует; H-WS — единственное записанное исключение.

ПРЕ-КОММИТ (§2.33, зафиксирован ДО первого запуска; других вариантов нет):
  k ∈ {1.0, 1.5, 2.0, 3.0} — множитель исходной дистанции стопа d0 = |entry − sl|; новый SL = entry ∓ k·d0;
  режим A — TP = исходный tp_price; режим B — TP = entry ± 2·k·d0; единый таймаут 48 ч для ВСЕХ вариантов (выход по close бара таймаута);
  база = (k=1.0, A); приоритет в баре SL > TP; семантика баров — как в _shadow_exit_eval (бары, целиком лежащие до open_time, отбрасываются, бар входа участвует).
  R = pnl_net / (k·d0): риск в деньгах постоянный. Критерии варианта (по основной популяции): (а) парная разность к базе > 0 при t ≥ 2.6;
  (б) mean R net > 0 при t ≥ 2.4; (в) PF net ≥ 1.3; (г) n ≥ 200. Значимость (а)/(б) — по сделкам и по дням; при расхождении вердикт — по дням.
"""
import asyncio
import sqlite3
import time
from datetime import datetime, timezone

import research_htf as H

MAIN_STRATS = ('SMC2_MKT', 'SMC2_RT', 'SMC_SIG', 'PO3', 'ORB_MKT2', 'ORB_RT')
REF_STRAT, REF_FROM = 'RB', '2026-09-15'
LIMIT_ENTRY = ('SMC2_RT', 'ORB_RT')            # вход лимитом → FEE_MAKER (как _LIMIT_ENTRY_STRATS в async_bot)
KS = (1.0, 1.5, 2.0, 3.0)
MODES = ('A', 'B')
BASE = (1.0, 'A')
VARIANTS = [(k, m) for k in KS for m in MODES]
TIMEOUT_H = 48
TF_MS = 15 * 60_000
TP_B_R = 2.0
CRIT_PAIR_T, CRIT_T, CRIT_PF, CRIT_N = 2.6, 2.4, 1.3, 200
SLIP = H.SLIP
FETCH_HORIZON_H = 100                           # 48 ч окна + 48 ч «после выбивания» для диагностики 1.5 + запас
MAX_MISSING_BARS = 2
BOT_GUARD_NOTIONAL = 20.0                       # guard живого бота: notional < $20 отклоняется (CLAUDE.md, решение №6)
RISKS_EQ = (0.005, 0.01)


def vname(v):
    return f'k={v[0]:g} {v[1]}'


# ── популяции ──────────────────────────────────────────────────────────────────────────
def load_population(db_path, strategies, since=None):
    """Закрытые shadow-записи с sl_price > 0 и recalc != 2 → список dict (id, strategy, symbol, direction, entry, sl, tp, open_time, limit)."""
    con = sqlite3.connect(db_path)
    q = ("SELECT id, strategy, symbol, direction, entry_price, sl_price, tp_price, open_time FROM shadow_signals "
         "WHERE status='closed' AND recalc!=2 AND COALESCE(sl_price,0)>0 AND strategy IN (%s)" % ','.join('?' * len(strategies)))
    args = list(strategies)
    if since:
        q += ' AND open_time>=?'; args.append(since)
    rows = con.execute(q + ' ORDER BY id', args).fetchall()
    con.close()
    return [{'id': r[0], 'strategy': r[1], 'symbol': r[2], 'direction': r[3], 'entry': float(r[4] or 0), 'sl': float(r[5] or 0),
             'tp': float(r[6] or 0), 'open_time': r[7], 'limit': r[1] in LIMIT_ENTRY} for r in rows]


def _open_ms(iso):
    dt = datetime.fromisoformat(iso)
    dt = dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def check_row(row):
    """Валидность записи для пересимуляции → причина исключения или ''."""
    if row['entry'] <= 0 or row['sl'] <= 0:
        return 'нет entry/sl'
    if row['tp'] <= 0:
        return 'нет tp_price'
    long_ = row['direction'] == 'Long'
    if (long_ and not (row['sl'] < row['entry'] < row['tp'])) or ((not long_) and not (row['sl'] > row['entry'] > row['tp'])):
        return 'sl/tp на неверной стороне от входа'
    return ''


# ── симуляция выхода ───────────────────────────────────────────────────────────────────
def simulate(row, bars, k, mode, fee_maker, fee_taker, slip=SLIP, timeout_h=TIMEOUT_H, now_ms=None, tf_ms=TF_MS):
    """Выход одной записи при множителе стопа k и режиме TP. Семантика баров — как _shadow_exit_eval: бары, целиком лежащие до open_time,
    отбрасываются, бар входа участвует; внутри бара SL > TP; таймаут — по close бара, содержащего момент open+timeout_h (SL/TP в этом баре
    проверяются по всему бару раньше). now_ms=None → «сейчас» берётся как бесконечность (окно считается завершённым — решает вызывающий).
    → dict(reason, R, R_gross, R_cost, held_bars, exit_ts, exit_price) или None (окно не завершено)."""
    open_ms = _open_ms(row['open_time'])
    entry_bar_ms = int(open_ms // tf_ms * tf_ms)
    t_to_ms = open_ms + timeout_h * 3_600_000
    long_ = row['direction'] == 'Long'
    side = 1 if long_ else -1
    entry, d0 = row['entry'], abs(row['entry'] - row['sl'])
    sl = entry - side * k * d0
    tp = row['tp'] if mode == 'A' else entry + side * TP_B_R * k * d0
    now_ms = float('inf') if now_ms is None else now_ms
    res = None
    for b in bars:
        ts = int(b[0])
        if ts + tf_ms <= open_ms:
            continue
        hi, lo, cl = float(b[2]), float(b[3]), float(b[4])
        held = max(0, int((ts - entry_bar_ms) // tf_ms))
        sl_hit = (lo <= sl) if long_ else (hi >= sl)
        tp_hit = (hi >= tp) if long_ else (lo <= tp)
        if sl_hit:
            res = ('SL', sl, held, ts); break
        if tp_hit:
            res = ('TP', tp, held, ts); break
        if now_ms >= t_to_ms and ts + tf_ms > t_to_ms:
            res = ('TIMEOUT', cl, held, ts); break
    if res is None:
        return None
    reason, px, held, ts = res
    fee_in = fee_taker if not row['limit'] else fee_maker
    fee_out = fee_maker if reason == 'TP' else fee_taker
    slip_sides = (0 if row['limit'] else 1) + (0 if reason == 'TP' else 1)      # проскальзывание — только на рыночных исполнениях
    cost = fee_in + fee_out + slip * slip_sides
    sl_frac = k * d0 / entry
    move = side * (px - entry) / entry
    return {'reason': reason, 'R': (move - cost) / sl_frac, 'R_gross': move / sl_frac, 'R_cost': cost / sl_frac, 'held_bars': held,
            'exit_ts': ts, 'exit_price': px}


# ── статистика ──────────────────────────────────────────────────────────────────────────
def _stats(results):
    """results: список dict simulate(). → n, WR, PF net, mean R, t, средняя длительность (ч), доли SL/TP/TIMEOUT, комиссия в R."""
    n = len(results)
    if n == 0:
        return {'n': 0, 'wr': 0.0, 'pf': 0.0, 'mean_r': 0.0, 't': 0.0, 'hours': 0.0, 'sl': 0.0, 'tp': 0.0, 'to': 0.0, 'cost_r': 0.0}
    rs = [r['R'] for r in results]
    ws = H.weekly_stats(rs)
    gw, gl = sum(x for x in rs if x > 0), -sum(x for x in rs if x < 0)
    pf = (gw / gl) if gl > 0 else (float('inf') if gw > 0 else 0.0)
    cnt = lambda s: sum(1 for r in results if r['reason'] == s) / n
    return {'n': n, 'wr': sum(1 for x in rs if x > 0) / n * 100, 'pf': pf, 'mean_r': ws['mean'], 't': ws['t'],
            'hours': sum(r['held_bars'] for r in results) / n * TF_MS / 3_600_000, 'sl': cnt('SL') * 100, 'tp': cnt('TP') * 100, 'to': cnt('TIMEOUT') * 100,
            'cost_r': sum(r['R_cost'] for r in results) / n}


def _by_days(values_by_row, rows_by_id):
    """Среднее по календарному дню (UTC дата open_time) → ряд дневных значений."""
    days = {}
    for rid, v in values_by_row.items():
        d = rows_by_id[rid]['open_time'][:10]
        days.setdefault(d, []).append(v)
    return [sum(v) / len(v) for v in days.values()]


def variant_table(rows, sims):
    """sims: {rid: {variant: result}}. → {variant: {'rows': {rid: result}, 'stats':..., 'tr': {n, mean, t}, 'day': {n, mean, t}, 'pair_tr', 'pair_day'}}."""
    rows_by_id = {r['id']: r for r in rows}
    out = {}
    base = {rid: s[BASE] for rid, s in sims.items()}
    for v in VARIANTS:
        vr = {rid: s[v] for rid, s in sims.items()}
        diff = {rid: vr[rid]['R'] - base[rid]['R'] for rid in vr}
        out[v] = {'rows': vr, 'stats': _stats(list(vr.values())),
                  'tr': H.weekly_stats([x['R'] for x in vr.values()]), 'day': H.weekly_stats(_by_days({r: x['R'] for r, x in vr.items()}, rows_by_id)),
                  'pair_tr': H.weekly_stats(list(diff.values())), 'pair_day': H.weekly_stats(_by_days(diff, rows_by_id))}
    return out


def verdicts(table):
    """Критерии §2.33.5 по основной популяции. Значимость (а)/(б) считается по сделкам и по дням; при расхождении вердикт — по дням.
    → {variant: {'pass': bool|None, 'status', 'checks', 'diverge'}}; None = недостаточно данных (n < 200)."""
    out = {}
    for v in VARIANTS:
        if v == BASE:
            continue
        t = table[v]; n = t['stats']['n']
        a_tr = t['pair_tr']['mean'] > 0 and t['pair_tr']['t'] >= CRIT_PAIR_T
        a_day = t['pair_day']['mean'] > 0 and t['pair_day']['t'] >= CRIT_PAIR_T
        b_tr = t['tr']['mean'] > 0 and t['tr']['t'] >= CRIT_T
        b_day = t['day']['mean'] > 0 and t['day']['t'] >= CRIT_T
        c = t['stats']['pf'] >= CRIT_PF
        diverge = (a_tr != a_day) or (b_tr != b_day)
        checks = {f'(а) парная разность к базе&gt;0, t≥{CRIT_PAIR_T}': a_day, f'(б) mean R net&gt;0, t≥{CRIT_T}': b_day, f'(в) PF net≥{CRIT_PF}': c}
        if n < CRIT_N:
            out[v] = {'pass': None, 'status': f'недостаточно данных (n={n} &lt; {CRIT_N})', 'checks': {}, 'diverge': diverge, 'by_tr': (a_tr, b_tr), 'by_day': (a_day, b_day)}
        else:
            ok = all(checks.values())
            out[v] = {'pass': ok, 'status': 'ПРОЙДЕН' if ok else 'не пройден', 'checks': checks, 'diverge': diverge, 'by_tr': (a_tr, b_tr), 'by_day': (a_day, b_day)}
    return out


def decision(table, ver):
    """§2.33.6: прошёл ли хоть один вариант; победитель — один: максимальный mean R net из прошедших."""
    passed = [v for v, x in ver.items() if x['pass']]
    if not passed:
        return None
    return max(passed, key=lambda v: table[v]['stats']['mean_r'])


# ── диагностика гипотезы владельца (1.5) ──────────────────────────────────────────────────
def diagnose(rows, bars_of, sims, tf_ms=TF_MS, horizon_h=TIMEOUT_H):
    """Среди сделок, закрытых по SL в БАЗЕ (k=1.0, A): доля тех, где цена после выбивания в течение 48 ч дошла до ИСХОДНОГО tp_price;
    и доля тех, где это произошло БЕЗ касания entry ∓ 2·d0 / entry ∓ 3·d0 (по пути от входа до бара, где достигнут tp; касание в баре TP считается
    консервативно — low/high всего бара). Учитываются только записи с полным 48-ч хвостом после выбивания (иначе — «недостаточно хвоста»)."""
    d = {'sl': 0, 'tail_ok': 0, 'reached': 0, 'no2': 0, 'no3': 0, 'no_tail': 0}
    for r in rows:
        s = sims.get(r['id'])
        if not s or s[BASE]['reason'] != 'SL':
            continue
        d['sl'] += 1
        bars = bars_of[r['id']]
        sl_ts = s[BASE]['exit_ts']; end_ms = sl_ts + tf_ms + horizon_h * 3_600_000
        if not bars or int(bars[-1][0]) + tf_ms < end_ms:
            d['no_tail'] += 1; continue
        d['tail_ok'] += 1
        long_ = r['direction'] == 'Long'; side = 1 if long_ else -1
        d0 = abs(r['entry'] - r['sl']); lv2, lv3 = r['entry'] - side * 2 * d0, r['entry'] - side * 3 * d0
        open_ms = _open_ms(r['open_time']); touched2 = touched3 = False; hit = False
        for b in bars:
            ts = int(b[0])
            if ts + tf_ms <= open_ms:
                continue
            if ts > end_ms:
                break
            hi, lo = float(b[2]), float(b[3])
            if (lo <= lv2) if long_ else (hi >= lv2):
                touched2 = True
            if (lo <= lv3) if long_ else (hi >= lv3):
                touched3 = True
            if ts > sl_ts and ((hi >= r['tp']) if long_ else (lo <= r['tp'])):
                hit = True; break
        if hit:
            d['reached'] += 1
            d['no2'] += (not touched2); d['no3'] += (not touched3)
    return d


# ── минимальное эквити под минимальный ордер биржи (1.7) ──────────────────────────────────
def min_notional_of(market, entry):
    """Минимальный notional (USDT) ордера из ccxt markets: max(limits.cost.min, limits.amount.min × цена × contractSize); None, если биржа лимитов не даёт."""
    if not market:
        return None
    lim = market.get('limits') or {}
    c = (lim.get('cost') or {}).get('min')
    a = (lim.get('amount') or {}).get('min')
    cs = float(market.get('contractSize') or 1)
    vals = [float(c)] if c else []
    if a:
        vals.append(float(a) * entry * cs)
    return max(vals) if vals else None


def median_min_equity(rows, k, risk, markets, guard=0.0):
    """Эквити, при котором МЕДИАННАЯ сделка проходит минимальный ордер: notional = эквити·risk·entry/(k·d0) ≥ min_notional → эквити ≥ min_notional·k·d0/(entry·risk).
    guard — доп. нижняя планка notional (guard бота $20). → медиана по записям или None."""
    need = []
    for r in rows:
        mn = min_notional_of(markets.get(r['symbol']), r['entry'])
        if mn is None and not guard:
            continue
        mn = max(mn or 0.0, guard)
        need.append(mn * k * abs(r['entry'] - r['sl']) / (r['entry'] * risk))
    if not need:
        return None
    need.sort(); m = len(need)
    return need[m // 2] if m % 2 else (need[m // 2 - 1] + need[m // 2]) / 2


# ── кэш баров и загрузка ────────────────────────────────────────────────────────────────
def init_ws_cache(db_path):
    con = sqlite3.connect(db_path)
    con.execute("CREATE TABLE IF NOT EXISTS ws_bars (symbol TEXT, ts INTEGER, o REAL, h REAL, l REAL, c REAL, v REAL, PRIMARY KEY (symbol, ts))")
    con.commit(); con.close()


def cached_ts(db_path, sym, t0, t1):
    con = sqlite3.connect(db_path)
    rows = con.execute("SELECT ts FROM ws_bars WHERE symbol=? AND ts>=? AND ts<=?", (sym, t0, t1)).fetchall()
    con.close()
    return {r[0] for r in rows}


def cached_bars(db_path, sym, t0, t1):
    con = sqlite3.connect(db_path)
    rows = con.execute("SELECT ts,o,h,l,c,v FROM ws_bars WHERE symbol=? AND ts>=? AND ts<=? ORDER BY ts", (sym, t0, t1)).fetchall()
    con.close()
    return [list(r) for r in rows]


def plan_windows(rows, now_ms, tf_ms=TF_MS):
    """Окна загрузки по символам: [бар входа, min(now, open+FETCH_HORIZON_H)], перекрывающиеся окна одного символа склеиваются."""
    per = {}
    for r in rows:
        o = _open_ms(r['open_time']); b0 = o // tf_ms * tf_ms
        per.setdefault(r['symbol'], []).append((b0, min(now_ms, o + FETCH_HORIZON_H * 3_600_000)))
    out = {}
    for s, ws in per.items():
        ws.sort(); merged = [list(ws[0])]
        for a, b in ws[1:]:
            if a <= merged[-1][1] + tf_ms:
                merged[-1][1] = max(merged[-1][1], b)
            else:
                merged.append([a, b])
        out[s] = merged
    return out


async def load_bars(exchange, db_path, rows, now_ms, info, progress=None, tf_ms=TF_MS):
    """Догружает 15m-бары (только ЗАКРЫТЫЕ) в ws_bars: повторы/пауза как в v87 (H.call_with_retry, HTF_FETCH_PAUSE_SEC), постраничное сохранение.
    Сбой окна не останавливает загрузку: info['failed_windows'][(sym, t0)] = причина."""
    init_ws_cache(db_path)
    info.setdefault('failed_windows', {})
    plan = plan_windows(rows, now_ms, tf_ms)
    for sym, wins in plan.items():
        for t0, t1 in wins:
            have = cached_ts(db_path, sym, t0, t1)
            exp = list(range(t0, (t1 // tf_ms) * tf_ms + 1, tf_ms))
            miss = [t for t in exp if t not in have and t + tf_ms <= now_ms]
            if not miss:
                continue
            start = miss[0]

            async def f(cur, sym=sym):
                return await H.call_with_retry(f'{sym} 15m', cur, lambda: exchange.fetch_ohlcv(sym, '15m', since=cur - 1, limit=1000), info)

            def save(batch, sym=sym):
                rws = [(sym, int(b[0]), b[1], b[2], b[3], b[4], b[5]) for b in batch if b[0] + tf_ms <= now_ms]
                if rws:
                    con = sqlite3.connect(db_path)
                    con.executemany("INSERT OR REPLACE INTO ws_bars VALUES (?,?,?,?,?,?,?)", rws)
                    con.commit(); con.close()
            try:
                await H._fetch_paged(f, start, t1 + tf_ms, tf_ms, on_batch=save)
            except H.HtfFetchError as e:
                info['failed_windows'][(sym, t0)] = e.short()
                if progress:
                    await progress(f'{sym.split("/")[0]}: окно {H._date(t0)} не загружено ({e.short()})')
        if progress:
            await progress(f'{sym.split("/")[0]}: 15m загружены ({len(wins)} окон)')


def prepare(rows, db_path, now_ms, tf_ms=TF_MS):
    """Бары для каждой записи из кэша + отбор: окно 48 ч завершено и бары полные (≤ MAX_MISSING_BARS пропусков).
    → (ok_rows, bars_of{id: bars}, excluded{reason: count}, excluded_rows[(id, reason)])."""
    ok, bars_of, exc, exc_rows = [], {}, {}, []

    def ex(r, why):
        exc[why] = exc.get(why, 0) + 1; exc_rows.append((r['id'], why))
    for r in rows:
        why = check_row(r)
        if why:
            ex(r, why); continue
        o = _open_ms(r['open_time']); b0 = o // tf_ms * tf_ms
        t_to = o + TIMEOUT_H * 3_600_000
        if t_to + tf_ms > now_ms:
            ex(r, 'окно 48 ч ещё не завершено'); continue
        bars = cached_bars(db_path, r['symbol'], b0, min(now_ms, o + FETCH_HORIZON_H * 3_600_000))
        last_needed = (t_to // tf_ms) * tf_ms
        exp = (last_needed - b0) // tf_ms + 1
        got = len([b for b in bars if b0 <= b[0] <= last_needed])
        if exp - got > MAX_MISSING_BARS:
            ex(r, f'нет баров 15m (получено {got} из {exp})'); continue
        ok.append(r); bars_of[r['id']] = bars
    return ok, bars_of, exc, exc_rows


def simulate_all(rows, bars_of, fee_maker, fee_taker, slip=SLIP):
    sims = {}
    for r in rows:
        s = {}
        for v in VARIANTS:
            x = simulate(r, bars_of[r['id']], v[0], v[1], fee_maker, fee_taker, slip)
            if x is None:
                s = None; break
            s[v] = x
        if s is not None:
            sims[r['id']] = s
    return sims


# ── отчёт ────────────────────────────────────────────────────────────────────────────────
def _fpf(x):
    return 'inf' if x == float('inf') else f'{x:.2f}'


def _row(label, s):
    if s['n'] == 0:
        return f'  {label}: n=0'
    return (f'  {label}: n={s["n"]} | WR {s["wr"]:.0f}% | PF net {_fpf(s["pf"])} | mean R net {s["mean_r"]:+.3f} | t {s["t"]:.2f} | ср. длит. {s["hours"]:.1f} ч | '
            f'SL {s["sl"]:.0f}% / TP {s["tp"]:.0f}% / таймаут {s["to"]:.0f}% | комиссия {s["cost_r"]:.3f}R')


def build_report(main_rows, main_sims, ref_rows, ref_sims, exc_main, exc_ref, diag, markets, fees, info, now_ms) -> str:
    fee_maker, fee_taker = fees
    table = variant_table(main_rows, main_sims) if main_sims else None
    L = ['📈 <b>H-WS («широкий стоп»): пересимуляция выхода на записанных shadow-сигналах</b>',
         f'Основная популяция (по ней вердикт): {", ".join(MAIN_STRATS)}; закрытые, sl_price&gt;0, recalc!=2. Справочно: {REF_STRAT} с {REF_FROM} (без вердикта). '
         f'Варианты: k∈{{1.0,1.5,2.0,3.0}} × TP «A» (исходный) / «B» (2R от нового стопа); единый таймаут {TIMEOUT_H} ч для ВСЕХ; база = k=1.0 A; SL раньше TP в баре; '
         f'семантика баров как _shadow_exit_eval (бар входа участвует). R = pnl_net/(k·d0), риск постоянный. Комиссии: вход лимитом (SMC2_RT, ORB_RT) и TP — FEE_MAKER={fee_maker}, '
         f'иначе FEE_TAKER={fee_taker}; проскальзывание {SLIP} на сторону на рыночных исполнениях. ФАНДИНГ НЕ УЧТЁН (удержание ≤ {TIMEOUT_H} ч). Пре-коммит — BOT_SPEC §2.33.',
         f'Записей в основной популяции: {sum(exc_main.values()) + len(main_sims)} | в тесте: {len(main_sims)} | исключено: {sum(exc_main.values())}']
    for why, c in sorted(exc_main.items(), key=lambda x: -x[1]):
        L.append(f'   исключено {c}: {why}')
    for k_, why in info.get('failed_windows', {}).items():
        L.append(f'   ⚠️ окно {k_[0].split("/")[0]} {H._date(k_[1])}: не загружено ({why})')
    verd, win = {}, None
    if table:
        verd = verdicts(table); win = decision(table, verd)
        L.append('\n<b>Основная популяция × вариант</b>')
        for v in VARIANTS:
            t = table[v]
            L.append(_row(vname(v) + (' (БАЗА)' if v == BASE else ''), t['stats']))
            if v != BASE:
                L.append(f'      парная разность к базе: по сделкам {t["pair_tr"]["mean"]:+.3f}R (t {t["pair_tr"]["t"]:.2f}, n={t["pair_tr"]["n"]}), '
                         f'по дням {t["pair_day"]["mean"]:+.3f}R (t {t["pair_day"]["t"]:.2f}, дней {t["pair_day"]["n"]}); mean R по дням {t["day"]["mean"]:+.3f} (t {t["day"]["t"]:.2f})')
        L.append('\n<b>Вердикты по вариантам</b> (значимость по дням; при расхождении сделки/дни — вердикт по дням)')
        for v in VARIANTS:
            if v == BASE:
                continue
            x = verd[v]
            L.append(f' ВЕРДИКТ {vname(v)}: {x["status"]}' + (' | ' + ' | '.join(f'{k} {"✅" if ok else "❌"}' for k, ok in x['checks'].items()) if x['checks'] else '')
                     + (' | ⚠️ сделки и дни расходятся' if x['diverge'] else ''))
        L.append('\n<b>РЕШЕНИЕ H-WS: ' + (f'прошёл вариант {vname(win)} (макс. mean R net среди прошедших {table[win]["stats"]["mean_r"]:+.3f}) → shadow-форвард SMC2 с даты деплоя решения; живая торговля — только после форварда (n≥100)'
                                           if win else 'ни один вариант не прошёл — H-WS закрыта, правило остановки §2.32.8 действует без исключений') + '</b>')
        L.append('\n<b>Справочно: по стратегиям</b> (mean R net, n)')
        for st in MAIN_STRATS:
            ids = [r['id'] for r in main_rows if r['strategy'] == st and r['id'] in main_sims]
            if not ids:
                continue
            cells = []
            for v in VARIANTS:
                rs = [main_sims[i][v] for i in ids]
                s = _stats(rs); cells.append(f'{vname(v)} {s["mean_r"]:+.3f}')
            L.append(f'  {st} (n={len(ids)}): ' + ' | '.join(cells))
        L.append('\n<b>Минимальное эквити, при котором МЕДИАННАЯ сделка проходит минимальный ордер</b> (лимиты ccxt markets; в скобках — с guard бота notional ≥ $20)')
        for v in VARIANTS:
            cells = []
            for risk in RISKS_EQ:
                a = median_min_equity(main_rows, v[0], risk, markets)
                b = median_min_equity(main_rows, v[0], risk, markets, BOT_GUARD_NOTIONAL)
                cells.append(f'риск {risk * 100:g}%: ' + (f'{a:,.0f}' if a is not None else 'нет лимитов') + (f' ({b:,.0f})' if b is not None else ''))
            L.append(f'  {vname(v)}: ' + ' | '.join(cells) + ' USDT')
    else:
        L.append('\nВердикт невозможен: нет записей для пересимуляции.')
    L.append('\n<b>Диагностика гипотезы владельца (база, сделки по SL)</b>')
    if diag and diag['sl']:
        ok_ = max(diag['tail_ok'], 1); rc = max(diag['reached'], 1)
        L.append(f'  SL в базе: {diag["sl"]}; с полным 48-ч хвостом после выбивания: {diag["tail_ok"]} (без хвоста: {diag["no_tail"]})')
        L.append(f'  цена после выбивания дошла до исходного TP за 48 ч: {diag["reached"]} ({diag["reached"] / ok_ * 100:.1f}% от SL с хвостом)')
        L.append(f'  из них БЕЗ касания entry∓2·d0: {diag["no2"]} ({diag["no2"] / ok_ * 100:.1f}% от SL; {diag["no2"] / rc * 100:.1f}% от дошедших); '
                 f'БЕЗ касания entry∓3·d0: {diag["no3"]} ({diag["no3"] / ok_ * 100:.1f}% от SL; {diag["no3"] / rc * 100:.1f}% от дошедших)')
        L.append('  (касание считается по low/high всего бара, в т.ч. бара достижения TP — консервативно; без поправки на множественные сравнения — это диагностика, не критерий)')
    else:
        L.append('  нет сделок по SL в базе или нет данных')
    L.append(f'\n<b>Справочная популяция {REF_STRAT} (open_time ≥ {REF_FROM}, TP = tp_price, БЕЗ вердикта)</b>')
    if ref_sims:
        rt = variant_table(ref_rows, ref_sims)
        for v in VARIANTS:
            L.append(_row(vname(v) + (' (БАЗА)' if v == BASE else ''), rt[v]['stats']))
    else:
        L.append('  нет записей')
    for why, c in sorted(exc_ref.items(), key=lambda x: -x[1]):
        L.append(f'   исключено {c}: {why}')
    L.append('\nЕсли ни один вариант не прошёл — H-WS закрыта; правило остановки BOT_SPEC §2.32.8 действует без исключений. Варианты заново не подбираются.')
    return '\n'.join(L)


def compute(main_rows, ref_rows, bars_of, exc_main, exc_ref, markets, fees, info, now_ms):
    """Синхронная часть (в потоке): симуляция всех вариантов, вердикты, диагностика, отчёт."""
    fm, ft = fees
    main_sims = simulate_all(main_rows, bars_of, fm, ft)
    ref_sims = simulate_all(ref_rows, bars_of, fm, ft)
    m_ok = [r for r in main_rows if r['id'] in main_sims]
    diag = diagnose(m_ok, bars_of, main_sims)
    text = build_report(m_ok, main_sims, [r for r in ref_rows if r['id'] in ref_sims], ref_sims, exc_main, exc_ref, diag, markets, fees, info, now_ms)
    table = variant_table(m_ok, main_sims) if main_sims else None
    ver = verdicts(table) if table else {}
    return text, {'table': table, 'verdicts': ver, 'winner': decision(table, ver) if table else None, 'diag': diag, 'n': len(main_sims)}


async def prepare_wide_stop(exchange, db_path, progress=None, now_ms=None):
    """[v97] Сетевая часть /wide_stop_test (в процессе бота, через общий exchange): популяции → докачка 15m-баров в кэш ws_bars → рынки.
    → payload для расчёта в дочернем процессе: {'main_rows', 'ref_rows', 'markets', 'info', 'now_ms'}."""
    now_ms = now_ms or int(time.time() * 1000)
    info = {'requests': 0, 'fetch_log': []}
    main_rows = load_population(db_path, MAIN_STRATS)
    ref_rows = load_population(db_path, (REF_STRAT,), since=REF_FROM)
    valid = [r for r in main_rows + ref_rows if not check_row(r) and _open_ms(r['open_time']) + TIMEOUT_H * 3_600_000 + TF_MS <= now_ms]
    if progress:
        await progress(f'H-WS: записей основной популяции {len(main_rows)}, справочной {len(ref_rows)}; к загрузке окон 15m: {len({r["symbol"] for r in valid})} символов')
    await load_bars(exchange, db_path, valid, now_ms, info, progress)
    markets = {}
    try:
        await H.call_with_retry('markets', 0, lambda: exchange.load_markets(), info)
        markets = exchange.markets or {}
    except H.HtfFetchError:
        info['markets_failed'] = True
    return {'main_rows': main_rows, 'ref_rows': ref_rows, 'markets': markets, 'info': info, 'now_ms': now_ms}


def finish_wide_stop(payload, db_path, fee_maker, fee_taker):
    """[v97] Расчётная часть /wide_stop_test (синхронная; в дочернем процессе): бары из кэша ws_bars → пересимуляция → отчёт. → (text, res)."""
    now_ms = payload['now_ms']
    ok_m, bars_m, exc_m, _ = prepare(payload['main_rows'], db_path, now_ms)
    ok_r, bars_r, exc_r, _ = prepare(payload['ref_rows'], db_path, now_ms)
    bars_m.update(bars_r)
    return compute(ok_m, ok_r, bars_m, exc_m, exc_r, payload['markets'], (fee_maker, fee_taker), payload['info'], now_ms)


async def run_wide_stop(exchange, db_path, fee_maker, fee_taker, progress=None, now_ms=None):
    """/wide_stop_test: популяции из shadow_signals → 15m-бары (кэш ws_bars, повторы/пауза) → пересимуляция в потоке."""
    payload = await prepare_wide_stop(exchange, db_path, progress=progress, now_ms=now_ms)
    text, res = await asyncio.to_thread(finish_wide_stop, payload, db_path, fee_maker, fee_taker)
    return text, payload['info'], res
