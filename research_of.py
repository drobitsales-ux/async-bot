"""research_of.py — /of_backtest: бэктест H-OF1 («прокол с поглощением», 15m) и H-OF2 («сброс с ликвидациями», 1h) СТРОГО по BOT_SPEC §2.34
(+ уточнения §2.34.1–2.34.3, принятые владельцем) и §2.35 (реализационные уточнения). Никаких дополнительных фильтров и вариантов.
Данные — архив data.binance.vision через research_vision (npz-кэш); расчёт по символу целиком в памяти; в SQLite — только сделки (of_trades) и отчёт.
Издержки BingX: FEE_TAKER на вход и выход (включая TP) + проскальзывание 0.0003 на сторону; исполнение моделируется по свечам Binance."""
import asyncio
import sqlite3
from datetime import datetime, timezone

import numpy as np
from numpy.lib.stride_tricks import sliding_window_view as swv

import research_htf as H
import research_vision as V

SLIP_OF = 0.0003
TP_R = 2.0
TIMEOUT_H = 48
IS_END, OOS_START = '2024-12-31', '2025-01-01'
CRIT_T, CRIT_PF, CRIT_RATIO, CRIT_N, CRIT_DAYS = 2.5, 1.3, 0.5, 200, 100
ORDER, MED_BARS, DELTA_MULT, ATR_N, SL_ATR = 3, 96, 2.0, 14, 0.5
DIST1 = (0.003, 0.04)
DROP_ATR, OI_DROP, WIN, WAIT = 2.5, 0.03, 4, 4
DIST2 = (0.003, 0.06)
TF15, TF1H = 15 * 60_000, 3_600_000
HYPS = ('H-OF1', 'H-OF2')


def _ms(date_str):
    return int(datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc).timestamp() * 1000)


# ── pivots (идентично get_pivots бота) ───────────────────────────────────────────────────────
def get_pivots(highs, lows, order=ORDER):
    """Swing High / Low — та же логика, что async_bot.get_pivots (строгий максимум/минимум окна ±order, единственный в окне), векторно."""
    highs, lows = np.asarray(highs, float), np.asarray(lows, float)
    n = len(highs)
    if n < 2 * order + 1:
        return [], []
    wh, wl = swv(highs, 2 * order + 1), swv(lows, 2 * order + 1)
    mx, mn = wh.max(axis=1), wl.min(axis=1)
    ch, cl = highs[order:n - order], lows[order:n - order]
    ph = np.where((ch == mx) & ((wh == mx[:, None]).sum(axis=1) == 1))[0] + order
    pl = np.where((cl == mn) & ((wl == mn[:, None]).sum(axis=1) == 1))[0] + order
    return ph.tolist(), pl.tolist()


def last_confirmed(piv, n, order=ORDER):
    """lastp[b] = индекс последнего pivot p с p+order ≤ b−1 (подтверждён к закрытию b−1, §2.34.2), иначе −1. Без заглядывания вперёд."""
    mark = np.full(n, -1, dtype=np.int64)
    for p in piv:
        c = p + order + 1
        if c < n:
            mark[c] = p
    return np.maximum.accumulate(mark)


# ── выход и издержки ──────────────────────────────────────────────────────────────────────────
def simulate_exit(ts, o, h, l, c, ei, side, sl, tf_ms, fee, slip=SLIP_OF):
    """Вход по open[ei]; TP = entry ± 2R (R = |entry − sl|), таймаут 48 ч (выход по close бара, содержащего момент таймаута), SL раньше TP, бар входа участвует.
    → dict сделки или None (не закрыта к концу данных / SL с неверной стороны)."""
    entry = float(o[ei])
    r = (entry - sl) * side
    if r <= 0:
        return None
    tp = entry + side * TP_R * r
    t_to = int(ts[ei]) + TIMEOUT_H * 3_600_000
    n = len(ts)
    for k in range(ei, n):
        sl_hit = (l[k] <= sl) if side == 1 else (h[k] >= sl)
        tp_hit = (h[k] >= tp) if side == 1 else (l[k] <= tp)
        if sl_hit:
            ex, why = sl, 'SL'
        elif tp_hit:
            ex, why = tp, 'TP'
        elif int(ts[k]) + tf_ms > t_to:
            ex, why = float(c[k]), 'TIMEOUT'
        else:
            continue
        dist = r / entry
        move = side * (ex - entry) / entry
        return {'entry_idx': ei, 'exit_idx': k, 'entry_ts': int(ts[ei]), 'exit_ts': int(ts[k]), 'side': side, 'entry': entry, 'sl': float(sl), 'tp': float(tp),
                'exit': float(ex), 'reason': why, 'dist': dist, 'R': (move - 2 * fee - 2 * slip) / dist, 'R_fee': 2 * fee / dist, 'R_slip': 2 * slip / dist}
    return None


# ── H-OF1 ───────────────────────────────────────────────────────────────────────────────────────
def of1_signals(ts, o, h, l, c, v, tbv):
    """Сигнальные бары H-OF1 (15m). LONG: low[b] < L_ref (последний подтверждённый pivot low до b), close[b] > L_ref, delta[b] < 0 и |delta| ≥ 2×медиана |delta|
    за 96 баров до b (b−96…b−1); SL = low[b] − 0.5·ATR14(b). SHORT — зеркально. → список (b, side, sl) по возрастанию b."""
    n = len(ts)
    atr = H.atr_series(h, l, c, ATR_N)
    delta = 2 * tbv - v
    ph, pl = get_pivots(h, l, ORDER)
    lp_l, lp_h = last_confirmed(pl, n), last_confirmed(ph, n)
    Lref = np.where(lp_l >= 0, l[np.maximum(lp_l, 0)], np.nan)
    Href = np.where(lp_h >= 0, h[np.maximum(lp_h, 0)], np.nan)
    with np.errstate(invalid='ignore'):
        cl_long = (l < Lref) & (c > Lref) & (delta < 0)
        cl_short = (h > Href) & (c < Href) & (delta > 0)
    out = []
    for b in np.where((cl_long | cl_short) & ~np.isnan(atr))[0]:
        if b < MED_BARS:
            continue
        med = float(np.median(np.abs(delta[b - MED_BARS:b])))
        if abs(delta[b]) < DELTA_MULT * med:
            continue
        if cl_long[b]:
            out.append((int(b), 1, float(l[b] - SL_ATR * atr[b])))
        elif cl_short[b]:
            out.append((int(b), -1, float(h[b] + SL_ATR * atr[b])))
    return out


def run_of1(arr, fee, slip=SLIP_OF):
    """H-OF1 по одному символу: одна позиция одновременно; сигналы, пока позиция открыта, игнорируются; SL-дистанция вне [0.3%, 4%] → пропуск."""
    ts, o, h, l, c = arr['ts'], arr['o'], arr['h'], arr['l'], arr['c']
    n = len(ts)
    trades, free_from = [], 0
    for b, side, sl in of1_signals(ts, o, h, l, c, arr['v'], arr['tbv']):
        if b < free_from or b + 1 >= n:
            continue
        ei = b + 1
        d = abs(float(o[ei]) - sl) / float(o[ei])
        if not (DIST1[0] <= d <= DIST1[1]):
            continue
        t = simulate_exit(ts, o, h, l, c, ei, side, sl, TF15, fee, slip)
        if t is None:
            continue
        trades.append(t); free_from = t['exit_idx']
    return trades


# ── H-OF2 ───────────────────────────────────────────────────────────────────────────────────────
def align_oi(ts, E, oi):
    """OI на конец каждого 1h-бара (E = ts+1ч); нет значения → NaN."""
    out = np.full(len(ts), np.nan)
    if len(E) == 0:
        return out
    want = np.asarray(ts, dtype=np.int64) + TF1H
    idx = np.searchsorted(E, want)
    ok = (idx < len(E)) & (E[np.minimum(idx, len(E) - 1)] == want)
    out[ok] = oi[idx[ok]]
    return out


def of2_conditions(ts, o, h, l, c, oi_end):
    """Бары b, где выполнено условие сброса/выброса: LONG — max close окна (b−3…b) − close[b] ≥ 2.5·ATR14(b) И OI(конец b)/OI(конец b−4) − 1 ≤ −3%
    (конец b−4 = начало b−3); SHORT — close[b] − min close окна ≥ 2.5·ATR И то же падение OI. SL: LONG — min low окна − 0.5·ATR; SHORT — max high + 0.5·ATR.
    → список (b, side, sl_signal)."""
    n = len(ts)
    atr = H.atr_series(h, l, c, ATR_N)
    out = []
    if n < WIN + 1:
        return out
    wc, wl, wh = swv(c, WIN), swv(l, WIN), swv(h, WIN)          # окно, заканчивающееся на b: индекс b−3 → b−WIN+1
    for b in range(WIN, n):
        a = atr[b]
        if np.isnan(a) or a <= 0:
            continue
        i0, i1 = oi_end[b - WIN], oi_end[b]
        if np.isnan(i0) or np.isnan(i1) or i0 <= 0 or i1 / i0 - 1 > -OI_DROP:
            continue
        w = b - WIN + 1
        if wc[w].max() - c[b] >= DROP_ATR * a:
            out.append((b, 1, float(wl[w].min() - SL_ATR * a)))
        elif c[b] - wc[w].min() >= DROP_ATR * a:
            out.append((b, -1, float(wh[w].max() + SL_ATR * a)))
    return out


def run_of2(arr, E, oi, fee, slip=SLIP_OF):
    """H-OF2 по одному символу: после условия — вход по open бара после ПЕРВОГО бара b+1…b+4, закрывшегося выше открытия (LONG) / ниже (SHORT); иначе отмена.
    Пока ожидание/позиция — новые сигналы игнорируются. SL-дистанция вне [0.3%, 6%] → пропуск."""
    ts, o, h, l, c = arr['ts'], arr['o'], arr['h'], arr['l'], arr['c']
    n = len(ts)
    oi_end = align_oi(ts, E, oi)
    trades, blocked = [], -1
    for b, side, sl in of2_conditions(ts, o, h, l, c, oi_end):
        if b <= blocked:
            continue
        ei = None
        for j in range(b + 1, min(b + WAIT, n - 1) + 1):
            if (c[j] > o[j]) if side == 1 else (c[j] < o[j]):
                ei = j + 1; break
        blocked = (ei - 1) if ei is not None else b + WAIT           # пока ожидание входа (до решения на open[ei]) новые сигналы игнорируются; отмена — до b+4
        if ei is None or ei >= n:
            continue
        d = abs(float(o[ei]) - sl) / float(o[ei])
        if not (DIST2[0] <= d <= DIST2[1]):
            continue
        t = simulate_exit(ts, o, h, l, c, ei, side, sl, TF1H, fee, slip)
        if t is None:
            continue
        t['signal_idx'] = b
        trades.append(t); blocked = max(blocked, t['exit_idx'] - 1)
    return trades


# ── статистика и критерии ────────────────────────────────────────────────────────────────────────
def split_trades(trades):
    cut = _ms(OOS_START)
    return [t for t in trades if t['entry_ts'] < cut], [t for t in trades if t['entry_ts'] >= cut]


def stats(trades):
    n = len(trades)
    if n == 0:
        return {'n': 0, 'wr': 0.0, 'pf': 0.0, 'mean_r': 0.0, 't': 0.0, 'sl': 0.0, 'tp': 0.0, 'to': 0.0, 'fee_r': 0.0, 'slip_r': 0.0, 'hours': 0.0}
    rs = [t['R'] for t in trades]
    ws = H.weekly_stats(rs)
    gw, gl = sum(x for x in rs if x > 0), -sum(x for x in rs if x < 0)
    pf = (gw / gl) if gl > 0 else (float('inf') if gw > 0 else 0.0)
    cnt = lambda w: sum(1 for t in trades if t['reason'] == w) / n * 100
    return {'n': n, 'wr': sum(1 for x in rs if x > 0) / n * 100, 'pf': pf, 'mean_r': ws['mean'], 't': ws['t'], 'sl': cnt('SL'), 'tp': cnt('TP'), 'to': cnt('TIMEOUT'),
            'fee_r': sum(t['R_fee'] for t in trades) / n, 'slip_r': sum(t['R_slip'] for t in trades) / n,
            'hours': sum(t['exit_ts'] - t['entry_ts'] for t in trades) / n / 3_600_000}


def daily_portfolio(trades):
    """Дневной портфель: среднее R сделок, ЗАКРЫТЫХ в день (UTC дата exit_ts). → H.weekly_stats по ряду дневных значений (n = число дней)."""
    days = {}
    for t in trades:
        days.setdefault(int(t['exit_ts'] // 86_400_000), []).append(t['R'])
    return H.weekly_stats([sum(v) / len(v) for v in days.values()])


def verdict(trades):
    """Критерии §2.34.5 по OOS (лонг и шорт вместе): дневной mean R net > 0 при t ≥ 2.5; PF net сделок ≥ 1.3; OOS дневной mean R ≥ 0.5 × IS;
    n OOS ≥ 200 и дней с закрытыми сделками ≥ 100, иначе «недостаточно данных»."""
    is_t, oos_t = split_trades(trades)
    d_is, d_oos = daily_portfolio(is_t), daily_portfolio(oos_t)
    st_oos = stats(oos_t)
    res = {'is': is_t, 'oos': oos_t, 'd_is': d_is, 'd_oos': d_oos, 'st_oos': st_oos}
    if st_oos['n'] < CRIT_N or d_oos['n'] < CRIT_DAYS:
        res.update(status='NODATA', text=f'недостаточно данных (OOS сделок {st_oos["n"]}, дней {d_oos["n"]}; нужно ≥ {CRIT_N} и ≥ {CRIT_DAYS})', checks={})
        return res
    checks = {
        f'дневной mean R net&gt;0 при t≥{CRIT_T}': d_oos['mean'] > 0 and d_oos['t'] >= CRIT_T,
        f'PF net сделок OOS≥{CRIT_PF}': st_oos['pf'] >= CRIT_PF,
        f'OOS mean R ≥ {CRIT_RATIO}×IS': d_oos['mean'] >= CRIT_RATIO * d_is['mean'],
    }
    ok = all(checks.values())
    res.update(status='PASS' if ok else 'FAIL', text='ПРОЙДЕНА' if ok else 'не пройдена', checks=checks)
    return res


# ── хранение сделок ────────────────────────────────────────────────────────────────────────────────
def init_trades_table(db_path):
    con = sqlite3.connect(db_path)
    con.execute("CREATE TABLE IF NOT EXISTS of_trades (id INTEGER PRIMARY KEY AUTOINCREMENT, run_ts TEXT, hyp TEXT, symbol TEXT, side INTEGER, entry_ts INTEGER, exit_ts INTEGER, "
                "entry REAL, sl REAL, tp REAL, exit_px REAL, reason TEXT, R REAL, R_fee REAL, R_slip REAL, period TEXT)")
    con.commit(); con.close()


def save_trades(db_path, hyp, trades_by_sym):
    """Заменяет сделки гипотезы: хранятся только сделки (не строки свечей/OI)."""
    init_trades_table(db_path)
    ts = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    cut = _ms(OOS_START)
    rows = [(ts, hyp, sym, t['side'], t['entry_ts'], t['exit_ts'], t['entry'], t['sl'], t['tp'], t['exit'], t['reason'], t['R'], t['R_fee'], t['R_slip'],
             'OOS' if t['entry_ts'] >= cut else 'IS') for sym, trs in trades_by_sym.items() for t in trs]
    con = sqlite3.connect(db_path)
    con.execute("DELETE FROM of_trades WHERE hyp=?", (hyp,))
    con.executemany("INSERT INTO of_trades (run_ts,hyp,symbol,side,entry_ts,exit_ts,entry,sl,tp,exit_px,reason,R,R_fee,R_slip,period) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)
    con.commit(); con.close()
    return len(rows)


# ── отчёт ───────────────────────────────────────────────────────────────────────────────────────────
def _fpf(x):
    return 'inf' if x == float('inf') else f'{x:.2f}'


def _row(label, s):
    if s['n'] == 0:
        return f'  {label}: n=0'
    return (f'  {label}: n={s["n"]} | WR {s["wr"]:.0f}% | PF net {_fpf(s["pf"])} | mean R net {s["mean_r"]:+.3f} | t {s["t"]:.2f} | ср. длит. {s["hours"]:.1f} ч | '
            f'SL {s["sl"]:.0f}% / TP {s["tp"]:.0f}% / таймаут {s["to"]:.0f}% | комиссия {s["fee_r"]:.3f}R + проскальзывание {s["slip_r"]:.3f}R')


def build_report(results, skipped, missing, fee, note=''):
    """results: {hyp: {'trades_by_sym', 'verdict'}}; skipped: {hyp: причина}; missing: {hyp: [символы без данных]}."""
    L = ['📈 <b>/of_backtest: H-OF1 («прокол с поглощением», 15m) и H-OF2 («сброс с ликвидациями», 1h) по BOT_SPEC §2.34</b>',
         f'Данные: архив data.binance.vision (USDT-M), сигналы по свечам Binance; издержки BingX: FEE_TAKER={fee} на вход и выход (в т.ч. TP) + проскальзывание {SLIP_OF} на сторону; '
         f'TP 2R, таймаут {TIMEOUT_H} ч, SL раньше TP, вход по open следующего бара. IS до {IS_END}, OOS с {OOS_START}. Критерии: дневной портфель mean R&gt;0 при t≥{CRIT_T}, PF≥{CRIT_PF}, '
         f'OOS≥{CRIT_RATIO}×IS, n OOS≥{CRIT_N} и дней≥{CRIT_DAYS}. Пороги и правила не подбирались.',
         '⚠️ ОШИБКА ВЫЖИВШЕГО: вселенная — фиксированные 16 символов H-XS (отобраны по объёму на 2026-10), результат на ней систематически оптимистичен.']
    if note:
        L.append(note)
    for hyp in HYPS:
        L.append(f'\n<b>{hyp}</b>')
        if hyp in skipped:
            L.append(f'  ПРОПУЩЕНА: {skipped[hyp]}')
            continue
        r = results[hyp]; v = r['verdict']
        if missing.get(hyp):
            L.append(f'  ⚠️ без данных/частично: {", ".join(missing[hyp])}')
        trades = [t for trs in r['trades_by_sym'].values() for t in trs]
        for per, tr in (('IS', v['is']), ('OOS', v['oos'])):
            L.append(f' {per}:')
            L.append(_row('лонг+шорт', stats(tr)))
            L.append(_row('лонг', stats([t for t in tr if t['side'] == 1])))
            L.append(_row('шорт', stats([t for t in tr if t['side'] == -1])))
            d = v['d_is'] if per == 'IS' else v['d_oos']
            L.append(f'    дневной портфель: mean R {d["mean"]:+.3f}, t {d["t"]:.2f}, дней с закрытыми сделками {d["n"]}')
        L.append(' По символам (справочно, OOS): ' + '; '.join(
            f'{s.replace("USDT", "")} n={stats([t for t in split_trades(trs)[1]])["n"]} R {stats([t for t in split_trades(trs)[1]])["mean_r"]:+.2f}' for s, trs in r['trades_by_sym'].items()))
        L.append(' Критерии: ' + (' | '.join(f'{k} {"✅" if ok else "❌"}' for k, ok in v['checks'].items()) if v['checks'] else '—'))
        L.append(f' <b>ВЕРДИКТ {hyp}: {v["text"]}</b> (всего сделок {len(trades)})')
    done = [h for h in HYPS if h in results]
    if done:
        passed = [h for h in done if results[h]['verdict']['status'] == 'PASS']
        L.append('\n<b>РЕШЕНИЕ: ' + (f'прошла {", ".join(passed)} → отдельный пре-коммит shadow-форварда с живыми данными (отдельный деплой)'
                                     if passed else 'ни одна гипотеза не прошла — исследования на этих данных завершены, правило остановки §2.32.8 действует без исключений') + '</b>')
    return '\n'.join(L)


# ── оркестрация ────────────────────────────────────────────────────────────────────────────────────────
def binance_symbols(universe):
    return [s.split('/')[0] + 'USDT' for s in universe]


def gate(status):
    """Разрешение запуска по последнему /data_probe v2: → (allowed{hyp: bool}, reasons{hyp: str})."""
    if not status:
        return {h: False for h in HYPS}, {h: 'нет результата /data_probe — запусти /data_probe' for h in HYPS}
    allowed = {'H-OF1': bool(status['ok1']), 'H-OF2': bool(status['ok2'])}
    reasons = {'H-OF1': status['h_of1'], 'H-OF2': status['h_of2']}
    return allowed, reasons


def load_arrays(d, sym, tf):
    k = V.load_klines(d, sym, tf)
    if not k or len(k['ts']) < 300:
        return None
    return k


async def run_of(fetch, db_path, universe, allowed, reasons, fee, progress=None, d=None):
    """Загрузка (фон, по символам, с продолжением из кэша) → расчёт по символу в памяти → сделки в of_trades → отчёт."""
    d = d or V.vision_dir()
    syms = binance_symbols(universe)
    skipped = {h: reasons[h] for h in HYPS if not allowed.get(h)}
    results, missing, stopped = {}, {h: [] for h in HYPS}, ''

    async def say(m):
        if progress:
            try:
                await progress(m)
            except Exception:
                pass
    for sym in syms:
        try:
            if allowed.get('H-OF1'):
                r = await V.sync_klines(fetch, d, sym, '15m', say)
                if r['failed']:
                    await say(f'{sym} 15m: не загружено {len(r["failed"])} файлов (пропущены, остальное считается)')
            if allowed.get('H-OF2'):
                r = await V.sync_klines(fetch, d, sym, '1h', say)
                if r['failed']:
                    await say(f'{sym} 1h: не загружено {len(r["failed"])} файлов')
                r = await V.sync_oi(fetch, d, sym, say)
                if r['failed']:
                    await say(f'{sym} OI: не загружено {len(r["failed"])} дней')
        except V.VisionError as e:
            if e.kind == 'low_disk':
                stopped = f'⛔ ЗАГРУЗКА ОСТАНОВЛЕНА: {e}'
                break
            await say(f'{sym}: ошибка загрузки — {str(e)[:150]}')
        except Exception as e:
            await say(f'{sym}: ошибка загрузки — {type(e).__name__}: {str(e)[:150]}')
    if stopped:
        await say(stopped)
    for hyp in HYPS:
        if hyp in skipped:
            continue
        by_sym = {}
        for sym in syms:
            if hyp == 'H-OF1':
                k = load_arrays(d, sym, '15m')
                trs = await asyncio.to_thread(run_of1, k, fee) if k else None
            else:
                k = load_arrays(d, sym, '1h'); oi = V.load_oi(d, sym)
                trs = await asyncio.to_thread(run_of2, k, oi['E'], oi['oi'], fee) if (k and oi and len(oi['E'])) else None
            if trs is None:
                missing[hyp].append(sym); continue
            by_sym[sym] = trs
            await say(f'{hyp} {sym}: сделок {len(trs)}')
            await asyncio.sleep(0)
        all_trades = [t for trs in by_sym.values() for t in trs]
        results[hyp] = {'trades_by_sym': by_sym, 'verdict': verdict(all_trades)}
        save_trades(db_path, hyp, by_sym)
    return build_report(results, skipped, missing, fee, stopped), results
