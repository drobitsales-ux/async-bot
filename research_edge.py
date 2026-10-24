"""research_edge.py — /edge_audit: R0 (калибровка измерительной системы на случайных входах) и R1 (SMC2 на истории 2020–2026)
СТРОГО по BOT_SPEC §2.37 (пре-коммит записан ДО кода). Никаких дополнительных вариантов, порогов и фильтров.

Логика входов — РОВНО `_smc2_eval` бота: функция и `get_pivots` передаются аргументами (smc2_eval, get_pivots), копии кода нет (иначе
модуль импортировал бы async_bot — циклический импорт). Данные — архив data.binance.vision через research_vision (npz-кэш); месячные
файлы содержат только завершённые месяцы, текущий месяц добирается дневными файлами (хвост `klines_15m_tail.npz`); ставки фандинга —
месячные файлы fundingRate (`funding.npz`). Расчёт по символу целиком в памяти; в БД пишется только отчёт (htf_reports)."""
import asyncio
import bisect
import math
import os
import sqlite3
import zipfile
from datetime import datetime, timezone

import numpy as np

import research_htf as H
import research_vision as V

SEED = 20261024
N_RANDOM = 2000
SLIP = 0.0003
TF_MS = 15 * 60_000
WINDOW = 63                    # закрытых баров в окне scan_smc2 (fetch_ohlcv limit=64 минус формирующийся)
ORD = 3                        # SMC_PIVOT_ORDER
TP_R = 2.0
TIMEOUT_BARS = 16
SL_BOUNDS = (0.3, 3.0)         # % цены входа
VOL_MULT = 1.5
DEDUP_MS = 2 * 3600 * 1000
OOS_START = '2025-01-01'
CRIT_T, CRIT_PF, CRIT_RATIO, CRIT_N, CRIT_DAYS = 3.0, 1.3, 0.5, 200, 100
R0_STOP_T, R0_WARN_T = 3.0, 2.0
FUND_MIN_COV = 0.9
FUND_MAX_AGE_MS = 24 * 3_600_000
R0_WARM, R0_AFTER = 30, 18
VARIANTS = ('V0', 'V1', 'V2', 'F')
ENTRIES = ('MKT', 'RT')
SIDES = ('Long', 'Short')
TERMINAL_RT = ('expired', 'cancel_2r', 'invalid', 'rt_sl_bounds')


def _ms(date_str):
    return int(datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc).timestamp() * 1000)


def _month(ms):
    return datetime.fromtimestamp(ms / 1000, timezone.utc).strftime('%Y-%m')


def _year(ms):
    return datetime.fromtimestamp(ms / 1000, timezone.utc).year


# ── данные: сегменты, ATR ───────────────────────────────────────────────────────────────────────
def segments(ts, min_len=1):
    """Непрерывные сегменты ряда (разрыв > 15 мин начинает новый). → [(start, end_exclusive)]."""
    ts = np.asarray(ts)
    if len(ts) == 0:
        return []
    cut = np.where(np.diff(ts) != TF_MS)[0] + 1
    st = [0] + cut.tolist()
    en = cut.tolist() + [len(ts)]
    return [(a, b) for a, b in zip(st, en) if b - a >= min_len]


def atr14(h, l, c):
    """atr[i] = простое среднее TR последних 14 баров ≤ i (как `calc_atr(h[:i+1], …)` бота); NaN, пока < 15 баров."""
    n = len(c)
    out = np.full(n, np.nan)
    if n < 15:
        return out
    tr = np.maximum(h[1:] - l[1:], np.maximum(np.abs(h[1:] - c[:-1]), np.abs(l[1:] - c[:-1])))
    cs = np.concatenate([[0.0], np.cumsum(tr)])
    # tr[j] относится к бару j+1; atr[i] = mean(tr[i-14 .. i-1]) = mean по барам i-13..i
    out[14:] = (cs[14:] - cs[:-14]) / 14.0
    return out


# ── выход (семантика _shadow_exit_eval для SMC2_MKT/SMC2_RT) и издержки ─────────────────────────────
def sim_exit(h, l, c, eb, is_long, sl, tp, end):
    """Путь с бара входа eb включительно; внутри бара SL раньше TP; таймаут — бар eb+16 (выход по close, SL/TP этого бара проверяются раньше).
    end — индекс за последним доступным баром сегмента. → (k, цена, причина) или None (не закрыта к концу данных)."""
    last = eb + TIMEOUT_BARS
    for k in range(eb, min(last, end - 1) + 1):
        sl_hit = (l[k] <= sl) if is_long else (h[k] >= sl)
        tp_hit = (h[k] >= tp) if is_long else (l[k] <= tp)
        if sl_hit:
            return k, float(sl), 'SL'
        if tp_hit:
            return k, float(tp), 'TP'
        if k == last:
            return k, float(c[k]), 'TIMEOUT'
    return None


def trade_costs(etype, reason, fee_maker, fee_taker, slip=SLIP):
    """Доля цены: комиссия входа + выхода и проскальзывание (BOT_SPEC §2.37.2)."""
    fee_in = fee_maker if etype == 'RT' else fee_taker
    fee_out = fee_maker if reason == 'TP' else fee_taker
    sl_ = (slip if etype != 'RT' else 0.0) + (slip if reason in ('SL', 'TIMEOUT') else 0.0)
    return fee_in + fee_out + sl_


def make_trade(sym, etype, side_long, entry, sl, tp, eb, ts, h, l, c, end, fees):
    ex = sim_exit(h, l, c, eb, side_long, sl, tp, end)
    if ex is None:
        return None
    k, px, why = ex
    d = abs(entry - sl) / entry
    if d <= 0:
        return None
    move = ((px - entry) if side_long else (entry - px)) / entry
    cost = trade_costs(etype, why, fees[0], fees[1])
    return {'sym': sym, 'etype': etype, 'long': bool(side_long), 'entry_ts': int(ts[eb]), 'exit_ts': int(ts[k]) + TF_MS,
            'entry': float(entry), 'exit': px, 'reason': why, 'dist': d, 'Rg': move / d, 'R': (move - cost) / d, 'held': k - eb}


# ── R0: случайные входы ──────────────────────────────────────────────────────────────────────────────
def r0_trades(sym, idx, arr, fees, n_target=N_RANDOM, seed_base=SEED):
    """По n_target случайных входов на символ; i — номер символа в алфавитном порядке (seed = SEED + i). Вход = close[b], путь с бара b+1,
    SL = 1.0×ATR14, границы [0.3%, 3%], TP 2R, таймаут 16 баров. → (trades, диагностика)."""
    ts, h, l, c = arr['ts'], arr['h'], arr['l'], arr['c']
    rng = np.random.default_rng(seed_base + idx)
    atr = atr14(h, l, c)
    valid = []
    for a, b in segments(ts):
        lo, hi = a + R0_WARM, b - 1 - R0_AFTER
        if hi >= lo:
            valid.append(np.arange(lo, hi + 1))
    diag = {'valid': 0, 'tried': 0, 'bounds': 0, 'unclosed': 0}
    if not valid:
        return [], diag
    valid = np.concatenate(valid)
    perm = rng.permutation(valid)
    dirs = rng.integers(0, 2, size=len(perm))
    diag['valid'] = int(len(valid))
    ends = np.zeros(len(ts), dtype=np.int64)
    for a, b in segments(ts):
        ends[a:b] = b
    out = []
    for j, b in enumerate(perm):
        if len(out) >= n_target:
            break
        diag['tried'] += 1
        entry = float(c[b])
        a_ = float(atr[b])
        if not (a_ > 0) or math.isnan(a_):
            diag['bounds'] += 1
            continue
        d_pct = a_ / entry * 100
        if not (SL_BOUNDS[0] <= d_pct <= SL_BOUNDS[1]):
            diag['bounds'] += 1
            continue
        is_long = bool(dirs[j])
        sl = entry - a_ if is_long else entry + a_
        tp = entry + TP_R * a_ if is_long else entry - TP_R * a_
        t = make_trade(sym, 'MKT', is_long, entry, sl, tp, int(b) + 1, ts, h, l, c, int(ends[b]), fees)
        if t is None:
            diag['unclosed'] += 1
            continue
        out.append(t)
    return out, diag


# ── R1: воспроизведение scan_smc2 ───────────────────────────────────────────────────────────────────
def scan_segment(ts, o, h, l, c, v, ph, pl, smc2_eval, vol_mult=None, window=WINDOW):
    """Для каждого закрытого бара e сегмента — окно из `window` баров, pivot-ы окна (из pivot-ов полного ряда с обрезкой [начало+3, e−3],
    тождественно get_pivots на окне), оба направления, `smc2_eval` бота. Идемпотентность записи как в `_smc2_persist`: MKT — при первом
    появлении события с mkt; RT — при первом окне со состоянием filled; маркер NF — состояние без заполнения.
    → dict {(mode, ts_b): запись}; индексы b, f — внутри сегмента."""
    n = len(c)
    events = {}
    for e in range(window - 1, n):
        s = e - window + 1
        a = bisect.bisect_left(ph, s + ORD); b_ = bisect.bisect_right(ph, e - ORD)
        pwh = [x - s for x in ph[a:b_]]
        a = bisect.bisect_left(pl, s + ORD); b_ = bisect.bisect_right(pl, e - ORD)
        pwl = [x - s for x in pl[a:b_]]
        sl_ = slice(s, e + 1)
        tsw, ow, hw, lw, cw, vw = ts[sl_], o[sl_], h[sl_], l[sl_], c[sl_], v[sl_]
        for mode in SIDES:
            kw = {} if vol_mult is None else {'vol_mult': vol_mult}
            evs, _stage = smc2_eval(tsw, ow, hw, lw, cw, vw, mode, pwh, pwl, **kw)
            for ev in evs:
                key = (mode, int(ev['ts_b']))
                rec = events.get(key)
                if rec is None:
                    rec = events[key] = {'mode': mode, 'ts_b': key[1], 'b': s + ev['b'], 'rec_e': None, 'mkt': None, 'rt': None, 'nf': False}
                if rec['mkt'] is None and ev['mkt'] is not None:
                    m = ev['mkt']
                    rec['mkt'] = (m['entry'], m['sl'], m['tp'])
                    if rec['rec_e'] is None:
                        rec['rec_e'] = e
                rt = ev['rt']
                if rt['state'] == 'filled' and rec['rt'] is None:
                    rec['rt'] = (s + rt['f'], rt['entry'], rt['sl'], rt['tp'])
                    if rec['rec_e'] is None:
                        rec['rec_e'] = e
                elif rt['state'] in TERMINAL_RT and rec['rec_e'] is None and rec['rt'] is None:
                    rec['nf'] = True
                    rec['rec_e'] = e
    for rec in events.values():
        b = rec['b']
        med0 = float(np.median(v[b - 20:b]))
        rec['v0'] = not (med0 <= 0 or v[b] < VOL_MULT * med0)
        med2 = float(np.median(v[b - 20:b - 1])) if b >= 21 else 0.0
        rec['v2'] = not (med2 <= 0 or v[b - 1] < VOL_MULT * med2)
    return events


def fund_at(ft, fr, ms):
    """Ставка фандинга на ПОСЛЕДНЕМ расчёте ≤ ms; None — нет расчёта или он старше 24 ч."""
    if ft is None or len(ft) == 0:
        return None
    i = int(np.searchsorted(ft, ms, side='right')) - 1
    if i < 0 or ms - int(ft[i]) > FUND_MAX_AGE_MS:
        return None
    return float(fr[i])


def _fund_ok(mode, rate):
    return rate is not None and ((rate > 0) if mode == 'Short' else (rate < 0))


def variant_candidates(events, variant, ts, funding=None):
    """События варианта ДО дедупа (фильтр варианта — единственное, чем варианты различаются): V1 — все записанные события; V0 — с объёмом пробоя;
    V2 — с объёмом предыдущей свечи; F — V0 + знак фандинга на момент закрытия бара пробоя (шорт: > 0, лонг: < 0). → (список событий, число без фандинга)."""
    ft, fr = funding if funding else (None, None)
    cand, nofund = [], 0
    for rec in events.values():
        if rec['rec_e'] is None:
            continue
        if variant in ('V0', 'F') and not rec['v0']:
            continue
        if variant == 'V2' and not rec['v2']:
            continue
        if variant == 'F':
            r_ = fund_at(ft, fr, int(ts[rec['b']]) + TF_MS)
            if r_ is None:
                nofund += 1
                continue
            if not _fund_ok(rec['mode'], r_):
                continue
        cand.append(rec)
    return cand, nofund


def variant_trades(sym, events, variant, arr_seg, fees, funding=None):
    """Сделки варианта по событиям символа/сегмента. Дедуп — ПОСЛЕ фильтра варианта (2 ч на (символ, направление), как `_smc2_dedup_blocked`; решение — в
    момент первой записи события). У варианта F RT-сделка дополнительно требует знака фандинга на момент заполнения. arr_seg = (ts, h, l, c, end)."""
    ts, h, l, c, end = arr_seg
    diag = {'events': 0, 'nofund': 0, 'dedup': 0, 'late_mkt': 0}
    ft, fr = funding if funding else (None, None)
    cand, diag['nofund'] = variant_candidates(events, variant, ts, funding)
    cand.sort(key=lambda r: (r['rec_e'], r['ts_b']))
    kept = {}
    trades = []
    for rec in cand:
        key = rec['mode']
        if any(abs(rec['ts_b'] - x) < DEDUP_MS for x in kept.get(key, ())):
            diag['dedup'] += 1
            continue
        kept.setdefault(key, []).append(rec['ts_b'])
        diag['events'] += 1
        is_long = rec['mode'] == 'Long'
        if rec['mkt'] is not None:
            if rec['rec_e'] > rec['b']:
                diag['late_mkt'] += 1
            entry, sl, tp = rec['mkt']
            t = make_trade(sym, 'MKT', is_long, entry, sl, tp, rec['b'] + 1, ts, h, l, c, end, fees)
            if t:
                t['ts_b'] = rec['ts_b']
                trades.append(t)
        if rec['rt'] is not None:
            f, entry, sl, tp = rec['rt']
            if variant == 'F' and not _fund_ok(rec['mode'], fund_at(ft, fr, int(ts[f]))):
                continue
            t = make_trade(sym, 'RT', is_long, entry, sl, tp, f, ts, h, l, c, end, fees)
            if t:
                t['ts_b'] = rec['ts_b']
                trades.append(t)
    return trades, diag


def r1_symbol(sym, arr, fees, smc2_eval, get_pivots, funding=None):
    """Полный расчёт R1 по символу: scan (V1 = без фильтра объёма — надмножество), затем варианты. → {variant: (trades, diag)}, события V0."""
    ts, o, h, l, c, v = (arr[k] for k in ('ts', 'o', 'h', 'l', 'c', 'v'))
    res = {k: ([], {'events': 0, 'nofund': 0, 'dedup': 0, 'late_mkt': 0}) for k in VARIANTS}
    v0_events = []
    gaps = 0
    for a, b in segments(ts):
        gaps += 1
        if b - a < WINDOW + TIMEOUT_BARS + 2:
            continue
        sts, so, sh, sl_, sc, sv = ts[a:b], o[a:b], h[a:b], l[a:b], c[a:b], v[a:b]
        ph, pl = get_pivots(sh, sl_, order=ORD)
        events = scan_segment(sts, so, sh, sl_, sc, sv, ph, pl, smc2_eval, vol_mult=0.0)
        for var in VARIANTS:
            tr, dg = variant_trades(sym, events, var, (sts, sh, sl_, sc, b - a), fees, funding)
            res[var][0].extend(tr)
            for k, x in dg.items():
                res[var][1][k] += x
        for rec in events.values():
            if rec['v0'] and rec['rec_e'] is not None:
                v0_events.append((rec['mode'], rec['ts_b']))
    return res, v0_events, gaps - 1 if gaps else 0


# ── статистика и критерии ────────────────────────────────────────────────────────────────────────────
def _tstat(vals):
    n = len(vals)
    if n < 2:
        return 0.0
    m = sum(vals) / n
    sd = math.sqrt(sum((x - m) ** 2 for x in vals) / (n - 1))
    return m / (sd / math.sqrt(n)) if sd > 0 else 0.0


def daily_stats(trades, key='R'):
    days = {}
    for t in trades:
        days.setdefault(int(t['exit_ts'] // 86_400_000), []).append(t[key])
    return H.weekly_stats([sum(x) / len(x) for x in days.values()])


def stats(trades):
    n = len(trades)
    if n == 0:
        return {'n': 0, 'wr': 0.0, 'pf': 0.0, 'mean': 0.0, 't': 0.0, 'mean_g': 0.0, 't_g': 0.0, 'dmean': 0.0, 'dt': 0.0, 'days': 0,
                'sl': 0.0, 'tp': 0.0, 'to': 0.0}
    rs = [t['R'] for t in trades]; rg = [t['Rg'] for t in trades]
    gw, gl = sum(x for x in rs if x > 0), -sum(x for x in rs if x < 0)
    d = daily_stats(trades)
    cnt = lambda w: sum(1 for t in trades if t['reason'] == w) / n * 100
    return {'n': n, 'wr': sum(1 for x in rs if x > 0) / n * 100, 'pf': (gw / gl) if gl > 0 else (float('inf') if gw > 0 else 0.0),
            'mean': sum(rs) / n, 't': _tstat(rs), 'mean_g': sum(rg) / n, 't_g': _tstat(rg), 'dmean': d['mean'], 'dt': d['t'], 'days': d['n'],
            'sl': cnt('SL'), 'tp': cnt('TP'), 'to': cnt('TIMEOUT')}


def split_is_oos(trades):
    cut = _ms(OOS_START)
    return [t for t in trades if t['entry_ts'] < cut], [t for t in trades if t['entry_ts'] >= cut]


def btc_month_dirs(arr):
    """{YYYY-MM: доходность BTC за месяц} (первый open → последний close месяца)."""
    if not arr:
        return {}
    ts, o, c = arr['ts'], arr['o'], arr['c']
    out, first, last = {}, {}, {}
    for i in range(len(ts)):
        m = _month(int(ts[i]))
        if m not in first:
            first[m] = float(o[i])
        last[m] = float(c[i])
    for m in first:
        if first[m] > 0:
            out[m] = last[m] / first[m] - 1
    return out


def month_split(trades, btc):
    """mean R net по входам в месяцы роста / падения BTC: {'up': (n, mean), 'down': (n, mean)}; месяцы без данных BTC пропускаются."""
    up, dn = [], []
    for t in trades:
        r = btc.get(_month(t['entry_ts']))
        if r is None:
            continue
        (up if r > 0 else dn).append(t['R'])
    f = lambda x: (len(x), (sum(x) / len(x)) if x else 0.0)
    return {'up': f(up), 'down': f(dn)}


def cell_verdict(trades, btc):
    """Критерии §2.37.5. → dict(status PASS|FAIL|NODATA, checks, is_stats, oos_stats, months, months_oos)."""
    is_t, oos_t = split_is_oos(trades)
    s_is, s_oos = stats(is_t), stats(oos_t)
    ms = month_split(trades, btc)
    res = {'is': s_is, 'oos': s_oos, 'months': ms, 'months_oos': month_split(oos_t, btc), 'checks': {}}
    if s_oos['n'] < CRIT_N or s_oos['days'] < CRIT_DAYS:
        res.update(status='NODATA', text=f'недостаточно данных (OOS сделок {s_oos["n"]}, дней {s_oos["days"]}; нужно ≥ {CRIT_N} и ≥ {CRIT_DAYS})')
        return res
    checks = {
        f'OOS дневной mean R&gt;0 при t≥{CRIT_T}': s_oos['dmean'] > 0 and s_oos['dt'] >= CRIT_T,
        f'PF net OOS≥{CRIT_PF}': s_oos['pf'] >= CRIT_PF,
        f'OOS mean R≥{CRIT_RATIO}×IS': s_oos['dmean'] >= CRIT_RATIO * s_is['dmean'],
        'mean R&gt;0 в месяцы роста BTC': ms['up'][0] > 0 and ms['up'][1] > 0,
        'mean R&gt;0 в месяцы падения BTC': ms['down'][0] > 0 and ms['down'][1] > 0,
    }
    ok = all(checks.values())
    res.update(status='PASS' if ok else 'FAIL', checks=checks, text='ПРОЙДЕНА' if ok else 'не пройдена')
    return res


def by_year(trades):
    out = {}
    for t in trades:
        out.setdefault(_year(t['entry_ts']), []).append(t['R'])
    return {y: (len(v), sum(v) / len(v)) for y, v in sorted(out.items())}


# ── сверка с shadow (1.6) ────────────────────────────────────────────────────────────────────────────
def compare_shadow(db_path, r1_events, bingx_of, data_end_ms, since='2026-10-02'):
    """r1_events: {binance_sym: [(mode, ts_b)]} (V0). Shadow: SMC2_MKT с open_time ≥ since по тем же символам. Период сверки: [since, min(конец данных,
    последняя запись shadow SMC2_MKT)]. Совпадение: символ + направление + |open_time − (ts_b+15 мин)| ≤ 15 мин. Только справочно."""
    t0 = _ms(since)
    try:
        con = sqlite3.connect(db_path)
        rows = con.execute("SELECT symbol, direction, open_time FROM shadow_signals WHERE strategy='SMC2_MKT' AND open_time>=?", (since,)).fetchall()
        con.close()
    except Exception as e:
        return {'ok': False, 'why': f'БД недоступна: {type(e).__name__}'}
    if not rows:
        return {'ok': False, 'why': f'в shadow нет записей SMC2_MKT с {since}'}
    inv = {v: k for k, v in bingx_of.items()}
    sh = []
    for s_, d_, ot in rows:
        if s_ not in inv:
            continue
        try:
            dt = datetime.fromisoformat(ot)
            dt = dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
            sh.append((inv[s_], d_, int(dt.timestamp() * 1000)))
        except Exception:
            continue
    if not sh:
        return {'ok': False, 'why': 'записи shadow есть, но не по символам вселенной (16)'}
    t1 = min(data_end_ms, max(x[2] for x in sh))
    if t1 <= t0:
        return {'ok': False, 'why': f'нет пересечения периодов: данные Binance до {datetime.fromtimestamp(data_end_ms / 1000, timezone.utc):%Y-%m-%d}'}
    sh = [x for x in sh if t0 <= x[2] <= t1]
    r1 = [(s_, m, tb + TF_MS) for s_, evs in r1_events.items() for (m, tb) in evs if t0 <= tb + TF_MS <= t1]
    sh_idx, r1_idx = {}, {}
    for s_, d_, ms in sh:
        sh_idx.setdefault((s_, d_), []).append(ms)
    for s_, d_, ms in r1:
        r1_idx.setdefault((s_, d_), []).append(ms)
    near = lambda lst, x: any(abs(y - x) <= TF_MS for y in lst)
    r1_in_sh = sum(1 for s_, d_, ms in r1 if near(sh_idx.get((s_, d_), ()), ms))
    sh_in_r1 = sum(1 for s_, d_, ms in sh if near(r1_idx.get((s_, d_), ()), ms))
    cnt = lambda lst, d: sum(1 for x in lst if x[1] == d)
    return {'ok': True, 't0': t0, 't1': t1, 'r1': len(r1), 'shadow': len(sh),
            'r1_long': cnt(r1, 'Long'), 'r1_short': cnt(r1, 'Short'), 'sh_long': cnt(sh, 'Long'), 'sh_short': cnt(sh, 'Short'),
            'r1_in_sh': r1_in_sh, 'sh_in_r1': sh_in_r1}


# ── фандинг: разбор и загрузка ───────────────────────────────────────────────────────────────────────
def parse_funding_csv(text):
    """Файл fundingRate: заголовок (calc_time, funding_interval_hours, last_funding_rate) или без него; колонки определяются по заголовку, без заголовка —
    время в 1-й колонке, ставка в последней. → (ts мс int64, rate float64) по возрастанию."""
    lines = [x for x in text.splitlines() if x.strip()]
    if not lines:
        raise V.VisionError('parse', 'пустой файл фандинга')
    head = [x.strip().lower() for x in lines[0].split(',')]
    has_head = not lines[0].strip()[:1].isdigit()
    rows = lines[1:] if has_head else lines
    it, ir = 0, -1
    if has_head:
        tcols = [i for i, x in enumerate(head) if 'time' in x]
        rcols = [i for i, x in enumerate(head) if 'funding_rate' in x or 'fundingrate' in x]
        if not tcols or not rcols:
            raise V.VisionError('parse', f'фандинг: не найдены колонки времени/ставки в заголовке {head}')
        it, ir = tcols[0], rcols[-1]
    ts, rate = [], []
    for ln in rows:
        p = ln.split(',')
        if len(p) <= max(it, ir if ir >= 0 else 0):
            continue
        ts.append(V._ts_cell(p[it])); rate.append(float(p[ir]))
    if not ts:
        raise V.VisionError('parse', 'фандинг: нет строк данных')
    ts = np.array(ts, dtype=np.int64); rate = np.array(rate, dtype=np.float64)
    o = np.argsort(ts, kind='stable')
    return ts[o], rate[o]


def funding_path(d, sym):
    return os.path.join(d, sym, 'funding.npz')


def load_funding(d, sym):
    z = V._load_npz(funding_path(d, sym))
    if z is None:
        return None
    return {'ts': z['ts'], 'rate': z['rate'].astype(np.float64), 'months': [str(x) for x in z['months']]}


async def sync_funding(fetch, d, sym, progress=None):
    """Догружает недостающие месяцы fundingRate; сохранение после каждого месяца (атомарно)."""
    sem = asyncio.Semaphore(V.concurrency())
    keys, _ = await V.list_keys(fetch, f'data/futures/um/monthly/fundingRate/{sym}/')
    cur = load_funding(d, sym)
    have = set(cur['months']) if cur else set()
    todo = [k for k in keys if V.key_date(k) and V.key_date(k) not in have]
    res = {'listed': len(keys), 'loaded_now': 0, 'failed': {}}
    lock = asyncio.Lock()

    async def one(k):
        async with sem:
            V.check_space(d)
            try:
                body = await V.download_zip(fetch, k)
                ts, rate = parse_funding_csv(V.unzip_text(body))
            except V.VisionError as e:
                if e.kind == 'low_disk':
                    raise
                res['failed'][k] = str(e)[:200]; return
            except (zipfile.BadZipFile, ValueError, KeyError) as e:
                res['failed'][k] = f'parse: {type(e).__name__}: {str(e)[:100]}'; return
            async with lock:
                c_ = load_funding(d, sym)
                if c_:
                    ts = np.concatenate([c_['ts'], ts]); rate = np.concatenate([c_['rate'], rate]); months = sorted(set(c_['months']) | {V.key_date(k)})
                else:
                    months = [V.key_date(k)]
                o = np.argsort(ts, kind='stable'); ts, rate = ts[o], rate[o]
                keep = np.r_[True, np.diff(ts) != 0] if len(ts) > 1 else np.ones(len(ts), bool)
                V._atomic_savez(funding_path(d, sym), ts=ts[keep].astype(np.int64), rate=rate[keep].astype(np.float64), months=np.array(months, dtype='U7'))
                res['loaded_now'] += 1
    outs = await asyncio.gather(*[one(k) for k in todo], return_exceptions=True)
    bad = [x for x in outs if isinstance(x, BaseException)]
    if bad:
        raise bad[0]
    if progress and todo:
        await progress(f'{sym} фандинг: загружено {res["loaded_now"]} из {len(todo)} месяцев, ошибок {len(res["failed"])}')
    return res


# ── дневной хвост свечей (текущий незавершённый месяц) ──────────────────────────────────────────────
def tail_path(d, sym, tf):
    return os.path.join(d, sym, f'klines_{tf}_tail.npz')


def load_tail(d, sym, tf):
    z = V._load_npz(tail_path(d, sym, tf))
    if z is None:
        return None
    out = {k: z[k].astype(np.float64) for k in ('o', 'h', 'l', 'c', 'v', 'tbv')}
    out['ts'] = z['ts']; out['days'] = [str(x) for x in z['days']]
    return out


def tail_days(months, now_ms):
    """Дни для дневной догрузки: с первого дня месяца после последнего загруженного ЗАВЕРШЁННОГО месяца до вчера (UTC). months — загруженные месяцы."""
    if not months:
        return []
    y, m = map(int, max(months).split('-'))
    y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    start = datetime(y, m, 1, tzinfo=timezone.utc)
    end = datetime.fromtimestamp(now_ms / 1000, timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    out, cur = [], start
    while cur < end:
        out.append(cur.strftime('%Y-%m-%d'))
        cur = datetime.fromtimestamp(cur.timestamp() + 86400, timezone.utc)
    return out


async def sync_tail(fetch, d, sym, tf, now_ms, progress=None):
    """Дневные файлы текущего месяца → klines_{tf}_tail.npz. Недоступные дни (404 и т. п.) пропускаются."""
    base = V.load_klines(d, sym, tf)
    if not base:
        return {'loaded_now': 0, 'failed': 0, 'skipped': 'нет месячных свечей'}
    cur = load_tail(d, sym, tf)
    have = set(cur['days']) if cur else set()
    todo = [x for x in tail_days(base['months'], now_ms) if x not in have]
    res = {'loaded_now': 0, 'failed': 0}
    for day in todo:
        V.check_space(d)
        key = f'data/futures/um/daily/klines/{sym}/{tf}/{sym}-{tf}-{day}.zip'
        try:
            body = await V.download_zip(fetch, key)
            p = V.parse_klines_csv(V.unzip_text(body))
        except V.VisionError as e:
            if e.kind == 'low_disk':
                raise
            res['failed'] += 1
            continue
        except (zipfile.BadZipFile, ValueError, KeyError):
            res['failed'] += 1
            continue
        cur = load_tail(d, sym, tf)
        cols = {k: p[k] for k in ('o', 'h', 'l', 'c', 'v', 'tbv')}
        if cur:
            ts = np.concatenate([cur['ts'], p['ts']]); cols = {k: np.concatenate([cur[k], cols[k]]) for k in cols}; days = sorted(set(cur['days']) | {day})
        else:
            ts, days = p['ts'], [day]
        o = np.argsort(ts, kind='stable'); ts = ts[o]
        keep = np.r_[True, np.diff(ts) != 0] if len(ts) > 1 else np.ones(len(ts), bool)
        V._atomic_savez(tail_path(d, sym, tf), ts=ts[keep].astype(np.int64), days=np.array(days, dtype='U10'),
                        **{k: v[o][keep].astype(np.float32) for k, v in cols.items()})
        res['loaded_now'] += 1
    if progress and todo:
        await progress(f'{sym} хвост {tf}: загружено {res["loaded_now"]} из {len(todo)} дней, недоступно {res["failed"]}')
    return res


def load_series(d, sym, tf='15m'):
    """Месячные свечи + дневной хвост (уникальные ts по возрастанию) → dict(ts,o,h,l,c,v,tbv) или None."""
    k = V.load_klines(d, sym, tf)
    if not k or len(k['ts']) < 300:
        return None
    t = load_tail(d, sym, tf)
    if t and len(t['ts']):
        ts = np.concatenate([k['ts'], t['ts']])
        cols = {x: np.concatenate([k[x], t[x]]) for x in ('o', 'h', 'l', 'c', 'v', 'tbv')}
        o = np.argsort(ts, kind='stable'); ts = ts[o]
        keep = np.r_[True, np.diff(ts) != 0] if len(ts) > 1 else np.ones(len(ts), bool)
        k = {'ts': ts[keep], **{x: cols[x][o][keep] for x in cols}}
    return k


# ── отчёт ────────────────────────────────────────────────────────────────────────────────────────────
def _fpf(x):
    return 'inf' if x == float('inf') else f'{x:.2f}'


def _row(label, s):
    if s['n'] == 0:
        return f'  {label}: n=0'
    return (f'  {label}: n={s["n"]} | WR {s["wr"]:.0f}% | PF net {_fpf(s["pf"])} | mean R net {s["mean"]:+.3f} (gross {s["mean_g"]:+.3f}) | '
            f't сделок {s["t"]:.2f} | дневной портфель: mean R {s["dmean"]:+.3f}, t {s["dt"]:.2f}, дней {s["days"]} | '
            f'SL {s["sl"]:.0f}% / TP {s["tp"]:.0f}% / таймаут {s["to"]:.0f}%')


def r0_verdict(trades):
    s = stats(trades)
    if s['n'] == 0:
        return 'NODATA', 'нет сделок — калибровка невозможна'
    if abs(s['t_g']) >= R0_STOP_T:
        return 'STOP', f'ОШИБКА ИЗМЕРЕНИЯ: |t| = {abs(s["t_g"]):.2f} ≥ {R0_STOP_T:g} при mean R до издержек {s["mean_g"]:+.4f}'
    if abs(s['t_g']) >= R0_WARN_T:
        return 'WARN', f'предупреждение: |t| = {abs(s["t_g"]):.2f} в [{R0_WARN_T:g}; {R0_STOP_T:g}) — граница, R1 запускается'
    return 'OK', f'измерительная система в норме: |t| = {abs(s["t_g"]):.2f} < {R0_WARN_T:g} при mean R до издержек {s["mean_g"]:+.4f}'


def build_r0(trades, diags, missing, fees):
    s = stats(trades)
    status, text = r0_verdict(trades)
    L = ['📈 <b>/edge_audit — R0: калибровка на случайных входах (BOT_SPEC §2.37.3)</b>',
         f'16 символов, 15m, по {N_RANDOM} входов на символ, SEED={SEED} (+ номер символа), направление 50/50; вход = close, SL 1.0×ATR14 [{SL_BOUNDS[0]}%; {SL_BOUNDS[1]}%], '
         f'TP {TP_R:g}R, таймаут {TIMEOUT_BARS} баров, SL раньше TP; издержки: FEE_MAKER={fees[0]} / FEE_TAKER={fees[1]}, проскальзывание {SLIP} на рыночных исполнениях.']
    if missing:
        L.append(f'⚠️ без данных: {", ".join(missing)}')
    L.append(f'Розыгрыш: допустимых баров {sum(d["valid"] for d in diags.values())}, перебрано {sum(d["tried"] for d in diags.values())}, '
             f'отсеяно по границам SL {sum(d["bounds"] for d in diags.values())}, не закрыто {sum(d["unclosed"] for d in diags.values())}')
    L.append(f' ВСЕ: n={s["n"]} | mean R ДО издержек {s["mean_g"]:+.4f} (t {s["t_g"]:.2f}) | ПОСЛЕ издержек {s["mean"]:+.4f} (t {s["t"]:.2f}) | '
             f'дневной портфель net: mean {s["dmean"]:+.4f}, t {s["dt"]:.2f} | SL {s["sl"]:.0f}% / TP {s["tp"]:.0f}% / таймаут {s["to"]:.0f}%')
    for name, sel in (('лонг', True), ('шорт', False)):
        x = stats([t for t in trades if t['long'] == sel])
        L.append(f'  {name}: n={x["n"]} | ДО {x["mean_g"]:+.4f} (t {x["t_g"]:.2f}) | ПОСЛЕ {x["mean"]:+.4f} (t {x["t"]:.2f}) | '
                 f'SL {x["sl"]:.0f}% / TP {x["tp"]:.0f}% / таймаут {x["to"]:.0f}%')
    yrs = {}
    for t in trades:
        yrs.setdefault(_year(t['entry_ts']), []).append(t)
    L.append('  по годам (ДО издержек): ' + '; '.join(f'{y}: n={len(v)} {stats(v)["mean_g"]:+.3f} (t {stats(v)["t_g"]:.1f})' for y, v in sorted(yrs.items())))
    L.append(f'<b>ВЕРДИКТ R0: {text}</b>')
    if status == 'STOP':
        L.append('R1 НЕ ЗАПУЩЕН. Исследования стоят до исправления измерения; искать причину в системе измерения (сегменты данных, ATR, семантика выхода, издержки).')
    return '\n'.join(L), status


def build_r1(cells, diag, btc_ok, fund_info, cmp_, gaps, fees, since_note=''):
    L = ['', '📈 <b>R1: SMC2 на истории 2020–2026 (BOT_SPEC §2.37.4–2.37.5)</b>',
         f'Логика — `_smc2_eval` бота, окно {WINDOW} закрытых баров, дедуп 2 ч, V0/V1/V2/F, MKT/RT; издержки BingX: maker {fees[0]}, taker {fees[1]}, проскальзывание {SLIP}; '
         f'IS до {OOS_START}, OOS с {OOS_START}. Критерии: OOS дневной mean R&gt;0 при t≥{CRIT_T:g}, PF net≥{CRIT_PF}, OOS≥{CRIT_RATIO}×IS, n≥{CRIT_N} и дней≥{CRIT_DAYS}, '
         f'mean R&gt;0 в месяцы роста И падения BTC (по всей истории). 16 ячеек, пороги не подбирались.',
         '⚠️ ОШИБКА ВЫЖИВШЕГО: 16 фиксированных символов, отобраны по объёму на 2026-10 — результат оптимистичен.',
         f'Разрывов в рядах (сегментов − 1, сумма по символам): {gaps}.' + (' ' + since_note if since_note else '')]
    if not btc_ok:
        L.append('⚠️ BTCUSDT без данных — условие по месяцам BTC не проверяемо, ячейки не могут быть пройдены.')
    L.append('Диагностика событий (сумма по символам): ' + '; '.join(
        f'{v}: событий {diag[v]["events"]}, дедуп {diag[v]["dedup"]}, MKT записан позже бара пробоя {diag[v]["late_mkt"]}' + (f', без фандинга {diag[v]["nofund"]}' if v == 'F' else '')
        for v in VARIANTS))
    L.append(fund_info)
    passed = []
    for var in VARIANTS:
        L.append(f'\n<b>Вариант {var}</b>' + {'V0': ' — объём на свече пробоя ≥1.5×медиана (как shadow SMC2)', 'V1': ' — без фильтра объёма',
                                              'V2': ' — объём ПРЕДЫДУЩЕЙ свечи ≥1.5×медиана (как старый SMC)',
                                              'F': ' — V0 + шорт при фандинге&gt;0 / лонг при &lt;0'}[var])
        for side in SIDES:
            for et in ENTRIES:
                c = cells[(var, side, et)]
                L.append(f' {side} {et}:')
                if c.get('unverifiable'):
                    L.append(f'   НЕПРОВЕРЯЕМ: {c["unverifiable"]}')
                    continue
                L.append(_row('IS', c['is']))
                L.append(_row('OOS', c['oos']))
                yr = c['years']
                L.append('   по годам входа (n, mean R net): ' + '; '.join(f'{y}: {n}, {m:+.3f}' for y, (n, m) in yr.items()))
                mo, mo2 = c['months'], c['months_oos']
                L.append(f'   месяцы BTC (вся история): рост n={mo["up"][0]} mean R {mo["up"][1]:+.3f} | падение n={mo["down"][0]} mean R {mo["down"][1]:+.3f}; '
                         f'только OOS (справочно): рост n={mo2["up"][0]} {mo2["up"][1]:+.3f} | падение n={mo2["down"][0]} {mo2["down"][1]:+.3f}')
                if c['checks']:
                    L.append('   критерии: ' + ' | '.join(f'{k} {"✅" if ok else "❌"}' for k, ok in c['checks'].items()))
                L.append(f'   <b>{var} {side} {et}: {c["text"]}</b>')
                if c['status'] == 'PASS':
                    passed.append(f'{var} {side} {et}')
    L.append('\n<b>Сверка с shadow (1.6, справочно, не критерий)</b>')
    if cmp_ and cmp_.get('ok'):
        fmt = lambda ms: datetime.fromtimestamp(ms / 1000, timezone.utc).strftime('%Y-%m-%d %H:%M')
        L.append(f'  период {fmt(cmp_["t0"])} … {fmt(cmp_["t1"])} UTC, 16 символов, V0 против записей SMC2_MKT (±1 бар): R1 событий {cmp_["r1"]} (Long {cmp_["r1_long"]}, Short {cmp_["r1_short"]}), '
                 f'shadow {cmp_["shadow"]} (Long {cmp_["sh_long"]}, Short {cmp_["sh_short"]}); доля событий R1, найденных в shadow: '
                 f'{cmp_["r1_in_sh"] / cmp_["r1"] * 100 if cmp_["r1"] else 0:.0f}%; доля записей shadow, найденных в R1: {cmp_["sh_in_r1"] / cmp_["shadow"] * 100 if cmp_["shadow"] else 0:.0f}%. '
                 f'Расхождения ожидаемы: свечи Binance против BingX, новостной фильтр и суточный потолок бота не воспроизводятся.')
    else:
        L.append(f'  не выполнена: {cmp_.get("why", "нет данных") if cmp_ else "нет данных"}')
    L.append(f'\n<b>ВЕРДИКТ R1: пройдено ячеек {len(passed)} из 16' + (f' ({", ".join(passed)})' if passed else '') + '</b>')
    L.append('<b>РЕШЕНИЕ: ' + (f'прошли {", ".join(passed)} → отдельный пре-коммит shadow-форварда этих ячеек (отдельный деплой), затем свой капитал на риске 0.5–1%'
                              if passed else 'ни одна ячейка не прошла — исследования на OHLCV/потоке ордеров закрыты окончательно (правило §2.32.8 без исключений)') + '</b>')
    return '\n'.join(L), passed


# ── оркестрация ──────────────────────────────────────────────────────────────────────────────────────
def binance_symbols(universe):
    return sorted(s.split('/')[0] + 'USDT' for s in universe)


async def run_edge_audit(fetch, db_path, universe, fee_maker, fee_taker, smc2_eval, get_pivots, progress=None, d=None, now_ms=None, sync=True):
    """Загрузка (с продолжением из кэша) → R0 → (если не STOP) R1 → отчёт. → (текст, итог dict)."""
    d = d or V.vision_dir()
    now_ms = now_ms or int(datetime.now(timezone.utc).timestamp() * 1000)
    fees = (fee_maker, fee_taker)
    syms = binance_symbols(universe)
    bingx_of = {s.split('/')[0] + 'USDT': s for s in universe}

    async def say(m):
        if progress:
            try:
                await progress(m)
            except Exception:
                pass
    stopped = ''
    if sync:
        for sym in syms:
            try:
                r = await V.sync_klines(fetch, d, sym, '15m', say)
                if r['failed']:
                    await say(f'{sym} 15m: не загружено {len(r["failed"])} файлов (пропущены)')
                await sync_tail(fetch, d, sym, '15m', now_ms, say)
            except V.VisionError as e:
                if e.kind == 'low_disk':
                    stopped = f'⛔ ЗАГРУЗКА ОСТАНОВЛЕНА: {e}'
                    break
                await say(f'{sym}: ошибка загрузки свечей — {str(e)[:150]}')
            except Exception as e:
                await say(f'{sym}: ошибка загрузки свечей — {type(e).__name__}: {str(e)[:150]}')
            try:
                await sync_funding(fetch, d, sym, say)
            except V.VisionError as e:
                if e.kind == 'low_disk':
                    stopped = f'⛔ ЗАГРУЗКА ОСТАНОВЛЕНА: {e}'
                    break
                await say(f'{sym}: фандинг не загружен — {str(e)[:150]}')
            except Exception as e:
                await say(f'{sym}: фандинг не загружен — {type(e).__name__}: {str(e)[:150]}')
    if stopped:
        await say(stopped)
    arrs, missing = {}, []
    for sym in syms:
        a = load_series(d, sym)
        if a is None:
            missing.append(sym)
        else:
            arrs[sym] = a
    # ── R0 ──
    r0_all, r0_diag = [], {}
    for i, sym in enumerate(syms):
        if sym not in arrs:
            continue
        tr, dg = await asyncio.to_thread(r0_trades, sym, i, arrs[sym], fees)
        r0_all.extend(tr); r0_diag[sym] = dg
        await asyncio.sleep(0)
    text0, status0 = build_r0(r0_all, r0_diag, missing + ([stopped] if stopped else []), fees)
    await say(f'R0: сделок {len(r0_all)}; ' + text0.splitlines()[-1].replace('<b>', '').replace('</b>', '')[:200])
    if status0 in ('STOP', 'NODATA'):
        return text0, {'r0': status0, 'passed': []}
    # ── R1 ──
    funding = {}
    for sym in syms:
        f = load_funding(d, sym)
        if f is not None and len(f['ts']):
            funding[sym] = (f['ts'], f['rate'])
    all_tr = {v: [] for v in VARIANTS}
    diag = {v: {'events': 0, 'nofund': 0, 'dedup': 0, 'late_mkt': 0} for v in VARIANTS}
    v0_events, gaps = {}, 0
    for sym in syms:
        if sym not in arrs:
            continue
        res, v0e, g = await asyncio.to_thread(r1_symbol, sym, arrs[sym], fees, smc2_eval, get_pivots, funding.get(sym))
        for v in VARIANTS:
            all_tr[v].extend(res[v][0])
            for k, x in res[v][1].items():
                diag[v][k] += x
        v0_events[sym] = v0e; gaps += g
        await say(f'R1 {sym}: сделок V0 {len(res["V0"][0])}, V1 {len(res["V1"][0])}')
    btc = btc_month_dirs(arrs.get('BTCUSDT'))
    # покрытие фандинга: доля событий V0 с известной ставкой
    n_v0_cand = diag['V0']['events'] + diag['V0']['dedup']       # кандидаты V0 до дедупа — та же база, что у счётчика nofund варианта F
    fund_syms = len(funding)
    cov = 1.0 - diag['F']['nofund'] / max(1, n_v0_cand)
    f_unver = ''
    if fund_syms == 0:
        f_unver = 'ставки фандинга из архива не загружены'
    elif cov < FUND_MIN_COV:
        f_unver = f'ставки покрыты для {cov * 100:.0f}% событий V0 (< {FUND_MIN_COV * 100:.0f}%)'
    fund_info = (f'Фандинг: ставки загружены по {fund_syms} из {len(syms)} символов; покрытие событий V0 {cov * 100:.0f}%' +
                 (f' → вариант F НЕПРОВЕРЯЕМ ({f_unver})' if f_unver else ' → вариант F проверяем'))
    cells = {}
    for var in VARIANTS:
        for side in SIDES:
            for et in ENTRIES:
                if var == 'F' and f_unver:
                    cells[(var, side, et)] = {'unverifiable': f_unver, 'status': 'UNVERIFIABLE'}
                    continue
                tr = [t for t in all_tr[var] if t['long'] == (side == 'Long') and t['etype'] == et]
                cv = cell_verdict(tr, btc)
                cv['years'] = by_year(tr)
                if not btc:
                    cv['status'] = 'FAIL'; cv['text'] = 'BTC без данных — условие по месяцам не проверяемо'
                cells[(var, side, et)] = cv
    data_end = max(int(a['ts'][-1]) + TF_MS for a in arrs.values()) if arrs else 0
    cmp_ = await asyncio.to_thread(compare_shadow, db_path, v0_events, bingx_of, data_end)
    text1, passed = build_r1(cells, diag, bool(btc), fund_info, cmp_, gaps, fees)
    return text0 + '\n' + text1, {'r0': status0, 'passed': passed}
