"""research_edge.py — /edge_audit: R0 (калибровка измерительной системы на случайных входах) и R1 (SMC2 на истории 2020–2026)
СТРОГО по BOT_SPEC §2.37 (пре-коммит записан ДО кода). Никаких дополнительных вариантов, порогов и фильтров.

Логика входов — РОВНО `_smc2_eval` бота: функция и `get_pivots` передаются аргументами (smc2_eval, get_pivots), копии кода нет (иначе
модуль импортировал бы async_bot — циклический импорт). Данные — архив data.binance.vision через research_vision (npz-кэш); месячные
файлы содержат только завершённые месяцы, текущий месяц добирается дневными файлами (хвост `klines_15m_tail.npz`); ставки фандинга —
месячные файлы fundingRate (`funding.npz`). Расчёт по символу целиком в памяти; в БД пишется только отчёт (htf_reports)."""
import asyncio
import bisect
import gc
import json
import math
import os
import sqlite3
import zipfile
from datetime import datetime, timezone

import numpy as np

import research_htf as H
import research_runner as R
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


def _empty_diag():
    return {k: {'events': 0, 'nofund': 0, 'dedup': 0, 'late_mkt': 0} for k in VARIANTS}


def r1_symbol_stream(sym, arr, fees, smc2_eval, get_pivots, funding, sink):
    """Расчёт R1 по символу БЕЗ накопления сделок: scan (V1 = без фильтра объёма — надмножество), затем варианты по одному; готовый список сделок варианта
    отдаётся в sink(variant, trades) и сразу освобождается. → (diag по вариантам, события V0 [(mode, ts_b)], число разрывов в ряду)."""
    ts, o, h, l, c, v = (arr[k] for k in ('ts', 'o', 'h', 'l', 'c', 'v'))
    diag = _empty_diag()
    v0_events = []
    segs = 0
    for a, b in segments(ts):
        segs += 1
        if b - a < WINDOW + TIMEOUT_BARS + 2:
            continue
        sts, so, sh, sl_, sc, sv = ts[a:b], o[a:b], h[a:b], l[a:b], c[a:b], v[a:b]
        ph, pl = get_pivots(sh, sl_, order=ORD)
        events = scan_segment(sts, so, sh, sl_, sc, sv, ph, pl, smc2_eval, vol_mult=0.0)
        for var in VARIANTS:
            tr, dg = variant_trades(sym, events, var, (sts, sh, sl_, sc, b - a), fees, funding)
            sink(var, tr)
            for k, x in dg.items():
                diag[var][k] += x
            del tr
        for rec in events.values():
            if rec['v0'] and rec['rec_e'] is not None:
                v0_events.append((rec['mode'], rec['ts_b']))
        del events, ph, pl
    return diag, v0_events, segs - 1 if segs else 0


def r1_symbol(sym, arr, fees, smc2_eval, get_pivots, funding=None):
    """Те же расчёты с накоплением в памяти (тесты и малые наборы). → {variant: (trades, diag)}, события V0, разрывы."""
    res = {k: [] for k in VARIANTS}

    def sink(var, tr):
        res[var].extend(tr)
    diag, v0, gaps = r1_symbol_stream(sym, arr, fees, smc2_eval, get_pivots, funding, sink)
    return {k: (res[k], diag[k]) for k in VARIANTS}, v0, gaps


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


def verdict_from_parts(s_is, s_oos, ms, ms_oos):
    """Критерии §2.37.5 по готовым агрегатам (одни и те же для расчёта в памяти и по SQL). → dict(status PASS|FAIL|NODATA, checks, text, is, oos, months, months_oos)."""
    res = {'is': s_is, 'oos': s_oos, 'months': ms, 'months_oos': ms_oos, 'checks': {}}
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


def cell_verdict(trades, btc):
    """Критерии §2.37.5 по списку сделок в памяти (тесты, малые наборы)."""
    is_t, oos_t = split_is_oos(trades)
    return verdict_from_parts(stats(is_t), stats(oos_t), month_split(trades, btc), month_split(oos_t, btc))


# ── хранилище R0/R1 (SQLite, отдельный файл edge.db) и агрегация SQL-запросами (v97) ─────────────────────────────
EDGE_R0_REV = '1'                # менять при любой правке кода измерения R0: иначе итог R0 берётся из сохранённого для этого seed
EDGE_MIN_FREE_MB = 200           # меньше — остановка до прогона
EST_ROWS_PER_SYMBOL = 30_000     # оценка по проду 2026-10-24: V0 4–8 тыс. + V1 10–14.5 тыс. + V2 ≈ V0 + F ≈ V0/2 на символ
EST_BYTES_PER_ROW = 140          # измерено на синтетике: edge.db 3.14 МБ на 22 445 строк (данные + индекс) ≈ 140 Б/строка
INSERT_CHUNK = 5000


def edge_db_path(db_path):
    return os.environ.get('EDGE_DB') or os.path.join(os.path.dirname(os.path.abspath(db_path)), 'edge.db')


def estimate_edge_mb(n_symbols):
    """Оценка размера edge_trades (R0 + R1), МБ."""
    return (n_symbols * (EST_ROWS_PER_SYMBOL + N_RANDOM)) * EST_BYTES_PER_ROW / 1e6


def init_edge_db(path):
    con = sqlite3.connect(path, isolation_level=None)
    con.execute("CREATE TABLE IF NOT EXISTS edge_runs (run_id TEXT PRIMARY KEY, kind TEXT, seed INTEGER, started TEXT, status TEXT, finished TEXT)")
    con.execute("CREATE TABLE IF NOT EXISTS edge_trades (run_id TEXT, symbol TEXT, variant TEXT, entry TEXT, direction TEXT, open_ts INTEGER, close_ts INTEGER, "
                "r_gross REAL, r_net REAL, exit_reason TEXT)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_edge_trades ON edge_trades (run_id, variant, entry, direction, open_ts)")
    con.execute("CREATE TABLE IF NOT EXISTS edge_progress (run_id TEXT, symbol TEXT, done INTEGER, diag TEXT, PRIMARY KEY (run_id, symbol))")
    con.execute("CREATE TABLE IF NOT EXISTS edge_v0_events (run_id TEXT, symbol TEXT, mode TEXT, ts_b INTEGER)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_edge_v0 ON edge_v0_events (run_id, ts_b)")
    con.close()


def insert_trades(con, run_id, sym, variant, trades):
    """Пачками по INSERT_CHUNK строк (внутри транзакции вызывающего)."""
    buf = []
    for t in trades:
        buf.append((run_id, sym, variant, t['etype'], 'Long' if t['long'] else 'Short', int(t['entry_ts']), int(t['exit_ts']), float(t['Rg']), float(t['R']), t['reason']))
        if len(buf) >= INSERT_CHUNK:
            con.executemany("INSERT INTO edge_trades VALUES (?,?,?,?,?,?,?,?,?,?)", buf)
            buf = []
    if buf:
        con.executemany("INSERT INTO edge_trades VALUES (?,?,?,?,?,?,?,?,?,?)", buf)


def _where(run_id, variant=None, entry=None, direction=None, ts_lo=None, ts_hi=None):
    cl, args = ['run_id=?'], [run_id]
    for col, val in (('variant', variant), ('entry', entry), ('direction', direction)):
        if val is not None:
            cl.append(f'{col}=?'); args.append(val)
    if ts_lo is not None:
        cl.append('open_ts>=?'); args.append(int(ts_lo))
    if ts_hi is not None:
        cl.append('open_ts<?'); args.append(int(ts_hi))
    return ' AND '.join(cl), args


def _t_from_sums(n, s1, s2):
    if n < 2:
        return 0.0
    m = s1 / n
    var = (s2 - n * m * m) / (n - 1)
    return m / math.sqrt(var / n) if var > 1e-15 else 0.0


def sql_stats(con, run_id, variant=None, entry=None, direction=None, ts_lo=None, ts_hi=None):
    """Те же показатели, что stats(список сделок), но агрегацией SQL (без загрузки сделок в память); дневной портфель — чтением по дням порциями."""
    w, args = _where(run_id, variant, entry, direction, ts_lo, ts_hi)
    r = con.execute(
        "SELECT COUNT(*), SUM(r_net), SUM(r_net*r_net), SUM(r_gross), SUM(r_gross*r_gross), "
        "SUM(CASE WHEN r_net>0 THEN r_net ELSE 0 END), SUM(CASE WHEN r_net<0 THEN -r_net ELSE 0 END), SUM(CASE WHEN r_net>0 THEN 1 ELSE 0 END), "
        "SUM(CASE WHEN exit_reason='SL' THEN 1 ELSE 0 END), SUM(CASE WHEN exit_reason='TP' THEN 1 ELSE 0 END), SUM(CASE WHEN exit_reason='TIMEOUT' THEN 1 ELSE 0 END) "
        f"FROM edge_trades WHERE {w}", args).fetchone()
    n = r[0] or 0
    if n == 0:
        return stats([])
    s1, s2, g1, g2, gw, gl, wins, sl, tp, to = (x or 0 for x in r[1:])
    nd = sd = sd2 = 0
    cur = con.execute(f"SELECT AVG(r_net) FROM edge_trades WHERE {w} GROUP BY close_ts/86400000", args)
    while True:
        rows = cur.fetchmany(2000)
        if not rows:
            break
        for (m,) in rows:
            nd += 1; sd += m; sd2 += m * m
    return {'n': n, 'wr': wins / n * 100, 'pf': (gw / gl) if gl > 0 else (float('inf') if gw > 0 else 0.0),
            'mean': s1 / n, 't': _t_from_sums(n, s1, s2), 'mean_g': g1 / n, 't_g': _t_from_sums(n, g1, g2),
            'dmean': (sd / nd) if nd else 0.0, 'dt': _t_from_sums(nd, sd, sd2), 'days': nd,
            'sl': sl / n * 100, 'tp': tp / n * 100, 'to': to / n * 100}


def sql_by_year(con, run_id, variant=None, entry=None, direction=None, gross=False):
    """{год входа: (n, mean R net)}; gross=True → {год: (n, mean R gross, t gross)}."""
    w, args = _where(run_id, variant, entry, direction)
    y = "CAST(strftime('%Y', open_ts/1000, 'unixepoch') AS INTEGER)"
    out = {}
    if gross:
        for yr, n, g1, g2 in con.execute(f"SELECT {y}, COUNT(*), SUM(r_gross), SUM(r_gross*r_gross) FROM edge_trades WHERE {w} GROUP BY 1 ORDER BY 1", args):
            out[yr] = (n, g1 / n, _t_from_sums(n, g1, g2))
    else:
        for yr, n, s1 in con.execute(f"SELECT {y}, COUNT(*), SUM(r_net) FROM edge_trades WHERE {w} GROUP BY 1 ORDER BY 1", args):
            out[yr] = (n, s1 / n)
    return out


def sql_month_split(con, run_id, variant, entry, direction, btc, ts_lo=None):
    """mean R net по входам в месяцы роста/падения BTC (как month_split), агрегацией по месяцам."""
    w, args = _where(run_id, variant, entry, direction, ts_lo)
    up = [0, 0.0]; dn = [0, 0.0]
    for m, n, s1 in con.execute(f"SELECT strftime('%Y-%m', open_ts/1000, 'unixepoch'), COUNT(*), SUM(r_net) FROM edge_trades WHERE {w} GROUP BY 1", args):
        r = btc.get(m)
        if r is None:
            continue
        tgt = up if r > 0 else dn
        tgt[0] += n; tgt[1] += s1
    f = lambda x: (x[0], (x[1] / x[0]) if x[0] else 0.0)
    return {'up': f(up), 'down': f(dn)}


def cell_verdict_sql(con, run_id, variant, entry, direction, btc):
    """Критерии §2.37.5 по SQL-агрегатам. Те же правила и числа, что cell_verdict(список сделок)."""
    cut = _ms(OOS_START)
    s_is = sql_stats(con, run_id, variant, entry, direction, ts_hi=cut)
    s_oos = sql_stats(con, run_id, variant, entry, direction, ts_lo=cut)
    res = verdict_from_parts(s_is, s_oos, sql_month_split(con, run_id, variant, entry, direction, btc),
                             sql_month_split(con, run_id, variant, entry, direction, btc, ts_lo=cut))
    res['years'] = sql_by_year(con, run_id, variant, entry, direction)
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


def load_series_lean(d, sym, tf='15m'):
    """[v97] Свечи символа для расчёта R0/R1 с минимумом памяти: время int64, o float32, h/l/c/v float64 (точный апкаст float32 из кэша — те же значения, что у
    load_series); колонка taker buy (tbv) не загружается. Месячные свечи + дневной хвост (уникальные ts по возрастанию) → dict(ts,o,h,l,c,v) или None."""
    z = V._load_npz(V.klines_path(d, sym, tf))
    if z is None or len(z['ts']) < 300:
        return None
    ts = z['ts']
    cols = {'o': z['o'], 'h': z['h'], 'l': z['l'], 'c': z['c'], 'v': z['v']}
    del z
    t = V._load_npz(tail_path(d, sym, tf))
    if t is not None and len(t['ts']):
        ts = np.concatenate([ts, t['ts']])
        cols = {k: np.concatenate([cols[k], t[k]]) for k in cols}
        o = np.argsort(ts, kind='stable'); ts = ts[o]
        keep = np.r_[True, np.diff(ts) != 0] if len(ts) > 1 else np.ones(len(ts), bool)
        ts = ts[keep]
        cols = {k: cols[k][o][keep] for k in cols}
    del t
    out = {'ts': ts.astype(np.int64, copy=False), 'o': cols['o'].astype(np.float32, copy=False)}
    for k in ('h', 'l', 'c', 'v'):
        out[k] = cols[k].astype(np.float64)
    return out


# ── отчёт ────────────────────────────────────────────────────────────────────────────────────────────
def _fpf(x):
    return 'inf' if x == float('inf') else f'{x:.2f}'


def _row(label, s):
    if s['n'] == 0:
        return f'  {label}: n=0'
    return (f'  {label}: n={s["n"]} | WR {s["wr"]:.0f}% | PF net {_fpf(s["pf"])} | mean R net {s["mean"]:+.3f} (gross {s["mean_g"]:+.3f}) | '
            f't сделок {s["t"]:.2f} | дневной портфель: mean R {s["dmean"]:+.3f}, t {s["dt"]:.2f}, дней {s["days"]} | '
            f'SL {s["sl"]:.0f}% / TP {s["tp"]:.0f}% / таймаут {s["to"]:.0f}%')


def summary_from_trades(trades):
    """Итог R0 по списку сделок в памяти: {'all','long','short': stats, 'years': {год: (n, mean R gross, t gross)}}."""
    yrs = {}
    for t in trades:
        yrs.setdefault(_year(t['entry_ts']), []).append(t['Rg'])
    return {'all': stats(trades), 'long': stats([t for t in trades if t['long']]), 'short': stats([t for t in trades if not t['long']]),
            'years': {y: (len(v), sum(v) / len(v), _tstat(v)) for y, v in sorted(yrs.items())}}


def r0_summary_sql(con, run_id):
    """Итог R0 SQL-агрегацией (та же структура, что summary_from_trades)."""
    return {'all': sql_stats(con, run_id, 'R0'), 'long': sql_stats(con, run_id, 'R0', direction='Long'),
            'short': sql_stats(con, run_id, 'R0', direction='Short'), 'years': sql_by_year(con, run_id, 'R0', gross=True)}


def r0_verdict(x):
    """Вердикт R0 по stats-словарю (SQL) или по списку сделок."""
    s = x if isinstance(x, dict) else stats(x)
    if s['n'] == 0:
        return 'NODATA', 'нет сделок — калибровка невозможна'
    if abs(s['t_g']) >= R0_STOP_T:
        return 'STOP', f'ОШИБКА ИЗМЕРЕНИЯ: |t| = {abs(s["t_g"]):.2f} ≥ {R0_STOP_T:g} при mean R до издержек {s["mean_g"]:+.4f}'
    if abs(s['t_g']) >= R0_WARN_T:
        return 'WARN', f'предупреждение: |t| = {abs(s["t_g"]):.2f} в [{R0_WARN_T:g}; {R0_STOP_T:g}) — граница, R1 запускается'
    return 'OK', f'измерительная система в норме: |t| = {abs(s["t_g"]):.2f} < {R0_WARN_T:g} при mean R до издержек {s["mean_g"]:+.4f}'


def build_r0(summary, diags, missing, fees):
    """Отчёт R0. summary — итог (SQL или summary_from_trades); при списке сделок — считается в памяти."""
    if isinstance(summary, list):
        summary = summary_from_trades(summary)
    s = summary['all']
    status, text = r0_verdict(s)
    L = ['📈 <b>/edge_audit — R0: калибровка на случайных входах (BOT_SPEC §2.37.3)</b>',
         f'16 символов, 15m, по {N_RANDOM} входов на символ, SEED={SEED} (+ номер символа), направление 50/50; вход = close, SL 1.0×ATR14 [{SL_BOUNDS[0]}%; {SL_BOUNDS[1]}%], '
         f'TP {TP_R:g}R, таймаут {TIMEOUT_BARS} баров, SL раньше TP; издержки: FEE_MAKER={fees[0]} / FEE_TAKER={fees[1]}, проскальзывание {SLIP} на рыночных исполнениях.']
    if missing:
        L.append(f'⚠️ без данных: {", ".join(missing)}')
    L.append(f'Розыгрыш: допустимых баров {sum(d["valid"] for d in diags.values())}, перебрано {sum(d["tried"] for d in diags.values())}, '
             f'отсеяно по границам SL {sum(d["bounds"] for d in diags.values())}, не закрыто {sum(d["unclosed"] for d in diags.values())}')
    L.append(f' ВСЕ: n={s["n"]} | mean R ДО издержек {s["mean_g"]:+.4f} (t {s["t_g"]:.2f}) | ПОСЛЕ издержек {s["mean"]:+.4f} (t {s["t"]:.2f}) | '
             f'дневной портфель net: mean {s["dmean"]:+.4f}, t {s["dt"]:.2f} | SL {s["sl"]:.0f}% / TP {s["tp"]:.0f}% / таймаут {s["to"]:.0f}%')
    for name, key in (('лонг', 'long'), ('шорт', 'short')):
        x = summary[key]
        L.append(f'  {name}: n={x["n"]} | ДО {x["mean_g"]:+.4f} (t {x["t_g"]:.2f}) | ПОСЛЕ {x["mean"]:+.4f} (t {x["t"]:.2f}) | '
                 f'SL {x["sl"]:.0f}% / TP {x["tp"]:.0f}% / таймаут {x["to"]:.0f}%')
    L.append('  по годам (ДО издержек): ' + '; '.join(f'{y}: n={n} {m:+.3f} (t {t:.1f})' for y, (n, m, t) in summary['years'].items()))
    L.append(f'<b>ВЕРДИКТ R0: {text}</b>')
    if status == 'STOP':
        L.append('R1 НЕ ЗАПУЩЕН. Исследования стоят до исправления измерения; искать причину в системе измерения (сегменты данных, ATR, семантика выхода, издержки).')
    return '\n'.join(L), status


def build_r1(cells, diag, btc_ok, fund_info, cmp_, gaps, fees, since_note='', missing=None):
    L = ['', '📈 <b>R1: SMC2 на истории 2020–2026 (BOT_SPEC §2.37.4–2.37.5)</b>',
         f'Логика — `_smc2_eval` бота, окно {WINDOW} закрытых баров, дедуп 2 ч, V0/V1/V2/F, MKT/RT; издержки BingX: maker {fees[0]}, taker {fees[1]}, проскальзывание {SLIP}; '
         f'IS до {OOS_START}, OOS с {OOS_START}. Критерии: OOS дневной mean R&gt;0 при t≥{CRIT_T:g}, PF net≥{CRIT_PF}, OOS≥{CRIT_RATIO}×IS, n≥{CRIT_N} и дней≥{CRIT_DAYS}, '
         f'mean R&gt;0 в месяцы роста И падения BTC (по всей истории). 16 ячеек, пороги не подбирались.',
         '⚠️ ОШИБКА ВЫЖИВШЕГО: 16 фиксированных символов, отобраны по объёму на 2026-10 — результат оптимистичен.',
         f'Разрывов в рядах (сегментов − 1, сумма по символам): {gaps}.' + (' ' + since_note if since_note else '')]
    if missing:
        L.append(f'⚠️ R1 без данных/не посчитано: {", ".join(missing)}')
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
def _connect(edb):
    return sqlite3.connect(edb, isolation_level=None, timeout=60)


def _tx(con, fn):
    """Транзакция одним куском: при любой ошибке откат (частичных строк символа не остаётся)."""
    con.execute('BEGIN IMMEDIATE')
    try:
        out = fn()
        con.execute('COMMIT')
        return out
    except BaseException:
        try:
            con.execute('ROLLBACK')
        except Exception:
            pass
        raise


def process_symbol_r0(edb, run_id, sym, idx, d, fees):
    """R0 одного символа: загрузка свечей → случайные входы → запись сделок и отметка done ОДНОЙ транзакцией → освобождение. → (n, diag) или None (нет данных)."""
    arr = load_series_lean(d, sym)
    if arr is None:
        return None
    box = {}
    box['tr'], dg = r0_trades(sym, idx, arr, fees)
    arr = None
    con = _connect(edb)
    try:
        def work():
            con.execute('DELETE FROM edge_trades WHERE run_id=? AND symbol=?', (run_id, sym))
            insert_trades(con, run_id, sym, 'R0', box['tr'])
            con.execute('INSERT OR REPLACE INTO edge_progress VALUES (?,?,1,?)', (run_id, sym, json.dumps(dg)))
        _tx(con, work)
    finally:
        con.close()
    n = len(box['tr'])
    box.clear()
    gc.collect()
    return n, dg


def process_symbol_r1(edb, run_id, sym, d, fees, smc2_eval, get_pivots, funding):
    """R1 одного символа: свечи (float32/int64 → рабочие массивы) → все варианты, сделки варианта сразу в SQLite пачками → освобождение. Сделки, события V0 и
    отметка done — в одной транзакции (прерывание не оставляет частичных данных символа). → (counts по вариантам, diag) или None (нет данных)."""
    arr = load_series_lean(d, sym)
    if arr is None:
        return None
    last_ts = int(arr['ts'][-1]) + TF_MS
    box = {'arr': arr}
    arr = None
    con = _connect(edb)
    counts = {k: 0 for k in VARIANTS}
    try:
        def work():
            con.execute('DELETE FROM edge_trades WHERE run_id=? AND symbol=?', (run_id, sym))
            con.execute('DELETE FROM edge_v0_events WHERE run_id=? AND symbol=?', (run_id, sym))

            def sink(var, tr):
                counts[var] += len(tr)
                insert_trades(con, run_id, sym, var, tr)
            diag, v0, gaps = r1_symbol_stream(sym, box['arr'], fees, smc2_eval, get_pivots, funding, sink)
            con.executemany('INSERT INTO edge_v0_events VALUES (?,?,?,?)', [(run_id, sym, m, int(tb)) for m, tb in v0])
            diag['_meta'] = {'gaps': gaps, 'last_ts': last_ts, 'has_fund': bool(funding is not None and len(funding[0])), 'counts': counts}
            con.execute('INSERT OR REPLACE INTO edge_progress VALUES (?,?,1,?)', (run_id, sym, json.dumps(diag)))
            return diag
        diag = _tx(con, work)
    finally:
        con.close()
    box.clear()
    gc.collect()
    return counts, diag


def _done_map(edb, run_id):
    con = _connect(edb)
    try:
        return {sym: json.loads(dg or '{}') for sym, dg in con.execute('SELECT symbol, diag FROM edge_progress WHERE run_id=? AND done=1', (run_id,))}
    finally:
        con.close()


def _ensure_run(edb, run_id, kind):
    con = _connect(edb)
    try:
        con.execute('INSERT OR IGNORE INTO edge_runs VALUES (?,?,?,?,?,NULL)', (run_id, kind, SEED, datetime.now(timezone.utc).isoformat(), 'running'))
    finally:
        con.close()


def _finish_run(edb, run_id):
    con = _connect(edb)
    try:
        con.execute("UPDATE edge_runs SET status='done', finished=? WHERE run_id=?", (datetime.now(timezone.utc).isoformat(), run_id))
    finally:
        con.close()


def _pick_r1_run(edb):
    """Незавершённый R1-прогон (прерванный) → продолжаем с первого незавершённого символа; иначе новый run_id."""
    con = _connect(edb)
    try:
        row = con.execute("SELECT run_id FROM edge_runs WHERE kind='r1' AND status!='done' ORDER BY started DESC LIMIT 1").fetchone()
    finally:
        con.close()
    if row:
        return row[0], True
    return 'r1-' + datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S'), False


def aggregate_r0(edb, run_id):
    con = _connect(edb)
    try:
        return r0_summary_sql(con, run_id)
    finally:
        con.close()


def aggregate_r1(edb, run_id, btc):
    """Все 16 ячеек SQL-агрегацией (чтение порциями, без загрузки сделок в память)."""
    con = _connect(edb)
    try:
        cells = {}
        for var in VARIANTS:
            for side in SIDES:
                for et in ENTRIES:
                    cells[(var, side, et)] = cell_verdict_sql(con, run_id, var, et, side, btc)
        return cells
    finally:
        con.close()


def compare_shadow_db(db_path, edb, run_id, bingx_of, data_end_ms, since='2026-10-02'):
    """Сверка с shadow (1.6): события V0 берутся из edge_v0_events только за окно сверки."""
    t0 = _ms(since)
    con = _connect(edb)
    try:
        ev = {}
        for sym, mode, tb in con.execute('SELECT symbol, mode, ts_b FROM edge_v0_events WHERE run_id=? AND ts_b>=?', (run_id, t0 - TF_MS)):
            ev.setdefault(sym, []).append((mode, tb))
    finally:
        con.close()
    return compare_shadow(db_path, ev, bingx_of, data_end_ms, since)


def binance_symbols(universe):
    return sorted(s.split('/')[0] + 'USDT' for s in universe)


async def run_edge_audit(fetch, db_path, universe, fee_maker, fee_taker, smc2_eval, get_pivots, progress=None, d=None, now_ms=None, sync=True, edge_db=None, inline=False):
    """Загрузка (с продолжением из кэша) → R0 → (если не STOP) R1 → отчёт. По символам, без накопления сделок в памяти: сделки — в SQLite (edge.db), контрольные точки —
    edge_progress; повторный запуск продолжает с первого незавершённого символа, R0 для текущего seed не пересчитывается. → (текст, итог dict)."""
    d = d or V.vision_dir()
    now_ms = now_ms or int(datetime.now(timezone.utc).timestamp() * 1000)
    fees = (fee_maker, fee_taker)
    syms = binance_symbols(universe)
    bingx_of = {s.split('/')[0] + 'USDT': s for s in universe}
    edb = edge_db or edge_db_path(db_path)

    async def call(fn, *args):
        """inline=True (дочерний процесс исследований): расчёт в том же потоке — без рабочего потока (его стек и арена malloc ≈ 70 МБ адресного пространства
        при лимите RLIMIT_AS); в процессе бота — в потоке, чтобы не блокировать цикл событий."""
        if inline:
            return fn(*args)
        return await asyncio.to_thread(fn, *args)

    async def say(m):
        if progress:
            try:
                await progress(m)
            except Exception:
                pass
    os.makedirs(os.path.dirname(os.path.abspath(edb)), exist_ok=True)
    free = V.free_mb(os.path.dirname(os.path.abspath(edb)))
    est = estimate_edge_mb(len(syms))
    if free < EDGE_MIN_FREE_MB:
        return (f'⛔ /edge_audit остановлен до прогона: свободно {free:.0f} МБ < {EDGE_MIN_FREE_MB} МБ (оценка размера edge_trades ≈ {est:.0f} МБ). Освободите место и повторите.',
                {'r0': 'DISK', 'passed': []})
    init_edge_db(edb)
    await say(f'edge.db: {edb}; свободно {free:.0f} МБ; оценка размера edge_trades ≈ {est:.0f} МБ ({EST_ROWS_PER_SYMBOL} строк × {len(syms)} символов R1 + R0, ≈{EST_BYTES_PER_ROW} Б/строка); '
              f'RSS {R.rss_mb():.0f} МБ')
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
    # ── R0 (по символам; итог сохраняется для seed + ревизии кода измерения) ──
    r0_id = f'r0-{SEED}-{EDGE_R0_REV}'
    _ensure_run(edb, r0_id, 'r0')
    done0 = _done_map(edb, r0_id)
    missing = []
    for i, sym in enumerate(syms):
        if sym in done0:
            continue
        out = await call(process_symbol_r0, edb, r0_id, sym, i, d, fees)
        if out is None:
            missing.append(sym)
            continue
        done0[sym] = out[1]
        await say(f'R0 {sym}: сделок {out[0]} | RSS {R.rss_mb():.0f} МБ')
        await asyncio.sleep(0)
    summ0 = await call(aggregate_r0, edb, r0_id)
    text0, status0 = build_r0(summ0, done0, missing + ([stopped] if stopped else []), fees)
    await say('R0: ' + text0.splitlines()[-1].replace('<b>', '').replace('</b>', '')[:200])
    if status0 in ('STOP', 'NODATA'):
        return text0, {'r0': status0, 'passed': []}
    # ── R1 ──
    run_id, resumed = _pick_r1_run(edb)
    _ensure_run(edb, run_id, 'r1')
    done1 = _done_map(edb, run_id)
    if resumed:
        await say(f'R1: продолжаю прогон {run_id} — готово символов {len(done1)} из {len(syms)}, продолжение с первого незавершённого')
    missing1 = []
    for sym in syms:
        if sym in done1:
            continue
        f = load_funding(d, sym)
        fund = (f['ts'], f['rate']) if f is not None and len(f['ts']) else None
        out = await call(process_symbol_r1, edb, run_id, sym, d, fees, smc2_eval, get_pivots, fund)
        f = fund = None
        if out is None:
            missing1.append(sym)
            continue
        counts, diag = out
        done1[sym] = diag
        await say(f'R1 {sym}: сделок V0 {counts["V0"]}, V1 {counts["V1"]}, V2 {counts["V2"]}, F {counts["F"]} | RSS {R.rss_mb():.0f} МБ')
        await asyncio.sleep(0)
    btc_arr = await call(load_series_lean, d, 'BTCUSDT')
    btc = btc_month_dirs(btc_arr)
    del btc_arr
    gc.collect()
    diag = _empty_diag()
    gaps, fund_syms, data_end = 0, 0, 0
    for sym, dg in done1.items():
        for v in VARIANTS:
            for k, x in (dg.get(v) or {}).items():
                diag[v][k] += x
        meta = dg.get('_meta') or {}
        gaps += meta.get('gaps', 0); fund_syms += 1 if meta.get('has_fund') else 0; data_end = max(data_end, meta.get('last_ts', 0))
    n_v0_cand = diag['V0']['events'] + diag['V0']['dedup']       # кандидаты V0 до дедупа — та же база, что у счётчика nofund варианта F
    cov = 1.0 - diag['F']['nofund'] / max(1, n_v0_cand)
    f_unver = ''
    if fund_syms == 0:
        f_unver = 'ставки фандинга из архива не загружены'
    elif cov < FUND_MIN_COV:
        f_unver = f'ставки покрыты для {cov * 100:.0f}% событий V0 (< {FUND_MIN_COV * 100:.0f}%)'
    fund_info = (f'Фандинг: ставки загружены по {fund_syms} из {len(syms)} символов; покрытие событий V0 {cov * 100:.0f}%' +
                 (f' → вариант F НЕПРОВЕРЯЕМ ({f_unver})' if f_unver else ' → вариант F проверяем'))
    cells_all = await call(aggregate_r1, edb, run_id, btc)
    cells = {}
    for key, cv in cells_all.items():
        if key[0] == 'F' and f_unver:
            cells[key] = {'unverifiable': f_unver, 'status': 'UNVERIFIABLE'}
        else:
            if not btc:
                cv['status'] = 'FAIL'; cv['text'] = 'BTC без данных — условие по месяцам не проверяемо'
            cells[key] = cv
    cmp_ = await call(compare_shadow_db, db_path, edb, run_id, bingx_of, data_end) if data_end else {'ok': False, 'why': 'нет данных'}
    text1, passed = build_r1(cells, diag, bool(btc), fund_info, cmp_, gaps, fees, missing=missing1)
    if not missing1:
        _finish_run(edb, run_id)
    return text0 + '\n' + text1, {'r0': status0, 'passed': passed, 'run_id': run_id}
