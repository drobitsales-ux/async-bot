"""research_fengine.py — движок «фабрики» (BOT_SPEC §2.41 + поправка §2.41.17): индикаторы, сигналы конфигураций библиотеки (T-MA, T-DC, T-MOM, R-RSI2, R-BB, B-VC, X-MOM) и новых
семейств (P-PAIR, L-LAG), симулятор сделок с защитным стопом 3·ATR20 и равным риском, издержки, статистика (Ньюи–Уэст, Бенджамини–Хохберг).

Модель исполнения: сигнал — по закрытому бару t, вход — по open t+1; стоп с учётом гэпа (бар открылся за стопом — выход по open); после стопа flat до следующего сигнала входа;
размер w = RISK / (3·ATR20/цена) фиксируется при входе, доходность — простая сумма (без реинвестирования): вклад бара = направление · w · (изменение цены)/цена входа.
Симулятор работает по СДЕЛКАМ (срезы numpy), а не по барам: стоимость ∝ числу сделок."""
import math
import os

import numpy as np
from numpy.lib.stride_tricks import sliding_window_view as swv

import research_ma_lib as L

DAY_MS = 86_400_000
RISK = L.RISK                              # 0.005
STOP_ATR = L.STOP_ATR                      # 3.0
ATR_N = L.ATR_N                            # 20
SLIP = 0.0003                              # проскальзывание CRYPTO на сторону
NW_LAG = L.NW_LAG
LOCK_FLAT = {'1h': 1, '4h': 4, '1D': 24}


def fee_taker():
    try:
        return float(os.getenv('FEE_TAKER', '0.0005'))
    except ValueError:
        return 0.0005


def fin_rate_yr():
    try:
        return float(os.getenv('FIN_RATE_YR', '0.06'))
    except ValueError:
        return 0.06


# спред Dukascopy (медиана активных часов, б.п., проба v2, BOT_SPEC §2.41.17); XAGUSD измеряется загрузкой (kv 'spread:XAGUSD')
SPREAD_BPS = {'XAUUSD': 1.76, 'BRENT': 6.02, 'WTI': 6.51, 'EURUSD': 0.26, 'GBPUSD': 0.57, 'USDJPY': 0.32, 'AUDUSD': 1.39, 'USDCHF': 0.98, 'USDCAD': 0.82, 'NZDUSD': 1.72,
              'SPX': 1.19, 'NDX': 1.04, 'DAX': 0.92, 'NIKKEI': 2.42, 'FTSE': 1.66, 'BUND': 2.00, 'TNOTE': 3.67}


class Cost:
    """Издержки одного инструмента: side — доля номинала на сторону (вход ИЛИ выход), swap_day — доля номинала за календарные сутки удержания, mult — стресс (×2)."""

    def __init__(self, side, swap_day=0.0, mult=1.0):
        self.side, self.swap_day, self.mult = side * mult, swap_day * mult, mult


def cost_crypto(mult=1.0):
    return Cost(fee_taker() + SLIP, 0.0, mult)


def cost_noncrypto(key, spreads, mult=1.0):
    return Cost(float(spreads[key]) * 1e-4, fin_rate_yr() / 365.0, mult)


# ── индикаторы ───────────────────────────────────────────────────────────────────────────────────────
def sma(x, n):
    out = np.full(len(x), np.nan)
    if len(x) >= n:
        cs = np.cumsum(np.r_[0.0, x])
        out[n - 1:] = (cs[n:] - cs[:-n]) / n
    return out


def rolling_std(x, n):
    """σ по n барам включая текущий, ddof=0 (через центрированные суммы для устойчивости)."""
    out = np.full(len(x), np.nan)
    if len(x) < n:
        return out
    w = swv(x, n)
    out[n - 1:] = w.std(axis=1)
    return out


def atr20(h, l, c):
    pc = np.r_[c[0], c[:-1]]
    tr = np.maximum(h - l, np.maximum(np.abs(h - pc), np.abs(l - pc)))
    tr[0] = h[0] - l[0]
    return sma(tr, ATR_N)


def prev_max(x, N):
    """max по N ПРЕДЫДУЩИМ барам (t−N … t−1); первые N значений — nan."""
    out = np.full(len(x), np.nan)
    if len(x) > N:
        out[N:] = swv(x, N).max(axis=1)[:-1]
    return out


def prev_min(x, N):
    out = np.full(len(x), np.nan)
    if len(x) > N:
        out[N:] = swv(x, N).min(axis=1)[:-1]
    return out


def rsi_wilder(c, n=2):
    """RSI по Уайлдеру: сглаживание (prev·(n−1)+x)/n, начальное среднее — по первым n изменениям."""
    out = np.full(len(c), np.nan)
    if len(c) <= n:
        return out
    d = np.diff(c)
    g = np.maximum(d, 0.0)
    ls = np.maximum(-d, 0.0)
    ag, al = float(g[:n].mean()), float(ls[:n].mean())
    out[n] = 100.0 if al == 0 else 100.0 - 100.0 / (1.0 + ag / al)
    for i in range(n, len(d)):
        ag = (ag * (n - 1) + g[i]) / n
        al = (al * (n - 1) + ls[i]) / n
        out[i + 1] = 100.0 if al == 0 else 100.0 - 100.0 / (1.0 + ag / al)
    return out


def make_series(key, bars, trade_from_ms, fund=None):
    """Ряд для движка: bars — dict ts,o,h,l,c,day (build_*), trade_from_ms — начало TRAIN. Добавляет atr, номер первого торгуемого бара и кэш индикаторов."""
    S = {k: np.asarray(bars[k]) for k in ('ts', 'o', 'h', 'l', 'c', 'day')}
    S['key'] = key
    S['n'] = len(S['c'])
    S['atr'] = atr20(S['h'], S['l'], S['c'])
    S['trade_from'] = int(np.searchsorted(S['ts'], trade_from_ms, side='left'))
    S['fund'] = fund
    S['_c'] = {}
    return S


def cached(S, name, fn):
    c = S['_c']
    if name not in c:
        c[name] = fn()
    return c[name]


def cut_series(S, end_idx):
    """Ряд до индекса end_idx (исключительно) — для калибровки на TRAIN; кэш индикаторов не копируется (индикаторы — по префиксу, пересчитываются)."""
    T = {k: S[k][:end_idx] for k in ('ts', 'o', 'h', 'l', 'c', 'day', 'atr')}
    T['key'], T['n'] = S['key'], end_idx
    T['trade_from'] = min(S['trade_from'], end_idx)
    T['fund'] = None if S['fund'] is None else S['fund'][:end_idx]
    T['_c'] = {}
    return T


# ── симулятор по сделкам ─────────────────────────────────────────────────────────────────────────────
def _empty_sig(n):
    return np.zeros(n, np.int8)


def simulate(S, sig, cost, exit_l=None, exit_s=None, reverse=False, hold=None, entry_px=None, trade_from=None, stop_mult=STOP_ATR):
    """Симуляция одного инструмента. sig[t] ∈ {−1,0,1} — сигнал входа по закрытию бара t (исполнение open t+1; entry_px[t] — цена входа стоп-ордером, nan = open t+1);
    exit_l/exit_s[t] — сигнал выхода из лонга/шорта по закрытию t (исполнение open t+1); reverse — противоположный сигнал при позиции = разворот; hold[t] = k > 0 — выход по
    close бара входа+k−1 (если стоп не раньше). Сделки не пересекаются: пока позиция открыта, новые сигналы игнорируются (кроме разворота).
    → dict(ret — вклад по барам (доля эквити, после издержек), day, trades — список (вход, последний бар, направление, w, вид выхода))."""
    n = S['n']
    o, h, l, c, ts, atr = S['o'], S['h'], S['l'], S['c'], S['ts'], S['atr']
    fund = S['fund']
    ret = np.zeros(n)
    t0 = S['trade_from'] if trade_from is None else trade_from
    ev = np.flatnonzero(sig != 0)
    ev = ev[(ev >= t0) & (ev < n - 1)]
    ev_long = ev[sig[ev] > 0]
    ev_short = ev[sig[ev] < 0]
    el = np.flatnonzero(exit_l) if exit_l is not None else np.zeros(0, np.int64)
    es = np.flatnonzero(exit_s) if exit_s is not None else np.zeros(0, np.int64)
    trades = []
    cursor = t0
    BIG = n + 10
    while True:
        p = int(np.searchsorted(ev, cursor))
        if p >= len(ev):
            break
        t = int(ev[p])
        d = int(sig[t])
        e = t + 1
        if e >= n:
            break
        px = float(entry_px[t]) if entry_px is not None and not np.isnan(entry_px[t]) else float(o[e])
        a = float(atr[t])
        if not (a > 0) or not (px > 0) or not math.isfinite(px):
            cursor = t + 1
            continue
        sd = stop_mult * a
        stop = px - d * sd
        w = RISK / (sd / px)
        ex = el if d > 0 else es
        k = int(np.searchsorted(ex, e))
        u1 = int(ex[k]) if k < len(ex) else BIG
        opp = ev_short if d > 0 else ev_long
        k2 = int(np.searchsorted(opp, e))
        u2 = int(opp[k2]) if (reverse and k2 < len(opp)) else BIG
        ux = min(u1, u2)
        hb = (e + int(hold[t]) - 1) if hold is not None and hold[t] > 0 else BIG
        end_bar = min(hb, ux, n - 1)
        seg = l[e:end_bar + 1] if d > 0 else h[e:end_bar + 1]
        hit = np.flatnonzero(seg <= stop) if d > 0 else np.flatnonzero(seg >= stop)
        if len(hit):
            j = e + int(hit[0])
            if j > e and ((d > 0 and o[j] <= stop) or (d < 0 and o[j] >= stop)):
                px_out = float(o[j])
            else:
                px_out = stop
            pts = np.concatenate(([px], c[e:j], [px_out]))
            kind, cursor_next = 'stop', j
        elif hb <= ux and hb <= n - 1:
            pts = np.concatenate(([px], c[e:hb + 1]))
            kind, cursor_next = 'close', hb
        elif ux + 1 <= n - 1:
            x = ux + 1
            pts = np.concatenate(([px], c[e:x], [o[x]]))
            kind = 'open'
            cursor_next = ux if (u2 <= u1 and u2 < BIG) else ux + 1
        else:                                                       # позиция дожила до конца данных — закрытие по последнему close
            pts = np.concatenate(([px], c[e:n]))
            kind, cursor_next = 'end', n
        m = len(pts) - 1
        last = e + m - 1
        ret[e:e + m] += (d * w / px) * np.diff(pts)
        ret[e] -= w * cost.side
        ret[last] -= w * cost.side
        if cost.swap_day and m > 1:
            ret[e + 1:last + 1] -= w * cost.swap_day * np.diff(ts[e:last + 1]) / DAY_MS
        if fund is not None:
            hi = last if kind == 'open' else last + 1
            ret[e:hi] -= d * w * fund[e:hi]
        trades.append((e, last, d, w, kind))
        cursor = cursor_next
        if kind == 'end':
            break
    return {'ret': ret, 'day': S['day'], 'trades': trades}


# ── сигналы конфигураций библиотеки (по одному инструменту) ──────────────────────────────────────────
def sig_t_ma(S, fast, slow):
    c = S['c']
    st = np.sign(cached(S, f'sma{fast}', lambda: sma(c, fast)) - cached(S, f'sma{slow}', lambda: sma(c, slow)))
    st = np.nan_to_num(st).astype(np.int8)
    sig = np.zeros(S['n'], np.int8)
    chg = np.r_[True, st[1:] != st[:-1]] & (st != 0)
    sig[chg] = st[chg]
    tf = S['trade_from']
    if tf < S['n'] and st[tf] != 0:
        sig[tf] = st[tf]                                           # начальное состояние на первом баре TRAIN — сигнал входа
    return {'sig': sig, 'reverse': True}


def sig_t_dc(S, N):
    c, h, l = S['c'], S['h'], S['l']
    hh = cached(S, f'pmax{N}', lambda: prev_max(h, N))
    ll = cached(S, f'pmin{N}', lambda: prev_min(l, N))
    sig = np.zeros(S['n'], np.int8)
    with np.errstate(invalid='ignore'):
        sig[c > hh] = 1
        sig[c < ll] = -1
        hx = cached(S, f'pmax{N // 2}', lambda: prev_max(h, N // 2))
        lx = cached(S, f'pmin{N // 2}', lambda: prev_min(l, N // 2))
        exit_l = c < lx
        exit_s = c > hx
    return {'sig': sig, 'exit_l': exit_l, 'exit_s': exit_s, 'reverse': True}


def sig_t_mom(S, lookback, rebalance=L.REBALANCE_BARS):
    c = S['c']
    n = S['n']
    sig = np.zeros(n, np.int8)
    idx = np.arange(n)
    ok = (idx % rebalance == 0) & (idx >= lookback)
    r = np.zeros(n)
    r[lookback:] = c[lookback:] / c[:-lookback] - 1.0
    sg = np.sign(r).astype(np.int8)
    sig[ok] = sg[ok]
    return {'sig': sig, 'reverse': True}


def sig_r_rsi2(S, lo, hi, exit_lvl, sma200_filter):
    c = S['c']
    rsi = cached(S, 'rsi2', lambda: rsi_wilder(c, 2))
    sig = np.zeros(S['n'], np.int8)
    with np.errstate(invalid='ignore'):
        long_ = rsi < lo
        short_ = rsi > hi
        if sma200_filter:
            m = cached(S, 'sma200', lambda: sma(c, 200))
            long_ &= c > m
            short_ &= c < m
        sig[long_] = 1
        sig[short_] = -1
        return {'sig': sig, 'exit_l': rsi >= exit_lvl, 'exit_s': rsi <= exit_lvl, 'reverse': False}


def sig_r_bb(S, z, win=20):
    c = S['c']
    m = cached(S, f'sma{win}', lambda: sma(c, win))
    sd = cached(S, f'std{win}', lambda: rolling_std(c, win))
    sig = np.zeros(S['n'], np.int8)
    with np.errstate(invalid='ignore', divide='ignore'):
        zz = (c - m) / sd
        sig[zz <= -z] = 1
        sig[zz >= z] = -1
        return {'sig': sig, 'exit_l': c >= m, 'exit_s': c <= m, 'reverse': False}


def sig_b_vc(S, nr=7, hold_after=5, oco=False):
    o, h, l = S['o'], S['h'], S['l']
    n = S['n']
    rng = h - l
    sig = np.zeros(n, np.int8)
    epx = np.full(n, np.nan)
    if n < nr + 2:
        return {'sig': sig, 'entry_px': epx, 'hold': np.zeros(n, np.int32), 'reverse': False}
    pm = np.full(n, np.nan)
    w = swv(rng, nr - 1).min(axis=1)                               # min диапазона по nr−1 ПРЕДЫДУЩИМ барам: окно i..i+nr−2 относится к t = i+nr−1
    pm[nr - 1:] = w[:n - nr + 1]
    nr7 = np.zeros(n, bool)
    with np.errstate(invalid='ignore'):
        nr7[nr - 1:] = rng[nr - 1:] <= pm[nr - 1:]
    t = np.arange(n - 1)
    lh = h[t + 1] >= h[t]
    sh = l[t + 1] <= l[t]
    if oco:
        # ДИАГНОСТИКА (не входит в N): без отбора по исходу бара t+1 — OCO-пара стоп-ордеров; если достигнуты оба уровня, первым считается ближайший к open уровень
        both = lh & sh
        first_long = np.abs(o[1:] - h[:-1]) <= np.abs(o[1:] - l[:-1])
        go_long = nr7[:-1] & ((lh & ~sh) | (both & first_long))
        go_short = nr7[:-1] & ((sh & ~lh) | (both & ~first_long))
        lh_use = go_long
        sig[:-1][go_long] = 1
        sig[:-1][go_short] = -1
        epx[:-1] = np.where(lh_use, np.maximum(o[1:], h[:-1]), np.minimum(o[1:], l[:-1]))
        hold = np.zeros(n, np.int32)
        hold[sig != 0] = hold_after + 1
        return {'sig': sig, 'entry_px': epx, 'hold': hold, 'reverse': False}
    valid = nr7[:-1] & (lh != sh)
    sig[:-1][valid & lh] = 1
    sig[:-1][valid & sh] = -1
    epx[:-1] = np.where(lh, np.maximum(o[1:], h[:-1]), np.minimum(o[1:], l[:-1]))
    hold = np.zeros(n, np.int32)
    hold[sig != 0] = hold_after + 1                                # выход по close бара входа + hold_after
    return {'sig': sig, 'entry_px': epx, 'hold': hold, 'reverse': False}


def library_signals(cfg, S):
    """Сигналы конфигурации cfg (L.Config, не X-MOM) для ряда S."""
    p = dict(cfg.params)
    f = cfg.family
    if f == 'T-MA':
        return sig_t_ma(S, p['fast'], p['slow'])
    if f == 'T-DC':
        return sig_t_dc(S, p['n'])
    if f == 'T-MOM':
        return sig_t_mom(S, p['lookback'], p['rebalance'])
    if f == 'R-RSI2':
        return sig_r_rsi2(S, p['lo'], p['hi'], p['exit'], p['sma200_filter'])
    if f == 'R-BB':
        return sig_r_bb(S, p['z'], p['sma'])
    if f == 'B-VC':
        return sig_b_vc(S, p['nr'], p['hold'])
    raise ValueError(f)


def grid_key(S, tf):
    """Ключ выравнивания баров разных инструментов: 1D — метка дня, иначе — время бара."""
    return S['day'] if tf == '1D' else S['ts']


def xmom_signals(series, tf, lookback, rebalance=L.REBALANCE_BARS, min_valid=L.XMOM_MIN_INSTRUMENTS):
    """Кросс-секционный моментум класса: на узлах сетки (номер кратен rebalance от начала объединённой сетки) ранжирование по close[t]/close[t−L] − 1; лонг — верхние ⌊n/3⌋,
    шорт — нижние ⌊n/3⌋; остальные — выход. → {ключ: dict(sig, exit_l, exit_s, reverse)}. Узел пропускается, если инструментов с валидным моментумом < min_valid."""
    keys = [grid_key(S, tf) for S in series]
    grid = np.unique(np.concatenate(keys))
    out = {S['key']: {'sig': np.zeros(S['n'], np.int8), 'exit_l': np.zeros(S['n'], bool), 'exit_s': np.zeros(S['n'], bool), 'reverse': True} for S in series}
    mom = []
    for S in series:
        m = np.full(S['n'], np.nan)
        if S['n'] > lookback:
            m[lookback:] = S['c'][lookback:] / S['c'][:-lookback] - 1.0
        mom.append(m)
    pos = [np.searchsorted(k, grid) for k in keys]               # позиция узла в ряде инструмента (или соседний бар)
    for gi in range(0, len(grid), rebalance):
        vals = []
        for j, S in enumerate(series):
            i = int(pos[j][gi])
            if i < S['n'] and keys[j][i] == grid[gi] and math.isfinite(mom[j][i]):
                vals.append((mom[j][i], j, i))
        if len(vals) < min_valid:
            continue
        vals.sort(key=lambda x: (x[0], x[1]))
        k = len(vals) // 3
        low = {x[1] for x in vals[:k]}
        high = {x[1] for x in vals[len(vals) - k:]}
        for _m, j, i in vals:
            o_ = out[series[j]['key']]
            if j in high:
                o_['sig'][i] = 1
            elif j in low:
                o_['sig'][i] = -1
            o_['exit_l'][i] = j not in high
            o_['exit_s'][i] = j not in low
    return out


# ── новые семейства ──────────────────────────────────────────────────────────────────────────────────
def sig_l_lag(S_alt, S_btc, H, win=100):
    """L-LAG: на баре t (общем для альта и BTC) r_BTC > 1σ и r_alt < r_BTC − 1σ → лонг альта; зеркально — шорт; σ — std r_BTC за win баров (включая t, ddof=0).
    Удержание H баров: вход open t+1, выход по close бара t+H (hold = H)."""
    cb, ca = S_btc['c'], S_alt['c']
    rb = np.r_[np.nan, cb[1:] / cb[:-1] - 1.0]
    sd = rolling_std(np.nan_to_num(rb), win)
    sd[:win] = np.nan                                              # первые win значений rb включают фиктивный nan→0
    ra = np.r_[np.nan, ca[1:] / ca[:-1] - 1.0]
    ib = np.searchsorted(S_btc['ts'], S_alt['ts'])
    ib_c = np.minimum(ib, len(cb) - 1)
    ok = S_btc['ts'][ib_c] == S_alt['ts']
    rbt, sdt = rb[ib_c], sd[ib_c]
    n = S_alt['n']
    sig = np.zeros(n, np.int8)
    with np.errstate(invalid='ignore'):
        up = ok & (rbt > sdt) & (ra < rbt - sdt)
        dn = ok & (rbt < -sdt) & (ra > rbt + sdt)
    sig[up] = 1
    sig[dn] = -1
    hold = np.zeros(n, np.int32)
    hold[sig != 0] = H
    return {'sig': sig, 'hold': hold, 'reverse': False}


def simulate_pair(SA, SB, Lb, cost_a, cost_b, trade_from_ms, z_in=2.0, z_stop=4.0):
    """P-PAIR одной пары на общих барах: z = (ln(A/B) − SMA_L)/σ_L; вход при |z| > z_in (шорт убежавшей, лонг отставшей, равный номинал w = RISK/(2σ_L), ориентация A<B),
    исполнение open t+1; выход — z пересекает 0, стоп |z| > z_stop (исполнение open следующего бара) или 2L баров после бара входа (open). Издержки — обе ноги, вход и выход.
    → dict(ret, day, trades)."""
    ia = np.searchsorted(SB['ts'], SA['ts'])
    ia_c = np.minimum(ia, SB['n'] - 1)
    ok = SB['ts'][ia_c] == SA['ts']
    A_idx = np.flatnonzero(ok)
    B_idx = ia_c[ok]
    n = len(A_idx)
    ts = SA['ts'][A_idx]
    day = SA['day'][A_idx]
    o_a, c_a = SA['o'][A_idx], SA['c'][A_idx]
    o_b, c_b = SB['o'][B_idx], SB['c'][B_idx]
    ret = np.zeros(n)
    trades = []
    if n < Lb + 10:
        return {'ret': ret, 'day': day, 'trades': trades}
    x = np.log(c_a / c_b)
    m = sma(x, Lb)
    sd = rolling_std(x, Lb)
    with np.errstate(invalid='ignore', divide='ignore'):
        z = (x - m) / sd
    t0 = int(np.searchsorted(ts, trade_from_ms, side='left'))
    fa = SA['fund'][A_idx] if SA['fund'] is not None else None
    fb = SB['fund'][B_idx] if SB['fund'] is not None else None
    absz = np.abs(z)
    ent = np.flatnonzero(absz > z_in)
    ent = ent[(ent >= t0) & (ent < n - 1)]
    cursor = t0
    while True:
        p = int(np.searchsorted(ent, cursor))
        if p >= len(ent):
            break
        t = int(ent[p])
        e = t + 1
        zs = 1 if z[t] > 0 else -1                                 # z>0: A «убежала» — шорт A, лонг B
        s = sd[t]
        if not (s > 0):
            cursor = t + 1
            continue
        w = RISK / (2.0 * s)
        da, db = -zs, zs
        seg = z[e:]
        cond = (seg <= 0) if zs > 0 else (seg >= 0)
        cond = cond | (np.abs(seg) > z_stop)
        hit = np.flatnonzero(cond)
        u = e + int(hit[0]) if len(hit) else n + 5
        x_exec = min(u + 1, e + 2 * Lb)
        forced = False
        if x_exec > n - 1:
            x_exec = n - 1
            forced = True
        pts_a = np.concatenate(([o_a[e]], c_a[e:x_exec], [o_a[x_exec]]))
        pts_b = np.concatenate(([o_b[e]], c_b[e:x_exec], [o_b[x_exec]]))
        if forced:
            pts_a[-1], pts_b[-1] = c_a[x_exec], c_b[x_exec]        # данные кончились — закрытие по close последнего бара
        mlen = len(pts_a) - 1
        last = e + mlen - 1
        ret[e:e + mlen] += w * (da * np.diff(pts_a) / o_a[e] + db * np.diff(pts_b) / o_b[e])
        ret[e] -= w * (cost_a.side + cost_b.side)
        ret[last] -= w * (cost_a.side + cost_b.side)
        if cost_a.swap_day and mlen > 1:
            ret[e + 1:last + 1] -= w * (cost_a.swap_day + cost_b.swap_day) * np.diff(ts[e:last + 1]) / DAY_MS
        hi = last if not forced else last + 1
        if fa is not None:
            ret[e:hi] -= da * w * fa[e:hi]
        if fb is not None:
            ret[e:hi] -= db * w * fb[e:hi]
        trades.append((e, last, zs, w, 'open'))
        cursor = x_exec if not forced else n
        if forced:
            break
    return {'ret': ret, 'day': day, 'trades': trades}


# ── портфель и статистика ────────────────────────────────────────────────────────────────────────────
def portfolio_daily(parts):
    """Сумма вкладов по дням: parts — список dict(day, ret) (по инструментам/парам). → (дни int64 по возрастанию, доходность по дням)."""
    if not parts:
        return np.zeros(0, np.int64), np.zeros(0)
    days = np.concatenate([p['day'] for p in parts]).astype(np.int64)
    r = np.concatenate([p['ret'] for p in parts])
    u, inv = np.unique(days, return_inverse=True)
    return u, np.bincount(inv, weights=r, minlength=len(u))


def day_bounds(name):
    lo, hi = L.period_bounds_ms(name)
    return lo // DAY_MS, (None if hi is None else hi // DAY_MS)


def period_slice(days, r, name):
    lo, hi = day_bounds(name)
    m = days >= lo
    if hi is not None:
        m &= days < hi
    return days[m], r[m]


def nw_t(x, lag=NW_LAG):
    """t-статистика среднего с поправкой Ньюи–Уэста (Бартлетт). → (mean, t | nan, n)."""
    x = np.asarray(x, float)
    n = len(x)
    if n < 30:
        return (float(x.mean()) if n else 0.0), float('nan'), n
    m = float(x.mean())
    e = x - m
    s = float(e @ e) / n
    for k in range(1, lag + 1):
        s += 2.0 * (1.0 - k / (lag + 1.0)) * float(e[k:] @ e[:-k]) / n
    if not (s > 0):
        return m, float('nan'), n
    return m, m / math.sqrt(s / n), n


def p_one_sided(t):
    """p = 1 − Φ(t) (H1: mean > 0); нет t — 1.0."""
    if t is None or not math.isfinite(t):
        return 1.0
    return 0.5 * math.erfc(t / math.sqrt(2.0))


def bh_reject(pvals, q=L.FDR_Q):
    """Бенджамини–Хохберг: булев массив «отвергнуто» (прошло по FDR q)."""
    p = np.asarray(pvals, float)
    m = len(p)
    rej = np.zeros(m, bool)
    if not m:
        return rej
    order = np.argsort(p, kind='stable')
    ok = p[order] <= q * (np.arange(1, m + 1) / m)
    if ok.any():
        k = int(np.flatnonzero(ok)[-1])
        rej[order[:k + 1]] = True
    return rej


def max_drawdown(r):
    """Максимальная просадка накопленной суммы (от пика до дна), доля эквити."""
    if not len(r):
        return 0.0
    cs = np.cumsum(r)
    peak = np.maximum.accumulate(np.r_[0.0, cs])[1:]
    return float(np.max(peak - cs))


def sharpe(r, periods):
    r = np.asarray(r, float)
    if len(r) < 2 or r.std() == 0:
        return float('nan')
    return float(r.mean() / r.std() * math.sqrt(periods))
