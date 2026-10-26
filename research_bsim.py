"""research_bsim.py — симуляторы гипотез серии «батарея» (BOT_SPEC §2.43): H1 FUND-CONTRA, H2 NEW-LIST, H3 FUND-TIME, H5 GRID; синтетические ряды (случайное блуждание, мартингал по
цене) для проверки смещения; статистика (t по меньшему из лагов 5 и 20, BH). Доходности — доли капитала, простая сумма без реинвестирования, по календарным суткам UTC.
Модель издержек: комиссия биржи FEE_TAKER/FEE_MAKER (параметры) + проскальзывание на сторону (0.0003 топ-30 по обороту на дату, иначе 0.0005); стресс ×2 — множитель mult
(фандинг не удваивается)."""
import math
from statistics import NormalDist

import numpy as np

import research_bdata as B
import research_fengine as E

RISK = 0.005                       # риск на позицию (H1, H2): доля капитала
STOP_ATR = 3.0                     # H1
H1_HOLD = 3                        # дней между пересборками
H1_N_SIDE = 5
H2_HOLD = 30
H2_STOP = 0.5                      # +50% от входа
H2_RISK_CAP = 0.10                 # суммарный риск открытых позиций ≤ 10% капитала
GRID_LEVELS = 5                    # уровней с каждой стороны (всего 10)
GRID_STOP_MULT = 1.5               # выход за диапазон на a/2: P·(1 ± 1.5a)
GRID_WEEK_H = 168
HXS_N = 16                         # равный капитал на символ
CELLS = [('H1', 'L3'), ('H1', 'L7'), ('H2', 'D3'), ('H2', 'D7'), ('H3', 'before'), ('H3', 'after'), ('H5', 'm1.5'), ('H5', 'm3')]
CELL_PARAM = {'L3': 3, 'L7': 7, 'D3': 3, 'D7': 7, 'm1.5': 1.5, 'm3': 3.0}
N_PLANNED = len(CELLS)
BH_Q = 0.05
MAX_S_MIN = 2.0                    # шаг 0: S_min > 2.0 → «непроверяема»


def cid(h, v):
    return f'{h}-{v}'


# ── статистика ──────────────────────────────────────────────────────────────────────────────────────
def tstat(x):
    """t по меньшему из двух t Ньюи–Уэста (лаг 5 и лаг 20), U5 §2.43. NaN, если нет."""
    x = np.asarray(x, float)
    t5 = E.nw_t(x, 5)[1]
    t20 = E.nw_t(x, 20)[1]
    c = [t for t in (t5, t20) if math.isfinite(t)]
    return min(c) if c else float('nan')


def t_period(days, r, name):
    d, x = E.period_slice(days, r, name)
    return tstat(x), (float(x.mean()) if len(x) else 0.0), len(x)


def t_bh(n, q=BH_Q):
    """Наименьший порог BH: односторонний t при p = q/N."""
    return NormalDist().inv_cdf(1.0 - q / max(1, n))


def s_min(n, d_days, q=BH_Q):
    """Шаг 0 (§2.43.4): минимальный обнаружимый годовой Шарп при t = порог BH, D — число дней покрытия на TRAIN."""
    if d_days <= 0:
        return float('inf')
    return t_bh(n, q) * math.sqrt(365.0 / d_days)


# ── подготовка контекстов ───────────────────────────────────────────────────────────────────────────
def make_ctx(P):
    """Контекст дневной панели: PIT-ликвидность, топ-30, ATR20 (все — на дату, без заглядывания вперёд)."""
    adv, cnt = B.pit_adv(P['QV'])
    return {'P': P, 'adv': adv, 'liq': B.liquid_mask(adv), 'top': B.top_n_mask(adv), 'atr': B.atr_panel(P['H'], P['L'], P['C'])}


def with_prices(ctx, P2):
    """Контекст с другими ценами (синтетика), ликвидность/топ-30 — реальные (оборот не меняется)."""
    return {'P': P2, 'adv': ctx['adv'], 'liq': ctx['liq'], 'top': ctx['top'], 'atr': B.atr_panel(P2['H'], P2['L'], P2['C'])}


def _result(P, ret, **meta):
    return {'days': np.arange(P['day0'], P['day0'] + P['nd'], dtype=np.int64), 'r': ret, 'meta': meta}


# ── H1 FUND-CONTRA ─────────────────────────────────────────────────────────────────────────────────
def h1_signal(ctx, d, L_days):
    """Рейтинг на 00:00 дня d: средний фандинг за L суток строго до 00:00 (сумма G[d−L…d−1]/L). → (avg[ns], допустимые[ns] bool). Допуск: ликвидность на дату, есть ATR и open,
    есть расчёты фандинга в окне."""
    P = ctx['P']
    lo = d - L_days
    if lo < 0:
        return None, None
    avg = P['G'][lo:d].astype(np.float64).sum(0) / L_days
    cnt = P['GC'][lo:d].sum(0)
    ok = ctx['liq'][d] & np.isfinite(ctx['atr'][d]) & np.isfinite(P['O'][d]) & (cnt > 0)
    return avg, ok


def h1_select(avg, ok, rng=None, rand=False):
    """5 шортов (наибольший положительный средний фандинг) и 5 лонгов (наименьший отрицательный). Случайный вариант — 5+5 из допустимых без учёта фандинга."""
    idx = np.flatnonzero(ok)
    if rand:
        if len(idx) < 2:
            return [], []
        k = min(H1_N_SIDE, len(idx) // 2)
        pick = rng.choice(idx, size=2 * k, replace=False)
        return list(pick[:k]), list(pick[k:])
    sh = idx[avg[idx] > 0]
    lg = idx[avg[idx] < 0]
    sh = sh[np.argsort(-avg[sh], kind='stable')][:H1_N_SIDE]
    lg = lg[np.argsort(avg[lg], kind='stable')][:H1_N_SIDE]
    return list(sh), list(lg)


def sim_h1(ctx, L_days, fees, mult=1.0, fund_on=True, rand=False, rng=None, costs_on=True):
    """H1: единый портфель, пересборка каждые 3 суток (дни с (epoch_day % 3 == 0)), вход по open 00:00, выход по open через 3 суток либо по стопу 3·ATR20 (гэп — по open); вес
    w = RISK/(3·ATR20/цена); фандинг по начислениям внутри удержания (на день стопа — не начисляется). → dict(days, r, meta)."""
    P = ctx['P']
    nd = P['nd']
    O, H, Lo, C, F = P['O'], P['H'], P['L'], P['C'], P['F']
    atr, top = ctx['atr'], ctx['top']
    ret = np.zeros(nd)
    fee = fees['taker'] * mult if costs_on else 0.0
    meta = {'rebalances': 0, 'positions': 0, 'stops': 0, 'delist': 0, 'max_w': 0.0, 'days_active': 0}
    start = max(H1_HOLD, L_days + 1)
    active = np.zeros(nd, bool)
    for d in range(start, nd - H1_HOLD):
        if (P['day0'] + d) % H1_HOLD:
            continue
        avg, ok = h1_signal(ctx, d, L_days)
        if avg is None:
            continue
        sh, lg = h1_select(avg, ok, rng, rand)
        if not sh and not lg:
            continue
        meta['rebalances'] += 1
        active[d:d + H1_HOLD] = True
        for side, group in ((-1, sh), (1, lg)):
            for s in group:
                _h1_position(P, ret, d, int(s), side, atr, top, fee, mult if costs_on else 0.0, fund_on, F, O, H, Lo, C, meta)
    meta['days_active'] = int(active.sum())
    return _result(P, ret, **meta)


def _h1_position(P, ret, d, s, side, atr, top, fee, mult, fund_on, F, O, H, Lo, C, meta):
    entry = float(O[d, s])
    a = float(atr[d, s])
    w = RISK / (STOP_ATR * a / entry)
    meta['positions'] += 1
    meta['max_w'] = max(meta['max_w'], w)
    slip = (0.0003 if top[d, s] else 0.0005) * mult
    side_cost = w * (fee + slip)
    ret[d] -= side_cost
    stop = entry - side * STOP_ATR * a
    ref = entry
    for k in range(d, d + H1_HOLD + 1):
        o = float(O[k, s])
        if k == d + H1_HOLD:
            x = o if math.isfinite(o) else ref
            ret[k] += side * w * (x - ref) / entry - side_cost
            return
        c = float(C[k, s])
        if not math.isfinite(c):                                     # бара нет (делистинг/разрыв): выход по последнему close
            ret[k] -= side_cost
            meta['delist'] += 1
            return
        h, lo = float(H[k, s]), float(Lo[k, s])
        gap = (o <= stop) if side > 0 else (o >= stop)
        hit = (lo <= stop) if side > 0 else (h >= stop)
        if k > d and gap:
            x = o
        elif hit:
            x = stop
        else:
            ret[k] += side * w * (c - ref) / entry
            if fund_on:
                ret[k] -= side * w * float(F[k, s])
            ref = c
            continue
        ret[k] += side * w * (x - ref) / entry - side_cost
        meta['stops'] += 1
        return


# ── H2 NEW-LIST ────────────────────────────────────────────────────────────────────────────────────
def h2_events(ctx, D):
    """Реальные события: листинг = первый дневной бар (first[s]); вход — close бара first+D; допуск — ликвидность на дату входа (PIT, дни до входа). → список (e, s), по (e, имя)."""
    P = ctx['P']
    ev = []
    for s in range(len(P['syms'])):
        if int(P['first'][s]) <= 0:                                  # первый бар в первый день архива — символ существовал ранее, не листинг (§2.43.8)
            continue
        e = int(P['first'][s]) + D
        if e > int(P['last'][s]) or not math.isfinite(float(P['C'][e, s])):
            continue
        if not bool(ctx['liq'][e, s]):
            continue
        ev.append((e, s))
    ev.sort(key=lambda x: (x[0], P['syms'][x[1]]))
    return ev


def h2_random_events(ctx, n, rng, lo=0, hi=None):
    """Случайные события (символ, день) из ликвидных ячеек панели в диапазоне дней [lo, hi) — для калибровки (§2.43.4 п. 2)."""
    P = ctx['P']
    hi = P['nd'] if hi is None else hi
    ok = ctx['liq'].copy()
    ok &= np.isfinite(P['C'])
    ok[:lo] = False
    ok[hi:] = False
    flat = np.flatnonzero(ok.ravel())
    if not len(flat):
        return []
    pick = rng.choice(flat, size=min(n, len(flat)), replace=False)
    ns = ok.shape[1]
    ev = [(int(p // ns), int(p % ns)) for p in pick]
    ev.sort(key=lambda x: (x[0], P['syms'][x[1]]))
    return ev


def sim_h2(ctx, events, fees, mult=1.0, fund_on=True, costs_on=True):
    """H2: шорт по close входа, стоп +50% (гэп — по open), выход close через 30 суток либо по close последнего бара (делистинг); w = RISK/0.5; не более 20 открытых позиций
    (риск ≤ 10% капитала; лишние пропускаются). Дневная разметка по close, фандинг: шорт при положительной ставке получает (дни входа+1…выход, на день стопа не начисляется)."""
    P = ctx['P']
    nd = P['nd']
    C, H, O, F = P['C'], P['H'], P['O'], P['F']
    top = ctx['top']
    ret = np.zeros(nd)
    fee = fees['taker'] * mult if costs_on else 0.0
    w = RISK / H2_STOP
    cap = int(round(H2_RISK_CAP / RISK))
    open_exits = []
    meta = {'events': len(events), 'opened': 0, 'skipped_cap': 0, 'incomplete': 0, 'stops': 0, 'delist': 0, 'days_open': 0}
    covered = np.zeros(nd, bool)
    for e, s in events:
        open_exits = [x for x in open_exits if x > e]
        if len(open_exits) >= cap:
            meta['skipped_cap'] += 1
            continue
        last = int(P['last'][s])
        if e + H2_HOLD > nd - 1 and last >= nd - 1:
            meta['incomplete'] += 1
            continue
        entry = float(C[e, s])
        slip = (0.0003 if top[e, s] else 0.0005) * (mult if costs_on else 0.0)
        side_cost = w * (fee + slip)
        ret[e] -= side_cost
        stop = entry * (1.0 + H2_STOP)
        ref = entry
        exit_k = None
        for k in range(e + 1, e + H2_HOLD + 1):
            if k > last or not math.isfinite(float(C[k, s])):
                exit_k = min(k, nd - 1)
                ret[exit_k] -= side_cost
                meta['delist'] += 1
                break
            o, h, c = float(O[k, s]), float(H[k, s]), float(C[k, s])
            if (math.isfinite(o) and o >= stop) or h >= stop:
                x = o if (math.isfinite(o) and o >= stop) else stop
                ret[k] += -w * (x - ref) / entry - side_cost
                meta['stops'] += 1
                exit_k = k
                break
            ret[k] += -w * (c - ref) / entry
            if fund_on:
                ret[k] += w * float(F[k, s])
            ref = c
            if k == e + H2_HOLD:
                ret[k] -= side_cost
                exit_k = k
        meta['opened'] += 1
        end = exit_k if exit_k is not None else e + H2_HOLD
        open_exits.append(end)
        covered[e:min(end, nd - 1) + 1] = True
    meta['days_open'] = int(covered.sum())
    return _result(P, ret, **meta)


# ── H3 FUND-TIME ───────────────────────────────────────────────────────────────────────────────────
def h3_prepare(hc, ctx, variant, moments=(0, 8, 16)):
    """Подготовка сделок H3 (массивы по сделкам): час бара, символ, r = close/open − 1, фандинг за бар, издержка на сторону, день, знак сигнала.
    hc: dict(grid, FB, fund{sym: (ts, rate)}, pmap[j] → индекс символа панели или −1). Вариант «до»: бар [T−1ч, T], знак = последняя ставка ≤ T−1ч (шорт при положительной);
    «после»: бар [T, T+1ч], знак = ставка, начисленная в T (лонг при положительной). Допуск — ликвидность на дату (PIT)."""
    g = hc['grid']
    nh, names = g['nh'], g['names']
    hod = (g['t0'] + np.arange(nh)) % 24
    start_h = [(m - 1) % 24 for m in moments] if variant == 'before' else list(moments)
    rows = np.flatnonzero(np.isin(hod, start_h))
    P = ctx['P']
    out = {'row': [], 'sym': [], 'r': [], 'fund': [], 'slip': [], 'day': [], 'sig': []}
    for j, s in enumerate(names):
        f = hc['fund'].get(s)
        pj = hc['pmap'][j]
        if f is None or not len(f[0]) or pj < 0:
            continue
        ft, fr = f
        bar_ts = (g['t0'] + rows) * B.HOUR_MS
        o, c = g['O'][rows, j], g['C'][rows, j]
        ok = np.isfinite(o) & np.isfinite(c)
        day = (g['t0'] + rows) // 24
        di = day - P['day0']
        inb = (di >= 0) & (di < P['nd'])
        ok &= inb
        dic = np.clip(di, 0, P['nd'] - 1)
        ok &= ctx['liq'][dic, pj]
        if variant == 'before':
            k = np.searchsorted(ft, bar_ts, side='right') - 1                # последняя ставка с временем ≤ начала бара
            has = k >= 0
            sig = np.where(has, -np.sign(fr[np.clip(k, 0, None)]), 0.0)      # положительная → шорт
        else:
            k = np.searchsorted(ft, bar_ts, side='left')
            has = (k < len(ft)) & (ft[np.clip(k, 0, len(ft) - 1)] == bar_ts)
            sig = np.where(has, np.sign(fr[np.clip(k, 0, len(fr) - 1)]), 0.0)  # положительная → лонг
        ok &= has & (sig != 0)
        sl = np.where(ctx['top'][dic, pj], 0.0003, 0.0005)
        out['row'].append(rows[ok]); out['sym'].append(np.full(int(ok.sum()), j))
        out['r'].append(c[ok] / o[ok] - 1.0); out['fund'].append(hc['FB'][rows[ok], j])
        out['slip'].append(sl[ok]); out['day'].append(day[ok]); out['sig'].append(sig[ok])
    if not out['row']:
        return {k: np.zeros(0) for k in out}
    return {k: np.concatenate(v) for k, v in out.items()}


def sim_h3(prep, fees, days_all, mult=1.0, fund_on=True, rand=False, rng=None, costs_on=True):
    """H3: вес сделки 1/16 капитала, вклад = w·(сторона·(r − фандинг) − 2·(комиссия+проскальзывание)). days_all — вектор дней (epoch) для результата."""
    n = len(prep['r'])
    side = (rng.choice([-1.0, 1.0], size=n) if rand else prep['sig'])
    cost = 2.0 * (fees['taker'] + prep['slip']) * mult if costs_on else np.zeros(n)
    x = (side * (prep['r'] - (prep['fund'] if fund_on else 0.0)) - cost) / HXS_N
    d0 = int(days_all[0])
    ret = np.bincount((prep['day'] - d0).astype(np.int64), weights=x, minlength=len(days_all))[:len(days_all)] if n else np.zeros(len(days_all))
    return {'days': days_all, 'r': ret, 'meta': {'trades': int(n)}}


# ── H5 GRID (векторно по всем сеткам: недели × символы) ───────────────────────────────────────────
def daily_from_hourly(grid):
    """Суточные H/L/C/O из часовой сетки (по суткам UTC; сутки неполные → NaN в close, если нет часа 23). → (day_first, H, L, C) массивы (ndays × ns)."""
    t0, nh = grid['t0'], grid['nh']
    pad = t0 % 24
    n_all = int(math.ceil((nh + pad) / 24.0)) * 24
    ns = grid['O'].shape[1]

    def padded(a):
        x = np.full((n_all, ns), np.nan)
        x[pad:pad + nh] = a
        return x.reshape(-1, 24, ns)
    Hh, Ll, Cc = padded(grid['H']), padded(grid['L']), padded(grid['C'])
    with np.errstate(invalid='ignore'):
        Hd = np.fmax.reduce(Hh, axis=1)
        Ld = np.fmin.reduce(Ll, axis=1)
    Cd = Cc[:, 23, :]
    return (t0 - pad) // 24, Hd, Ld, Cd


def grid_prepare(hc, ctx, m, offset_h=0):
    """Окна сеток: старт — понедельник 00:00 UTC + offset_h (0 — реальная стратегия; случайный сдвиг — калибровка). → dict окон (G = недели × символы)."""
    g = hc['grid']
    nh, names = g['nh'], g['names']
    ns = len(names)
    day_first, Hd, Ld, Cd = daily_from_hourly(g)
    atr14 = B.atr_panel(Hd, Ld, Cd, 14)
    # индексы часов старта
    first_day = g['t0'] // 24
    mon = [dd for dd in range(first_day, (g['t0'] + nh) // 24 + 1) if (dd - 4) % 7 == 0]
    starts = np.array([dd * 24 + offset_h - g['t0'] for dd in mon], np.int64)
    starts = starts[(starts >= 0) & (starts + GRID_WEEK_H < nh)]
    W = len(starts)
    T = GRID_WEEK_H
    ii = starts[:, None] + np.arange(T)[None, :]
    P = ctx['P']
    arr = {}
    for k in ('O', 'H', 'L', 'C'):
        arr[k] = np.transpose(g[k][ii], (0, 2, 1)).reshape(W * ns, T)
    arr['FB'] = np.transpose(hc['FB'][ii], (0, 2, 1)).reshape(W * ns, T)
    nxt = g['O'][starts + T]                                                # open следующего понедельника (W × ns)
    sday = (g['t0'] + starts) // 24
    atr_idx = np.clip(sday - day_first, 0, atr14.shape[0] - 1)
    atr = atr14[atr_idx]                                                    # W × ns
    P0 = g['O'][starts]                                                     # W × ns
    pj = np.array(hc['pmap'])
    di = np.clip(sday - P['day0'], 0, P['nd'] - 1)
    liq = np.zeros((W, ns), bool)
    top = np.zeros((W, ns), bool)
    for j in range(ns):
        if pj[j] >= 0:
            liq[:, j] = ctx['liq'][di, pj[j]]
            top[:, j] = ctx['top'][di, pj[j]]
    inb = ((sday - P['day0']) >= 0) & ((sday - P['day0']) < P['nd'])
    liq &= inb[:, None]
    valid = liq & np.isfinite(P0) & np.isfinite(atr)
    arr.update({'P0': P0.reshape(-1), 'atr': atr.reshape(-1), 'valid': valid.reshape(-1), 'top': top.reshape(-1), 'nxt': nxt.reshape(-1),
                'bar_day': ((g['t0'] + ii) // 24), 'W': W, 'ns': ns, 'T': T, 'starts': starts, 'm': m, 'tail_day': ((g['t0'] + starts + T) // 24)})
    return arr


def grid_sim(win, fees, mult=1.0, fund_on=True, costs_on=True, order='worst'):
    """Нейтральная сетка: 10 уровней (5 лимитных покупок ниже P, 5 продаж выше, шаг s = a·P/5, a = m·ATR14/P); исполнение — бар коснулся уровня; закрывающая заявка — на шаг к P,
    после закрытия уровень взводится со СЛЕДУЮЩЕГО бара. Худший порядок внутри бара: все открывающие заявки, которых коснулся бар, исполняются; закрывающая заявка позиции, открытой в
    этом же баре, — нет; в баре выхода за диапазон (P·(1 ± 1.5a)) закрывающие заявки не исполняются, всё закрывается по рынку по цене выхода (худшей из двух при касании обеих).
    Комиссия maker на лимитных исполнениях, taker + проскальзывание на принудительном закрытии (в т. ч. в следующий понедельник по open). Эквити = кэш + позиция·close.
    → (daily dict days/r, meta)."""
    G, T = win['O'].shape
    off = np.array([-5, -4, -3, -2, -1, 1, 2, 3, 4, 5], float)
    isbuy = off < 0
    sgn = np.where(isbuy, 1.0, -1.0)
    ok = win['valid']
    P0 = np.where(ok, win['P0'], 1.0)
    a = np.where(ok, win['m'] * win['atr'] / P0, 0.01)
    s = a * P0 / GRID_LEVELS
    LP = P0[:, None] + off[None, :] * s[:, None]
    CP = P0[:, None] + (off + np.where(isbuy, 1.0, -1.0))[None, :] * s[:, None]
    q = (1.0 / HXS_N / 10.0) / P0
    up, dn = P0 * (1.0 + GRID_STOP_MULT * a), P0 * (1.0 - GRID_STOP_MULT * a)
    fm = fees['maker'] * mult if costs_on else 0.0
    ft = fees['taker'] * mult if costs_on else 0.0
    sl = np.where(win['top'], 0.0003, 0.0005) * (mult if costs_on else 0.0)
    arm = np.repeat(ok[:, None], 10, axis=1)
    pos = np.zeros((G, 10), bool)
    stopped = ~ok
    cash = np.zeros(G)
    eq_prev = np.zeros(G)
    lastc = P0.copy()
    ret = np.zeros((G, T))
    n_stop = 0
    n_fill = 0
    for t in range(T):
        h, l, c = win['H'][:, t], win['L'][:, t], win['C'][:, t]
        fin = np.isfinite(c) & np.isfinite(h) & np.isfinite(l)
        bar = fin & ~stopped
        h_ = np.where(fin, h, 0.0)
        l_ = np.where(fin, l, 1e30)
        stop_now = bar & ((h_ >= up) | (l_ <= dn))
        touch = arm & bar[:, None] & np.where(isbuy[None, :], l_[:, None] <= LP, h_[:, None] >= LP)
        both = (h_ >= up) & (l_ <= dn) & bar
        if both.any() and order == 'worst':
            # бар задел оба края: худший порядок пути — сначала в ту сторону, где итог хуже; до выхода исполняются ТОЛЬКО заявки этой стороны (покупки при движении вниз, продажи — вверх)
            t_sell, t_buy = touch & ~isbuy[None, :], touch & isbuy[None, :]

            def total(te, X):
                dc = -q * (te * sgn * LP).sum(1) - fm * q * (te * LP).sum(1)
                nt = q * ((pos | te) * sgn).sum(1)
                ex_ = X * (1.0 - np.sign(nt) * sl)
                return dc + nt * ex_ - ft * np.abs(nt) * ex_
            a_first = total(t_sell, up) <= total(t_buy, dn)                  # A: вверх, до выхода исполняются продажи; B: вниз — покупки
            touch = np.where(both[:, None], np.where(a_first[:, None], t_sell, t_buy), touch)
            first_up = a_first
        else:
            first_up = None
        if order == 'best':                                                    # ДИАГНОСТИКА (не для решений): оптимистичный порядок — закрытие в том же баре и до выхода за диапазон
            tclose = (pos | touch) & bar[:, None] & np.where(isbuy[None, :], h_[:, None] >= CP, l_[:, None] <= CP)
        else:
            tclose = pos & (bar & ~stop_now)[:, None] & np.where(isbuy[None, :], h_[:, None] >= CP, l_[:, None] <= CP)
        cash += -q * (touch * sgn * LP).sum(1) - fm * q * (touch * LP).sum(1)
        cash += q * (tclose * sgn * CP).sum(1) - fm * q * (tclose * CP).sum(1)
        n_fill += int(touch.sum() + tclose.sum())
        pos = (pos | touch) & ~tclose
        arm = (arm & ~touch) | tclose
        if stop_now.any():
            net = q * (pos * sgn).sum(1)
            tp = np.where(h_ >= up, up, dn)
            if first_up is not None:
                tp = np.where(both, np.where(first_up, up, dn), tp)
            else:
                tp = np.where(both, np.where(net > 0, dn, up), tp)
            ex = tp * (1.0 - np.sign(net) * sl)
            cash += np.where(stop_now, net * ex - ft * np.abs(net) * ex, 0.0)
            pos[stop_now] = False
            arm[stop_now] = False
            stopped = stopped | stop_now
            n_stop += int(stop_now.sum())
        net_end = q * (pos * sgn).sum(1)
        lastc = np.where(fin, c, lastc)
        if fund_on:
            fb = np.nan_to_num(win['FB'][:, t])
            cash -= net_end * lastc * np.where(fin, fb, 0.0)
        eq = cash + net_end * lastc
        ret[:, t] = eq - eq_prev
        eq_prev = eq
    net_end = q * (pos * sgn).sum(1)
    pn = np.where(np.isfinite(win['nxt']), win['nxt'], lastc)
    ex = pn * (1.0 - np.sign(net_end) * sl)
    tail = net_end * (ex - lastc) - ft * np.abs(net_end) * ex
    return {'ret': ret, 'tail': tail, 'n_stop': n_stop, 'n_fill': n_fill}


def grid_daily(win, res, days_all):
    """Суточные суммы: вклад баров — в сутки бара, хвост (закрытие по open следующего понедельника) — в сутки этого понедельника."""
    d0 = int(days_all[0])
    n = len(days_all)
    W, ns, T = win['W'], win['ns'], win['T']
    r = res['ret'].reshape(W, ns, T)
    bd = win['bar_day']
    out = np.zeros(n)
    di = (bd - d0)
    for j in range(ns):
        m = (di >= 0) & (di < n)
        out += np.bincount(di[m], weights=r[:, j, :][m], minlength=n)[:n]
    tail = res['tail'].reshape(W, ns).sum(1)
    td = win['tail_day'] - d0
    m = (td >= 0) & (td < n)
    out += np.bincount(td[m], weights=tail[m], minlength=n)[:n]
    return out


def sim_h5(win, fees, days_all, mult=1.0, fund_on=True, costs_on=True, order='worst'):
    res = grid_sim(win, fees, mult, fund_on, costs_on, order)
    return {'days': days_all, 'r': grid_daily(win, res, days_all), 'meta': {'grids': int(win['valid'].sum()), 'stops': res['n_stop'], 'fills': res['n_fill']}}


# ── синтетика (мартингал по цене) ───────────────────────────────────────────────────────────────────
def _wicks(o, c, eh, el, rng, shape):
    hi = np.maximum(o, c) * (1.0 + np.abs(rng.normal(0.0, eh * math.sqrt(math.pi / 2), shape)))
    lo = np.minimum(o, c) * np.maximum(1.0 - np.abs(rng.normal(0.0, el * math.sqrt(math.pi / 2), shape)), 1e-6)
    return hi, lo


def real_stats(O, H, L_, C):
    """σ лог-доходности close/close, средние выходы high/low за max/min(open, close) по ряду (NaN пропускаются)."""
    c = C[np.isfinite(C)]
    lr = np.diff(np.log(c)) if len(c) > 2 else np.array([0.01])
    lr = lr[np.isfinite(lr)]
    sigma = float(lr.std()) if len(lr) > 1 else 0.01
    ok = np.isfinite(O) & np.isfinite(C) & np.isfinite(H) & np.isfinite(L_)
    if not ok.any():
        return sigma, 0.001, 0.001
    hi, lo = np.maximum(O[ok], C[ok]), np.minimum(O[ok], C[ok])
    return sigma, float(np.mean(np.clip((H[ok] - hi) / hi, 0, None))), float(np.mean(np.clip((lo - L_[ok]) / lo, 0, None)))


def synth_daily_panel(P, rng):
    """Синтетическая панель: тот же календарь, маски, оборот и фандинг; цены каждого символа — случайное блуждание с σ реального ряда, мартингал по цене (дрейф лог-доходности −σ²/2),
    open = предыдущий close (первый бар: open = close·exp(шум)), high/low — выходы за max/min(open, close). Для H2 — «листинги» с теми же датами и длительностью жизни."""
    P2 = dict(P)                                                                 # оборот, фандинг и маски — общие (не копируются: экономия памяти)
    for k in ('O', 'H', 'L', 'C'):
        P2[k] = np.full(P[k].shape, np.nan, np.float32)
    nd, ns = P['nd'], len(P['syms'])
    for j in range(ns):
        f, l = int(P['first'][j]), int(P['last'][j])
        n = l - f + 1
        sg, eh, el = real_stats(P['O'][f:l + 1, j], P['H'][f:l + 1, j], P['L'][f:l + 1, j], P['C'][f:l + 1, j])
        c0 = float(P['C'][f, j]) if math.isfinite(float(P['C'][f, j])) else 1.0
        r = rng.normal(-0.5 * sg * sg, sg, n)
        c = c0 * np.exp(np.cumsum(r))
        o = np.r_[c0, c[:-1]]
        hi, lo = _wicks(o, c, eh, el, rng, n)
        for k, a in zip(('O', 'H', 'L', 'C'), (o, hi, lo, c)):
            col = np.full(nd, np.nan, np.float32)
            col[f:l + 1] = a
            col[np.isnan(P[k][:, j])] = np.nan                                   # те же пропуски, что в реальном ряду
            P2[k][:, j] = col
    return P2


def synth_hourly(grid, rng):
    """Синтетическая часовая сетка той же формы и с теми же пропусками: мартингал по цене, σ реального символа (часовая)."""
    g2 = dict(grid)
    nh, ns = grid['nh'], len(grid['names'])
    for k in ('O', 'H', 'L', 'C'):
        g2[k] = np.full((nh, ns), np.nan)
    for j in range(ns):
        okc = np.isfinite(grid['C'][:, j])
        if okc.sum() < 10:
            continue
        sg, eh, el = real_stats(grid['O'][:, j], grid['H'][:, j], grid['L'][:, j], grid['C'][:, j])
        c0 = float(grid['C'][np.flatnonzero(okc)[0], j])
        r = rng.normal(-0.5 * sg * sg, sg, nh)
        c = c0 * np.exp(np.cumsum(r))
        o = np.r_[c0, c[:-1]]
        hi, lo = _wicks(o, c, eh, el, rng, nh)
        for k, a in zip(('O', 'H', 'L', 'C'), (o, hi, lo, c)):
            col = a.copy()
            col[~np.isfinite(grid[k][:, j])] = np.nan
            g2[k][:, j] = col
    return g2


# ── BH / отбор ──────────────────────────────────────────────────────────────────────────────────────
def step_a(ts):
    """Шаг A: проходят t > 0 и попавшие в BH q = 0.05 по списку тестов. → bool-массив."""
    ts = np.asarray(ts, float)
    p = np.array([E.p_one_sided(t) for t in ts])
    rej = E.bh_reject(p, BH_Q)
    return rej & np.isfinite(ts) & (ts > 0)
