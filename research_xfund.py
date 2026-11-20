"""research_xfund.py — X1: межбиржевой арбитраж фандинга (BingX/Bybit), BOT_SPEC §2.48. Нейтральная к направлению пара ног (шорт на бирже с более высокой ставкой, лонг на другой); суточная
симуляция по дневным барам и событиям фандинга обеих бирж; ни одного реального ордера. Справочно — пары с Binance (архив v108)."""
import asyncio
import math
import os
import time
from datetime import datetime, timezone

import numpy as np

import research_bdata as B
import research_factory as RF
import research_fengine as E
import research_xdata as X

DAY = X.DAY
HOUR = 3_600_000
THRESHOLDS = (0.0002, 0.0005, 0.0010)  # T ∈ {0.02%, 0.05%, 0.10%} за 8 ч (доли) — BOT_SPEC §2.48.6 п. 1
EXIT_D = 0.00005                       # выход при d < 0.005%
MAX_HOLD_DAYS = 14
MAX_POS = 5
LEV = 2.0
EMERG_MOVE = 0.30                      # аварийное закрытие: ход против ноги ≥ 30% от входа (§2.48.6 п. 3)
GAP_MOVE = 0.45                        # гэп ≥ 45% в одном баре — нога ликвидирована (−100% маржи)
HOUR = 3_600_000
SLIP = 0.0005
FEE_DEFAULT = 0.0005                   # комиссия по умолчанию (формулировка владельца: «FEE_TAKER биржи, иначе 0.05%»)
WINDOW_H = 72
MIN_EVENTS = 2
CORR_MIN, CORR_DAYS = 0.95, 60
LIQ_MIN = 20_000_000.0
CRIT_APR, CRIT_SHARPE, CRIT_DD = 0.04, 1.0, 0.10
KV_REPORT, KV_PASSED = 'x1_report', 'x1_passed'
C_POS = 1.0 / MAX_POS                  # номинал каждой ноги = доля капитала (маржа ноги c/2 при плече 2x)


def fm(x, nd=2):
    return 'н/д' if x is None or (isinstance(x, float) and not math.isfinite(x)) else f'{x:.{nd}f}'


# ── ставки: приведение к 8 ч ────────────────────────────────────────────────────────────────────────
def avg8_matrix(day0, nd, ev, window_h=WINDOW_H, min_events=MIN_EVENTS):
    """Средняя ставка за 8 ч на 00:00 суток d: сумма ставок всех событий с временем в [t − 72 ч, t) ÷ (72/8) — эквивалент «за 8 ч» при любом интервале начисления (1/4/8 ч). Нужно ≥ min_events
    события в окне, иначе NaN. ev = (ts мс, rate) | None. → массив длины nd."""
    out = np.full(nd, np.nan)
    if ev is None or len(ev[0]) == 0:
        return out
    ts, r = np.asarray(ev[0], np.int64), np.asarray(ev[1], float)
    cs = np.r_[0.0, np.cumsum(r)]
    t = (day0 + np.arange(nd, dtype=np.int64)) * DAY
    hi = np.searchsorted(ts, t, side='left')                       # события строго до 00:00
    lo = np.searchsorted(ts, t - window_h * HOUR, side='left')     # окно [t − 72 ч, t): закрыто слева, открыто справа — ровно 9 расчётов при 8-часовом графике
    cnt = hi - lo
    ok = cnt >= min_events
    out[ok] = (cs[hi] - cs[lo])[ok] / (window_h / 8.0)
    return out


def interval_hours(ts):
    """Справочно: интервал начисления из данных (медиана разностей времён, ч)."""
    ts = np.asarray(ts, np.int64)
    return float(np.median(np.diff(ts)) / HOUR) if len(ts) > 2 else None


# ── пары ────────────────────────────────────────────────────────────────────────────────────────────
def pair_ok(a, b):
    """Один и тот же актив на двух площадках: корреляция суточных лог-доходностей ≥ 0.95 на ≥ 60 общих сутках (масштаб цены, например 1000PEPE/PEPE, не важен)."""
    ta, tb = a['ts'], b['ts']
    common, ia, ib = np.intersect1d(ta, tb, return_indices=True)
    if len(common) < CORR_DAYS + 1:
        return False, 0.0, len(common)
    ca, cb = a['c'][ia], b['c'][ib]
    ok = (ca > 0) & (cb > 0)
    ra, rb = np.diff(np.log(ca[ok])), np.diff(np.log(cb[ok]))
    if len(ra) < CORR_DAYS or ra.std() == 0 or rb.std() == 0:
        return False, 0.0, len(ra)
    c = float(np.corrcoef(ra, rb)[0, 1])
    return bool(c >= CORR_MIN), c, len(ra)


def _hseries(h, hb0):
    """Часовые массивы пары: dict(off — индекс первого часа (абсолютный, часы с эпохи), h/l/c float32) | None."""
    if h is None or len(h['ts']) == 0:
        return None
    hours = (np.asarray(h['ts']) // HOUR).astype(np.int64)
    off, n = int(hours[0]), int(hours[-1] - hours[0] + 1)
    out = {'off': off, 'n': n}
    for k in 'hlc':
        arr = np.full(n, np.nan, np.float32)
        arr[hours - off] = np.asarray(h[k], np.float32)
        out[k] = arr
    return out


def _hget(hs, k, hour):
    """Значение часового ряда на абсолютном часе или NaN."""
    if hs is None:
        return float('nan')
    i = hour - hs['off']
    return float(hs[k][i]) if 0 <= i < hs['n'] else float('nan')


class _Lru(dict):
    """Маленький кэш с вытеснением самого старого (для открытых позиций: ≤ MAX_POS пар одновременно)."""

    def __init__(self, cap):
        super().__init__()
        self.cap = cap

    def put(self, k, v):
        if len(self) >= self.cap:
            del self[next(iter(self))]
        self[k] = v
        return v


class _Src:
    """Дневной ряд пары j площадки (один файл на пару): общий кэш нескольких недавних пар для всех полей O/H/L/C/Q — одно чтение файла обслуживает пять полей."""

    def __init__(self, ven, syms):
        self.ven, self.syms = ven, syms
        self.cache = _Lru(12)

    def get(self, j):
        x = self.cache.get(j)
        if x is None:
            x = self.cache.put(j, self.ven['bars'][self.syms[j]])
        return x


class PairMat:
    """Поле дневной матрицы «сутки × пара» (O/H/L/C/Q для площадки A или B), НЕ хранимое целиком: `m[d, j]` — значение, `m[:, j]` — столбец пары j; столбец строится из ряда пары
    тем же присваиванием, что и прежняя плотная матрица (значения побитово те же), и кэшируется для немногих недавних пар (v123, BOT_SPEC §2.55)."""

    def __init__(self, src, key, day0, nd):
        self.src, self.key, self.day0, self.nd = src, key, day0, nd
        self.cache = _Lru(24)

    def col(self, j):
        c = self.cache.get(j)
        if c is None:
            x = self.src.get(j)
            di = (x['ts'] // DAY - self.day0).astype(np.int64)
            c = np.full(self.nd, np.nan)
            c[di] = x[self.key]
            self.cache.put(j, c)
        return c

    def __getitem__(self, k):
        d, j = k
        return self.col(int(j))[d]


class LazyFund:
    """inp['fund'][j] → (события A, события B) пары j; ряды читаются из площадок при обращении (кэш — несколько недавних пар)."""

    def __init__(self, venA, venB, syms):
        self.venA, self.venB, self.syms = venA, venB, syms
        self.cache = _Lru(24)

    def __len__(self):
        return len(self.syms)

    def __getitem__(self, j):
        v = self.cache.get(j)
        if v is None:
            sa, sb = self.syms[j]
            v = self.cache.put(j, (self.venA['fund'].get(sa), self.venB['fund'].get(sb)))
        return v


class LazyHs:
    """hs[tag][j] → часовые массивы пары j (или None, если часовых данных нет — файл не читается); строятся при обращении."""

    def __init__(self, ven, syms, day0):
        self.ven, self.syms, self.day0 = ven, syms, day0
        self.cache = _Lru(16)

    def __len__(self):
        return len(self.syms)

    def __getitem__(self, j):
        if j in self.cache:
            return self.cache[j]
        hb = self.ven.get('hbars') or {}
        sym = self.syms[j]
        return self.cache.put(j, _hseries(hb.get(sym) if sym in hb else None, self.day0))


def build_inputs(venA, venB, pairs):
    """venX = {'bars': {sym: дневные}, 'hbars': {sym: часовые h/l/c} (необязательно), 'fund': {sym: (ts, rate)}, 'fee': доля}; pairs = [(метка, symA, symB)]. → inputs для simulate_x1.
    v123 (BOT_SPEC §2.55): ПОТОКОВО — пара за парой: сигнал d (avg8), допуск ликвидности PIT и «присутствие» считаются по одной паре (плотные массивы только avg/elig/present «сутки × пары»),
    дневные поля O/H/L/C/Q, ряды фандинга и часовые ряды — ленивые (PairMat, LazyFund, LazyHs) и читаются лишь для открытых позиций; значения побитово те же, что у плотной версии v122.
    Допуск: оба бара + ликвидность PIT ≥ 20 млн на КАЖДОЙ площадке + ≥ 2 события в окне на каждой."""
    keep, rejected, ext = [], [], []
    for lab, sa, sb in pairs:
        a, b = venA['bars'].get(sa), venB['bars'].get(sb)
        if a is None or b is None:
            rejected.append((lab, 'нет данных'))
            continue
        if B.stable_like(a['c']) or B.stable_like(b['c']):             # [v120, §2.52] стейблкоин-подобные не входят в X1
            rejected.append((lab, 'стейблкоин-подобный'))
            continue
        ok, corr, n = pair_ok(a, b)
        if ok:
            keep.append((lab, sa, sb))
            ext.append((int(min(a['ts'][0], b['ts'][0])) // DAY, int(max(a['ts'][-1], b['ts'][-1])) // DAY))
        else:
            rejected.append((lab, f'корреляция {corr:.2f} на {n} сутках'))
        del a, b
    if not keep:
        return None, rejected
    day0 = min(e[0] for e in ext)
    day1 = max(e[1] for e in ext)
    nd, n = day1 - day0 + 1, len(keep)
    avg = {'A': np.full((nd, n), np.nan), 'B': np.full((nd, n), np.nan)}
    present = np.zeros((nd, n), dtype=bool)
    elig = np.zeros((nd, n), dtype=bool)
    for j, (lab, sa, sb) in enumerate(keep):
        cols, liq = {}, {}
        for tag, ven, sym in (('A', venA, sa), ('B', venB, sb)):
            x = ven['bars'][sym]
            di = (x['ts'] // DAY - day0).astype(np.int64)
            for k, kk in (('O', 'o'), ('C', 'c'), ('Q', 'qv')):
                col = np.full(nd, np.nan)
                col[di] = x[kk]
                cols[k + tag] = col
            avg[tag][:, j] = avg8_matrix(day0, nd, ven['fund'].get(sym))
            adv, _c = B.pit_adv(cols['Q' + tag].astype(np.float32).reshape(-1, 1))
            liq[tag] = B.liquid_mask(adv)[:, 0]
            del x, di
        pj = np.isfinite(cols['OA']) & np.isfinite(cols['CA']) & np.isfinite(cols['OB']) & np.isfinite(cols['CB'])
        present[:, j] = pj
        elig[:, j] = pj & liq['A'] & liq['B'] & np.isfinite(avg['A'][:, j]) & np.isfinite(avg['B'][:, j])
        del cols, liq
    syms = [(k[1], k[2]) for k in keep]
    M = {}
    srcA, srcB = _Src(venA, [s_[0] for s_ in syms]), _Src(venB, [s_[1] for s_ in syms])
    for k in 'OHLCQ':
        kk = {'O': 'o', 'H': 'h', 'L': 'l', 'C': 'c', 'Q': 'qv'}[k]
        M[k + 'A'] = PairMat(srcA, kk, day0, nd)
        M[k + 'B'] = PairMat(srcB, kk, day0, nd)
    return {'day0': day0, 'nd': nd, 'M': M, 'avg': avg, 'elig': elig, 'present': present, 'labels': [k[0] for k in keep], 'syms': syms,
            'hs': {'A': LazyHs(venA, [s_[0] for s_ in syms], day0), 'B': LazyHs(venB, [s_[1] for s_ in syms], day0)},
            'fund': LazyFund(venA, venB, syms), 'fees': (venA['fee'], venB['fee'])}, rejected


# ── симуляция ───────────────────────────────────────────────────────────────────────────────────────
def leg_funding(ev, t0, t1, short, c):
    """События фандинга ноги в (t0, t1] — СТРОГО по фактическим отметкам времени биржи ноги: шорт получает положительную ставку (+rate·c), лонг платит (−rate·c).
    → [(индекс суток по правилу (ts, ts+сутки], сумма)]. Приведения к 8 ч здесь нет (оно только в avg8_matrix для сигнала d)."""
    if ev is None or len(ev[0]) == 0:
        return []
    ts, r = np.asarray(ev[0], np.int64), np.asarray(ev[1], float)
    lo, hi = np.searchsorted(ts, t0, side='right'), np.searchsorted(ts, t1, side='right')
    sgn = 1.0 if short else -1.0
    return [(int((t - 1) // DAY), sgn * c * float(x)) for t, x in zip(ts[lo:hi], r[lo:hi])]


def simulate_x1(inp, T, mult=1.0, max_pos=MAX_POS):
    """Суточная симуляция X1 (BOT_SPEC §2.48.2–2.48.6). Сигнал — 00:00 UTC; вход/выход — по close 1h-свечи, начинающейся в 00:00 (время сделки 01:00), на обеих площадках; при отсутствии часовых
    свечей — по дневному close (запасной вариант, счётчик n_fb). Аварийное закрытие: ход против ноги ≥ 30% от входа (по hi/lo часов, иначе дней) → обе ноги закрываются (сработавшая — по уровню, вторая —
    по close бара); ≥ 45% в одном баре (гэп) — нога ликвидирована (−100% маржи). Возвращает dict: pnl/fund/price/cost (по суткам), trades, liq, emerg, n_h, n_fb, open_end."""
    nd, M, elig, hs = inp['nd'], inp['M'], inp['elig'], inp['hs']
    n = len(inp['labels'])
    day0 = inp['day0']
    pnl, fund, price, cost = (np.zeros(nd) for _ in range(4))
    feeA, feeB = inp['fees']
    c = 1.0 / max_pos
    fills = (feeA * mult + SLIP * mult, feeB * mult + SLIP * mult)
    pos, trades = [], []
    cnt = {'liq': 0, 'emerg': 0, 'n_h': 0, 'n_fb': 0, 'em_h': 0, 'em_d': 0}
    TAGS = (('A', 0), ('B', 1))
    hists = []                                                                   # [v124, §2.53.2] история P&L каждой сделки (параллельно trades) — только для разбивки отчёта 1h/дневной; суммарные массивы считаются теми же операциями, что и раньше

    def cr(p, name, arr, k, amt):
        """arr[k] += amt (та же операция, что и прежде) + запись вклада в историю позиции p."""
        arr[k] += amt
        h = p['h'][name]
        h[k] = h.get(k, 0.0) + amt

    def accrue(p, t0, t1):
        for vi in (0, 1):
            short = p['short'] == vi
            for d_idx, amt in leg_funding(inp['fund'][p['j']][vi], t0, t1, short, c):
                k = d_idx - day0
                if 0 <= k < nd:
                    cr(p, 'fund', fund, k, amt)
                    cr(p, 'pnl', pnl, k, amt)

    def hour_abs(d, h=0):
        return (day0 + d) * 24 + h

    def scan(p, d, bars):
        """Контроль аварийного закрытия: bars — список (час | None, ...) → первый по времени бар, где сработал порог. Часовой режим: bars = часы (абс.), дневной: [None] (весь день).
        → None | (idx, kinds[2], extremes) где kinds[vi] ∈ {None, 'emerg', 'gap'}."""
        j = p['j']
        for b in bars:
            kinds = [None, None]
            for tag, vi in TAGS:
                short = p['short'] == vi
                if b is None:
                    hi_, lo_ = M['H' + tag][d, j], M['L' + tag][d, j]
                else:
                    hh = hs[tag][j]
                    hi_, lo_ = _hget(hh, 'h', b), _hget(hh, 'l', b)
                p0 = p['p0'][vi]
                if short and math.isfinite(hi_) and hi_ >= p0 * (1.0 + EMERG_MOVE):
                    kinds[vi] = 'gap' if hi_ >= p0 * (1.0 + GAP_MOVE) else 'emerg'
                if (not short) and math.isfinite(lo_) and lo_ <= p0 * (1.0 - EMERG_MOVE):
                    kinds[vi] = 'gap' if lo_ <= p0 * (1.0 - GAP_MOVE) else 'emerg'
            if kinds[0] or kinds[1]:
                return b, kinds
        return None

    def bar_close(p, tag, d, b):
        j = p['j']
        if b is None:
            return M['C' + tag][d, j], (day0 + d + 1) * DAY
        v = _hget(hs[tag][j], 'c', b)
        return (v if math.isfinite(v) else M['C' + tag][d, j]), (b + 1) * HOUR

    def emergency(p, d, hit):
        """Закрытие по аварийному правилу в баре hit=(бар, kinds). Дневные P&L ног, издержки, фандинг до конца бара, учёт."""
        b, kinds = hit
        cnt['em_d' if b is None else 'em_h'] += 1                                # источник срабатывания: дневные high/low (b is None) либо 1h
        t_end = None
        for tag, vi in TAGS:
            d_dir = -1.0 if p['short'] == vi else 1.0
            if kinds[vi] == 'gap':
                day_pnl = -c / LEV - p['legpnl'][vi]
                p['legpnl'][vi] = -c / LEV
                cnt['liq'] += 1
            elif kinds[vi] == 'emerg':
                level = p['p0'][vi] * (1.0 + EMERG_MOVE) if p['short'] == vi else p['p0'][vi] * (1.0 - EMERG_MOVE)
                day_pnl = d_dir * c * (level - p['ref'][vi]) / p['p0'][vi]
                cr(p, 'cost', cost, d, c * fills[vi])
                cr(p, 'pnl', pnl, d, -(c * fills[vi]))
                cnt['emerg'] += 1
            else:                                                             # вторая нога — по close бара своей биржи
                px, _t = bar_close(p, tag, d, b)
                day_pnl = d_dir * c * (px - p['ref'][vi]) / p['p0'][vi]
                cr(p, 'cost', cost, d, c * fills[vi])
                cr(p, 'pnl', pnl, d, -(c * fills[vi]))
            _px, t_end = bar_close(p, tag, d, b)
            cr(p, 'pnl', pnl, d, day_pnl)
            cr(p, 'price', price, d, day_pnl)
        accrue(p, p['te'], t_end)
        trades.append({'sym': inp['labels'][p['j']], 'e': p['e'], 'x': d, 'reason': 'ликвидация (гэп)' if 'gap' in kinds else 'аварийное 30%', 'fb': p['fb']})
        hists.append(p['h'])
        pos.remove(p)

    for d in range(nd):
        exited_today = set()
        # 1. выходы (решение на 00:00; сделка — по close часа 00:00, время 01:00)
        for p in list(pos):
            j = p['j']
            held = d - p['e']
            dnow = (inp['avg']['A'][d, j] - inp['avg']['B'][d, j]) * (1.0 if p['short'] == 0 else -1.0)
            gone = not inp['present'][d, j]
            reason = 'время' if held >= MAX_HOLD_DAYS else ('данные' if gone else ('d' if (not math.isfinite(dnow) or dnow < EXIT_D) else None))
            if reason is None:
                continue
            hourly = (p['mode'] == 'h') and all(math.isfinite(_hget(hs[t][j], 'c', hour_abs(d))) for t in 'AB')
            if not gone:
                hit = scan(p, d, [hour_abs(d)] if hourly else ([None] if p['mode'] == 'd' else []))
                if hit is not None:
                    emergency(p, d, hit)
                    exited_today.add(j)
                    continue
            if hourly:
                pxs = [_hget(hs[t][j], 'c', hour_abs(d)) for t in 'AB']
                t_exit = (hour_abs(d) + 1) * HOUR
            else:
                pxs = [M['C' + t][d, j] if not gone else p['ref'][i] for i, t in enumerate('AB')]
                t_exit = (day0 + d + 1) * DAY
                if p['mode'] == 'h' and not gone:
                    p['fb'] = True                                             # вход по часам, выход по дневному close — запасной вариант выхода
            for vi in (0, 1):
                d_dir = -1.0 if p['short'] == vi else 1.0
                pdl = 0.0 if gone else d_dir * c * (pxs[vi] - p['ref'][vi]) / p['p0'][vi]
                p['legpnl'][vi] += pdl
                cr(p, 'pnl', pnl, d, pdl)
                cr(p, 'price', price, d, pdl)
                cr(p, 'cost', cost, d, c * fills[vi])
                cr(p, 'pnl', pnl, d, -(c * fills[vi]))
            accrue(p, p['te'], t_exit)
            trades.append({'sym': inp['labels'][j], 'e': p['e'], 'x': d, 'reason': reason, 'fb': p['fb']})
            hists.append(p['h'])
            pos.remove(p)
            exited_today.add(j)
        # 2. входы
        if len(pos) < max_pos:
            busy = {p['j'] for p in pos}
            cands = []
            for j in range(n):
                if j in busy or j in exited_today or not elig[d, j]:
                    continue
                diff = inp['avg']['A'][d, j] - inp['avg']['B'][d, j]
                dv = abs(diff)
                if dv >= T:
                    cands.append((-dv, inp['labels'][j], j, 0 if diff >= 0 else 1))      # short на площадке с большей ставкой
            cands.sort()
            for _dv, _lab, j, sv in cands[:max_pos - len(pos)]:
                ha = [_hget(hs[t][j], 'c', hour_abs(d)) for t in 'AB']
                if all(math.isfinite(x) for x in ha):
                    mode, p0, te, fb = 'h', (ha[0], ha[1]), (hour_abs(d) + 1) * HOUR, False
                    cnt['n_h'] += 1
                else:
                    mode, p0, te, fb = 'd', (M['CA'][d, j], M['CB'][d, j]), (day0 + d + 1) * DAY, True
                    cnt['n_fb'] += 1
                npos = {'j': j, 'short': sv, 'e': d, 'mode': mode, 'fb': fb, 'p0': p0, 'ref': [p0[0], p0[1]], 'legpnl': [0.0, 0.0], 'te': te, 'h': {'pnl': {}, 'fund': {}, 'price': {}, 'cost': {}}}
                pos.append(npos)
                for vi in (0, 1):
                    cr(npos, 'cost', cost, d, c * fills[vi])
                    cr(npos, 'pnl', pnl, d, -(c * fills[vi]))
        # 3. контроль аварийного закрытия внутри суток и разметка дня
        for p in list(pos):
            j = p['j']
            if not inp['present'][d, j]:
                continue
            if p['mode'] == 'h':
                h0 = hour_abs(d) if d > p['e'] else hour_abs(d) + 1
                hit = scan(p, d, range(h0, hour_abs(d) + 24))
            else:
                hit = scan(p, d, [None]) if d > p['e'] else None
            if hit is None and p['mode'] == 'h':
                # часовые бары могли отсутствовать: страховка по дневным high/low (запасной вариант с пометкой)
                dh = scan(p, d, [None]) if d > p['e'] else None
                if dh is not None:
                    p['fb'] = True
                    hit = dh
            if hit is not None:
                emergency(p, d, hit)
                continue
            for tag, vi in TAGS:
                d_dir = -1.0 if p['short'] == vi else 1.0
                day_pnl = d_dir * c * (M['C' + tag][d, j] - p['ref'][vi]) / p['p0'][vi]
                p['legpnl'][vi] += day_pnl
                p['ref'][vi] = M['C' + tag][d, j]
                cr(p, 'pnl', pnl, d, day_pnl)
                cr(p, 'price', price, d, day_pnl)
    for p in pos:                                                                # открытые на конец данных: фандинг до конца, издержки выхода не платятся
        accrue(p, p['te'], (day0 + nd) * DAY)
    grp = {g: {'pnl': np.zeros(nd), 'fund': np.zeros(nd), 'price': np.zeros(nd), 'cost': np.zeros(nd), 'trades': [], 'liq': 0, 'emerg': 0, 'open_end': 0, 'open_syms': []} for g in ('h', 'd')}
    for tr, h in zip(trades, hists):                                              # группа сделки: «d» — запасной дневной базис использован (вход, выход или контроль хода), иначе «h» — 1h
        g = grp['d' if tr['fb'] else 'h']
        g['trades'].append(tr)
        for name, hh in h.items():
            for k, amt in hh.items():
                g[name][k] += amt
    for p in pos:
        g = grp['d' if p['fb'] else 'h']
        g['open_end'] += 1
        g['open_syms'].append(inp['labels'][p['j']])
        for name, hh in p['h'].items():
            for k, amt in hh.items():
                g[name][k] += amt
    return {'pnl': pnl, 'fund': fund, 'price': price, 'cost': cost, 'trades': trades, 'liq': cnt['liq'], 'emerg': cnt['emerg'], 'n_h': cnt['n_h'], 'n_fb': cnt['n_fb'], 'open_end': len(pos), 'open_syms': [inp['labels'][p_['j']] for p_ in pos], 'em_h': cnt['em_h'], 'em_d': cnt['em_d'], 'groups': grp}


# ── метрики и отчёт ─────────────────────────────────────────────────────────────────────────────────
def metrics(res, inp):
    nd, day0 = inp['nd'], inp['day0']
    first = int(np.argmax(inp['elig'].any(axis=1))) if inp['elig'].any() else 0
    x = res['pnl'][first:]
    days = np.arange(day0 + first, day0 + nd) * DAY
    mean = float(x.mean()) if len(x) else 0.0
    sd = float(x.std()) if len(x) else 0.0
    sharpe = mean / sd * math.sqrt(365) if sd > 0 else float('nan')
    dd = E.max_drawdown(x)
    yr = np.array([datetime.fromtimestamp(t / 1000, timezone.utc).year for t in days])
    mo = np.array([datetime.fromtimestamp(t / 1000, timezone.utc).year * 12 + datetime.fromtimestamp(t / 1000, timezone.utc).month for t in days])
    years = {int(y): float(x[yr == y].sum()) for y in sorted(set(yr))}
    months = {int(m): float(x[mo == m].sum()) for m in sorted(set(mo))}
    worst = min(months.values()) if months else 0.0
    f, pr, co = float(res['fund'][first:].sum()), float(res['price'][first:].sum()), float(res['cost'][first:].sum())
    durs = [t['x'] - t['e'] for t in res['trades']]
    n_fb = sum(1 for t in res['trades'] if t.get('fb'))
    return {'n_fb': n_fb, 'emerg': res.get('emerg', 0), 'apr': mean * 365, 'sharpe': sharpe, 'dd': dd, 'worst_month': worst, 'years': years, 'n_trades': len(res['trades']), 'avg_days': (float(np.mean(durs)) if durs else None),
            'fund': f, 'price': pr, 'cost': co, 'fund_share': (f / (f + pr) if (f + pr) != 0 else None), 'cost_to_fund': (co / f if f > 0 else None), 'liq': res['liq'], 'n_days': len(x), 'open_end': res['open_end']}


def criteria(m):
    c = {'apr': bool(m['apr'] > CRIT_APR), 'sharpe': bool(math.isfinite(m['sharpe']) and m['sharpe'] >= CRIT_SHARPE), 'dd': bool(m['dd'] <= CRIT_DD),
         'y2025': bool(m['years'].get(2025, 0.0) > 0), 'y2026': bool(m['years'].get(2026, 0.0) > 0)}
    return c, bool(all(c.values()))


def top_symbols(res, inp, k=10):
    """Топ-k символов по вкладу в P&L (по закрытым сделкам — число сделок) с ёмкостью: K_max = 5% × min(медианный дневной оборот на двух площадках) (0.2K ≤ 1% оборота на ногу)."""
    by = {}
    for t in res['trades']:
        by.setdefault(t['sym'], 0)
        by[t['sym']] += 1
    caps = {}
    for j, lab in enumerate(inp['labels']):
        qa, qb = inp['M']['QA'][:, j], inp['M']['QB'][:, j]
        va, vb = np.nanmedian(qa) if np.isfinite(qa).any() else float('nan'), np.nanmedian(qb) if np.isfinite(qb).any() else float('nan')
        caps[lab] = 0.05 * min(va, vb)
    return sorted(((s, n, caps.get(s)) for s, n in by.items()), key=lambda x: -x[1])[:k]


def fmt_metrics(label, T, m, c, ok):
    mark = lambda v: 'да' if v else 'НЕТ'
    yrs = ', '.join(f'{y}: {v * 100:+.2f}%' for y, v in m['years'].items())
    return [f'<b>{label}, T = {T * 100:.2f}% за 8 ч</b>: APR {m["apr"] * 100:+.2f}% [{mark(c["apr"])}], Шарп {fm(m["sharpe"])} [{mark(c["sharpe"])}], просадка {m["dd"] * 100:.1f}% [{mark(c["dd"])}], худший месяц {m["worst_month"] * 100:+.2f}%',
            f'  сделок {m["n_trades"]} (базис по 1h: {m["n_trades"] - m["n_fb"]}, по дневному close — запасной вариант: {m["n_fb"]}), средняя длительность {fm(m["avg_days"], 1)} сут., '
            f'аварийных закрытий (ход ≥ 30%): {m["emerg"]}, ликвидаций ног (гэп ≥ 45%): {m["liq"]}, открыто на конец {m["open_end"]}; доход: фандинг {m["fund"] * 100:+.2f}%, разница цен {m["price"] * 100:+.2f}%, '
            f'издержки {m["cost"] * 100:.2f}% (издержки / валовый фандинг {fm(m["cost_to_fund"], 2)}); доля фандинга в P&L {fm(m["fund_share"], 2) if m["fund_share"] is not None else "н/д"}',
            f'  по годам: {yrs}; 2025 [{mark(c["y2025"])}], 2026 [{mark(c["y2026"])}]; ' + ('<b>критерии выполнены</b>' if ok else 'критерии НЕ выполнены')]


def fmt_split(res, inp):
    """[v124, §2.53.2] Разбивка отчёта (СПРАВОЧНО, не критерии): метрики по каждому T отдельно для сделок с 1h-базисом и с дневным запасным (группа — по признаку fb сделки: запасной дневной close
    использован при входе, выходе или контроле хода), число аварийных закрытий (позиций) по 1h и по дневным high/low."""
    out = []
    for key, name in (('h', '1h-базис'), ('d', 'дневной запасной базис')):
        g = res['groups'][key]
        m = metrics(g, inp)
        out.append(f'{name}: сделок {m["n_trades"]} (открыто на конец {m["open_end"]}), APR {m["apr"] * 100:+.2f}%, Шарп {fm(m["sharpe"])}, просадка {m["dd"] * 100:.1f}%, '
                   f'фандинг {m["fund"] * 100:+.2f}%, цена {m["price"] * 100:+.2f}%, издержки {m["cost"] * 100:.2f}%')
    return ['  разбивка по базису (справочно, не критерии): ' + '; '.join(out),
            f'  аварийных закрытий (позиций): определено по 1h {res["em_h"]}, по дневным high/low {res["em_d"]}']


# ── данные и команда ────────────────────────────────────────────────────────────────────────────────
LazyStore = X.LazyStore                                          # v123: определение — в research_xdata (общее для X1 и H2)


def load_venues(xdir, bdir, fee_taker):
    """Площадки: BingX, Bybit, OKX (кэш XDATA_DIR) и Binance (архив v108). v123: ряды НЕ загружаются — ключи из индекса (запись есть и файл существует), ряд читается по одному при обращении."""
    idx = X.load_index(xdir)
    ven = {}
    for ex in X.EXCHANGES:
        ks = [s_ for s_, r in idx['bars'].get(ex, {}).items() if int(r.get('n', 1) or 0) > 0 and os.path.exists(X.bars_path(xdir, ex, s_))]
        fk = [s_ for s_ in ks if os.path.exists(X.fund_path(xdir, ex, s_))]
        hk = [s_ for s_ in ks if os.path.exists(X.hbars_path(xdir, ex, s_))]
        ven[ex] = {'bars': LazyStore(ks, lambda s_, ex=ex: X.load_bars(xdir, ex, s_)), 'fund': LazyStore(fk, lambda s_, ex=ex: X.load_fund(xdir, ex, s_)),
                   'hbars': LazyStore(hk, lambda s_, ex=ex: X.load_hbars(xdir, ex, s_)), 'fee': (fee_taker if ex == 'bingx' else FEE_DEFAULT), 'markets': idx['markets'].get(ex, {})}
    return ven


def _non_crypto(mk_, sym):
    """[v120, §2.52] Не-крипто по записи markets (поле excl от загрузки v120) либо по префиксу базы (кэш прошлых версий)."""
    return bool((mk_ or {}).get('excl') or X.noncrypto_reason((mk_ or {}).get('base') or sym.split('/')[0]))


def common_pairs(ven, a='bingx', b='bybit'):
    na, nb = {}, {}
    for sym in ven[a]['bars']:
        if _non_crypto(ven[a]['markets'].get(sym), sym):
            continue
        na.setdefault(ven[a]['markets'].get(sym, {}).get('norm') or X.norm_base(sym.split('/')[0]), []).append(sym)
    for sym in ven[b]['bars']:
        if _non_crypto(ven[b]['markets'].get(sym), sym):
            continue
        nb.setdefault(ven[b]['markets'].get(sym, {}).get('norm') or X.norm_base(sym.split('/')[0]), []).append(sym)
    out = []
    for norm in sorted(set(na) & set(nb)):
        # несколько рынков одной базы: берём с большим оборотом (корреляция всё равно проверяется)
        sa = max(na[norm], key=lambda s: float(np.mean(ven[a]['bars'][s]['qv'])))
        sb = max(nb[norm], key=lambda s: float(np.mean(ven[b]['bars'][s]['qv'])))
        out.append((norm, sa, sb))
    return out


def _npz_len(path):
    """Число строк ряда (длина `ts`) без загрузки остальных массивов; None — файла нет/не читается."""
    try:
        with np.load(path) as z:
            return int(len(z['ts']))
    except Exception:
        return None


def binance_venue(bdir, norms, fee_taker, with_h1=False):
    """Binance из архива: символы, чья нормализованная база среди norms; выбирается ряд с наибольшим числом суток.
    with_h1 [v118, §2.50]: добавляются часовые свечи архива (`hbars`, для базиса X1 по 1h); ставки — fundingRate архива с ФАКТИЧЕСКИМИ отметками времени (calc_time).
    v123 (§2.55): выбор символа — по длине `ts` (остальные массивы не читаются), ряды читаются лениво по одному."""
    idx = B.load_index(bdir)
    best = {}
    for s in idx.get('symbols', []):
        nb = X.norm_base(s[:-4] if s.endswith('USDT') else s)
        if nb not in norms:
            continue
        n_ = _npz_len(B.d1_path(bdir, s))
        if not n_:
            continue
        if nb not in best or n_ > best[nb][1]:
            best[nb] = (s, n_)
    sym_of = {nb: s for nb, (s, _n) in best.items()}

    def d1(nb):
        z = B.load_d1(bdir, sym_of[nb])
        return {'ts': z['ts'], 'o': z['o'], 'h': z['h'], 'l': z['l'], 'c': z['c'], 'qv': z['qv']}

    def fu(nb):
        f = B.load_fund(bdir, sym_of[nb])
        return (f['ts'], f['rate'])

    def h1(nb):
        h = B.load_h1(bdir, sym_of[nb])
        return {'ts': h['ts'], 'h': h['h'], 'l': h['l'], 'c': h['c']}
    fk = [nb for nb in sym_of if os.path.exists(B.fr_path(bdir, sym_of[nb]))]
    hk = [nb for nb in sym_of if with_h1 and (_npz_len(B.h1_path(bdir, sym_of[nb])) or 0) > 0]
    return {'bars': LazyStore(list(sym_of), d1), 'fund': LazyStore(fk, fu), 'hbars': LazyStore(hk, h1), 'fee': fee_taker, 'markets': {}, 'symbols': dict(sym_of)}


def main_pairs_binance(ven_bingx, bn):
    """[v118] Пары (метка, символ BingX, база Binance): база есть в обеих площадках; несколько рынков одной базы на BingX — с большим оборотом."""
    nm = {}
    for sym in ven_bingx['bars']:
        mk_ = ven_bingx['markets'].get(sym, {})
        if mk_.get('excl') or X.noncrypto_reason(mk_.get('base') or sym.split('/')[0]):          # [v120, §2.52] не-крипто исключены из X1
            continue
        nm.setdefault(mk_.get('norm') or X.norm_base(sym.split('/')[0]), []).append(sym)
    return [(norm, max(nm[norm], key=lambda s: float(np.mean(ven_bingx['bars'][s]['qv']))), norm) for norm in sorted(set(nm) & set(bn['bars']))]


async def sync_binance_h1(params, progress=None, fetch=None):
    """[v118] 1h-свечи архива Binance для баз, загруженных с BingX (X1, §2.50); продолжение с места остановки. → строка отчёта."""
    xdir = params.get('xdir') or X.xdata_dir()
    bdir = params.get('bdir') or B.battery_dir()
    idx_b = B.load_index(bdir)
    if not idx_b.get('symbols'):
        return '⛔ архив Binance пуст (/battery_load) — 1h-свечи Binance не загружены'
    idx = X.load_index(xdir)
    norms = {idx['markets'].get('bingx', {}).get(s, {}).get('norm') for s in idx['fund'].get('bingx', {})}
    norms.discard(None)
    if params.get('only_norms') is not None:                         # [v121] 1h архива — только для баз символов сделок (прогон назад по всем общим базам — часы)
        norms &= set(params['only_norms'])
    syms = []
    for s in sorted(idx_b['symbols']):
        nb = X.norm_base(s[:-4] if s.endswith('USDT') else s)
        if nb in norms and B.load_d1(bdir, s) is not None:
            syms.append(s)
    deadline = time.time() + float(params.get('max_minutes', 150)) * 60
    own = fetch is None
    if own:
        import aiohttp
        import research_vision
        async with aiohttp.ClientSession() as ses:
            r = await B.sync_h1(research_vision.make_fetcher(ses), bdir, syms, deadline, progress)
    else:
        r = await B.sync_h1(fetch, bdir, syms, deadline, progress)
    tail = ' — ⏱ бюджет времени исчерпан, повторите команду' if r['stopped'] else ''
    return f'  Binance 1h-свечи (архив): символов {r["processed"]}/{len(syms)}, месяцев загружено {r["loaded"]}, не получено {r["failed"]}{tail}'


def prelim_traded(xdir, bdir, fee):
    """[v121, §2.53] Символы, попавшие в сделки X1 в предварительном прогоне на дневных данных (все пороги THRESHOLDS; закрытые и открытые на конец позиции):
    → ([(метка, символ BingX)], число пар в прогоне). 1h-свечи нужны только им (базис и ход против ноги по 1h)."""
    ven = load_venues(xdir, bdir, fee)
    if not ven['bingx']['bars']:
        return [], 0
    norms = {X.norm_base(sym.split('/')[0]) for sym in ven['bingx']['bars']} | {v.get('norm') for v in ven['bingx']['markets'].values() if v.get('norm')}
    bn = binance_venue(bdir, norms, FEE_DEFAULT, with_h1=False)
    pairs = main_pairs_binance(ven['bingx'], bn)
    inp, _rej = build_inputs(dict(ven['bingx'], hbars={}), bn, pairs)
    if inp is None:
        return [], len(pairs)
    lab2sym = {p[0]: p[1] for p in pairs}
    traded = set()
    for T in THRESHOLDS:
        res = simulate_x1(inp, T)
        traded |= {t['sym'] for t in res['trades']} | set(res['open_syms'])
    return sorted((lab, lab2sym[lab]) for lab in traded if lab in lab2sym), len(pairs)


async def load_traded_hourly(params, progress=None, exchanges=None, now_ms=None, sleep=asyncio.sleep):
    """[v121] 1h BingX для символов сделок предварительного прогона X1 (по одному символу, сразу в журнал; продолжение с места остановки). → строка отчёта."""
    xdir = params.get('xdir') or X.xdata_dir()
    bdir = params.get('bdir') or B.battery_dir()
    fee = float(params.get('fee_taker', FEE_DEFAULT))
    traded, npairs = prelim_traded(xdir, bdir, fee)
    if not traded:
        return f'  1h BingX: символов сделок в предварительном прогоне нет (пар {npairs})'
    now_ms = int(now_ms or time.time() * 1000)
    own = exchanges is None
    if own:
        import ccxt.async_support as ccxt
        exchanges = {'bingx': ccxt.bingx({'options': {'defaultType': 'swap'}, 'enableRateLimit': True})}
    try:
        idx = X.load_index(xdir)
        fx = {'bingx': X.Fetcher(exchanges['bingx'], sleep=sleep)}
        dn, em, er, lf, stp = await X.load_hourly(xdir, idx, fx, 'bingx', [s_ for _l, s_ in traded], now_ms, time.time() + float(params.get('max_minutes', 150)) * 60, progress)
        X.save_index(xdir, idx)
    finally:
        if own:
            import access_probe
            for c in exchanges.values():
                await access_probe.close_ccxt(c)
    msg = (f'  1h BingX по символам сделок предварительного прогона: загружено {dn} из {len(traded)}, осталось {lf}; подтверждённо пусто (дневной запасной базис): {em}; ошибка биржи: {er}'
           + (' — ⏱ бюджет времени исчерпан, повторите команду' if stp else ''))
    errs = X.error_lines(X.load_index(xdir), 'h1', [('bingx', s_) for _l, s_ in traded])               # [v125] что именно ответила биржа
    if errs:
        msg += '\n' + '\n'.join(errs) + '\n  (постоянные ошибки не перезапрашиваются 7 суток; повторить сейчас: /xfund_test load retry)'
    return msg


async def run_xfund(params, progress=None):
    """`/xfund_test [load]`: load — загрузка данных; иначе симуляция X1 по T ∈ {0.02%, 0.05%} (+ справочно Binance-пары), критерии §2.48.3."""
    xdir = params.get('xdir') or X.xdata_dir()
    bdir = params.get('bdir') or B.battery_dir()
    fdir = params.get('fdir') or RF.FD.factory_dir()
    fee = float(params.get('fee_taker', FEE_DEFAULT))
    main_binance = params.get('x1_pair', X.x1_pair_from_spec()) == 'binance'     # [v118] маркер X1-MAIN-PAIR в BOT_SPEC (§2.50)
    if params.get('mode') == 'load':
        pre = ''
        if params.get('retry'):                                     # [v125] `/xfund_test load retry`: сброс отрицательных записей-ОШИБОК (не «пусто») — символы запрашиваются снова
            n_clr = X.clear_neg_errors(xdir, X.compact_index(xdir))
            pre = f'↺ сброшено отрицательных записей-ошибок: {n_clr} (запрашиваются снова; «подтверждённо пусто» не сбрасывается)\n'
        txt = await X.run_xload(dict(params, x1_main=main_binance, venues=params.get('venues') or (('bingx',) if main_binance else None)), progress)
        if main_binance:
            traded, _np = prelim_traded(xdir, bdir, fee)                      # 1h — только по символам сделок (Binance из архива, BingX через ccxt)
            txt += '\n' + await sync_binance_h1(dict(params, only_norms=[lab for lab, _s in traded]), progress, params.get('b_fetch'))
            txt += '\n' + await load_traded_hourly(params, progress, params.get('ccxt_exchanges'))
        return pre + txt
    ven = load_venues(xdir, bdir, fee)
    nameB = 'Binance' if main_binance else 'Bybit'
    if main_binance:
        if not ven['bingx']['bars']:
            return '⛔ /xfund_test отказ: нет данных BingX — выполните /xfund_test load до сообщения «загрузка завершена»'
        bn = binance_venue(bdir, {X.norm_base(sym.split('/')[0]) for sym in ven['bingx']['bars']} | {v.get('norm') for v in ven['bingx']['markets'].values() if v.get('norm')}, FEE_DEFAULT, with_h1=True)
        if not bn['bars']:
            return '⛔ /xfund_test отказ: нет данных Binance (архив /battery_load) для баз BingX'
        ven['binance'] = bn
        pairs = main_pairs_binance(ven['bingx'], bn)
    else:
        if not ven['bingx']['bars'] or not ven['bybit']['bars']:
            return '⛔ /xfund_test отказ: нет данных Bybit/BingX — выполните /xfund_test load до сообщения «загрузка завершена»'
        pairs = common_pairs(ven)
    cov = None
    if main_binance and float(params.get('coverage_min', X.COVERAGE_MIN)) > 0:
        traded, _np = prelim_traded(xdir, bdir, fee)
        cov = X.coverage_report(xdir, 'x1', bdir, hourly_syms=[('bingx', s_) for _l, s_ in traded], minimum=float(params.get('coverage_min', X.COVERAGE_MIN)))
        if not cov['ok']:
            return ('⛔ /xfund_test отказ: данные неполны: ' + '; '.join(cov['lines']) + '. Расчёта и вердикта нет (BOT_SPEC §2.53): выполните /xfund_test load (повторяйте до «✅ загрузка завершена»).')
    venB = ven['binance'] if main_binance else ven['bybit']
    inp, rej = build_inputs(ven['bingx'], venB, pairs)
    R = [f'🔗 <b>/xfund_test — межбиржевой арбитраж фандинга X1 (BOT_SPEC §2.48{", основная пара BingX/Binance — §2.50" if main_binance else ""})</b> | код {params.get("version", "")}; ордеров нет',
         f'комиссии: BingX FEE_TAKER {fee * 100:.4f}%, {nameB} {FEE_DEFAULT * 100:.2f}% (по умолчанию), проскальзывание {SLIP * 100:.2f}% на исполнение; плечо {LEV:g}x, позиций ≤ {MAX_POS}',
         f'Общих баз BingX∩{nameB}: {len(pairs)}; принято пар (корреляция ≥ {CORR_MIN}): {0 if inp is None else len(inp["labels"])}; отклонено: {len(rej)}']
    if inp is None:
        return '\n'.join(R + ['Нет пар для симуляции.'])
    ints = {}
    for tag, ix in (('BingX', 0), (nameB, 1)):
        vals = [interval_hours(inp['fund'][j][ix][0]) for j in range(len(inp['labels'])) if inp['fund'][j][ix] is not None]
        ints[tag] = float(np.median([v for v in vals if v])) if vals else None
    nh = len(ven['bingx']['hbars']) + len(venB['hbars'])
    R.append(f'1h-свечи: символов с часовыми рядами BingX {len(ven["bingx"]["hbars"])}, {nameB} {len(venB["hbars"])}' + (' — НЕТ часовых данных: весь базис по дневному close (запасной вариант)' if not nh else ''))
    if cov is not None and cov.get('hourly') is not None:                                        # [v124, §2.53.2] 1h по символам сделок — показатель, не гейт (ошибка загрузки — гейт)
        R.append(X.hourly_line(cov['hourly']))
    R.append(f'Интервалы начисления (медиана по парам, ч): BingX {fm(ints["BingX"], 1)}, {nameB} {fm(ints[nameB], 1)}; ставки приводятся к 8 ч (сумма за 72 ч ÷ 9)' + ('; ставки Binance — из архива fundingRate по фактическим отметкам времени' if main_binance else ''))
    first = int(np.argmax(inp['elig'].any(axis=1))) if inp['elig'].any() else 0
    R.append(f'Допущенных суток-пар (ликвидность ≥ 20 млн на обеих биржах и ≥ 2 события): {int(inp["elig"].sum())}; период {datetime.fromtimestamp((inp["day0"] + first) * DAY / 1000, timezone.utc):%Y-%m-%d} … {datetime.fromtimestamp((inp["day0"] + inp["nd"] - 1) * DAY / 1000, timezone.utc):%Y-%m-%d}')
    best, passed = None, False
    for T in THRESHOLDS:
        res = simulate_x1(inp, T)
        m = metrics(res, inp)
        c, ok = criteria(m)
        R += [''] + fmt_metrics(f'BingX/{nameB}', T, m, c, ok) + fmt_split(res, inp)
        top = top_symbols(res, inp)
        if top:
            R.append('  топ-10 символов по числу сделок (ёмкость K_max = 5% × мин. медианный дневной оборот двух бирж): ' + '; '.join(f'{s} n={n}, K_max {cap / 1e3:.0f}k$' if cap and math.isfinite(cap) else f'{s} n={n}' for s, n, cap in top))
        passed = passed or ok
        if ok and (best is None or m['sharpe'] > best[1]):
            best = (T, m['sharpe'])
    R += ['', '<b>Решение (§2.48.3 + уточнение п. 8):</b> ' + ('направление проходит критерии хотя бы при одном T → шаг 2.5 (/exec_check x1, ≤ 7 дней)' if passed else 'критерии НЕ выполнены ни при одном T → направление закрыто')]
    RF.kv_set(KV_PASSED, bool(passed), fdir)
    # справочно: пары с Binance
    norms = {p[0] for p in pairs}
    bn = {'bars': {}} if main_binance else binance_venue(bdir, norms, fee)      # [v118] при основной паре с Binance справочный блок не нужен
    if main_binance:
        R += ['', 'Пара с Bybit — справочно, если Bybit станет доступен (§2.50); живое исполнение пары с Binance требует сервера вне США (Binance 451) — решается только при прохождении теста.']
    if bn['bars']:
        R += ['', '<b>Справочно (не критерий): пары с Binance (архив v108)</b>']
        for other, ven_o in (('Bybit', ven['bybit']), ('BingX', ven['bingx'])):
            pr = []
            for norm in sorted(bn['bars']):
                syms = [s for s in ven_o['bars'] if (ven_o['markets'].get(s, {}).get('norm') or '') == norm]
                if syms:
                    pr.append((norm, norm, max(syms, key=lambda s: float(np.mean(ven_o['bars'][s]['qv'])))))
            i2, rej2 = build_inputs(bn, ven_o, pr)
            if i2 is None:
                R.append(f'  Binance/{other}: пар нет')
                continue
            for T in THRESHOLDS:
                m = metrics(simulate_x1(i2, T), i2)
                R.append(f'  Binance/{other}, T = {T * 100:.2f}%: пар {len(i2["labels"])}, APR {m["apr"] * 100:+.2f}%, Шарп {fm(m["sharpe"])}, просадка {m["dd"] * 100:.1f}%, сделок {m["n_trades"]}, ликвидаций {m["liq"]}')
    RF.kv_set(KV_REPORT, '\n'.join(R), fdir)
    return '\n'.join(R)


def scan_list(xdir, limit=40):
    """Список [(метка, символ BingX, символ Bybit)] для почасового /exec_check x1: базы с историей фандинга на обеих биржах, по убыванию минимального 30-дневного оборота (кэш XDATA_DIR)."""
    idx = X.load_index(xdir)
    nm = {ex: {} for ex in X.EXCHANGES}
    for ex in X.EXCHANGES:
        for sym in idx['fund'].get(ex, {}):
            if _non_crypto(idx['markets'].get(ex, {}).get(sym), sym):
                continue
            nm[ex].setdefault(idx['markets'].get(ex, {}).get(sym, {}).get('norm') or X.norm_base(sym.split('/')[0]), sym)
    out = []
    for norm in sorted(set(nm['bingx']) & set(nm['bybit'])):
        sa, sb = nm['bingx'][norm], nm['bybit'][norm]
        va, vb = X.load_bars(xdir, 'bingx', sa), X.load_bars(xdir, 'bybit', sb)
        if va is None or vb is None:
            continue
        out.append((min(float(np.mean(va['qv'][-30:])), float(np.mean(vb['qv'][-30:]))), norm, sa, sb))
    out.sort(reverse=True)
    return [(n, a, b) for _v, n, a, b in out[:limit]]
