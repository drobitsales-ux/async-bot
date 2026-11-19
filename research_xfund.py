"""research_xfund.py — X1: межбиржевой арбитраж фандинга (BingX/Bybit), BOT_SPEC §2.48. Нейтральная к направлению пара ног (шорт на бирже с более высокой ставкой, лонг на другой); суточная
симуляция по дневным барам и событиям фандинга обеих бирж; ни одного реального ордера. Справочно — пары с Binance (архив v108)."""
import math
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


def build_inputs(venA, venB, pairs):
    """venX = {'bars': {sym: дневные}, 'hbars': {sym: часовые h/l/c} (необязательно), 'fund': {sym: (ts, rate)}, 'fee': доля}; pairs = [(метка, symA, symB)]. → inputs для simulate_x1: общая дневная сетка,
    массивы по парам, допуск (оба бара + ликвидность PIT ≥ 20 млн на КАЖДОЙ площадке + ≥ 2 события в окне на каждой), avg8 по площадкам, часовые ряды (если есть)."""
    keep, rejected = [], []
    for lab, sa, sb in pairs:
        a, b = venA['bars'].get(sa), venB['bars'].get(sb)
        if a is None or b is None:
            rejected.append((lab, 'нет данных'))
            continue
        ok, corr, n = pair_ok(a, b)
        (keep if ok else rejected).append((lab, sa, sb) if ok else (lab, f'корреляция {corr:.2f} на {n} сутках'))
    if not keep:
        return None, rejected
    day0 = min(int(min(venA['bars'][sa]['ts'][0], venB['bars'][sb]['ts'][0])) // DAY for _l, sa, sb in keep)
    day1 = max(int(max(venA['bars'][sa]['ts'][-1], venB['bars'][sb]['ts'][-1])) // DAY for _l, sa, sb in keep)
    nd, n = day1 - day0 + 1, len(keep)
    M = {k: np.full((nd, n), np.nan) for k in ('OA', 'HA', 'LA', 'CA', 'QA', 'OB', 'HB', 'LB', 'CB', 'QB')}
    avg = {'A': np.full((nd, n), np.nan), 'B': np.full((nd, n), np.nan)}
    hs = {'A': [], 'B': []}
    for j, (lab, sa, sb) in enumerate(keep):
        for tag, ven, sym in (('A', venA, sa), ('B', venB, sb)):
            x = ven['bars'][sym]
            di = (x['ts'] // DAY - day0).astype(np.int64)
            for k, kk in zip('OHLCQ', ('o', 'h', 'l', 'c', 'qv')):
                M[k + tag][di, j] = x[kk]
            avg[tag][:, j] = avg8_matrix(day0, nd, ven['fund'].get(sym))
            hs[tag].append(_hseries((ven.get('hbars') or {}).get(sym), day0))
    liq = {}
    for tag in 'AB':
        adv, _c = B.pit_adv(M['Q' + tag].astype(np.float32))
        liq[tag] = B.liquid_mask(adv)
    present = np.isfinite(M['OA']) & np.isfinite(M['CA']) & np.isfinite(M['OB']) & np.isfinite(M['CB'])
    elig = present & liq['A'] & liq['B'] & np.isfinite(avg['A']) & np.isfinite(avg['B'])
    return {'day0': day0, 'nd': nd, 'M': M, 'avg': avg, 'elig': elig, 'present': present, 'labels': [k[0] for k in keep], 'syms': [(k[1], k[2]) for k in keep], 'hs': hs,
            'fund': [(venA['fund'].get(sa), venB['fund'].get(sb)) for _l, sa, sb in keep], 'fees': (venA['fee'], venB['fee'])}, rejected


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
    cnt = {'liq': 0, 'emerg': 0, 'n_h': 0, 'n_fb': 0}
    TAGS = (('A', 0), ('B', 1))

    def accrue(p, t0, t1):
        for vi in (0, 1):
            short = p['short'] == vi
            for d_idx, amt in leg_funding(inp['fund'][p['j']][vi], t0, t1, short, c):
                k = d_idx - day0
                if 0 <= k < nd:
                    fund[k] += amt
                    pnl[k] += amt

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
                cost[d] += c * fills[vi]
                pnl[d] -= c * fills[vi]
                cnt['emerg'] += 1
            else:                                                             # вторая нога — по close бара своей биржи
                px, _t = bar_close(p, tag, d, b)
                day_pnl = d_dir * c * (px - p['ref'][vi]) / p['p0'][vi]
                cost[d] += c * fills[vi]
                pnl[d] -= c * fills[vi]
            _px, t_end = bar_close(p, tag, d, b)
            pnl[d] += day_pnl
            price[d] += day_pnl
        accrue(p, p['te'], t_end)
        trades.append({'sym': inp['labels'][p['j']], 'e': p['e'], 'x': d, 'reason': 'ликвидация (гэп)' if 'gap' in kinds else 'аварийное 30%', 'fb': p['fb']})
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
                pnl[d] += pdl
                price[d] += pdl
                cost[d] += c * fills[vi]
                pnl[d] -= c * fills[vi]
            accrue(p, p['te'], t_exit)
            trades.append({'sym': inp['labels'][j], 'e': p['e'], 'x': d, 'reason': reason, 'fb': p['fb']})
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
                pos.append({'j': j, 'short': sv, 'e': d, 'mode': mode, 'fb': fb, 'p0': p0, 'ref': [p0[0], p0[1]], 'legpnl': [0.0, 0.0], 'te': te})
                for vi in (0, 1):
                    cost[d] += c * fills[vi]
                    pnl[d] -= c * fills[vi]
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
                pnl[d] += day_pnl
                price[d] += day_pnl
    for p in pos:                                                                # открытые на конец данных: фандинг до конца, издержки выхода не платятся
        accrue(p, p['te'], (day0 + nd) * DAY)
    return {'pnl': pnl, 'fund': fund, 'price': price, 'cost': cost, 'trades': trades, 'liq': cnt['liq'], 'emerg': cnt['emerg'], 'n_h': cnt['n_h'], 'n_fb': cnt['n_fb'], 'open_end': len(pos)}


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


# ── данные и команда ────────────────────────────────────────────────────────────────────────────────
def load_venues(xdir, bdir, fee_taker):
    """Площадки: BingX, Bybit (кэш XDATA_DIR) и Binance (архив v108) — только для норм-баз, загруженных для X1."""
    idx = X.load_index(xdir)
    ven = {}
    for ex in X.EXCHANGES:
        bars, fund, hbars = {}, {}, {}
        for sym in idx['bars'].get(ex, {}):
            b = X.load_bars(xdir, ex, sym)
            if b is not None and len(b['ts']):
                bars[sym] = b
                f = X.load_fund(xdir, ex, sym)
                if f is not None:
                    fund[sym] = f
                hb = X.load_hbars(xdir, ex, sym)
                if hb is not None:
                    hbars[sym] = hb
        ven[ex] = {'bars': bars, 'fund': fund, 'hbars': hbars, 'fee': (fee_taker if ex == 'bingx' else FEE_DEFAULT), 'markets': idx['markets'].get(ex, {})}
    return ven


def common_pairs(ven, a='bingx', b='bybit'):
    na, nb = {}, {}
    for sym in ven[a]['bars']:
        na.setdefault(ven[a]['markets'].get(sym, {}).get('norm') or X.norm_base(sym.split('/')[0]), []).append(sym)
    for sym in ven[b]['bars']:
        nb.setdefault(ven[b]['markets'].get(sym, {}).get('norm') or X.norm_base(sym.split('/')[0]), []).append(sym)
    out = []
    for norm in sorted(set(na) & set(nb)):
        # несколько рынков одной базы: берём с большим оборотом (корреляция всё равно проверяется)
        sa = max(na[norm], key=lambda s: float(np.mean(ven[a]['bars'][s]['qv'])))
        sb = max(nb[norm], key=lambda s: float(np.mean(ven[b]['bars'][s]['qv'])))
        out.append((norm, sa, sb))
    return out


def binance_venue(bdir, norms, fee_taker):
    """Binance из архива: символы, чья нормализованная база среди norms; выбирается ряд с наибольшим числом суток."""
    idx = B.load_index(bdir)
    best = {}
    for s in idx.get('symbols', []):
        nb = X.norm_base(s[:-4] if s.endswith('USDT') else s)
        if nb not in norms:
            continue
        z = B.load_d1(bdir, s)
        if z is None or not len(z['ts']):
            continue
        if nb not in best or len(z['ts']) > len(best[nb][1]['ts']):
            best[nb] = (s, z)
    bars, fund = {}, {}
    for nb, (s, z) in best.items():
        bars[nb] = {'ts': z['ts'], 'o': z['o'], 'h': z['h'], 'l': z['l'], 'c': z['c'], 'qv': z['qv']}
        f = B.load_fund(bdir, s)
        if f is not None:
            fund[nb] = (f['ts'], f['rate'])
    return {'bars': bars, 'fund': fund, 'fee': fee_taker, 'markets': {}}


async def run_xfund(params, progress=None):
    """`/xfund_test [load]`: load — загрузка данных; иначе симуляция X1 по T ∈ {0.02%, 0.05%} (+ справочно Binance-пары), критерии §2.48.3."""
    xdir = params.get('xdir') or X.xdata_dir()
    bdir = params.get('bdir') or B.battery_dir()
    fdir = params.get('fdir') or RF.FD.factory_dir()
    fee = float(params.get('fee_taker', FEE_DEFAULT))
    if params.get('mode') == 'load':
        return await X.run_xload(params, progress)
    ven = load_venues(xdir, bdir, fee)
    if not ven['bingx']['bars'] or not ven['bybit']['bars']:
        return '⛔ /xfund_test отказ: нет данных Bybit/BingX — выполните /xfund_test load до сообщения «загрузка завершена»'
    pairs = common_pairs(ven)
    inp, rej = build_inputs(ven['bingx'], ven['bybit'], pairs)
    R = [f'🔗 <b>/xfund_test — межбиржевой арбитраж фандинга X1 (BOT_SPEC §2.48)</b> | код {params.get("version", "")}; ордеров нет',
         f'комиссии: BingX FEE_TAKER {fee * 100:.4f}%, Bybit {FEE_DEFAULT * 100:.2f}% (по умолчанию), проскальзывание {SLIP * 100:.2f}% на исполнение; плечо {LEV:g}x, позиций ≤ {MAX_POS}',
         f'Общих баз BingX∩Bybit: {len(pairs)}; принято пар (корреляция ≥ {CORR_MIN}): {0 if inp is None else len(inp["labels"])}; отклонено: {len(rej)}']
    if inp is None:
        return '\n'.join(R + ['Нет пар для симуляции.'])
    ints = {}
    for tag, vv, ix in (('BingX', ven['bingx'], 0), ('Bybit', ven['bybit'], 1)):
        vals = [interval_hours(inp['fund'][j][ix][0]) for j in range(len(inp['labels'])) if inp['fund'][j][ix] is not None]
        ints[tag] = float(np.median([v for v in vals if v])) if vals else None
    nh = sum(1 for vv in (ven['bingx'], ven['bybit']) for _ in vv['hbars'])
    R.append(f'1h-свечи: символов с часовыми рядами BingX {len(ven["bingx"]["hbars"])}, Bybit {len(ven["bybit"]["hbars"])}' + (' — НЕТ часовых данных: весь базис по дневному close (запасной вариант)' if not nh else ''))
    R.append(f'Интервалы начисления (медиана по парам, ч): BingX {fm(ints["BingX"], 1)}, Bybit {fm(ints["Bybit"], 1)}; ставки приводятся к 8 ч (сумма за 72 ч ÷ 9)')
    first = int(np.argmax(inp['elig'].any(axis=1))) if inp['elig'].any() else 0
    R.append(f'Допущенных суток-пар (ликвидность ≥ 20 млн на обеих биржах и ≥ 2 события): {int(inp["elig"].sum())}; период {datetime.fromtimestamp((inp["day0"] + first) * DAY / 1000, timezone.utc):%Y-%m-%d} … {datetime.fromtimestamp((inp["day0"] + inp["nd"] - 1) * DAY / 1000, timezone.utc):%Y-%m-%d}')
    best, passed = None, False
    for T in THRESHOLDS:
        res = simulate_x1(inp, T)
        m = metrics(res, inp)
        c, ok = criteria(m)
        R += [''] + fmt_metrics('BingX/Bybit', T, m, c, ok)
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
    bn = binance_venue(bdir, norms, fee)
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
