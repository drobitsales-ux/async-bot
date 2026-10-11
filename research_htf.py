"""research_htf.py — исторический бэктест трендследования на старших ТФ (BTC, ETH, SOL; 1D/4h).

ТОЛЬКО ИССЛЕДОВАНИЕ: модуль не торгует, не пишет в таблицы торговли и не импортирует async_bot (получает биржу
и параметры аргументами). Логика бота и живые позиции не затрагиваются.

ПРЕ-КОММИТ (BOT_SPEC «Смена вектора: старшие ТФ», зафиксирован ДО запуска; параметры НЕ подбираются):
  T1 «Donchian-55» (1D), T2 «TSMOM-28» (1D), T3 «MTF-pullback» (1D-направление + 4h-вход) — константы ниже.
  Критерии системы (OOS, все три символа вместе): n ≥ 30; mean R net > 0 при t ≥ 2.4; PF net ≥ 1.3;
  OOS mean R ≥ 0.5 × IS mean R; OOS mean R > 0 минимум у 2 из 3 символов. n < 30 → «недостаточно данных».
Соглашения (без заглядывания вперёд):
  * сигнал считается по ЗАКРЫТОМУ бару i, вход — строго по open бара i+1;
  * ATR = среднее True Range за n последних баров (как calc_atr бота), EMA — рекурсивная (alpha = 2/(n+1)), с прогревом;
  * внутри бара SL проверяется раньше TP (консервативно); гэп через стоп — исполнение по open;
  * издержки: вход и выход — FEE_TAKER каждая сторона + проскальзывание SLIP каждая сторона (аддитивно, в долях
    цены входа) + фандинг по каждому 8ч-интервалу удержания (лонг платит положительный, шорт получает);
  * R = pnl_net / начальная дистанция SL (в долях цены входа); незавершённые на конец данных сделки не учитываются.
"""
import asyncio
import bisect
import math
import os
import sqlite3
import time
from datetime import datetime, timezone

import numpy as np

# ── ПРЕ-КОММИТ: параметры систем (не менять до решения) ──────────────────────────────
SYMBOLS = ('BTC/USDT:USDT', 'ETH/USDT:USDT', 'SOL/USDT:USDT')
T1_N_ENTRY, T1_N_EXIT, T1_ATR_N, T1_SL_ATR = 55, 20, 20, 2.0
T2_LOOKBACK, T2_ATR_N, T2_SL_ATR = 28, 20, 2.0
T3_EMA_D, T3_SLOPE_D, T3_EMA_4H, T3_ATR_N, T3_SL_ATR, T3_TP_R, T3_TIMEOUT = 50, 5, 20, 14, 0.5, 2.0, 30
SLIP = 0.0002                    # проскальзывание на сторону, доля цены
IS_END = '2024-12-31'            # IS: от начала данных до этой даты включительно
OOS_START = '2025-01-01'         # OOS: с этой даты до конца данных
# критерии 2.5
CRIT_MIN_N, CRIT_T, CRIT_PF, CRIT_OOS_IS_RATIO, CRIT_MIN_SYMBOLS = 30, 2.4, 1.3, 0.5, 2
RISKS = (0.0025, 0.005)          # риск на сделку для кривой эквити
BOOT_RUNS, BOOT_BLOCK = 10_000, 5
BOOT_RISKS = (0.0025, 0.005, 0.01)
DAY_MS = 86_400_000
TF_MS = {'1d': DAY_MS, '4h': 4 * 3600_000}
FUNDING_MS = 8 * 3600_000


def prop_params() -> dict:
    """Параметры проп-челленджа из ENV; пока владелец их не задал — ПРИМЕРНЫЕ 10/5/10/static/30/0."""
    g = os.getenv
    given = any(g(k) is not None for k in ('PROP_TARGET_PCT', 'PROP_DAILY_DD_PCT', 'PROP_MAX_DD_PCT',
                                            'PROP_MAX_DD_MODE', 'PROP_TIME_LIMIT_DAYS', 'PROP_MIN_DAYS'))
    return {
        'target': float(g('PROP_TARGET_PCT', '10')) / 100, 'daily': float(g('PROP_DAILY_DD_PCT', '5')) / 100,
        'maxdd': float(g('PROP_MAX_DD_PCT', '10')) / 100, 'mode': g('PROP_MAX_DD_MODE', 'static'),
        'limit': int(float(g('PROP_TIME_LIMIT_DAYS', '30'))), 'min_days': int(float(g('PROP_MIN_DAYS', '0'))),
        'example': not given,
    }


# ── индикаторы ────────────────────────────────────────────────────────────────────
def atr_series(h, l, c, n):
    """ATR[i] = среднее True Range за n баров, заканчивающихся баром i (включительно); NaN до прогрева."""
    h, l, c = np.asarray(h, float), np.asarray(l, float), np.asarray(c, float)
    tr = np.empty(len(c)); tr[0] = h[0] - l[0]
    tr[1:] = np.maximum(h[1:] - l[1:], np.maximum(np.abs(h[1:] - c[:-1]), np.abs(l[1:] - c[:-1])))
    out = np.full(len(c), np.nan)
    if len(c) >= n:
        cs = np.cumsum(tr)
        out[n - 1:] = (cs[n - 1:] - np.r_[0.0, cs[:-n]]) / n
    return out


def ema_series(x, n):
    x = np.asarray(x, float); a = 2 / (n + 1)
    out = np.empty(len(x))
    if len(x) == 0:
        return out
    out[0] = x[0]
    for i in range(1, len(x)):
        out[i] = a * x[i] + (1 - a) * out[i - 1]
    return out


def _roll_prev(x, n, fn):
    """out[i] = fn(x[i-n:i]) — окно ПРЕДЫДУЩИХ n баров без текущего; NaN, если баров меньше n."""
    out = np.full(len(x), np.nan)
    for i in range(n, len(x)):
        out[i] = fn(x[i - n:i])
    return out


# ── фандинг и издержки ───────────────────────────────────────────────────────────────
def funding_sum(funding, entry_ts, exit_ts) -> float:
    """Сумма ставок фандинга за события с timestamp в [entry_ts, exit_ts). funding — отсортированный список (ts, rate)."""
    if not funding:
        return 0.0
    ts = [f[0] for f in funding]
    i, j = bisect.bisect_left(ts, entry_ts), bisect.bisect_left(ts, exit_ts)
    return float(sum(f[1] for f in funding[i:j]))


def make_trade(side, entry_ts, exit_ts, entry_px, exit_px, sl, funding, fee, slip, reason='', sys_name='', sym=''):
    """Сделка в R. pnl_frac = side·(exit−entry)/entry − 2·fee − 2·slip − side·Σфандинг; R = pnl_frac / sl_frac."""
    slf = abs(entry_px - sl) / entry_px
    move = side * (exit_px - entry_px) / entry_px
    cost = 2 * fee + 2 * slip
    fund = side * funding_sum(funding, entry_ts, exit_ts)
    pnl = move - cost - fund
    return {'sys': sys_name, 'sym': sym, 'side': side, 'entry_ts': int(entry_ts), 'exit_ts': int(exit_ts),
            'entry_px': float(entry_px), 'exit_px': float(exit_px), 'sl': float(sl), 'sl_frac': slf, 'reason': reason,
            'R': pnl / slf, 'R_gross': move / slf, 'R_cost': cost / slf, 'R_fund': fund / slf,
            'days': max(0.0, (exit_ts - entry_ts) / DAY_MS)}


def _stop_fill(side, open_px, sl):
    """Исполнение стопа в баре: гэп через стоп — по open, иначе по уровню."""
    return min(open_px, sl) if side == 1 else max(open_px, sl)


# ── системы ────────────────────────────────────────────────────────────────────────
def run_t1(bars, funding, fee, slip, sym=''):
    """T1 «Donchian-55» (1D). SL = цена входа ∓ 2·ATR20(сигнального бара); цена входа известна только на open j."""
    ts = [int(b[0]) for b in bars]
    o = np.array([b[1] for b in bars], float); h = np.array([b[2] for b in bars], float)
    l = np.array([b[3] for b in bars], float); c = np.array([b[4] for b in bars], float)
    n = len(c)
    atr = atr_series(h, l, c, T1_ATR_N)
    hi_e, lo_e = _roll_prev(h, T1_N_ENTRY, np.max), _roll_prev(l, T1_N_ENTRY, np.min)
    hi_x, lo_x = _roll_prev(h, T1_N_EXIT, np.max), _roll_prev(l, T1_N_EXIT, np.min)
    trades, pos, pend_entry, pend_exit = [], None, None, False
    for j in range(n):
        if pos and pend_exit:
            trades.append(make_trade(pos['side'], pos['ts'], ts[j], pos['px'], o[j], pos['sl'], funding, fee, slip, 'channel', 'T1', sym))
            pos, pend_exit = None, False
        if pend_entry is not None and pos is None:
            side, a = pend_entry
            sl = o[j] - side * T1_SL_ATR * a
            if sl > 0:
                pos = {'side': side, 'ts': ts[j], 'px': o[j], 'sl': sl}
        pend_entry = None
        if pos:
            s = pos['side']
            if (s == 1 and l[j] <= pos['sl']) or (s == -1 and h[j] >= pos['sl']):
                trades.append(make_trade(s, pos['ts'], ts[j], pos['px'], _stop_fill(s, o[j], pos['sl']), pos['sl'], funding, fee, slip, 'sl', 'T1', sym))
                pos, pend_exit = None, False
        if j >= n - 1:
            break
        if pos and not math.isnan(lo_x[j]):
            s = pos['side']
            if (s == 1 and c[j] < lo_x[j]) or (s == -1 and c[j] > hi_x[j]):
                pend_exit = True
        if (pos is None or pend_exit) and not math.isnan(hi_e[j]) and not math.isnan(atr[j]) and atr[j] > 0:
            if c[j] > hi_e[j]:
                pend_entry = (1, atr[j])
            elif c[j] < lo_e[j]:
                pend_entry = (-1, atr[j])
    return trades


def run_t2(bars, funding, fee, slip, sym=''):
    """T2 «TSMOM-28» (1D): по понедельникам 00:00 UTC позиция = sign(c[i−1]/c[i−1−28] − 1); SL = 2·ATR20 от входа."""
    ts = [int(b[0]) for b in bars]
    o = np.array([b[1] for b in bars], float); h = np.array([b[2] for b in bars], float)
    l = np.array([b[3] for b in bars], float); c = np.array([b[4] for b in bars], float)
    n = len(c)
    atr = atr_series(h, l, c, T2_ATR_N)
    trades, pos = [], None
    for j in range(T2_LOOKBACK + 2, n):
        dt = datetime.fromtimestamp(ts[j] / 1000, timezone.utc)
        if dt.weekday() == 0 and dt.hour == 0:                       # понедельник: решение по закрытию воскресенья (j−1)
            mom = c[j - 1] / c[j - 1 - T2_LOOKBACK] - 1
            target = 1 if mom > 0 else (-1 if mom < 0 else 0)
            a = atr[j - 1]
            if pos and (target != pos['side']):
                trades.append(make_trade(pos['side'], pos['ts'], ts[j], pos['px'], o[j], pos['sl'], funding, fee, slip, 'flip' if target else 'flat', 'T2', sym))
                pos = None
            if pos is None and target != 0 and not math.isnan(a) and a > 0:
                sl = o[j] - target * T2_SL_ATR * a
                if sl > 0:
                    pos = {'side': target, 'ts': ts[j], 'px': o[j], 'sl': sl}
        if pos:
            s = pos['side']
            if (s == 1 and l[j] <= pos['sl']) or (s == -1 and h[j] >= pos['sl']):
                trades.append(make_trade(s, pos['ts'], ts[j], pos['px'], _stop_fill(s, o[j], pos['sl']), pos['sl'], funding, fee, slip, 'sl', 'T2', sym))
                pos = None
    return trades


def run_t3(bars4h, bars1d, funding, fee, slip, sym=''):
    """T3 «MTF-pullback»: направление по 1D (close > EMA50 и EMA50 выше, чем 5 дней назад), вход по 4h-откату к EMA20."""
    d_ts = [int(b[0]) for b in bars1d]
    d_c = np.array([b[4] for b in bars1d], float)
    ema_d = ema_series(d_c, T3_EMA_D)
    dir_d = np.zeros(len(d_c), int)
    for d in range(3 * T3_EMA_D, len(d_c)):                           # прогрев EMA
        if d_c[d] > ema_d[d] and ema_d[d] > ema_d[d - T3_SLOPE_D]:
            dir_d[d] = 1
        elif d_c[d] < ema_d[d] and ema_d[d] < ema_d[d - T3_SLOPE_D]:
            dir_d[d] = -1
    d_close_ts = [t + DAY_MS for t in d_ts]
    ts = [int(b[0]) for b in bars4h]
    o = np.array([b[1] for b in bars4h], float); h = np.array([b[2] for b in bars4h], float)
    l = np.array([b[3] for b in bars4h], float); c = np.array([b[4] for b in bars4h], float)
    n = len(c)
    ema = ema_series(c, T3_EMA_4H); atr = atr_series(h, l, c, T3_ATR_N)
    trades, pos, pend = [], None, None
    for k in range(n):
        if pend is not None and pos is None:
            side, sl, ek = pend
            if (side == 1 and o[k] > sl) or (side == -1 and o[k] < sl):
                R = abs(o[k] - sl)
                pos = {'side': side, 'ts': ts[k], 'px': o[k], 'sl': sl, 'tp': o[k] + side * T3_TP_R * R, 'k0': k}
        pend = None
        if pos:
            s = pos['side']; done = None
            if (s == 1 and l[k] <= pos['sl']) or (s == -1 and h[k] >= pos['sl']):
                done = (_stop_fill(s, o[k], pos['sl']), 'sl')
            elif (s == 1 and h[k] >= pos['tp']) or (s == -1 and l[k] <= pos['tp']):
                done = ((max(o[k], pos['tp']) if s == 1 else min(o[k], pos['tp'])), 'tp')
            elif k - pos['k0'] + 1 >= T3_TIMEOUT:
                done = (c[k], 'timeout')
            if done:
                trades.append(make_trade(s, pos['ts'], ts[k] + TF_MS['4h'] if done[1] == 'timeout' else ts[k], pos['px'], done[0], pos['sl'], funding, fee, slip, done[1], 'T3', sym))
                pos = None
        if k >= n - 1 or pos is not None or k < 3 * T3_EMA_4H or math.isnan(atr[k]) or atr[k] <= 0:
            continue
        di = bisect.bisect_right(d_close_ts, ts[k] + TF_MS['4h']) - 1          # последний ЗАКРЫТЫЙ 1D-бар на закрытии 4h-бара k
        if di < 0:
            continue
        dr = dir_d[di]
        if dr == 1 and l[k] <= ema[k] and c[k] > ema[k]:
            sl = float(np.min(l[max(0, k - 2):k + 1])) - 0.5 * atr[k]
            pend = (1, sl, k)
        elif dr == -1 and h[k] >= ema[k] and c[k] < ema[k]:
            sl = float(np.max(h[max(0, k - 2):k + 1])) + 0.5 * atr[k]
            pend = (-1, sl, k)
    return trades


# ── статистика ──────────────────────────────────────────────────────────────────────
def split_period(trades):
    """IS/OOS по времени входа (UTC)."""
    cut = int(datetime.strptime(OOS_START, '%Y-%m-%d').replace(tzinfo=timezone.utc).timestamp() * 1000)
    return [t for t in trades if t['entry_ts'] < cut], [t for t in trades if t['entry_ts'] >= cut]


def trade_stats(trades) -> dict:
    n = len(trades)
    if n == 0:
        return {'n': 0, 'wr': 0.0, 'pf': 0.0, 'mean_r': 0.0, 't': 0.0, 'days': 0.0, 'cost_r': 0.0, 'fund_r': 0.0, 'streak': 0}
    rs = [t['R'] for t in trades]
    gw = sum(r for r in rs if r > 0); gl = -sum(r for r in rs if r < 0)
    pf = (gw / gl) if gl > 0 else (float('inf') if gw > 0 else 0.0)
    m = sum(rs) / n
    t = 0.0
    if n >= 2:
        sd = math.sqrt(sum((r - m) ** 2 for r in rs) / (n - 1))
        t = m / (sd / math.sqrt(n)) if sd > 0 else 0.0
    streak = cur = 0
    for tr in sorted(trades, key=lambda x: x['exit_ts']):
        cur = cur + 1 if tr['R'] <= 0 else 0
        streak = max(streak, cur)
    return {'n': n, 'wr': sum(1 for r in rs if r > 0) / n * 100, 'pf': pf, 'mean_r': m, 't': t,
            'days': sum(t_['days'] for t_ in trades) / n, 'cost_r': sum(t_['R_cost'] for t_ in trades) / n,
            'fund_r': sum(t_['R_fund'] for t_ in trades) / n, 'streak': streak}


def verdict(trades_by_sym: dict) -> dict:
    """Критерии 2.5 по OOS (все символы вместе)."""
    allt = [t for ts_ in trades_by_sym.values() for t in ts_]
    is_t, oos_t = split_period(allt)
    si, so = trade_stats(is_t), trade_stats(oos_t)
    per_sym = {}
    for sym, ts_ in trades_by_sym.items():
        _i, o_ = split_period(ts_)
        per_sym[sym] = trade_stats(o_)
    pos_syms = sum(1 for s in per_sym.values() if s['n'] > 0 and s['mean_r'] > 0)
    if so['n'] < CRIT_MIN_N:
        return {'status': 'NODATA', 'text': f'недостаточно данных (OOS n={so["n"]} &lt; {CRIT_MIN_N})', 'is': si, 'oos': so, 'per_sym': per_sym,
                'checks': {}}
    checks = {
        f'n≥{CRIT_MIN_N}': so['n'] >= CRIT_MIN_N,
        f'mean R&gt;0 при t≥{CRIT_T}': so['mean_r'] > 0 and so['t'] >= CRIT_T,
        f'PF net≥{CRIT_PF}': so['pf'] >= CRIT_PF,
        f'OOS mean R ≥ {CRIT_OOS_IS_RATIO}×IS': so['mean_r'] >= CRIT_OOS_IS_RATIO * si['mean_r'],
        f'OOS mean R&gt;0 у ≥{CRIT_MIN_SYMBOLS} из 3 символов': pos_syms >= CRIT_MIN_SYMBOLS,
    }
    ok = all(checks.values())
    return {'status': 'PASS' if ok else 'FAIL', 'text': 'ПРОЙДЕНО' if ok else 'не пройдено', 'is': si, 'oos': so, 'per_sym': per_sym, 'checks': checks}


# ── эквити при фиксированном риске, дневная переоценка ───────────────────────────────────
def trade_daily_path(t, d_close_by_day):
    """Дневные приращения R сделки по переоценке на close дня: {day_ordinal: ΔR}. Издержки: половина (fee+slip)
    на день входа, вторая половина и фандинг — на день выхода; на день выхода — итоговый R."""
    d0 = int(t['entry_ts'] // DAY_MS); d1 = int(t['exit_ts'] // DAY_MS)
    if t['exit_ts'] % DAY_MS == 0 and d1 > d0:
        d1 -= 1                                  # выход ровно в 00:00 следующего дня относим к предыдущему дню
    side, slf, e = t['side'], t['sl_frac'], t['entry_px']
    entry_cost = (t['R_cost'] / 2)
    cum_prev, out = 0.0, {}
    for d in range(d0, d1 + 1):
        if d == d1:
            cum = t['R']
        else:
            px = d_close_by_day.get(d)
            cum = (side * (px - e) / e / slf if px else cum_prev) - entry_cost
        out[d] = cum - cum_prev
        cum_prev = cum
    return out


def equity_stats(trades, d_close_maps, risk, start_ord=None, end_ord=None):
    """Эквити по портфелю: eq_d = eq_{d−1}·(1 + risk·ΔR_d). → {max_dd, worst_day, pos_months, n_months, total_ret}."""
    daily = {}
    for t in trades:
        for d, dr in trade_daily_path(t, d_close_maps[t['sym']]).items():
            daily[d] = daily.get(d, 0.0) + dr
    if not daily:
        return {'max_dd': 0.0, 'worst_day': 0.0, 'pos_months': 0, 'n_months': 0, 'total_ret': 0.0}
    eq, peak, maxdd, worst = 1.0, 1.0, 0.0, 0.0
    month_ret = {}
    for d in range(min(daily), max(daily) + 1):
        r = risk * daily.get(d, 0.0)
        worst = min(worst, r)
        eq_prev = eq; eq *= (1 + r)
        peak = max(peak, eq); maxdd = max(maxdd, (peak - eq) / peak)
        dt = datetime.fromordinal(d + 719163)           # day since 1970-01-01 → ordinal
        key = (dt.year, dt.month)
        month_ret[key] = month_ret.get(key, 1.0) * (eq / eq_prev)
    pos = sum(1 for v in month_ret.values() if v > 1.0)
    return {'max_dd': maxdd, 'worst_day': worst, 'pos_months': pos, 'n_months': len(month_ret), 'total_ret': eq - 1.0}


# ── бутстрап проп-челленджа ───────────────────────────────────────────────────────────────
def trade_features(t, d_close_by_day):
    """Для бутстрапа: итог R, экстремумы накопленного пути и порядок, худший дневной ΔR, длительность в днях."""
    path = trade_daily_path(t, d_close_by_day)
    cum, cs, daily = 0.0, [], []
    for d in sorted(path):
        cum += path[d]; cs.append(cum); daily.append(path[d])
    imin = int(np.argmin(cs)); imax = int(np.argmax(cs))
    return {'R': t['R'], 'min_c': min(0.0, min(cs)), 'max_c': max(0.0, max(cs)), 'min_day': min(daily),
            'max_first': imax < imin, 'days': len(cs)}


def prop_bootstrap(feats, risk, params, runs=BOOT_RUNS, block=BOOT_BLOCK, seed=12345, max_steps=None):
    """Блочный бутстрап последовательностей сделок. Проверки (в % от начального баланса, аддитивно):
    дневной — по худшему дневному ΔR сделки × риск; общий DD — внутри сделки по экстремумам пути (static: от
    начального баланса; trailing: от пика); цель — по итогу сделки (закрытая прибыль) и при days ≥ min_days;
    лимит времени — по суммарным дням удержания (простаи между сделками не считаются). Сделки идут последовательно.
    → {pass, fail_maxdd, fail_daily, fail_time, unfinished, median_days}."""
    n = len(feats)
    out = {'pass': 0, 'fail_maxdd': 0, 'fail_daily': 0, 'fail_time': 0, 'unfinished': 0, 'median_days': None}
    if n == 0:
        return out
    rng = np.random.default_rng(seed)
    max_steps = max_steps or max(500, 5 * n)
    days_to_target = []
    for _ in range(runs):
        eq, peak, days, steps, res = 1.0, 1.0, 0, 0, None
        while res is None and steps < max_steps:
            s = int(rng.integers(0, n))
            for f in (feats[(s + k) % n] for k in range(block)):
                steps += 1
                if risk * f['min_day'] <= -params['daily']:
                    res = 'fail_daily'; break
                lo = eq + risk * f['min_c']
                hi = eq + risk * f['max_c']
                pk = max(peak, hi) if f['max_first'] else peak
                floor = (1 - params['maxdd']) if params['mode'] == 'static' else pk * (1 - params['maxdd'])
                if lo <= floor:
                    res = 'fail_maxdd'; break
                eq += risk * f['R']; peak = max(peak, eq, hi if f['max_first'] else peak); days += f['days']
                if params['limit'] and days > params['limit']:       # цель, достигнутая позже лимита, не засчитывается
                    res = 'fail_time'; break
                if eq >= 1 + params['target'] and days >= params['min_days']:
                    res = 'pass'; days_to_target.append(days); break
        if res is None:
            res = 'unfinished'
        out[res] += 1
    out['median_days'] = float(np.median(days_to_target)) if days_to_target else None
    out['p90_days'] = float(np.percentile(days_to_target, 90)) if days_to_target else None
    for k in ('pass', 'fail_maxdd', 'fail_daily', 'fail_time', 'unfinished'):
        out['p_' + k] = out[k] / runs
    return out


# ── данные: загрузка с кэшем в SQLite ───────────────────────────────────────────────────
def init_cache(db_path):
    con = sqlite3.connect(db_path)
    con.execute("CREATE TABLE IF NOT EXISTS htf_reports (id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT, version TEXT, text TEXT)")
    con.execute("CREATE TABLE IF NOT EXISTS htf_bars (symbol TEXT, tf TEXT, ts INTEGER, o REAL, h REAL, l REAL, c REAL, v REAL, PRIMARY KEY (symbol, tf, ts))")
    con.execute("CREATE TABLE IF NOT EXISTS htf_funding (symbol TEXT, ts INTEGER, rate REAL, PRIMARY KEY (symbol, ts))")
    con.commit(); con.close()


def save_report(db_path, version, text) -> int:
    """Полный отчёт бэктеста — в БД (htf_reports) ДО отправки. → id записи."""
    init_cache(db_path)
    con = sqlite3.connect(db_path)
    cur = con.execute("INSERT INTO htf_reports (ts, version, text) VALUES (?,?,?)", (datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'), version, text))
    con.commit(); rid = cur.lastrowid; con.close()
    return rid


def last_report(db_path):
    """Последний сохранённый отчёт → (id, ts, version, text) или None."""
    init_cache(db_path)
    con = sqlite3.connect(db_path)
    row = con.execute("SELECT id, ts, version, text FROM htf_reports ORDER BY id DESC LIMIT 1").fetchone()
    con.close()
    return row


def cache_bars(db_path, sym, tf):
    con = sqlite3.connect(db_path)
    rows = con.execute("SELECT ts,o,h,l,c,v FROM htf_bars WHERE symbol=? AND tf=? ORDER BY ts", (sym, tf)).fetchall()
    con.close()
    return [list(r) for r in rows]


def cache_funding(db_path, sym):
    con = sqlite3.connect(db_path)
    rows = con.execute("SELECT ts,rate FROM htf_funding WHERE symbol=? ORDER BY ts", (sym,)).fetchall()
    con.close()
    return [(r[0], r[1]) for r in rows]


# ── устойчивая загрузка: повторы, паузы, частичные сбои ─────────────────────────────────────
RETRY_DELAYS = (2, 4, 8, 16, 32, 60)     # паузы между попытками, с; попыток всего len(RETRY_DELAYS) (после последней пауза не нужна)
MAX_ATTEMPTS = len(RETRY_DELAYS)
IS_START = '2024-01-01'                  # IS короче года (данные начинаются позже) → вердикт по символу невозможен
_sleep = asyncio.sleep                   # подменяется в тестах


def fetch_pause() -> float:
    """HTF_FETCH_PAUSE_SEC (ENV, по умолчанию 2.0 с) — минимум между историческими запросами, поверх throttler'а ccxt."""
    try:
        return max(0.0, float(os.getenv('HTF_FETCH_PAUSE_SEC', '2.0')))
    except ValueError:
        return 2.0


def is_retryable(e) -> bool:
    """Повторяем: ExchangeError с кодом 100410 («повторите позже»), ошибки сети, RateLimitExceeded. Остальное — без повторов."""
    try:
        import ccxt as ce                                # классы ошибок — из пакета верхнего уровня (ccxt.base.errors как модуль не импортируем: в новых ccxt имя ccxt.base перекрыто)
        if isinstance(e, ce.NetworkError):               # RateLimitExceeded ⊂ DDoSProtection ⊂ NetworkError; RequestTimeout, ExchangeNotAvailable
            return True
        if isinstance(e, ce.ExchangeError) and '100410' in str(e):
            return True
    except ImportError:
        pass
    return isinstance(e, (asyncio.TimeoutError, ConnectionError))


class HtfFetchError(Exception):
    def __init__(self, label, attempts, last):
        self.label, self.attempts, self.last = label, attempts, last
        super().__init__(f'{label}: {self.short()}')

    def short(self):
        m = str(self.last)
        code = '100410' if '100410' in m else type(self.last).__name__
        return f'{code} после {self.attempts} попыток' if self.attempts > 1 else f'{code}'


async def call_with_retry(label, since, fn, info):
    """Один исторический запрос: до MAX_ATTEMPTS попыток с паузами RETRY_DELAYS; пауза после запроса ≥ HTF_FETCH_PAUSE_SEC.
    Каждая попытка пишется в лог и в info['fetch_log']. Исчерпание → HtfFetchError (наружу — только он)."""
    import logging
    last = None
    for k in range(1, MAX_ATTEMPTS + 1):
        info['requests'] = info.get('requests', 0) + 1
        try:
            res = await fn()
            n = len(res or [])
            info.setdefault('fetch_log', []).append({'label': label, 'since': since, 'n': n, 'attempt': k, 'error': ''})
            logging.info(f'[HTF] {label} since={since} получено {n} попытка {k}')
            await _sleep(fetch_pause())
            return res
        except Exception as e:
            if not is_retryable(e):
                info.setdefault('fetch_log', []).append({'label': label, 'since': since, 'n': 0, 'attempt': k, 'error': f'{type(e).__name__}: {e}'[:200]})
                logging.warning(f'[HTF] {label} since={since} попытка {k}: {type(e).__name__}: {str(e)[:150]} (без повторов)')
                raise HtfFetchError(label, k, e)
            last = e
            info.setdefault('fetch_log', []).append({'label': label, 'since': since, 'n': 0, 'attempt': k, 'error': f'{type(e).__name__}: {e}'[:200]})
            logging.warning(f'[HTF] {label} since={since} попытка {k}/{MAX_ATTEMPTS}: {type(e).__name__}: {str(e)[:150]}')
            if k < MAX_ATTEMPTS:
                await _sleep(max(RETRY_DELAYS[k - 1], fetch_pause()))
    raise HtfFetchError(label, MAX_ATTEMPTS, last)


async def _fetch_paged(fetch, since_ms, now_ms, step_ms, on_batch=None, max_iter=400):
    """Пагинация с since. on_batch(batch) вызывается после КАЖДОЙ страницы (сохранение в кэш по мере загрузки)."""
    out, cur, it = [], since_ms, 0
    while cur < now_ms and it < max_iter:
        it += 1
        batch = await fetch(cur)
        batch = [b for b in (batch or []) if b[0] >= cur]
        if not batch:
            cur += step_ms * 500                      # пустое окно (до листинга) — прыгаем вперёд
            continue
        if on_batch:
            on_batch(batch)
        out.extend(batch)
        nxt = batch[-1][0] + step_ms
        if nxt <= cur:
            break
        cur = nxt
        if len(batch) < 2:
            break
    return out


def _date(ms):
    return f'{datetime.fromtimestamp(ms / 1000, timezone.utc):%Y-%m-%d}'


async def deepen_funding(exchange, db_path, sym, since_ms, info, max_pages=60):
    """[v88] Попытка получить фандинг ГЛУБЖЕ последних ~1000 событий: пагинация назад через endTime (сырой параметр BingX
    quote/fundingRate; ccxt 4.2.14 'until' кладёт в startTime, поэтому endTime передаётся напрямую). Остановка: страница
    без более ранних записей (API игнорирует endTime или история закончилась), достигнут since, ошибка после повторов.
    Результат — info['funding_depth'][sym] = {'earliest': мс, 'status': ...}. Критерии не затрагивает."""
    depth = info.setdefault('funding_depth', {})
    fund = cache_funding(db_path, sym)
    if not fund:
        depth[sym] = {'earliest': None, 'status': 'нет данных'}
        return
    earliest = fund[0][0]
    status = 'достигнут предел страниц'
    for _ in range(max_pages):
        if earliest <= since_ms:
            status = 'дошли до начала запрошенного периода'; break
        end = earliest - 1

        async def go(end=end):
            r = await exchange.fetch_funding_rate_history(sym, since=since_ms, limit=1000, params={'endTime': end})
            return [[int(x['timestamp']), float(x.get('fundingRate') or 0.0)] for x in (r or []) if x.get('timestamp') is not None]
        try:
            batch = await call_with_retry(f'{sym} funding<{_date(end)}', since_ms, go, info)
        except HtfFetchError as e:
            status = f'ошибка: {e.short()}'; break
        older = [b for b in batch if b[0] < earliest]
        if not older:
            status = 'API не отдаёт более раннюю историю (endTime не дал новых записей)'; break
        con = sqlite3.connect(db_path)
        con.executemany("INSERT OR REPLACE INTO htf_funding VALUES (?,?,?)", [(sym, int(a), float(b)) for a, b in older])
        con.commit(); con.close()
        earliest = min(b[0] for b in older)
    depth[sym] = {'earliest': earliest, 'status': status}


async def load_data(exchange, db_path, symbols=SYMBOLS, now_ms=None, since_ms=None, progress=None, tfs=('1d', '4h'), deep_funding=True):
    """Догружает кэш и возвращает {sym: {'1d': bars, '4h': bars, 'funding': [...]}} + служебную информацию.
    Только ЗАКРЫТЫЕ бары. Сбой набора (символ×ТФ или фандинг символа) после всех повторов НЕ прерывает загрузку:
    info['failed'][(sym, tf)] / info['funding_failed'][sym]; такой набор в данных пуст (фандинг = 0, «фандинг не учтён»).
    Уже загруженное страницами пишется в кэш сразу — повторный запуск продолжает с места обрыва."""
    init_cache(db_path)
    now_ms = now_ms or int(time.time() * 1000)
    since_ms = since_ms if since_ms is not None else int(datetime(2019, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
    data = {}
    info = {'funding_ok': True, 'funding_note': '', 'requests': 0, 'failed': {}, 'funding_failed': {}, 'fetch_log': []}
    has_fund = bool(getattr(exchange, 'has', {}).get('fetchFundingRateHistory', False))

    async def say(msg):
        if progress:
            try:
                await progress(msg)
            except Exception:
                pass

    for sym in symbols:
        short = sym.split('/')[0]
        entry = {'1d': [], '4h': [], 'funding': []}
        for tf in tfs:
            have = cache_bars(db_path, sym, tf)
            start = (have[-1][0] + TF_MS[tf]) if have else since_ms

            async def f(cur, sym=sym, tf=tf):
                return await call_with_retry(f'{sym} {tf}', cur, lambda: exchange.fetch_ohlcv(sym, tf, since=cur, limit=1000), info)

            def save(batch, sym=sym, tf=tf):
                rows = [(sym, tf, int(b[0]), b[1], b[2], b[3], b[4], b[5]) for b in batch if b[0] + TF_MS[tf] <= now_ms]   # незакрытый бар не берём
                if rows:
                    con = sqlite3.connect(db_path)
                    con.executemany("INSERT OR REPLACE INTO htf_bars VALUES (?,?,?,?,?,?,?,?)", rows)
                    con.commit(); con.close()
            try:
                await _fetch_paged(f, start, now_ms, TF_MS[tf], on_batch=save)
                entry[tf] = cache_bars(db_path, sym, tf)
                await say(f'{short} {tf.upper()}: {len(entry[tf])} баров с {_date(entry[tf][0][0])}' if entry[tf] else f'{short} {tf.upper()}: баров нет')
            except HtfFetchError as e:
                info['failed'][(sym, tf)] = e.short()
                await say(f'{short} {tf.upper()}: не загружен ({e.short()})')
        # фандинг
        fund, why = [], ''
        if not has_fund:
            why = 'биржа не поддерживает fetch_funding_rate_history'
        else:
            fund = cache_funding(db_path, sym)
            start = (fund[-1][0] + 1) if fund else since_ms

            async def ff(cur, sym=sym):
                async def go():
                    r = await exchange.fetch_funding_rate_history(sym, since=cur, limit=1000)
                    return [[int(x['timestamp']), float(x.get('fundingRate') or 0.0)] for x in (r or []) if x.get('timestamp') is not None]
                return await call_with_retry(f'{sym} funding', cur, go, info)

            def savef(batch, sym=sym):
                con = sqlite3.connect(db_path)
                con.executemany("INSERT OR REPLACE INTO htf_funding VALUES (?,?,?)", [(sym, int(a), float(b)) for a, b in batch])
                con.commit(); con.close()
            try:
                await _fetch_paged(ff, start, now_ms, FUNDING_MS, on_batch=savef)
                fund = cache_funding(db_path, sym)
                if not fund:
                    why = 'история фандинга пуста'
                elif deep_funding:
                    await deepen_funding(exchange, db_path, sym, since_ms, info)
                    fund = cache_funding(db_path, sym)
            except HtfFetchError as e:
                why = e.short(); fund = []
        if why:
            info['funding_ok'] = False
            info['funding_failed'][sym] = why
            info['funding_note'] = '; '.join(f'{k.split("/")[0]}: {v}' for k, v in info['funding_failed'].items())
            await say(f'{short} funding: не загружен ({why})')
        else:
            await say(f'{short} funding: {len(fund)} событий с {_date(fund[0][0])}')
        entry['funding'] = fund
        data[sym] = entry
    return data, info


def history_flags(data, info) -> dict:
    """{(sym, tf): 'ok' | 'not_loaded' | 'short_is'}; short_is — данные начинаются позже IS_START (IS короче года)."""
    cut = int(datetime.strptime(IS_START, '%Y-%m-%d').replace(tzinfo=timezone.utc).timestamp() * 1000)
    out = {}
    for sym, d in data.items():
        for tf in ('1d', '4h'):
            if (sym, tf) in info.get('failed', {}) or not d[tf]:
                out[(sym, tf)] = 'not_loaded'
            elif d[tf][0][0] > cut:
                out[(sym, tf)] = 'short_is'
            else:
                out[(sym, tf)] = 'ok'
    return out


SYS_TFS = {'T1': ('1d',), 'T2': ('1d',), 'T3': ('1d', '4h')}


def system_blockers(key, flags) -> list:
    """Что мешает вердикту системы: не загруженные наборы и слишком короткий IS (критерии §2.28.4 не меняются)."""
    out = []
    for (sym, tf), st in flags.items():
        if tf in SYS_TFS[key] and st != 'ok':
            out.append(f'{sym.split("/")[0]} {tf.upper()}: ' + ('не загружен' if st == 'not_loaded' else 'IS слишком короткий, вердикт по символу невозможен'))
    return out


# ── прогон и отчёт ────────────────────────────────────────────────────────────────────
def run_all(data, fee, slip=SLIP):
    """→ {sys: {sym: trades}} для T1/T2/T3."""
    res = {'T1': {}, 'T2': {}, 'T3': {}}
    for sym, d in data.items():
        have1, have4 = len(d['1d']) > 0, len(d['4h']) > 0
        res['T1'][sym] = run_t1(d['1d'], d['funding'], fee, slip, sym) if have1 else []
        res['T2'][sym] = run_t2(d['1d'], d['funding'], fee, slip, sym) if have1 else []
        res['T3'][sym] = run_t3(d['4h'], d['1d'], d['funding'], fee, slip, sym) if (have1 and have4) else []
    return res


SYS_NAMES = {'T1': 'T1 Donchian-55 (1D)', 'T2': 'T2 TSMOM-28 (1D, по понедельникам)', 'T3': 'T3 MTF-pullback (1D направление + 4h вход)'}


def _fpf(v):
    return 'inf' if v == float('inf') else f'{v:.2f}'


def _line(label, s):
    if s['n'] == 0:
        return f'  {label}: n=0'
    return (f'  {label}: n={s["n"]} | WR {s["wr"]:.0f}% | PF net {_fpf(s["pf"])} | mean R net {s["mean_r"]:+.3f} | t {s["t"]:.2f} | '
            f'ср. длительность {s["days"]:.1f} дн | издержки {s["cost_r"]:.3f}R + фандинг {s["fund_r"]:+.3f}R на сделку | макс. серия убытков {s["streak"]}')


def build_report(data, info, results, fee, boot=None, params=None) -> str:
    params = params or prop_params()
    L = ['📈 <b>HTF-бэктест: трендследование на старших ТФ (BTC, ETH, SOL)</b>',
         f'Издержки: FEE_TAKER={fee} на сторону + проскальзывание {SLIP} на сторону; вход по open следующего бара; '
         f'IS до {IS_END}, OOS с {OOS_START}. Параметры систем фиксированы пре-коммитом (BOT_SPEC «Смена вектора»).',
         ('⚠️ ФАНДИНГ НЕ УЧТЁН (фандинг = 0): ' + info.get('funding_note', '')) if not info.get('funding_ok', True)
         else 'Фандинг: учтён по каждому 8ч-интервалу удержания (лонг платит положительный, шорт получает).',
         'Глубина истории:']
    for sym, d in data.items():
        def span(b):
            return (f'{datetime.fromtimestamp(b[0][0] / 1000, timezone.utc):%Y-%m-%d}…{datetime.fromtimestamp(b[-1][0] / 1000, timezone.utc):%Y-%m-%d} ({len(b)})') if b else 'нет данных'
        L.append(f'  {sym}: 1D {span(d["1d"])} | 4h {span(d["4h"])} | фандинг: {len(d["funding"])} событий')
    L.append('Фандинг (глубина и оценка вклада по доступному периоду, на примере T2):')
    L.extend(funding_coverage_text(data, results.get('T2', {}), info))
    flags = history_flags(data, info)
    for (sym, tf), r in info.get('failed', {}).items():
        L.append(f'⚠️ {sym.split("/")[0]} {tf.upper()}: НЕ ЗАГРУЖЕН ({r}) — набор исключён из расчёта; повторный запуск продолжит с кэша')
    for (sym, tf), st in flags.items():
        if st == 'short_is':
            L.append(f'⚠️ {sym.split("/")[0]} {tf.upper()}: данные начинаются {_date(data[sym][tf][0][0])} (позже {IS_START}) — IS слишком короткий, вердикт по символу невозможен')
    d_maps = {sym: {int(b[0] // DAY_MS): b[4] for b in d['1d']} for sym, d in data.items()}
    verdicts = {}
    for key in ('T1', 'T2', 'T3'):
        by_sym = results[key]
        allt = [t for ts_ in by_sym.values() for t in ts_]
        is_t, oos_t = split_period(allt)
        L.append(f'\n<b>{SYS_NAMES[key]}</b>')
        for per, tr in (('IS', is_t), ('OOS', oos_t)):
            L.append(f' {per}:')
            L.append(_line('все символы', trade_stats(tr)))
            for sym in data:
                L.append(_line(sym.split('/')[0], trade_stats([t for t in tr if t['sym'] == sym])))
            for r in RISKS:
                e = equity_stats(tr, d_maps, r)
                L.append(f'    эквити при риске {r * 100:.2f}%: макс. просадка от пика {e["max_dd"] * 100:.1f}% | худший день {e["worst_day"] * 100:.2f}% | '
                         f'положительных месяцев {e["pos_months"]} из {e["n_months"]} | итог {e["total_ret"] * 100:+.1f}%')
        v = verdict(by_sym); verdicts[key] = v
        blockers = system_blockers(key, flags)
        if blockers:                                       # неполные данные: вердикт по §2.28.4 невозможен (критерии не менялись)
            v['status'] = 'INCOMPLETE'
            v['text'] = 'вердикт невозможен — неполные данные (' + '; '.join(blockers) + ')'
            v['checks'] = {}
        L.append(' Критерии (OOS, все символы): ' + (' | '.join(f'{k} {"✅" if ok else "❌"}' for k, ok in v['checks'].items()) if v['checks'] else '—'))
        L.append(f' <b>ВЕРДИКТ {key}: {v["text"]}</b> (OOS mean R {v["oos"]["mean_r"]:+.3f}, IS mean R {v["is"]["mean_r"]:+.3f})')
    if boot:
        L.append('\n<b>Симуляция проп-челленджа (бутстрап OOS-сделок, блоки по 5, ' + str(BOOT_RUNS) + ' прогонов)</b>')
        L.append(('⚠️ ПРИМЕРНЫЕ параметры, не правила конкретной фирмы: ' if params['example'] else 'Параметры из ENV: ')
                 + f'цель {params["target"] * 100:g}%, дневной DD {params["daily"] * 100:g}%, общий DD {params["maxdd"] * 100:g}% ({params["mode"]}), '
                 f'лимит {params["limit"]} дн (0 = нет), мин. дней {params["min_days"]}')
        for key, rows in boot.items():
            for r, b in rows.items():
                if b['median_days'] is None and b['pass'] == 0 and sum(b[k] for k in ('fail_maxdd', 'fail_daily', 'fail_time', 'unfinished')) == 0:
                    continue
                L.append(f' {key}, риск {r * 100:.2f}%: P(пройти) {b["p_pass"] * 100:.1f}% | P(провал по общему DD) {b["p_fail_maxdd"] * 100:.1f}% | '
                         f'P(провал по дневному) {b["p_fail_daily"] * 100:.1f}% | P(не успеть по времени) {b["p_fail_time"] * 100:.1f}% | '
                         f'не завершён {b["p_unfinished"] * 100:.1f}% | медиана дней до цели {("%.0f" % b["median_days"]) if b["median_days"] is not None else "—"}')
    L.append('\n<b>Дальнейшие шаги (пре-коммит):</b> система, прошедшая критерии, добавляется в бот как shadow-стратегия с ТЕМИ ЖЕ правилами '
             '(форвард 30 дней или 10 сделок — что позже); живая торговля/проп — только после форварда на риске 0.25–0.5% с учётом минимального '
             'notional. Не прошедшие системы НЕ докручиваются — параметры заново не подбираются.')
    return '\n'.join(L)


def compute(data, info, fee, slip=SLIP, runs=BOOT_RUNS):
    """Синхронная тяжёлая часть (запускать в потоке): прогон систем, статистика, бутстрап."""
    results = run_all(data, fee, slip)
    d_maps = {sym: {int(b[0] // DAY_MS): b[4] for b in d['1d']} for sym, d in data.items()}
    boot, params = {}, prop_params()
    for key in ('T1', 'T2', 'T3'):
        allt = [t for ts_ in results[key].values() for t in ts_]
        _i, oos = split_period(allt)
        st = trade_stats(oos)
        if st['n'] > 0 and st['mean_r'] > 0 and not system_blockers(key, history_flags(data, info)):
            feats = [trade_features(t, d_maps[t['sym']]) for t in sorted(oos, key=lambda x: x['entry_ts'])]
            boot[key] = {r: prop_bootstrap(feats, r, params, runs=runs) for r in BOOT_RISKS}
    return build_report(data, info, results, fee, boot, params), results, boot


async def run_backtest(exchange, db_path, fee, progress=None, runs=BOOT_RUNS):
    """Полный прогон: загрузка (async, через общий экземпляр биржи) + расчёт в потоке (цикл бота не блокируется)."""
    data, info = await load_data(exchange, db_path, progress=progress)
    text, results, boot = await asyncio.to_thread(compute, data, info, fee, SLIP, runs)
    return text, info, results, boot


# ══ [v88] H-T2W: T2 (правила §2.28 БЕЗ изменений) на широкой корзине USDT-перпетуалов BingX ══════════════════════
# ПРЕ-КОММИТ — BOT_SPEC §2.30 (записан ДО запуска). Вселенная формируется ПРАВИЛОМ, без ручного списка.
STABLE_BASES = frozenset({'USDT', 'USDC', 'DAI', 'TUSD', 'FDUSD', 'BUSD', 'USDE', 'USDD', 'PYUSD', 'USDP', 'GUSD', 'LUSD', 'SUSD',
                          'USD1', 'USDS', 'FRAX', 'UST', 'USTC', 'USDX', 'EUR', 'EURC', 'EURT', 'EURS', 'GBP', 'TRY', 'AUD', 'BRL',
                          'RUB', 'JPY', 'CNH', 'CHF', 'CAD'})
WIDE_TOP_N = 30                    # топ-N по 30-дневному объёму
WIDE_CAND_N = 60                   # предотбор по 24ч-объёму (тикеры одним запросом) → 30-дневный объём считается по 1D-барам
WIDE_HIST_MAX = '2023-01-01'       # 1D-история должна начинаться не позже
WIDE_CRIT_MIN_N = 30               # OOS: недельных значений и сделок не меньше (иначе «недостаточно данных»)
BOOT_BLOCK_WEEKS = 4
MAIN3 = frozenset(SYMBOLS)


def portf_risk() -> float:
    """PORTF_RISK_PCT (ENV, по умолчанию 1.0%) → доля. Суммарный риск на неделю, делится поровну между открытыми позициями."""
    try:
        return max(0.0, float(os.getenv('PORTF_RISK_PCT', '1.0'))) / 100
    except ValueError:
        return 0.01


def is_stable_base(base) -> bool:
    return str(base or '').upper() in STABLE_BASES


def _ms(date_str):
    return int(datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc).timestamp() * 1000)


async def select_universe(exchange, info, progress=None, now_ms=None, top_n=WIDE_TOP_N, cand_n=WIDE_CAND_N):
    """Вселенная правилом 1.2: USDT-перпетуалы (swap, quote=USDT, активные), без стейблкоинов и фиат-пар; предотбор cand_n
    по 24ч-объёму (fetch_tickers, 1 запрос), затем 30-дневный объём = Σ close·volume по 30 последним ЗАКРЫТЫМ 1D-барам;
    топ-top_n. → [{'sym','vol30','vol24'}] по убыванию vol30. Ручного списка нет."""
    now_ms = now_ms or int(time.time() * 1000)
    await call_with_retry('markets', 0, lambda: exchange.load_markets(), info)
    perp = {s: m for s, m in (exchange.markets or {}).items()
            if m.get('swap') and m.get('quote') == 'USDT' and m.get('active', True) is not False and not is_stable_base(m.get('base'))}
    tick = await call_with_retry('tickers', 0, lambda: exchange.fetch_tickers(list(perp)), info) or {}
    rows = []
    for s in perp:
        t = tick.get(s) or {}
        v = t.get('quoteVolume')
        if v is None and t.get('baseVolume') and t.get('last'):
            v = float(t['baseVolume']) * float(t['last'])
        rows.append((float(v or 0.0), s))
    rows.sort(reverse=True)
    cands = [s for v, s in rows[:cand_n] if v > 0]
    vol24 = {s: v for v, s in rows}
    out = []
    for s in cands:
        try:
            bars = await call_with_retry(f'{s} vol30', 0, lambda s=s: exchange.fetch_ohlcv(s, '1d', limit=35), info)
        except HtfFetchError:
            info.setdefault('universe_skipped', []).append(s)
            continue
        closed = [b for b in (bars or []) if b[0] + DAY_MS <= now_ms][-30:]
        if len(closed) < 20:
            info.setdefault('universe_skipped', []).append(s)
            continue
        out.append({'sym': s, 'vol30': float(sum(b[4] * b[5] for b in closed)), 'vol24': vol24.get(s, 0.0)})
    out.sort(key=lambda r: -r['vol30'])
    top = out[:top_n]
    if progress:
        try:
            await progress(f'Вселенная H-T2W: топ-{len(top)} по 30-дн. объёму из {len(out)} кандидатов')
        except Exception:
            pass
    return top


def filter_history(data, info, hist_max=WIDE_HIST_MAX):
    """Правило 1.2: 1D-история начинается не позже hist_max. → (kept[list sym], excluded[{sym, reason}])."""
    cut = _ms(hist_max)
    kept, excluded = [], []
    for sym, d in data.items():
        if (sym, '1d') in info.get('failed', {}) or not d['1d']:
            excluded.append({'sym': sym, 'reason': 'не загружен'})
        elif d['1d'][0][0] > cut:
            excluded.append({'sym': sym, 'reason': f'история с {_date(d["1d"][0][0])}'})
        else:
            kept.append(sym)
    return kept, excluded


def week_of_day(d):
    """Номер недели (Пн–Вс) по порядковому дню от 1970-01-01 (четверг): (d+3)//7."""
    return (d + 3) // 7


def week_start_ms(w):
    return (w * 7 - 3) * DAY_MS


def portfolio_series(trades, d_close_maps):
    """Недельный портфель T2: для каждой недели k_w = число символов, у которых в неделе была позиция; дневная доходность
    портфеля в R = Σ_sym ΔR_{sym,d} / k_w; недельное значение W_w = Σ_d (среднее R по активным символам). Дневной ΔR — по переоценке
    на close дня (trade_daily_path; издержки/фандинг — как в сделках), Σ ΔR по сделке = итоговый R сделки.
    → {'daily': {d: R_d}, 'weekly': {w: W_w}, 'k': {w: k_w}, 'sym_days': {d: {sym: ΔR}}}."""
    sd = {}
    for t in trades:
        for d, dr in trade_daily_path(t, d_close_maps[t['sym']]).items():
            sd.setdefault(d, {}); sd[d][t['sym']] = sd[d].get(t['sym'], 0.0) + dr
    wk_syms = {}
    for d, m in sd.items():
        wk_syms.setdefault(week_of_day(d), set()).update(m)
    k = {w: len(v) for w, v in wk_syms.items()}
    daily = {d: sum(m.values()) / k[week_of_day(d)] for d, m in sd.items()}
    weekly = {}
    for d, r in daily.items():
        w = week_of_day(d); weekly[w] = weekly.get(w, 0.0) + r
    return {'daily': daily, 'weekly': weekly, 'k': k, 'sym_days': sd}


def portfolio_week_risks(trades, d_close_maps, risk):
    """Риск на позицию по неделям: {w: {sym: risk/k_w}} — суммарный риск открытых позиций недели = risk."""
    ps = portfolio_series(trades, d_close_maps)
    out = {}
    for d, m in ps['sym_days'].items():
        w = week_of_day(d)
        for sym in m:
            out.setdefault(w, {})[sym] = risk / ps['k'][w]
    return out


def weekly_stats(vals) -> dict:
    """n, mean, t (стандартная ошибка по недельному ряду, ddof=1)."""
    n = len(vals)
    if n == 0:
        return {'n': 0, 'mean': 0.0, 't': 0.0}
    m = sum(vals) / n
    t = 0.0
    if n >= 2:
        sd = math.sqrt(sum((v - m) ** 2 for v in vals) / (n - 1))
        t = m / (sd / math.sqrt(n)) if sd > 0 else 0.0
    return {'n': n, 'mean': m, 't': t}


def equity_from_daily(ret_by_day, from_day=None) -> dict:
    """Эквити по дневным доходностям портфеля (доля): макс. просадка от пика, худший день, доля положительных месяцев, итог."""
    days = sorted(d for d in ret_by_day if from_day is None or d >= from_day)
    if not days:
        return {'max_dd': 0.0, 'worst_day': 0.0, 'pos_months': 0, 'n_months': 0, 'total_ret': 0.0}
    eq, peak, maxdd, worst, months = 1.0, 1.0, 0.0, 0.0, {}
    for d in range(days[0], days[-1] + 1):
        r = ret_by_day.get(d, 0.0)
        worst = min(worst, r)
        prev = eq; eq *= (1 + r)
        peak = max(peak, eq); maxdd = max(maxdd, (peak - eq) / peak)
        dt = datetime.fromordinal(d + 719163)
        months[(dt.year, dt.month)] = months.get((dt.year, dt.month), 1.0) * (eq / prev)
    return {'max_dd': maxdd, 'worst_day': worst, 'pos_months': sum(1 for v in months.values() if v > 1.0), 'n_months': len(months), 'total_ret': eq - 1.0}


def week_features(daily_ret, weeks):
    """Для бутстрапа по неделям: доходность недели (доля), экстремумы накопленного пути, худший день, порядок max/min. Недели без позиции — нули."""
    out = []
    for w in weeks:
        cum, cs, ds = 0.0, [], []
        for d in range(w * 7 - 3, w * 7 + 4):
            r = daily_ret.get(d, 0.0); cum += r; cs.append(cum); ds.append(r)
        imin, imax = int(np.argmin(cs)), int(np.argmax(cs))
        out.append({'R': cum, 'min_c': min(0.0, min(cs)), 'max_c': max(0.0, max(cs)), 'min_day': min(ds), 'max_first': imax < imin, 'days': 7})
    return out


def wide_verdict(trades_new: list, d_close_maps: dict) -> dict:
    """ГЛАВНЫЙ критерий H-T2W (§2.30.3) — только НОВЫЕ символы (без BTC/ETH/SOL). Значимость — по недельному портфелю:
    OOS недельный mean R > 0 при t ≥ 2.4; PF net по сделкам OOS ≥ 1.3; полный период — недельный mean R > 0;
    OOS mean R (по сделкам) > 0 минимум у половины символов. n(OOS недель) < 30 или n(OOS сделок) < 30 → «недостаточно данных»."""
    ps = portfolio_series(trades_new, d_close_maps)
    oos_w0 = week_of_day(_ms(OOS_START) // DAY_MS)           # первая неделя (Пн–Вс), начинающаяся не раньше OOS_START
    if week_start_ms(oos_w0) < _ms(OOS_START):
        oos_w0 += 1
    full_w = list(ps['weekly'].values())
    oos_w = [v for w, v in ps['weekly'].items() if w >= oos_w0]
    _is, oos_t = split_period(trades_new)
    st_oos, st_all = trade_stats(oos_t), trade_stats(trades_new)
    ws_oos, ws_full = weekly_stats(oos_w), weekly_stats(full_w)
    per_sym = {}
    for sym in sorted({t['sym'] for t in trades_new}):
        per_sym[sym] = {'all': trade_stats([t for t in trades_new if t['sym'] == sym]), 'oos': trade_stats([t for t in oos_t if t['sym'] == sym])}
    with_oos = [s for s, v in per_sym.items() if v['oos']['n'] > 0]
    pos = sum(1 for s in with_oos if per_sym[s]['oos']['mean_r'] > 0)
    res = {'ps': ps, 'oos_w0': oos_w0, 'ws_oos': ws_oos, 'ws_full': ws_full, 'st_oos': st_oos, 'st_all': st_all,
           'per_sym': per_sym, 'pos_syms': pos, 'n_syms_oos': len(with_oos)}
    if ws_oos['n'] < WIDE_CRIT_MIN_N or st_oos['n'] < WIDE_CRIT_MIN_N:
        res.update(status='NODATA', text=f'недостаточно данных (OOS недель {ws_oos["n"]}, сделок {st_oos["n"]}; нужно ≥ {WIDE_CRIT_MIN_N} и ≥ {WIDE_CRIT_MIN_N})', checks={})
        return res
    checks = {
        f'OOS недельный mean R&gt;0 при t≥{CRIT_T}': ws_oos['mean'] > 0 and ws_oos['t'] >= CRIT_T,
        f'PF net (сделки OOS)≥{CRIT_PF}': st_oos['pf'] >= CRIT_PF,
        'полный период: недельный mean R&gt;0': ws_full['mean'] > 0,
        'OOS mean R&gt;0 минимум у половины символов': len(with_oos) > 0 and 2 * pos >= len(with_oos),
    }
    ok = all(checks.values())
    res.update(status='PASS' if ok else 'FAIL', text='ПРОЙДЕНО' if ok else 'не пройдено', checks=checks)
    return res


def run_wide_trades(data, fee, slip=SLIP, syms=None):
    """T2 (правила §2.28 без изменений) по символам. → {sym: trades}."""
    return {sym: run_t2(data[sym]['1d'], data[sym]['funding'], fee, slip, sym) for sym in (syms or data) if len(data[sym]['1d']) > 0}


def funding_coverage_text(data, trades_by_sym, info) -> list:
    """«фандинг до ДАТЫ не учтён» + оценка вклада по доступному периоду: средний R фандинга на сделку (для сделок целиком после первой записи)."""
    L = []
    for sym, d in data.items():
        f = d.get('funding') or []
        dep = (info.get('funding_depth') or {}).get(sym, {})
        if not f:
            L.append(f'  {sym.split("/")[0]}: фандинга нет — не учтён за весь период')
            continue
        first = f[0][0]
        trs = [t for t in trades_by_sym.get(sym, []) if t['entry_ts'] >= first]
        est = (sum(t['R_fund'] for t in trs) / len(trs)) if trs else None
        L.append(f'  {sym.split("/")[0]}: фандинг с {_date(first)} ({len(f)} событий); до {_date(first)} не учтён (глубже: {dep.get("status", "не пробовали")}); '
                 + (f'средний вклад фандинга {est:+.3f}R на сделку по {len(trs)} сделкам с покрытием' if est is not None else 'сделок с покрытием нет'))
    return L


def build_wide_report(univ, kept, excluded, data, info, trades_by_sym, verd, boot, params, fee) -> str:
    risk = portf_risk()
    new_syms = [s for s in kept if s not in MAIN3]
    L = ['📈 <b>H-T2W: T2 TSMOM-28 на широкой корзине USDT-перпетуалов BingX</b>',
         f'Правила T2 — как в §2.28 (28 дней, понедельник 00:00 UTC, SL 2·ATR20, разворот при смене знака); издержки: FEE_TAKER={fee} + проскальзывание {SLIP} на сторону, '
         f'фандинг по 8ч-интервалам там, где он доступен. IS до {IS_END}, OOS с {OOS_START}. Пре-коммит — BOT_SPEC §2.30.',
         '⚠️ ОШИБКА ВЫЖИВШЕГО: вселенная отобрана по ТЕКУЩЕМУ объёму — монеты, которые выросли и остались ликвидными; результат на ней систематически оптимистичен.',
         f'Вселенная (правило: топ-{WIDE_TOP_N} по 30-дн. объёму, без стейблкоинов/фиат-пар, 1D-история не позже {WIDE_HIST_MAX}):']
    for r in univ:
        st = 'в тесте' if r['sym'] in kept else 'исключён: ' + next((e['reason'] for e in excluded if e['sym'] == r['sym']), '?')
        if r['sym'] in MAIN3 and r['sym'] in kept:
            st = 'BTC/ETH/SOL — вне главного критерия'
        L.append(f'  {r["sym"].split("/")[0]}: объём 30д ≈ {r["vol30"] / 1e6:,.0f}M USDT | {st}')
    for sym, r in info.get('failed', {}).items():
        L.append(f'⚠️ {sym[0].split("/")[0]} {sym[1].upper()}: НЕ ЗАГРУЖЕН ({r}) — символ исключён из расчёта')
    L.append(f'Символов в главном критерии (без BTC/ETH/SOL): {len(new_syms)}')
    L.append('\n<b>Главный критерий — НОВЫЕ символы, недельный портфель</b>')
    ws_o, ws_f = verd['ws_oos'], verd['ws_full']
    L.append(f'  Недельный портфель (среднее R по символам с позицией): полный период n={ws_f["n"]} недель, mean R net {ws_f["mean"]:+.4f}, t {ws_f["t"]:.2f}')
    L.append(f'  OOS: n={ws_o["n"]} недель, mean R net {ws_o["mean"]:+.4f}, t {ws_o["t"]:.2f}')
    L.append(_line('Сделки, весь период', verd['st_all']))
    L.append(_line('Сделки, OOS', verd['st_oos']))
    L.append(f'  Символов с OOS mean R&gt;0: {verd["pos_syms"]} из {verd["n_syms_oos"]}')
    for sym, v in verd['per_sym'].items():
        L.append(_line(f'{sym.split("/")[0]} OOS', v['oos']))
    L.append(' Критерии: ' + (' | '.join(f'{k} {"✅" if ok else "❌"}' for k, ok in verd['checks'].items()) if verd['checks'] else '—'))
    L.append(f' <b>ВЕРДИКТ H-T2W: {verd["text"]}</b>')
    ps = verd['ps']
    d_oos = max(_ms(OOS_START) // DAY_MS, 0)
    L.append(f'\n<b>Кривая эквити: суммарный риск {risk * 100:g}% на неделю, поровну между открытыми позициями (дневная переоценка)</b>')
    for label, fr in (('OOS', d_oos), ('полный период', None)):
        e = equity_from_daily({d: risk * r for d, r in ps['daily'].items()}, fr)
        L.append(f'  {label}: макс. просадка от пика {e["max_dd"] * 100:.1f}% | худший день {e["worst_day"] * 100:.2f}% | положительных месяцев {e["pos_months"]} из {e["n_months"]} | итог {e["total_ret"] * 100:+.1f}%')
    if boot:
        L.append(f'\n<b>Проп-бутстрап по недельной последовательности портфеля OOS (блоки по {BOOT_BLOCK_WEEKS} недели, {BOOT_RUNS} прогонов)</b>')
        L.append(('⚠️ ПРИМЕРНЫЕ параметры, не правила конкретной фирмы: ' if params['example'] else 'Параметры из ENV: ')
                 + f'цель {params["target"] * 100:g}%, дневной DD {params["daily"] * 100:g}%, общий DD {params["maxdd"] * 100:g}% ({params["mode"]}), мин. дней {params["min_days"]}')
        for label, b in boot.items():
            L.append(f' {label}: P(пройти) {b["p_pass"] * 100:.1f}% | P(провал по общему DD) {b["p_fail_maxdd"] * 100:.1f}% | P(провал по дневному) {b["p_fail_daily"] * 100:.1f}% | '
                     f'P(не успеть по времени) {b["p_fail_time"] * 100:.1f}% | не завершён {b["p_unfinished"] * 100:.1f}% | '
                     f'медиана дней до цели {("%.0f" % b["median_days"]) if b["median_days"] is not None else "—"} | 90-й перцентиль {("%.0f" % b["p90_days"]) if b.get("p90_days") is not None else "—"}')
    L.append('\n<b>Фандинг:</b>')
    L.extend(funding_coverage_text({s: data[s] for s in new_syms if s in data}, trades_by_sym, info)[:40])
    L.append('\nНе прошедшая гипотеза не докручивается (параметры и вселенная не подбираются заново).')
    return '\n'.join(L)


def compute_wide(univ, data, info, fee, slip=SLIP, runs=BOOT_RUNS):
    """Синхронная тяжёлая часть (в потоке): фильтр вселенной по истории, T2 по символам, недельный портфель, критерий, бутстрап."""
    kept, excluded = filter_history(data, info)
    trades_by_sym = run_wide_trades(data, fee, slip, kept)
    new_syms = [s for s in kept if s not in MAIN3]
    d_maps = {sym: {int(b[0] // DAY_MS): b[4] for b in data[sym]['1d']} for sym in kept}
    trades_new = [t for s in new_syms for t in trades_by_sym.get(s, [])]
    verd = wide_verdict(trades_new, d_maps)
    risk, params = portf_risk(), prop_params()
    boot = {}
    ps = verd['ps']
    if ps['weekly'] and verd['ws_oos']['n'] > 0 and verd['ws_oos']['mean'] > 0:
        weeks = list(range(verd['oos_w0'], max(ps['weekly']) + 1))
        feats = week_features({d: risk * r for d, r in ps['daily'].items()}, weeks)
        for label, lim in (('лимит 30 дн', 30), ('без лимита времени', 0)):
            p = dict(params); p['limit'] = lim
            boot[label] = prop_bootstrap(feats, 1.0, p, runs=runs, block=BOOT_BLOCK_WEEKS, max_steps=1500)
    return build_wide_report(univ, kept, excluded, data, info, trades_by_sym, verd, boot, params, fee), verd, boot


async def run_wide(exchange, db_path, fee, progress=None, runs=BOOT_RUNS):
    """/htf_backtest_wide: выбор вселенной правилом → загрузка 1D (+фандинг) с повторами/паузой/кэшем → расчёт в потоке."""
    sel_info = {'requests': 0, 'fetch_log': []}
    univ = await select_universe(exchange, sel_info, progress=progress)
    syms = tuple(r['sym'] for r in univ)
    data, info = await load_data(exchange, db_path, symbols=syms, progress=progress, tfs=('1d',))
    info['requests'] += sel_info['requests']; info['universe_skipped'] = sel_info.get('universe_skipped', [])
    text, verd, boot = await asyncio.to_thread(compute_wide, univ, data, info, fee, SLIP, runs)
    return text, info, verd, boot
