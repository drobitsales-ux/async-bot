"""research_carry.py — /carry_test: сбор фандинга (cash-and-carry) C1 СТРОГО по BOT_SPEC §2.40 (пре-коммит записан ДО кода). Только история; ничего не торгует.

C1: каждый понедельник 00:00 UTC — средний фандинг каждого символа за прошлые 7 дней; «лонг спот + шорт перп равного номинала» по топ-5 символам с положительным
средним; оставшиеся в топ-5 не переоткрываются; капитал = 5 равных слотов, перп 3× (EFF = 0.75), фандинг не реинвестируется. Доход — начисления фандинга по шорту;
издержки — спот 0.1% (FEE_SPOT) + перп FEE_TAKER + проскальзывание 0.0003 на сторону по обеим ногам, открытие и закрытие. Базис спот/перп не моделируется.
Без критериев «пройдено/не пройдено» — оценка для решения владельца."""
from datetime import datetime, timezone

import numpy as np
from numpy.lib.stride_tricks import sliding_window_view as swv

import research_vision as V

DAY_MS = 86_400_000
WEEK_MS = 7 * DAY_MS
SLOTS = 5
TOPN = 5
LEV = 3.0
EFF = 1.0 / (1.0 + 1.0 / LEV)          # N / (N + N/3) = 0.75
MIN_SETTLE = 6                          # начислений в окне 7 дней, иначе символ не участвует
SLIP = 0.0003
LIQ_RISE = 0.30
OOS_START = '2025-01-01'
C0_DEFAULT = 10_000.0
DAILY_TARGET = 50.0


def _ms(date_str):
    return int(datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc).timestamp() * 1000)


def _ym(ms):
    return datetime.fromtimestamp(ms / 1000, timezone.utc).strftime('%Y-%m')


def _y(ms):
    return datetime.fromtimestamp(ms / 1000, timezone.utc).year


def _date(ms):
    return datetime.fromtimestamp(ms / 1000, timezone.utc).strftime('%Y-%m-%d')


def monday_ceil(ms):
    """Ближайший понедельник 00:00 UTC не раньше ms. 1970-01-01 — четверг: понедельник = (дни + 3) % 7 == 0."""
    d = -(-ms // DAY_MS)
    while (d + 3) % 7 != 0:
        d += 1
    return d * DAY_MS


def monday_floor(ms):
    d = ms // DAY_MS
    while (d + 3) % 7 != 0:
        d -= 1
    return d * DAY_MS


# ── отбор и начисления ─────────────────────────────────────────────────────────────────────────────
def window_avg(ts, rate, t_lo, t_hi):
    """Среднее по начислениям с временем в (t_lo, t_hi]; → (mean | None, n)."""
    i0 = int(np.searchsorted(ts, t_lo, side='right'))
    i1 = int(np.searchsorted(ts, t_hi, side='right'))
    n = i1 - i0
    return (float(np.mean(rate[i0:i1])) if n else None), n


def select_top(f_by_sym, t, topn=TOPN, min_n=MIN_SETTLE):
    """Символы недели: средний фандинг за (t−7д, t] > 0, не менее min_n начислений; топ-N по среднему (ничья — по имени)."""
    cand = []
    for sym, (ts, rate) in f_by_sym.items():
        m, n = window_avg(ts, rate, t - WEEK_MS, t)
        if m is not None and n >= min_n and m > 0:
            cand.append((-m, sym))
    cand.sort()
    return [s for _, s in cand[:topn]]


def accrue(ts, rate, t_open, t_close, notional):
    """Фандинг шорта по начислениям в (t_open, t_close]: получает при положительной ставке, платит при отрицательной."""
    i0 = int(np.searchsorted(ts, t_open, side='right'))
    i1 = int(np.searchsorted(ts, t_close, side='right'))
    return float(np.sum(rate[i0:i1])) * notional


def side_cost(notional, fee_spot, fee_perp, slip=SLIP):
    """Комиссии и проскальзывание ОДНОЙ стороны (открытие либо закрытие) по обеим ногам."""
    return (fee_spot + fee_perp + 2 * slip) * notional


def slot_notional(c0):
    return c0 / SLOTS * EFF


def simulate_c1(f_by_sym, c0, fee_spot, fee_perp, slip=SLIP, t_start=None, t_end=None):
    """C1 по историческим ставкам. f_by_sym = {sym: (ts int64 мс по возрастанию, rate float)}.
    → {'weeks': [{t, end, held, opened, closed, funding, fees, pnl}], 'runs': [{sym, open, close}]}.
    Позиции, оставшиеся в топ-5, продолжаются без комиссий; в последний понедельник все закрываются (комиссии закрытия — в последней неделе)."""
    if not f_by_sym:
        return {'weeks': [], 'runs': []}
    first = min(int(ts[0]) for ts, _ in f_by_sym.values() if len(ts))
    last = max(int(ts[-1]) for ts, _ in f_by_sym.values() if len(ts))
    t_start = t_start if t_start is not None else monday_ceil(first + WEEK_MS)
    t_end = t_end if t_end is not None else monday_floor(last)
    if t_end <= t_start:
        return {'weeks': [], 'runs': []}
    N = slot_notional(c0)
    cost = side_cost(N, fee_spot, fee_perp, slip)
    mondays = list(range(t_start, t_end + 1, WEEK_MS))
    held, runs, weeks = {}, [], []
    for i, t in enumerate(mondays[:-1]):
        t_next = mondays[i + 1]
        sel = select_top(f_by_sym, t)
        closed = [s for s in sorted(held) if s not in sel]
        opened = [s for s in sel if s not in held]
        for s in closed:
            runs[held.pop(s)]['close'] = t
        for s in opened:
            held[s] = len(runs)
            runs.append({'sym': s, 'open': t, 'close': None})
        fund = sum(accrue(*f_by_sym[s], t, t_next, N) for s in held)
        fees = cost * (len(closed) + len(opened))
        weeks.append({'t': t, 'end': t_next, 'held': sorted(held), 'opened': opened, 'closed': closed, 'funding': fund, 'fees': fees, 'pnl': fund - fees})
    if weeks:                                   # конец периода: закрываем всё
        for s in sorted(held):
            runs[held[s]]['close'] = t_end
        weeks[-1]['closed'] = weeks[-1]['closed'] + sorted(held)
        weeks[-1]['fees'] += cost * len(held)
        weeks[-1]['pnl'] = weeks[-1]['funding'] - weeks[-1]['fees']
    return {'weeks': weeks, 'runs': runs}


# ── метрики ──────────────────────────────────────────────────────────────────────────────────────────
def _block(weeks, c0):
    """Метрики блока недель (годовая доходность на капитал и пр.). → dict или None."""
    if not weeks:
        return None
    pnl = sum(w['pnl'] for w in weeks)
    fund = sum(w['funding'] for w in weeks)
    fees = sum(w['fees'] for w in weeks)
    days = len(weeks) * 7
    apr = pnl / c0 * 365 / days
    held_weeks = [w for w in weeks if w['held']]
    neg = sum(1 for w in held_weeks if w['funding'] < 0)
    months = {}
    for w in weeks:
        months[_ym(w['t'])] = months.get(_ym(w['t']), 0.0) + w['pnl']
    worst = min(months.items(), key=lambda kv: kv[1]) if months else None
    eq, peak, dd = 0.0, 0.0, 0.0
    for w in weeks:
        eq += w['pnl'] / c0
        peak = max(peak, eq)
        dd = max(dd, peak - eq)
    return {'weeks': len(weeks), 'days': days, 'pnl': pnl, 'funding': fund, 'fees': fees, 'apr': apr,
            'neg_share': (neg / len(held_weeks)) if held_weeks else 0.0, 'held_weeks': len(held_weeks),
            'worst_month': (worst[0], worst[1] / c0) if worst else None, 'max_dd': dd,
            'cost_share': (fees / fund) if fund > 0 else None,
            'capital_50': (DAILY_TARGET * 365 / apr) if apr > 0 else None}


def summarize(weeks, c0, oos_ms=None):
    """{'all': блок, 'oos': блок, 'years': {год: блок}}."""
    oos_ms = oos_ms if oos_ms is not None else _ms(OOS_START)
    years = {}
    for w in weeks:
        years.setdefault(_y(w['t']), []).append(w)
    return {'all': _block(weeks, c0), 'oos': _block([w for w in weeks if w['t'] >= oos_ms], c0),
            'years': {y: _block(ws, c0) for y, ws in sorted(years.items())}}


# ── риск ликвидации шорта: максимальный рост за 8 часов ───────────────────────────────────────────────
def max_rise(o, h, win):
    """Максимум по окнам из win баров: max(high в окне) / open первого бара − 1; None, если баров меньше окна."""
    n = len(h)
    if n < win:
        return None
    mx = swv(h, win).max(axis=1)
    return float(np.max(mx / o[:n - win + 1] - 1.0))


def liquidation_check(runs, bars_1d, binance_of, vision_dir, threshold=LIQ_RISE):
    """Для каждой позиции — максимальный 8ч-рост за удержание: 1h-свечи Binance из кэша, иначе 15m (окна по 32 бара), иначе дневные BingX (high/open дня).
    → {'items': [{sym, open, close, rise, source}], 'hits': [..., rise ≥ threshold], 'sources': {источник: число}}. Кэш символа читается один раз."""
    by_sym = {}
    for r in runs:
        by_sym.setdefault(r['sym'], []).append(r)
    items, sources = [], {}
    for sym, rs in sorted(by_sym.items()):
        loaded = {}
        bsym = binance_of.get(sym)
        if bsym and vision_dir:
            for tf, win in (('1h', 8), ('15m', 32)):
                k = V.load_klines(vision_dir, bsym, tf)
                if k and len(k['ts']):
                    loaded[tf] = (win, k['ts'], k['o'], k['h'])
        daily = bars_1d.get(sym) or []
        for r in rs:
            t0, t1 = r['open'], r['close']
            rise, src = None, None
            for tf in ('1h', '15m'):
                if tf in loaded:
                    win, ts, o, h = loaded[tf]
                    i0, i1 = int(np.searchsorted(ts, t0, 'left')), int(np.searchsorted(ts, t1, 'left'))
                    cover = (ts[i0] - t0 <= 2 * (3_600_000 if tf == '1h' else 900_000)) if i0 < len(ts) and i1 > i0 else False
                    if cover:
                        v = max_rise(o[i0:i1], h[i0:i1], win)
                        if v is not None:
                            rise, src = v, f'Binance {tf}'
                            break
            if rise is None:
                vals = [b[2] / b[1] - 1.0 for b in daily if t0 <= b[0] < t1 and b[1] > 0]
                if vals:
                    rise, src = max(vals), 'BingX 1D (high/open дня)'
            if rise is not None:
                items.append({'sym': sym, 'open': t0, 'close': t1, 'rise': rise, 'source': src})
                sources[src] = sources.get(src, 0) + 1
        loaded.clear()
    return {'items': items, 'hits': [i for i in items if i['rise'] >= threshold], 'sources': sources}


# ── отчёт ──────────────────────────────────────────────────────────────────────────────────────────────
def _fmt_block(label, b):
    if not b:
        return f'  {label}: нет данных'
    cap = f'{b["capital_50"]:,.0f} USDT'.replace(',', ' ') if b['capital_50'] else 'недостижимо (APR ≤ 0)'
    cs = f'{b["cost_share"] * 100:.0f}%' if b['cost_share'] is not None else 'н/д (валовый фандинг ≤ 0)'
    wm = f'{b["worst_month"][0]} {b["worst_month"][1] * 100:+.2f}% от C0' if b['worst_month'] else 'н/д'
    return (f'  {label}: недель {b["weeks"]} ({b["held_weeks"]} с позициями) | APR {b["apr"] * 100:+.1f}% | недель с отрицательным фандингом {b["neg_share"] * 100:.0f}% | '
            f'худший месяц {wm} | макс. просадка {b["max_dd"] * 100:.2f}% от C0 | издержки/валовый фандинг {cs} | капитал для {DAILY_TARGET:g} USDT/день: {cap}')


def coverage_lines(f_by_sym, label, syms, week_ms=WEEK_MS):
    L = []
    for sym in syms:
        short = sym.split('/')[0].replace('USDT', '')
        if sym in f_by_sym and len(f_by_sym[sym][0]):
            ts = f_by_sym[sym][0]
            L.append(f'  {short}: {_date(int(ts[0]))} … {_date(int(ts[-1]))}, начислений {len(ts)}')
        else:
            L.append(f'  {short}: нет данных')
    return [f'{label}:'] + L


def build_report(sim, summ, liq, cov_main, ref, c0, fees, notes):
    """ref — {'sim', 'summ', 'syms'} или None (справка по Binance)."""
    L = ['📈 <b>/carry_test — C1: сбор фандинга (лонг спот + шорт перп), ТОЛЬКО история (BOT_SPEC §2.40)</b>',
         f'Правила C1 фиксированы ДО запуска: еженедельно (пн 00:00 UTC) топ-{TOPN} символов по среднему фандингу за 7 дней (среднее &gt; 0, ≥ {MIN_SETTLE} начислений), '
         f'оставшиеся в топ-{TOPN} не переоткрываются; капитал C0 = {SLOTS} равных слота (пустой слот — без дохода), перп {LEV:g}× → номинал = {EFF:.2f} × слот, фандинг не реинвестируется. '
         f'Издержки на каждую сторону по обеим ногам: спот {fees[0] * 100:.2f}% (FEE_SPOT) + перп {fees[1] * 100:.3f}% (FEE_TAKER) + проскальзывание {SLIP} × 2 ноги. '
         f'Расчёт на C0 = {c0:,.0f} USDT условно (доходность в % от C0 от величины не зависит).'.replace(',', ' '),
         '⚠️ ОШИБКА ВЫЖИВШЕГО: 16 символов H-XS отобраны по объёму на 2026-10 — символы, потерявшие ликвидность/делистингованные, в выборку не попали; фандинг исторически непостоянен '
         '(высокие ставки 2020–2021 и 2024 не гарантируют будущего). ⚠️ НЕ УЧТЕНЫ: базис спот/перп (разница цен ног, риск при выводе позиции), заём/хранение, ограничения '
         'вывода, ставка фандинга, меняющаяся внутри интервала.']
    L += notes
    L.append('')
    L += cov_main
    if not sim['weeks']:
        L.append('\n<b>ВЕРДИКТ C1: данных недостаточно для расчёта (нет недель с допустимыми символами) — оценки нет.</b>')
        return '\n'.join(L)
    a, o = summ['all'], summ['oos']
    L.append(f'\n<b>Период расчёта:</b> {_date(sim["weeks"][0]["t"])} … {_date(sim["weeks"][-1]["end"])} ({a["weeks"]} недель), позиций открыто {len(sim["runs"])}, '
             f'валовый фандинг {a["funding"]:+.1f} USDT, издержки {a["fees"]:.1f} USDT, чистый P&L {a["pnl"]:+.1f} USDT.')
    L.append('<b>Основной расчёт — ставки BingX:</b>')
    L.append(_fmt_block('весь период', a))
    L.append(_fmt_block(f'OOS с {OOS_START}', o))
    L.append('  по годам:')
    for y, b in summ['years'].items():
        L.append(_fmt_block(str(y), b))
    L.append(f'\n<b>Риск ликвидации шорта ({LEV:g}×): максимальный рост цены за любые 8 ч удержания (§2.40.5)</b>')
    if liq['items']:
        mx = max(liq['items'], key=lambda i: i['rise'])
        L.append(f'  оценено позиций {len(liq["items"])} из {len(sim["runs"])}; источники: ' + ', '.join(f'{k}: {v}' for k, v in liq['sources'].items()) +
                 f'; максимум {mx["rise"] * 100:.1f}% ({mx["sym"].split("/")[0].replace("USDT", "")}, неделя {_date(mx["open"])}, {mx["source"]})')
        L.append(f'  случаев роста ≥ {LIQ_RISE * 100:.0f}%: {len(liq["hits"])}')
        for i in sorted(liq['hits'], key=lambda i: -i['rise'])[:30]:
            L.append(f'    {i["sym"].split("/")[0].replace("USDT", "")}: открыта {_date(i["open"])}, закрыта {_date(i["close"])}, рост {i["rise"] * 100:.1f}% ({i["source"]})')
        if len(liq['hits']) > 30:
            L.append(f'    … и ещё {len(liq["hits"]) - 30}')
        if 'BingX 1D (high/open дня)' in liq['sources']:
            L.append('  ⚠️ часть позиций оценена по дневным свечам: это ВЕРХНЯЯ оценка для окон внутри суток (окна через полночь не учтены).')
    else:
        L.append('  данных для оценки нет.')
    L.append('\n<b>Справка — те же правила C1 по ставкам Binance (архив v95)</b>')
    if ref and ref['sim']['weeks']:
        L += coverage_lines(ref['f'], 'Покрытие фандинга Binance', ref['syms'])
        L.append(_fmt_block('весь период', ref['summ']['all']))
        L.append(_fmt_block(f'OOS с {OOS_START}', ref['summ']['oos']))
    else:
        L.append('  нет данных (запусти /edge_audit — он догружает фандинг Binance в кэш) или недостаточно для расчёта.')
    cap = lambda b: (f'{b["capital_50"]:,.0f} USDT'.replace(',', ' ') if b and b['capital_50'] else 'недостижимо (APR ≤ 0)')
    apr = lambda b: (f'{b["apr"] * 100:+.1f}%' if b else 'н/д')
    L.append(f'\n<b>ВЕРДИКТ C1 (оценка, критериев «пройдено/не пройдено» нет — решение за владельцем): APR после всех издержек {apr(a)} за весь период / {apr(o)} на OOS; '
             f'капитал для {DAILY_TARGET:g} USDT в день: {cap(a)} / {cap(o)}.</b>')
    return '\n'.join(L)


def arrays_from_bars_funding(funding_rows):
    """[[ts, rate], ...] → (ts int64, rate float64) по возрастанию, без дублей."""
    if not funding_rows:
        return np.array([], dtype=np.int64), np.array([], dtype=np.float64)
    a = np.array(funding_rows, dtype=np.float64)
    ts = a[:, 0].astype(np.int64)
    o = np.argsort(ts, kind='stable')
    ts, rate = ts[o], a[o, 1]
    keep = np.r_[True, np.diff(ts) != 0] if len(ts) > 1 else np.ones(len(ts), bool)
    return ts[keep], rate[keep]


def run_carry(payload, params):
    """Расчётная часть /carry_test (в дочернем процессе). payload = (data, info) из research_htf.load_data(...); params: fee_spot, fee_perp, vision_dir, c0.
    → текст отчёта."""
    data, info = payload
    fees = (float(params['fee_spot']), float(params['fee_perp']))
    c0 = float(params.get('c0') or C0_DEFAULT)
    syms = sorted(data)
    f_bx = {s: arrays_from_bars_funding(data[s].get('funding')) for s in syms}
    f_bx = {s: v for s, v in f_bx.items() if len(v[0])}
    bars_1d = {s: data[s].get('1d') or [] for s in syms}
    sim = simulate_c1(f_bx, c0, fees[0], fees[1])
    summ = summarize(sim['weeks'], c0)
    d = params.get('vision_dir')
    binance_of = {s: s.split('/')[0] + 'USDT' for s in syms}
    liq = liquidation_check(sim['runs'], bars_1d, binance_of, d)
    cov = coverage_lines(f_bx, 'Покрытие фандинга BingX (кэш htf_funding)', syms)
    ff = (info or {}).get('funding_failed') or {}
    notes = []
    if ff:
        notes.append('⚠️ фандинг BingX не загружен: ' + '; '.join(f'{k.split("/")[0]}: {v}' for k, v in ff.items()))
    miss1d = [s.split('/')[0] for s in syms if not bars_1d.get(s)]
    if miss1d:
        notes.append('⚠️ нет дневных свечей BingX: ' + ', '.join(miss1d))
    # справка: Binance
    ref = None
    f_bn = {}
    if d:
        try:
            import research_edge as E
            for s in syms:
                f = E.load_funding(d, binance_of[s])
                if f is not None and len(f['ts']):
                    f_bn[s] = (f['ts'], f['rate'])
        except Exception:
            f_bn = {}
    if f_bn:
        sim_b = simulate_c1(f_bn, c0, fees[0], fees[1])
        ref = {'sim': sim_b, 'summ': summarize(sim_b['weeks'], c0), 'f': f_bn, 'syms': syms}
    return build_report(sim, summ, liq, cov, ref, c0, fees, notes)


async def prepare_carry(exchange, db_path, syms, progress=None):
    """Сетевая часть /carry_test (в процессе бота, через общий exchange): докачка 1D и фандинга BingX в кэш v86–v88 → (data, info) для расчёта в дочернем процессе."""
    import research_htf as H
    return await H.load_data(exchange, db_path, symbols=tuple(syms), progress=progress, tfs=('1d',), deep_funding=True)
