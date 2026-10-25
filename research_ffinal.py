"""research_ffinal.py — ОДИНОЧНЫЙ ФИНАЛ «фабрики»: CRYPTO 4h, T-MA-10-50 (BOT_SPEC §2.42, пре-регистрация записана ДО кода). Расчёт k (инфляция t на синтетике, LOCKBOX НЕ читается) и один прогон
на LOCKBOX по критериям §2.42 п. 3 с общим флагом финала. Реализация кандидата — та же функция `research_factory.run_config`, что и у `/factory` (побитово те же сделки на TRAIN)."""
import math
import time
from datetime import datetime, timezone

import numpy as np

import research_fbias as RB
import research_fengine as E
import research_factory as RF
import research_ma_lib as L
import research_vision as V

CAND_CLASS, CAND_TF, CAND_ID = 'CRYPTO', '4h', 'T-MA-10-50'
K_REPS = 200
K_WARM = 300                                   # баров прогрева перед измеряемым участком синтетики
K_SEED_TAG = 4242                              # generator: default_rng([SEED, K_SEED_TAG, повтор, номер символа])
NULL_MEDIAN_ABS_T = 0.674                      # медиана |t| стандартной нормальной величины
MIN_T_ADJ = 2.0
SPEC_K_KEY = 'FINAL4H-K:'
STEP4H = 4 * 3_600_000


def candidate_spec():
    cfg = next(c for c in L.library() if c.id == CAND_ID)
    return ('lib', cfg)


def cand_tid():
    return RF.tid(CAND_CLASS, CAND_TF, CAND_ID)


def read_spec_k(path=None):
    """Значение k из строки `FINAL4H-K: <число> …` файла BOT_SPEC → float | None (строки нет или заглушка)."""
    try:
        with open(path or RF.SPEC_PATH, encoding='utf-8') as f:
            for ln in f:
                if ln.startswith(SPEC_K_KEY):
                    tok = ln.split(':', 1)[1].strip().split()
                    return float(tok[0]) if tok else None
    except (OSError, ValueError):
        return None
    return None


def lockbox_bars_count(vdir):
    """Число 4h-баров ряда от начала LOCKBOX до конца кэша Binance (последняя неполная группа не считается, как у build_crypto). Используются ТОЛЬКО метки времени BTCUSDT — цены LOCKBOX
    не читаются. → int (0 — кэша нет)."""
    k = V.load_klines(vdir, 'BTCUSDT', '15m')
    if k is None or not len(k['ts']):
        return 0
    last_ts = int(k['ts'][-1])
    del k
    first_group = L.lockbox_start_ms() // STEP4H
    last_group = last_ts // STEP4H
    return max(0, int(last_group - first_group))


def synth_4h(stats, ts, day, rng, trade_from_ms, key):
    """Синтетический ряд на заданном календаре ts: независимые нормальные доходности (σ — из stats), open = предыдущий close, high/low — выходы по модулю нормальной величины."""
    sigma, eh, el = stats
    n = len(ts)
    c0 = 100.0
    c = c0 * np.exp(np.cumsum(rng.normal(0.0, sigma, n)))
    o = np.r_[c0, c[:-1]]
    sh, sl = eh * math.sqrt(math.pi / 2), el * math.sqrt(math.pi / 2)
    hi = np.maximum(o, c) * (1.0 + np.abs(rng.normal(0.0, sh, n)))
    lo = np.minimum(o, c) * np.maximum(1.0 - np.abs(rng.normal(0.0, sl, n)), 1e-6)
    S = E.make_series(key, {'ts': ts, 'o': o, 'h': hi, 'l': lo, 'c': c, 'day': day}, trade_from_ms, None)
    return S


def compute_k(vdir, syms, reps=K_REPS):
    """BOT_SPEC §2.42 п. 2: k = max(1, m0/0.674), m0 — медианный |t| (Ньюи–Уэста лаг 5, дневная сумма портфеля) кандидата на синтетике нулевой гипотезы: 16 независимых рядов, σ каждого
    символа — из баров 4h ДО LOCKBOX (σ LOCKBOX недоступна без его открытия), длина измеряемого участка — число 4h-баров от начала LOCKBOX до конца кэша, без издержек и фандинга.
    LOCKBOX не читается. → dict(k, m0, n_lock, reps, n_series, t (список))."""
    real = RF.crypto_series(vdir, syms, CAND_TF, mode='pre')
    stats = [RB.real_stats(S) for S in real]
    keys = [S['key'] for S in real]
    real = None
    n_lock = lockbox_bars_count(vdir)
    if n_lock < 50 or not stats:
        return {'k': None, 'm0': None, 'n_lock': n_lock, 'reps': reps, 'n_series': len(stats), 't': [], 'error': 'нет данных кэша для длины LOCKBOX'}
    lock0 = L.lockbox_start_ms()
    ts = lock0 - K_WARM * STEP4H + np.arange(K_WARM + n_lock, dtype=np.int64) * STEP4H
    day = ts // E.DAY_MS
    lock_day = lock0 // E.DAY_MS
    spec = candidate_spec()
    ts_list = []
    for rep in range(reps):
        syn = [synth_4h(stats[i], ts, day, np.random.default_rng([RF.SEED, K_SEED_TAG, rep, i]), lock0, keys[i]) for i in range(len(stats))]
        out = RF.run_config(spec, syn, CAND_TF, CAND_CLASS, {}, [], 0.0)
        x = out['r'][out['days'] >= lock_day]
        _m, t, _n = E.nw_t(x)
        if math.isfinite(t):
            ts_list.append(float(t))
    m0 = float(np.median(np.abs(ts_list))) if ts_list else float('nan')
    k = max(1.0, m0 / NULL_MEDIAN_ABS_T) if math.isfinite(m0) else None
    return {'k': k, 'm0': m0, 'n_lock': n_lock, 'reps': len(ts_list), 'n_series': len(stats), 't': ts_list}


# ── критерии LOCKBOX ─────────────────────────────────────────────────────────────────────────────────
def month_key(ts_ms):
    t = datetime.fromtimestamp(int(ts_ms) / 1000, timezone.utc)
    return t.year * 12 + t.month - 1


def btc_months(ts, c, last_ts_end):
    """Месячные доходности BTC по календарным месяцам UTC за период LOCKBOX: {индекс месяца: доходность}. Доходность = close последнего 4h-бара месяца / close последнего 4h-бара предыдущего месяца − 1;
    месяц считается полным, если ряд покрывает его конец (last_ts_end ≥ начало следующего месяца); неполный последний месяц исключается."""
    ts = np.asarray(ts)
    c = np.asarray(c)
    lb = L.lockbox_start_ms()
    mk = np.array([month_key(t) for t in ts])
    out = {}
    first_m = month_key(lb)
    last_m = int(mk[-1])
    for m in range(first_m, last_m + 1):
        nxt = (m + 1)
        start_next = int(datetime(nxt // 12, nxt % 12 + 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
        if last_ts_end < start_next:
            continue                                                       # неполный месяц
        idx = np.flatnonzero(mk <= m)
        idx_prev = np.flatnonzero(mk <= m - 1)
        if not len(idx_prev) or not len(idx) or mk[idx[-1]] != m:
            continue
        out[m] = float(c[idx[-1]] / c[idx_prev[-1]] - 1.0)
    return out


def month_group_means(days, r, months_ret):
    """Среднее дневной доходности портфеля по дням месяцев роста / падения BTC. → dict(up={n_months, n_days, mean|None}, down=…)."""
    lo = E.day_bounds('LOCKBOX')[0]
    m_of = np.array([month_key(int(d) * E.DAY_MS) for d in days])
    res = {}
    for name, cond in (('up', lambda x: x > 0), ('down', lambda x: x < 0)):
        ms = [m for m, x in months_ret.items() if cond(x)]
        mask = (days >= lo) & np.isin(m_of, ms)
        res[name] = {'n_months': len(ms), 'n_days': int(mask.sum()), 'mean': float(r[mask].mean()) if mask.any() else None}
    return res


def evaluate(base, stress, k, months_ret, periods=365):
    """Критерии §2.42 п. 3 по результатам базового (detail) и стресс-прогона. → dict метрик и вердикт по каждому критерию."""
    lo = E.day_bounds('LOCKBOX')[0]
    days, r = base['days'], base['r']
    m = days >= lo
    x = r[m]
    mean, t5, n = E.nw_t(x)
    t20 = E.nw_t(x, 20)[1]
    t_adj = t5 / k if math.isfinite(t5) else float('nan')
    sh = E.sharpe(x, periods)
    dd = E.max_drawdown(x)
    xs = stress['r'][stress['days'] >= lo]
    ms = float(xs.mean()) if len(xs) and len(xs) == len(x) else float('nan')
    grp = month_group_means(days, r, months_ret)
    crit = {
        't_adj': bool(math.isfinite(t_adj) and t_adj >= MIN_T_ADJ),
        'sharpe': bool(math.isfinite(sh) and sh >= L.FINAL_MIN_SHARPE),
        'dd': bool(dd <= L.FINAL_MAX_DD),
        'stress': bool(math.isfinite(ms) and ms > 0),
        'up': (None if grp['up']['mean'] is None else bool(grp['up']['mean'] >= 0)),
        'down': (None if grp['down']['mean'] is None else bool(grp['down']['mean'] >= 0)),
    }
    passed = all(v for v in crit.values() if v is not None) and n > 0
    return {'n': n, 'mean': mean, 't5': t5, 't20': t20, 't_adj': t_adj, 'k': k, 'sharpe': sh, 'dd': dd, 'stress_mean': ms, 'groups': grp, 'crit': crit, 'pass': bool(passed)}


def reference_breakdown(base):
    """Справочно (не критерии): лонг/шорт-ноги, по годам, по символам, вклад фандинга, плечо. б.п./день капитала."""
    lo = E.day_bounds('LOCKBOX')[0]
    days = base['days']
    m = days >= lo
    mu = base['multi']
    nd = int(m.sum())
    k = 1e4
    out = {'long': float(mu['ret_long'][m].mean() * k) if nd and 'ret_long' in mu else None, 'short': float(mu['ret_short'][m].mean() * k) if nd and 'ret_short' in mu else None,
           'fund': float(-mu['c_fund'][m].mean() * k) if nd else None, 'lev': float(mu['expo'][m].mean()) if nd else None, 'lev95': float(np.percentile(mu['expo'][m], 95)) if nd else None}
    years = {}
    yrs = np.array([datetime.fromtimestamp(int(d) * E.DAY_MS / 1000, timezone.utc).year for d in days])
    for y in sorted(set(yrs[m])):
        mm = m & (yrs == y)
        years[int(y)] = {'n': int(mm.sum()), 'mean': float(base['r'][mm].mean() * k)}
    out['years'] = years
    sym = {}
    for p in base.get('parts', []):
        pm = p['day'] >= lo
        sym[p['key']] = float(p['ret'][pm].sum() / max(1, nd) * k)
    out['symbols'] = sym
    return out


# ── команда ──────────────────────────────────────────────────────────────────────────────────────────
def spec_summary():
    return ['<b>§2.42 — одиночный финал: CRYPTO 4h, T-MA-10-50</b> (BOT_SPEC §2.42, пре-регистрация записана ДО кода)',
            'Кандидат: ячейка CRYPTO 4h, конфигурация T-MA-10-50 (реализация фабрики без изменений: 16 символов, таргетинг волатильности, стоп 3·ATR20, издержки и фандинг как в /factory). N = 1.',
            'Критерии на LOCKBOX (все одновременно): t_adj = t(NW лаг 5)/k ≥ 2.0; годовой Шарп после издержек ≥ 0.5; макс. просадка ≤ 15%; при стрессе ×2 (комиссии и проскальзывание) mean > 0; '
            'mean ≥ 0 отдельно в месяцы роста и в месяцы падения BTC (календарные месяцы UTC, неполный последний — исключён; пустая группа — не применима).',
            'Справочно: лонг/шорт-ноги, годы, символы, фандинг, плечо, t без поправки и по лагу 20.',
            'Решение: пройдено → shadow-форвард этой конфигурации на BingX 4h (8 недель, отдельный пре-коммит); не пройдено → поиск стратегий закрыт окончательно (§2.32.8 без оговорок).',
            'Оговорка: кандидат выбран по TRAIN/VALID из 10 коррелированных конфигураций (проклятие победителя); порог t_adj ≥ 2.0 рассчитан на один заранее заданный тест и этот выбор не учитывает.']


async def run_final4h(params, progress=None):
    """/final_crypto4h: без confirm — показ §2.42 и k (LOCKBOX не читается); confirm — единственный прогон на LOCKBOX с общим флагом финала."""
    ctx = RF.build_ctx(params)
    d = ctx['fdir']
    confirm = bool(params.get('confirm'))
    flag = RF.kv_get('final_started', d) or RF.kv_get('final_done', d)

    async def say(m):
        if progress:
            await progress(m)
    for s in ctx['syms']:
        if V.load_klines(ctx['vdir'], s, '15m') is None:
            return f'⛔ /final_crypto4h отказ: {s}: нет кэша 15m (/data/vision)' + (' (флаг финала НЕ взят)' if confirm else '')
    if not ctx['syms']:
        return '⛔ /final_crypto4h отказ: вселенная H-XS не заморожена (/htf_xsmom)'
    await say(f'расчёт k: синтетика {K_REPS} повторов × {len(ctx["syms"])} символов, LOCKBOX не читается…')
    kres = compute_k(ctx['vdir'], ctx['syms'], reps=K_REPS)
    if kres['k'] is None:
        return '⛔ /final_crypto4h отказ: k не рассчитан — ' + str(kres.get('error')) + (' (флаг финала НЕ взят)' if confirm else '')
    RF.kv_set('final4h_k', {k: v for k, v in kres.items() if k != 't'}, d)
    spec_k = read_spec_k(ctx.get('spec_path'))
    k_ok = spec_k is not None and round(spec_k, 3) == round(kres['k'], 3)
    head = spec_summary() + ['',
                             f'<b>k = {kres["k"]:.3f}</b> (m0 = {kres["m0"]:.3f}; k = max(1, m0/{NULL_MEDIAN_ABS_T}); {kres["reps"]} повторов, {kres["n_series"]} символов, измеряемый участок {kres["n_lock"]} 4h-баров ≈ {kres["n_lock"] / 6:.0f} суток; '
                             f'σ — из данных до LOCKBOX, seed {RF.SEED}).',
                             f'Строка в BOT_SPEC: {SPEC_K_KEY} {("%.3f" % spec_k) if spec_k is not None else "не записана"} — ' + ('СОВПАДАЕТ с расчётом.' if k_ok else
                                                                                                                        'НЕ совпадает/не записана: пришлите k, я запишу его в BOT_SPEC отдельным коммитом; без совпадения confirm не запустится.'),
                             f'Флаг финала: {"УЖЕ ВЗЯТ — запуск невозможен" if flag else "свободен"}.']
    if not confirm:
        head.append('Запуск: /final_crypto4h confirm (один прогон на LOCKBOX, повтор запрещён).')
        return '\n'.join(head)
    if flag:
        return '⛔ /final_crypto4h отказ: флаг финала уже взят (/factory_final или предыдущий запуск) — финал выполняется один раз, повтор запрещён (BOT_SPEC §2.42 п. 6)'
    if not k_ok:
        return '⛔ /final_crypto4h отказ: k не записан в BOT_SPEC или не совпадает с расчётом (флаг финала НЕ взят)\n' + '\n'.join(head[-3:])
    if not RF.kv_set_if_absent('final_started', {'t': time.time(), 'kind': 'final_crypto4h', 'k': kres['k'], 'rev': RF.REV}, d):
        return '⛔ /final_crypto4h отказ: флаг финала уже взят параллельным запуском'
    session = L.open_lockbox_session(final=True)
    await say('LOCKBOX открыт (единственный прогон)…')
    series = RF.crypto_series(ctx['vdir'], ctx['syms'], CAND_TF, mode='full', session=session)
    spec = candidate_spec()
    base = RF.run_config(spec, series, CAND_TF, CAND_CLASS, {}, [], 1.0, detail=True)
    stress = RF.run_config(spec, series, CAND_TF, CAND_CLASS, {}, [], 2.0)
    btc = next((S for S in series if S['key'] == RF.BTC), None)
    months = btc_months(btc['ts'], btc['c'], int(btc['ts'][-1]) + STEP4H) if btc is not None else {}
    ev = evaluate(base, stress, kres['k'], months)
    ref = reference_breakdown(base)
    res = {'eval': ev, 'ref': ref, 'months': {str(k_): v for k_, v in months.items()}, 'k': kres['k'], 'rev': RF.REV}
    RF.kv_set('final4h_result', res, d)
    RF.kv_set('final_done', True, d)
    return build_final_report(ctx, head, ev, ref, months)


def build_final_report(ctx, head, ev, ref, months):
    fm = lambda x, nd=2: 'н/д' if x is None or (isinstance(x, float) and not math.isfinite(x)) else f'{x:.{nd}f}'
    c = ev['crit']
    mark = lambda v: 'н/п' if v is None else ('да' if v else 'НЕТ')
    g = ev['groups']
    R = [f'🔒 <b>/final_crypto4h — одиночный финал на LOCKBOX (2025+), единственный прогон</b> | код {ctx.get("version") or RF.REV}', *head[-3:], '',
         f'<b>РЕЗУЛЬТАТ: {"ПРОЙДЕНО" if ev["pass"] else "НЕ ПРОЙДЕНО"}</b> (дней {ev["n"]}, k = {ev["k"]:.3f})',
         f'  t_adj = {fm(ev["t_adj"])} (t NW лаг 5 = {fm(ev["t5"])}) ≥ 2.0: {mark(c["t_adj"])}',
         f'  годовой Шарп = {fm(ev["sharpe"])} ≥ 0.5: {mark(c["sharpe"])}',
         f'  макс. просадка = {ev["dd"] * 100:.1f}% ≤ 15%: {mark(c["dd"])}',
         f'  mean при издержках ×2 = {fm(ev["stress_mean"] * 1e4 if math.isfinite(ev["stress_mean"]) else None)} б.п./день > 0: {mark(c["stress"])}',
         f'  mean в месяцы роста BTC ({g["up"]["n_months"]} мес., {g["up"]["n_days"]} дн.) = {fm(g["up"]["mean"] * 1e4 if g["up"]["mean"] is not None else None)} б.п./день ≥ 0: {mark(c["up"])}',
         f'  mean в месяцы падения BTC ({g["down"]["n_months"]} мес., {g["down"]["n_days"]} дн.) = {fm(g["down"]["mean"] * 1e4 if g["down"]["mean"] is not None else None)} б.п./день ≥ 0: {mark(c["down"])}',
         f'  mean (весь LOCKBOX) = {fm(ev["mean"] * 1e4)} б.п./день', '',
         '<b>Справочно (не критерии)</b>: лонг-нога %s, шорт-нога %s б.п./день; вклад фандинга %s б.п./день; плечо ср %s (p95 %s); t по лагу 20 = %s' % (
             fm(ref['long']), fm(ref['short']), fm(ref['fund']), fm(ref['lev']), fm(ref['lev95']), fm(ev['t20'])),
         '  по годам: ' + '; '.join(f'{y}: {fm(v["mean"])} б.п./день ({v["n"]} дн.)' for y, v in ref['years'].items()),
         '  по символам (вклад, б.п./день капитала): ' + '; '.join(f'{k_}: {fm(v)}' for k_, v in sorted(ref['symbols'].items(), key=lambda x: -x[1])),
         '  месяцы BTC (индекс месяца UTC → доходность): ' + ', '.join(f'{(m_ // 12)}-{m_ % 12 + 1:02d}: {x * 100:+.1f}%' for m_, x in sorted(months.items())), '',
         'Решение (BOT_SPEC §2.42 п. 4): пройдено → shadow-форвард этой конфигурации на BingX 4h (8 недель, отдельный пре-коммит), затем живая торговля на риске 0.25–0.5%; '
         'не пройдено → поиск стратегий закрыт окончательно, без исключений (§2.32.8).',
         'Оговорка: кандидат выбран по TRAIN/VALID из 10 коррелированных конфигураций; порог рассчитан на один заранее заданный тест (проклятие победителя не учтено).']
    return '\n'.join(R)
