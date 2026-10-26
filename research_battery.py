"""research_battery.py — финальная серия «батарея» (BOT_SPEC §2.43, пре-регистрация записана ДО кода): загрузка (/battery_load), шаги 0–3 (/battery), финал на LOCKBOX (/battery_final confirm).
Расчёты — в дочернем процессе (research_runner); LOCKBOX читается только в шаге 4 через сессию финала после взятия НОВОГО флага серии `battery_final_started` (kv factory.db)."""
import math
import os
import time

import numpy as np

import research_bdata as B
import research_bsim as S
import research_factory as RF
import research_fengine as E
import research_ffinal as FF
import research_ma_lib as L
import research_vision as V

SEED = 20261025
BIAS_REPS = 50
CALIB_N = L.CALIB_N                              # 200
CALIB_MAX = int(L.CALIB_MAX_FRAC * L.CALIB_N)    # > 10 из 200 — СТОП
MEDIAN_ABS_MAX = 1.0
TAIL_T = 3.0
TAIL_SHARE = 0.10
MAX_FINALISTS = 2
VALID_MIN_T = L.VALID_MIN_T                      # 1.5
HYPS = ('H1', 'H2', 'H3', 'H5')
KV_STATE = 'battery_state'
KV_CANDS = 'battery_candidates'
KV_FLAG = 'battery_final_started'
KV_DONE = 'battery_final_done'
KV_RESULT = 'battery_final_result'


def fees_of(params):
    return {'taker': float(params.get('fee_taker', 0.0005)), 'maker': float(params.get('fee_maker', 0.0005))}


def fees_note(fees):
    s = f'комиссии: FEE_TAKER {fees["taker"] * 100:.4f}%, FEE_MAKER {fees["maker"] * 100:.4f}%'
    if abs(fees['maker'] - fees['taker']) < 1e-12:
        s += ' — ВНИМАНИЕ: FEE_MAKER = FEE_TAKER (ENV FEE_MAKER не задана; сетка H5 переоценена по издержкам лимитных ордеров, открытая задача владельца)'
    return s


# ── окружение расчёта ───────────────────────────────────────────────────────────────────────────────
def build_env(params, mode='pre', session=None):
    """Панель D1 (все перпетуалы), контекст PIT-ликвидности и часовые ряды 16 символов H-XS с фандингом. mode='full' — только в шаге 4 (через сессию)."""
    d = params.get('bdir') or B.battery_dir()
    P = B.load_panel(d, mode, session)
    if P is None:
        return None
    ctx = S.make_ctx(P)
    vdir = params['vdir']
    syms16 = list(params.get('syms') or [])
    series = {}
    for s in syms16:
        h = B.hourly_series(vdir, s, mode, session)
        if h is not None and len(h['ts']):
            series[s] = h
    names = [s for s in syms16 if s in series]
    env = {'P': P, 'ctx': ctx, 'bdir': d, 'days': np.arange(P['day0'], P['day0'] + P['nd'], dtype=np.int64), 'hc': None, 'missing16': [s for s in syms16 if s not in series]}
    if names:
        grid = B.hourly_grid(series, names)
        fund = {}
        for s in names:
            fz = B.load_fund(d, s)
            if fz is not None and len(fz['ts']):
                ts, rate = B._clip_period(fz['ts'], mode, session, fz['rate'])
                fund[s] = (ts, rate)
        FB = B.hourly_funding(grid, fund)
        pmap = [P['syms'].index(s) if s in P['syms'] else -1 for s in names]
        env['hc'] = {'grid': grid, 'FB': FB, 'fund': fund, 'pmap': pmap}
        env['no_fund16'] = [s for s in names if s not in fund]
        env['no_panel16'] = [s for s, j in zip(names, pmap) if j < 0]
    return env


def run_cell(cell, env, fees, mult=1.0, fund_on=True, costs_on=True, synth=None, rand=False, rng=None, n_events=None, offset_h=0, order='worst'):
    """Один прогон ячейки (гипотеза, вариант). synth — dict синтетики {'P': панель, 'grid': часовая сетка} (шаг 1); rand — случайная стратегия той же частоты (шаг 2)."""
    h, v = cell
    ctx = env['ctx']
    p = S.CELL_PARAM[v] if v in S.CELL_PARAM else None
    days = env['days']
    if h == 'H1':
        c2 = S.with_prices(ctx, synth['P']) if synth else ctx
        return S.sim_h1(c2, int(p), fees, mult, fund_on, rand, rng, costs_on)
    if h == 'H2':
        c2 = S.with_prices(ctx, synth['P']) if synth else ctx
        if rand:
            lo_d, hi_d = E.day_bounds('TRAIN')[0] - P_day0(env), E.day_bounds('VALID')[1] - P_day0(env)
            ev = S.h2_random_events(ctx, n_events or 1, rng, max(0, lo_d), hi_d)
        else:
            ev = S.h2_events(ctx, int(p))
        return S.sim_h2(c2, ev, fees, mult, fund_on, costs_on)
    hc = env['hc']
    if hc is None:
        return {'days': days, 'r': np.zeros(len(days)), 'meta': {'trades': 0, 'no_hxs': True}}
    if synth:
        hc = dict(hc, grid=synth['grid'])
    if h == 'H3':
        prep = S.h3_prepare(hc, ctx, v)
        return S.sim_h3(prep, fees, days, mult, fund_on, rand, rng, costs_on)
    if h == 'H5':
        win = S.grid_prepare(hc, ctx, float(p), offset_h)
        return S.sim_h5(win, fees, days, mult, fund_on, costs_on, order)
    raise ValueError(cell)


def P_day0(env):
    return env['P']['day0']


def train_t(res):
    return S.t_period(res['days'], res['r'], 'TRAIN')


# ── шаг 0: мощность ─────────────────────────────────────────────────────────────────────────────────
def coverage_days(cell, env):
    """D — число календарных дней TRAIN, в которые у гипотезы есть позиция или допустимый символ (§2.43.4 п. 0), и число событий. Доходности не считаются."""
    h, v = cell
    ctx, P = env['ctx'], env['P']
    lo, hi = E.day_bounds('TRAIN')
    days = env['days']
    in_train = (days >= lo) & (days < hi)
    p = S.CELL_PARAM.get(v)
    if h == 'H1':
        L_days = int(p)
        n = 0
        ev = 0
        for d in np.flatnonzero(in_train):
            avg, ok = S.h1_signal(ctx, int(d), L_days)
            if avg is not None and ok.sum() >= 10:
                n += 1
        for d in np.flatnonzero(in_train):
            if (P['day0'] + int(d)) % S.H1_HOLD == 0:
                avg, ok = S.h1_signal(ctx, int(d), L_days)
                if avg is not None:
                    sh, lg = S.h1_select(avg, ok)
                    ev += len(sh) + len(lg)
        return n, ev
    if h == 'H2':
        evs = [(e, s) for e, s in S.h2_events(ctx, int(p)) if in_train[e]]
        cov = np.zeros(P['nd'], bool)
        for e, s in evs:
            cov[e:min(e + S.H2_HOLD, P['nd'] - 1) + 1] = True
        return int((cov & in_train).sum()), len(evs)
    hc = env['hc']
    if hc is None:
        return 0, 0
    if h == 'H3':
        prep = S.h3_prepare(hc, ctx, v)
        dd = np.unique(prep['day'])
        di = dd - P['day0']
        di = di[(di >= 0) & (di < P['nd'])]
        return int(in_train[di].sum()), int(len(prep['r']))
    win = S.grid_prepare(hc, ctx, float(p))
    bd = win['bar_day'][np.repeat(win['valid'].reshape(win['W'], win['ns']).any(1), 1)]
    di = np.unique(bd) - P['day0']
    di = di[(di >= 0) & (di < P['nd'])]
    return int(in_train[di].sum()), int(win['valid'].sum())


def step0(env, n_planned=S.N_PLANNED):
    out = {}
    tb = S.t_bh(n_planned)
    for cell in S.CELLS:
        D, ev = coverage_days(cell, env)
        sm = S.s_min(n_planned, D)
        out[S.cid(*cell)] = {'D': int(D), 'events': int(ev), 't_bh': tb, 's_min': (sm if math.isfinite(sm) else None), 'excluded': bool(sm > S.MAX_S_MIN)}
    return out


# ── шаг 1: смещение правил на синтетике ────────────────────────────────────────────────────────────
def step1(env, fees, cells, rep_from=0, rep_to=BIAS_REPS):
    """Каждая гипотеза-вариант на синтетике (мартингал по цене, σ и календарь реальные; без издержек и без фандинга в P&L), TRAIN, t по U5; повторы [rep_from, rep_to). → {id: [t…]}."""
    out = {S.cid(*c): [] for c in cells}
    daily_cells = [c for c in cells if c[0] in ('H1', 'H2')]
    hourly_cells = [c for c in cells if c[0] in ('H3', 'H5')]
    for rep in range(rep_from, rep_to):
        if daily_cells:
            rng = np.random.default_rng([SEED, 4301, rep])
            syn = {'P': S.synth_daily_panel(env['P'], rng)}
            for c in daily_cells:
                t = train_t(run_cell(c, env, fees, 1.0, False, False, synth=syn))[0]
                if math.isfinite(t):
                    out[S.cid(*c)].append(float(t))
        if hourly_cells and env['hc'] is not None:
            rng = np.random.default_rng([SEED, 4302, rep])
            syn = {'grid': S.synth_hourly(env['hc']['grid'], rng)}
            for c in hourly_cells:
                t = train_t(run_cell(c, env, fees, 1.0, False, False, synth=syn))[0]
                if math.isfinite(t):
                    out[S.cid(*c)].append(float(t))
                if c[0] == 'H5':                                                  # диагностика порядка внутри бара (оптимистичный), не для решений
                    td = train_t(run_cell(c, env, fees, 1.0, False, False, synth=syn, order='best'))[0]
                    if math.isfinite(td):
                        out.setdefault(S.cid(*c) + '~best', []).append(float(td))
    return out


def bias_verdict(ts_by_cell):
    """Решение по гипотезе — по объединённой выборке её вариантов: медианный |t| > 1.0 или |t| > 3 в ≥ 10% повторов → смещена (исключается из N). → {гипотеза: dict}."""
    out = {}
    for h in HYPS:
        ts = [t for c, v in ts_by_cell.items() if c.startswith(h + '-') and not c.endswith('~best') for t in v]
        if not ts:
            continue
        a = np.abs(np.asarray(ts))
        med, sh = float(np.median(a)), float(np.mean(a > TAIL_T))
        reasons = []
        if med > MEDIAN_ABS_MAX:
            reasons.append(f'медианный |t| {med:.2f} > {MEDIAN_ABS_MAX:g}')
        if sh >= TAIL_SHARE:
            reasons.append(f'доля |t| > {TAIL_T:g}: {sh * 100:.0f}% ≥ {TAIL_SHARE * 100:.0f}%')
        out[h] = {'n': len(ts), 'median_abs': med, 'share3': sh, 'biased': bool(reasons), 'reasons': '; '.join(reasons)}
    return out


# ── шаг 2: калибровка ───────────────────────────────────────────────────────────────────────────────
def step2(env, fees, cells, n=CALIB_N, cell_offset=0):
    """200 случайных стратегий на ячейку (та же частота), TRAIN, шаг A по 200 (t > 0 и BH q = 0.05). → {id: dict(passed, n, ts)}; > 10 из 200 — СТОП."""
    out = {}
    for ci, c in enumerate(cells, start=cell_offset):
        ts = []
        n_ev = None
        if c[0] == 'H2':
            lo_d, hi_d = E.day_bounds('TRAIN')[0] - P_day0(env), E.day_bounds('VALID')[1] - P_day0(env)
            n_ev = len([1 for e, s_ in S.h2_events(env['ctx'], int(S.CELL_PARAM[c[1]])) if lo_d <= e < hi_d])
        for i in range(n):
            rng = np.random.default_rng([SEED, 4303, ci, i])
            off = int(rng.integers(0, S.GRID_WEEK_H)) if c[0] == 'H5' else 0
            res = run_cell(c, env, fees, 1.0, True, True, rand=True, rng=rng, n_events=n_ev, offset_h=off)
            t = train_t(res)[0]
            ts.append(float(t) if math.isfinite(t) else float('nan'))
        arr = np.array(ts)
        passed = int(S.step_a(np.where(np.isfinite(arr), arr, -9.0)).sum())
        out[S.cid(*c)] = {'passed': passed, 'n': n, 'median_t': float(np.nanmedian(arr)) if np.isfinite(arr).any() else None}
    return out


# ── шаг 3: TRAIN (BH) и VALID ───────────────────────────────────────────────────────────────────────
def step3(env, fees, cells):
    rows = {}
    for c in cells:
        res = run_cell(c, env, fees)
        t_tr, m_tr, n_tr = S.t_period(res['days'], res['r'], 'TRAIN')
        t_va, m_va, n_va = S.t_period(res['days'], res['r'], 'VALID')
        d_all, r_all = E.period_slice(res['days'], res['r'], 'TRAIN')
        d_v, r_v = E.period_slice(res['days'], res['r'], 'VALID')
        t_all = S.tstat(np.concatenate([r_all, r_v]))
        rows[S.cid(*c)] = {'train': {'t': t_tr, 'mean': m_tr, 'n': n_tr}, 'valid': {'t': t_va, 'mean': m_va, 'n': n_va}, 't_all': t_all, 'meta': res['meta'],
                           'sharpe_train': E.sharpe(r_all, 365)}
    ids = list(rows)
    passA = S.step_a([rows[i]['train']['t'] if math.isfinite(rows[i]['train']['t']) else float('nan') for i in ids])
    for i, a in zip(ids, passA):
        rows[i]['pass_a'] = bool(a)
        v = rows[i]['valid']
        rows[i]['pass_b'] = bool(a and v['mean'] > 0 and math.isfinite(v['t']) and v['t'] >= VALID_MIN_T)
    return rows


def pick_candidates(rows):
    ok = [i for i in rows if rows[i]['pass_b']]
    ok.sort(key=lambda i: -rows[i]['t_all'] if math.isfinite(rows[i]['t_all']) else 1e9)
    return ok[:MAX_FINALISTS]


# ── отчёт /battery ──────────────────────────────────────────────────────────────────────────────────
def fm(x, nd=2):
    return 'н/д' if x is None or (isinstance(x, float) and not math.isfinite(x)) else f'{x:.{nd}f}'


def bp(x):
    return 'н/д' if x is None or not math.isfinite(x) else f'{x * 1e4:+.2f}'


def cov_lines(env, cov, fees, version):
    P = env['P']
    L_ = [f'📊 <b>/battery — серия «батарея» (BOT_SPEC §2.43)</b> | код {version}; LOCKBOX НЕ читается',
          fees_note(fees),
          f'Данные D1: символов в панели {len(P["syms"])} (исключены как стейблкоин-подобные: {len(P["excluded_stable"])}' + (f' — {", ".join(P["excluded_stable"][:12])}{"…" if len(P["excluded_stable"]) > 12 else ""}' if P['excluded_stable'] else '')
          + f'), дней {P["nd"]} (до начала LOCKBOX); делистингованных в архиве по правилу ≥ {B.DELIST_MONTHS} мес.: {len(cov["delisted"])}'
          + ('' if cov['delisted'] else ' — ОШИБКА ВЫЖИВШЕГО: делистингованных символов в архиве нет, H1/H2 оценены по выжившим (оценка сверху)')]
    if env['hc'] is None:
        L_.append('⚠ часовые ряды H-XS не загружены (нет кэша 15m) — H3 и H5 не считаются')
    else:
        L_.append(f'Часовые ряды H-XS: {len(env["hc"]["grid"]["names"])} из 16' + (f'; нет кэша: {", ".join(env["missing16"])}' if env['missing16'] else '')
                  + (f'; нет фандинга: {", ".join(env["no_fund16"])}' if env.get('no_fund16') else '') + (f'; нет в панели D1: {", ".join(env["no_panel16"])}' if env.get('no_panel16') else ''))
    return L_


def build_report(env, cov, fees, version, s0, s1, v1, s2, s3, cands, stopped=None):
    R = cov_lines(env, cov, fees, version)
    R += ['', '<b>0. МОЩНОСТЬ (до расчёта доходностей; §2.43.4 п. 0)</b>',
          f't_BH = Φ⁻¹(1 − 0.05/8) = {fm(S.t_bh(S.N_PLANNED), 3)}; S_min = t_BH·√(365/D); «непроверяема», если S_min > {S.MAX_S_MIN:g}',
          'ячейка | D (дней TRAIN) | событий/сделок | S_min | вердикт']
    for i, v in s0.items():
        R.append(f'  {i} | {v["D"]} | {v["events"]} | {fm(v["s_min"])} | ' + ('ИСКЛЮЧЕНА (непроверяема)' if v['excluded'] else 'ок'))
    if s1 is not None:
        R += ['', f'<b>1. СМЕЩЕНИЕ ПРАВИЛ НА СИНТЕТИКЕ</b> (seed {SEED}, {BIAS_REPS} повторов, мартингал по цене, без издержек и фандинга; критерий: медианный |t| > {MEDIAN_ABS_MAX:g} или |t| > {TAIL_T:g} в ≥ {TAIL_SHARE * 100:.0f}% повторов)',
              'гипотеза | наблюдений | медиана |t| | доля |t|>3 | вердикт']
        for h, v in v1.items():
            R.append(f'  {h} | {v["n"]} | {fm(v["median_abs"])} | {v["share3"] * 100:.1f}% | ' + ('ИСКЛЮЧЕНА: ' + v['reasons'] if v['biased'] else 'норма'))
        diag = {k: x for k, x in s1.items() if k.endswith('~best') and x}
        for k, x in diag.items():
            R.append(f'  диагностика (НЕ для решений): {k[:-5]} при оптимистичном порядке внутри бара — медиана t {fm(float(np.median(x)))}, медиана |t| {fm(float(np.median(np.abs(x))))} '
                     f'(при худшем порядке: медиана t {fm(float(np.median(s1[k[:-5]])))}) — смещение H5 при худшем порядке задано правилом §2.43 U4, истина между двумя оценками')
    if s2 is not None:
        R += ['', f'<b>2. КАЛИБРОВКА</b> ({CALIB_N} случайных стратегий на ячейку, TRAIN, шаг A; СТОП при > {CALIB_MAX})', 'ячейка | прошло шаг A | медиана t']
        for i, v in s2.items():
            R.append(f'  {i} | {v["passed"]} из {v["n"]} | {fm(v["median_t"])}' + ('  ← СТОП: процедура ненадёжна' if v['passed'] > CALIB_MAX else ''))
    if stopped:
        R += ['', stopped]
        return '\n'.join(R)
    if s3 is not None:
        R += ['', '<b>3. TRAIN 2020–2023 (BH q = 0.05 по N) и VALID 2024</b> (mean — б.п./день капитала; t — меньший из NW лаг 5/20)',
              'ячейка | TRAIN mean, t | Шарп | VALID mean, t | A (BH) | B (VALID) | сделок/событий']
        for i, v in s3.items():
            m = v['meta']
            cnt = m.get('positions', m.get('opened', m.get('trades', m.get('grids', ''))))
            R.append(f'  {i} | {bp(v["train"]["mean"])}, t={fm(v["train"]["t"])} | {fm(v["sharpe_train"])} | {bp(v["valid"]["mean"])}, t={fm(v["valid"]["t"])} | '
                     f'{"да" if v["pass_a"] else "нет"} | {"да" if v["pass_b"] else "нет"} | {cnt}')
            extra = {k: m[k] for k in ('stops', 'skipped_cap', 'incomplete', 'delist', 'rebalances', 'max_w', 'fills') if k in m}
            if extra:
                R.append('     ' + ', '.join(f'{k} {fm(x, 1) if isinstance(x, float) else x}' for k, x in extra.items()))
        R += ['', '<b>Решение (§2.43.4 п. 4–5):</b> ' + ('кандидаты на финал (≤ 2): ' + ', '.join(cands) + '. Шаг 4 — ТОЛЬКО вручную: /battery_final confirm (один прогон на LOCKBOX, флаг серии).'
                                                      if cands else 'кандидатов нет (шаг A/B не пройден) → финал не запускается; по §2.43.4 п. 5: поиск закрыт для класса «публичные рыночные данные + розничный доступ» окончательно.')]
    return '\n'.join(R)


# ── команды ─────────────────────────────────────────────────────────────────────────────────────────
async def run_load(params, progress=None):
    """/battery_load: листинг символов, загрузка D1-свечей и fundingRate по месяцам с места остановки; бюджет времени max_minutes. → текст отчёта."""
    import aiohttp
    import research_ma as M
    d = params.get('bdir') or B.battery_dir()
    deadline = time.time() + float(params.get('max_minutes', 150)) * 60
    os.makedirs(d, exist_ok=True)
    head = [f'📥 <b>/battery_load</b> — Binance USDT-M (data.binance.vision): D1 + fundingRate → {d}; свободно {V.free_mb(d):.0f} МБ']
    async with aiohttp.ClientSession(headers=M.UA) as ses:
        fetch = V.make_fetcher(ses)
        try:
            r = await B.sync_all(fetch, d, progress=progress, deadline=deadline, refresh=bool(params.get('refresh')))
        except V.VisionError as e:
            return '\n'.join(head + [f'⛔ ЗАГРУЗКА ОСТАНОВЛЕНА: {e}'])
    cov = B.coverage(d)
    lines = head + [f'Символов в архиве (…USDT, без дат поставки): {cov["n_symbols"]}; завершено {cov["n_done"]} из {cov["n_symbols"]}; начато, но не завершено {len(cov["partial"])}; не начато {len(cov["not_started"])}; постоянных разрывов (месяцев) {cov["gaps"]}; '
                    f'первый месяц {cov["first_month"]}, последний {cov["last_month"]}',
                    f'Делистингованных (последний месяц ≥ {B.DELIST_MONTHS} мес. раньше последнего в архиве): {len(cov["delisted"])}'
                    + ('' if cov['delisted'] else ' — ОШИБКА ВЫЖИВШЕГО: в архиве нет делистингованных (или листинг неполон) — результаты H1/H2 — оценка сверху')]
    if cov['delisted']:
        lines.append('  примеры: ' + ', '.join(cov['delisted'][:15]) + ('…' if len(cov['delisted']) > 15 else ''))
    if cov['not_started']:
        lines.append(f'Не обработано загрузчиком: {len(cov["not_started"])} символов (например {", ".join(cov["not_started"][:5])}) — это НЕ отсутствие файлов, до них не дошла очередь')
    if cov['no_files']:
        lines.append(f'В листинге архива нет файлов D1: {len(cov["no_files"])} ({", ".join(cov["no_files"][:10])}…)')
    lines.append('⏱ бюджет времени исчерпан — повторите /battery_load (продолжит с места остановки)' if r['stopped'] else
                 ('✅ загрузка завершена по всем символам — можно /battery' if cov['n_done'] == cov['n_symbols'] else f'Не завершено символов: {len(cov["partial"])} — повторите /battery_load'))
    return '\n'.join(lines)


async def run_battery(params, progress=None):
    """/battery: шаги 0–3 (LOCKBOX не читается). Если флаг финала серии уже взят — показывает сохранённый отчёт, кандидатов не перезаписывает."""
    fdir = params.get('fdir') or RF.FD.factory_dir()
    d = params.get('bdir') or B.battery_dir()
    version = params.get('version', '')
    fees = fees_of(params)

    async def say(m):
        if progress:
            await progress(m)
    if RF.kv_get(KV_FLAG, fdir) or RF.kv_get(KV_DONE, fdir):
        st = RF.kv_get(KV_STATE, fdir) or {}
        return '🔒 /battery: флаг финала серии уже взят — результаты шагов 0–3 заморожены (кандидаты не перезаписываются).\n' + (st.get('report') or 'сохранённого отчёта нет')
    idx = B.load_index(d)
    cov = B.coverage(d, idx)
    if not idx['symbols'] or cov['n_done'] < cov['n_symbols']:
        return (f'⛔ /battery отказ: данные неполны (символов {cov["n_symbols"]}, завершено {cov["n_done"]}) — выполните /battery_load до сообщения «загрузка завершена»'
                + (f'. Не завершены: {", ".join(cov["partial"][:10])}' if cov['partial'] else ''))
    await say('сборка панелей D1 и часовых рядов H-XS (LOCKBOX не читается)…')
    env = build_env(dict(params, bdir=d), 'pre')
    if env is None:
        return '⛔ /battery отказ: в кэше нет ни одного дневного ряда'
    await say(f'шаг 0: мощность ({len(env["P"]["syms"])} символов)…')
    s0 = step0(env)
    cells = [c for c in S.CELLS if not s0[S.cid(*c)]['excluded']]
    s1 = v1 = s2 = s3 = None
    cands = []
    stopped = None
    if cells:
        await say(f'шаг 1: смещение правил на синтетике ({BIAS_REPS} повторов)…')
        s1 = {S.cid(*c): [] for c in cells}
        for r0 in range(0, BIAS_REPS, 10):
            part = step1(env, fees, cells, r0, min(BIAS_REPS, r0 + 10))
            for k_, v_ in part.items():
                s1.setdefault(k_, []).extend(v_)
            await say(f'шаг 1: повторов {min(BIAS_REPS, r0 + 10)}/{BIAS_REPS}')
        v1 = bias_verdict(s1)
        excl = {h for h, v in v1.items() if v['biased']}
        cells = [c for c in cells if c[0] not in excl]
    if cells:
        await say(f'шаг 2: калибровка ({CALIB_N} случайных на ячейку, {len(cells)} ячеек)…')
        s2 = {}
        for ci, c in enumerate(cells):
            s2.update(step2(env, fees, [c], CALIB_N, cell_offset=S.CELLS.index(c)))
            await say(f'шаг 2: ячейка {S.cid(*c)} ({ci + 1}/{len(cells)})')
        bad = [i for i, v in s2.items() if v['passed'] > CALIB_MAX]
        if bad:
            stopped = f'⛔ СТОП: калибровка не пройдена в ячейках {", ".join(bad)} (> {CALIB_MAX} из {CALIB_N} случайных прошли шаг A) — процедура отбора ненадёжна, результаты гипотез не показываются.'
    if cells and not stopped:
        await say('шаг 3: TRAIN/VALID…')
        s3 = step3(env, fees, cells)
        cands = pick_candidates(s3)
    N = len(cells)
    text = build_report(env, cov, fees, version, s0, s1, v1, s2, s3, cands, stopped)
    text += f'\nN после исключений: {N} из {S.N_PLANNED}.'
    RF.kv_set(KV_STATE, {'t': time.time(), 'report': text, 'N': N, 'cells': [S.cid(*c) for c in cells], 'step0': s0, 'bias': v1, 'calib': s2, 'version': version}, fdir)
    RF.kv_set(KV_CANDS, cands, fdir)
    return text


def evaluate_final(base, stress, months_ret, days, periods=365):
    """Критерии шага 4 (§2.43.4 п. 4) на LOCKBOX: Шарп ≥ 0.5, mean > 0 при t ≥ 2.0 (меньший из NW лаг 5/20), макс. просадка ≤ 15%, стресс ×2 mean > 0, mean ≥ 0 в месяцы роста и падения BTC."""
    lo = E.day_bounds('LOCKBOX')[0]
    m = base['days'] >= lo
    x = base['r'][m]
    mean, t5, n = E.nw_t(x, 5)
    t20 = E.nw_t(x, 20)[1]
    t = S.tstat(x)
    sh = E.sharpe(x, periods)
    dd = E.max_drawdown(x)
    xs = stress['r'][stress['days'] >= lo]
    ms = float(xs.mean()) if len(xs) else float('nan')
    grp = FF.month_group_means(base['days'], base['r'], months_ret)
    crit = {'t': bool(math.isfinite(t) and t >= L.FINAL_MIN_T and mean > 0), 'sharpe': bool(math.isfinite(sh) and sh >= L.FINAL_MIN_SHARPE), 'dd': bool(dd <= L.FINAL_MAX_DD),
            'stress': bool(math.isfinite(ms) and ms > 0),
            'up': (None if grp['up']['mean'] is None else bool(grp['up']['mean'] >= 0)), 'down': (None if grp['down']['mean'] is None else bool(grp['down']['mean'] >= 0))}
    return {'n': n, 'mean': mean, 't5': t5, 't20': t20, 't': t, 'sharpe': sh, 'dd': dd, 'stress_mean': ms, 'groups': grp, 'crit': crit,
            'pass': bool(all(v for v in crit.values() if v is not None) and n > 0)}


def final_report(version, fees, cands, evs, months):
    mark = lambda v: 'н/п' if v is None else ('да' if v else 'НЕТ')
    R = [f'🔒 <b>/battery_final — финал серии «батарея» на LOCKBOX (2025+), единственный прогон</b> | код {version}', fees_note(fees),
         'Критерии (все одновременно): Шарп ≥ 0.5; mean > 0 при t ≥ 2.0 (меньший из NW лаг 5/20); макс. просадка ≤ 15%; стресс ×2 mean > 0; mean ≥ 0 в месяцы роста и падения BTC.',
         f'Месяцев BTC в LOCKBOX: рост {sum(1 for x in months.values() if x > 0)}, падение {sum(1 for x in months.values() if x < 0)}', '']
    for c in cands:
        ev = evs[c]
        g = ev['groups']
        cr = ev['crit']
        R += [f'<b>{c}</b>: дней {ev["n"]}, mean {bp(ev["mean"])} б.п./день, t(лаг 5) {fm(ev["t5"])}, t(лаг 20) {fm(ev["t20"])}, t(критерий) {fm(ev["t"])}',
              f'  Шарп {fm(ev["sharpe"])} [{mark(cr["sharpe"])}], просадка {fm(ev["dd"] * 100, 1)}% [{mark(cr["dd"])}], t≥2 и mean>0 [{mark(cr["t"])}], стресс ×2 mean {bp(ev["stress_mean"])} [{mark(cr["stress"])}]',
              f'  месяцы роста BTC: {g["up"]["n_months"]} мес., mean {bp(g["up"]["mean"]) if g["up"]["mean"] is not None else "н/п"} [{mark(cr["up"])}]; падения: {g["down"]["n_months"]} мес., mean '
              f'{bp(g["down"]["mean"]) if g["down"]["mean"] is not None else "н/п"} [{mark(cr["down"])}]',
              f'  <b>{"ПРОЙДЕНО" if ev["pass"] else "НЕ ПРОЙДЕНО"}</b>', '']
    if any(evs[c]['pass'] for c in cands):
        R.append('Решение (§2.43.4 п. 5): прошло → shadow-форвард 8 недель (отдельный пре-коммит); торговли нет до форварда. Оговорки §2.43.6 (множественность, проклятие победителя, цены 2025+ частично известны) остаются.')
    else:
        R.append('Решение (§2.43.4 п. 5): НЕ ПРОЙДЕНО → поиск закрыт для класса «публичные рыночные данные + розничный доступ» окончательно (§2.32.8 без исключений); запись в RESEARCH_SUMMARY.md. '
                 '«Не пройдено» = «эффект не доказан на этом уровне», а не «эффекта нет» (§2.43.6).')
    return '\n'.join(R)


async def run_battery_final(params, progress=None):
    """/battery_final: без confirm — показ кандидатов и флага; с confirm — единственный прогон на LOCKBOX (флаг ДО чтения LOCKBOX, повтор запрещён навсегда)."""
    fdir = params.get('fdir') or RF.FD.factory_dir()
    d = params.get('bdir') or B.battery_dir()
    version = params.get('version', '')
    fees = fees_of(params)
    confirm = bool(params.get('confirm'))
    cands = RF.kv_get(KV_CANDS, fdir) or []
    flag = RF.kv_get(KV_FLAG, fdir) or RF.kv_get(KV_DONE, fdir)
    head = ['🔒 <b>/battery_final</b> (BOT_SPEC §2.43.4 п. 4): один прогон кандидатов (≤ 2) на LOCKBOX 2025+, повтор запрещён навсегда (флаг серии в БД; прежний флаг §2.41/§2.42 не затрагивается)',
            f'Кандидаты (из /battery): {", ".join(cands) or "нет"}', f'Флаг финала серии: {"УЖЕ ВЗЯТ — запуск невозможен" if flag else "свободен"}.']
    if not confirm:
        head.append('Запуск: /battery_final confirm')
        return '\n'.join(head)
    if flag:
        return '⛔ /battery_final отказ: флаг финала серии уже взят — финал выполняется один раз, повтор запрещён (§2.43.4 п. 4)'
    if not cands:
        return '⛔ /battery_final отказ: кандидатов нет — сначала /battery (флаг НЕ взят)'
    idx = B.load_index(d)
    cov = B.coverage(d, idx)
    if cov['n_done'] < cov['n_symbols']:
        return '⛔ /battery_final отказ: данные неполны (флаг НЕ взят)'
    if not RF.kv_set_if_absent(KV_FLAG, {'t': time.time(), 'cands': cands, 'version': version}, fdir):
        return '⛔ /battery_final отказ: флаг финала серии уже взят параллельным запуском'
    session = L.open_lockbox_session(final=True)
    if progress:
        await progress('LOCKBOX открыт (единственный прогон)…')
    env = build_env(dict(params, bdir=d), 'full', session)
    evs, months = {}, {}
    if env is not None:
        P = env['P']
        if 'BTCUSDT' in P['syms']:
            j = P['syms'].index('BTCUSDT')
            ts = (P['day0'] + np.arange(P['nd'])) * B.DAY_MS
            ok = np.isfinite(P['C'][:, j])
            months = FF.btc_months(ts[ok], P['C'][ok, j].astype(float), int(ts[-1]) + B.DAY_MS)
        for c in cands:
            cell = tuple(c.split('-', 1))
            base = run_cell(cell, env, fees, 1.0)
            stress = run_cell(cell, env, fees, 2.0)
            evs[c] = evaluate_final(base, stress, months, env['days'])
    RF.kv_set(KV_RESULT, {'evs': {c: {k: v for k, v in e.items()} for c, e in evs.items()}, 'version': version}, fdir)
    RF.kv_set(KV_DONE, True, fdir)
    if env is None:
        return '⛔ /battery_final: LOCKBOX прочитать не удалось — нет данных D1 (флаг финала серии взят)'
    return final_report(version, fees, cands, evs, months)
