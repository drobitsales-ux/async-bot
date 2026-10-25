"""research_fbias.py — проверка смещения правил «фабрики» на синтетических случайных блужданиях (BOT_SPEC §2.41.20). Выполняется ПЕРЕД калибровкой /factory: каждое семейство
по всем конфигурациям — на случайных блужданиях той же длины и волатильности, что реальные ряды ячейки (seed 20261025), 50 повторов на конфигурацию, без издержек.
Критерий смещения: медианный |t| > 1.0 или |t| > 3 хотя бы в 10% повторов. Решение принимается по ОБЪЕДИНЁННОЙ выборке всех единиц (ячейка × конфигурация) семейства;
нарушения отдельных единиц показываются справочно. Смещённое семейство исключается из N до шага A."""
import math

import numpy as np

import research_fengine as E

REPS = 50
MEDIAN_ABS_MAX = 1.0
TAIL_T = 3.0
TAIL_SHARE = 0.10
FAMILIES = ('T-MA', 'T-DC', 'T-MOM', 'R-RSI2', 'R-BB', 'B-VC', 'X-MOM', 'P-PAIR', 'L-LAG')


def real_stats(S):
    """σ лог-доходности close/close и средние выходы high/low за max/min(open, close) (в долях цены) реального ряда."""
    c, o, h, l = S['c'], S['o'], S['h'], S['l']
    lr = np.diff(np.log(c))
    lr = lr[np.isfinite(lr)]
    sigma = float(lr.std()) if len(lr) > 1 else 0.001
    hi = np.maximum(o, c)
    lo = np.minimum(o, c)
    eh = float(np.mean(np.clip((h - hi) / hi, 0, None)))
    el = float(np.mean(np.clip((lo - l) / lo, 0, None)))
    return sigma, eh, el


def synth_like(S, rng, stats=None, trade_from_ms=None):
    """Синтетический ряд той же длины, тех же временных меток и календаря (day), с σ реального ряда: доходности независимые нормальные, open = предыдущий close,
    high/low — выходы за max/min(open, close) по модулю нормальной величины (среднее — как у реального ряда). → ряд для движка."""
    sigma, eh, el = stats or real_stats(S)
    n = S['n']
    c0 = float(S['c'][0])
    c = c0 * np.exp(np.cumsum(rng.normal(0.0, sigma, n)))
    o = np.r_[c0, c[:-1]]
    sh, sl = eh * math.sqrt(math.pi / 2), el * math.sqrt(math.pi / 2)
    hi = np.maximum(o, c) * (1.0 + np.abs(rng.normal(0.0, sh, n)))
    lo = np.minimum(o, c) * np.maximum(1.0 - np.abs(rng.normal(0.0, sl, n)), 1e-6)
    return E.make_series(S['key'], {'ts': S['ts'], 'o': o, 'h': hi, 'l': lo, 'c': c, 'day': S['day']}, trade_from_ms, None)


def bias_cell(cell_idx, series, specs, run_fn, stat_fn, trade_from_ms, seed, reps=REPS, progress=None):
    """50 повторов по ячейке: для каждой конфигурации — t (Ньюи–Уэста, TRAIN) на синтетике без издержек. specs — [(id, семейство, spec)]; run_fn(spec, series) → dict(days, r);
    stat_fn(days, r) → t | None. → {id: [t…]} (None → пропущен)."""
    stats = [real_stats(S) for S in series]
    out = {cid: [] for cid, _f, _s in specs}
    for rep in range(reps):
        syn = [synth_like(S, np.random.default_rng([seed, cell_idx, rep, i]), stats[i], trade_from_ms) for i, S in enumerate(series)]
        for cid, _fam, spec in specs:
            res = run_fn(spec, syn)
            t = stat_fn(res['days'], res['r'])
            if t is not None and math.isfinite(t):
                out[cid].append(float(t))
    return out


def unit_stats(ts):
    a = np.abs(np.asarray(ts, float))
    if not len(a):
        return {'n': 0, 'median_abs': None, 'share3': None}
    return {'n': int(len(a)), 'median_abs': float(np.median(a)), 'share3': float(np.mean(a > TAIL_T))}


def verdict(per_cell, family_of):
    """per_cell = {ячейка: {id конфигурации: [t…]}}; family_of = {id: семейство}. → dict(families={сем: {...}}, units=[нарушения единиц], excluded=[семейства])."""
    pooled = {}
    units = []
    for ci, cfgs in per_cell.items():
        for cid, ts in cfgs.items():
            fam = family_of[cid]
            pooled.setdefault(fam, []).extend(ts)
            u = unit_stats(ts)
            if u['n'] and (u['median_abs'] > MEDIAN_ABS_MAX or u['share3'] >= TAIL_SHARE):
                units.append({'cell': ci, 'id': cid, 'family': fam, **u})
    fams = {}
    for fam in FAMILIES:
        if fam not in pooled:
            continue
        u = unit_stats(pooled[fam])
        reasons = []
        if u['n'] == 0:
            reasons.append('нет данных')
        else:
            if u['median_abs'] > MEDIAN_ABS_MAX:
                reasons.append(f'медианный |t| {u["median_abs"]:.2f} > {MEDIAN_ABS_MAX}')
            if u['share3'] >= TAIL_SHARE:
                reasons.append(f'доля |t| > {TAIL_T:g}: {u["share3"] * 100:.0f}% ≥ {TAIL_SHARE * 100:.0f}%')
        fams[fam] = dict(u, biased=bool(reasons) and u['n'] > 0, reasons='; '.join(reasons), units=sum(1 for cfgs in per_cell.values() for cid in cfgs if family_of[cid] == fam))
    return {'families': fams, 'units': units, 'excluded': [f for f, v in fams.items() if v['biased']]}


def report_section(vd, cell_names, reps=REPS, seed=20261025):
    """Первый раздел отчёта /factory."""
    L = [f'<b>0. ПРОВЕРКА СМЕЩЕНИЯ ПРАВИЛ НА СИНТЕТИКЕ (BOT_SPEC §2.41.20; случайные блуждания той же длины и σ, seed {seed}, {reps} повторов на конфигурацию, без издержек)</b>',
         f'Критерий: медианный |t| > {MEDIAN_ABS_MAX:g} или |t| > {TAIL_T:g} в ≥ {TAIL_SHARE * 100:.0f}% повторов — семейство смещено и исключается из N до шага A (решение по объединённой выборке семейства).',
         'семейство | единиц | наблюдений | медиана |t| | доля |t|>3 | вердикт']
    for fam, v in vd['families'].items():
        if not v['n']:
            L.append(f'  {fam} | {v["units"]} | 0 | н/д | н/д | нет данных')
            continue
        L.append(f'  {fam} | {v["units"]} | {v["n"]} | {v["median_abs"]:.2f} | {v["share3"] * 100:.1f}% | ' + ('ИСКЛЮЧЕНО: ' + v['reasons'] if v['biased'] else 'норма'))
    if vd['units']:
        L.append('  Единицы (ячейка, конфигурация) с нарушением критерия по отдельности (справочно; решение — по семейству):')
        for u in vd['units'][:40]:
            L.append(f'    {cell_names[u["cell"]]} {u["id"]}: медиана |t| {u["median_abs"]:.2f}, доля |t|>3 {u["share3"] * 100:.0f}%')
        if len(vd['units']) > 40:
            L.append(f'    … ещё {len(vd["units"]) - 40}')
    L.append('  Исключены из N: ' + (', '.join(vd['excluded']) if vd['excluded'] else 'нет'))
    return L
