"""research_h2final.py — ОДИНОЧНЫЙ ФИНАЛ H2-D3 (BOT_SPEC §2.43.11, пре-регистрация записана ДО кода): расчёт k (инфляция t на синтетике, LOCKBOX НЕ читается) и один прогон на LOCKBOX
по критериям §2.43.11 п. 3 с отдельным флагом `final_h2_started`. Реализация кандидата — та же `research_battery.run_cell(('H2','D3'))`, что и в `/battery` (без изменений)."""
import math
import time

import numpy as np

import research_bdata as B
import research_bsim as S
import research_battery as RB
import research_factory as RF
import research_fengine as E
import research_ffinal as FF
import research_ma_lib as L

CELL = ('H2', 'D3')
K_REPS = 200
K_SEED_TAG = 4401                              # generator: default_rng([SEED, K_SEED_TAG, повтор])
NULL_MEDIAN_ABS_T = 0.674
MIN_T_ADJ = 2.0
SPEC_K_KEY = 'FINALH2-K:'
KV_FLAG, KV_DONE, KV_RESULT, KV_K, KV_EXHAUSTED = 'final_h2_started', 'final_h2_done', 'final_h2_result', 'final_h2_k', 'lockbox_battery_exhausted'
DAY = B.DAY_MS
MIN_LOCK_DAYS = 50


def read_spec_k(path=None):
    """Значение k из строки `FINALH2-K: <число> …` файла BOT_SPEC → float | None (строки нет или «не записан»)."""
    try:
        with open(path or RF.SPEC_PATH, encoding='utf-8') as f:
            for ln in f:
                if ln.startswith(SPEC_K_KEY):
                    tok = ln.split(':', 1)[1].strip().split()
                    return float(tok[0]) if tok else None
    except (OSError, ValueError):
        return None
    return None


def lockbox_days(d):
    """(D_lock, первый день LOCKBOX) — число суток от начала LOCKBOX до конца последнего месяца архива. Только имена файлов индекса: цены и объёмы LOCKBOX не читаются."""
    idx = B.load_index(d)
    lasts = [idx['keys'][s]['d1'][-1] for s in idx['symbols'] if idx['keys'].get(s, {}).get('d1')]
    lock_day = L.lockbox_start_ms() // DAY
    if not lasts:
        return 0, lock_day
    y, m = max(lasts).split('-')
    ny, nm = (int(y) + 1, 1) if int(m) == 12 else (int(y), int(m) + 1)
    return max(0, B._month_start_day(f'{ny}-{nm:02d}') - lock_day), lock_day


def make_env(P):
    return {'P': P, 'ctx': S.make_ctx(P), 'days': np.arange(P['day0'], P['day0'] + P['nd'], dtype=np.int64)}


def compute_k(params, reps=K_REPS, progress=None):
    """§2.43.11 п. 2: k = max(1, m0/0.674), m0 — медианный |t| (меньший из NW лаг 5/20, U5) ячейки H2-D3 на синтетике нулевой гипотезы (`synth_daily_panel`, как шаг 1 `/battery`): календарь,
    ликвидность и длительности — реальные ДО LOCKBOX; измеряемый участок — последние D_lock суток перед LOCKBOX; без издержек и фандинга. LOCKBOX не читается (панель mode='pre').
    → dict(k, m0, n_lock, reps, n_events, t[])."""
    d = params.get('bdir') or B.battery_dir()
    fees = RB.fees_of(params)
    d_lock, lock_day = lockbox_days(d)
    if d_lock < MIN_LOCK_DAYS:
        return {'k': None, 'm0': None, 'n_lock': d_lock, 'reps': 0, 'error': 'в индексе архива нет месяцев LOCKBOX (или их слишком мало) для длины участка'}
    P = B.load_panel(d, 'pre')
    if P is None:
        return {'k': None, 'm0': None, 'n_lock': d_lock, 'reps': 0, 'error': 'нет дневных рядов в кэше'}
    env = make_env(P)
    n_events = len(S.h2_events(env['ctx'], int(S.CELL_PARAM[CELL[1]])))
    seg = (env['days'] >= lock_day - d_lock) & (env['days'] < lock_day)
    ts = []
    for rep in range(reps):
        rng = np.random.default_rng([RF.SEED, K_SEED_TAG, rep])
        syn = {'P': S.synth_daily_panel(P, rng)}
        res = RB.run_cell(CELL, env, fees, 1.0, False, False, synth=syn)
        t = S.tstat(res['r'][seg])
        if math.isfinite(t):
            ts.append(float(t))
        if progress and (rep + 1) % 50 == 0:
            progress(f'k: повторов {rep + 1}/{reps}')
    m0 = float(np.median(np.abs(ts))) if ts else float('nan')
    k = max(1.0, m0 / NULL_MEDIAN_ABS_T) if math.isfinite(m0) else None
    return {'k': k, 'm0': m0, 'n_lock': d_lock, 'reps': len(ts), 'n_events': n_events, 't': ts}


# ── критерии LOCKBOX ─────────────────────────────────────────────────────────────────────────────────
def evaluate(base, stress, k, months_ret, periods=365):
    """Критерии §2.43.11 п. 3 по базовому и стресс-прогону (дни LOCKBOX)."""
    lo = E.day_bounds('LOCKBOX')[0]
    m = base['days'] >= lo
    x = base['r'][m]
    mean, t5, n = E.nw_t(x, 5)
    t20 = E.nw_t(x, 20)[1]
    t = S.tstat(x)
    t_adj = t / k if math.isfinite(t) else float('nan')
    sh = E.sharpe(x, periods)
    dd = E.max_drawdown(x)
    xs = stress['r'][stress['days'] >= lo]
    ms = float(xs.mean()) if len(xs) and len(xs) == len(x) else float('nan')
    grp = FF.month_group_means(base['days'], base['r'], months_ret)
    crit = {'t_adj': bool(math.isfinite(t_adj) and t_adj >= MIN_T_ADJ and mean > 0), 'sharpe': bool(math.isfinite(sh) and sh >= L.FINAL_MIN_SHARPE), 'dd': bool(dd <= L.FINAL_MAX_DD),
            'stress': bool(math.isfinite(ms) and ms > 0),
            'up': (None if grp['up']['mean'] is None else bool(grp['up']['mean'] >= 0)), 'down': (None if grp['down']['mean'] is None else bool(grp['down']['mean'] >= 0))}
    return {'n': n, 'mean': mean, 't': t, 't5': t5, 't20': t20, 't_adj': t_adj, 'k': k, 'sharpe': sh, 'dd': dd, 'stress_mean': ms, 'groups': grp, 'crit': crit,
            'pass': bool(all(v for v in crit.values() if v is not None) and n > 0)}


def reference(env, base, fees):
    """Справочно (не критерии): по годам, листинги LOCKBOX, открытые позиции, доля стопов +50%, пропуски по лимиту риска, делистинги."""
    P, ctx = env['P'], env['ctx']
    lo = E.day_bounds('LOCKBOX')[0]
    lock_idx = lo - P['day0']
    p = int(S.CELL_PARAM[CELL[1]])
    ev_all = S.h2_events(ctx, p)
    pre = [x for x in ev_all if x[0] < lock_idx]
    m_full = S.sim_h2(ctx, ev_all, fees)['meta']
    m_pre = S.sim_h2(ctx, pre, fees)['meta']
    diff = {k: m_full[k] - m_pre[k] for k in ('events', 'opened', 'skipped_cap', 'incomplete', 'stops', 'delist')}
    n_list = int(sum(1 for j in range(len(P['syms'])) if int(P['first'][j]) > 0 and int(P['first'][j]) >= lock_idx))
    from datetime import datetime, timezone
    years = {}
    days, r = base['days'], base['r']
    yrs = np.array([datetime.fromtimestamp(int(dd) * DAY / 1000, timezone.utc).year for dd in days])
    for y in sorted(set(yrs[days >= lo])):
        mm = (days >= lo) & (yrs == y)
        years[int(y)] = {'n': int(mm.sum()), 'mean': float(r[mm].mean() * 1e4)}
    return {'years': years, 'listings': n_list, 'meta': diff, 'stop_share': (diff['stops'] / diff['opened'] if diff['opened'] else None)}


# ── команда ──────────────────────────────────────────────────────────────────────────────────────────
def fm(x, nd=2):
    return 'н/д' if x is None or (isinstance(x, float) and not math.isfinite(x)) else f'{x:.{nd}f}'


def spec_summary():
    return ['<b>§2.43.11 — одиночный финал H2-D3</b> (BOT_SPEC §2.43.11, пре-регистрация записана ДО кода; исключение из §2.43.4 п. 5 по решению владельца 2026-10-29)',
            'Кандидат: H2-D3 в реализации /battery без изменений (шорт нового листинга по close дня 3, удержание 30 суток, стоп +50%, риск 0.5%, не более 10% капитала в риске). N = 1.',
            'Критерии на LOCKBOX (все одновременно): t_adj = t/k ≥ 2.0 (t — меньший из NW лаг 5/20) и mean &gt; 0; годовой Шарп ≥ 0.5; макс. просадка ≤ 15%; стресс ×2 (комиссии и проскальзывание) mean &gt; 0; '
            'mean ≥ 0 отдельно в месяцы роста и в месяцы падения BTC.',
            'Справочно: по годам, число листингов, доля стопов +50%, пропуски по лимиту риска.',
            'Решение: пройдено → отдельный пре-коммит shadow-форварда на листингах BingX (без денег); не пройдено → запись «LOCKBOX battery исчерпан».',
            'Оговорки: кандидат — лучшая из 8 ячеек по TRAIN и не прошёл BH (проклятие победителя не учтено порогом); мощность низкая (≈ 20% при истинном mean как на TRAIN — грубая оценка); k считается на последних D_lock сутках ДО LOCKBOX.']


async def run_final_h2(params, progress=None):
    """/final_h2: без confirm — показ §2.43.11 и k (LOCKBOX не читается); confirm — единственный прогон на LOCKBOX с флагом `final_h2_started` (берётся ДО открытия сессии)."""
    fdir = params.get('fdir') or RF.FD.factory_dir()
    d = params.get('bdir') or B.battery_dir()
    version = params.get('version', '')
    fees = RB.fees_of(params)
    confirm = bool(params.get('confirm'))
    flag = RF.kv_get(KV_FLAG, fdir) or RF.kv_get(KV_DONE, fdir)

    async def say(m):
        if progress:
            await progress(m)
    idx = B.load_index(d)
    cov = B.coverage(d, idx)
    suffix = ' (флаг НЕ взят)' if confirm else ''
    if not idx['symbols'] or cov['n_done'] < cov['n_symbols']:
        return f'⛔ /final_h2 отказ: данные неполны (символов {cov["n_symbols"]}, завершено {cov["n_done"]}) — выполните /battery_load{suffix}'
    await say(f'расчёт k: синтетика {K_REPS} повторов, LOCKBOX не читается…')
    kres = compute_k(dict(params, bdir=d), K_REPS)
    if kres['k'] is None:
        return f'⛔ /final_h2 отказ: k не рассчитан — {kres.get("error")}{suffix}'
    RF.kv_set(KV_K, {k_: v for k_, v in kres.items() if k_ != 't'}, fdir)
    spec_k = read_spec_k(params.get('spec_path'))
    k_ok = spec_k is not None and round(spec_k, 3) == round(kres['k'], 3)
    head = spec_summary() + ['',
                             f'<b>k = {kres["k"]:.3f}</b> (m0 = {kres["m0"]:.3f}; k = max(1, m0/{NULL_MEDIAN_ABS_T}); {kres["reps"]} повторов, событий H2-D3 на пре-панели {kres["n_events"]}, '
                             f'измеряемый участок {kres["n_lock"]} суток перед LOCKBOX; seed {RF.SEED}).',
                             f'Строка в BOT_SPEC: {SPEC_K_KEY} {("%.3f" % spec_k) if spec_k is not None else "не записана"} — ' + ('СОВПАДАЕТ с расчётом.' if k_ok else
                                                                                                          'НЕ совпадает/не записана: пришлите k, я запишу его в BOT_SPEC отдельным коммитом; без совпадения confirm не запустится.'),
                             f'Флаг финала H2: {"УЖЕ ВЗЯТ — запуск невозможен" if flag else "свободен"}.']
    if not confirm:
        head.append('Запуск: /final_h2 confirm (один прогон на LOCKBOX, повтор запрещён).')
        return '\n'.join(head)
    if flag:
        return '⛔ /final_h2 отказ: флаг финала H2 уже взят — финал выполняется один раз, повтор запрещён (BOT_SPEC §2.43.11 п. 4)'
    if not k_ok:
        return '⛔ /final_h2 отказ: k не записан в BOT_SPEC или не совпадает с расчётом (флаг НЕ взят)\n' + '\n'.join(head[-3:])
    if not RF.kv_set_if_absent(KV_FLAG, {'t': time.time(), 'kind': 'final_h2', 'k': kres['k'], 'version': version}, fdir):
        return '⛔ /final_h2 отказ: флаг финала H2 уже взят параллельным запуском'
    session = L.open_lockbox_session(final=True)
    await say('LOCKBOX открыт (единственный прогон)…')
    P = B.load_panel(d, 'full', session)
    if P is None:
        RF.kv_set(KV_DONE, True, fdir)
        return '⛔ /final_h2: LOCKBOX прочитать не удалось — нет данных D1 (флаг финала H2 взят)'
    env = make_env(P)
    base = RB.run_cell(CELL, env, fees, 1.0)
    stress = RB.run_cell(CELL, env, fees, 2.0)
    months = {}
    if 'BTCUSDT' in P['syms']:
        j = P['syms'].index('BTCUSDT')
        ts = (P['day0'] + np.arange(P['nd'])) * DAY
        ok = np.isfinite(P['C'][:, j])
        months = FF.btc_months(ts[ok], P['C'][ok, j].astype(float), int(ts[-1]) + DAY)
    ev = evaluate(base, stress, kres['k'], months)
    ref = reference(env, base, fees)
    RF.kv_set(KV_RESULT, {'eval': ev, 'ref': ref, 'months': {str(k_): v for k_, v in months.items()}, 'k': kres['k'], 'version': version}, fdir)
    RF.kv_set(KV_DONE, True, fdir)
    if not ev['pass']:
        RF.kv_set(KV_EXHAUSTED, {'t': time.time(), 'by': 'final_h2'}, fdir)
    return build_final_report(version, head, ev, ref, months, base['meta'])


def build_final_report(version, head, ev, ref, months, meta):
    c = ev['crit']
    g = ev['groups']
    mark = lambda v: 'н/п' if v is None else ('да' if v else 'НЕТ')
    bp = lambda x: fm(x * 1e4 if x is not None and math.isfinite(x) else None)
    R = [f'🔒 <b>/final_h2 — одиночный финал H2-D3 на LOCKBOX (2025+), единственный прогон</b> | код {version}', *head[-3:], '',
         f'<b>РЕЗУЛЬТАТ: {"ПРОЙДЕНО" if ev["pass"] else "НЕ ПРОЙДЕНО"}</b> (дней {ev["n"]}, k = {ev["k"]:.3f})',
         f'  t_adj = {fm(ev["t_adj"])} (t = {fm(ev["t"])}; NW лаг 5 = {fm(ev["t5"])}, лаг 20 = {fm(ev["t20"])}) ≥ 2.0 и mean &gt; 0: {mark(c["t_adj"])}',
         f'  годовой Шарп = {fm(ev["sharpe"])} ≥ 0.5: {mark(c["sharpe"])}',
         f'  макс. просадка = {ev["dd"] * 100:.1f}% ≤ 15%: {mark(c["dd"])}',
         f'  mean при издержках ×2 = {bp(ev["stress_mean"])} б.п./день &gt; 0: {mark(c["stress"])}',
         f'  mean в месяцы роста BTC ({g["up"]["n_months"]} мес., {g["up"]["n_days"]} дн.) = {bp(g["up"]["mean"])} б.п./день ≥ 0: {mark(c["up"])}',
         f'  mean в месяцы падения BTC ({g["down"]["n_months"]} мес., {g["down"]["n_days"]} дн.) = {bp(g["down"]["mean"])} б.п./день ≥ 0: {mark(c["down"])}',
         f'  mean (весь LOCKBOX) = {bp(ev["mean"])} б.п./день', '',
         f'<b>Справочно (не критерии)</b>: листингов в LOCKBOX {ref["listings"]}, допущено по ликвидности {ref["meta"]["events"]}, открыто позиций со входом в LOCKBOX {ref["meta"]["opened"]}, из них стопов +50%: {ref["meta"]["stops"]} '
         f'({fm(ref["stop_share"] * 100 if ref["stop_share"] is not None else None, 0)}%), пропусков по лимиту риска {ref["meta"]["skipped_cap"]}, делистингов {ref["meta"]["delist"]}, не завершено к концу данных {ref["meta"]["incomplete"]}',
         '  по годам: ' + '; '.join(f'{y}: {fm(v["mean"])} б.п./день ({v["n"]} дн.)' for y, v in ref['years'].items()),
         '  месяцы BTC (UTC → доходность): ' + ', '.join(f'{(m_ // 12)}-{m_ % 12 + 1:02d}: {x * 100:+.1f}%' for m_, x in sorted(months.items())), '']
    if ev['pass']:
        R.append('Решение (§2.43.11 п. 4): пройдено → отдельный пре-коммит shadow-форварда H2-D3 на листингах BingX (без денег); торговли нет. Оговорки (проклятие победителя, мощность) остаются.')
    else:
        R.append('Решение (§2.43.11 п. 4): <b>LOCKBOX battery исчерпан</b> (флаг lockbox_battery_exhausted записан в БД). «Не пройдено» = «эффект не доказан на этом уровне».')
    return '\n'.join(R)
