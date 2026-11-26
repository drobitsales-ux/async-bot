"""research_h2repl.py — H2: проверка фандинга (`/h2_funding_check`) и РЕПЛИКАЦИЯ на независимых монетах Bybit/BingX (`/h2_replicate`), BOT_SPEC §2.47. Кандидат — ТА ЖЕ функция
`research_battery.run_cell(('H2', 'D3'))` на другой панели (правила H2-D3 без изменений, фандинг учитывается). LOCKBOX батареи (исчерпан, §2.43.20) не читается."""
import inspect
import math
import time
from datetime import datetime, timezone

import os

import numpy as np

import research_battery as RB
import research_bdata as B
import research_bsim as S
import research_factory as RF
import research_fengine as E
import research_h2final as H2F
import research_xdata as X

CELL = ('H2', 'D3')
SEED = 20261025
K_TAG = 4601
K_REPS = 200
NULL_MEDIAN_ABS_T = 0.674
MIN_T_ADJ = 2.0
MIN_SHARPE, MAX_DD = 0.5, 0.15
SPEC_K_KEY, SPEC_U_KEY = 'H2REPL-K:', 'H2REPL-UNIVERSE:'
KV_STARTED, KV_DONE, KV_RESULT, KV_REPORT, KV_PASSED, KV_K = 'h2_repl_started', 'h2_repl_done', 'h2_repl_result', 'h2_repl_report', 'h2_repl_passed', 'h2_repl_k'
LOCK_MEAN_OWNER = 3.13e-4                # §2.43.20: весь LOCKBOX +3.13 б.п./день (с фандингом) — со слов владельца
DAY = X.DAY


def fm(x, nd=2):
    return 'н/д' if x is None or (isinstance(x, float) and not math.isfinite(x)) else f'{x:.{nd}f}'


def bp(x):
    return 'н/д' if x is None or not math.isfinite(x) else f'{x * 1e4:+.2f}'


def spec_value(key, path=None):
    """Остаток строки `<KEY> …` из BOT_SPEC (последняя такая строка) → str | None."""
    val = None
    try:
        with open(path or RF.SPEC_PATH, encoding='utf-8') as f:
            for ln in f:
                if ln.startswith(key):
                    val = ln[len(key):].strip()
    except OSError:
        return None
    return val


def read_spec_k(path=None):
    v = spec_value(SPEC_K_KEY, path)
    try:
        return float(v.split()[0]) if v else None
    except ValueError:
        return None


# ── ШАГ 1.0: фандинг в исходном H2 ───────────────────────────────────────────────────────────────────
def code_evidence():
    """Проверка по исходнику: учитывает ли sim_h2 фандинг шорта и вызывает ли run_cell его с fund_on=True по умолчанию. → dict."""
    src = inspect.getsource(S.sim_h2)
    rc = inspect.getsource(RB.run_cell)
    sig = inspect.signature(RB.run_cell).parameters['fund_on'].default
    return {'accrues': ('+= w * float(F[k, s])' in src and 'if fund_on' in src), 'default_on': bool(sig is True), 'h2_branch': ('S.sim_h2(c2, ev, fees, mult, fund_on, costs_on)' in rc)}


def period_stats(res, name):
    d, x = E.period_slice(res['days'], res['r'], name)
    mean, _t5, n = E.nw_t(x, 5)
    return {'mean': mean, 't': S.tstat(x), 'sharpe': E.sharpe(x, 365), 'dd': E.max_drawdown(x), 'n': n}


def listing_funding(P, ctx, p=3):
    """Фандинг за первые 30 суток после листинга для допущенных событий H2: сумма ставок на единицу номинала (шорт получает при > 0) и среднее на расчёт (GC — число расчётов)."""
    ev = S.h2_events(ctx, p)
    sums, per = [], []
    for e, s in ev:
        if e + 31 > P['nd']:
            continue
        sums.append(float(P['F'][e + 1:e + 31, s].sum()))
        gc = float(P['GC'][e + 1:e + 31, s].sum()) if 'GC' in P else 0.0
        if gc > 0:
            per.append(float(P['G'][e + 1:e + 31, s].sum()) / gc)
    return {'n': len(sums), 'mean_sum': (float(np.mean(sums)) if sums else None), 'median_sum': (float(np.median(sums)) if sums else None), 'mean_per_event': (float(np.mean(per)) if per else None),
            'share_pos': (float(np.mean(np.array(sums) > 0)) if sums else None)}


def funding_check(params, progress=None):
    """`/h2_funding_check`: H2-D3 на TRAIN и VALID «без фандинга / с фандингом»; LOCKBOX не читается; решение владельца по mean VALID+LOCKBOX с фандингом."""
    d = params.get('bdir') or B.battery_dir()
    fees = RB.fees_of(params)
    ev = code_evidence()
    P = B.load_panel(d, 'pre')
    if P is None:
        return '⛔ /h2_funding_check: нет кэша /battery_load'
    Pg = {'F': P['F'], 'G': P['G'], 'GC': P['GC'], 'nd': P['nd']}          # ссылки до освобождения матриц в make_env_final
    env = H2F.make_env_final(P)
    fund_stats = listing_funding(Pg, env['ctx'])
    runs = {fo: RB.run_cell(CELL, env, fees, 1.0, fund_on=fo) for fo in (False, True)}
    R = ['🔎 <b>/h2_funding_check — фандинг в исходном H2-D3 (BOT_SPEC §2.47.1)</b> | LOCKBOX не читается (исчерпан, §2.43.20)',
         f'Проверка по исходнику: `sim_h2` начисляет фандинг шорта: {"ДА" if ev["accrues"] else "НЕТ"}; `run_cell` вызывает `sim_h2` с fund_on={"True" if ev["default_on"] else "False"} по умолчанию; ветка H2 — {"та же" if ev["h2_branch"] else "ИНАЯ"}.',
         '→ ' + ('фандинг в исходном H2 УЧИТЫВАЛСЯ (шорт получает при положительной ставке, платит при отрицательной); условие пересчёта «если не учитывался» не выполнено.' if ev['accrues'] and ev['default_on'] and ev['h2_branch'] else '⚠ фандинг мог не учитываться — требуется ручная проверка'),
         'период | без фандинга: mean б.п./день, t, Шарп, просадка | с фандингом: mean, t, Шарп, просадка']
    out = {}
    for name in ('TRAIN', 'VALID'):
        a, b = period_stats(runs[False], name), period_stats(runs[True], name)
        out[name] = (a, b)
        R.append(f'  {name} | {bp(a["mean"])}, t={fm(a["t"])}, {fm(a["sharpe"])}, {a["dd"] * 100:.1f}% | {bp(b["mean"])}, t={fm(b["t"])}, {fm(b["sharpe"])}, {b["dd"] * 100:.1f}%')
    R.append('  LOCKBOX | не пересчитывался (не читается); сообщённые владельцем числа §2.43.20 — уже С фандингом: mean +3.13 б.п./день, Шарп 1.18, просадка 10.1%')
    if fund_stats['n']:
        R.append(f'Фандинг за первые 30 суток после листинга (допущенные листинги TRAIN+VALID, n={fund_stats["n"]}): средняя сумма ставок {fund_stats["mean_sum"] * 100:+.3f}% номинала '
                 f'(медиана {fund_stats["median_sum"] * 100:+.3f}%, доля > 0 {fund_stats["share_pos"] * 100:.0f}%), среднее на расчёт {fund_stats["mean_per_event"] * 1e4 if fund_stats["mean_per_event"] is not None else float("nan"):+.2f} б.п.; '
                 '(знак «+» — шорт получает).')
    # решение: mean VALID + LOCKBOX с фандингом (средневзвешенное по суткам)
    fdir = params.get('fdir') or RF.FD.factory_dir()
    res = RF.kv_get('final_h2_result', fdir) or {}
    ev_l = (res.get('ev') or res.get('eval') or {}) if isinstance(res, dict) else {}
    lock_mean = ev_l.get('mean') if isinstance(ev_l.get('mean'), (int, float)) else LOCK_MEAN_OWNER
    lock_n = ev_l.get('n') if isinstance(ev_l.get('n'), (int, float)) else H2F.lockbox_days(d)[0]
    v = out['VALID'][1]
    comb = (v['mean'] * v['n'] + lock_mean * lock_n) / max(1, v['n'] + lock_n)
    R.append(f'Решение (§2.47.1): mean VALID с фандингом {bp(v["mean"])} (дней {v["n"]}), LOCKBOX {bp(lock_mean)} (дней {int(lock_n)}) → средневзвешенное {bp(comb)} б.п./день — '
             + ('<b>> 0: репликация разрешена</b> (/h2_replicate load → /h2_replicate).' if comb > 0 else '<b>≤ 0: H2 ЗАКРЫТ, репликация не запускается</b>.'))
    RF.kv_set('h2_funding_check', {'comb': comb, 'allow': bool(comb > 0), 't': time.time()}, fdir)
    return '\n'.join(R)


# ── ШАГ 1.1–1.2: вселенная, панель, k, критерии ──────────────────────────────────────────────────────
def repl_inputs(xdir, bdir=None, venues=None, ext=None):
    """Вселенная и ленивые ряды: → (series, fund, uni, ref, where, mk). Общая часть `build_repl_panel` и лёгкой панели `research_h2ext.build_lean` (v132, §2.62): правило вселенной то же."""
    idx = X.load_index(xdir)
    vn = tuple(venues) if venues else X.venues_from_spec()                                          # [v118] H2REPL-VENUES (BOT_SPEC §2.50): без маркера — обе площадки
    mk = {ex: ({s: dict(v) for s, v in idx['markets'].get(ex, {}).items()} if ex in vn else {}) for ex in X.EXCHANGES}
    def keys_of(ex):                                                   # v123 (§2.55): ключи из индекса (запись есть, ряд непуст, файл существует) — ряды читаются лениво по одному
        return [s_ for s_, r in idx['bars'].get(ex, {}).items() if int(r.get('n', 1) or 0) > 0 and os.path.exists(X.bars_path(xdir, ex, s_))]
    bars = {ex: X.LazyStore(keys_of(ex) if ex in vn else [], lambda s_, ex=ex: X.load_bars(xdir, ex, s_)) for ex in X.EXCHANGES}
    if ext is not None:                                                # [v134, §2.64.2] H2EXT: механическое исключение не-крипто (а → б → в → г); а — §2.52 (excl из индекса) + поля метаданных (§2.65.2)
        for ex in vn:
            for sym, why in ((ext.get('meta') or {}).get(ex) or {}).items():
                m = mk[ex].get(sym)
                if m is not None and not m.get('excl'):
                    m['excl'] = why
            X.apply_ext_filter(mk[ex], (ext.get('coins') or {}).get(ex))
        amap = X.binance_alias_map(ext['binance_bases'] if ext.get('binance_bases') is not None else X.binance_raw_bases(bdir))      # [v136, §2.66.2] binance-alias — после а → б → в → г
        for ex in vn:
            X.apply_alias(mk[ex], amap)
    uni, ref = X.h2_universe(mk, bars, X.binance_norm_set(bdir))
    if ext is not None:                                                # [v136, §2.66.3] дедупликация одной монеты под разными тикерами (по цене, без P&L)
        groups, dropped = X.dedupe_universe(uni, bars)
        ref['duplicates'] = groups
        keep_of = {d['name']: g['keep'] for g in groups for d in g['drop']}
        for ex_, sym_ in sorted(dropped):
            ref['excluded_syms'].append((ex_, sym_, f'дубликат={keep_of[f"{ex_}:{sym_}"]}'))
        uni = [u for u in uni if (u['ex'], u['sym']) not in dropped]
    where = {f'{u["ex"]}:{u["sym"]}': (u['ex'], u['sym']) for u in uni}
    series = X.LazyStore(list(where), lambda name: bars[where[name][0]][where[name][1]])
    fk = [nm for nm, (ex_, s_) in where.items() if os.path.exists(X.fund_path(xdir, ex_, s_))]
    fund = X.LazyStore(fk, lambda name: X.load_fund(xdir, where[name][0], where[name][1]))
    return series, fund, uni, ref, where, mk


def end_data_flags(names, where, mk):
    """[v131, §2.61.3] BingX/OKX: конец кэша — «конец данных», делистинг — только неактивные в markets."""
    return np.array([where[nm][0] in ('bingx', 'okx') and (mk[where[nm][0]].get(where[nm][1]) or {}).get('active', True) is not False for nm in names], bool)


def build_repl_panel(xdir, bdir=None, venues=None):
    """Загруженный кэш → (панель, вселенная, справка). Имена символов в панели — `биржа:символ`."""
    series, fund, uni, ref, where, mk = repl_inputs(xdir, bdir, venues)
    P = X.panel_from_series(series, fund)
    if P is not None:
        P['end_data'] = end_data_flags(P['syms'], where, mk)
    if P is not None:                                                  # [v119, §2.51] первый день с фандингом по символу (−1 — истории нет): счётчик «фандинг = 0»
        P['fund_first'] = np.array([(int(fund[nm][0][0]) // DAY - P['day0']) if (nm in fund and len(fund[nm][0])) else -1 for nm in P['syms']], np.int64)
    return P, uni, ref


def make_env(P):
    return {'P': P, 'ctx': S.make_ctx(P), 'days': np.arange(P['day0'], P['day0'] + P['nd'], dtype=np.int64), 'hc': None}


def first_event_day(env):
    ev = S.h2_events(env['ctx'], 3)
    return (min(e for e, _s in ev) if ev else None), ev


def power(env, fees):
    """Шаг 0: D — число суток с открытыми позициями; S_min = 2.0·√(365/D); S_min > 2.0 → «непроверяема»."""
    res = RB.run_cell(CELL, env, fees, 1.0)
    D = int(res['meta'].get('days_open', 0))
    sm = 2.0 * math.sqrt(365.0 / D) if D > 0 else float('inf')
    return {'D': D, 's_min': sm, 'testable': bool(D > 0 and sm <= 2.0), 'meta': res['meta']}


def compute_k(env, fees, reps=K_REPS, progress=None):
    """§2.47.3 п. 1: k = max(1, m0/0.674) по синтетике ЭТОЙ панели (мартингал, σ реальных рядов; без издержек и фандинга); ряд суток — от первых допущенных входов до конца панели."""
    e0, ev = first_event_day(env)
    if e0 is None:
        return {'k': None, 'm0': None, 'reps': 0, 'n_events': 0, 'error': 'нет допущенных листингов'}
    seg = np.arange(env['P']['nd']) >= e0
    ts = []
    for rep in range(reps):
        rng = np.random.default_rng([SEED, K_TAG, rep])
        syn = {'P': S.synth_daily_panel(env['P'], rng)}
        res = RB.run_cell(CELL, env, fees, 1.0, False, False, synth=syn)
        t = S.tstat(res['r'][seg])
        if math.isfinite(t):
            ts.append(float(t))
        if progress and (rep + 1) % 50 == 0:
            progress(f'k: повторов {rep + 1}/{reps}')
    m0 = float(np.median(np.abs(ts))) if ts else float('nan')
    return {'k': (max(1.0, m0 / NULL_MEDIAN_ABS_T) if math.isfinite(m0) else None), 'm0': m0, 'reps': len(ts), 'n_events': len(ev), 'n_days': int(seg.sum())}


def btc_months_all(ts, c):
    """Месячные доходности BTC по календарным месяцам UTC за ВЕСЬ период (close последнего дня месяца / close последнего дня предыдущего − 1); неполный месяц исключён."""
    ts = np.asarray(ts)
    c = np.asarray(c, float)
    mk = np.array([_month_key(t) for t in ts])
    out = {}
    for m in sorted(set(mk.tolist())):
        nxt = m + 1
        start_next = int(datetime(nxt // 12, nxt % 12 + 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
        if int(ts[-1]) + DAY < start_next:
            continue
        idx, prev = np.flatnonzero(mk == m), np.flatnonzero(mk == m - 1)
        if len(idx) and len(prev):
            out[m] = float(c[idx[-1]] / c[prev[-1]] - 1.0)
    return out


def _month_key(ts_ms):
    t = datetime.fromtimestamp(int(ts_ms) / 1000, timezone.utc)
    return t.year * 12 + t.month - 1


def month_means(days, r, months_ret):
    m_of = np.array([_month_key(int(d) * DAY) for d in days])
    res = {}
    for name, cond in (('up', lambda x: x > 0), ('down', lambda x: x < 0)):
        ms = [m for m, x in months_ret.items() if cond(x)]
        mask = np.isin(m_of, ms)
        res[name] = {'n_months': len(ms), 'n_days': int(mask.sum()), 'mean': float(r[mask].mean()) if mask.any() else None}
    return res


def evaluate(days, base, stress, seg, k, months_ret):
    """Критерии §2.47.3 п. 2 на всём периоде панели (ряд суток seg)."""
    x = base[seg]
    mean, t5, n = E.nw_t(x, 5)
    t = S.tstat(x)
    t_adj = t / k if (math.isfinite(t) and k) else float('nan')
    sh, dd = E.sharpe(x, 365), E.max_drawdown(x)
    ms = float(stress[seg].mean()) if seg.any() else float('nan')
    grp = month_means(days[seg], x, months_ret)
    crit = {'t_adj': bool(math.isfinite(t_adj) and t_adj >= MIN_T_ADJ and mean > 0), 'sharpe': bool(math.isfinite(sh) and sh >= MIN_SHARPE), 'dd': bool(dd <= MAX_DD), 'stress': bool(math.isfinite(ms) and ms > 0),
            'up': (None if grp['up']['mean'] is None else bool(grp['up']['mean'] >= 0)), 'down': (None if grp['down']['mean'] is None else bool(grp['down']['mean'] >= 0))}
    return {'n': n, 'mean': mean, 't': t, 't5': t5, 't_adj': t_adj, 'k': k, 'sharpe': sh, 'dd': dd, 'stress_mean': ms, 'groups': grp, 'crit': crit, 'pass': bool(all(v for v in crit.values() if v is not None) and n > 0)}


def reference(env, base, fees, uni, seg):
    """Справочно: по годам, по биржам, листингов / допущено / позиций / стопов, средний фандинг за 30 суток после листинга."""
    P, ctx = env['P'], env['ctx']
    ev = S.h2_events(ctx, 3)
    meta = S.sim_h2(ctx, ev, fees)['meta']
    years = {}
    yrs = np.array([datetime.fromtimestamp(int(d) * DAY / 1000, timezone.utc).year for d in env['days']])
    for y in sorted(set(yrs[seg])):
        mm = seg & (yrs == y)
        years[int(y)] = {'n': int(mm.sum()), 'mean': float(base[mm].mean())}
    by_ex = {}
    for ex in X.EXCHANGES:
        sub = [(e, s) for e, s in ev if P['syms'][s].startswith(ex + ':')]
        if sub:
            r = S.sim_h2(ctx, sub, fees)
            by_ex[ex] = {'events': len(sub), 'opened': r['meta']['opened'], 'mean': float(r['r'][seg].mean()), 'stops': r['meta']['stops']}
            ff = P.get('fund_first')
            if ff is not None:                                         # [v119, §2.51] окно позиции (дни e+1…e+H2_HOLD): фандинг не загружен → в симуляции фандинг = 0 (консервативно)
                by_ex[ex]['fund_none'] = sum(1 for e, s_ in sub if ff[s_] < 0 or ff[s_] > e + S.H2_HOLD)
                by_ex[ex]['fund_part'] = sum(1 for e, s_ in sub if 0 <= ff[s_] and e + 1 < ff[s_] <= e + S.H2_HOLD)
    return {'years': years, 'by_ex': by_ex, 'listings': len(uni), 'admitted': len(ev), 'opened': meta['opened'], 'stops': meta['stops'], 'skipped_cap': meta['skipped_cap'], 'delist': meta['delist'],
            'funding30': listing_funding(P, ctx), 'stop_share': (meta['stops'] / meta['opened'] if meta['opened'] else None)}


def report_lines(P, uni, ref, pw, kres, spec_k, spec_u, u_line, head=None):
    R = list(head or [])
    R += [f'Вселенная: символов {len(uni)} (Bybit {sum(1 for u in uni if u["ex"] == "bybit")}, BingX {sum(1 for u in uni if u["ex"] == "bingx")}, OKX {sum(1 for u in uni if u["ex"] == "okx")}); монет на обеих биржах {ref["both"]}; '
          f'исключено: стейблкоин-подобных {len(ref["stable"])}, существовавших до начала истории биржи {len(ref["pre_existing"])}; '
          f'не-крипто (§2.52): ' + (', '.join(f'{k}: {len(v)}' for k, v in sorted(ref.get('noncrypto', {}).items())) or 'нет') + '.',
          f'Панель: суток {P["nd"]}, символов {len(P["syms"])}; с историей фандинга {int(np.sum(P["has_fund"]))}.',
          f'Мощность (шаг 0): D = {pw["D"]} суток с позициями, S_min = {fm(pw["s_min"])} → ' + ('проверяема' if pw['testable'] else '<b>НЕПРОВЕРЯЕМА</b> (D < 365)')]
    if kres:
        R.append(f'<b>k = {kres["k"]:.3f}</b> (m0 = {kres["m0"]:.3f}; k = max(1, m0/{NULL_MEDIAN_ABS_T}); {kres["reps"]} повторов, событий H2-D3 {kres["n_events"]}, ряд суток {kres["n_days"]}, seed {SEED})' if kres.get('k') else f'k не рассчитан: {kres.get("error")}')
    R.append('Строка вселенной в BOT_SPEC: ' + ('СОВПАДАЕТ.' if spec_u == u_line.split(SPEC_U_KEY)[1].strip() else f'не записана/не совпадает — запишите ОТДЕЛЬНЫМ коммитом ДО confirm:\n<code>{u_line}</code>'))
    R.append(f'Строка k в BOT_SPEC: {SPEC_K_KEY} ' + (f'{spec_k:.3f} — ' + ('СОВПАДАЕТ.' if kres and kres.get('k') and round(spec_k, 3) == round(kres['k'], 3) else 'НЕ совпадает с расчётом.') if spec_k is not None else 'не записана — запишите ОТДЕЛЬНЫМ коммитом ДО confirm.'))
    return R


def final_report(P, ev, ref_, head):
    c = ev['crit']
    mark = lambda v: 'н/п' if v is None else ('да' if v else 'НЕТ')
    g = ev['groups']
    R = list(head) + ['', f'<b>Критерии репликации (весь период панели, ряд суток {ev["n"]})</b>',
                      f'  t_adj = {fm(ev["t_adj"])} (t = {fm(ev["t"])}, k = {fm(ev["k"], 3)}) ≥ {MIN_T_ADJ} и mean {bp(ev["mean"])} б.п./день > 0 [{mark(c["t_adj"])}]',
                      f'  годовой Шарп {fm(ev["sharpe"])} ≥ {MIN_SHARPE} [{mark(c["sharpe"])}]; просадка {ev["dd"] * 100:.1f}% ≤ {MAX_DD * 100:.0f}% [{mark(c["dd"])}]; стресс ×2 mean {bp(ev["stress_mean"])} [{mark(c["stress"])}]',
                      f'  месяцы роста BTC: {g["up"]["n_months"]} мес., mean {bp(g["up"]["mean"]) if g["up"]["mean"] is not None else "н/п"} [{mark(c["up"])}]; падения: {g["down"]["n_months"]} мес., mean '
                      f'{bp(g["down"]["mean"]) if g["down"]["mean"] is not None else "н/п"} [{mark(c["down"])}]',
                      f'  <b>{"ПРОЙДЕНО → шаг 1.3 (/exec_check h2, ≤ 7 дней)" if ev["pass"] else "НЕ ПРОЙДЕНО → H2 закрыт окончательно"}</b>', '',
                      '<b>Справочно</b>']
    R.append('  по годам (mean б.п./день): ' + '; '.join(f'{y}: {bp(v["mean"])} (n={v["n"]})' for y, v in ref_['years'].items()))
    R.append('  по биржам: ' + '; '.join(f'{ex}: событий {v["events"]}, позиций {v["opened"]}, стопов {v["stops"]}, mean {bp(v["mean"])}' for ex, v in ref_['by_ex'].items()))
    nf = [f'{ex}: окно позиции без истории фандинга целиком — {v["fund_none"]} из {v["events"]} допущенных событий, частично — {v["fund_part"]}' for ex, v in ref_['by_ex'].items() if v.get('fund_none') or v.get('fund_part')]
    if nf:
        R.append('  фандинг = 0 там, где истории нет (консервативно для H2, §2.51; счёт по допущенным событиям, окно — 30 суток после входа): ' + '; '.join(nf))
    R.append(f'  листингов {ref_["listings"]}, допущено по ликвидности {ref_["admitted"]}, позиций {ref_["opened"]}, стопов +50% {ref_["stops"]}' + (f' ({ref_["stop_share"] * 100:.0f}%)' if ref_['stop_share'] is not None else '') +
             f', пропусков по лимиту риска {ref_["skipped_cap"]}, делистингов {ref_["delist"]}')
    f30 = ref_['funding30']
    if f30['n']:
        R.append(f'  фандинг за 30 суток после листинга (n={f30["n"]}): средняя сумма {f30["mean_sum"] * 100:+.3f}% номинала, доля > 0 {f30["share_pos"] * 100:.0f}% («+» — шорт получает)')
    return R


# ── команда ─────────────────────────────────────────────────────────────────────────────────────────
async def run_h2repl(params, progress=None):
    """`/h2_replicate [load | confirm | report]`; load — загрузка данных; без аргумента — вселенная, мощность и k (доходности НЕ считаются); confirm — единственный расчёт; report — сохранённый отчёт."""
    mode = params.get('mode', 'show')
    fdir = params.get('fdir') or RF.FD.factory_dir()
    xdir = params.get('xdir') or X.xdata_dir()
    fees = RB.fees_of(params)
    version = params.get('version', '')

    async def say(m):
        if progress:
            await progress(m)
    if mode == 'load':
        return await X.run_xload(dict(params, venues=params.get('venues') or X.venues_from_spec()), progress)
    if mode == 'report':
        return RF.kv_get(KV_REPORT, fdir) or 'Сохранённого отчёта нет'
    chk = RF.kv_get('h2_funding_check', fdir)
    if not chk or not chk.get('allow'):
        return '⛔ /h2_replicate отказ: сначала /h2_funding_check с решением «репликация разрешена» (§2.47.1)'
    if RF.kv_get(KV_DONE, fdir):
        return '⛔ /h2_replicate: расчёт уже выполнен (повтор запрещён, §2.47.3 п. 3)\n' + (RF.kv_get(KV_REPORT, fdir) or '')
    vn = tuple(params.get('venues') or X.venues_from_spec())
    cmin = float(params.get('coverage_min', X.COVERAGE_MIN))
    if cmin > 0 and mode in ('show', 'confirm'):                       # [v121, §2.53 п. 3] полнота ПЕРЕД любым расчётом: < 90% — отказ без расчёта и без вердикта
        cov = X.coverage_report(xdir, 'h2', params.get('bdir'), venues=vn, minimum=cmin)
        if not cov['ok']:
            return ('⛔ /h2_replicate отказ: данные неполны: ' + '; '.join(cov['lines']) + '. Расчёта и вердикта нет (BOT_SPEC §2.53): выполните /h2_replicate load (повторяйте до «✅ загрузка завершена»).')
    P, uni, ref = build_repl_panel(xdir, params.get('bdir'), vn)
    if P is None or not uni:
        return '⛔ /h2_replicate отказ: нет данных репликации — выполните /h2_replicate load до сообщения «загрузка завершена»'
    if cmin > 0 and mode in ('show', 'confirm'):
        idx_ = X.load_index(xdir)
        lost = [ex for ex in vn if idx_['bars'].get(ex) and not any(u['ex'] == ex for u in uni)]
        if lost:                                                       # биржа с загруженными свечами не дала ни одного символа в панель
            return f'⛔ /h2_replicate отказ: данные неполны: биржа(и) {", ".join(lost)} не попали в панель (свечи загружены, символов во вселенной 0). Расчёта и вердикта нет.'
    u_line = X.universe_line(uni)
    spec_u = spec_value(SPEC_U_KEY, params.get('spec_path'))
    spec_k = read_spec_k(params.get('spec_path'))
    env = make_env(P)
    pw = power(env, fees)
    head = [f'🔁 <b>/h2_replicate — репликация H2-D3 на независимых монетах (BOT_SPEC §2.47)</b> | код {version}; ЛОКБОКС не читается']
    kres = None
    if pw['testable']:
        await say('k: синтетика этой панели (200 повторов)…')
        kres = compute_k(env, fees, K_REPS, progress=None)
        if kres.get('k'):
            RF.kv_set(KV_K, {k: v for k, v in kres.items()}, fdir)
    lines = report_lines(P, uni, ref, pw, kres, spec_k, spec_u, u_line, head)
    spec_u_ok = spec_u is not None and spec_u == u_line.split(SPEC_U_KEY)[1].strip()
    k_ok = bool(kres and kres.get('k') and spec_k is not None and round(spec_k, 3) == round(kres['k'], 3))
    if mode != 'confirm':
        lines.append('Запуск расчёта: /h2_replicate confirm (после записи обеих строк в BOT_SPEC).' if pw['testable'] else 'Расчёт невозможен: гипотеза непроверяема (шаг 0).')
        return '\n'.join(lines)
    if not pw['testable']:
        RF.kv_set(KV_DONE, True, fdir)
        RF.kv_set(KV_RESULT, {'untestable': True, 'D': pw['D']}, fdir)
        RF.kv_set(KV_REPORT, '\n'.join(lines + ['<b>H2 «непроверяема» на этой выборке (шаг 0) — репликация не даёт ответа; H2 закрыт (решение владельца: не прошла = закрыт).</b>']), fdir)
        return RF.kv_get(KV_REPORT, fdir)
    if not spec_u_ok or not k_ok:
        return '⛔ /h2_replicate confirm отказ: строки вселенной и k не записаны в BOT_SPEC или не совпадают с расчётом (расчёт доходностей НЕ выполнялся)\n' + '\n'.join(lines[-3:])
    if not RF.kv_set_if_absent(KV_STARTED, {'t': time.time(), 'version': version}, fdir):
        if RF.kv_get(KV_RESULT, fdir) is not None:
            return '⛔ /h2_replicate: результат уже записан — /h2_replicate report'
        # старт был, результата нет — технический сбой до показа результата: повтор допускается
    await say('расчёт на панели репликации…')
    seg = np.arange(P['nd']) >= first_event_day(env)[0]
    base = RB.run_cell(CELL, env, fees, 1.0)['r']
    stress = RB.run_cell(CELL, env, fees, 2.0)['r']
    months = {}
    d1 = B.load_d1(params.get('bdir') or B.battery_dir(), 'BTCUSDT')
    if d1 is not None:
        months = btc_months_all(d1['ts'], d1['c'])
    ev = evaluate(env['days'], base, stress, seg, kres['k'], months)
    ref_ = reference(env, base, fees, uni, seg)
    text = '\n'.join(final_report(P, ev, ref_, lines))
    RF.kv_set(KV_RESULT, {'ev': {k: v for k, v in ev.items() if k != 'groups'}, 'pass': ev['pass']}, fdir)
    RF.kv_set(KV_REPORT, text, fdir)
    RF.kv_set(KV_PASSED, bool(ev['pass']), fdir)
    RF.kv_set(KV_DONE, True, fdir)
    return text
