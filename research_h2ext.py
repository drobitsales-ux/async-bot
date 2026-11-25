"""research_h2ext.py — H2-D3 на НОВЫХ биржах (Gate.io, MEXC, Bitget) — гипотеза H2EXT — и гипотеза H2F (BOT_SPEC §2.59).
Правила H2-D3 БЕЗ изменений: те же `research_bsim.h2_events` / `sim_h2`, что в `research_battery.run_cell(('H2','D3'))`; панель и вселенная — `research_h2repl.build_repl_panel`
(правило `research_xdata.h2_universe`, §2.47.2) с площадками из маркера `H2EXT-VENUES:`. H2F отличается ТОЛЬКО списком событий (фильтр по фандингу, только символы новых бирж).
Порядок (§2.59.5): мощность ДО доходностей → k по синтетике → строки `…-UNIVERSE:`/`…-K:` в BOT_SPEC → `confirm` (один прогон). Порог t_adj ≥ 2.0 у обеих гипотез; последовательная проверка (§2.59.9):
H2F проверяется ТОЛЬКО если H2EXT пройден; иначе H2F не считается и H2 закрыт целиком.
LOCKBOX батареи (исчерпан, §2.43.20) не читается. Ордеров нет."""
import hashlib
import math
import os
import time

import numpy as np

import research_battery as RB
import research_bdata as B
import research_bsim as S
import research_factory as RF
import research_h2repl as H
import disk_usage as DU
import research_leadlag as LL
import research_xdata as X

SEED = 20261121
K_TAG = {'ext': 4701, 'f': 4702}
K_REPS = 200
T_ADJ_MIN = H.MIN_T_ADJ                  # §2.59.9: fixed-sequence, порог 2.0 (§2.47 без изменений); поправка Бонферрони 2.24 снята
CFG = {
    'ext': {'ns': 'h2_ext', 'title': 'H2EXT — H2-D3 на новых биржах', 'spec_u': 'H2EXT-UNIVERSE:', 'spec_k': 'H2EXT-K:', 'cmd': '/h2_ext'},
    'f': {'ns': 'h2f', 'title': 'H2F — H2-D3 + шорт только при среднем фандинге > 0 за сутки до входа (символы новых бирж)', 'spec_u': 'H2F-UNIVERSE:', 'spec_k': 'H2F-K:', 'cmd': '/h2f'},
}


def space_guard(xdir, ll_dir, params=None):
    """[§2.59.11 п. 3] Функция для `run_xload`: после списка контрактов и до скачивания свечей — отказ, если идёт замер §2.60 и после загрузки свободного места останется
    меньше LEADLAG_MIN_FREE_MB + 50 МБ. Замер не идёт — проверка не применяется. → callable(syms_all, idx, now) → str | None."""
    def guard(syms_all, idx, now=None):
        st = LL.load_state(ll_dir)
        now = time.time() if now is None else now
        if not (st.get('running') and float(st.get('until', 0)) > now):
            return None
        todo = [1 for ex, s_ in syms_all if not (idx['bars'].get(ex, {}).get(s_) or {}).get('complete')]
        free = (params or {}).get('free_mb')
        free = LL.free_mb(xdir) if free is None else float(free)
        ok, d = DU.space_check(free, len(todo), st, now, LL.min_free_mb())
        if ok:
            return None
        return (f'⛔ /h2_ext load отказ (пока идёт замер /leadlag, §2.59.11 п. 3): свободно {free:.0f} МБ; к загрузке {len(todo)} символов ≈ {d["load"]:.0f} МБ (верхняя оценка 140 КБ/символ) '
                f'+ рост замера до конца срока ≈ {d["growth"]:.0f} МБ → после загрузки останется ≈ {d["after"]:.0f} МБ < {d["need_after"]:.0f} МБ (LEADLAG_MIN_FREE_MB + 50). '
                f'Не хватает ≈ {d["short"]:.0f} МБ. Что можно освободить: /disk_usage (архивы закрытых исследований — /disk_cleanup <имя> confirm); либо дождаться конца замера. Ничего не загружено.')
    return guard


def sequence_gate(fdir):
    """[§2.59.9] None — H2F разрешена (H2EXT выполнен и пройден); иначе текст отказа."""
    if not RF.kv_get('h2_ext_done', fdir):
        return '⛔ /h2f отказ: последовательная проверка (§2.59.9) — сначала H2EXT (/h2_ext confirm); H2F считается только если H2EXT пройден'
    if not RF.kv_get('h2_ext_passed', fdir):
        return '⛔ /h2f отказ: H2EXT не пройден (или «непроверяема») — по §2.59.9 H2F не считается, H2 закрыт целиком'
    return None


def last_bar_info(xdir, venues):
    """Последний дневной бар (максимум по символам биржи) для каждой площадки вселенной; spread_days — разница между самой свежей и самой старой."""
    idx = X.load_index(xdir)
    by = {}
    for ex in venues:
        ls = [int(r['last']) for r in idx['bars'].get(ex, {}).values() if r.get('last')]
        if ls:
            by[ex] = max(ls)
    if not by:
        return None
    return {'by_ex': {ex: time.strftime('%Y-%m-%d', time.gmtime(v / 1000)) for ex, v in by.items()}, 'spread_days': (max(by.values()) - min(by.values())) / X.DAY}


async def count_contracts(venues, bdir=None, clients=None):
    """[§2.59.11] Число контрактов новых бирж по спискам (публичные GET): всего и «к загрузке» (база не в архиве Binance, не не-крипто). → {биржа: dict(markets, want | err)}."""
    import research_venues
    bn = X.binance_norm_set(bdir)
    out = {}
    for ex in venues:
        if ex not in research_venues.CLASSES:
            continue
        c = (clients or {}).get(ex) or research_venues.CLASSES[ex]()
        try:
            sw = X.usdt_swaps(await c.load_markets())
            out[ex] = {'markets': len(sw), 'want': sum(1 for v in sw.values() if not v.get('excl') and v['norm'] not in bn)}
        except Exception as e:
            out[ex] = {'markets': None, 'want': None, 'err': f'{type(e).__name__}: {str(e)[:80]}'}
        finally:
            try:
                await c.close()
            except Exception:
                pass
    return out


async def disk_job(params):
    """Задача `/disk_usage` (дочерний процесс): структура диска, оценка H2EXT по спискам контрактов, рост замера. Ничего не изменяет."""
    import shutil
    root = params.get('root') or ('/data' if os.path.exists('/data') else os.path.dirname(os.path.abspath(params.get('xdir') or '/tmp')))
    paths = {k: params.get(k) for k in ('vision', 'factory', 'edge', 'battery', 'xdata', 'leadlag', 'jobs') if params.get(k)}
    items = DU.layout(root, paths)
    du_ = shutil.disk_usage(root)
    venues = tuple(params.get('venues') or X.ext_venues_from_spec(params.get('spec_path')) or ())
    new = tuple(v for v in venues if v in X.NEW_VENUES)
    contracts = await count_contracts(new, params.get('bdir'), params.get('clients')) if new else None
    st = LL.load_state(params.get('ll_dir') or LL.leadlag_dir())
    return DU.format_report(root, items, du_.free / 1e6, du_.total / 1e6, st, time.time(), LL.min_free_mb(), contracts, new)


def keys(hyp):
    ns = CFG[hyp]['ns']
    return {k: f'{ns}_{k}' for k in ('started', 'done', 'result', 'report', 'passed', 'k')}


def sim(env, fees, ev, mult=1.0, fund_on=True, costs_on=True, synth_P=None):
    """Тот же вызов, что `run_cell(('H2','D3'), …)`, но с явным списком событий."""
    ctx = env['ctx']
    c2 = S.with_prices(ctx, synth_P) if synth_P is not None else ctx
    return S.sim_h2(c2, ev, fees, mult, fund_on, costs_on, data_end=c2['P'].get('end_data'))


def all_events(env):
    return S.h2_events(env['ctx'], 3)


# ── H2F: символы новых бирж и фильтр по фандингу (§2.59.6) ────────────────────────────────────────
def old_norms(xdir, venues=('bingx', 'okx')):
    """Нормализованные базы, по которым на BingX/OKX есть дневные свечи (кандидаты этих бирж — их данные «видели»)."""
    idx = X.load_index(xdir)
    out = set()
    for ex in venues:
        for sym in idx['bars'].get(ex, {}):
            m = idx['markets'].get(ex, {}).get(sym)
            if m:
                out.add(m['norm'])
    return out


def new_symbols(uni, venues, seen_norms):
    """Символы вселенной на новых биржах (из маркера), чья база не встречалась на BingX/OKX → список имён `биржа:символ` (§2.59.6 п. 1)."""
    new_ex = set(venues) - {'bingx', 'okx'}
    return sorted(f'{u["ex"]}:{u["sym"]}' for u in uni if u['ex'] in new_ex and u['norm'] not in seen_norms)


def f_events(P, ev, allowed):
    """H2F: события символов `allowed`, у которых `GC[e, s] > 0` и `G[e, s] / GC[e, s] > 0` (расчёты фандинга в сутках e, строго до момента входа — close дня e; как сигналы H1, §2.59.6 п. 2).
    → (события, счётчики)."""
    names = set(allowed)
    out, c = [], {'candidates': 0, 'no_funding': 0, 'neg_or_zero': 0}
    for e, s in ev:
        if P['syms'][s] not in names:
            continue
        c['candidates'] += 1
        gc = float(P['GC'][e, s])
        if gc <= 0:
            c['no_funding'] += 1
            continue
        if float(P['G'][e, s]) / gc > 0:
            out.append((e, s))
        else:
            c['neg_or_zero'] += 1
    c['admitted'] = len(out)
    return out, c


def first_day(ev):
    return min(e for e, _s in ev) if ev else None


def power(env, fees, ev):
    """Шаг 0: D — суток с позициями (по результату симуляции без издержек-зависимости: `meta.days_open`), S_min = 2.0·√(365/D); S_min > 2.0 → «непроверяема»."""
    res = sim(env, fees, ev)
    D = int(res['meta'].get('days_open', 0))
    sm = 2.0 * math.sqrt(365.0 / D) if D > 0 else float('inf')
    return {'D': D, 's_min': sm, 'testable': bool(D > 0 and sm <= 2.0), 'meta': res['meta']}


def compute_k(env, fees, ev, hyp, reps=K_REPS, progress=None):
    """k = max(1, m0/0.674) по синтетике ЭТОЙ панели и ЭТИХ событий (§2.47.3 п. 1; мартингал, σ реальных рядов; без издержек и фандинга; ряд суток — от первого события)."""
    e0 = first_day(ev)
    if e0 is None:
        return {'k': None, 'm0': None, 'reps': 0, 'n_events': 0, 'error': 'нет допущенных событий'}
    seg = np.arange(env['P']['nd']) >= e0
    ts = []
    for rep in range(reps):
        rng = np.random.default_rng([SEED, K_TAG[hyp], rep])
        P2 = S.synth_daily_panel(env['P'], rng)
        res = sim(env, fees, ev, 1.0, False, False, synth_P=P2)
        t = S.tstat(res['r'][seg])
        if math.isfinite(t):
            ts.append(float(t))
        if progress and (rep + 1) % 50 == 0:
            progress(f'k: повторов {rep + 1}/{reps}')
    m0 = float(np.median(np.abs(ts))) if ts else float('nan')
    return {'k': (max(1.0, m0 / H.NULL_MEDIAN_ABS_T) if math.isfinite(m0) else None), 'm0': m0, 'reps': len(ts), 'n_events': len(ev), 'n_days': int(seg.sum())}


def evaluate(days, base, stress, seg, k, months_ret):
    """Критерии §2.47.3 п. 2 без изменений (порог t_adj ≥ 2.0; §2.59.9) — расчёт `research_h2repl.evaluate`."""
    return H.evaluate(days, base, stress, seg, k, months_ret)


def lines_of(uni_sub):
    h = hashlib.sha256(','.join(sorted(uni_sub)).encode()).hexdigest()[:12]
    return f'N={len(uni_sub)} sha={h} ' + ','.join(sorted(uni_sub))


def reference(env, base, fees, ev, seg):
    """Справочно: по годам, по биржам, позиций/стопов, фандинг в окне позиции (счётчик «фандинг = 0», §2.51)."""
    from datetime import datetime, timezone
    P = env['P']
    res = sim(env, fees, ev)
    meta = res['meta']
    years = {}
    yrs = np.array([datetime.fromtimestamp(int(d) * X.DAY / 1000, timezone.utc).year for d in env['days']])
    for y in sorted(set(yrs[seg])):
        mm = seg & (yrs == y)
        years[int(y)] = {'n': int(mm.sum()), 'mean': float(base[mm].mean())}
    by_ex = {}
    ff = P.get('fund_first')
    for ex in X.EXCHANGES:
        sub = [(e, s) for e, s in ev if P['syms'][s].startswith(ex + ':')]
        if not sub:
            continue
        r = sim(env, fees, sub)
        by_ex[ex] = {'events': len(sub), 'opened': r['meta']['opened'], 'stops': r['meta']['stops'], 'mean': float(r['r'][seg].mean())}
        if ff is not None:
            by_ex[ex]['fund_none'] = sum(1 for e, s_ in sub if ff[s_] < 0 or ff[s_] > e + S.H2_HOLD)
            by_ex[ex]['fund_part'] = sum(1 for e, s_ in sub if 0 <= ff[s_] and e + 1 < ff[s_] <= e + S.H2_HOLD)
    return {'years': years, 'by_ex': by_ex, 'events': len(ev), 'opened': meta['opened'], 'stops': meta['stops'], 'skipped_cap': meta['skipped_cap'], 'delist': meta['delist'], 'data_end': meta.get('data_end', 0),
            'stop_share': (meta['stops'] / meta['opened'] if meta['opened'] else None)}


def final_lines(ev, ref_):
    c = ev['crit']
    mark = lambda v: 'н/п' if v is None else ('да' if v else 'НЕТ')
    g = ev['groups']
    R = ['', f'<b>Критерии (весь период панели, ряд суток {ev["n"]}); порог t_adj ≥ {T_ADJ_MIN} (§2.47 без изменений; последовательная проверка H2EXT → H2F, §2.59.9)</b>',
         f'  t_adj = {H.fm(ev["t_adj"])} (t = {H.fm(ev["t"])}, k = {H.fm(ev["k"], 3)}) и mean {H.bp(ev["mean"])} б.п./день > 0 [{mark(c["t_adj"])}]',
         f'  годовой Шарп {H.fm(ev["sharpe"])} ≥ {H.MIN_SHARPE} [{mark(c["sharpe"])}]; просадка {ev["dd"] * 100:.1f}% ≤ {H.MAX_DD * 100:.0f}% [{mark(c["dd"])}]; стресс ×2 mean {H.bp(ev["stress_mean"])} [{mark(c["stress"])}]',
         f'  месяцы роста BTC: {g["up"]["n_months"]} мес., mean {H.bp(g["up"]["mean"]) if g["up"]["mean"] is not None else "н/п"} [{mark(c["up"])}]; падения: {g["down"]["n_months"]} мес., mean '
         f'{H.bp(g["down"]["mean"]) if g["down"]["mean"] is not None else "н/п"} [{mark(c["down"])}]',
         f'  <b>{"ПРОЙДЕНО → решение владельца о следующем шаге (форвард/исполнение — отдельная пре-регистрация)" if ev["pass"] else "НЕ ПРОЙДЕНО → гипотеза закрыта"}</b>', '', '<b>Справочно</b>']
    R.append('  по годам (mean б.п./день): ' + '; '.join(f'{y}: {H.bp(v["mean"])} (n={v["n"]})' for y, v in ref_['years'].items()))
    R.append('  по биржам: ' + '; '.join(f'{ex}: событий {v["events"]}, позиций {v["opened"]}, стопов {v["stops"]}, mean {H.bp(v["mean"])}' for ex, v in ref_['by_ex'].items()))
    nf = [f'{ex}: окно позиции без истории фандинга целиком — {v["fund_none"]} из {v["events"]}, частично — {v["fund_part"]}' for ex, v in ref_['by_ex'].items() if v.get('fund_none') is not None]
    if nf:
        R.append('  фандинг = 0 там, где истории нет (консервативно для H2, §2.51): ' + '; '.join(nf))
    R.append(f'  событий {ref_["events"]}, позиций {ref_["opened"]}, стопов +50% {ref_["stops"]}' + (f' ({ref_["stop_share"] * 100:.0f}%)' if ref_['stop_share'] is not None else '')
             + f', пропусков по лимиту риска {ref_["skipped_cap"]}, делистингов {ref_["delist"]}, конец данных (кэш BingX/OKX, §2.61.3) {ref_["data_end"]}')
    return R


async def run_h2ext(params, progress=None):
    """`/h2_ext [load | confirm | report]` (hyp='ext') и `/h2f [confirm | report]` (hyp='f'). Без аргумента — вселенная, мощность и k (доходности НЕ считаются)."""
    hyp = params.get('hyp', 'ext')
    cfg, K = CFG[hyp], keys(hyp)
    mode = params.get('mode', 'show')
    fdir = params.get('fdir') or RF.FD.factory_dir()
    xdir = params.get('xdir') or X.xdata_dir()
    fees = RB.fees_of(params)
    version = params.get('version', '')
    spec_path = params.get('spec_path')

    async def say(m):
        if progress:
            await progress(m)
    if mode == 'report':
        return RF.kv_get(K['report'], fdir) or 'Сохранённого отчёта нет'
    venues = tuple(params.get('venues') or X.ext_venues_from_spec(spec_path) or ())
    if not venues:
        return ('⛔ ' + cfg['cmd'] + ' отказ: в BOT_SPEC нет строки-маркера `H2EXT-VENUES:` (§2.59.1). Сначала `/net_probe` (вердикт по Gate.io/MEXC/Bitget), затем запись маркера '
                'ОТДЕЛЬНЫМ коммитом ДО загрузки данных.')
    if mode == 'load':
        if hyp != 'ext':
            return '⛔ /h2f load отсутствует: данные общие — /h2_ext load'
        load_venues = tuple(v for v in venues if v in X.NEW_VENUES)       # §2.59.10: загружаются только новые биржи; BingX/OKX — из кэша §2.47
        guard = space_guard(xdir, params.get('ll_dir') or LL.leadlag_dir(), params)
        return await X.run_xload(dict(params, venues=load_venues, ext=True, hourly=False, space_guard=guard), progress)
    if hyp == 'f':                                                      # §2.59.9: fixed-sequence — H2F только после H2EXT «пройдено»
        gate = sequence_gate(fdir)
        if gate:
            return gate
    if RF.kv_get(K['done'], fdir):
        return f'⛔ {cfg["cmd"]}: расчёт уже выполнен (повтор запрещён, §2.59.5)\n' + (RF.kv_get(K['report'], fdir) or '')
    cmin = float(params.get('coverage_min', X.COVERAGE_MIN))
    if cmin > 0:                                                         # полнота ПЕРЕД любым расчётом (§2.53 п. 3)
        cov = X.coverage_report(xdir, 'h2', params.get('bdir'), venues=venues, minimum=cmin)
        if not cov['ok']:
            return (f'⛔ {cfg["cmd"]} отказ: данные неполны: ' + '; '.join(cov['lines']) + '. Расчёта и вердикта нет (BOT_SPEC §2.53): выполните /h2_ext load (повторяйте до «✅ загрузка завершена»).')
    P, uni, ref = H.build_repl_panel(xdir, params.get('bdir'), venues)
    if P is None or not uni:
        return f'⛔ {cfg["cmd"]} отказ: нет данных — выполните /h2_ext load до сообщения «загрузка завершена»'
    env = H.make_env(P)
    ev_all = all_events(env)
    counters = {}
    if hyp == 'ext':
        ev, uni_names = ev_all, [f'{u["ex"]}:{u["sym"]}' for u in uni]
    else:
        uni_names = new_symbols(uni, venues, old_norms(xdir))
        ev, counters = f_events(P, ev_all, uni_names)
    u_body = lines_of(uni_names)
    spec_u = H.spec_value(cfg['spec_u'], spec_path)
    spec_k = H.spec_value(cfg['spec_k'], spec_path)
    try:
        spec_k = float(spec_k.split()[0]) if spec_k else None
    except ValueError:
        spec_k = None
    pw = power(env, fees, ev)
    per_ex = {}
    for u in uni:
        per_ex[u['ex']] = per_ex.get(u['ex'], 0) + 1
    head = [f'🔁 <b>{cfg["cmd"]} — {cfg["title"]} (BOT_SPEC §2.59)</b> | код {version}; площадки: {", ".join(venues)}; LOCKBOX не читается',
            f'Вселенная H2 (базы вне архива Binance): символов {len(uni)} (' + ', '.join(f'{ex} {n}' for ex, n in sorted(per_ex.items())) + f'); монет на нескольких биржах {ref["both"]}; '
            f'исключено: стейблкоин-подобных {len(ref["stable"])}, существовавших до начала истории биржи {len(ref["pre_existing"])}; не-крипто (§2.52): '
            + (', '.join(f'{k}: {len(v)}' for k, v in sorted(ref.get('noncrypto', {}).items())) or 'нет') + '.',
            f'Панель: суток {P["nd"]}, символов {len(P["syms"])}; с историей фандинга {int(np.sum(P["has_fund"]))}.']
    if hyp == 'f':
        head.append(f'H2F: символов новых бирж (база не встречалась на BingX/OKX) {len(uni_names)}; событий-кандидатов {counters["candidates"]}, без фандинга в сутки входа {counters["no_funding"]}, '
                    f'фандинг ≤ 0 {counters["neg_or_zero"]}, допущено {counters["admitted"]} (всего событий H2 по вселенной {len(ev_all)}).')
    ends = last_bar_info(xdir, venues)
    if ends:
        head.append('Последний дневной бар по биржам: ' + ', '.join(f'{ex} {d_}' for ex, d_ in ends['by_ex'].items())
                    + (f'. ⚠️ расхождение {ends["spread_days"]:.0f} сут. > 2: у BingX/OKX кэш старше (§2.59.10) — для листингов последних суток выход «делистинг» наступает раньше; правила не менялись' if ends['spread_days'] > 2 else '.'))
    head.append(f'Мощность (шаг 0): D = {pw["D"]} суток с позициями, S_min = {H.fm(pw["s_min"])} → ' + ('проверяема' if pw['testable'] else '<b>НЕПРОВЕРЯЕМА</b> (D < 365)'))
    kres = None
    if pw['testable']:
        await say('k: синтетика этой панели (200 повторов)…')
        kres = compute_k(env, fees, ev, hyp, K_REPS)
        if kres.get('k'):
            RF.kv_set(K['k'], dict(kres), fdir)
            head.append(f'<b>k = {kres["k"]:.3f}</b> (m0 = {kres["m0"]:.3f}; {kres["reps"]} повторов, событий {kres["n_events"]}, ряд суток {kres["n_days"]}, seed {SEED})')
        else:
            head.append('k не вычислен')
    u_ok = spec_u is not None and spec_u == u_body
    k_ok = bool(kres and kres.get('k') and spec_k is not None and round(spec_k, 3) == round(kres['k'], 3))
    head.append('Строка вселенной в BOT_SPEC: ' + ('СОВПАДАЕТ.' if u_ok else f'не записана/не совпадает — запишите ОТДЕЛЬНЫМ коммитом ДО confirm:\n{cfg["spec_u"]} {u_body}'))
    head.append(f'Строка k в BOT_SPEC: {cfg["spec_k"]} ' + ((f'{spec_k:.3f} — ' + ('СОВПАДАЕТ.' if k_ok else 'НЕ совпадает с расчётом.')) if spec_k is not None else 'не записана'
                + (f' — запишите `{cfg["spec_k"]} {kres["k"]:.3f}`' if kres and kres.get('k') else '')))
    if mode != 'confirm':
        head.append(f'Запуск расчёта: {cfg["cmd"]} confirm (после записи обеих строк в BOT_SPEC).' if pw['testable'] else 'Расчёт невозможен: гипотеза непроверяема (шаг 0).')
        return '\n'.join(head)
    if not pw['testable']:
        RF.kv_set(K['done'], True, fdir)
        RF.kv_set(K['result'], {'untestable': True, 'D': pw['D']}, fdir)
        RF.kv_set(K['report'], '\n'.join(head + ['<b>«Непроверяема» (шаг 0): ответа нет; ' + ('H2 закрыт окончательно (§2.59.5).' if hyp == 'ext' else 'H2F закрыта (§2.59.6).') + '</b>']), fdir)
        return RF.kv_get(K['report'], fdir)
    if not u_ok or not k_ok:
        return f'⛔ {cfg["cmd"]} confirm отказ: строки вселенной и k не записаны в BOT_SPEC или не совпадают с расчётом (расчёт доходностей НЕ выполнялся)\n' + '\n'.join(head)
    if not RF.kv_set_if_absent(K['started'], {'t': time.time(), 'version': version}, fdir):
        if RF.kv_get(K['result'], fdir) is not None:
            return f'⛔ {cfg["cmd"]}: результат уже записан — {cfg["cmd"]} report'
    await say('расчёт…')
    seg = np.arange(P['nd']) >= first_day(ev)
    base = sim(env, fees, ev)['r']
    stress = sim(env, fees, ev, 2.0)['r']
    months = {}
    d1 = B.load_d1(params.get('bdir') or B.battery_dir(), 'BTCUSDT')
    if d1 is not None:
        months = H.btc_months_all(d1['ts'], d1['c'])
    evl = evaluate(env['days'], base, stress, seg, kres['k'], months)
    ref_ = reference(env, base, fees, ev, seg)
    text = '\n'.join(head + final_lines(evl, ref_))
    RF.kv_set(K['result'], {'ev': {k: v for k, v in evl.items() if k != 'groups'}, 'pass': evl['pass']}, fdir)
    RF.kv_set(K['report'], text, fdir)
    RF.kv_set(K['passed'], bool(evl['pass']), fdir)
    RF.kv_set(K['done'], True, fdir)
    return text
