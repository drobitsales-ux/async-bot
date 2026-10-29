"""research_h2final.py — ОДИНОЧНЫЙ ФИНАЛ H2-D3 (BOT_SPEC §2.43.11, пре-регистрация записана ДО кода): расчёт k (инфляция t на синтетике, LOCKBOX НЕ читается) и один прогон на LOCKBOX
по критериям §2.43.11 п. 3 с отдельным флагом `final_h2_started`. Реализация кандидата — та же `research_battery.run_cell(('H2','D3'))`, что и в `/battery` (без изменений)."""
import gc
import math
import os
import time

import numpy as np

import research_bdata as B
import research_bsim as S
import research_battery as RB
import research_factory as RF
import research_finalstate as FS
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
KV_CRASH = 'final_h2_crash'
COL_BLOCK, ROW_BLOCK = 64, 256                 # [v114] блоки для масок ликвидности/топ-30 (экономия памяти, результаты побитово те же)
MEM_BASE_MB, MEM_PER_MCELL_MB = 120.0, 43.0    # [v114] оценка пика памяти финального пути: база + коэффициент × миллионы ячеек «день × символ» (калибровка — BOT_SPEC §2.43.14)
MEM_SAFETY = 0.85


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


def make_ctx_h2(P):
    """[v114] Контекст H2 без неиспользуемого: H2 читает только P (O, H, C, F, first, last) и маски `liq` (допуск по ликвидности PIT) и `top` (проскальзывание топ-30); `atr` и `adv` в H2 не читаются.
    Маски те же, что в `research_bsim.make_ctx`, но считаются блоками (pit_adv — по столбцам, top_n_mask — по строкам; операции независимы по блокам → результаты побитово те же, тест сравнивает).
    После расчёта освобождаются матрицы, которые H2 не читает (QV, L, G, GC)."""
    qv = P['QV']
    nd, ns = qv.shape
    adv = np.empty((nd, ns), np.float64)
    for c0 in range(0, ns, COL_BLOCK):
        adv[:, c0:c0 + COL_BLOCK] = B.pit_adv(qv[:, c0:c0 + COL_BLOCK])[0]
    liq = B.liquid_mask(adv)
    top = np.empty((nd, ns), bool)
    for r0 in range(0, nd, ROW_BLOCK):
        top[r0:r0 + ROW_BLOCK] = B.top_n_mask(adv[r0:r0 + ROW_BLOCK])
    del adv
    for k in ('QV', 'L', 'G', 'GC'):
        P.pop(k, None)
    return {'P': P, 'adv': None, 'liq': liq, 'top': top, 'atr': None}


def make_env_final(P):
    return {'P': P, 'ctx': make_ctx_h2(P), 'days': np.arange(P['day0'], P['day0'] + P['nd'], dtype=np.int64)}


def estimate_peak_mb(d):
    """Оценка пика памяти полной панели по ИНДЕКСУ архива (имена файлов; данные не читаются): ячейки = (суток от первого месяца до конца последнего) × (символов с файлами D1). → (МБ, ячеек)."""
    idx = B.load_index(d)
    syms = [s_ for s_ in idx['symbols'] if idx['keys'].get(s_, {}).get('d1')]
    if not syms:
        return 0.0, 0
    firsts = [idx['keys'][s_]['d1'][0] for s_ in syms]
    lasts = [idx['keys'][s_]['d1'][-1] for s_ in syms]
    y, m = max(lasts).split('-')
    ny, nm = (int(y) + 1, 1) if int(m) == 12 else (int(y), int(m) + 1)
    nd = B._month_start_day(f'{ny}-{nm:02d}') - B._month_start_day(min(firsts))
    cells = max(0, nd) * len(syms)
    return MEM_BASE_MB + MEM_PER_MCELL_MB * cells / 1e6, cells


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


def mem_limit_mb():
    import research_runner as RR
    return RR.mem_limit_mb()


def lockbox_eval(d, fees, k, session):
    """Единственный расчёт на LOCKBOX (вызывается только после взятия флага и открытия сессии): полная панель → H2-D3 базовый и стресс ×2 → критерии и справочные счётчики. → dict | None (нет данных)."""
    P = B.load_panel(d, 'full', session)
    if P is None:
        return None
    env = make_env_final(P)
    base = RB.run_cell(CELL, env, fees, 1.0)
    stress = RB.run_cell(CELL, env, fees, 2.0)
    months = {}
    if 'BTCUSDT' in P['syms']:
        j = P['syms'].index('BTCUSDT')
        ts = (P['day0'] + np.arange(P['nd'])) * DAY
        ok = np.isfinite(P['C'][:, j])
        months = FF.btc_months(ts[ok], P['C'][ok, j].astype(float), int(ts[-1]) + DAY)
    ev = evaluate(base, stress, k, months)
    ref = reference(env, base, fees)
    return {'ev': ev, 'ref': ref, 'months': months, 'meta': base['meta']}


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


NAME = 'final_h2'
LEGACY = {'started': KV_FLAG, 'done': KV_DONE}
KV_REPORT = 'final_h2_report'
# Маркеры итогового отчёта H2 (строки, которых нет ни в показе k, ни в сообщениях об ошибках): их присутствие в любом хранилище = вывод по LOCKBOX существует (BOT_SPEC §2.43.15)
MARKERS = ('РЕЗУЛЬТАТ: ПРОЙДЕНО', 'РЕЗУЛЬТАТ: НЕ ПРОЙДЕНО', 'mean при издержках ×2 =', 'листингов в LOCKBOX', 'месяцы BTC (UTC → доходность)', 'финал H2-D3 на LOCKBOX (2025+)')
TEXT_EXT = ('.log', '.jsonl', '.json', '.txt', '.md', '.csv', '.html')
SKIP_DIRS = ('sym', 'vision', 'audit_cache', '__pycache__', '.git')
AUDIT_DEADLINE_S = 90.0
MAX_TEXT_BYTES = 4 * 1024 * 1024


def _short(m):
    return m[:10] + '…'                                            # в отчёт audit маркеры целиком не выводятся (иначе сохранённый отчёт сам станет «находкой»)


def audit_roots(params, fdir, d):
    import tempfile
    import audit_db as AD
    import research_runner as RR
    if params.get('scan_dirs') is not None:
        return list(params['scan_dirs'])
    data = '/data' if os.path.exists('/data') else tempfile.gettempdir()
    roots = [data, fdir, d, os.path.dirname(AD.db_path()) or data, RR.workdir_base(), tempfile.gettempdir()]
    out = []
    for r_ in roots:
        r_ = os.path.abspath(r_)
        if os.path.isdir(r_) and r_ not in out:
            out.append(r_)
    return out


def leak_audit(params, fdir, d, deadline_s=AUDIT_DEADLINE_S):
    """Шаг 0 (BOT_SPEC §2.43.15): ТОЛЬКО ЧТЕНИЕ. Ищет маркеры итогового отчёта H2 в ключах factory.db, во всех SQLite-базах (htf_reports, edge/trades/audit/батарея…), в текстовых файлах каталогов данных, задач и временных
    файлов. → dict(hits [(где, маркер №, число)], scanned {…}, kv {ключ: есть/нет}, jobs [каталоги задач final_h2], clean bool, truncated bool)."""
    import sqlite3
    t0 = time.time()
    hits, scanned = [], {'dbs': 0, 'tables': 0, 'columns': 0, 'files': 0, 'bytes': 0, 'dirs': 0}
    truncated = False
    roots = audit_roots(params, fdir, d)
    kv = {k_: RF.kv_get(k_, fdir) is not None for k_ in (KV_FLAG, KV_K, KV_DONE, KV_RESULT, KV_REPORT, KV_EXHAUSTED, KV_CRASH, 'finalstate:' + NAME)}
    seen_db, seen_files = set(), set()
    jobs = []
    enc = [m.encode('utf-8') for m in MARKERS]
    for root in roots:
        for dp, dn, fn in os.walk(root):
            depth = dp[len(root):].count(os.sep)
            dn[:] = [x for x in dn if x not in SKIP_DIRS] if depth < 4 else []
            scanned['dirs'] += 1
            if os.path.basename(dp).startswith('final_h2_'):
                jobs.append(dp)
            for f in fn:
                if time.time() - t0 > deadline_s:
                    truncated = True
                    break
                path = os.path.join(dp, f)
                if path in seen_files:
                    continue
                seen_files.add(path)
                low = f.lower()
                try:
                    if low.endswith('.db'):
                        seen_db.add(path)
                        con = sqlite3.connect(f'file:{path}?mode=ro', uri=True, timeout=5)
                        scanned['dbs'] += 1
                        try:
                            for (tn,) in con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall():
                                scanned['tables'] += 1
                                cols = [c[1] for c in con.execute(f'PRAGMA table_info("{tn}")').fetchall() if (c[2] or '').upper() in ('', 'TEXT') or 'CHAR' in (c[2] or '').upper() or 'CLOB' in (c[2] or '').upper()]
                                for c in cols:
                                    scanned['columns'] += 1
                                    for i, m in enumerate(MARKERS):
                                        n = con.execute(f'SELECT count(*) FROM "{tn}" WHERE instr("{c}", ?) > 0', (m,)).fetchone()[0]
                                        if n:
                                            hits.append((f'{path} :: {tn}.{c}', i + 1, int(n)))
                        finally:
                            con.close()
                    elif low.endswith(TEXT_EXT) and os.path.getsize(path) <= MAX_TEXT_BYTES:
                        with open(path, 'rb') as fh:
                            data = fh.read()
                        scanned['files'] += 1
                        scanned['bytes'] += len(data)
                        for i, m in enumerate(enc):
                            n = data.count(m)
                            if n:
                                hits.append((path, i + 1, n))
                except Exception:
                    continue
            if truncated:
                break
        if truncated:
            break
    return {'hits': hits, 'scanned': scanned, 'kv': kv, 'jobs': jobs, 'roots': roots, 'clean': (not hits) and not truncated, 'truncated': truncated}


def audit_text(res):
    L_ = ['🔎 <b>/final_h2 audit — проверка утечки вывода по LOCKBOX (BOT_SPEC §2.43.15), только чтение</b>',
          'Маркеры итогового отчёта H2 (' + str(len(MARKERS)) + '): ' + ', '.join(f'{i + 1}) {_short(m)}' for i, m in enumerate(MARKERS)),
          'Проверено: каталоги ' + ', '.join(res['roots']) + f'; просмотрено каталогов {res["scanned"]["dirs"]}, SQLite-баз {res["scanned"]["dbs"]} (таблиц {res["scanned"]["tables"]}, текстовых колонок {res["scanned"]["columns"]}), '
          f'текстовых файлов {res["scanned"]["files"]} ({res["scanned"]["bytes"] / 1e6:.1f} МБ); оставшихся каталогов задач final_h2: {len(res["jobs"])}',
          'Ключи factory.db: ' + ', '.join(f'{k_} — {"ЕСТЬ" if v else "нет"}' for k_, v in res['kv'].items())]
    if res['truncated']:
        L_.append('⚠ проверка прервана по времени — вердикт невозможен, повторите.')
    if res['hits']:
        L_.append('<b>НАЙДЕНО (вывод по LOCKBOX существует):</b>')
        L_ += [f'  {w}: маркер №{i}, вхождений {n}' for w, i, n in res['hits'][:30]]
        L_.append('<b>Вердикт: УТЕЧКА / вывод есть. Повтор ЗАПРЕЩЁН</b> (§2.43.15): запись «H2-D3: технический сбой с частичным выводом, финал закрыт без результата».')
    elif not res['truncated']:
        L_.append('Маркеров результата в проверенных хранилищах НЕТ. <b>Вердикт по проверенному: утечки нет.</b> Логи платформы Render (stdout/stderr бота) эта команда не видит — поищите в них те же строки '
                  'вручную (начала маркеров выше; полные тексты — в BOT_SPEC §2.43.15 и research_h2final.MARKERS).')
    return '\n'.join(L_)


async def run_final_h2(params, progress=None):
    """/final_h2 [confirm | retry confirm | audit | report]. Без аргумента — показ §2.43.11, k и состояния флага (LOCKBOX не читается). confirm — единственный прогон (free → running). retry confirm — ровно одна
    техническая повторная попытка (tech_fail_no_output → running) после чистого audit (§2.43.16–17). audit — проверка утечки. report — сохранённый итоговый отчёт."""
    fdir = params.get('fdir') or RF.FD.factory_dir()
    d = params.get('bdir') or B.battery_dir()
    version = params.get('version', '')
    fees = RB.fees_of(params)
    mode = params.get('mode') or ('confirm' if params.get('confirm') else 'show')
    st = FS.get(NAME, fdir, LEGACY)

    async def say(m):
        if progress:
            await progress(m)
    if mode == 'report':
        return RF.kv_get(KV_REPORT, fdir) or f'⛔ /final_h2 report: итогового отчёта нет (состояние флага: {st["state"]})'
    if mode == 'audit':
        return audit_text(leak_audit(params, fdir, d))
    acting = mode in ('confirm', 'retry')
    suffix = ' (флаг НЕ взят)' if acting else ''
    idx = B.load_index(d)
    cov = B.coverage(d, idx)
    if not idx['symbols'] or cov['n_done'] < cov['n_symbols']:
        return f'⛔ /final_h2 отказ: данные неполны (символов {cov["n_symbols"]}, завершено {cov["n_done"]}) — выполните /battery_load{suffix}'
    await say(f'расчёт k: синтетика {K_REPS} повторов, LOCKBOX не читается…')
    kres = compute_k(dict(params, bdir=d), K_REPS)
    gc.collect()                                                    # пре-панель и синтетика k не должны жить вместе с полной панелью LOCKBOX
    if kres['k'] is None:
        return f'⛔ /final_h2 отказ: k не рассчитан — {kres.get("error")}{suffix}'
    RF.kv_set(KV_K, {k_: v for k_, v in kres.items() if k_ != 't'}, fdir)
    spec_k = read_spec_k(params.get('spec_path'))
    k_ok = spec_k is not None and round(spec_k, 3) == round(kres['k'], 3)
    stale = FS.is_stale(st)
    state_line = f'Состояние флага финала H2: <b>{st["state"]}</b> (попытка {st.get("attempt", 0)} из {FS.MAX_ATTEMPTS})' + (' [legacy: ключ v113]' if st.get('legacy') else '') + (' — зависший' if stale else '')
    head = spec_summary() + ['',
                             f'<b>k = {kres["k"]:.3f}</b> (m0 = {kres["m0"]:.3f}; k = max(1, m0/{NULL_MEDIAN_ABS_T}); {kres["reps"]} повторов, событий H2-D3 на пре-панели {kres["n_events"]}, '
                             f'измеряемый участок {kres["n_lock"]} суток перед LOCKBOX; seed {RF.SEED}).',
                             f'Строка в BOT_SPEC: {SPEC_K_KEY} {("%.3f" % spec_k) if spec_k is not None else "не записана"} — ' + ('СОВПАДАЕТ с расчётом.' if k_ok else
                                                                                                          'НЕ совпадает/не записана: пришлите k, я запишу его в BOT_SPEC отдельным коммитом; без совпадения confirm не запустится.'),
                             state_line]
    retry_ok = (st['state'] == FS.TECH_FAIL_NO_OUTPUT or stale) and (st.get('attempt') or 1) == 1
    if mode == 'show':
        if st['state'] == FS.FREE:
            head.append('Запуск: /final_h2 confirm (один прогон на LOCKBOX).')
        elif retry_ok:
            head.append('Сбой без вывода по LOCKBOX. Доступна РОВНО ОДНА повторная попытка: сначала /final_h2 audit (проверка утечки), затем /final_h2 retry confirm (BOT_SPEC §2.43.16).')
        elif st['state'] == FS.DONE:
            head.append('Финал выполнен: итоговый отчёт — /final_h2 report. Повтор запрещён.')
        else:
            head.append('Запуск невозможен (состояние флага выше).')
        return '\n'.join(head)
    if mode == 'confirm':
        if st['state'] != FS.FREE:
            hint = ' Повторная попытка: /final_h2 retry confirm (после /final_h2 audit).' if retry_ok else ''
            return f'⛔ /final_h2 отказ: флаг финала H2 уже взят (состояние {st["state"]}) — финал выполняется один раз (BOT_SPEC §2.43.11 п. 4, §2.43.17).{hint}'
        attempt = 1
    elif mode == 'retry':
        if not retry_ok:
            why = {FS.FREE: 'финал ещё не запускался — используйте /final_h2 confirm', FS.DONE: 'финал уже выполнен (/final_h2 report)', FS.TECH_FAIL_FINAL: 'повторная попытка уже использована: H2-D3 закрыт как технически непроверяемый',
                   FS.RUNNING: 'расчёт, возможно, ещё идёт (не прошло 30 минут или процесс жив) — подождите'}.get(st['state'], 'повтор недоступен')
            return f'⛔ /final_h2 retry отказ: {why} (флаг не тронут)'
        res = leak_audit(params, fdir, d)
        if not res['clean']:
            return audit_text(res) + '\n⛔ /final_h2 retry отказ: audit не чистый — повтор ЗАПРЕЩЁН (флаг не тронут)'
        attempt = 2
    else:
        return f'⛔ /final_h2: неизвестный режим {mode}'
    if not k_ok:
        return '⛔ /final_h2 отказ: k не записан в BOT_SPEC или не совпадает с расчётом (флаг НЕ взят)\n' + '\n'.join(head[-3:])
    est, cells = estimate_peak_mb(d)
    limit = mem_limit_mb()
    if est > MEM_SAFETY * limit:
        return (f'⛔ /final_h2 отказ: оценка пика памяти полной панели {est:.0f} МБ ({cells / 1e6:.2f} млн ячеек) выше {MEM_SAFETY * 100:.0f}% лимита RESEARCH_MEM_MB={limit:.0f} — '
                f'поднимите RESEARCH_MEM_MB в ENV и повторите (флаг НЕ взят)')
    if attempt == 2 and st['state'] == FS.RUNNING:
        if FS.reconcile_stale(NAME, fdir, LEGACY, note='audit чистый, зависший running → tech_fail_no_output (BOT_SPEC §2.43.16)') is None:
            return '⛔ /final_h2 retry отказ: состояние изменилось параллельно (флаг не тронут)'
    if FS.begin(NAME, fdir, attempt, LEGACY, k=kres['k'], version=version) is None:
        return '⛔ /final_h2 отказ: флаг финала H2 уже взят параллельным запуском'
    RF.kv_set_if_absent(KV_FLAG, {'t': time.time(), 'kind': 'final_h2', 'k': kres['k'], 'version': version}, fdir)      # старый ключ — для совместимости
    ok_write = False
    try:
        session = L.open_lockbox_session(final=True)
        await say(f'LOCKBOX открыт (попытка {attempt} из {FS.MAX_ATTEMPTS})…')
        out = lockbox_eval(d, fees, kres['k'], session)
        if out is None:
            raise RuntimeError('нет данных D1 для LOCKBOX')
        ev, ref, months, base_meta = out['ev'], out['ref'], out['months'], out['meta']
        text = build_final_report(version, head, ev, ref, months, base_meta)
        RF.kv_set(KV_RESULT, {'eval': ev, 'ref': ref, 'months': {str(k_): v for k_, v in months.items()}, 'k': kres['k'], 'version': version, 'attempt': attempt}, fdir)
        RF.kv_set(KV_REPORT, text, fdir)
        ok_write = True                                              # итоговый отчёт записан в БД — только теперь done
    except Exception as e:
        if ok_write:
            raise
        err = f'{type(e).__name__}: {str(e)[:200]}'
        RF.kv_set(KV_CRASH, {'t': time.time(), 'error': err, 'attempt': attempt, 'version': version}, fdir)
        new = FS.fail_no_output(NAME, fdir, LEGACY, error=err)
        final = new is not None and new['state'] == FS.TECH_FAIL_FINAL
        return (f'❌ /final_h2: технический сбой на попытке {attempt} из {FS.MAX_ATTEMPTS}: {err}. Вывода по LOCKBOX нет (ничего не записано). Состояние флага: <b>{new["state"] if new else "?"}</b>. ' +
                ('H2-D3 закрыт как технически непроверяемый, третьей попытки нет (BOT_SPEC §2.43.16).' if final else 'Доступна РОВНО ОДНА повторная попытка: /final_h2 audit, затем /final_h2 retry confirm.'))
    FS.finish_done(NAME, fdir, LEGACY, note='итоговый отчёт записан')
    RF.kv_set(KV_DONE, True, fdir)
    if not ev['pass']:
        RF.kv_set(KV_EXHAUSTED, {'t': time.time(), 'by': 'final_h2'}, fdir)
    return text


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
