"""v113: одиночный финал H2-D3 (BOT_SPEC §2.43.11) — k считается до открытия и без чтения LOCKBOX, confirm без совпавшего k отказывает, повтор запрещён, флаги соседних финалов не затронуты;
премия за волатильность (§2.46) — разбор ответа Deribit, стоп при 451/403, VRP по неперекрывающимся точкам, гейт, кондор на синтетике с IV = фактической волатильности."""
import asyncio
import json
import math
import os
import tempfile
import unittest

import numpy as np

import _common
import battery_fixtures as BF
import research_bdata as B
import research_factory as RF
import research_h2final as H2
import research_ma_lib as L
import research_runner as RR
import research_vrp as V

ROOT = _common.ROOT
DAY = V.DAY


def run(c):
    return asyncio.run(c)


def spec_file(k_text):
    p = os.path.join(tempfile.mkdtemp(), 'SPEC.md')
    with open(p, 'w', encoding='utf-8') as f:
        f.write('текст\n' + H2.SPEC_K_KEY + ' ' + k_text + '\nконец\n')
    return p


class SpecK(unittest.TestCase):
    def test_placeholder_is_none_and_number_is_read(self):
        self.assertIsNone(H2.read_spec_k(spec_file('не записан')))
        self.assertAlmostEqual(H2.read_spec_k(spec_file('1.234 (m0=0.8)')), 1.234)
        real = H2.read_spec_k()
        self.assertAlmostEqual(real, 1.122)                       # k записан отдельным коммитом по отчёту /final_h2 2026-10-29 (v113)
        text = open(os.path.join(ROOT, 'BOT_SPEC.md'), encoding='utf-8').read()
        self.assertIn('FINALH2-K:', text)
        self.assertIn('#### 2.43.11.', text)
        self.assertIn('### 2.46.', text)


class H2Final(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.old_reps = H2.K_REPS
        H2.K_REPS = 3
        cls.bdir, cls.fdir = tempfile.mkdtemp(), tempfile.mkdtemp()
        BF.write_battery(cls.bdir, n_syms=90, delist_n=3, list_hi=1900)        # листинги и в последних сутках перед LOCKBOX, и внутри него
        cls.params = {'bdir': cls.bdir, 'fdir': cls.fdir, 'fee_taker': 0.0005, 'fee_maker': 0.0002, 'version': 'vT'}

    @classmethod
    def tearDownClass(cls):
        H2.K_REPS = cls.old_reps

    def reset(self):
        con = RF._con(self.fdir)
        con.execute('DELETE FROM kv')
        con.commit()
        con.close()
        L._lockbox_session_state['opened'] = False

    def test_lockbox_days_from_index_names_only(self):
        d_lock, lock_day = H2.lockbox_days(self.bdir)
        self.assertEqual(d_lock, 90)                              # 2025-01-01 … 2025-03-31 (индекс до 2025-03)
        self.assertEqual(lock_day * DAY, L.lockbox_start_ms())

    def test_k_is_computed_without_lockbox(self):
        modes = []
        orig, orig_open = B.load_panel, L.open_lockbox_session

        def spy(d, mode='pre', session=None, *a, **kw):
            modes.append((mode, session))
            return orig(d, mode, session, *a, **kw)

        def boom(*a, **kw):
            raise AssertionError('LOCKBOX открыт при расчёте k')
        B.load_panel, L.open_lockbox_session = spy, boom
        try:
            r = H2.compute_k(self.params, reps=3)
        finally:
            B.load_panel, L.open_lockbox_session = orig, orig_open
        self.assertEqual(modes, [('pre', None)])
        self.assertGreaterEqual(r['k'], 1.0)
        self.assertEqual(r['reps'], 3)
        self.assertEqual(r['n_lock'], 90)
        r2 = H2.compute_k(self.params, reps=3)
        self.assertEqual(r['k'], r2['k'])                         # детерминированно (seed)

    def test_confirm_requires_matching_k_single_run_and_other_flags_untouched(self):
        self.reset()
        no = run(H2.run_final_h2(dict(self.params, spec_path=spec_file('не записан'))))
        self.assertIn('свободен', no)
        self.assertIn('k = ', no)
        self.assertIsNone(RF.kv_get(H2.KV_FLAG, self.fdir))
        refused = run(H2.run_final_h2(dict(self.params, confirm=True, spec_path=spec_file('не записан'))))
        self.assertIn('отказ', refused)
        self.assertIsNone(RF.kv_get(H2.KV_FLAG, self.fdir))                 # флаг НЕ взят
        wrong = run(H2.run_final_h2(dict(self.params, confirm=True, spec_path=spec_file('9.999'))))
        self.assertIn('не совпадает', wrong)
        self.assertIsNone(RF.kv_get(H2.KV_FLAG, self.fdir))
        self.assertFalse(L._lockbox_session_state['opened'])                # LOCKBOX не открывался
        k = H2.compute_k(self.params, reps=3)['k']
        ok = run(H2.run_final_h2(dict(self.params, confirm=True, spec_path=spec_file('%.3f' % k))))
        self.assertIn('РЕЗУЛЬТАТ', ok)
        self.assertIsNotNone(RF.kv_get(H2.KV_FLAG, self.fdir))
        self.assertTrue(RF.kv_get(H2.KV_DONE, self.fdir))
        res = RF.kv_get(H2.KV_RESULT, self.fdir)
        passed = res['eval']['pass']
        self.assertEqual(RF.kv_get(H2.KV_EXHAUSTED, self.fdir) is not None, not passed)
        self.assertEqual('LOCKBOX battery исчерпан' in ok, not passed)
        self.assertIsNone(RF.kv_get('battery_final_started', self.fdir))    # флаг серии /battery не затронут
        self.assertIsNone(RF.kv_get('final_started', self.fdir))            # общий флаг §2.41/§2.42 не затронут
        again = run(H2.run_final_h2(dict(self.params, confirm=True, spec_path=spec_file('%.3f' % k))))
        self.assertIn('уже взят', again)                                    # повтор запрещён

    def test_criteria_strict(self):
        lo = H2.E.day_bounds('LOCKBOX')[0]
        days = np.arange(lo - 10, lo + 200, dtype=np.int64)
        rng = np.random.default_rng(1)
        r = rng.normal(0.0001, 0.0005, len(days))
        base, stress = {'days': days, 'r': r}, {'days': days, 'r': r * 0.9}
        months = {}
        ev = H2.evaluate(base, stress, 1.0, months)
        self.assertIsNone(ev['crit']['up'])                                 # нет месяцев BTC — критерий не применим (как §2.42)
        ev2 = H2.evaluate(base, stress, 5.0, months)
        self.assertFalse(ev2['crit']['t_adj'])                              # большой k гасит t
        self.assertLess(ev2['t_adj'], ev['t_adj'])


# ── VRP ─────────────────────────────────────────────────────────────────────────────────────────────
def walk_closes(n, seed=3, vol=0.03, day0=18000):
    rng = np.random.default_rng(seed)
    c = 100.0 * np.exp(np.cumsum(rng.normal(0, vol, n)))
    return {day0 + i: float(x) for i, x in enumerate(c)}


class Rv(unittest.TestCase):
    def test_rv_forward_formula_and_gaps(self):
        c = {i: 100.0 * (1.01 if i % 2 else 1.0) for i in range(40)}
        cs = np.array([c[i] for i in range(0, 31)])
        exp = float(np.diff(np.log(cs)).std(ddof=1) * math.sqrt(365) * 100)
        self.assertAlmostEqual(V.rv_forward(c, 0), exp)
        c.update({i: 100.0 for i in range(40, 80)})
        del c[10]
        self.assertIsNone(V.rv_forward(c, 0))                              # пропущены сутки внутри окна
        self.assertIsNotNone(V.rv_forward(c, 11))                          # окно после пропуска полное

    def test_window_is_after_t(self):
        c = walk_closes(80)
        d0 = min(c)
        base = V.rv_forward(c, d0 + 5)
        c2 = dict(c)
        c2[d0 + 4] *= 3.0                                                   # данные ДО t не влияют
        self.assertEqual(V.rv_forward(c2, d0 + 5), base)
        c3 = dict(c)
        c3[d0 + 5 + 10] *= 1.5                                              # данные внутри окна влияют
        self.assertNotEqual(V.rv_forward(c3, d0 + 5), base)


class NonOverlap(unittest.TestCase):
    def test_points_every_30_days_and_t_formula(self):
        rng = np.random.default_rng(5)
        days = np.arange(1000, 1300, dtype=np.int64)
        slow = np.repeat(rng.normal(5.0, 4.0, 10), 30)                      # VRP почти постоянно внутри 30-дневных блоков (перекрытие)
        vrp = slow + rng.normal(0, 0.2, 300)
        ser = {'days': days, 'vrp': vrp, 'dvol': vrp + 50, 'rv': np.full(300, 50.0)}
        no = V.nonoverlap_t(ser)
        pts = vrp[::30]
        self.assertEqual(no['n'], 10)
        self.assertAlmostEqual(no['t'], float(pts.mean() / (pts.std(ddof=1) / math.sqrt(10))))
        naive = float(vrp.mean() / (vrp.std(ddof=1) / math.sqrt(300)))
        self.assertGreater(abs(naive), abs(no['t']))                        # наивный t по всем дням завышен (окна перекрываются)

    def test_gate_depends_on_non_overlapping_t_and_share(self):
        c = walk_closes(700, vol=0.03)
        rv = {t: V.rv_forward(c, t) for t in range(min(c), min(c) + 600)}
        rng = np.random.default_rng(2)
        hi = {t: r + 12 + rng.normal(0, 3) for t, r in rv.items()}
        lo = {t: r - 12 + rng.normal(0, 3) for t, r in rv.items()}
        self.assertTrue(V.measure(hi, c)['gate'])
        self.assertFalse(V.measure(lo, c)['gate'])
        eq = V.measure({t: r for t, r in rv.items()}, c)
        self.assertFalse(eq['gate'])                                        # VRP ≡ 0 → гейт закрыт

    def test_top_drops_are_non_overlapping(self):
        c = walk_closes(500, seed=9)
        dv = {t: 60.0 for t in range(min(c), min(c) + 420)}
        m = V.measure(dv, c)
        ds = sorted(d[0] for d in m['drops'])
        self.assertLessEqual(len(ds), 10)
        self.assertTrue(all(b - a >= 30 for a, b in zip(ds, ds[1:])))


def fake_fetch(pages, status=200, fail_first_res=None):
    """Поддельный fetch: pages — список ответов result по убыванию дат; сохраняет вызовы."""
    calls = []

    async def fetch(url, params=None, method='GET'):
        calls.append(dict(params))
        if fail_first_res and params['resolution'] == fail_first_res:
            return {'status': 400, 'headers': {}, 'body': b'{"error":"bad resolution"}', 'error': ''}
        if status != 200:
            return {'status': status, 'headers': {}, 'body': b'blocked', 'error': ''}
        i = len([c for c in calls if c['resolution'] == params['resolution']]) - 1
        if i >= len(pages):
            return {'status': 200, 'headers': {}, 'body': json.dumps({'result': {'data': [], 'continuation': None}}).encode(), 'error': ''}
        return {'status': 200, 'headers': {}, 'body': json.dumps({'result': pages[i]}).encode(), 'error': ''}
    fetch.calls = calls
    return fetch


def dvol_pages(n=60, end_day=19800):
    rows = [[(end_day - i) * DAY, 50.0, 55.0, 48.0, 52.0 + i] for i in range(n)]
    rows.sort()
    p1, p2 = rows[n // 2:], rows[:n // 2]
    return [{'data': p1, 'continuation': p2[-1][0] + DAY * 0 + 0 if False else p1[0][0]}, {'data': p2, 'continuation': None}]


class Probe(unittest.TestCase):
    def test_pagination_and_report(self):
        pages = dvol_pages()
        f = fake_fetch(pages)
        r = run(V.fetch_dvol(f, 'BTC', 0, 19900 * DAY))
        self.assertEqual(len(r['rows']), 60)
        self.assertEqual(r['status'], 200)
        self.assertEqual(r['resolution'], '1D')
        self.assertEqual(r['pages'] >= 2, True)
        res = {'BTC': r, 'ETH': run(V.fetch_dvol(fake_fetch(pages), 'ETH', 0, 19900 * DAY))}
        text, ok = V.probe_report(res, 0)
        self.assertTrue(ok)
        self.assertIn('точек 60', text)
        self.assertIn('DVOL доступен', text)

    def test_http_451_stops(self):
        f = fake_fetch([], status=451)
        r = run(V.fetch_dvol(f, 'BTC', 0, 19900 * DAY))
        self.assertTrue(r['blocked'])
        self.assertEqual(len(f.calls), 1)                                   # дальше не идём
        text, ok = V.probe_report({'BTC': r, 'ETH': r}, 0)
        self.assertFalse(ok)
        self.assertIn('ДОСТУП ЗАКРЫТ', text)
        self.assertIn('дальше не идём', text)

    def test_bad_resolution_falls_back(self):
        f = fake_fetch(dvol_pages(), fail_first_res='1D')
        r = run(V.fetch_dvol(f, 'BTC', 0, 19900 * DAY))
        self.assertEqual(r['resolution'], '86400')
        self.assertTrue(r['rows'])

    def test_run_probe_saves_cache_and_blocks_measure(self):
        d = tempfile.mkdtemp()
        txt = run(V.run_probe({'vrp_dir': d, 'now_ms': 19900 * DAY}, fetch=fake_fetch([], status=403)))
        self.assertIn('ДОСТУП ЗАКРЫТ', txt)
        out = run(V.run_measure({'vrp_dir': d, 'bdir': tempfile.mkdtemp()}))
        self.assertIn('дальше не идём', out)
        out2 = run(V.run_sim({'vrp_dir': d, 'bdir': tempfile.mkdtemp()}))
        self.assertIn('дальше не идём', out2)
        self.assertIn('/vrp_probe', run(V.run_measure({'vrp_dir': tempfile.mkdtemp()})))


class Condor(unittest.TestCase):
    def test_fair_iv_gives_zero_before_costs_and_negative_after(self):
        iv, s0, n = 60.0, 100.0, 60000
        sw = iv / 100 * math.sqrt(7 / 365)
        rng = np.random.default_rng(11)
        st = s0 * np.exp(-0.5 * sw * sw + sw * rng.standard_normal(n))      # мартингал по цене: IV = фактическая волатильность
        w = [V.condor_week(s0, x, iv) for x in st]
        gross = np.array([x['pnl_gross'] for x in w])
        net = np.array([x['pnl_net'] for x in w])
        costs = w[0]['costs']
        se = gross.std(ddof=1) / math.sqrt(n)
        self.assertLess(abs(gross.mean()), 3 * se)                           # около нуля до издержек
        self.assertLess(net.mean(), -0.5 * costs)                            # после издержек — минус около издержек
        self.assertLess(abs(net.mean() + costs), 3 * se)
        self.assertGreater(costs, 0)
        rets = np.array([x['ret'] for x in w])
        self.assertGreaterEqual(rets.min(), -V.RISK_WEEK - 1e-12)            # максимальный убыток кондора = 2% капитала
        self.assertAlmostEqual(rets.min(), -V.RISK_WEEK, places=6)

    def test_bs_put_call_parity_and_payoff_beyond_wings(self):
        c, p = V.bs_call(100, 105, 0.6, 7 / 365), V.bs_put(100, 105, 0.6, 7 / 365)
        self.assertAlmostEqual(c - p, 100 - 105)
        w = V.condor_week(100.0, 1000.0, 60.0)                               # цена далеко за колл-крылом
        self.assertAlmostEqual(w['pnl_net'], -w['maxloss'], places=9)
        up = V.condor_week(100.0, 100.0, 60.0)                               # цена на месте: вся премия минус издержки
        self.assertGreater(up['pnl_net'], 0)


class Pipeline(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.bdir = tempfile.mkdtemp()
        BF.write_battery(cls.bdir, n_syms=18, delist_n=0)
        cls.closes = V.load_closes(cls.bdir, 'BTCUSDT')

    def vrp_dir(self, shift):
        d = tempfile.mkdtemp()
        days = sorted(self.closes)
        rng = np.random.default_rng(4)
        for cur in V.CURRENCIES:
            c = V.load_closes(self.bdir, f'{cur}USDT')
            rows = {}
            for t in days[400:1500]:
                r = V.rv_forward(c, t)
                if r is not None:
                    rows[t * DAY] = r + shift + float(rng.normal(0, 3))
            V.save_dvol(cur, rows, d)
        with open(os.path.join(d, 'probe.json'), 'w') as f:
            json.dump({'ok': True, 'blocked': False, 't': 0}, f)
        return d

    def test_gate_open_runs_sim_and_marks_approximation(self):
        d = self.vrp_dir(15.0)
        p = {'vrp_dir': d, 'bdir': self.bdir, 'version': 'vT'}
        txt = run(V.run_measure(p))
        self.assertIn('ГЕЙТ ПРОЙДЕН', txt)
        self.assertIn('BTC', txt)
        self.assertIn('ETH', txt)
        self.assertIn('перед крупнейшими падениями BTC', txt)
        sim = run(V.run_sim(p))
        self.assertIn('приближение: без перекоса по страйкам и временной структуры; решения о торговле по нему не принимаются', sim)
        self.assertIn('Шарп', sim)
        self.assertIn('доля убыточных недель', sim)

    def test_gate_closed_refuses_sim(self):
        d = self.vrp_dir(-15.0)
        p = {'vrp_dir': d, 'bdir': self.bdir}
        self.assertIn('ГЕЙТ НЕ ПРОЙДЕН', run(V.run_measure(p)))
        out = run(V.run_sim(p))
        self.assertIn('отказ', out)
        self.assertNotIn('Шарп', out)


class Wiring(unittest.TestCase):
    def test_tasks_commands_version_and_worker(self):
        for t in ('final_h2', 'vrp_probe', 'vrp_measure', 'vrp_sim'):
            self.assertIn(t, RR.TASKS)
        src = open(os.path.join(ROOT, 'async_bot.py'), encoding='utf-8').read()
        for c in ("cmd == '/final_h2'", "cmd == '/vrp_probe'", "cmd == '/vrp_measure'", "cmd == '/vrp_sim'", "'2026-11-12-v113'", "'2026-11-13-v114'"):
            self.assertIn(c, src)
        import subprocess
        out = subprocess.run(['git', 'diff', '--name-only', '478834e', '--', 'bybit_worker.py'], cwd=ROOT, capture_output=True, text=True).stdout
        self.assertEqual(out.strip(), '')


if __name__ == '__main__':
    unittest.main()


# ── v114: память финального пути и сбой после взятия флага (BOT_SPEC §2.43.13–14) ──────────────────
class MemoryFix(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.bdir = tempfile.mkdtemp()
        BF.write_battery(cls.bdir, n_syms=60, delist_n=3, list_hi=1900)
        cls.fdir = tempfile.mkdtemp()

    def test_blockwise_masks_equal_make_ctx_and_results_identical(self):
        P = B.load_panel(self.bdir, 'pre')
        full = H2.S.make_ctx(P)
        liq, top = full['liq'].copy(), full['top'].copy()
        P2 = B.load_panel(self.bdir, 'pre')
        ctx = H2.make_ctx_h2(P2)
        self.assertTrue(np.array_equal(ctx['liq'], liq))
        self.assertTrue(np.array_equal(ctx['top'], top))
        for k in ('QV', 'L', 'G', 'GC'):
            self.assertNotIn(k, P2)                                      # неиспользуемое в H2 освобождено
        fees = {'taker': 0.0005, 'maker': 0.0002}
        env_old = {'P': P, 'ctx': full, 'days': np.arange(P['day0'], P['day0'] + P['nd'], dtype=np.int64)}
        env_new = {'P': P2, 'ctx': ctx, 'days': env_old['days']}
        for mult in (1.0, 2.0):
            a, b = H2.RB.run_cell(H2.CELL, env_old, fees, mult), H2.RB.run_cell(H2.CELL, env_new, fees, mult)
            self.assertTrue(np.array_equal(a['r'], b['r']))              # H2-D3 даёт побитово тот же результат
            self.assertEqual(a['meta'], b['meta'])

    def test_estimate_guard_refuses_before_flag(self):
        fdir = tempfile.mkdtemp()
        old_reps = H2.K_REPS
        H2.K_REPS = 2
        os.environ['RESEARCH_MEM_MB'] = '100'
        try:
            k = H2.compute_k({'bdir': self.bdir}, reps=2)['k']
            txt = run(H2.run_final_h2({'bdir': self.bdir, 'fdir': fdir, 'confirm': True, 'spec_path': spec_file('%.3f' % k)}))
        finally:
            del os.environ['RESEARCH_MEM_MB']
            H2.K_REPS = old_reps
        self.assertIn('оценка пика памяти', txt)                            # k совпал, отказ именно по памяти
        self.assertIn('флаг НЕ взят', txt)
        self.assertIsNone(RF.kv_get(H2.KV_FLAG, fdir))

    def test_crash_after_flag_is_recorded_and_flag_stays(self):
        old_reps, old_eval = H2.K_REPS, H2.lockbox_eval
        H2.K_REPS = 3
        fdir = tempfile.mkdtemp()
        L._lockbox_session_state['opened'] = False

        def boom(*a, **kw):
            raise MemoryError('имитация нехватки памяти')
        H2.lockbox_eval = boom
        try:
            k = H2.compute_k({'bdir': self.bdir}, reps=3)['k']
            with self.assertRaises(MemoryError):
                run(H2.run_final_h2({'bdir': self.bdir, 'fdir': fdir, 'confirm': True, 'spec_path': spec_file('%.3f' % k)}))
        finally:
            H2.lockbox_eval, H2.K_REPS = old_eval, old_reps
        self.assertIsNotNone(RF.kv_get(H2.KV_FLAG, fdir))                   # флаг остаётся (повтор запрещён, снять может только владелец)
        self.assertIn('MemoryError', RF.kv_get(H2.KV_CRASH, fdir)['error'])
        self.assertIsNone(RF.kv_get(H2.KV_DONE, fdir))                      # результата нет
        self.assertIsNone(RF.kv_get(H2.KV_RESULT, fdir))
        self.assertIsNone(RF.kv_get(H2.KV_EXHAUSTED, fdir))                 # «исчерпан» не записывается без результата

    def test_full_panel_fits_300mb_address_space(self):
        """Воспроизведение падения 2026-10-29: полная панель ≈ 700 символов под RLIMIT_AS = 300 МБ (в дочернем процессе, как в research_runner)."""
        import subprocess
        import sys
        d = tempfile.mkdtemp()
        BF.write_battery(d, n_syms=700, first_day='2020-01-01', last_day='2026-09-30', delist_n=20, list_hi=2400)
        code = (
            "import resource, sys\n"
            "lim = 300 * 1024 * 1024\n"
            "resource.setrlimit(resource.RLIMIT_AS, (lim, lim))\n"
            f"sys.path[:0] = [{ROOT!r}]\n"
            "import research_h2final as H2, research_battery as RB, research_ma_lib as L\n"
            f"out = H2.lockbox_eval({d!r}, RB.fees_of({{}}), 1.122, L.open_lockbox_session(final=True))\n"
            "print('OK', out['ev']['n'])\n")
        env = dict(os.environ, MALLOC_ARENA_MAX='2', OMP_NUM_THREADS='1', OPENBLAS_NUM_THREADS='1', MKL_NUM_THREADS='1', NUMEXPR_NUM_THREADS='1')
        r = subprocess.run([sys.executable, '-c', code], capture_output=True, text=True, env=env, timeout=600)
        self.assertIn('OK 638', r.stdout, r.stderr[-500:])
        est, cells = H2.estimate_peak_mb(d)
        self.assertLess(est, 0.85 * 300)                                    # такая панель проходит предохранитель
