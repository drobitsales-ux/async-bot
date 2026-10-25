"""v106: одиночный финал CRYPTO 4h T-MA-10-50 (BOT_SPEC §2.42): k на синтетике без чтения LOCKBOX, кандидат = ячейка фабрики побитово на TRAIN, один прогон (общий флаг), критерии."""
import os
import tempfile
import unittest

import numpy as np

import _common
from _common import a
import factory_fixtures as F
import research_fdata as FD
import research_factory as RF
import research_ffinal as FIN
import research_ma_lib as L
import test_v103_e2e as E2E

ROOT = _common.ROOT


def run(c):
    return E2E.run(c)


class Criteria(unittest.TestCase):
    LB = 20_089                                                                 # 2025-01-01 как номер дня

    def parts(self, mean, std, n=120, seed=1, stress_shift=0.0):
        rng = np.random.default_rng(seed)
        days = np.arange(self.LB, self.LB + n)
        r = rng.normal(mean, std, n)
        return {'days': days, 'r': r}, {'days': days, 'r': r - stress_shift}

    def test_pass_and_each_criterion_blocks(self):
        base, stress = self.parts(0.004, 0.002, seed=2)
        ev = FIN.evaluate(base, stress, 1.0, {})
        self.assertTrue(ev['crit']['t_adj'] and ev['crit']['sharpe'] and ev['crit']['dd'] and ev['crit']['stress'])
        self.assertIsNone(ev['crit']['up'])                                      # пустые группы месяцев — критерии не применимы
        self.assertTrue(ev['pass'])
        # поправка k делит t: большое k блокирует
        ev2 = FIN.evaluate(base, stress, 1000.0, {})
        self.assertFalse(ev2['crit']['t_adj'])
        self.assertFalse(ev2['pass'])
        self.assertAlmostEqual(ev2['t_adj'], ev2['t5'] / 1000.0)
        # стресс ×2: mean ≤ 0 блокирует
        _b, st = self.parts(0.004, 0.002, seed=2, stress_shift=0.02)
        self.assertFalse(FIN.evaluate(base, st, 1.0, {})['pass'])
        # просадка > 15% блокирует
        r = base['r'].copy(); r[10:40] = -0.01
        ev3 = FIN.evaluate({'days': base['days'], 'r': r}, stress, 1.0, {})
        self.assertFalse(ev3['crit']['dd'])
        self.assertFalse(ev3['pass'])
        # Шарп < 0.5 блокирует
        low, lows = self.parts(0.00005, 0.01, seed=3)
        self.assertFalse(FIN.evaluate(low, lows, 1.0, {})['crit']['sharpe'])

    def test_month_groups_and_btc_months(self):
        # BTC: январь 2025 растёт, февраль падает, март — неполный (последний бар в середине марта)
        step = 4 * 3_600_000
        t0 = F.ms(2024, 12, 20)
        ts = np.arange(t0, F.ms(2025, 3, 15), step)
        c = np.full(len(ts), 100.0)
        jan_end = np.searchsorted(ts, F.ms(2025, 2, 1))
        feb_end = np.searchsorted(ts, F.ms(2025, 3, 1))
        c[jan_end - 1:] = 110.0                                                  # к концу января +10% относительно декабря
        c[feb_end - 1:] = 99.0                                                   # к концу февраля −10% относительно января
        months = FIN.btc_months(ts, c, int(ts[-1]) + step)
        k_jan, k_feb = FIN.month_key(F.ms(2025, 1, 15)), FIN.month_key(F.ms(2025, 2, 15))
        self.assertEqual(set(months), {k_jan, k_feb})                            # март неполный — исключён
        self.assertAlmostEqual(months[k_jan], 0.10)
        self.assertAlmostEqual(months[k_feb], 99.0 / 110.0 - 1)
        # доходность портфеля: +1 б.п. в январе, −2 б.п. в феврале
        days = np.arange(F.ms(2025, 1, 1) // 86_400_000, F.ms(2025, 3, 1) // 86_400_000)
        r = np.where(days < F.ms(2025, 2, 1) // 86_400_000, 1e-4, -2e-4)
        g = FIN.month_group_means(days, r, months)
        self.assertEqual((g['up']['n_months'], g['down']['n_months']), (1, 1))
        self.assertAlmostEqual(g['up']['mean'], 1e-4)
        self.assertAlmostEqual(g['down']['mean'], -2e-4)
        # критерий «mean ≥ 0 в месяцы падения» блокирует прохождение
        base = {'days': days, 'r': r}
        ev = FIN.evaluate(base, {'days': days, 'r': r}, 1.0, months)
        self.assertFalse(ev['crit']['down'])
        self.assertFalse(ev['pass'])

    def test_spec_k_line(self):
        f = tempfile.NamedTemporaryFile('w', suffix='.md', delete=False, encoding='utf-8')
        f.write('x\nFINAL4H-K: 1.137 (m0=0.77)\n')
        f.close()
        self.assertEqual(FIN.read_spec_k(f.name), 1.137)
        g = tempfile.NamedTemporaryFile('w', suffix='.md', delete=False, encoding='utf-8')
        g.write('FINAL4H-K: (не записан — ожидается)\n')
        g.close()
        self.assertIsNone(FIN.read_spec_k(g.name))
        spec = open(os.path.join(ROOT, 'BOT_SPEC.md'), encoding='utf-8').read()
        self.assertTrue(any(ln.startswith('FINAL4H-K:') for ln in spec.splitlines()))
        self.assertIn('### 2.42.', spec)
        self.assertIn('T-MA-10-50', spec)


class FinalFlow(E2E.Base):
    def setUp(self):
        self.reset(keep=())
        L._lockbox_session_state['opened'] = False
        self.k_reps = FIN.K_REPS
        FIN.K_REPS = 16
        self.params = dict(self.__class__.params, version='2026-11-05-v106')

    def tearDown(self):
        FIN.K_REPS = self.k_reps
        L._lockbox_session_state['opened'] = False

    def spec_with_k(self, k):
        f = tempfile.NamedTemporaryFile('w', suffix='.md', delete=False, encoding='utf-8')
        f.write(f'{FIN.SPEC_K_KEY} {k:.3f}\n')
        f.close()
        return f.name

    def test_k_on_synthetic_without_lockbox(self):
        orig_load, orig_open = FD.load_crypto_15m, L.open_lockbox_session
        modes = []

        def load(vdir, sym, mode='pre', session=None):
            modes.append(mode)
            assert mode == 'pre' and session is None, 'k не должен читать LOCKBOX'
            return orig_load(vdir, sym, mode, session)

        def boom(*aa, **kk):
            raise AssertionError('k не открывает LOCKBOX')
        FD.load_crypto_15m, L.open_lockbox_session = load, boom
        try:
            k1 = FIN.compute_k(self.vd, E2E.SYMS, reps=16)
            k2 = FIN.compute_k(self.vd, E2E.SYMS, reps=16)
        finally:
            FD.load_crypto_15m, L.open_lockbox_session = orig_load, orig_open
        self.assertTrue(modes and set(modes) == {'pre'})
        self.assertEqual(k1['k'], k2['k'])                                         # воспроизводимо (seed)
        self.assertEqual(k1['t'], k2['t'])
        self.assertGreaterEqual(k1['k'], 1.0)
        m0 = float(np.median(np.abs(k1['t'])))
        self.assertAlmostEqual(k1['m0'], m0)
        self.assertAlmostEqual(k1['k'], max(1.0, m0 / FIN.NULL_MEDIAN_ABS_T))
        self.assertEqual(k1['reps'], 16)
        self.assertEqual(k1['n_series'], len(E2E.SYMS))
        self.assertTrue(abs(k1['n_lock'] - 59 * 6) <= 2, k1['n_lock'])              # 2025-01-01 … конец кэша (конец февраля): 59 суток × 6 баров

    def test_info_mode_never_opens_lockbox_or_takes_flag(self):
        orig_open, orig_load = L.open_lockbox_session, FD.load_crypto_15m

        def boom(*aa, **kk):
            raise AssertionError('без confirm LOCKBOX не открывается')

        def load(vdir, sym, mode='pre', session=None):
            assert mode == 'pre'
            return orig_load(vdir, sym, mode, session)
        L.open_lockbox_session, FD.load_crypto_15m = boom, load
        try:
            txt = run(FIN.run_final4h(self.params))
        finally:
            L.open_lockbox_session, FD.load_crypto_15m = orig_open, orig_load
        self.assertIn('§2.42', txt)
        self.assertIn('k = ', txt)
        self.assertIn('Флаг финала: свободен', txt)
        self.assertIn('/final_crypto4h confirm', txt)
        self.assertIsNone(RF.kv_get('final_started', self.d))
        self.assertIsNotNone(RF.kv_get('final4h_k', self.d))

    def test_confirm_requires_k_in_spec_and_does_not_take_flag_otherwise(self):
        txt = run(FIN.run_final4h(dict(self.params, confirm=True, spec_path=self.spec_with_k(9.999))))
        self.assertIn('/final_crypto4h отказ', txt)
        self.assertIn('флаг финала НЕ взят', txt)
        self.assertIsNone(RF.kv_get('final_started', self.d))
        placeholder = tempfile.NamedTemporaryFile('w', suffix='.md', delete=False, encoding='utf-8')
        placeholder.write('FINAL4H-K: (не записан)\n')
        placeholder.close()
        txt = run(FIN.run_final4h(dict(self.params, confirm=True, spec_path=placeholder.name)))
        self.assertIn('флаг финала НЕ взят', txt)
        self.assertIsNone(RF.kv_get('final_started', self.d))

    def test_confirm_runs_once_and_shares_flag_with_factory_final(self):
        run(FIN.run_final4h(self.params))
        k = RF.kv_get('final4h_k', self.d)['k']
        params = dict(self.params, confirm=True, spec_path=self.spec_with_k(k))
        txt = run(FIN.run_final4h(params))
        self.assertIn('одиночный финал на LOCKBOX', txt)
        self.assertTrue('ПРОЙДЕНО' in txt or 'НЕ ПРОЙДЕНО' in txt)
        for needle in ('t_adj', 'Шарп', 'просадка', 'издержках ×2', 'месяцы роста', 'месяцы падения', 'лонг-нога', 'по годам', 'по символам'):
            self.assertIn(needle, txt)
        self.assertTrue(RF.kv_get('final_started', self.d))
        self.assertTrue(RF.kv_get('final_done', self.d))
        res = RF.kv_get('final4h_result', self.d)
        self.assertEqual(res['k'], k)
        self.assertGreater(res['eval']['n'], 30)
        # повтор запрещён (в том же и в новом процессе)
        for _ in range(2):
            L._lockbox_session_state['opened'] = False
            txt2 = run(FIN.run_final4h(params))
            self.assertIn('флаг финала уже взят', txt2)
        # общий флаг: /factory_final после финала CRYPTO 4h невозможен
        RF.kv_set('candidates', ['FX|1D|T-MA-10-50'], self.d)
        L._lockbox_session_state['opened'] = False
        txt3 = run(RF.run_final(self.params))
        self.assertIn('финал уже запускался', txt3)

    def test_factory_final_flag_blocks_final4h(self):
        run(FIN.run_final4h(self.params))
        k = RF.kv_get('final4h_k', self.d)['k']
        RF.kv_set('final_started', {'by': 'factory_final'}, self.d)
        txt = run(FIN.run_final4h(dict(self.params, confirm=True, spec_path=self.spec_with_k(k))))
        self.assertIn('флаг финала уже взят', txt)
        self.assertIsNone(RF.kv_get('final4h_result', self.d))

    def test_candidate_is_factory_cell_bitwise_on_train(self):
        series = RF.crypto_series(self.vd, E2E.SYMS, '4h', mode='pre')
        spreads = {}
        cand = RF.run_config(FIN.candidate_spec(), series, '4h', 'CRYPTO', spreads, [])
        specs = {cid: spec for cid, fam, spec in RF.specs_for_cell('CRYPTO', '4h', len(series), ('B-VC',))}
        self.assertIn(FIN.CAND_ID, specs)
        series2 = RF.crypto_series(self.vd, E2E.SYMS, '4h', mode='pre')
        fac = RF.run_config(specs[FIN.CAND_ID], series2, '4h', 'CRYPTO', spreads, [])
        self.assertEqual(cand['trades'], fac['trades'])                           # те же сделки (ключ, вход, выход, время)
        self.assertTrue(np.array_equal(cand['days'], fac['days']) and np.array_equal(cand['r'], fac['r']))
        self.assertEqual(RF.period_stats(cand['days'], cand['r'])['TRAIN'], RF.period_stats(fac['days'], fac['r'])['TRAIN'])
        # detail-прогон (используется финалом) даёт ту же доходность, что обычный
        det = RF.run_config(FIN.candidate_spec(), series, '4h', 'CRYPTO', spreads, [], detail=True)
        self.assertTrue(np.array_equal(det['r'], cand['r']))
        mu = det['multi']
        self.assertTrue(np.allclose(mu['ret_long'] + mu['ret_short'], det['r']))     # лонг- и шорт-нога в сумме дают портфель
        self.assertTrue(np.any(mu['ret_long'] != 0) and np.any(mu['ret_short'] != 0))
        self.assertGreater(len(det['parts']), 0)

    def test_bot_gate_passes_confirm_only_with_word(self):
        jobs = []

        async def fake_job(cmd, task, extra=None, need_universe=True):
            jobs.append((cmd, task, extra))
        old = a.factory_job
        a.factory_job = fake_job
        try:
            run(a.final_crypto4h_gate([]))
            run(a.final_crypto4h_gate(['да']))
            run(a.final_crypto4h_gate(['CONFIRM']))
        finally:
            a.factory_job = old
        self.assertEqual(jobs, [('/final_crypto4h', 'final_crypto4h', {'confirm': False}), ('/final_crypto4h', 'final_crypto4h', {'confirm': False}),
                                ('/final_crypto4h', 'final_crypto4h', {'confirm': True})])
        import research_runner as RR
        self.assertIn('final_crypto4h', RR.TASKS)
        src = open(os.path.join(ROOT, 'async_bot.py'), encoding='utf-8').read()
        self.assertIn("cmd == '/final_crypto4h' or cmd.startswith('/final_crypto4h ')", src)


if __name__ == '__main__':
    unittest.main()
