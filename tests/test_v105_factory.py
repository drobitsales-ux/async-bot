"""v105: поправка §2.41.23 — декомпозиция доходности (/factory_decomp), перенос 0.025, NW лаг 20 для 1D, перезапуск ТОЛЬКО не-крипто ячеек без LOCKBOX, сценарий финала и ставка ФРС."""
import os
import unittest

import numpy as np

import _common
ROOT = _common.ROOT                                                        # подключает корень репозитория в sys.path
import factory_fixtures as F
import research_fdata as FD
import research_fengine as E
import research_factory as RF
import research_ma_lib as L
import test_v103_e2e as E2E


def daily_series(n, rng, vol, key='X', start=(2019, 1, 1)):
    t0 = F.ms(*start)
    ts = t0 + np.arange(n) * 86_400_000
    o, h, l, c = F.gbm(n, rng, vol)
    return E.make_series(key, {'ts': ts, 'o': o, 'h': h, 'l': l, 'c': c, 'day': ts // 86_400_000}, L.period_bounds_ms('TRAIN')[0])


class SwapLeverage(unittest.TestCase):
    def build(self):
        """Плечо ровно 3: w = RISK/(3·ATR/px) при ATR = 0.0556 на цене 100; 1D-бары, позиция 11 баров."""
        n = 80
        t0 = F.ms(2020, 3, 1)
        ts = t0 + np.arange(n) * 86_400_000
        c = np.full(n, 100.0)
        h, l = c + 0.0278, c - 0.0278                                           # TR = 0.0556 → ATR20 = 0.0556
        S = E.make_series('X', {'ts': ts, 'o': c.copy(), 'h': h, 'l': l, 'c': c, 'day': ts // 86_400_000}, t0)
        sig = np.zeros(n, np.int8); sig[40] = 1
        hold = np.zeros(n, np.int32); hold[40] = 11                              # вход open[41], выход close[51]: 11 баров, 10 суток переноса
        return S, sig, hold

    def test_swap_0025_at_leverage_3(self):
        S, sig, hold = self.build()
        cost = E.cost_noncrypto('X', {'X': 0.0}, swap_yr=0.025)
        r = E.simulate(S, sig, cost, hold=hold, detail=True)
        e, last, d, w, kind = r['trades'][0]
        self.assertAlmostEqual(w, 3.0, places=2)
        expected_day = w * 0.025 / 365                                           # доля капитала за сутки
        self.assertAlmostEqual(expected_day * 1e4, 2.05, places=1)               # ≈ 2.05 б.п./день при плече 3
        self.assertTrue(np.allclose(r['c_swap'][e + 1:last + 1], expected_day))
        self.assertEqual(float(r['c_swap'][:e + 1].sum()), 0.0)
        self.assertTrue(np.allclose(r['expo'][e:last + 1], w))
        self.assertAlmostEqual(r['ret'].sum(), -(last - e) * expected_day, places=10)    # цена не двигалась, спред 0 ⇒ только перенос

    def test_decomp_stats_units_and_identity(self):
        S, sig, hold = self.build()
        cost = E.cost_noncrypto('X', {'X': 0.5}, swap_yr=0.025)
        r = E.simulate(S, sig, cost, hold=hold, detail=True)
        days, multi = E.portfolio_daily_multi([r], ('ret', 'c_side', 'c_swap', 'c_fund', 'expo'))
        st = RF.decomp_stats({'days': days, 'multi': multi}, 'TRAIN')
        n_days = len(S['ts'])
        w = r['trades'][0][3]
        self.assertEqual(st['n'], n_days)
        self.assertAlmostEqual(st['swap'], 10 * w * 0.025 / 365 / n_days * 1e4, places=6)    # б.п./день капитала (среднее по дням, дни без позиции — нули)
        self.assertAlmostEqual(st['side'], 2 * w * 0.5e-4 / n_days * 1e4, places=6)
        self.assertAlmostEqual(st['gross'], st['net'] + st['side'] + st['swap'], places=9)
        self.assertAlmostEqual(st['lev'], w * 11 / n_days, places=6)                       # плечо по факту: среднее по дням
        self.assertAlmostEqual(st['lev95'], w, places=6)
        self.assertAlmostEqual(st['fund'], 0.0)


class NwLag(unittest.TestCase):
    """Гипотеза владельца (BOT_SPEC §2.41.22а): инфляция t трендовых конфигураций — из-за лага NW 5. Проверка на синтетике с длинными удержаниями."""

    def medians(self, vol, reps=24, n_inst=10):
        cfgs = [c for c in L.library() if c.id in ('T-MA-10-50', 'T-DC-55')]
        t5, t20 = [], []
        n = len(np.arange(F.ms(2019, 1, 1), F.ms(2025, 1, 1), 86_400_000))
        for rep in range(reps):
            ser = [daily_series(n, np.random.default_rng(1000 * rep + i), vol, f'S{i}') for i in range(n_inst)]
            for c in cfgs:
                out = RF.run_config(('lib', c), ser, '1D', 'CRYPTO', {}, [], 0.0)
                _d, x = E.period_slice(out['days'], out['r'], 'TRAIN')
                t5.append(abs(E.nw_t(x, 5)[1]))
                t20.append(abs(E.nw_t(x, 20)[1]))
        return float(np.median(t5)), float(np.median(t20))

    def test_lag20_median_abs_t_at_nonconcrypto_volatility(self):
        m5, m20 = self.medians(0.01)                                              # σ дневной 1% — порядок индексов/металлов/нефти
        self.assertLessEqual(m20, 1.0, (m5, m20))
        self.assertLessEqual(m5, 1.0, (m5, m20))

    def test_inflation_at_crypto_volatility_is_not_a_lag_effect(self):
        m5, m20 = self.medians(0.06)                                              # σ дневной 6% — как у крипты: медиана |t| заметно выше идеальной 0.67 при ЛЮБОМ лаге
        self.assertGreater(m5, 0.8, (m5, m20))
        self.assertGreater(m20, 0.8, (m5, m20))
        self.assertLess(abs(m20 - m5), 0.2, (m5, m20))                            # лаг 20 почти ничего не меняет ⇒ причина не в лаге

    def test_t_select_takes_smaller_t_and_stats_keys(self):
        rng = np.random.default_rng(3)
        x = np.convolve(rng.normal(0.0004, 0.01, 1520), np.ones(8) / 8, mode='valid')
        m, t_sel, ts, n = RF.t_select(x, (5, 20))
        self.assertEqual(t_sel, min(ts.values()))
        days = np.arange(18_262, 18_262 + len(x))                                 # с 2020-01-01
        st = RF.period_stats(days, x, (5, 20))
        self.assertIn('tl', st['TRAIN'])
        self.assertEqual(st['TRAIN']['t'], min(v for v in st['TRAIN']['tl'].values()))
        self.assertNotIn('tl', RF.period_stats(days, x)['TRAIN'])                 # 4h/1h и /factory — только лаг 5
        self.assertEqual(RF.nc_lags('1D'), (5, 20))
        self.assertEqual(RF.nc_lags('4h'), (5,))
        self.assertEqual(RF.nc_lags('1h'), (5,))


class NcFlow(E2E.Base):
    def setUp(self):
        self.reset(keep=('spread:XAGUSD',))
        RF.kv_set('excluded_families', ['B-VC'], self.d)
        self.params = dict(self.__class__.params, version='2026-11-04-v105')

    def guard_no_crypto_no_lockbox(self):
        """Любое обращение к крипто-данным, сессии LOCKBOX или полному ряду — ошибка."""
        self._orig = (RF.crypto_series, FD.load_crypto_15m, L.open_lockbox_session, FD.load_raw)

        def boom(*a, **k):
            raise AssertionError('обращение к крипто/LOCKBOX запрещено')
        orig_raw = FD.load_raw

        def raw(d, key, mode='pre', session=None):
            assert mode == 'pre' and session is None, 'LOCKBOX недоступен в /factory_nc и /factory_decomp'
            return orig_raw(d, key, mode, session)
        RF.crypto_series, FD.load_crypto_15m, L.open_lockbox_session, FD.load_raw = boom, boom, boom, raw

    def unguard(self):
        RF.crypto_series, FD.load_crypto_15m, L.open_lockbox_session, FD.load_raw = self._orig

    def test_nc_restarts_only_noncrypto_and_never_touches_lockbox_or_crypto(self):
        con = RF._con(self.d)
        before = {k for (k,) in con.execute("SELECT k FROM kv WHERE k LIKE 'cell:%' OR k LIKE 'bias:%'")}
        con.close()
        self.guard_no_crypto_no_lockbox()
        try:
            txt = E2E.run(RF.run_nc(self.params))
        finally:
            self.unguard()
        self.assertIn('/factory_nc', txt)
        self.assertIn('2026-11-04-v105', txt)                                     # заголовок показывает версию кода, не REV кэша
        cells = RF.nc_cells()
        self.assertEqual([FD.CELLS[i][0] for i in cells], ['COMMODITIES', 'FX', 'FX', 'MULTI'])
        con = RF._con(self.d)
        keys = {k for (k,) in con.execute("SELECT k FROM kv")}
        con.close()
        nc_keys = {k for k in keys if k.startswith('nc:')}
        self.assertEqual(len(nc_keys), len(cells))                                # по ключу на каждую не-крипто ячейку; крипто-ячейки не затронуты
        self.assertEqual({k for k in keys if k.startswith('cell:') or k.startswith('bias:')}, before)
        self.assertIsNone(RF.kv_get('final_started', self.d))                     # флаг финала не взят
        self.assertIsNone(RF.kv_get('final_done', self.d))
        sc = RF.kv_get('candidates_scenario', self.d)
        self.assertEqual((sc['swap_yr'], sc['source']), (0.025, 'nc'))
        self.assertIsNotNone(RF.kv_get('candidates', self.d))
        # сохранённый результат ячейки: 1D — t по двум лагам, 4h — только лаг 5; диагностика «перенос 0» хранится отдельно
        saved = RF.kv_get(f'nc:{RF.NC_REV}:{cells[0]}:B-VC', self.d)
        rec = next(iter(saved['real'].values()))
        self.assertIn('tl', rec['TRAIN'])
        self.assertEqual(set(saved['real']), set(saved['diag']))
        saved4 = RF.kv_get(f'nc:{RF.NC_REV}:{cells[2]}:B-VC', self.d)
        self.assertNotIn('tl', next(iter(saved4['real'].values()))['TRAIN'])
        # отчёт: честная пометка о пост-фактум, калибровка, таблица с диагностикой
        for needle in ('ПОСЛЕ просмотра', 'КАЛИБРОВКА', 'ТАБЛИЦА ВСЕХ N_nc ТЕСТОВ', 'перенос 0', 'N_nc ='):
            self.assertIn(needle, txt)
        n_nc = len([t for t in RF.enumerate_tests({'CRYPTO': 6, 'COMMODITIES': 2, 'FX': 6, 'INDICES': 0, 'BONDS': 0, 'MULTI': 8}, ('B-VC',)) if t[0] in cells])
        self.assertIn(f'N_nc = {n_nc}', txt)

    def test_nc_refuses_without_bias_check(self):
        con = RF._con(self.d)
        con.execute("DELETE FROM kv WHERE k='excluded_families'")
        con.commit(); con.close()
        txt = E2E.run(RF.run_nc(self.params))
        self.assertIn('сначала /factory', txt)

    def test_nc_calibration_failure_stops(self):
        orig = E.nw_t
        E.nw_t = lambda x, lag=5: (0.001, 6.0, len(x))
        try:
            txt = E2E.run(RF.run_nc(self.params))
        finally:
            E.nw_t = orig
        self.assertIn('ПРОЦЕДУРА ОТБОРА НЕНАДЁЖНА', txt)
        self.assertNotIn('ТАБЛИЦА ВСЕХ N_nc ТЕСТОВ', txt)

    def test_decomp_read_only_matches_regular_run(self):
        self.ready()
        self.guard_no_crypto_no_lockbox_except_crypto()
        try:
            txt = E2E.run(RF.run_decomp(self.params))
        finally:
            self.unguard()
        self.assertIn('/factory_decomp', txt)
        self.assertIn('решений по таблице не принимается', txt)
        self.assertIn('LOCKBOX и флаг финала не использовались', txt)
        self.assertIsNone(RF.kv_get('final_started', self.d))
        main = txt.split('Сводка по ячейкам')[0]
        rows = [ln for ln in main.splitlines() if ' | TRAIN | ' in ln or ' | VALID | ' in ln]
        tests = RF.enumerate_tests({'CRYPTO': 6, 'COMMODITIES': 2, 'FX': 6, 'INDICES': 0, 'BONDS': 0, 'MULTI': 8}, ('B-VC',))
        self.assertEqual(len(rows), 2 * len(tests))
        self.assertIn('Сводка по ячейкам', txt)
        # согласованность с обычным прогоном: net декомпозиции = mean TRAIN обычного прогона (б.п./день)
        ctx = RF.build_ctx(self.params)
        probs, spreads, pairs, excl = RF.preflight(ctx)
        series = RF.cell_series('FX', '1D', ctx)
        cfg = [c for c in L.library() if c.id == 'T-MA-10-50'][0]
        out = RF.run_config(('lib', cfg), series, '1D', 'FX', spreads, pairs)
        reg = RF.period_stats(out['days'], out['r'])['TRAIN']['mean'] * 1e4
        det = RF.run_config(('lib', cfg), series, '1D', 'FX', spreads, pairs, detail=True)
        self.assertAlmostEqual(RF.decomp_stats(det, 'TRAIN')['net'], reg, places=9)
        line = [ln for ln in txt.splitlines() if ln.startswith('FX 1D | T-MA-10-50 | TRAIN | ')][0]
        self.assertAlmostEqual(float(line.split(' | ')[3]), reg, places=2)

    def guard_no_crypto_no_lockbox_except_crypto(self):
        self._orig = (RF.crypto_series, FD.load_crypto_15m, L.open_lockbox_session, FD.load_raw)
        orig_raw = FD.load_raw

        def boom(*a, **k):
            raise AssertionError('LOCKBOX запрещён')

        def raw(d, key, mode='pre', session=None):
            assert mode == 'pre' and session is None
            return orig_raw(d, key, mode, session)
        L.open_lockbox_session, FD.load_raw = boom, raw

    def test_final_uses_nc_scenario_stress_and_fed_note(self):
        RF.kv_set('candidates', ['FX|1D|T-MA-10-50'], self.d)
        RF.kv_set('candidates_scenario', {'swap_yr': 0.025, 'source': 'nc'}, self.d)
        calls = []
        orig = RF.run_config

        def spy(spec, series, tf, cls, spreads, pairs, mult=1.0, swap_yr=None, detail=False):
            calls.append((mult, swap_yr))
            return orig(spec, series, tf, cls, spreads, pairs, mult, swap_yr, detail)
        RF.run_config = spy
        old_env = os.environ.pop('FED_RATE_BY_YEAR', None)
        try:
            txt = E2E.run(RF.run_final(self.params))
        finally:
            RF.run_config = orig
            if old_env is not None:
                os.environ['FED_RATE_BY_YEAR'] = old_env
        self.assertEqual(calls, [(1.0, 0.025), (2.0, 0.025)])                     # базовый прогон и стресс ×2 (перенос → 5% годовых)
        self.assertIn('0.025', txt)
        self.assertIn('не выполнена: ставка ФРС не задана', txt)                  # ставок в коде нет
        self.assertIn('лаг 20', txt)
        self.assertTrue(RF.kv_get('final_done', self.d))

    def test_final_fed_check_with_env_table(self):
        RF.kv_set('candidates', ['FX|1D|T-MA-10-50'], self.d)
        RF.kv_set('candidates_scenario', {'swap_yr': 0.025, 'source': 'nc'}, self.d)
        calls = []
        orig = RF.run_config

        def spy(spec, series, tf, cls, spreads, pairs, mult=1.0, swap_yr=None, detail=False):
            calls.append((mult, swap_yr))
            return orig(spec, series, tf, cls, spreads, pairs, mult, swap_yr, detail)
        RF.run_config = spy
        os.environ['FED_RATE_BY_YEAR'] = '2025:0.04'
        try:
            txt = E2E.run(RF.run_final(self.params))
        finally:
            RF.run_config = orig
            os.environ.pop('FED_RATE_BY_YEAR')
        self.assertEqual(len(calls), 3)
        self.assertAlmostEqual(calls[2][1], 0.065)                                # 0.025 + средняя ставка ФРС
        self.assertEqual(calls[2][0], 1.0)
        self.assertIn('0.025 + ФРС', txt)
        self.assertIn('средняя ставка за период 4.00%', txt)

    def test_fed_rate_avg_rules(self):
        days = np.arange(20_089, 20_089 + 40)                                      # 2025-01-01 …
        old = os.environ.pop('FED_RATE_BY_YEAR', None)
        try:
            self.assertEqual(RF.fed_rate_avg(days)[0], None)
            os.environ['FED_RATE_BY_YEAR'] = '2024:0.05'
            r, note = RF.fed_rate_avg(days)
            self.assertIsNone(r)
            self.assertIn('нет годов', note)
            os.environ['FED_RATE_BY_YEAR'] = 'мусор'
            self.assertIsNone(RF.fed_rate_avg(days)[0])
            os.environ['FED_RATE_BY_YEAR'] = '2025:0.04,2026:0.03'
            self.assertAlmostEqual(RF.fed_rate_avg(days)[0], 0.04)
        finally:
            os.environ.pop('FED_RATE_BY_YEAR', None)
            if old is not None:
                os.environ['FED_RATE_BY_YEAR'] = old

    def test_union_bh_note(self):
        tests = RF.enumerate_tests({'CRYPTO': 6, 'COMMODITIES': 2, 'FX': 6, 'INDICES': 0, 'BONDS': 0, 'MULTI': 8}, ('B-VC',))
        tests = [t for t in tests if t[1] == 'FX' and t[2] == '1D'][:3]
        ci = tests[0][0]
        first = {RF.tid(t[1], t[2], t[3]): {'TRAIN': {'t': 3.5, 'mean': 0.001, 'n': 1000}} for t in tests}
        RF.kv_set(f'cell:{RF.REV}:{ci}:B-VC', {'real': first, 'calib': {}}, self.d)
        new = {RF.tid(t[1], t[2], t[3]): {'TRAIN': {'t': 3.4, 'mean': 0.001, 'n': 1000}} for t in tests}
        note = RF.union_bh_note(tests, new, self.d, 'B-VC')
        self.assertIn('по объединению двух прогонов (6 p-значений)', note)
        self.assertIn('прошло 3 из 3', note)


if __name__ == '__main__':
    unittest.main()
