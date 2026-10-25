"""v104: поправки к §2.41 (BOT_SPEC §2.41.19–21): проверка смещения правил на синтетике (старый B-VC исключается, честные семейства — нет), полнота данных только с 2019-01,
постоянные разрывы (счётчик попыток, сегменты, закрытие позиции до разрыва, нет сигналов через разрыв), чтение списка пар из BOT_SPEC."""
import asyncio
import os
import tempfile
import unittest

import numpy as np

from _common import ROOT
import factory_fixtures as F
import research_fbias as RB
import research_fdata as FD
import research_fengine as E
import research_factory as RF
import research_ma_lib as L
from test_v100_ma2 import fake_duka


def run(coro):
    return asyncio.run(coro)


def series_set(n_inst=6, step=4 * 3_600_000, start=(2019, 1, 1), end=(2025, 1, 1), vol=0.004, seed=1):
    t0 = F.ms(*start)
    t1 = F.ms(*end)
    ts = np.arange(t0, t1, step)
    out = []
    for i in range(n_inst):
        o, h, l, c = F.gbm(len(ts), np.random.default_rng(seed + i), vol)
        out.append(E.make_series(f'I{i}', {'ts': ts, 'o': o, 'h': h, 'l': l, 'c': c, 'day': ts // 86_400_000}, L.period_bounds_ms('TRAIN')[0]))
    return out


def legacy_bvc(S, nr=7, hold_after=5):
    """СТАРОЕ правило B-VC (до v104): оба уровня на баре t+1 — сделка не открывается (отбор по исходу бара)."""
    o, h, l = S['o'], S['h'], S['l']
    n = S['n']
    rng = h - l
    sig = np.zeros(n, np.int8)
    epx = np.full(n, np.nan)
    hold = np.zeros(n, np.int32)
    from numpy.lib.stride_tricks import sliding_window_view as swv
    pm = np.full(n, np.nan)
    w = swv(rng, nr - 1).min(axis=1)
    pm[nr - 1:] = w[:n - nr + 1]
    nr7 = np.zeros(n, bool)
    nr7[nr - 1:] = rng[nr - 1:] <= pm[nr - 1:]
    t = np.arange(n - 1)
    lh = h[t + 1] >= h[t]
    sh = l[t + 1] <= l[t]
    valid = nr7[:-1] & (lh != sh)
    sig[:-1][valid & lh] = 1
    sig[:-1][valid & sh] = -1
    epx[:-1] = np.where(lh, np.maximum(o[1:], h[:-1]), np.minimum(o[1:], l[:-1]))
    hold[sig != 0] = hold_after + 1
    return {'sig': sig, 'entry_px': epx, 'hold': hold, 'reverse': False}


class BiasCheck(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.series = series_set()
        cls.spreads = {f'I{i}': 1.0 for i in range(6)}
        cfgs = [c for c in L.library() if c.family in ('T-MA', 'B-VC')]
        cls.specs = [(c.id, c.family, ('lib', c)) for c in cfgs]
        cls.family_of = {c.id: c.family for c in cfgs}

    def run_bias(self, reps=10):
        return RB.bias_cell(0, self.series, self.specs, lambda spec, syn: RF.run_config(spec, syn, '4h', 'FX', self.spreads, [], 0.0),
                            lambda days, r: RF.period_stats(days, r)['TRAIN']['t'], RF.FD_trade_from(), 20261025, reps=reps)

    def test_old_bvc_rule_is_excluded_honest_families_are_not(self):
        orig = E.library_signals

        def patched(cfg, S):
            return legacy_bvc(S, dict(cfg.params)['nr'], dict(cfg.params)['hold']) if cfg.family == 'B-VC' else orig(cfg, S)
        E.library_signals = patched
        try:
            res = self.run_bias()
        finally:
            E.library_signals = orig
        vd = RB.verdict({0: res}, self.family_of)
        self.assertIn('B-VC', vd['excluded'], vd['families'])                    # заведомо смещённое правило исключено
        self.assertNotIn('T-MA', vd['excluded'], vd['families'])
        self.assertTrue(vd['families']['B-VC']['median_abs'] > RB.MEDIAN_ABS_MAX or vd['families']['B-VC']['share3'] >= RB.TAIL_SHARE)

    def test_new_bvc_has_no_positive_outcome_selection(self):
        res = self.run_bias()
        ts = np.array(res['B-VC-NR7'])
        self.assertTrue(len(ts) >= 8)
        self.assertLess(float(np.mean(ts > 3.0)), 0.5)                            # положительного смещения (ложного t ≫ 3) у нового правила нет

    def test_verdict_pooled_rules(self):
        fam = {'a': 'T-MA', 'b': 'T-MA', 'c': 'B-VC'}
        rng = np.random.default_rng(1)
        ok = list(rng.normal(0, 1, 50))
        # объединённая медиана |t| ≤ 1 и доля |t|>3 < 10% — семейство в норме, даже если одна единица плохая (10 нормальных единиц + 1 плохая: 50/550 = 9.1%)
        fam10 = {f'u{k}': 'T-MA' for k in range(10)}
        fam10['bad'] = 'T-MA'
        cells = {0: {f'u{k}': ok for k in range(10)}, 1: {'bad': [4.0] * 50}}
        vd = RB.verdict(cells, fam10)
        self.assertEqual(vd['excluded'], [])                                      # плохая единица — только справочно
        self.assertTrue(any(u['id'] == 'bad' for u in vd['units']))
        # доля |t| > 3 ≥ 10% по объединённой выборке — смещено
        vd2 = RB.verdict({0: {'c': [0.1] * 45 + [3.5] * 5}}, fam)
        self.assertEqual(vd2['excluded'], ['B-VC'])
        self.assertIn('доля', vd2['families']['B-VC']['reasons'])
        # медиана |t| > 1 — смещено
        vd3 = RB.verdict({0: {'a': [1.5] * 30 + [0.2] * 20}}, fam)
        self.assertEqual(vd3['excluded'], ['T-MA'])
        self.assertIn('медианный', vd3['families']['T-MA']['reasons'])

    def test_synth_series_same_length_calendar_and_sigma(self):
        S = self.series[0]
        st = RB.real_stats(S)
        syn = RB.synth_like(S, np.random.default_rng(5), st, RF.FD_trade_from())
        self.assertEqual(syn['n'], S['n'])
        self.assertTrue(np.array_equal(syn['ts'], S['ts']) and np.array_equal(syn['day'], S['day']))
        lr = np.diff(np.log(syn['c']))
        self.assertAlmostEqual(float(lr.std()), st[0], delta=st[0] * 0.05)         # σ той же
        self.assertTrue(np.all(syn['h'] >= np.maximum(syn['o'], syn['c'])) and np.all(syn['l'] <= np.minimum(syn['o'], syn['c'])))
        # повторы воспроизводимы (seed), разные повторы различаются
        a = RB.synth_like(S, np.random.default_rng([20261025, 0, 1, 0]), st, RF.FD_trade_from())
        b = RB.synth_like(S, np.random.default_rng([20261025, 0, 1, 0]), st, RF.FD_trade_from())
        c2 = RB.synth_like(S, np.random.default_rng([20261025, 0, 2, 0]), st, RF.FD_trade_from())
        self.assertTrue(np.array_equal(a['c'], b['c']))
        self.assertFalse(np.array_equal(a['c'], c2['c']))

    def test_excluded_family_removed_from_n(self):
        n = {'CRYPTO': 16, 'COMMODITIES': 4, 'FX': 7, 'INDICES': 5, 'BONDS': 2, 'MULTI': 18}
        full = RF.count_n(n)
        self.assertEqual(full, 204)
        self.assertEqual(RF.count_n(n, ('B-VC',)), 204 - 12)                       # B-VC — по одному тесту в каждой из 12 ячеек
        self.assertEqual(RF.count_n(n, ('P-PAIR', 'L-LAG')), 204 - 10)
        self.assertEqual(RF.count_n(n, ('X-MOM',)), 204 - 14)


class Completeness(unittest.TestCase):
    def setUp(self):
        self.old = dict(FD.INSTRUMENTS)
        FD.INSTRUMENTS.clear()
        FD.INSTRUMENTS['EURUSD'] = ('FX', 'EURUSD', (0.5, 2.0), (2012, 1))         # по умолчанию история с 2012
        self.d = tempfile.mkdtemp()

    def tearDown(self):
        FD.INSTRUMENTS.clear()
        FD.INSTRUMENTS.update(self.old)

    async def _sleep(self, t):
        return None

    def test_coverage_ignores_months_before_2019(self):
        F.write_instrument(self.d, 'EURUSD', first=(2019, 1), last=(2019, 6), seed=2, p0=1.1)
        now = F.ms(2019, 7, 10)
        cov = FD.coverage(self.d, 'EURUSD', now)
        self.assertEqual((cov['wanted'], cov['missing'], cov['complete']), (6, [], True))
        ok, probs, avail, excl = RF.availability(self.d, now)
        self.assertEqual(avail, ['EURUSD'])
        self.assertEqual(probs, [])

    def test_load_requests_only_from_2019(self):
        calls = []
        fetch = fake_duka({'EURUSD'}, 1.10, 1e5, calls=calls)
        run(FD.load_instrument(fetch, 'EURUSD', self.d, F.ms(2019, 4, 10), pause=0, delays=(), sleep=self._sleep))
        years = {int(u.split('/datafeed/')[1].split('/')[1]) for u in calls}
        self.assertEqual(years, {2019})
        self.assertEqual(len(calls), 3)                                             # 2019-01 … 2019-03

    def test_three_failed_runs_make_permanent_gap_and_stop_requests(self):
        FD.INSTRUMENTS['EURUSD'] = ('FX', 'EURUSD', (0.5, 2.0), (2019, 1))
        calls = []

        async def fetch(url, params=None, method='GET'):
            calls.append(url)
            return {'status': 503, 'headers': {}, 'body': b'', 'error': ''}
        now = F.ms(2019, 2, 10)                                                     # последний полный месяц — 2019-01
        for k in range(1, 4):
            st = run(FD.load_instrument(fetch, 'EURUSD', self.d, now, pause=0, delays=(), sleep=self._sleep))
            idx = FD.load_index(self.d, 'EURUSD')
            self.assertEqual(idx['attempts']['2019-01'], k)
            self.assertEqual(idx['gaps'], ['2019-01'] if k == 3 else [])
            cov = FD.coverage(self.d, 'EURUSD', now)
            self.assertEqual(cov['missing'], [] if k == 3 else ['2019-01'])
        self.assertEqual(st['new_gaps'], ['2019-01'])
        n = len(calls)
        run(FD.load_instrument(fetch, 'EURUSD', self.d, now, pause=0, delays=(), sleep=self._sleep))
        self.assertEqual(len(calls), n)                                              # постоянный разрыв больше не запрашивается

    def test_gap_tolerance_two_months(self):
        FD.INSTRUMENTS['EURUSD'] = ('FX', 'EURUSD', (0.5, 2.0), (2019, 1))
        F.write_instrument(self.d, 'EURUSD', first=(2019, 1), last=(2019, 8), seed=2, p0=1.1)
        now = F.ms(2019, 9, 10)

        def make_gaps(tags):
            for t in tags:
                os.remove(FD.month_path(self.d, 'EURUSD', int(t[:4]), int(t[5:])))
            idx = FD.load_index(self.d, 'EURUSD')
            idx['gaps'] = list(tags)
            FD.save_index(self.d, 'EURUSD', idx)
        make_gaps(['2019-03', '2019-05'])
        ok, probs, avail, excl = RF.availability(self.d, now)
        self.assertEqual((avail, probs), (['EURUSD'], []))                           # 2 разрыва — допустимо
        self.assertEqual(RF.gaps_of(self.d, now, avail), {'EURUSD': ['2019-03', '2019-05']})
        os.remove(FD.month_path(self.d, 'EURUSD', 2019, 6))
        idx = FD.load_index(self.d, 'EURUSD'); idx['gaps'].append('2019-06'); FD.save_index(self.d, 'EURUSD', idx)
        ok, probs, avail, excl = RF.availability(self.d, now)
        self.assertEqual(avail, [])
        self.assertIn('EURUSD', excl)
        self.assertIn('3 постоянных разрывов', excl['EURUSD'])

    def test_gaps_in_lockbox_period_listed(self):
        idx = FD.load_index(self.d, 'EURUSD')
        idx['gaps'] = ['2024-11', '2025-02']
        FD.save_index(self.d, 'EURUSD', idx)
        self.assertEqual(FD.gaps_in_period(self.d, 'EURUSD', L.lockbox_start_ms()), ['2025-02'])


class GapSegments(unittest.TestCase):
    def make(self, n1=400, n2=400, slope=0.05, gap_hours=24 * 31):
        """Растущий ряд (сильный тренд) с разрывом ≈ месяц между двумя кусками."""
        t0 = F.ms(2020, 1, 1)
        ts1 = t0 + np.arange(n1) * 3_600_000
        ts2 = ts1[-1] + gap_hours * 3_600_000 + np.arange(1, n2 + 1) * 3_600_000
        ts = np.concatenate([ts1, ts2])
        n = len(ts)
        c = 100.0 + slope * np.arange(n) + 0.3 * np.sin(np.arange(n) / 7.0)
        o = np.r_[c[0] - slope, c[:-1]]
        h, l = np.maximum(o, c) + 0.2, np.minimum(o, c) - 0.2
        return {'ts': ts, 'o': o, 'h': h, 'l': l, 'c': c, 'day': ts // 86_400_000}, n1

    def test_gap_breaks_and_segments(self):
        d = tempfile.mkdtemp()
        ts = np.concatenate([F.ms(2020, 1, 1) + np.arange(700) * 3_600_000, F.ms(2020, 3, 1) + np.arange(700) * 3_600_000])      # весь февраль 2020 отсутствует
        o, h, l, c = F.gbm(len(ts), np.random.default_rng(2), 0.003)
        bars = {'ts': ts, 'o': o, 'h': h, 'l': l, 'c': c, 'day': ts // 86_400_000}
        idx = FD.load_index(d, 'EURUSD')
        idx['gaps'] = ['2020-02']
        FD.save_index(d, 'EURUSD', idx)
        S = E.make_series('EURUSD', bars, int(ts[0]))
        br = FD.gap_breaks(d, 'EURUSD', S['ts'])
        self.assertEqual(br, [700])
        segs = E.split_series(S, br, int(ts[0]))
        self.assertEqual([s_['key'] for s_ in segs], ['EURUSD', 'EURUSD#1'])
        self.assertEqual([s_['n'] for s_ in segs], [700, 700])
        self.assertEqual(E.base_key('EURUSD#1'), 'EURUSD')
        self.assertAlmostEqual(E.cost_noncrypto('EURUSD#1', {'EURUSD': 0.26}).side, 0.26e-4)    # издержки — по базовому ключу
        self.assertEqual(len(E.split_series(S, [], int(ts[0]))), 1)                         # без разрывов ряд не режется
        # разрыв, у которого есть бары внутри месяца (месяц не пуст), и разрыв на краю ряда разрезов не дают
        idx['gaps'] = ['2020-01']
        FD.save_index(d, 'EURUSD', idx)
        self.assertEqual(FD.gap_breaks(d, 'EURUSD', S['ts']), [])

    def test_position_closed_before_gap_and_no_signal_through_gap(self):
        bars, n1 = self.make()
        S = E.make_series('X', bars, int(bars['ts'][0]))
        segs = E.split_series(S, [n1], int(bars['ts'][0]))
        cfg = [c for c in L.library() if c.id == 'T-MA-10-50'][0]
        trades = []
        for g in segs:
            sg = E.library_signals(cfg, g)
            r = E.simulate(g, sg['sig'], E.Cost(0.0), reverse=sg['reverse'])
            trades.append(r['trades'])
        t1, t2 = trades
        self.assertTrue(t1 and t2)
        # у первого куска позиция (сильный тренд вверх) дожила до разрыва → закрыта по close последнего бара куска
        self.assertEqual(t1[-1][4], 'end')
        self.assertEqual(t1[-1][1], n1 - 1)
        # индикаторы второго куска пересчитываются с прогревом: до 50-го бара куска сигналов нет
        sg2 = E.library_signals(cfg, segs[1])
        self.assertTrue(np.all(sg2['sig'][:49] == 0))
        self.assertGreaterEqual(t2[0][0], 50)
        # на полном ряде SMA «через разрыв» была бы уже определена на барах сразу после разрыва — в сегменте её нет
        full_sma = E.sma(S['c'], 50)
        self.assertFalse(np.isnan(full_sma[n1 + 5]))
        self.assertTrue(np.isnan(E.sma(segs[1]['c'], 50)[5]))

    def test_xmom_handles_segments_as_separate_instruments(self):
        series = []
        for i in range(6):
            bars, n1 = self.make(slope=0.01 * (i + 1))
            S = E.make_series(f'I{i}', bars, int(bars['ts'][0]))
            series += E.split_series(S, [n1], int(bars['ts'][0])) if i == 2 else [S]
        keys = [S['key'] for S in series]
        self.assertEqual(len(set(keys)), len(keys))
        self.assertIn('I2#1', keys)
        out = E.xmom_signals(series, '1h', 21, min_valid=5)
        self.assertEqual(set(out), set(keys))
        # на каждом узле сетки инструмент представлен не более чем одним валидным сегментом
        n_sig = sum(int(np.sum(o['sig'] != 0)) for o in out.values())
        self.assertGreater(n_sig, 0)


class SpecPairs(unittest.TestCase):
    def test_parse(self):
        f = tempfile.NamedTemporaryFile('w', suffix='.md', delete=False, encoding='utf-8')
        f.write('x\nPAIRS-FROZEN: AAAUSDT/BBBUSDT, CCCUSDT/DDDUSDT\ny\n')
        f.close()
        self.assertEqual(RF.read_spec_pairs(f.name), ['AAAUSDT/BBBUSDT', 'CCCUSDT/DDDUSDT'])
        g = tempfile.NamedTemporaryFile('w', suffix='.md', delete=False, encoding='utf-8')
        g.write('PAIRS-FROZEN: (не записан — ожидается)\n')
        g.close()
        self.assertIsNone(RF.read_spec_pairs(g.name))
        self.assertIsNone(RF.read_spec_pairs('/nonexistent/file.md'))

    def test_repo_spec_has_marker_and_amendments(self):
        spec = open(os.path.join(ROOT, 'BOT_SPEC.md'), encoding='utf-8').read()
        self.assertTrue(any(ln.startswith('PAIRS-FROZEN:') for ln in spec.splitlines()))
        for h in ('#### 2.41.19.', '#### 2.41.20.', '#### 2.41.21.'):
            self.assertIn(h, spec)
        self.assertIn('10.99', spec)


if __name__ == '__main__':
    unittest.main()
