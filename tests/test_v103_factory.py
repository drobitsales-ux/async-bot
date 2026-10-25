"""v103: «фабрика» этап 2 — данные Dukascopy (нулевой объём, ny17 с переходом на летнее время, загрузка с места остановки), издержки (б) со спредом и переносом, симулятор (стоп с гэпом,
вход по open t+1), P-PAIR (равный номинал, обе ноги), L-LAG (без заглядывания), статистика (Ньюи–Уэст, BH), N = 204, LOCKBOX без флага финала."""
import asyncio
import os
import tempfile
import unittest

import numpy as np

from _common import ROOT
import factory_fixtures as F
import research_fdata as FD
import research_fengine as E
import research_factory as RF
import research_ma_lib as L
import research_vision as V
from test_v100_ma2 import fake_duka


def run(coro):
    return asyncio.run(coro)


def flat_series(n=60, step=3_600_000, t0=1_577_836_800_000, slope=0.1, key='X'):
    """Детерминированный ряд: c растёт на slope, диапазон бара 1.0, ATR20 ≈ 1.0–1.1."""
    ts = t0 + np.arange(n) * step
    c = 100.0 + slope * np.arange(n)
    o = np.r_[c[0] - slope, c[:-1]]
    h, l = np.maximum(o, c) + 0.5, np.minimum(o, c) - 0.5
    return E.make_series(key, {'ts': ts, 'o': o, 'h': h, 'l': l, 'c': c, 'day': ts // 86_400_000}, t0)


class Dst(unittest.TestCase):
    def test_ny17_boundary_winter_and_summer(self):
        w = np.array([F.ms(2024, 3, 8, 21), F.ms(2024, 3, 8, 22)])           # EST: 21:00 UTC = 16:00, 22:00 UTC = 17:00 (новая граница)
        dw, _ = FD.ny17_labels(w)
        self.assertEqual(dw[1] - dw[0], 1)
        s = np.array([F.ms(2024, 3, 12, 20), F.ms(2024, 3, 12, 21)])         # EDT: 20:00 UTC = 16:00, 21:00 UTC = 17:00
        ds, _ = FD.ny17_labels(s)
        self.assertEqual(ds[1] - ds[0], 1)
        # границы совпадают с 22:00/21:00 UTC и соседние часы остаются в одном дне
        w2 = np.array([F.ms(2024, 1, 9, 22), F.ms(2024, 1, 10, 20)])
        d2, _ = FD.ny17_labels(w2)
        self.assertEqual(d2[0], d2[1])

    def test_4h_six_per_day_from_boundary(self):
        base = F.ms(2024, 1, 9, 22)                                           # зима: граница 22:00 UTC
        ts = base + np.arange(24) * 3_600_000
        day, lab4 = FD.ny17_labels(ts)
        self.assertEqual(len(set(day)), 1)
        self.assertEqual(len(set(lab4)), 6)
        self.assertEqual(list(np.bincount(lab4 - lab4.min())), [4] * 6)
        base_s = F.ms(2024, 7, 9, 21)                                         # лето: граница 21:00 UTC
        ts = base_s + np.arange(24) * 3_600_000
        day, lab4 = FD.ny17_labels(ts)
        self.assertEqual(len(set(day)), 1)
        self.assertEqual(len(set(lab4)), 6)

    def test_daily_bars_per_year_with_dst(self):
        d = tempfile.mkdtemp()
        F.write_instrument(d, 'EURUSD', first=(2019, 1), last=(2019, 12), seed=3, p0=1.1)
        raw = FD.load_raw(d, 'EURUSD', 'pre')
        bars = FD.build_noncrypto(raw, '1D')
        self.assertTrue(250 <= len(bars['ts']) <= 262, len(bars['ts']))
        self.assertTrue(np.all(np.diff(bars['day']) >= 1))


class ZeroVolume(unittest.TestCase):
    def test_only_zero_volume_bars_dropped(self):
        n = 6
        p = {'ts': np.arange(n) * 3_600_000, 'o': np.array([1, 2, 2, 3, 3, 4.0]), 'h': np.array([1, 2.5, 2, 3, 3, 4.5]), 'l': np.array([1, 1.5, 2, 3, 3, 3.5]),
             'c': np.array([1, 2, 2, 3, 3, 4.0]), 'v': np.array([0, 5, 0, 7, 0.0, 9])}
        q = FD.exclude_zero_volume(p)
        self.assertEqual(list(q['v']), [5, 7, 9])
        # плоский бар с объёмом > 0 остаётся (в отличие от прежнего правила серий)
        p2 = dict(p, v=np.array([0, 5, 3, 7, 0.0, 9]))
        self.assertEqual(len(FD.exclude_zero_volume(p2)['v']), 4)

    def test_build_noncrypto_drops_weekend_fillers(self):
        d = tempfile.mkdtemp()
        F.write_instrument(d, 'EURUSD', first=(2019, 1), last=(2019, 3), seed=4, p0=1.1)
        raw = FD.load_raw(d, 'EURUSD', 'pre')
        self.assertTrue(np.any(raw['v'] == 0))
        bars = FD.build_noncrypto(raw, '1h')
        self.assertEqual(len(bars['ts']), int(np.sum(raw['v'] > 0)) - 1)       # минус последний бар ряда (неполная группа)


class Costs(unittest.TestCase):
    def test_noncrypto_side_is_one_spread_and_swap_per_day(self):
        c = E.cost_noncrypto('EURUSD', {'EURUSD': 0.26})
        self.assertAlmostEqual(c.side, 0.26e-4)
        self.assertAlmostEqual(c.swap_day, 0.06 / 365)
        c2 = E.cost_noncrypto('EURUSD', {'EURUSD': 0.26}, mult=2.0)
        self.assertAlmostEqual(c2.side, 0.52e-4)
        self.assertAlmostEqual(c2.swap_day, 2 * 0.06 / 365)

    def test_fin_rate_env(self):
        old = os.environ.get('FIN_RATE_YR')
        os.environ['FIN_RATE_YR'] = '0.10'
        try:
            self.assertAlmostEqual(E.cost_noncrypto('EURUSD', {'EURUSD': 1.0}).swap_day, 0.10 / 365)
        finally:
            if old is None:
                os.environ.pop('FIN_RATE_YR')
            else:
                os.environ['FIN_RATE_YR'] = old

    def test_crypto_cost(self):
        c = E.cost_crypto()
        self.assertAlmostEqual(c.side, E.fee_taker() + 0.0003)
        self.assertEqual(c.swap_day, 0.0)


class Simulator(unittest.TestCase):
    def test_exact_pnl_with_spread_and_swap(self):
        S = flat_series()
        n = S['n']
        sig = np.zeros(n, np.int8); sig[30] = 1
        hold = np.zeros(n, np.int32); hold[30] = 3                             # вход open[31], выход close[33]
        cost = E.Cost(0.001, 0.06 / 365)
        out = E.simulate(S, sig, cost, hold=hold)
        px = S['o'][31]
        sd = 3 * S['atr'][30]
        w = E.RISK / (sd / px)
        gross = E.RISK * (S['c'][33] - px) / sd
        costs = 2 * w * 0.001 + w * (0.06 / 365) * (S['ts'][33] - S['ts'][31]) / 86_400_000
        self.assertEqual(out['trades'][0][:3], (31, 33, 1))
        self.assertAlmostEqual(out['ret'].sum(), gross - costs, places=10)
        self.assertEqual(out['ret'][:31].sum(), 0.0)

    def test_stop_with_gap_exits_at_open(self):
        S = flat_series()
        sd = 3 * S['atr'][30]
        S['o'][33] = S['o'][31] - sd - 2.0                                     # бар 33 открылся ниже стопа
        S['l'][33] = S['o'][33] - 0.5
        S['h'][33] = S['o'][33] + 0.5
        S['c'][33] = S['o'][33]
        sig = np.zeros(S['n'], np.int8); sig[30] = 1
        out = E.simulate(S, sig, E.Cost(0.0))
        e, last, d, w, kind = out['trades'][0]
        self.assertEqual((kind, last), ('stop', 33))
        px = S['o'][31]
        expected = E.RISK / sd * (S['o'][33] - px)                             # выход по open, а не по уровню стопа
        self.assertAlmostEqual(out['ret'].sum(), expected, places=10)
        self.assertLess(expected, -E.RISK)                                     # убыток больше риска — гэп

    def test_entry_is_next_open_and_flat_after_stop(self):
        S = flat_series()
        sig = np.zeros(S['n'], np.int8); sig[30] = 1; sig[31] = 1; sig[32] = 1
        hold = np.zeros(S['n'], np.int32); hold[30:33] = 2
        out = E.simulate(S, sig, E.Cost(0.0), hold=hold)
        self.assertEqual([t[0] for t in out['trades']][:2], [31, 33])           # пока позиция открыта, повторные сигналы игнорируются

    def test_trade_from_blocks_early_entries(self):
        S = flat_series()
        S['trade_from'] = 40
        sig = np.zeros(S['n'], np.int8); sig[30] = 1; sig[45] = 1
        hold = np.zeros(S['n'], np.int32); hold[30] = hold[45] = 2
        out = E.simulate(S, sig, E.Cost(0.0), hold=hold)
        self.assertEqual([t[0] for t in out['trades']], [46])


class Families(unittest.TestCase):
    def test_pplair_equal_notional_and_two_leg_costs(self):
        rng = np.random.default_rng(5)
        n = 400
        ts = 1_577_836_800_000 + np.arange(n) * 3_600_000
        a = F.gbm(n, rng, 0.0002)
        b = F.gbm(n, rng, 0.001)
        A = {'ts': ts, 'o': a[0].copy(), 'h': a[1].copy(), 'l': a[2].copy(), 'c': a[3].copy(), 'day': ts // 86_400_000}
        B = {'ts': ts, 'o': b[0].copy(), 'h': b[1].copy(), 'l': b[2].copy(), 'c': b[3].copy(), 'day': ts // 86_400_000}
        for k in ('o', 'h', 'l', 'c'):                                         # всплеск A на барах 200–204 (z > 2), затем возврат
            A[k][200:205] *= 1.08
        SA, SB = E.make_series('A', A, ts[0]), E.make_series('B', B, ts[0])
        r0 = E.simulate_pair(SA, SB, 20, E.Cost(0.0), E.Cost(0.0), int(ts[0]))
        r1 = E.simulate_pair(SA, SB, 20, E.Cost(0.001), E.Cost(0.001), int(ts[0]))
        self.assertTrue(r0['trades'])
        e, last, zs, w, kind = r0['trades'][0]
        self.assertEqual(kind, 'open')
        x = np.log(A['c'] / B['c'])
        z = (x - E.sma(x, 20)) / E.rolling_std(x, 20)
        self.assertEqual(zs, 1 if z[e - 1] > 0 else -1)                         # z > 0: A «убежала» → шорт A, лонг B; сигнал на баре e−1, вход open e
        self.assertGreater(abs(z[e - 1]), 2.0)
        # издержки: 2 ноги × (вход + выход) × w × side
        diff = r0['ret'].sum() - r1['ret'].sum()
        self.assertAlmostEqual(diff, sum(4 * t[3] * 0.001 for t in r0['trades']), places=10)
        # равный номинал: P&L сделки = w · (−ΔA/A_вход + ΔB/B_вход) с ОДНИМ w на обе ноги
        gross = r0['ret'][e:last + 1].sum()
        expected = w * zs * (-(A['o'][last] - A['o'][e]) / A['o'][e] + (B['o'][last] - B['o'][e]) / B['o'][e])
        self.assertAlmostEqual(gross, expected, places=10)
        self.assertAlmostEqual(w, E.RISK / (2 * E.rolling_std(x, 20)[e - 1]), places=12)   # w = RISK/(2σ_L) на баре сигнала

    def test_llag_enters_next_open_without_lookahead(self):
        n = 300
        t0 = 1_577_836_800_000
        ts = t0 + np.arange(n) * 3_600_000
        rng = np.random.default_rng(8)
        b = F.gbm(n, rng, 0.002)
        a = F.gbm(n, rng, 0.002)
        B = {'ts': ts, 'o': b[0].copy(), 'h': b[1].copy(), 'l': b[2].copy(), 'c': b[3].copy(), 'day': ts // 86_400_000}
        Aa = {'ts': ts, 'o': a[0].copy(), 'h': a[1].copy(), 'l': a[2].copy(), 'c': a[3].copy(), 'day': ts // 86_400_000}
        for k in ('o', 'h', 'l', 'c'):
            B[k][200:] *= 1.06                                                 # скачок BTC на баре 200 (r_BTC ≫ 1σ)
        SB, SA = E.make_series('BTCUSDT', B, t0), E.make_series('ALT', Aa, t0)
        g = E.sig_l_lag(SA, SB, 1)
        self.assertEqual(int(g['sig'][200]), 1)                                # альт не двинулся → лонг
        self.assertEqual(int(g['hold'][200]), 1)
        out = E.simulate(SA, g['sig'], E.Cost(0.0), hold=g['hold'])
        tr = [t for t in out['trades'] if t[0] == 201]
        self.assertTrue(tr, out['trades'][:5])
        # без заглядывания вперёд: сигналы на префиксе совпадают с сигналами на полном ряде
        cut = 250
        Bc = E.make_series('BTCUSDT', {k: v[:cut] for k, v in B.items()}, t0)
        Ac = E.make_series('ALT', {k: v[:cut] for k, v in Aa.items()}, t0)
        gc = E.sig_l_lag(Ac, Bc, 1)
        self.assertTrue(np.array_equal(gc['sig'], g['sig'][:cut]))

    def test_library_signals_have_no_lookahead(self):
        rng = np.random.default_rng(11)
        n = 2500
        ts = 1_546_300_800_000 + np.arange(n) * 3_600_000
        o, h, l, c = F.gbm(n, rng, 0.004)
        bars = {'ts': ts, 'o': o, 'h': h, 'l': l, 'c': c, 'day': ts // 86_400_000}
        t0 = int(ts[400])
        full = E.make_series('X', bars, t0)
        cut = 1800
        part = E.make_series('X', {k: v[:cut] for k, v in bars.items()}, t0)
        for cfg in L.library():
            if cfg.cross_sectional:
                continue
            gf, gp = E.library_signals(cfg, full), E.library_signals(cfg, part)
            lim = cut - 2                                                       # B-VC использует бар t+1
            self.assertTrue(np.array_equal(gf['sig'][:lim], gp['sig'][:lim]), cfg.id)
            for k in ('exit_l', 'exit_s'):
                if k in gf:
                    self.assertTrue(np.array_equal(gf[k][:lim], gp[k][:lim]), (cfg.id, k))
            cost = E.Cost(0.0005)
            rf = E.simulate(full, gf['sig'], cost, exit_l=gf.get('exit_l'), exit_s=gf.get('exit_s'), reverse=gf['reverse'], hold=gf.get('hold'), entry_px=gf.get('entry_px'))
            rp = E.simulate(part, gp['sig'], cost, exit_l=gp.get('exit_l'), exit_s=gp.get('exit_s'), reverse=gp['reverse'], hold=gp.get('hold'), entry_px=gp.get('entry_px'))
            closed = [t for t in rp['trades'] if t[4] != 'end' and t[1] < cut - 10]
            self.assertEqual(closed, rf['trades'][:len(closed)], cfg.id)

    def test_bvc_both_levels_is_minus_one_r_and_not_skipped(self):
        """v104 (§2.41.19): оба уровня на баре t+1 — сделка открыта и закрыта по стопу: −1R плюс издержки обеих сторон; отбора по исходу бара нет."""
        n = 60
        t0 = 1_577_836_800_000
        ts = t0 + np.arange(n) * 3_600_000
        c = np.full(n, 100.0)
        o, h, l = c.copy(), c + 2.0, c - 2.0
        h[30], l[30] = 100.4, 99.6                                              # бар 30 — NR7 (узкий диапазон)
        h[31], l[31] = 101.0, 99.0                                              # бар 31 достигает ОБА уровня
        S = E.make_series('X', {'ts': ts, 'o': o, 'h': h, 'l': l, 'c': c, 'day': ts // 86_400_000}, t0)
        g = E.sig_b_vc(S)
        self.assertEqual(int(g['sig'][30]), E.BOTH)
        r0 = E.simulate(S, g['sig'], E.Cost(0.0), hold=g['hold'], entry_px=g['entry_px'])
        self.assertEqual([t[4] for t in r0['trades'] if t[0] == 31], ['both'])
        self.assertAlmostEqual(r0['ret'][31], -E.RISK, places=12)                # −1R без издержек
        r1 = E.simulate(S, g['sig'], E.Cost(0.001), hold=g['hold'], entry_px=g['entry_px'])
        w = E.RISK / (3 * S['atr'][30] / S['o'][31])
        self.assertAlmostEqual(r1['ret'][31], -E.RISK - 2 * w * 0.001, places=12)  # плюс издержки обеих сторон
        # событие занимает только бар t+1: следующий сигнал возможен сразу (нет блокировки позиции)
        self.assertEqual([t[:2] for t in r1['trades'] if t[0] == 31], [(31, 31)])

    def test_bvc_single_level_unchanged_and_no_outcome_selection(self):
        n = 80
        t0 = 1_577_836_800_000
        rng = np.random.default_rng(3)
        o, h, l, c = F.gbm(n, rng, 0.004)
        ts = t0 + np.arange(n) * 3_600_000
        S = E.make_series('X', {'ts': ts, 'o': o, 'h': h, 'l': l, 'c': c, 'day': ts // 86_400_000}, t0)
        g = E.sig_b_vc(S)
        # каждый NR7-бар, у которого на следующем баре задет хотя бы один уровень, даёт сигнал (1, −1 или BOTH): ничего не отбрасывается по исходу
        rng7 = h - l
        nr7 = np.zeros(n, bool)
        for t in range(6, n - 1):
            nr7[t] = rng7[t] <= rng7[t - 6:t].min()
        hit = (h[1:] >= h[:-1]) | (l[1:] <= l[:-1])
        expect = nr7[:-1] & hit
        self.assertTrue(np.array_equal(g['sig'][:-1] != 0, expect))
        both = nr7[:-1] & (h[1:] >= h[:-1]) & (l[1:] <= l[:-1])
        self.assertTrue(np.array_equal(g['sig'][:-1] == E.BOTH, both))
        self.assertTrue(np.all(g['hold'][(g['sig'] == 1) | (g['sig'] == -1)] == 6))
        self.assertTrue(np.all(g['hold'][g['sig'] == E.BOTH] == 0))


class Stats(unittest.TestCase):
    def test_bh(self):
        p = [0.0001, 0.0004, 0.0019, 0.0095, 0.0201, 0.0278, 0.0298, 0.0344, 0.0459, 0.3240, 0.4262, 0.5719, 0.6528, 0.7590, 1.000]    # Benjamini–Hochberg 1995
        rej = E.bh_reject(p, 0.05)
        self.assertEqual(int(rej.sum()), 4)
        self.assertEqual(list(np.flatnonzero(rej)), [0, 1, 2, 3])
        self.assertEqual(int(E.bh_reject(p[::-1], 0.05).sum()), 4)                # порядок входа не важен
        self.assertEqual(int(E.bh_reject([0.9, 0.8], 0.05).sum()), 0)

    def test_nw_matches_plain_for_iid_and_inflates_for_autocorr(self):
        rng = np.random.default_rng(1)
        x = rng.normal(0.1, 1.0, 5000)
        m, t, n = E.nw_t(x)
        self.assertAlmostEqual(t, x.mean() / (x.std() / np.sqrt(len(x))), delta=0.25)
        y = np.convolve(rng.normal(0, 1, 5005), np.ones(6) / 6, mode='valid') + 0.05           # сильная автокорреляция
        _m, ty, _n = E.nw_t(y)
        plain = y.mean() / (y.std() / np.sqrt(len(y)))
        self.assertLess(abs(ty), abs(plain))                                      # NW снижает t при положительной автокорреляции

    def test_p_and_dd_and_sharpe(self):
        self.assertAlmostEqual(E.p_one_sided(0.0), 0.5)
        self.assertAlmostEqual(E.p_one_sided(1.6449), 0.05, places=3)
        self.assertEqual(E.p_one_sided(None), 1.0)
        self.assertAlmostEqual(E.max_drawdown(np.array([0.1, -0.05, -0.05, 0.2, -0.3])), 0.3)
        self.assertAlmostEqual(E.sharpe(np.array([1.0, 3.0]), 252), 2.0 / 1.0 * np.sqrt(252))


class Count(unittest.TestCase):
    N_FULL = {'CRYPTO': 16, 'COMMODITIES': 4, 'FX': 7, 'INDICES': 5, 'BONDS': 2, 'MULTI': 18}

    def test_n_is_204(self):
        tests = RF.enumerate_tests(self.N_FULL)
        self.assertEqual(len(tests), 204)
        self.assertEqual(RF.count_n(self.N_FULL), 204)
        fams = [t[4] for t in tests]
        self.assertEqual(fams.count('P-PAIR'), 6)
        self.assertEqual(fams.count('L-LAG'), 4)
        self.assertEqual(fams.count('X-MOM'), 2 * 3 + 2 * 3 + 2)                  # CRYPTO 3 ТФ, FX 3 ТФ, MULTI 1D — по 2
        by_cell = {}
        for ci, cls, tf, cid, fam in tests:
            by_cell[(cls, tf)] = by_cell.get((cls, tf), 0) + 1
        self.assertEqual(by_cell[('CRYPTO', '1D')], 17 + 2)
        self.assertEqual(by_cell[('CRYPTO', '4h')], 17 + 2 + 2)
        self.assertEqual(by_cell[('CRYPTO', '1h')], 17 + 2 + 2)
        self.assertEqual(by_cell[('COMMODITIES', '1D')], 15)
        self.assertEqual(by_cell[('BONDS', '1D')], 15)
        self.assertEqual(by_cell[('MULTI', '1D')], 17)
        self.assertEqual(by_cell[('FX', '1h')], 17)
        self.assertEqual(by_cell[('INDICES', '4h')], 15)

    def test_without_xag_n_unchanged(self):
        n = dict(self.N_FULL, COMMODITIES=3, MULTI=17)
        self.assertEqual(RF.count_n(n), 204)

    def test_spec_records_n_and_seed(self):
        spec = open(os.path.join(ROOT, 'BOT_SPEC.md'), encoding='utf-8').read()
        self.assertIn('**N = 204**', spec)
        self.assertIn('20261025', spec)
        self.assertEqual(RF.SEED, 20261025)


class Loading(unittest.TestCase):
    def setUp(self):
        self.old = dict(FD.INSTRUMENTS)
        FD.INSTRUMENTS['EURUSD'] = ('FX', 'EURUSD', (0.5, 2.0), (2024, 1))
        self.d = tempfile.mkdtemp()
        self.now = F.ms(2024, 4, 10)                                            # последний полный месяц — 2024-03

    def tearDown(self):
        FD.INSTRUMENTS.clear()
        FD.INSTRUMENTS.update(self.old)

    async def _sleep(self, t):
        return None

    def test_load_and_resume(self):
        calls = []
        fetch = fake_duka({'EURUSD'}, 1.10, 1e5, calls=calls)
        st = run(FD.load_instrument(fetch, 'EURUSD', self.d, self.now, pause=0, delays=(), sleep=self._sleep))
        self.assertEqual((st['loaded'], st['failed'], st['requests']), (3, [], 3))
        self.assertEqual(FD.stored_tags(self.d, 'EURUSD'), ['2024-01', '2024-02', '2024-03'])
        self.assertEqual(FD.load_index(self.d, 'EURUSD')['scale'], 1e5)
        z = V._load_npz(FD.month_path(self.d, 'EURUSD', 2024, 1))
        self.assertTrue(0.9 < float(np.median(z['c'])) < 1.4)                   # цены поделены на масштаб
        calls.clear()
        st2 = run(FD.load_instrument(fetch, 'EURUSD', self.d, self.now, pause=0, delays=(), sleep=self._sleep))
        self.assertEqual((st2['loaded'], st2['skipped'], len(calls)), (0, 3, 0))   # продолжение с места остановки: повторных запросов нет
        self.assertTrue(FD.coverage(self.d, 'EURUSD', self.now)['complete'])

    def test_404_marked_absent_and_not_retried(self):
        calls = []
        fetch = fake_duka(set(), 1.10, 1e5, calls=calls)
        st = run(FD.load_instrument(fetch, 'EURUSD', self.d, self.now, pause=0, delays=(), sleep=self._sleep))
        self.assertEqual(st['absent_new'], 3)
        n = len(calls)
        run(FD.load_instrument(fetch, 'EURUSD', self.d, self.now, pause=0, delays=(), sleep=self._sleep))
        self.assertEqual(len(calls), n)
        ok, probs, avail, excl = RF.availability(self.d, self.now)
        self.assertIn('EURUSD', excl)

    def test_chronic_503_stops_after_three_and_resumes(self):
        state = {'bad': True}
        inner = fake_duka({'EURUSD'}, 1.10, 1e5)

        async def fetch(url, params=None, method='GET'):
            if state['bad']:
                return {'status': 503, 'headers': {}, 'body': b'', 'error': ''}
            return await inner(url, params, method)
        FD.INSTRUMENTS['EURUSD'] = ('FX', 'EURUSD', (0.5, 2.0), (2023, 10))
        st = run(FD.load_instrument(fetch, 'EURUSD', self.d, self.now, pause=0, delays=(0, 0), sleep=self._sleep, max_consec_fail=3))
        self.assertEqual(len(st['failed']), 3)
        self.assertIn('подряд', st['stopped'])
        state['bad'] = False
        st2 = run(FD.load_instrument(fetch, 'EURUSD', self.d, self.now, pause=0, delays=(), sleep=self._sleep))
        self.assertEqual(st2['loaded'], 6)
        self.assertTrue(FD.coverage(self.d, 'EURUSD', self.now)['complete'])

    def test_retry_delays_are_3_8_20_45(self):
        self.assertEqual(FD.RETRY_DELAYS, (3, 8, 20, 45))
        self.assertEqual(FD.PAUSE_S, 1.0)


class Lockbox(unittest.TestCase):
    def setUp(self):
        L._lockbox_session_state['opened'] = False
        self.d = tempfile.mkdtemp()
        F.write_instrument(self.d, 'EURUSD', first=(2024, 10), last=(2025, 2), seed=2, p0=1.1)

    def tearDown(self):
        L._lockbox_session_state['opened'] = False

    def test_pre_mode_never_opens_lockbox_files(self):
        opened = []
        orig = FD._read_month
        FD._read_month = lambda p: (opened.append(os.path.basename(p)), orig(p))[1]
        try:
            raw = FD.load_raw(self.d, 'EURUSD', 'pre')
        finally:
            FD._read_month = orig
        self.assertTrue(opened and all(o < '2025-01' for o in opened), opened)
        self.assertLess(int(raw['ts'].max()), L.lockbox_start_ms())

    def test_full_requires_session_and_session_opens_once(self):
        with self.assertRaises(L.LockboxError):
            FD.load_raw(self.d, 'EURUSD', 'full')
        with self.assertRaises(L.LockboxError):
            L.open_lockbox_session()
        with self.assertRaises(L.LockboxError):
            L.open_lockbox_session(final=False)
        sess = L.open_lockbox_session(final=True)
        raw = FD.load_raw(self.d, 'EURUSD', 'full', sess)
        self.assertGreaterEqual(int(raw['ts'].max()), L.lockbox_start_ms())
        with self.assertRaises(L.LockboxError):
            L.open_lockbox_session(final=True)

    def test_series_cut_before_lockbox(self):
        raw = FD.load_raw(self.d, 'EURUSD', 'pre')
        for tf in ('1h', '4h', '1D'):
            bars = FD.build_noncrypto(raw, tf)
            self.assertLess(int(bars['ts'].max()), L.lockbox_start_ms())
            self.assertLess(int(bars['day'].max()), L.lockbox_start_ms() // 86_400_000 + 1)


if __name__ == '__main__':
    unittest.main()
