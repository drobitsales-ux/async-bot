"""v124 (BOT_SPEC §2.53.2): 1h BingX по символам сделок — не гейт, а показатель («подтверждённо пусто» → дневной запасной базис); ошибка загрузки 1h — по-прежнему гейт;
разбивка отчёта X1 по базису (1h / дневной) и аварийные закрытия по 1h и по дневным high/low; суммарные массивы побитово как у эталона v122."""
import tempfile
import unittest

import numpy as np

import _common
import battery_fixtures as BF
import ref_xfund_v122 as REF
import research_xdata as X
import research_xfund as XF
import synth_prod as SP
from test_v116_h2x1 import run, set_hour, x1_inputs
from test_v123_stream import same

ROOT = _common.ROOT
BASES = ['BTC', 'ETH', 'SOL', 'BNB', 'XRP', 'ADA', 'DOGE', 'AVAX', 'LINK', 'DOT']                  # базы из архива Binance фикстуры battery (H16)


def idx_with(n_loaded, n_empty, n_error, n_pending):
    """Индекс X1: 10 символов BingX с базами из архива, свечи и фандинг полные; 1h: загружены / подтверждённо пусты / ошибка биржи / не обработаны."""
    d, b = tempfile.mkdtemp(), tempfile.mkdtemp()
    BF.write_battery(b, n_syms=18)
    idx = X.load_index(d)
    syms = []
    for base in BASES:
        s = f'{base}/USDT:USDT'
        syms.append(('bingx', s))
        idx['markets']['bingx'][s] = {'base': base, 'norm': base, 'active': True, 'created': None, 'excl': None}
        idx['bars']['bingx'][s] = {'first': 1, 'last': 2, 'n': 2, 't': 3, 'complete': True}
        idx['fund']['bingx'][s] = {'first': 1, 'last': 2, 'n': 2, 't': 3, 'complete': True}
    k = 0
    for count, kind in ((n_loaded, 'loaded'), (n_empty, 'empty'), (n_error, 'error'), (n_pending, 'pending')):
        for _ in range(count):
            s = syms[k][1]
            k += 1
            if kind == 'loaded':
                idx['hbars']['bingx'][s] = {'first': 1, 'last': 2, 'n': 2, 't': 3, 'complete': True}
            elif kind == 'empty':
                idx['neg'][f'bingx|{s}|h1'] = {'t': 3, 'why': 'empty'}
            elif kind == 'error':
                idx['neg'][f'bingx|{s}|h1'] = {'t': 3, 'why': 'ExchangeError'}
    assert k == 10
    X.save_index(d, idx)
    return d, b, syms


class Gate(unittest.TestCase):
    def test_confirmed_empty_hourly_does_not_block(self):
        d, b, syms = idx_with(3, 7, 0, 0)                                                         # 1h есть у 3 из 10, у остальных подтверждённо пусто
        c = X.coverage_report(d, 'x1', b, hourly_syms=syms)
        self.assertTrue(c['ok'], c['lines'])
        self.assertEqual((len(c['hourly']['loaded']), len(c['hourly']['empty'])), (3, 7))
        line = X.hourly_line(c['hourly'])
        self.assertIn('1h доступны для 3 из 10 символов сделок', line)
        self.assertIn('подтверждённо пустой 1h-историей (7) используют дневной запасной базис', line)
        self.assertIn('ошибка загрузки 1h: 0, не обработано: 0', line)

    def test_hourly_load_error_still_blocks(self):
        d, b, syms = idx_with(5, 3, 2, 0)                                                         # 2 ошибки биржи → (5+3)/10 = 80% < 90%
        c = X.coverage_report(d, 'x1', b, hourly_syms=syms)
        self.assertFalse(c['ok'])
        self.assertIn('ошибка загрузки 1h: 2', '; '.join(c['lines']))
        self.assertIn('< 90%', '; '.join(c['lines']))

    def test_pending_hourly_blocks_and_threshold_is_90_percent(self):
        d, b, syms = idx_with(5, 3, 0, 2)                                                         # 2 не обработаны (сеть/продолжение) → гейт
        self.assertFalse(X.coverage_report(d, 'x1', b, hourly_syms=syms)['ok'])
        d, b, syms = idx_with(5, 4, 1, 0)                                                         # одна ошибка из 10 → 90% — проходит (порог ≥ 90%)
        self.assertTrue(X.coverage_report(d, 'x1', b, hourly_syms=syms)['ok'])
        d, b, syms = idx_with(0, 10, 0, 0)                                                        # 1h нет вообще, но всё подтверждённо пусто — расчёт на дневном базисе разрешён
        c = X.coverage_report(d, 'x1', b, hourly_syms=syms)
        self.assertTrue(c['ok'], c['lines'])

    def test_daily_candles_funding_depth_remain_a_gate(self):
        d, b, syms = idx_with(10, 0, 0, 0)
        idx = X.load_index(d)
        for _ex, s in syms[:3]:
            idx['fund']['bingx'].pop(s)                                                           # у 3 из 10 нет фандинга → 70% < 90% — гейт остаётся
        X.save_index(d, idx)
        c = X.coverage_report(d, 'x1', b, hourly_syms=syms)
        self.assertFalse(c['ok'])
        self.assertIn('фандинг: 7 из 10', '; '.join(c['lines']))

    def test_loader_separates_empty_from_error(self):
        import asyncio
        d = tempfile.mkdtemp()
        idx = X.load_index(d)
        Bad = type('BadSymbol', (Exception,), {})

        class Ex:
            async def fetch_ohlcv(self, sym, tf, since=None, limit=None, params=None):
                if sym.startswith('ERR'):
                    raise Bad('no such symbol')
                return []                                                                         # подтверждённо пусто

        async def nosleep(_s):
            return None
        fx = {'bingx': X.Fetcher(Ex(), pause=0, sleep=nosleep)}
        out = asyncio.run(X.load_hourly(d, idx, fx, 'bingx', ['EMP1/USDT:USDT', 'EMP2/USDT:USDT', 'ERR1/USDT:USDT'], 10 ** 13, 10 ** 12 + 10 ** 13))
        self.assertEqual(out, (0, 2, 1, 0, False))                                                # (загружено, пусто, ошибка, осталось, остановлено)
        st = X.hourly_status(idx, [('bingx', 'EMP1/USDT:USDT'), ('bingx', 'ERR1/USDT:USDT'), ('bingx', 'NEW/USDT:USDT')])
        self.assertEqual((st['empty'], st['error'], st['pending']), (['EMP1/USDT:USDT'], ['ERR1/USDT:USDT'], ['NEW/USDT:USDT']))


class Report(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.xdir, cls.bdir = tempfile.mkdtemp(), tempfile.mkdtemp()
        cls.info = SP.build(cls.xdir, cls.bdir, scale=0.025, days=(700, 730), fund_gap=0.0008, h1_n=3)
        cls.traded, _n = XF.prelim_traded(cls.xdir, cls.bdir, 0.0005)

    def _run(self, xdir):
        return run(XF.run_xfund({'xdir': xdir, 'bdir': self.bdir, 'fdir': tempfile.mkdtemp(), 'fee_taker': 0.0005, 'version': 'vT', 'x1_pair': 'binance'}))

    def _copy(self):
        import shutil
        d = tempfile.mkdtemp()
        shutil.rmtree(d)
        shutil.copytree(self.xdir, d)
        return d

    def test_pending_hourly_refuses_then_confirmed_empty_runs_with_split_report(self):
        self.assertGreater(len(self.traded), 4)
        d = self._copy()
        txt = self._run(d)
        self.assertIn('⛔ /xfund_test отказ: данные неполны', txt)                              # 1h есть у 3 символов, остальные не обработаны → гейт
        self.assertNotIn('APR', txt)
        idx = X.load_index(d)
        for _lab, s in self.traded:
            if s not in idx['hbars']['bingx']:
                X.put(d, idx, 'neg', 'bingx', f'{s}|h1', {'t': 3, 'why': 'empty'})            # подтверждённо пусто → дневной запасной базис
        txt = self._run(d)
        self.assertNotIn('⛔', txt)
        self.assertIn(f'1h доступны для {len(self.info["h1"] and [s for _l, s in self.traded if s in self.info["h1"]])} из {len(self.traded)} символов сделок', txt)
        self.assertIn('используют дневной запасной базис', txt)
        self.assertEqual(txt.count('разбивка по базису (справочно, не критерии)'), 3)             # по одной на каждый T
        self.assertIn('1h-базис: сделок', txt)
        self.assertIn('дневной запасной базис: сделок', txt)
        self.assertIn('аварийных закрытий (позиций): определено по 1h', txt)

    def test_hourly_error_refuses(self):
        d = self._copy()
        idx = X.load_index(d)
        miss = [s for _l, s in self.traded if s not in idx['hbars']['bingx']]
        for s in miss:
            X.put(d, idx, 'neg', 'bingx', f'{s}|h1', {'t': 3, 'why': 'ExchangeError'})        # постоянная ошибка биржи — не «пусто» → гейт
        txt = self._run(d)
        self.assertIn('⛔ /xfund_test отказ: данные неполны', txt)
        self.assertIn(f'ошибка загрузки 1h: {len(miss)}', txt)


class Split(unittest.TestCase):
    """Разбивка не меняет расчёт: суммарные массивы побитово как у эталона v122; группы дают те же суммы; источники аварийных закрытий считаются верно."""

    def _both(self, vA, vB, pairs, T=0.0002):
        ni, _ = XF.build_inputs(vA, vB, pairs)
        ri, _ = REF.build_inputs(vA, vB, pairs)
        a_, b_ = XF.simulate_x1(ni, T), REF.simulate_x1(ri, T)
        return ni, a_, b_

    def _check(self, a_, b_):
        extra = {k: a_.pop(k) for k in ('em_h', 'em_d', 'groups')}
        same(a_, b_, 'sim')                                                                       # суммарные массивы, сделки, счётчики — побитово как v122
        g = extra['groups']
        for name in ('pnl', 'fund', 'price', 'cost'):
            self.assertTrue(np.allclose(g['h'][name] + g['d'][name], a_[name], rtol=0, atol=1e-12), name)
        self.assertEqual(len(g['h']['trades']) + len(g['d']['trades']), len(a_['trades']))
        self.assertEqual(g['h']['open_end'] + g['d']['open_end'], a_['open_end'])
        return extra

    def test_hourly_data_all_trades_in_hourly_group(self):
        vA, vB, pairs = x1_inputs(nd=120, n_pairs=3)
        ni, a_, b_ = self._both(vA, vB, pairs)
        self.assertGreater(len(a_['trades']), 0)
        ex = self._check(a_, b_)
        self.assertEqual(len(ex['groups']['d']['trades']), 0)
        self.assertEqual((ex['em_h'], ex['em_d']), (0, 0))

    def test_daily_fallback_all_trades_in_daily_group(self):
        vA, vB, pairs = x1_inputs(nd=120, n_pairs=3, hourly=False)
        ni, a_, b_ = self._both(vA, vB, pairs)
        self.assertGreater(len(a_['trades']), 0)
        ex = self._check(a_, b_)
        self.assertEqual(len(ex['groups']['h']['trades']), 0)
        self.assertTrue(all(t['fb'] for t in a_['trades']))

    def test_emergency_by_hourly_high_vs_daily_high(self):
        vA, vB, pairs = x1_inputs(nd=120)
        ni, _ = XF.build_inputs(vA, vB, pairs)
        r0 = XF.simulate_x1(ni, 0.0002)
        e = r0['trades'][0]['e']
        p0a = float(vA['hbars']['sa0']['c'][e * 24])
        set_hour(vA, 'sa0', (ni['day0'] + e) * 24 + 24 * 2 + 5, 'h', p0a * 1.35)
        ni, a_, b_ = self._both(vA, vB, pairs)
        ex = self._check(a_, b_)
        self.assertEqual((ex['em_h'], ex['em_d']), (1, 0))                                       # определено по 1h
        # дневной запасной путь: без часовых данных
        vA2, vB2, pairs2 = x1_inputs(nd=120, hourly=False)
        ni2, _ = XF.build_inputs(vA2, vB2, pairs2)
        e2 = XF.simulate_x1(ni2, 0.0002)['trades'][0]['e']
        vA2['bars']['sa0']['h'][e2 + 2] = float(vA2['bars']['sa0']['c'][e2]) * 1.36
        ni2, a2, b2 = self._both(vA2, vB2, pairs2)
        ex2 = self._check(a2, b2)
        self.assertEqual((ex2['em_h'], ex2['em_d']), (0, 1))                                     # определено по дневным high/low
        self.assertEqual(len(ex2['groups']['d']['trades']), len(a2['trades']))

    def test_gap_liquidation_path_is_bitwise_too(self):
        vA, vB, pairs = x1_inputs(nd=120)
        ni, _ = XF.build_inputs(vA, vB, pairs)
        e = XF.simulate_x1(ni, 0.0002)['trades'][0]['e']
        p0a = float(vA['hbars']['sa0']['c'][e * 24])
        set_hour(vA, 'sa0', (ni['day0'] + e) * 24 + 24 * 2 + 5, 'h', p0a * 1.50)
        ni, a_, b_ = self._both(vA, vB, pairs)
        self._check(a_, b_)
        self.assertEqual(a_['liq'], 1)


if __name__ == '__main__':
    unittest.main()
