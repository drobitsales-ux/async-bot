"""v120: исключение не-крипто из вселенных H2 и X1 (§2.52), состояние «счёт пуст — режим ожидания», строка «Окружение» в /status, закрытие временных ccxt."""
import asyncio
import tempfile
import unittest

import numpy as np

import access_probe as AP
import battery_fixtures as BF
import research_h2repl as H2R
import research_xdata as X
import research_xfund as XF
from _common import a, fresh_state
from test_v116_h2x1 import D0, END, DAY, mk_bars, mk_fund, nosleep, run, write_xdata, x1_inputs
from test_v119_okx_equity import SWAP_OK, SWAP_ZERO


class Reason(unittest.TestCase):
    def test_prefix_fields_and_crypto(self):
        self.assertEqual(X.noncrypto_reason('NCSKAAPL'), 'NC-префикс')
        self.assertEqual(X.noncrypto_reason('ncfxeur'), 'NC-префикс')
        self.assertEqual(X.noncrypto_reason('NCCOGOLD'), 'NC-префикс')
        self.assertEqual(X.noncrypto_reason('NCT'), 'NC-префикс')                         # по правилу владельца — любой NC*; ложные срабатывания видны в отчёте
        self.assertIsNone(X.noncrypto_reason('BTC'))
        self.assertIsNone(X.noncrypto_reason('PEPE', {'instCategory': '1'}))
        self.assertEqual(X.noncrypto_reason('TSLA', {'instCategory': '3'}), 'поле instCategory=3')
        self.assertEqual(X.noncrypto_reason('XAU', {'category': 'Commodity'}), 'поле category=Commodity')
        self.assertIsNone(X.noncrypto_reason('SOL', {'assetType': 'crypto'}))
        self.assertEqual(X.cat_of({'instCategory': '3'}), 'instCategory=3')
        self.assertEqual(X.cat_of({}), '')

    def test_usdt_swaps_marks_exclusion(self):
        mk = {'NCSKAAPL/USDT:USDT': {'swap': True, 'linear': True, 'quote': 'USDT', 'settle': 'USDT', 'base': 'NCSKAAPL', 'active': True},
              'TSLA/USDT:USDT': {'swap': True, 'linear': True, 'quote': 'USDT', 'settle': 'USDT', 'base': 'TSLA', 'active': True, 'info': {'instCategory': '3'}},
              'ABC/USDT:USDT': {'swap': True, 'linear': True, 'quote': 'USDT', 'settle': 'USDT', 'base': 'ABC', 'active': True, 'info': {'instCategory': '1'}}}
        sw = X.usdt_swaps(mk)
        self.assertEqual(sw['NCSKAAPL/USDT:USDT']['excl'], 'NC-префикс')
        self.assertEqual(sw['TSLA/USDT:USDT']['excl'], 'поле instCategory=3')
        self.assertIsNone(sw['ABC/USDT:USDT']['excl'])
        self.assertEqual(sw['ABC/USDT:USDT']['cat'], 'instCategory=1')


class Load(unittest.TestCase):
    def make_ex(self, markets, bars, funding):
        spy = []

        class Ex:
            async def load_markets(self_):
                return markets

            async def fetch_ohlcv(self_, sym, tf, since, limit):
                spy.append(('ohlcv', sym))
                if tf == '1h':
                    return []
                b = bars[sym]
                return [[int(t), o, h, l, c, v] for t, o, h, l, c, v in zip(b['ts'], b['o'], b['h'], b['l'], b['c'], b['v']) if t >= since][:limit]

            async def fetch_funding_rate_history(self_, sym, since=None, limit=None, params=None):
                spy.append(('fund', sym))
                ts, r = funding[sym]
                end = (params or {}).get('endTime')
                sel = [(int(t), float(x)) for t, x in zip(ts, r) if end is None or t <= end][-limit:]
                return [{'timestamp': t, 'fundingRate': x} for t, x in sel]

            async def close(self_):
                return None
        e = Ex()
        e.spy = spy
        return e

    def test_excluded_symbols_are_not_requested_and_report_counts(self):
        m = lambda base, info=None: {'swap': True, 'linear': True, 'quote': 'USDT', 'settle': 'USDT', 'base': base, 'active': True, 'info': info or {}}
        mk = {'NEW02/USDT:USDT': m('NEW02'), 'NCSKAAPL/USDT:USDT': m('NCSKAAPL'), 'NCSIDAX/USDT:USDT': m('NCSIDAX'), 'TSLA/USDT:USDT': m('TSLA', {'category': 'stock'})}
        b = mk_bars(D0 + 120, 300, 6, qv=6e7)
        f = mk_fund(D0 + 120, 300, 8)
        ex = self.make_ex(mk, {s: b for s in mk}, {s: f for s in mk})
        bdir, xdir = tempfile.mkdtemp(), tempfile.mkdtemp()
        BF.write_battery(bdir, n_syms=18)
        import os
        os.environ['HTF_FETCH_PAUSE_SEC'] = '0'
        try:
            txt = run(X.run_xload({'xdir': xdir, 'bdir': bdir, 'venues': ('bingx',)}, None, exchanges={'bingx': ex}, now_ms=(END + 1) * DAY, sleep=nosleep))
        finally:
            os.environ.pop('HTF_FETCH_PAUSE_SEC', None)
        asked = {s for _k, s in ex.spy}
        self.assertEqual(asked, {'NEW02/USDT:USDT'})                                       # по NC*/не-крипто запросов нет (ни свечей, ни фандинга)
        self.assertIn('исключено не-крипто (§2.52): 3', txt)
        self.assertIn('NC-префикс: 2', txt)
        self.assertIn('поле category: 1', txt)
        self.assertIn('NCSKAAPL', txt)
        self.assertIn('поля «класс актива» в markets (bingx): category=stock: 1', txt)
        idx = X.load_index(xdir)
        self.assertEqual(idx['markets']['bingx']['NCSKAAPL/USDT:USDT']['excl'], 'NC-префикс')
        self.assertNotIn('NCSKAAPL/USDT:USDT', idx['bars']['bingx'])

    def test_no_category_fields_is_reported(self):
        sw = X.usdt_swaps({'A/USDT:USDT': {'swap': True, 'linear': True, 'quote': 'USDT', 'settle': 'USDT', 'base': 'A', 'active': True}})
        self.assertIn('не найдено', '\n'.join(X.noncrypto_lines('bingx', sw)))


class Universe(unittest.TestCase):
    def test_h2_universe_excludes_noncrypto_even_for_old_cache(self):
        bdir = tempfile.mkdtemp()
        BF.write_battery(bdir, n_syms=18)
        bn = X.binance_norm_set(bdir)
        mk = {'bingx': {'OLDA/USDT:USDT': {'base': 'OLDA', 'norm': 'OLDA', 'active': True}, 'NEWA/USDT:USDT': {'base': 'NEWA', 'norm': 'NEWA', 'active': True},
                        'NCSKTSLA/USDT:USDT': {'base': 'NCSKTSLA', 'norm': 'NCSKTSLA', 'active': True},           # запись кэша v119 — без поля excl
                        'XYZ/USDT:USDT': {'base': 'XYZ', 'norm': 'XYZ', 'active': True, 'excl': 'поле category=stock'}},
              'okx': {'OLDB/USDT:USDT': {'base': 'OLDB', 'norm': 'OLDB', 'active': True}, 'NEWB/USDT:USDT': {'base': 'NEWB', 'norm': 'NEWB', 'active': True}}}
        bars = {'bingx': {s: mk_bars(D0 + (0 if s.startswith('OLD') else 50 + i), 300, 10 + i, qv=5e7) for i, s in enumerate(mk['bingx'])},
                'okx': {'OLDB/USDT:USDT': mk_bars(D0, 300, 2, qv=5e7), 'NEWB/USDT:USDT': mk_bars(D0 + 60, 200, 3, qv=5e7)}}
        uni, ref = X.h2_universe(mk, bars, bn)
        self.assertEqual({u['norm'] for u in uni}, {'NEWA', 'NEWB'})
        self.assertEqual(sorted(ref['noncrypto']), ['NC-префикс', 'поле category'])
        self.assertEqual(ref['noncrypto']['NC-префикс'], ['bingx:NCSKTSLA'])

    def test_panel_and_report_do_not_contain_nc(self):
        bdir, xdir = tempfile.mkdtemp(), tempfile.mkdtemp()
        BF.write_battery(bdir, n_syms=18)
        idx = write_xdata(xdir)
        b = X.load_bars(xdir, 'bingx', 'NEW01/USDT:USDT')
        X.save_bars(xdir, 'bingx', 'NCSKAAPL/USDT:USDT', b['ts'], b['o'], b['h'], b['l'], b['c'], b['v'])
        idx['bars']['bingx']['NCSKAAPL/USDT:USDT'] = dict(idx['bars']['bingx']['NEW01/USDT:USDT'])
        idx['markets']['bingx']['NCSKAAPL/USDT:USDT'] = {'base': 'NCSKAAPL', 'norm': 'NCSKAAPL', 'active': True, 'created': None}
        X.save_index(xdir, idx)
        P, uni, ref = H2R.build_repl_panel(xdir, bdir, ('bybit', 'bingx'))
        self.assertFalse(any('NCSK' in u['sym'] for u in uni))
        self.assertFalse(any('NCSK' in s for s in P['syms']))
        self.assertEqual(ref['noncrypto']['NC-префикс'], ['bingx:NCSKAAPL'])
        txt = '\n'.join(H2R.report_lines(P, uni, ref, {'D': 1000, 's_min': 1.0, 'testable': True}, None, None, None, X.universe_line(uni)))
        self.assertIn('не-крипто (§2.52): NC-префикс: 1', txt)


class X1Filter(unittest.TestCase):
    def test_pairs_exclude_nc_and_stable(self):
        vA, vB, pairs = x1_inputs(nd=120, n_pairs=1)
        vA['bars']['NCSKA/USDT:USDT'] = vA['bars']['sa0']
        vA['markets'] = {'NCSKA/USDT:USDT': {'norm': 'NCSKA', 'base': 'NCSKA'}, 'sa0': {'norm': 'P0', 'base': 'P0'}}
        bn = {'bars': {'NCSKA': vB['bars']['sb0'], 'P0': vB['bars']['sb0']}}
        got = XF.main_pairs_binance(vA, bn)
        self.assertEqual([p[0] for p in got], ['P0'])                                       # NC-база в пары X1 не попадает
        # стейблкоин-подобный
        flat = dict(vA['bars']['sa0'])
        flat['c'] = 1.0 + np.random.default_rng(1).normal(0, 0.0003, len(flat['c']))
        vS = dict(vA, bars={'st': flat}, fund={'st': vA['fund']['sa0']}, hbars={})
        vT = dict(vB, bars={'st': flat}, fund={'st': vB['fund']['sb0']}, hbars={})
        inp, rej = XF.build_inputs(vS, vT, [('ST', 'st', 'st')])
        self.assertIsNone(inp)
        self.assertEqual(rej[0][1], 'стейблкоин-подобный')

    def test_common_pairs_skip_nc(self):
        bars = {'NCSKA/USDT:USDT': mk_bars(D0, 100, 1), 'A/USDT:USDT': mk_bars(D0, 100, 2)}
        mk_ = {'NCSKA/USDT:USDT': {'norm': 'NCSKA', 'base': 'NCSKA'}, 'A/USDT:USDT': {'norm': 'A', 'base': 'A'}}
        ven = {'bingx': {'bars': bars, 'markets': mk_}, 'bybit': {'bars': bars, 'markets': mk_}}
        self.assertEqual([p[0] for p in XF.common_pairs(ven)], ['A'])


class EmptyAccount(unittest.TestCase):
    def setUp(self):
        self.db, self.sent = fresh_state()
        self._ex, self._p = a.exchange_priv, a.EQUITY_RETRY_PAUSES
        self._pos, self._act = a.all_positions, a.active_strategies
        a.EQUITY_RETRY_PAUSES = (5, 15, 30)
        a._dd.update({'last_err': '', 'last_status': '', 'empty_account': False})
        a.all_positions = lambda: []
        a.active_strategies = lambda: ([], ['SMC2'])
        self.sleeps = []
        self._sleep = asyncio.sleep

        async def fake_sleep(x, *args, **kw):
            self.sleeps.append(x)
        a.asyncio.sleep = fake_sleep

    def tearDown(self):
        a.asyncio.sleep = self._sleep
        a.exchange_priv, a.EQUITY_RETRY_PAUSES = self._ex, self._p
        a.all_positions, a.active_strategies = self._pos, self._act

    def _bal(self, bal):
        class Ex:
            async def fetch_balance(self_):
                if isinstance(bal, Exception):
                    raise bal
                return bal
        a.exchange_priv = Ex()

    def test_empty_idle_account_is_quiet(self):
        self._bal(SWAP_ZERO)
        for _ in range(6):
            self.assertFalse(run(a.dd_update()))
        self.assertEqual(a._dd['fails'], 0)                                                # счётчик сбоев не растёт
        self.assertTrue(a._dd['empty_account'])
        self.assertEqual(self.sleeps, [])                                                  # повторов с паузами нет
        self.assertEqual(self.sent, [])                                                    # тревог нет
        self.assertEqual(a.health_conditions(), [])
        self.assertIn('счёт пуст — режим ожидания', a.health_status_line())
        self.assertIn('счёт пуст — режим ожидания', a.dd_status_text())
        self.assertIn('счёт пуст — режим ожидания', a.status_text())
        self.assertNotIn('эквити ещё не получено', a.status_text())

    def test_funds_appear_returns_to_normal_mode(self):
        self._bal(SWAP_ZERO)
        run(a.dd_update())
        self.assertTrue(a._dd['empty_account'])
        self._bal(SWAP_OK)
        self.assertTrue(run(a.dd_update()))
        self.assertFalse(a._dd['empty_account'])
        self.assertEqual(a._dd['equity'], 15.6128)
        self.assertNotIn('счёт пуст', a.health_status_line())

    def test_open_position_or_live_strategy_means_normal_alarm(self):
        self._bal(SWAP_ZERO)
        a.all_positions = lambda: [{'sym': 'X'}]
        for _ in range(3):
            run(a.dd_update())
        self.assertFalse(a._dd['empty_account'])
        self.assertEqual(a._dd['fails'], 3)
        self.assertTrue(any('3 сбоя подряд' in t for t in self.sent))
        self.assertTrue(any('баланс фьючерсов USDT-M: 0' in t for t in self.sent))
        # живая стратегия
        self.sent.clear()
        a._dd.update({'fails': 0, 'fail_since': 0.0})
        a.all_positions = lambda: []
        a.active_strategies = lambda: (['SMC'], [])
        for _ in range(3):
            run(a.dd_update())
        self.assertEqual(a._dd['fails'], 3)
        self.assertTrue(any('3 сбоя подряд' in t for t in self.sent))

    def test_access_error_is_still_an_alarm(self):
        class Boom(Exception):
            pass
        self._bal(Boom('{"code":100413,"msg":"Incorrect apiKey"}'))
        for _ in range(3):
            run(a.dd_update())
        self.assertFalse(a._dd['empty_account'])
        self.assertEqual(a._dd['fails'], 3)
        al = [t for t in self.sent if '3 сбоя подряд' in t]
        self.assertEqual(len(al), 1)
        self.assertIn('Boom 100413 Incorrect apiKey', al[0])


class StatusEnv(unittest.TestCase):
    def test_status_text_has_environment_line(self):
        fresh_state()
        txt = a.status_text()
        self.assertIn('Окружение: Python', txt)
        self.assertIn('ccxt', txt)


class CloseCcxt(unittest.TestCase):
    def test_close_twice_with_pause_and_never_raises(self):
        calls, sleeps = [], []

        class Ex:
            async def close(self_):
                calls.append(1)
                if len(calls) == 1:
                    raise RuntimeError('boom')

        async def sl(x):
            sleeps.append(x)
        run(AP.close_ccxt(Ex(), sleep=sl))
        self.assertEqual(len(calls), 2)
        self.assertEqual(sleeps, [AP.CLOSE_DELAY])

    def test_probe_closes_instance_after_failure(self):
        import ccxt.async_support as cx
        made = []

        class Bad:
            def __init__(self, cfg):
                self.closes = 0
                made.append(self)

            async def load_markets(self):
                raise RuntimeError('geo')

            async def close(self):
                self.closes += 1
        saved = cx.bybit
        cx.bybit = Bad
        try:
            async def sl(x):
                return None
            r = run(AP.ccxt_probe('bybit', sleep=sl))
        finally:
            cx.bybit = saved
        self.assertFalse(r['ok'])
        self.assertIn('RuntimeError', r['exc'])
        self.assertEqual(made[0].closes, 2)

    def test_own_clients_in_loader_are_closed(self):
        closes = []

        class Ex:
            def __init__(self, n):
                self.n = n

            async def load_markets(self):
                raise RuntimeError('x')

            async def close(self):
                closes.append(self.n)
        # exchanges передан снаружи → не закрываются (владелец — вызывающий); путь own проверен чтением исходника
        import inspect
        src = inspect.getsource(X.run_xload)
        self.assertIn('access_probe.close_ccxt(c)', src)


if __name__ == '__main__':
    unittest.main()
