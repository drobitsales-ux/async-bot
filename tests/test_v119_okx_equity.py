"""v119: эквити «пустой ответ» (§2.51.1), OKX без ccxt (§2.51), вселенная H2 с OKX и фандинг = 0 со счётчиком, подсказки команд."""
import os
import re
import tempfile
import unittest

import numpy as np

import _common
import battery_fixtures as BF
import research_battery as RB
import research_h2repl as H2R
import research_okx as OKX
import research_xdata as X
from _common import a, fresh_state
from test_v116_h2x1 import D0, END, DAY, mk_bars, nosleep, run, write_xdata

# Формат взят из документации/комментариев ccxt 4.2.14 (swap), НЕ снимок прода: поля и значения вымышлены, id и ключей нет.
SWAP_OK = {'info': {'code': 0, 'msg': '', 'data': {'balance': {'userId': '1234567', 'asset': 'USDT', 'balance': '15.6128', 'equity': '15.6128', 'unrealizedProfit': '0.0000',
                                                                 'realisedProfit': '0.0000', 'availableMargin': '15.6128', 'usedMargin': '0.0000', 'freezedMargin': '0.0000'}}},
           'USDT': {'free': 15.6128, 'used': 0.0, 'total': 15.6128}, 'free': {'USDT': 15.6128}, 'used': {'USDT': 0.0}, 'total': {'USDT': 15.6128}}
SWAP_ZERO = {'info': {'code': 0, 'msg': '', 'data': {'balance': {'asset': 'USDT', 'balance': '0.0000', 'equity': '0.0000', 'unrealizedProfit': '0.0000',
                                                                  'availableMargin': '0.0000', 'usedMargin': '0.0000', 'freezedMargin': '0.0000'}}},
             'USDT': {'free': 0.0, 'used': 0.0, 'total': 0.0}}
SWAP_LIST = {'info': {'code': 0, 'data': {'balance': [{'asset': 'VST', 'balance': '100000'}, {'asset': 'USDT', 'balance': '42.5', 'equity': '43.0'}]}}}
SWAP_ODD = {'info': {'code': 0, 'data': {'wallets': [{'ccy': 'USDT', 'amt': '9'}]}}}


class Equity(unittest.TestCase):
    def setUp(self):
        fresh_state()
        self._ex, self._p = a.exchange_priv, a.EQUITY_RETRY_PAUSES
        a.EQUITY_RETRY_PAUSES = ()

    def tearDown(self):
        a.exchange_priv, a.EQUITY_RETRY_PAUSES = self._ex, self._p

    def test_extract_on_fixtures(self):
        self.assertEqual(a._extract_equity_ex(SWAP_OK), (15.6128, 'ok'))
        self.assertEqual(a._extract_equity_ex(SWAP_LIST), (43.0, 'ok'))                    # список балансов: берётся USDT, а не VST
        self.assertEqual(a._extract_equity_ex(SWAP_ZERO), (0.0, 'zero'))
        self.assertEqual(a._extract_equity_ex(SWAP_ODD), (0.0, 'unrecognized'))
        self.assertEqual(a._extract_equity_ex({}), (0.0, 'empty'))
        self.assertEqual(a._extract_equity(SWAP_OK), 15.6128)                              # прежний интерфейс

    def test_fetch_equity_reports_zero_wallet_instead_of_empty(self):
        class Ex:
            async def fetch_balance(self_):
                return SWAP_ZERO
        a.exchange_priv = Ex()
        self.assertIsNone(run(a._fetch_equity(retry=False)))
        self.assertIn('баланс фьючерсов USDT-M: 0; проверьте кошелёк', a._dd['last_err'])
        a._dd['fails'] = 1
        self.assertIn('баланс фьючерсов USDT-M: 0; проверьте кошелёк', a.dd_status_text())
        self.assertIn('баланс фьючерсов USDT-M: 0', a.health_status_line())
        self.assertNotIn('пустой ответ', a._dd['last_err'])

    def test_fetch_equity_unrecognized_shows_keys_only_and_ok_case(self):
        class Ex:
            async def fetch_balance(self_):
                return SWAP_ODD
        a.exchange_priv = Ex()
        self.assertIsNone(run(a._fetch_equity(retry=False)))
        self.assertIn('структура ответа баланса не распознана', a._dd['last_err'])
        self.assertIn('wallets', a._dd['last_err'])
        self.assertNotIn('9', a._dd['last_err'].split('(')[1])                             # значения полей в причину не попадают

        class Ok:
            async def fetch_balance(self_):
                return SWAP_OK
        a.exchange_priv = Ok()
        self.assertEqual(run(a._fetch_equity(retry=False)), 15.6128)

    def test_balance_shape_has_keys_and_numbers_but_no_ids(self):
        txt = '\n'.join(a.balance_shape(SWAP_OK))
        for need in ('equity = str 15.6128', 'availableMargin = str 15.6128', 'info.data.balance.asset = str USDT', 'USDT.total = float 15.6128'):
            self.assertIn(need, txt)
        self.assertNotIn('1234567', txt)                                                  # userId пропущен
        txt2 = '\n'.join(a.balance_shape({'data': {'balance': [{'asset': 'USDT', 'balance': '1'}]}}))
        self.assertIn('список из 1', txt2)

    def test_net_probe_balance_text_and_status_versions_line(self):
        t = a.net_probe_balance_text({'bal_ccxt': ['a'], 'bal_direct': ['b <x>']})
        self.assertIn('ccxt fetch_balance', t)
        self.assertIn('&lt;x&gt;', t)
        self.assertEqual(a.net_probe_balance_text({}), '')
        self.assertIn('Python', a.runtime_versions_text())
        src = open(os.path.join(_common.ROOT, 'async_bot.py'), encoding='utf-8').read()
        self.assertIn("lines.append(esc(runtime_versions_text()))", src)                   # версии — в /status одной строкой


def fake_okx_get(calls=None, n_days=250, fund_days=40, bad=False):
    """Фейковый OKX v5: instruments, history-candles (after = «старше ts», новые первыми, ≤100), funding-rate-history (after)."""
    t_last = (D0 + 300) * DAY
    cdl = [(t_last - i * DAY) for i in range(n_days)]                   # новые первыми
    fts = [t_last + DAY - (k + 1) * 8 * 3_600_000 for k in range(fund_days * 3)]

    async def get(path, params):
        if calls is not None:
            calls.append((path, dict(params)))
        if bad:
            return {'code': '50011', 'msg': 'Too Many Requests', 'data': []}
        if path.endswith('/instruments'):
            return {'code': '0', 'data': [
                {'instId': 'NEWQ-USDT-SWAP', 'ctType': 'linear', 'settleCcy': 'USDT', 'state': 'live', 'listTime': str(cdl[-1])},
                {'instId': 'OLDQ-USDT-SWAP', 'ctType': 'linear', 'settleCcy': 'USDT', 'state': 'suspend', 'listTime': ''},
                {'instId': 'BTC-USD-SWAP', 'ctType': 'inverse', 'settleCcy': 'BTC', 'state': 'live', 'listTime': '1'}]}
        if path.endswith('/history-candles'):
            assert params['limit'] <= 100
            after = int(params['after']) if 'after' in params else 10 ** 18
            rows = [t for t in cdl if t < after][:params['limit']]
            return {'code': '0', 'data': [[str(t), '1', '2', '0.5', '1.5', '100', '20000000', '30000000', '1'] for t in rows]}
        if path.endswith('/funding-rate-history'):
            assert params['limit'] <= 100
            after = int(params['after']) if 'after' in params else 10 ** 18
            rows = [t for t in fts if t < after][:params['limit']]
            return {'code': '0', 'data': [{'instId': 'NEWQ-USDT-SWAP', 'fundingRate': '0.0005', 'realizedRate': '0.0001', 'fundingTime': str(t)} for t in rows]}
        return {'code': '1', 'msg': 'нет', 'data': []}
    return get, cdl, fts


class Okx(unittest.TestCase):
    def test_markets_candles_funding_without_ccxt(self):
        calls = []
        get, cdl, fts = fake_okx_get(calls)
        ex = OKX.OkxPublic(get=get, sleep=nosleep, pause=0)
        mk = run(ex.load_markets())
        self.assertEqual(sorted(mk), ['NEWQ/USDT:USDT', 'OLDQ/USDT:USDT'])                 # инверсный BTC-USD исключён
        self.assertEqual(mk['NEWQ/USDT:USDT']['created'], cdl[-1])                          # дата листинга = listTime
        self.assertTrue(mk['NEWQ/USDT:USDT']['active'])
        self.assertFalse(mk['OLDQ/USDT:USDT']['active'])
        self.assertIsNone(mk['OLDQ/USDT:USDT']['created'])
        self.assertEqual(len(X.usdt_swaps(mk)), 2)
        rows = run(ex.fetch_ohlcv('NEWQ/USDT:USDT', '1d', cdl[-1], 1000))
        self.assertEqual(len(rows), 250)                                                   # пагинация назад: 100+100+50
        self.assertEqual([r[0] for r in rows], sorted(cdl))
        self.assertEqual(rows[0][5], 20000000.0)                                                 # объём в базовой монете (volCcy), qv = v·c считает загрузчик
        n_calls = len(calls)
        again = run(ex.fetch_ohlcv('NEWQ/USDT:USDT', '1d', cdl[-1] + 5 * DAY, 1000))
        self.assertEqual(len(calls), n_calls)                                              # следующий since — из кэша, без запросов
        self.assertEqual(len(again), 250 - 5)
        # фандинг: постранично назад, ставка — realizedRate
        ts, r = run(X.fetch_funding(X.Fetcher(ex, pause=0, sleep=nosleep), 'okx', 'NEWQ/USDT:USDT', 0))
        self.assertEqual(len(ts), len(fts))
        self.assertTrue(np.all(np.diff(ts) > 0))
        self.assertAlmostEqual(float(r[0]), 0.0001)
        self.assertTrue(all(p['limit'] <= 100 for path, p in calls if 'funding' in path))
        self.assertTrue(all(path.startswith('/api/v5/') for path, _p in calls))             # только публичные GET v5

    def test_error_codes_raise_and_loader_marks_exchange_unavailable(self):
        get, _c, _f = fake_okx_get(bad=True)
        ex = OKX.OkxPublic(get=get, sleep=nosleep, pause=0)
        with self.assertRaises(OKX.OkxError) as cm:
            run(ex.load_markets())
        self.assertIn('50011', str(cm.exception))
        bdir = tempfile.mkdtemp()
        BF.write_battery(bdir, n_syms=18)
        txt = run(X.run_xload({'xdir': tempfile.mkdtemp(), 'bdir': bdir, 'venues': ('okx',)}, None, exchanges={'okx': ex}, now_ms=(END + 1) * DAY, sleep=nosleep))
        self.assertIn('okx: недоступна', txt)
        self.assertIn('OkxError', txt)

    def test_load_okx_end_to_end_and_depth_line(self):
        get, cdl, fts = fake_okx_get()
        ex = OKX.OkxPublic(get=get, sleep=nosleep, pause=0)
        bdir, xdir = tempfile.mkdtemp(), tempfile.mkdtemp()
        BF.write_battery(bdir, n_syms=18)
        os.environ['HTF_FETCH_PAUSE_SEC'] = '0'
        try:
            txt = run(X.run_xload({'xdir': xdir, 'bdir': bdir, 'venues': ('okx',)}, None, exchanges={'okx': ex}, now_ms=(D0 + 305) * DAY, sleep=nosleep))
        finally:
            os.environ.pop('HTF_FETCH_PAUSE_SEC', None)
        idx = X.load_index(xdir)
        self.assertIn('NEWQ/USDT:USDT', idx['bars']['okx'])
        self.assertEqual(idx['markets']['okx']['NEWQ/USDT:USDT']['created'], cdl[-1])
        b = X.load_bars(xdir, 'okx', 'NEWQ/USDT:USDT')
        self.assertAlmostEqual(float(b['qv'][0]), 20000000.0 * 1.5)                              # qv = v·c
        self.assertIn('OKX: глубина истории фандинга', txt)


class H2Okx(unittest.TestCase):
    def test_universe_includes_okx_and_excludes_binance_coins(self):
        bdir = tempfile.mkdtemp()
        BF.write_battery(bdir, n_syms=18)
        bnorm = X.binance_norm_set(bdir)
        self.assertIn('COIN00', bnorm)
        mk = {'okx': {'NEWA/USDT:USDT': {'base': 'NEWA', 'norm': 'NEWA', 'active': True}, 'COIN00/USDT:USDT': {'base': 'COIN00', 'norm': 'COIN00', 'active': True},
                      'BOTH/USDT:USDT': {'base': 'BOTH', 'norm': 'BOTH', 'active': True}, 'FIRST/USDT:USDT': {'base': 'FIRST', 'norm': 'FIRST', 'active': True}},
              'bingx': {'BOTH/USDT:USDT': {'base': 'BOTH', 'norm': 'BOTH', 'active': True}}}
        bars = {'okx': {'NEWA/USDT:USDT': mk_bars(D0 + 50, 200, 1, qv=5e7), 'COIN00/USDT:USDT': mk_bars(D0 + 60, 200, 2), 'BOTH/USDT:USDT': mk_bars(D0 + 70, 200, 3, qv=9e7),
                        'FIRST/USDT:USDT': mk_bars(D0, 300, 4)},
                'bingx': {'BOTH/USDT:USDT': mk_bars(D0 + 70, 200, 5, qv=1e7)}}
        uni, ref = X.h2_universe(mk, bars, bnorm)
        got = {(u['norm'], u['ex']) for u in uni}
        self.assertIn(('NEWA', 'okx'), got)
        self.assertNotIn('COIN00', {n for n, _e in got})                                     # монета Binance исключена
        self.assertIn(('BOTH', 'okx'), got)                                                  # на обеих биржах — с большим оборотом первых 30 суток (OKX)
        self.assertEqual(sum(1 for n, _e in got if n == 'BOTH'), 1)
        self.assertTrue(any('FIRST' in x for x in ref['pre_existing']))                      # существовавший с начала истории — исключён

    def test_marker_venues_and_okx_funding_zero_counter(self):
        bdir, xdir = tempfile.mkdtemp(), tempfile.mkdtemp()
        BF.write_battery(bdir, n_syms=18)
        write_xdata(xdir)
        idx = X.load_index(xdir)
        idx['markets'].setdefault('okx', {})
        # «OKX» = копия Bybit-кэша с историей фандинга только за последние 25% периода (глубина API ограничена)
        for sym in list(idx['bars']['bybit']):
            b = X.load_bars(xdir, 'bybit', sym)
            X.save_bars(xdir, 'okx', sym, b['ts'], b['o'], b['h'], b['l'], b['c'], b['v'])
            idx['bars']['okx'][sym] = dict(idx['bars']['bybit'][sym])
            idx['markets']['okx'][sym] = dict(idx['markets']['bybit'][sym])
            f = X.load_fund(xdir, 'bybit', sym)
            if f is not None:
                k = int(len(f[0]) * 0.75)
                X.save_fund(xdir, 'okx', sym, f[0][k:], f[1][k:])
                idx['fund']['okx'][sym] = {'first': int(f[0][k]), 'last': int(f[0][-1]), 'n': len(f[0]) - k, 't': 1}
        X.save_index(xdir, idx)
        P, uni, ref = H2R.build_repl_panel(xdir, bdir, ('bingx', 'okx'))
        self.assertTrue(any(u['ex'] == 'okx' for u in uni))
        self.assertFalse(any(u['ex'] == 'bybit' for u in uni))                               # Bybit в вселенную не входит
        self.assertTrue(all(u['norm'] not in X.binance_norm_set(bdir) for u in uni))
        self.assertEqual(sum(1 for u in uni if u['norm'] == 'NEW00'), 1)
        env = H2R.make_env(P)
        nd = P['nd']
        r = H2R.reference(env, np.zeros(nd), RB.fees_of({'fee_taker': 0.0005, 'fee_maker': 0.0002}), uni, np.ones(nd, bool))
        okx = r['by_ex']['okx']
        self.assertGreater(okx['fund_none'] + okx['fund_part'], 0)                           # окна позиций без истории фандинга посчитаны
        self.assertLessEqual(okx['fund_none'], okx['events'])
        # фандинг в симуляции = 0 там, где истории нет: F панели нулевой до первой отметки OKX-символа
        j = [i for i, nm in enumerate(P['syms']) if nm.startswith('okx:')][0]
        ff = int(P['fund_first'][j])
        self.assertGreater(ff, 0)
        self.assertEqual(float(np.abs(P['F'][:ff, j]).sum()), 0.0)


class OldIndex(unittest.TestCase):
    def test_index_from_v118_without_okx_key_is_upgraded(self):
        import json
        d = tempfile.mkdtemp()
        json.dump({'markets': {'bybit': {}, 'bingx': {}}, 'bars': {'bybit': {}, 'bingx': {}}, 'fund': {'bybit': {}, 'bingx': {}}, 'hbars': {'bybit': {}, 'bingx': {}}, 'fail': {}, 't': 5},
                  open(X.index_path(d), 'w'))
        idx = X.load_index(d)
        for k in ('markets', 'bars', 'fund', 'hbars'):
            self.assertEqual(idx[k]['okx'], {})
        self.assertEqual(idx['t'], 5)


class Hints(unittest.TestCase):
    def test_typos_get_a_hint_known_and_far_commands_do_not(self):
        self.assertIn('/h2_replicate', a.command_hint('/h2_replicat'))
        self.assertIn('возможно, вы имели в виду /h2_replicate', a.command_hint('/h2_replicat').lower())
        self.assertIn('/xfund_test', a.command_hint('/xfund_tes load'))
        self.assertIn('/net_probe', a.command_hint('/net_prob'))
        self.assertIn('/h2_replicate', a.command_hint('/h2_replikate'))                      # расстояние 1
        self.assertIsNone(a.command_hint('/h2_replicate load'))
        self.assertIsNone(a.command_hint('/status'))
        self.assertIsNone(a.command_hint('/zzzzzzzz'))                                        # похожей нет — тишина, как раньше
        self.assertIsNone(a.command_hint('просто текст'))
        self.assertIn('/xfund_test load', a.command_hint('/xdata load'))                      # команды /xdata нет (прод-лог)
        self.assertIsNone(a.command_hint('/h2_replicat@some_other_bot'))                      # чужой бот — молчим (v96)
        txt = a._unknown_command_text('/h2_replicat')                                          # ответ диспетчера на неизвестную команду
        self.assertIn('возможно, вы имели в виду /h2_replicate', txt.lower())
        self.assertIn(a.CODE_VERSION, txt)
        self.assertEqual(a._unknown_command_text('/zzzzzzzz'), f'Неизвестная команда /zzzzzzzz. Версия бота: {a.CODE_VERSION}. /help — список команд')
        self.assertEqual(a.levenshtein('kitten', 'sitting'), 3)
        self.assertEqual(a.levenshtein('/h2_replicat', '/h2_replicate'), 1)

    def test_help_and_version(self):
        src = open(os.path.join(_common.ROOT, 'async_bot.py'), encoding='utf-8').read()
        self.assertIn('Порядок: /h2_replicate load → /h2_replicate (без аргумента', src)
        self.assertIn('Порядок: /xfund_test load → /xfund_test (без аргумента', src)
        self.assertIsNotNone(re.fullmatch(r'2026-\d\d-\d\d-v\d+', a.CODE_VERSION))
        self.assertIn("'2026-11-20-v119'", src)                                          # запись v119 в CHANGELOG сохранена

    def test_no_order_methods_in_okx_module(self):
        src = open(os.path.join(_common.ROOT, 'research_okx.py'), encoding='utf-8').read().lower()
        for w in ('create_order', 'cancel_order', '/trade/', 'apikey', 'sign'):
            self.assertNotIn(w, src)


if __name__ == '__main__':
    unittest.main()
