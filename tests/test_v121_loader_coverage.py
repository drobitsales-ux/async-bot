"""v121 (BOT_SPEC §2.53): журнал индекса и продолжение загрузки, отрицательный кэш, проход свечей назад, полнота данных как условие расчёта, эквити KeyError."""
import asyncio
import json
import os
import sqlite3
import tempfile
import unittest

import numpy as np

from _common import a, fresh_state
import battery_fixtures as BF
import research_bdata as B
import research_factory as RF
import research_h2repl as H2R
import research_xdata as X
import research_xfund as XF
from test_v116_h2x1 import D0, END, DAY, mk_bars, mk_fund, nosleep, ohlcv_window, run, x1_inputs
from test_v118_access import BinanceX1
from test_v119_okx_equity import SWAP_ZERO

NOW = (END + 1) * DAY


def mkt(base, active=True, info=None):
    return {'swap': True, 'linear': True, 'quote': 'USDT', 'settle': 'USDT', 'base': base, 'active': active, 'info': info or {}}


class FakeEx:
    """BingX-подобный клиент: свечи — окно «назад от endTime» либо «вперёд от since» (strict_forward имитирует пустое окно для листинга позже окна)."""

    def __init__(self, markets, bars, funding, boom_on=None, perm=(), empty=(), strict_forward=False):
        self.markets, self.bars, self.funding = markets, bars, funding
        self.spy, self.boom_on, self.perm, self.empty, self.strict = [], boom_on, set(perm), set(empty), strict_forward

    async def load_markets(self):
        return self.markets

    async def fetch_ohlcv(self, sym, tf, since=None, limit=None, params=None):
        self.spy.append(('ohlcv', sym, since, dict(params or {})))
        if sym in self.perm:
            raise type('BadSymbol', (Exception,), {})('no such symbol')
        if sym in self.empty:
            return []
        if self.boom_on and sym == self.boom_on[0] and len([1 for k in self.spy if k[0] == 'ohlcv' and k[1] == sym]) == 1:
            raise self.boom_on[1]
        if tf == '1h':
            return ohlcv_window(self.bars[sym + '#h'], since, limit, params) if (sym + '#h') in self.bars else []
        b = self.bars[sym]
        if self.strict and since is not None and int(b['ts'][0]) > since + limit * DAY:
            return []                                                                  # окно [since, since + limit·сутки] без данных
        return ohlcv_window(b, since, limit, params)

    async def fetch_funding_rate_history(self, sym, since=None, limit=None, params=None):
        self.spy.append(('fund', sym))
        ts, r = self.funding[sym]
        end = (params or {}).get('endTime')
        sel = [(int(t), float(x)) for t, x in zip(ts, r) if end is None or t <= end][-limit:]
        return [{'timestamp': t, 'fundingRate': x} for t, x in sel]

    async def close(self):
        return None


def setup_env(n=6, qv=6e7, first=None, nd=None):
    """n символов; по умолчанию ряды заканчиваются в последний день (свежие: повторный запуск их не обновляет)."""
    first = END - 400 if first is None else first
    bdir = tempfile.mkdtemp()
    BF.write_battery(bdir, n_syms=18)
    mk, bars, fund = {}, {}, {}
    for i in range(n):
        sym = f'NEW{i:02d}/USDT:USDT'
        k = nd or (END - (first + i) + 1)
        mk[sym] = mkt(f'NEW{i:02d}')
        bars[sym] = mk_bars(first + i, k, 40 + i, qv=qv)
        fund[sym] = mk_fund(first + i, k, 80 + i)
    return bdir, mk, bars, fund


def load(ex, xdir, bdir, **kw):
    os.environ['HTF_FETCH_PAUSE_SEC'] = '0'
    try:
        return run(X.run_xload(dict({'xdir': xdir, 'bdir': bdir, 'venues': ('bingx',)}, **kw), None, exchanges={'bingx': ex}, now_ms=NOW, sleep=nosleep))
    finally:
        os.environ.pop('HTF_FETCH_PAUSE_SEC', None)


class Journal(unittest.TestCase):
    def test_interrupted_load_resumes_without_reloading_done_symbols(self):
        bdir, mk, bars, fund = setup_env(6)
        xdir = tempfile.mkdtemp()
        syms = sorted(mk)
        ex1 = FakeEx(mk, bars, fund, boom_on=(syms[3], asyncio.CancelledError()))        # BaseException — как обрыв процесса: ни save_index, ни отчёта
        with self.assertRaises(asyncio.CancelledError):
            load(ex1, xdir, bdir)
        idx = X.load_index(xdir)                                                           # «перезапуск»: индекс читается заново с диска
        self.assertEqual(sorted(idx['bars']['bingx']), syms[:3])                           # три готовых символа в индексе (из журнала), а не 0
        ex2 = FakeEx(mk, bars, fund)
        txt = load(ex2, xdir, bdir)
        asked = [k[1] for k in ex2.spy if k[0] == 'ohlcv']
        self.assertEqual(sorted(set(asked)), syms[3:])                                     # готовые символы не загружаются повторно
        self.assertEqual([x for k, x in enumerate(asked) if k == 0 or asked[k - 1] != x], syms[3:])      # порядок — сортировка (по два запроса: страница + подтверждение конца истории)
        self.assertIn('свечи: загружено 6 из 6, осталось 0', txt)
        self.assertIn('фандинг: загружено 6 из 6, осталось 0', txt)
        self.assertEqual(txt.splitlines()[-1], '✅ загрузка завершена')                      # отдельной последней строкой
        ex3 = FakeEx(mk, bars, fund)
        txt3 = load(ex3, xdir, bdir)
        self.assertEqual(ex3.spy, [])                                                      # полностью загружено — запросов нет
        self.assertEqual(txt3.splitlines()[-1], '✅ загрузка завершена')

    def test_torn_journal_line_is_ignored_and_snapshot_is_atomic(self):
        d = tempfile.mkdtemp()
        idx = X.load_index(d)
        X.put(d, idx, 'bars', 'bingx', 'A/USDT:USDT', {'first': 1, 'last': 2, 'n': 2, 't': 3, 'complete': True})
        with open(X.journal_path(d), 'a', encoding='utf-8') as f:
            f.write('["bars", "bingx", "B/USDT:USDT", {"first": 1')                        # оборванная запись
        got = X.load_index(d)
        self.assertEqual(sorted(got['bars']['bingx']), ['A/USDT:USDT'])
        X.save_index(d, got)
        self.assertEqual(sorted(json.load(open(X.index_path(d)))['bars']['bingx']), ['A/USDT:USDT'])
        self.assertFalse([f for f in os.listdir(d) if f.endswith('.tmp')])                  # временных файлов не остаётся
        X.compact_index(d)
        self.assertEqual(open(X.journal_path(d)).read(), '')
        self.assertEqual(sorted(X.load_index(d)['bars']['bingx']), ['A/USDT:USDT'])

    def test_two_runs_writing_different_symbols_do_not_lose_each_other(self):
        d = tempfile.mkdtemp()
        i1, i2 = X.load_index(d), X.load_index(d)                                          # два процесса с устаревшими снимками в памяти
        X.put(d, i1, 'bars', 'bingx', 'A/USDT:USDT', {'first': 1, 'last': 2, 'n': 2, 't': 3, 'complete': True})
        X.put(d, i2, 'bars', 'bingx', 'B/USDT:USDT', {'first': 1, 'last': 2, 'n': 2, 't': 3, 'complete': True})
        self.assertEqual(sorted(X.load_index(d)['bars']['bingx']), ['A/USDT:USDT', 'B/USDT:USDT'])


class Negative(unittest.TestCase):
    def test_permanent_errors_and_empty_are_cached_and_cost_no_retry_delay(self):
        bdir, mk, bars, fund = setup_env(4)
        syms = sorted(mk)
        xdir = tempfile.mkdtemp()
        ex = FakeEx(mk, bars, fund, perm=[syms[0]], empty=[syms[1]])
        sleeps = []

        async def sl(x):
            sleeps.append(x)
        os.environ['HTF_FETCH_PAUSE_SEC'] = '0'
        try:
            txt = run(X.run_xload({'xdir': xdir, 'bdir': bdir, 'venues': ('bingx',)}, None, exchanges={'bingx': ex}, now_ms=NOW, sleep=sl))
        finally:
            os.environ.pop('HTF_FETCH_PAUSE_SEC', None)
        self.assertFalse([x for x in sleeps if x >= 2])                                    # постоянная ошибка не ждёт 2/6/15 с
        self.assertIn('свечи: загружено 2 из 4, осталось 0; подтверждённо пусто/постоянная ошибка: 2', txt)
        self.assertEqual(txt.splitlines()[-1], '✅ загрузка завершена')
        ex2 = FakeEx(mk, bars, fund, perm=[syms[0]], empty=[syms[1]])
        load(ex2, xdir, bdir)
        self.assertEqual([k for k in ex2.spy if k[1] in syms[:2]], [])                    # повторно не запрашиваются

    def test_transient_error_keeps_symbol_as_remaining(self):
        bdir, mk, bars, fund = setup_env(3)
        syms = sorted(mk)
        xdir = tempfile.mkdtemp()
        net = type('RequestTimeout', (type('NetworkError', (Exception,), {}),), {})
        ex = FakeEx(mk, bars, fund)
        n = {'k': 0}
        orig = ex.fetch_ohlcv

        async def flaky(sym, tf, since=None, limit=None, params=None):
            if sym == syms[1]:
                n['k'] += 1
                raise net('timeout')
            return await orig(sym, tf, since, limit, params)
        ex.fetch_ohlcv = flaky
        txt = load(ex, xdir, bdir)
        self.assertEqual(n['k'], 4)                                                        # транзиентная ошибка повторяется (1 + 3 попытки)
        self.assertIn('осталось 1', txt)
        self.assertIn('⏳ загрузка не завершена', txt)
        self.assertNotIn('✅ загрузка завершена', txt)


class Backward(unittest.TestCase):
    def test_late_listing_loaded_even_if_forward_window_would_be_empty(self):
        bdir, mk, bars, fund = setup_env(2, first=D0 + 900, nd=250)
        xdir = tempfile.mkdtemp()
        ex = FakeEx(mk, bars, fund, strict_forward=True)
        load(ex, xdir, bdir)
        idx = X.load_index(xdir)
        self.assertEqual(sorted(idx['bars']['bingx']), sorted(mk))                         # проход назад не зависит от даты листинга
        for sym in mk:
            self.assertTrue(idx['bars']['bingx'][sym]['complete'])
            self.assertEqual(idx['bars']['bingx'][sym]['n'], 250)
        self.assertTrue(all(k[2] is None for k in ex.spy if k[0] == 'ohlcv'))               # since=None, граница — endTime
        ts, *_r = run(X.fetch_bars_back(X.Fetcher(FakeEx({}, {'S': mk_bars(D0, 1500, 3)}, {}, strict_forward=True), pause=0, sleep=nosleep), 'bingx', 'S', X.SINCE_MS, NOW))
        self.assertEqual(len(ts), 1500)                                                    # страницы по 1000 назад: 1000 + 500 + пустая

    def test_funding_goes_to_earliest_event_and_reuses_htf_cache(self):
        bdir, mk, bars, fund = setup_env(1)
        sym = sorted(mk)[0]
        xdir = tempfile.mkdtemp()
        db = os.path.join(tempfile.mkdtemp(), 'trades.db')
        con = sqlite3.connect(db)
        con.execute('CREATE TABLE htf_funding (symbol TEXT, ts INTEGER, rate REAL, PRIMARY KEY (symbol, ts))')
        t0 = int(bars[sym]['ts'][0]) - 40 * DAY                                            # кэш v86–v88 глубже, чем отдаёт API
        con.executemany('INSERT INTO htf_funding VALUES (?,?,?)', [(sym, t0 + i * 8 * 3_600_000, 0.0002) for i in range(30)])
        con.commit()
        con.close()
        ex = FakeEx(mk, bars, fund)
        load(ex, xdir, bdir, htf_db=db)
        ts, r = X.load_fund(xdir, 'bingx', sym)
        self.assertEqual(int(ts[0]), t0)                                                   # глубина — из кэша
        self.assertEqual(len(ts), len(np.unique(ts)))                                      # без дублей
        self.assertEqual(len(ts), len(fund[sym][0]) + 30)
        self.assertTrue(X.load_index(xdir)['fund']['bingx'][sym]['complete'])


class X1Universe(unittest.TestCase):
    def test_all_common_symbols_loaded_without_liquidity_prefilter(self):
        bdir = tempfile.mkdtemp()
        BF.write_battery(bdir, n_syms=18)
        bn = X.binance_norm_set(bdir)
        self.assertTrue({'COIN00', 'COIN01'} <= bn)
        mk = {'COIN00/USDT:USDT': mkt('COIN00'), 'COIN01/USDT:USDT': mkt('COIN01')}
        bars = {s: mk_bars(D0 + 100, 300, 5 + i, qv=1e6) for i, s in enumerate(mk)}        # оборот 1 млн ≪ 20 млн
        fund = {s: mk_fund(D0 + 100, 300, 9 + i) for i, s in enumerate(mk)}
        ex = FakeEx(mk, bars, fund)
        xdir = tempfile.mkdtemp()
        txt = load(ex, xdir, bdir, x1_main=True)
        idx = X.load_index(xdir)
        self.assertEqual(sorted(idx['fund']['bingx']), sorted(mk))                          # фандинг — для ВСЕХ общих символов
        self.assertIn('без предфильтра ликвидности', txt)


class Coverage(unittest.TestCase):
    def _idx(self, n=20, with_bars=20, with_fund=20, complete=True):
        d = tempfile.mkdtemp()
        bdir = tempfile.mkdtemp()
        BF.write_battery(bdir, n_syms=18)
        idx = X.load_index(d)
        for i in range(n):
            sym = f'S{i:02d}/USDT:USDT'
            idx['markets']['bingx'][sym] = {'base': f'S{i:02d}', 'norm': f'S{i:02d}', 'active': True, 'created': None, 'excl': None}
            if i < with_bars:
                idx['bars']['bingx'][sym] = {'first': 1, 'last': 2, 'n': 2, 't': 3, 'complete': complete}
            if i < with_fund:
                idx['fund']['bingx'][sym] = {'first': 1, 'last': 2, 'n': 2, 't': 3, 'complete': complete}
        X.save_index(d, idx)
        return d, bdir

    def test_report_numbers_and_threshold(self):
        d, bdir = self._idx(20, 2, 2)
        c = X.coverage_report(d, 'h2', bdir, venues=('bingx',))
        self.assertFalse(c['ok'])
        self.assertIn('свечи: 2 из 20 (10%) < 90%', '; '.join(c['lines']))
        d, bdir = self._idx(20, 19, 19)
        self.assertTrue(X.coverage_report(d, 'h2', bdir, venues=('bingx',))['ok'])        # 95% ≥ 90%
        d, bdir = self._idx(20, 20, 20, complete=False)
        c = X.coverage_report(d, 'h2', bdir, venues=('bingx',))
        self.assertFalse(c['ok'])                                                          # свечи и фандинг есть, но не доведены до начала истории
        self.assertIn('глубина', '; '.join(c['lines']))

    def test_expected_excludes_noncrypto_and_inactive_and_okx_depth_exempt(self):
        d, bdir = self._idx(10, 10, 10)
        idx = X.load_index(d)
        idx['markets']['bingx']['NCSKX/USDT:USDT'] = {'base': 'NCSKX', 'norm': 'NCSKX', 'active': True, 'excl': 'NC-префикс'}
        idx['markets']['bingx']['OLD/USDT:USDT'] = {'base': 'OLD', 'norm': 'OLD', 'active': False, 'excl': None}
        idx['markets']['okx'] = {'OK/USDT:USDT': {'base': 'OK', 'norm': 'OK', 'active': True, 'excl': None}}
        idx['bars']['okx'] = {'OK/USDT:USDT': {'first': 1, 'last': 2, 'n': 2, 't': 3, 'complete': True}}
        idx['fund']['okx'] = {'OK/USDT:USDT': {'first': 1, 'last': 2, 'n': 2, 't': 3, 'complete': False}}         # API OKX: ≤ 92 суток — глубина не требуется
        X.save_index(d, idx)
        c = X.coverage_report(d, 'h2', bdir, venues=('bingx', 'okx'))
        self.assertEqual(c['expected'], 11)                                                # 10 + OKX; NC и неактивный — не в ожидаемых
        self.assertTrue(c['ok'], c['lines'])

    def test_h2_refuses_without_calculation_below_90(self):
        d, bdir = self._idx(20, 2, 2)
        fdir = tempfile.mkdtemp()
        RF.kv_set('h2_funding_check', {'comb': 1.0, 'allow': True, 't': 0}, fdir)
        for mode in ('show', 'confirm'):
            txt = run(H2R.run_h2repl({'xdir': d, 'bdir': bdir, 'fdir': fdir, 'fee_taker': 0.0005, 'fee_maker': 0.0002, 'version': 'vT', 'venues': ('bingx',), 'mode': mode}))
            self.assertIn('⛔ /h2_replicate отказ: данные неполны', txt)
            self.assertIn('свечи: 2 из 20', txt)
            self.assertNotIn('k =', txt)                                                  # ни k, ни доходностей, ни вердикта

    def test_x1_refuses_without_calculation_below_90(self):
        d = tempfile.mkdtemp()
        bdir = tempfile.mkdtemp()
        BF.write_battery(bdir, n_syms=18)
        idx = X.load_index(d)
        for i in range(10):
            sym = f'COIN{i:02d}/USDT:USDT'
            idx['markets']['bingx'][sym] = {'base': f'COIN{i:02d}', 'norm': f'COIN{i:02d}', 'active': True, 'excl': None}
        for i in range(2):
            sym = f'COIN{i:02d}/USDT:USDT'
            b = mk_bars(D0 + 100, 300, 5 + i)
            X.save_bars(d, 'bingx', sym, b['ts'], b['o'], b['h'], b['l'], b['c'], b['v'])
            idx['bars']['bingx'][sym] = {'first': int(b['ts'][0]), 'last': int(b['ts'][-1]), 'n': 300, 't': 1, 'complete': True}
        X.save_index(d, idx)
        txt = run(XF.run_xfund({'xdir': d, 'bdir': bdir, 'fdir': tempfile.mkdtemp(), 'fee_taker': 0.0005, 'version': 'vT', 'x1_pair': 'binance'}))
        self.assertIn('⛔ /xfund_test отказ: данные неполны', txt)
        self.assertNotIn('APR', txt)


class HourlyForTraded(unittest.TestCase):
    def test_prelim_traded_then_hourly_loaded_and_coverage_full(self):
        nd, day0 = 120, 20000
        rts = np.sort(np.concatenate([(day0 + np.arange(nd)) * DAY + h * B.HOUR_MS + 1000 for h in (1, 9, 17)])).astype(np.int64)
        bdir, _d0, _nd, _base = BinanceX1('test_binance_funding_read_by_actual_timestamps')._archive(rts, np.zeros(len(rts)))
        vA, _vB, _p = x1_inputs(nd=nd, rate_a=0.0007, rate_b=0.0)
        sym = 'NEWA/USDT:USDT'
        xdir = tempfile.mkdtemp()
        b, hb, ev = vA['bars']['sa0'], vA['hbars']['sa0'], vA['fund']['sa0']
        X.save_bars(xdir, 'bingx', sym, b['ts'], b['o'], b['h'], b['l'], b['c'], b['qv'] / b['c'])
        X.save_fund(xdir, 'bingx', sym, ev[0], ev[1])
        idx = X.load_index(xdir)
        idx['markets']['bingx'][sym] = {'base': 'NEWA', 'norm': 'NEWA', 'active': True, 'excl': None}
        idx['bars']['bingx'][sym] = {'first': int(b['ts'][0]), 'last': int(b['ts'][-1]), 'n': nd, 't': 1, 'complete': True}
        idx['fund']['bingx'][sym] = {'first': int(ev[0][0]), 'last': int(ev[0][-1]), 'n': len(ev[0]), 't': 1, 'complete': True}
        X.save_index(xdir, idx)
        traded, npairs = XF.prelim_traded(xdir, bdir, 0.0005)
        self.assertEqual(npairs, 1)
        self.assertEqual(traded, [('NEWA', sym)])                                          # высокая ставка на BingX → пара торгуется на дневных данных
        spy = FakeEx({sym: mkt('NEWA')}, {sym + '#h': dict(hb, o=hb['c'], v=np.ones(len(hb['ts'])))}, {})
        txt = run(XF.load_traded_hourly({'xdir': xdir, 'bdir': bdir, 'fee_taker': 0.0005}, None, {'bingx': spy}, now_ms=(day0 + nd + 1) * DAY, sleep=nosleep))
        self.assertIn('загружено 1 из 1, осталось 0', txt)
        got = X.load_index(xdir)['hbars']['bingx'][sym]
        self.assertTrue(got['complete'])
        self.assertEqual(got['n'], nd * 24)
        self.assertTrue(all(k[3].get('endTime') is None or k[3]['endTime'] > 0 for k in spy.spy))
        cov = X.coverage_report(xdir, 'x1', bdir, hourly_syms=[('bingx', sym)])
        self.assertTrue(cov['ok'], cov['lines'])
        self.assertIn('1h по символам сделок (загружены или подтверждённо пусты; ошибка/не обработано — гейт): 1 из 1', '; '.join(cov['lines']))
        # повторный запуск — запросов нет
        spy2 = FakeEx({sym: mkt('NEWA')}, {sym + '#h': dict(hb, o=hb['c'], v=np.ones(len(hb['ts'])))}, {})
        run(XF.load_traded_hourly({'xdir': xdir, 'bdir': bdir, 'fee_taker': 0.0005}, None, {'bingx': spy2}, now_ms=(day0 + nd + 1) * DAY, sleep=nosleep))
        self.assertEqual(spy2.spy, [])


NET_PROBE_BALANCE = {'code': 0, 'msg': '', 'data': {'balance': {'userId': '1', 'asset': 'USDT', 'balance': '0.0000', 'equity': '0.0000', 'unrealizedProfit': '0.0000',
                                                                    'realisedProfit': '0.0000', 'availableMargin': '0.0000', 'usedMargin': '0.0000', 'freezedMargin': '0.0000'}}}


class EquityKeyError(unittest.TestCase):
    def setUp(self):
        self.db, self.sent = fresh_state()
        self._ex, self._p, self._db = a.exchange_priv, a.EQUITY_RETRY_PAUSES, a._direct_balance
        self._pos, self._act = a.all_positions, a.active_strategies
        a.EQUITY_RETRY_PAUSES = ()
        a._dd.update({'last_err': '', 'last_status': '', 'empty_account': False})
        a.all_positions = lambda: []
        a.active_strategies = lambda: ([], [])

    def tearDown(self):
        a.exchange_priv, a.EQUITY_RETRY_PAUSES, a._direct_balance = self._ex, self._p, self._db
        a.all_positions, a.active_strategies = self._pos, self._act

    def test_real_response_from_net_probe_through_ccxt_parser(self):
        import ccxt.async_support as cx
        ex = cx.bingx({'options': {'defaultType': 'swap'}})
        try:
            bal = ex.parse_balance(NET_PROBE_BALANCE)                                      # настоящий разбор ccxt на ответе /net_probe 2026-11-19
        finally:
            run(ex.close())
        self.assertEqual(a._extract_equity_ex(bal), (0.0, 'zero'))

    def test_keyerror_from_ccxt_falls_back_to_direct_request_and_stays_quiet(self):
        class Ex:
            async def fetch_balance(self_):
                raise KeyError(6)

        async def direct():
            return NET_PROBE_BALANCE
        a.exchange_priv, a._direct_balance = Ex(), direct
        for _ in range(4):
            self.assertFalse(run(a.dd_update()))
        self.assertTrue(a._dd['empty_account'])
        self.assertEqual(a._dd['fails'], 0)
        self.assertEqual(self.sent, [])
        self.assertNotIn('KeyError', a.dd_status_text() + a.status_text().split('Данные')[0])
        self.assertIn('счёт пуст — режим ожидания', a.status_text())

    def test_keyerror_with_direct_request_failing_shows_place_and_alarms(self):
        class Ex:
            async def fetch_balance(self_):
                {}[6]

        async def direct():
            raise RuntimeError('HTTP 500 код None')
        a.exchange_priv, a._direct_balance = Ex(), direct
        for _ in range(3):
            run(a.dd_update())
        self.assertEqual(a._dd['fails'], 3)
        self.assertRegex(a._dd['last_err'], r'KeyError 6 @ test_v121_loader_coverage\.py:\d+ fetch_balance')
        self.assertIn('прямой запрос: RuntimeError HTTP 500', a._dd['last_err'])
        self.assertTrue(any('3 сбоя подряд' in t and 'KeyError' in t for t in self.sent))

    def test_direct_balance_with_funds_returns_equity(self):
        class Ex:
            async def fetch_balance(self_):
                raise KeyError(6)

        async def direct():
            return {'code': 0, 'data': {'balance': {'asset': 'USDT', 'balance': '12.0', 'equity': '12.5', 'unrealizedProfit': '0.5'}}}
        a.exchange_priv, a._direct_balance = Ex(), direct
        self.assertTrue(run(a.dd_update()))
        self.assertEqual(a._dd['equity'], 12.5)
        self.assertFalse(a._dd['empty_account'])

    def test_status_error_counter_shows_last_message(self):
        a._health_errs.clear()
        a._health_err_sample.clear()
        for k in range(6):
            a._health_note_error(f'[DD] update fail: KeyError: {k} @ async_bot.py:1 f')
        line = a.health_status_line()
        self.assertIn('ошибки: KeyError:6', line)
        self.assertIn('(последнее: [DD] update fail: KeyError: 5 @ async_bot.py:1 f)', line)           # откуда «KeyError:6» — видно в /status
        a._health_errs.clear()
        a._health_err_sample.clear()

    def test_zero_balance_does_not_count_as_error(self):
        class Ex:
            async def fetch_balance(self_):
                return SWAP_ZERO
        a._health_errs.clear()
        a.exchange_priv = Ex()
        for _ in range(3):
            run(a.dd_update())
        self.assertEqual(a._health_errs, {})                                                # «баланс 0» пишется на уровне INFO — счётчик ошибок не растёт

    def test_exc_where_has_no_paths(self):
        try:
            {}[1]
        except KeyError as e:
            w = a.exc_where(e)
        self.assertRegex(w, r'^test_v121_loader_coverage\.py:\d+ test_exc_where_has_no_paths$')
        self.assertNotIn('/', w)


if __name__ == '__main__':
    unittest.main()
