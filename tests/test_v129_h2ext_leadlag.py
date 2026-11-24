"""v129: H2 на новых биржах (H2EXT) и H2F (§2.59), клиенты Gate.io/MEXC/Bitget без ccxt, проба доступа, замер расхождений цен OKX↔BingX (§2.60). Сеть не используется."""
import asyncio
import os
import re
import tempfile
import unittest

import numpy as np

import _common
import battery_fixtures as BF
import research_battery as RB
import research_bsim as S
import research_factory as RF
import research_h2ext as HX
import research_h2repl as H2R
import research_leadlag as LL
import research_venues as V
import research_xdata as X
import access_probe as AP
import test_v116_h2x1 as T
from _common import a

ROOT = _common.ROOT
DAY = X.DAY
FEE = {'taker': 0.0005, 'maker': 0.0002}


def run(c):
    return asyncio.run(c)


async def nosleep(_s):
    return None


# ── клиенты бирж на фикстурах ────────────────────────────────────────────────────────────────────────
NOW = int(np.datetime64('2025-01-01', 'ms').astype(np.int64))


def gate_get(path, params):
    if path.endswith('/contracts'):
        return [{'name': 'BTC_USDT', 'type': 'direct', 'create_time': 1573000000, 'quanto_multiplier': '0.0001', 'in_delisting': False},
                {'name': 'ABC_USDT', 'type': 'direct', 'create_time': 1700000000, 'quanto_multiplier': '1', 'in_delisting': True},
                {'name': 'BTC_USD', 'type': 'inverse'}]
    if path.endswith('/candlesticks'):
        lo, hi = int(params['from']), int(params['to'])
        rows = []
        t = (lo // 86400 + 1) * 86400
        while t <= hi:
            rows.append({'t': t, 'o': '1', 'h': '2', 'l': '0.5', 'c': '1.5', 'v': '10', 'sum': '1234.5'})
            t += 86400
        return rows
    if path.endswith('/funding_rate'):
        return [{'t': 1_700_000_000 + 28800 * k, 'r': '0.0001'} for k in range(5)]
    raise AssertionError(path)


def mexc_get(path, params):
    if path.endswith('/detail'):
        return {'success': True, 'code': 0, 'data': [{'symbol': 'BTC_USDT', 'baseCoin': 'BTC', 'quoteCoin': 'USDT', 'settleCoin': 'USDT', 'state': 0, 'createTime': 1600000000000},
                                                     {'symbol': 'ETH_USD', 'baseCoin': 'ETH', 'quoteCoin': 'USD', 'settleCoin': 'ETH', 'state': 0}]}
    if '/kline/' in path:
        lo, hi = int(params['start']), int(params['end'])
        ts = list(range((lo // 86400 + 1) * 86400, hi + 1, 86400))
        return {'success': True, 'code': 0, 'data': {'time': ts, 'open': [1.0] * len(ts), 'close': [1.5] * len(ts), 'high': [2.0] * len(ts), 'low': [0.5] * len(ts), 'vol': [1] * len(ts), 'amount': [99.0] * len(ts)}}
    if path.endswith('/funding_rate/history'):
        page = int(params['page_num'])
        rows = [{'symbol': 'BTC_USDT', 'fundingRate': 0.0001 * (page * 100 - k), 'settleTime': 1_700_000_000_000 + 28_800_000 * (page * 100 - k)} for k in range(100)] if page <= 2 else []
        return {'success': True, 'code': 0, 'data': {'resultList': rows, 'totalPage': 2, 'currentPage': page}}
    raise AssertionError(path)


def bitget_get(path, params):
    if path.endswith('/contracts'):
        return {'code': '00000', 'data': [{'symbol': 'BTCUSDT', 'baseCoin': 'BTC', 'quoteCoin': 'USDT', 'symbolType': 'perpetual', 'launchTime': '1600000000000', 'symbolStatus': 'normal'},
                                          {'symbol': 'BTCUSD', 'baseCoin': 'BTC', 'quoteCoin': 'USD', 'symbolType': 'perpetual'}]}
    if path.endswith('/history-candles'):
        lo, hi = int(params['startTime']), int(params['endTime'])
        ts = list(range(((lo // DAY) + 1) * DAY, hi + 1, DAY))[:int(params['limit'])]
        return {'code': '00000', 'data': [[str(t), '1', '2', '0.5', '1.5', '10', '777'] for t in ts]}
    if path.endswith('/history-fund-rate'):
        page = int(params['pageNo'])
        rows = [{'symbol': 'BTCUSDT', 'fundingRate': '0.0001', 'fundingTime': str(1_700_000_000_000 + 28_800_000 * (page * 100 - k))} for k in range(100)] if page <= 2 else []
        return {'code': '00000', 'data': rows}
    raise AssertionError(path)


class Venues(unittest.TestCase):
    def mk(self, cls, get):
        async def aget(path, params):
            return get(path, params)
        return cls(get=aget, sleep=nosleep, pause=0.0, now=lambda: NOW)

    def test_markets_filtering_and_fields(self):
        g = run(self.mk(V.GatePublic, gate_get).load_markets())
        self.assertEqual(set(g), {'BTC/USDT:USDT', 'ABC/USDT:USDT'})
        self.assertTrue(g['BTC/USDT:USDT']['active'] and not g['ABC/USDT:USDT']['active'])
        self.assertEqual(g['BTC/USDT:USDT']['created'], 1573000000 * 1000)
        m = run(self.mk(V.MexcPublic, mexc_get).load_markets())
        self.assertEqual(list(m), ['BTC/USDT:USDT'])
        b = run(self.mk(V.BitgetPublic, bitget_get).load_markets())
        self.assertEqual(list(b), ['BTC/USDT:USDT'])
        for mk in (g, m, b):                                                     # те же поля, что у ccxt-markets для usdt_swaps
            sw = X.usdt_swaps(mk)
            self.assertTrue(sw and all(v['base'] and v['norm'] for v in sw.values()))

    def test_bars_pages_backward_through_fetch_bars_back_and_qv(self):
        for cls, get in ((V.GatePublic, gate_get), (V.MexcPublic, mexc_get), (V.BitgetPublic, bitget_get)):
            ex = self.mk(cls, get)
            run(ex.load_markets())
            f = X.Fetcher(ex, sleep=nosleep, pause=0.0)
            since = NOW - 420 * DAY
            ts, o, h, l, c, v, comp = run(X.fetch_bars_back(f, cls.name, 'BTC/USDT:USDT', since, NOW))
            self.assertTrue(len(ts) >= 400, (cls.name, len(ts)))
            self.assertTrue(np.all(np.diff(ts) == DAY))
            self.assertTrue(comp or ts[0] <= since + DAY)
            self.assertTrue(np.all(np.isfinite(c)))
        g = self.mk(V.GatePublic, gate_get)
        run(g.load_markets())
        rows = run(g.fetch_ohlcv('BTC/USDT:USDT', '1d', None, 1000, {'endTime': NOW}))
        self.assertEqual(rows[0][5], 1234.5)                                     # оборот — поле sum (USDT)

    def test_funding_history_slices_by_endtime(self):
        for cls, get in ((V.GatePublic, gate_get), (V.MexcPublic, mexc_get), (V.BitgetPublic, bitget_get)):
            ex = self.mk(cls, get)
            run(ex.load_markets())
            f = X.Fetcher(ex, sleep=nosleep, pause=0.0)
            ts, r, comp = run(X.fetch_funding_ex(f, cls.name, 'BTC/USDT:USDT', 0))
            self.assertTrue(len(ts) >= 5, cls.name)
            self.assertTrue(comp)
            self.assertTrue(np.all(np.diff(ts) > 0))

    def test_http_errors_classified(self):
        e429 = V.VenueError('gateio HTTP 429 too many requests: x')
        e503 = V.VenueError('mexc HTTP 503: y')
        e400 = V.VenueError('bitget HTTP 400: bad symbol')
        self.assertTrue(X.is_transient(e429) and X.is_transient(e503))
        self.assertFalse(X.is_transient(e400))
        self.assertTrue(X.is_rate_limit(e429))
        with self.assertRaises(V.VenueError):                                    # неожиданный ответ — ошибка с текстом, не молчание
            run(self.mk(V.MexcPublic, lambda p, q: {'success': False, 'code': 600, 'message': 'x'}).load_markets())

    def test_no_orders_no_keys_in_sources(self):
        bad = re.compile(r'create_order|createOrder|cancel_order|cancelOrder|\.transfer\(|\.withdraw\(|edit_order|apiKey|secret|signature', re.I)
        for f in ('research_venues.py', 'research_h2ext.py', 'research_leadlag.py'):
            self.assertIsNone(bad.search(open(os.path.join(ROOT, f), encoding='utf-8').read()), f)
        src = open(os.path.join(ROOT, 'async_bot.py'), encoding='utf-8').read()
        seg = src[src.index('async def _ll_launch'):src.index('async def exec_check_cmd')]
        self.assertIsNone(re.search(r'create_order|create_market|create_limit|exchange_priv|fetch_balance', seg))


# ── проба доступа ──────────────────────────────────────────────────────────────────────────────────
class Probe(unittest.TestCase):
    def test_new_exchanges_in_probes_and_classification(self):
        for ex in ('gateio', 'mexc', 'bitget'):
            self.assertIn(ex, AP.PROBES)
        self.assertNotIn('gateio', AP.CCXT_IDS)
        ok = {'gateio': '{"name":"BTC_USDT","type":"direct","quanto_multiplier":"0.0001"', 'mexc': '{"success":true,"code":0,"data":{"symbol":"BTC_USDT"',
              'bitget': '{"code":"00000","msg":"success","requestTime":1,"data":[{"symbol":"BTCUSDT"'}
        for ex, body in ok.items():
            self.assertEqual(AP.classify_http({'status': 200, 'headers': {}, 'body': body}), 'доступно', ex)
        self.assertEqual(AP.classify_http({'status': 403, 'headers': {'server': 'CloudFront'}, 'body': 'block access from your country'}), 'гео-блок')
        self.assertEqual(AP.classify_http({'status': 429, 'headers': {}, 'body': ''}), 'лимит частоты')
        self.assertEqual(AP.classify_http({'status': 200, 'headers': {}, 'body': '{"label":"INVALID","message":"x"}'}), 'не определено')

    def test_probe_all_default_and_no_ccxt_for_new(self):
        calls = []

        async def fake_ccxt(ex_id):
            calls.append(ex_id)
            return {'dt': 0.0, 'ok': True, 'exc': None, 'n': 1}

        class R:
            def __init__(self, st, body):
                self.status, self._b, self.headers = st, body, {}

            async def read(self):
                return self._b.encode()

            async def __aenter__(self):
                return self

            async def __aexit__(self, *x):
                return False

        class Sess:
            def get(self, url, timeout=None):
                return R(200, '{"code":"00000","success":true,"name":"BTC_USDT"}')
        res = run(AP.probe_all(session=Sess(), ccxt_fn=fake_ccxt))
        self.assertEqual(set(res) - {'_error'}, {'bybit', 'okx', 'gateio', 'mexc', 'bitget'})
        self.assertEqual(sorted(calls), ['bybit', 'okx'])
        txt = '\n'.join(AP.format_probe(res))
        self.assertIn('прямой REST без ccxt', txt)
        for ex in ('gateio', 'mexc', 'bitget'):
            self.assertEqual(res[ex]['verdict'], 'доступно')
        self.assertIn('Gate.io', a.net_probe_access_text({'access': res}))


# ── H2EXT / H2F ─────────────────────────────────────────────────────────────────────────────────────
D0, END = T.D0, T.END


def write_xdata_ext(d, n_new=36):
    """BingX и OKX — «старые» (их данные видели), Gate.io и MEXC — новые. DUP00 есть и на BingX, и на Gate.io (оборот Gate выше) — для H2F не годится (база видна на BingX)."""
    idx = X.load_index(d)
    mk = {ex: {} for ex in X.EXCHANGES}
    specs = [('bingx', 'OLD/USDT:USDT', 'OLD', D0, END - D0, 1, 5e7, 0.05), ('gateio', 'OLD/USDT:USDT', 'OLD', D0, END - D0, 2, 5e7, 0.05)]
    for i in range(n_new):
        fd = D0 + 60 + int(i * 1100 / n_new)
        ex = ('bingx', 'okx', 'gateio', 'mexc')[i % 4]
        specs.append((ex, f'NEW{i:02d}/USDT:USDT', f'NEW{i:02d}', fd, min(900, END - fd), 10 + i, 6e7, 0.07))
    specs.append(('bingx', 'DUP00/USDT:USDT', 'DUP00', D0 + 100, 900, 91, 2e7, 0.07))
    specs.append(('gateio', 'DUP00/USDT:USDT', 'DUP00', D0 + 100, 900, 92, 9e7, 0.07))
    for ex, sym, base, fd, n, seed, qv, vol in specs:
        bars = T.mk_bars(fd, n, seed, vol=vol, qv=qv)
        X.save_bars(d, ex, sym, bars['ts'], bars['o'], bars['h'], bars['l'], bars['c'], bars['v'])
        idx['bars'][ex][sym] = {'first': int(bars['ts'][0]), 'last': int(bars['ts'][-1]), 'n': int(n), 't': 1, 'complete': True}
        if not (ex == 'mexc' and (seed // 4) % 2 == 0):                              # часть символов MEXC — без истории фандинга (счётчик «фандинг = 0»)
            ft, fr = T.mk_fund(fd, n, seed + 500, level=(-0.0002 if (ex == 'gateio' and (seed // 4) % 2 == 0) else 0.0002))      # часть Gate.io — отрицательный фандинг
            X.save_fund(d, ex, sym, ft, fr)
            idx['fund'][ex][sym] = {'first': int(ft[0]), 'last': int(ft[-1]), 'n': len(ft), 't': 1, 'complete': True}
        else:
            idx['neg'][f'{ex}|{sym}|fund'] = {'t': 1, 'why': 'empty'}
        mk[ex][sym] = {'base': base, 'norm': X.norm_base(base), 'active': True, 'created': None, 'excl': None, 'cat': ''}
    idx['markets'] = mk
    X.save_index(d, idx)
    return idx


VEN = ('bingx', 'okx', 'gateio', 'mexc')


class Ext(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.old = HX.K_REPS
        HX.K_REPS = 6
        cls.bdir, cls.xdir, cls.fdir = tempfile.mkdtemp(), tempfile.mkdtemp(), tempfile.mkdtemp()
        BF.write_battery(cls.bdir, n_syms=18)
        write_xdata_ext(cls.xdir)
        cls.params = {'bdir': cls.bdir, 'xdir': cls.xdir, 'fdir': cls.fdir, 'fee_taker': 0.0005, 'fee_maker': 0.0002, 'version': 'vT', 'venues': VEN, 'coverage_min': 0.0}

    @classmethod
    def tearDownClass(cls):
        HX.K_REPS = cls.old

    def spec(self, lines):
        f = tempfile.NamedTemporaryFile('w', suffix='.md', delete=False, encoding='utf-8')
        f.write('# spec\n' + '\n'.join(lines) + '\n')
        f.close()
        return f.name

    def env(self):
        P, uni, ref = H2R.build_repl_panel(self.xdir, self.bdir, VEN)
        return P, uni, ref, H2R.make_env(P)

    def test_marker_parsing_and_refusal_without_marker(self):
        sp = self.spec(['H2EXT-VENUES: bingx, okx,gateio,xyz'])
        self.assertEqual(X.ext_venues_from_spec(sp), ('bingx', 'okx', 'gateio'))
        self.assertIsNone(X.ext_venues_from_spec(self.spec(['нет'])))
        p = {k: v for k, v in self.params.items() if k != 'venues'}
        for cmd_mode in ('show', 'load', 'confirm'):
            txt = run(HX.run_h2ext(dict(p, mode=cmd_mode, spec_path=self.spec([]), hyp='ext')))
            self.assertIn('H2EXT-VENUES', txt)

    def test_same_rules_bitwise_as_run_cell(self):
        P, uni, ref, env = self.env()
        fees = RB.fees_of(self.params)
        ev = HX.all_events(env)
        self.assertTrue(len(ev) > 10)
        for mult, fo, co in ((1.0, True, True), (2.0, True, True), (1.0, False, False)):
            r1 = RB.run_cell(('H2', 'D3'), env, fees, mult, fo, co)
            r2 = HX.sim(env, fees, ev, mult, fo, co)
            self.assertTrue(np.array_equal(r1['r'], r2['r']))
            self.assertEqual(r1['meta'], r2['meta'])
        syn = {'P': S.synth_daily_panel(P, np.random.default_rng(5))}
        r1 = RB.run_cell(('H2', 'D3'), env, fees, 1.0, False, False, synth=syn)
        r2 = HX.sim(env, fees, ev, 1.0, False, False, synth_P=syn['P'])
        self.assertTrue(np.array_equal(r1['r'], r2['r']))
        self.assertEqual(H2R.CELL, ('H2', 'D3'))
        # правила не изменены: те же константы
        self.assertEqual((S.H2_HOLD, S.H2_STOP, S.H2_RISK_CAP), (30, 0.5, 0.10))

    def test_universe_unions_new_venues_and_excludes_binance_bases(self):
        P, uni, ref, env = self.env()
        exs = {u['ex'] for u in uni}
        self.assertTrue({'gateio', 'mexc', 'bingx', 'okx'} <= exs)
        dup = [u for u in uni if u['norm'] == 'DUP00']
        self.assertEqual([u['ex'] for u in dup], ['gateio'])                      # монета на двух биржах — биржа с большим оборотом за первые 30 суток

    def test_h2f_only_new_symbols_not_seen_on_bingx_okx_and_funding_filter(self):
        P, uni, ref, env = self.env()
        seen = HX.old_norms(self.xdir)
        self.assertIn('DUP00', seen)
        names = HX.new_symbols(uni, VEN, seen)
        self.assertTrue(names and all(n.startswith(('gateio:', 'mexc:')) for n in names))
        self.assertFalse(any('DUP00' in n for n in names))                       # база видна на BingX → не годится
        ev_all = HX.all_events(env)
        ev, c = HX.f_events(P, ev_all, names)
        self.assertEqual(c['candidates'], c['no_funding'] + c['neg_or_zero'] + c['admitted'])
        self.assertTrue(c['no_funding'] > 0 and c['neg_or_zero'] > 0 and c['admitted'] > 0)      # у части MEXC истории фандинга нет, у части Gate.io фандинг отрицательный
        for e, s in ev:
            self.assertIn(P['syms'][s], names)
            self.assertTrue(P['GC'][e, s] > 0 and P['G'][e, s] / P['GC'][e, s] > 0)
        for e, s in ev_all:                                                      # события вне фильтра — ровно те, где условие не выполнено или символ не «новый»
            ok = P['syms'][s] in names and P['GC'][e, s] > 0 and P['G'][e, s] / P['GC'][e, s] > 0
            self.assertEqual((e, s) in set(ev), bool(ok))
        # фильтр не заглядывает вперёд: сигнал использует только сутки e (G[e]), не e+1
        P2 = dict(P)
        G2 = P['G'].copy()
        for e, s in ev_all:
            if e + 1 < P['nd']:
                G2[e + 1, s] = -1.0
        P2['G'] = G2
        ev2, _c2 = HX.f_events(P2, ev_all, names)
        self.assertEqual(ev2, ev)

    def test_h2f_differs_only_in_event_list(self):
        P, uni, ref, env = self.env()
        names = HX.new_symbols(uni, VEN, HX.old_norms(self.xdir))
        ev, _c = HX.f_events(P, HX.all_events(env), names)
        fees = RB.fees_of(self.params)
        r = HX.sim(env, fees, ev)
        self.assertTrue(r['meta']['opened'] > 0)
        self.assertEqual(r['meta']['events'], len(ev))

    def test_threshold_is_2_0_fixed_sequence_not_bonferroni(self):
        self.assertEqual(HX.T_ADJ_MIN, 2.0)
        self.assertEqual(HX.T_ADJ_MIN, H2R.MIN_T_ADJ)
        n = 400
        base = np.random.default_rng(1).normal(0.0004, 0.002, n)
        days, seg = np.arange(n), np.ones(n, bool)
        ev, old = HX.evaluate(days, base, base, seg, 1.0, {}), H2R.evaluate(days, base, base, seg, 1.0, {})
        self.assertEqual(ev['crit'], old['crit'])
        self.assertEqual(ev['pass'], old['pass'])
        src = open(os.path.join(ROOT, 'research_h2ext.py'), encoding='utf-8').read()
        self.assertNotIn('2.24', src.replace('# §2.59.9: fixed-sequence, порог 2.0 (§2.47 без изменений); поправка Бонферрони 2.24 снята', ''))

    def test_h2f_gate_fixed_sequence(self):
        fd = tempfile.mkdtemp()
        p = dict(self.params, fdir=fd, hyp='f', spec_path=self.spec([]))
        calls = []
        orig = HX.evaluate
        HX.evaluate = lambda *x, **k: calls.append(1)
        try:
            for mode in ('show', 'confirm'):
                t = run(HX.run_h2ext(dict(p, mode=mode)))
                self.assertIn('сначала H2EXT', t)                                 # H2EXT не выполнялся
            RF.kv_set('h2_ext_done', True, fd)
            RF.kv_set('h2_ext_passed', False, fd)
            for mode in ('show', 'confirm'):
                t = run(HX.run_h2ext(dict(p, mode=mode)))
                self.assertIn('H2 закрыт целиком', t)                             # H2EXT не пройден → H2F не считается
            self.assertIsNone(RF.kv_get('h2f_done', fd))
            self.assertIsNone(RF.kv_get('h2f_started', fd))
            self.assertEqual(calls, [])
            self.assertIn('нет', run(HX.run_h2ext(dict(p, mode='report'))))       # report доступен всегда
            RF.kv_set('h2_ext_passed', True, fd)
            t = run(HX.run_h2ext(dict(p, mode='show')))
            self.assertIn('H2F-UNIVERSE: N=', t)                                  # H2EXT пройден → H2F разрешена
        finally:
            HX.evaluate = orig

    def test_show_k_before_returns_and_confirm_needs_both_lines_once_only(self):
        calls = []
        orig = HX.evaluate

        def spy(*x, **k):
            calls.append(1)
            return orig(*x, **k)
        HX.evaluate = spy
        try:
            show = run(HX.run_h2ext(dict(self.params, mode='show', hyp='ext', spec_path=self.spec([]))))
            self.assertIn('H2EXT-UNIVERSE: N=', show)
            self.assertEqual(calls, [])
            refused = run(HX.run_h2ext(dict(self.params, mode='confirm', hyp='ext', spec_path=self.spec([]))))
            self.assertIn('не записаны', refused)
            self.assertIsNone(RF.kv_get('h2_ext_started', self.fdir))
            P, uni, ref, env = self.env()
            fees = RB.fees_of(self.params)
            kres = HX.compute_k(env, fees, HX.all_events(env), 'ext', 6)
            names = [f'{u["ex"]}:{u["sym"]}' for u in uni]
            sp = self.spec([f'H2EXT-UNIVERSE: {HX.lines_of(names)}', f'H2EXT-K: {kres["k"]:.3f} (тест)'])
            res = run(HX.run_h2ext(dict(self.params, mode='confirm', hyp='ext', spec_path=sp)))
            if 'НЕПРОВЕРЯЕМА' in res:
                self.assertEqual(calls, [])
            else:
                self.assertEqual(calls, [1])
                for needle in ('Критерии', 'Справочно', 'по биржам', 't_adj', 'последовательная проверка'):
                    self.assertIn(needle, res)
            self.assertTrue(RF.kv_get('h2_ext_done', self.fdir))
            again = run(HX.run_h2ext(dict(self.params, mode='confirm', hyp='ext', spec_path=sp)))
            self.assertIn('повтор запрещён', again)
            self.assertIsNone(RF.kv_get('h2f_done', self.fdir))                  # H2F — отдельное состояние
        finally:
            HX.evaluate = orig

    def test_h2f_has_own_state_and_lines(self):
        fd = tempfile.mkdtemp()
        RF.kv_set('h2_ext_done', True, fd)
        RF.kv_set('h2_ext_passed', True, fd)
        txt = run(HX.run_h2ext(dict(self.params, fdir=fd, mode='show', hyp='f', spec_path=self.spec([]))))
        self.assertIn('H2F-UNIVERSE: N=', txt)
        self.assertIn('H2F: символов новых бирж', txt)
        self.assertIn('Мощность (шаг 0)', txt)
        self.assertIsNone(RF.kv_get('h2f_done', fd))

    def test_untestable_closes_without_returns(self):
        fd = tempfile.mkdtemp()
        xd = tempfile.mkdtemp()
        write_xdata_ext(xd, n_new=8)                                             # мало листингов → D < 365 → «непроверяема»
        calls = []
        orig = HX.evaluate
        HX.evaluate = lambda *x, **k: calls.append(1)
        try:
            p = dict(self.params, xdir=xd, fdir=fd)
            txt = run(HX.run_h2ext(dict(p, mode='confirm', hyp='ext', spec_path=self.spec([]))))
            self.assertIn('НЕПРОВЕРЯЕМА', txt)
            self.assertIn('закрыт окончательно', txt)
            self.assertEqual(calls, [])
            self.assertTrue(RF.kv_get('h2_ext_done', fd))
        finally:
            HX.evaluate = orig

    def test_coverage_gate_new_venues_counts_empty_funding_as_covered(self):
        cov = X.coverage_report(self.xdir, 'h2', self.bdir, venues=VEN, minimum=0.9)
        self.assertTrue(cov['ok'], cov['lines'])
        idx = X.load_index(self.xdir)
        sym = next(iter(idx['bars']['gateio']))
        del idx['bars']['gateio'][sym]
        X.save_index(self.xdir, idx)
        try:
            cov = X.coverage_report(self.xdir, 'h2', self.bdir, venues=VEN, minimum=0.999)
            self.assertFalse(cov['ok'])
        finally:
            write_xdata_ext(self.xdir)


class Load(unittest.TestCase):
    def test_run_xload_ext_uses_new_venues_without_hourly_and_skips_bybit(self):
        d = tempfile.mkdtemp()
        bdir = tempfile.mkdtemp()
        BF.write_battery(bdir, n_syms=6)
        now = int(np.datetime64('2023-06-01', 'ms').astype(np.int64))
        first = int(np.datetime64('2023-01-01', 'D').astype(np.int64))
        bars = T.mk_bars(first, 140, 7, vol=0.06, qv=6e7)

        class Ex:
            def __init__(self, base):
                self.base = base
                self.called = []

            async def load_markets(self):
                self.called.append('load_markets')
                return {f'{self.base}/USDT:USDT': {'swap': True, 'linear': True, 'quote': 'USDT', 'settle': 'USDT', 'base': self.base, 'active': True, 'created': None, 'info': {}}}

            async def fetch_ohlcv(self, sym, tf, since=None, limit=None, params=None):
                self.called.append(f'ohlcv:{tf}')
                return T.ohlcv_window(bars, since, limit or 1000, params)

            async def fetch_funding_rate_history(self, sym, since=None, limit=None, params=None):
                ft, fr = T.mk_fund(first, 140, 3)
                end = (params or {}).get('endTime')
                rows = [{'timestamp': int(t), 'fundingRate': float(r)} for t, r in zip(ft, fr) if end is None or t <= end]
                return rows[-(limit or 100):]

            async def close(self):
                pass
        exs = {'bingx': Ex('AAA'), 'okx': Ex('BBB'), 'gateio': Ex('CCC'), 'mexc': Ex('DDD')}
        txt = run(X.run_xload({'xdir': d, 'bdir': bdir, 'venues': VEN, 'ext': True, 'hourly': False, 'max_minutes': 5}, None, exchanges=exs, now_ms=now, sleep=nosleep))
        self.assertIn('gateio: линейных USDT-перпетуалов 1', txt)
        self.assertIn('mexc: линейных USDT-перпетуалов 1', txt)
        self.assertIn('✅ загрузка завершена', txt)
        idx = X.load_index(d)
        for ex in VEN:
            self.assertEqual(len(idx['bars'][ex]), 1, ex)
            self.assertEqual(len(idx['fund'][ex]), 1, ex)
        self.assertFalse(any(c.startswith('ohlcv:1h') for e in exs.values() for c in e.called))      # 1h не грузится (H2EXT)
        self.assertIn('глубина истории фандинга', txt)
        self.assertIn('история фандинга начинается у листинга', txt)


# ── замер расхождений ───────────────────────────────────────────────────────────────────────────────
class Clock:
    def __init__(self, t=1_800_000_000.0):
        self.t = t

    def now(self):
        return self.t

    async def sleep(self, s):
        self.t += s


def snaps(gap_by_base, mid=100.0, hs=0.0002, qv=1e7):
    """Фабрика тикеров: BingX mid·(1+gap), OKX mid."""
    def mk(base, g, ex):
        m = mid * ((1 + g) if ex == 'b' else 1.0)
        return (m * (1 - hs), m * (1 + hs), qv)
    sb = {b: mk(b, g, 'b') for b, g in gap_by_base.items()}
    so = {b: mk(b, g, 'o') for b, g in gap_by_base.items()}
    return sb, so


class Collect(unittest.TestCase):
    def test_selection_rules_and_size_estimate(self):
        sb = {'AAA': (1, 1.01, 9e6), 'BBB': (1, 1.01, 9e6), 'NCSK': (1, 1.01, 9e6), 'LOW': (1, 1.01, 1e6), 'ONLYB': (1, 1.01, 9e6)}
        so = {'AAA': (1, 1.01, 9e6), 'BBB': (1, 1.01, 8e6), 'NCSK': (1, 1.01, 9e6), 'LOW': (1, 1.01, 9e6), 'ONLYO': (1, 1.01, 9e6)}
        syms, ref = LL.select_symbols(sb, so, 2.0, cap_mb=1000)
        self.assertEqual(syms, ['AAA', 'BBB'])
        self.assertEqual((ref['common'], ref['noncrypto'], ref['low_turnover']), (4, 1, 1))
        self.assertEqual(LL.estimate_bytes(150, 2.0), int(150 * 12 * 7 * 86400 / 2.0 + 16 * 7 * 86400 / 2.0))
        self.assertAlmostEqual(LL.estimate_bytes(150, 2.0) / 1e6, 553.0, delta=15)
        n = LL.max_syms_for(100.0, 2.0)                                          # лимит объёма урезает число символов по убыванию минимального оборота
        self.assertTrue(LL.estimate_bytes(n, 2.0) <= 100e6 < LL.estimate_bytes(n + 1, 2.0))
        sb2 = {f'S{i:03d}': (1, 1.01, 1e7 + i * 1e5) for i in range(300)}
        syms2, ref2 = LL.select_symbols(sb2, dict(sb2), 2.0, cap_mb=100.0)
        self.assertEqual(len(syms2), n)
        self.assertEqual(ref2['trimmed'], 300 - n)
        self.assertIn('S299', syms2)
        self.assertNotIn('S000', syms2)

    def test_collector_batches_resume_repair_and_warm_filter(self):
        d = tempfile.mkdtemp()
        clk = Clock()
        gaps = {'AAA': 0.0, 'BBB': 0.001, 'SCALE': 0.5}                          # SCALE: |gap| 50% — исключается по прогреву
        calls = {'b': 0, 'o': 0}

        async def gb():
            calls['b'] += 1
            return snaps(gaps)[0]

        async def go():
            calls['o'] += 1
            return snaps(gaps)[1]
        old_flush = LL.FLUSH_TICKS
        LL.FLUSH_TICKS = 20
        try:
            col = LL.Collector(d, gb, go, step=2.0, days=7, sleep=clk.sleep, now=clk.now)
            n_ticks = 77

            async def stopper():
                while col.ticks + len(col.buf_ts) < n_ticks:
                    await asyncio.sleep(0)
                col.stop = True
            async def both():
                await asyncio.gather(col.run(), stopper())
            run(both())
        finally:
            LL.FLUSH_TICKS = old_flush
        st = LL.load_state(d)
        self.assertEqual(sorted(st['symbols']), ['AAA', 'BBB'])
        self.assertEqual(st['excluded_scale'], ['SCALE'])
        ts = LL.load_ts(d)
        self.assertTrue(len(ts) >= 77)
        for b in st['symbols']:
            arr = LL.load_sym(d, b, len(ts))
            self.assertEqual(len(arr), len(ts))
        self.assertFalse(os.path.exists(LL.spath(d, 'SCALE')))
        arr = LL.load_sym(d, 'BBB', len(ts))
        self.assertAlmostEqual(float(arr['mb'][0] / arr['mo'][0] - 1), 0.001, places=5)
        self.assertEqual(int(arr['sb'][0]), 200)                                  # половина спреда 0.0002 в единицах 1e-6
        self.assertFalse(st['running'])
        # repair: оборванная запись — файлы символа длиннее ts.bin
        with open(LL.spath(d, 'AAA'), 'ab') as f:
            np.zeros(3, dtype=LL.DT).tofile(f)
        col2 = LL.Collector(d, gb, go, sleep=clk.sleep, now=clk.now)
        col2.syms = st['symbols']
        n = col2.repair()
        self.assertEqual(n, len(ts))
        self.assertEqual(os.path.getsize(LL.spath(d, 'AAA')) // LL.DT.itemsize, n)

    def test_rate_limit_raises_step_and_errors_dont_write(self):
        d = tempfile.mkdtemp()
        clk = Clock()
        n = {'k': 0}
        gaps = {'AAA': 0.0}

        async def gb():
            n['k'] += 1
            if 3 <= n['k'] <= 6:
                raise RuntimeError('bingx 100410 请稍后重试')
            return snaps(gaps)[0]

        async def go():
            return snaps(gaps)[1]
        col = LL.Collector(d, gb, go, step=2.0, days=7, sleep=clk.sleep, now=clk.now)
        st0 = LL.load_state(d)
        self.assertEqual(st0, {})

        async def both():
            async def stopper():
                while col.ticks + len(col.buf_ts) < 12:
                    await asyncio.sleep(0)
                col.stop = True
            await asyncio.gather(col.run(), stopper())
        run(both())
        st = LL.load_state(d)
        self.assertTrue(st.get('step_raised', 0) >= 1)
        self.assertTrue(col.step > 2.0 and col.step <= LL.STEP_MAX_S)
        self.assertEqual(col.errors['rate'] >= 1, True)

    def test_disk_cap_stops_writing(self):
        d = tempfile.mkdtemp()
        clk = Clock()
        old_cap, old_f = os.environ.get('LEADLAG_MAX_MB'), LL.FLUSH_TICKS
        os.environ['LEADLAG_MAX_MB'] = '0.0001'
        LL.FLUSH_TICKS = 10

        async def gb():
            return snaps({'AAA': 0.0})[0]

        async def go():
            return snaps({'AAA': 0.0})[1]
        sent = []

        async def notify(m):
            sent.append(m)
        try:
            col = LL.Collector(d, gb, go, step=2.0, sleep=clk.sleep, now=clk.now, notify=notify)
            run(col.run())
        finally:
            LL.FLUSH_TICKS = old_f
            if old_cap is None:
                os.environ.pop('LEADLAG_MAX_MB')
            else:
                os.environ['LEADLAG_MAX_MB'] = old_cap
        st = LL.load_state(d)
        self.assertFalse(st['running'])
        self.assertIn('лимит', st['stopped'])
        self.assertTrue(any('лимит' in m for m in sent))


def make_series(d, spec, n=4000, step=2.0, t0=1_800_000_000.0, sym='AAA', hs=0.0002):
    """Записывает данные одного символа в формат сборщика: spec(i) → (gap, dBingX, dOKX) относительно базовой цены."""
    mb = np.zeros(n)
    mo = np.zeros(n)
    for i in range(n):
        g, db, do = spec(i)
        mo[i] = 100.0 * (1 + do)
        mb[i] = mo[i] * (1 + g) * (1 + db)
    arr = np.zeros(n, dtype=LL.DT)
    arr['mb'], arr['mo'] = mb, mo
    arr['sb'] = arr['so'] = int(round(hs / LL.SPREAD_UNIT))
    os.makedirs(d, exist_ok=True)
    arr.tofile(LL.spath(d, sym))
    ts = np.zeros((n, 2), dtype='<i8')
    ts[:, 0] = ((t0 + step * np.arange(n)) * 1000).astype(np.int64)
    ts[:, 1] = ts[:, 0]
    ts.tofile(LL.ts_path(d))
    LL.save_state(d, {'running': False, 'step': step, 'symbols': [sym], 'ref': {'common': 1, 'noncrypto': 0, 'low_turnover': 0, 'trimmed': 0}, 'excluded_scale': [], 'ticks': n})
    return arr, ts


class Analysis(unittest.TestCase):
    def test_events_duration_and_who_converges(self):
        d = tempfile.mkdtemp()
        # событие 1: BingX на +0.5% выше 10 тиков, затем BingX падает на 0.5% (BingX сходится); событие 2: OKX уходит вверх (+0.5% к BingX... gap = −0.5%), затем OKX возвращается
        def spec(i):
            if 100 <= i < 110:
                return 0.005, 0.0, 0.0
            if 300 <= i < 305:
                return 0.0, 0.0, 0.0
            return 0.0, 0.0, 0.0
        arr, ts = make_series(d, spec, n=600)
        t = (ts[:, 0] + ts[:, 1]) / 2000.0
        gap = arr['mb'].astype(float) / arr['mo'].astype(float) - 1
        ev = LL.find_events(gap, t, 0.0025, 2.0)
        self.assertEqual(ev, [(100, 109, 110)])
        r = LL.analyse_symbol(arr, ts, 2.0, 0.0025, 0.0005)
        self.assertEqual(r['events'], 1)
        self.assertAlmostEqual(r['dur_all'][0], 18.0, places=3)
        # exec_gap = 0.5% − 2·0.02% − 2·0.05% = 0.36% > 0.10% и длительность 18 с ≥ 4 с → исполнимое; BingX сходится: mid BingX упал, OKX не двигался
        self.assertEqual(r['exec'], 1)
        self.assertEqual(r['b_conv'], 1)

    def test_okx_converges_to_bingx(self):
        d = tempfile.mkdtemp()

        def spec(i):
            if 50 <= i < 60:
                return 0.005, 0.0, 0.0                                           # OKX 100, BingX 100.5: gap +0.5%
            if i >= 60:
                return 0.0, 0.0, 0.005                                           # OKX поднялся до 100.5, BingX остался 100.5 → gap закрылся за счёт OKX
            return 0.0, 0.0, 0.0
        arr, ts = make_series(d, spec, n=300)
        r = LL.analyse_symbol(arr, ts, 2.0, 0.0025, 0.0005)
        self.assertEqual((r['events'], r['exec'], r['b_conv']), (1, 1, 0))

    def test_short_event_and_costs_not_executable(self):
        d = tempfile.mkdtemp()
        arr, ts = make_series(d, lambda i: (0.003, 0.0, 0.0) if i == 100 else (0.0, 0.0, 0.0), n=300)       # один тик — не событие критерия (§2.60.6)
        r = LL.analyse_symbol(arr, ts, 2.0, 0.0025, 0.0005)
        self.assertEqual((r['events'], r['single'], r['exec']), (1, 1, 0))
        d2 = tempfile.mkdtemp()
        arr, ts = make_series(d2, lambda i: (0.003, 0.0, 0.0) if 100 <= i < 110 else (0.0, 0.0, 0.0), n=300)      # 0.3% − 0.04% − 0.10% = 0.16% > 0.10% → исполнимо
        self.assertEqual(LL.analyse_symbol(arr, ts, 2.0, 0.0025, 0.0005)['exec'], 1)
        arr, ts = make_series(d2, lambda i: (0.003, 0.0, 0.0) if 100 <= i < 110 else (0.0, 0.0, 0.0), n=300, hs=0.001)   # половина спреда 0.1%: 0.3 − 0.2 − 0.1 = 0 → не исполнимо
        self.assertEqual(LL.analyse_symbol(arr, ts, 2.0, 0.0025, 0.0005)['exec'], 0)

    def test_gap_in_data_breaks_events_and_truncated_events_dropped(self):
        d = tempfile.mkdtemp()
        arr, ts = make_series(d, lambda i: (0.005, 0.0, 0.0) if 100 <= i < 120 else (0.0, 0.0, 0.0), n=300)
        ts2 = ts.copy()
        ts2[110:] += 600_000                                                      # разрыв 10 минут посреди события
        t = (ts2[:, 0] + ts2[:, 1]) / 2000.0
        gap = arr['mb'].astype(float) / arr['mo'].astype(float) - 1
        self.assertEqual(LL.find_events(gap, t, 0.0025, 2.0), [])                 # первая половина — без конца под порогом (оборвана разрывом), вторая — начало неизвестно (тик перед разрывом тоже над порогом)
        arr3, ts3 = make_series(tempfile.mkdtemp(), lambda i: (0.005, 0.0, 0.0) if 290 <= i else (0.0, 0.0, 0.0), n=300)
        t3 = (ts3[:, 0] + ts3[:, 1]) / 2000.0
        self.assertEqual(LL.find_events(arr3['mb'].astype(float) / arr3['mo'].astype(float) - 1, t3, 0.0025, 2.0), [])   # не закончилось до конца данных

    def test_report_b3_pass_and_fail(self):
        d = tempfile.mkdtemp()
        old = (LL.EXEC_PER_DAY_MIN, LL.EXEC_SYMS_MIN)
        try:
            # короткий ряд (n=600 ≈ 20 мин): порог «в сутки» масштабируем тестом — проверяем механику вердикта
            def spec(i):
                return (0.005, 0.0, 0.0) if (i % 40) < 12 else (0.0, 0.0, 0.0)
            make_series(d, spec, n=1200)
            LL.EXEC_PER_DAY_MIN, LL.EXEC_SYMS_MIN = 20.0, 1
            txt = LL.report(d, 0.0005)
            for needle in ('/leadlag_report', 'C = 0.15%', 'C = 0.25%', 'C = 0.40%', 'ИТОГ B3', 'BingX сходится'):
                self.assertIn(needle, txt)
            self.assertIn('критерий выполнен', txt.split('ИТОГ B3')[1])
            LL.EXEC_SYMS_MIN = 10
            txt2 = LL.report(d, 0.0005)
            self.assertIn('направление закрыто', txt2.split('ИТОГ B3')[1])
        finally:
            LL.EXEC_PER_DAY_MIN, LL.EXEC_SYMS_MIN = old

    def test_report_without_data(self):
        self.assertIn('данных нет', LL.report(tempfile.mkdtemp(), 0.0005))


class Wiring(unittest.TestCase):
    def test_commands_known_help_and_runner_tasks(self):
        pool, allv = a.command_pools()
        for c in ('/h2_ext', '/h2f', '/leadlag_start', '/leadlag_stop', '/leadlag_status', '/leadlag_report'):
            self.assertIn(c, allv)
            self.assertIsNone(a.command_hint(c))
        src = open(os.path.join(ROOT, 'async_bot.py'), encoding='utf-8').read()
        self.assertIn('/leadlag_start [new|resume]', src)
        import research_runner as R
        self.assertIn('h2_ext', R.TASKS)
        self.assertIn('leadlag_report', R.TASKS)
        self.assertTrue(a.CODE_VERSION.endswith('v130'))
        self.assertEqual(a.CHANGELOG[0][0], '2026-11-24-v130')
        self.assertIn('2026-11-21-v129', [x[0] for x in a.CHANGELOG])

    def test_leadlag_commands_do_not_start_in_idle_without_command(self):
        """Автопродолжение ничего не запускает, если сбор не начинали (состояния нет)."""
        d = tempfile.mkdtemp()
        os.environ['LEADLAG_DIR'] = d
        started = []
        orig = a._ll_launch

        async def fake(resume):
            started.append(resume)
            return {}
        a._ll_launch = fake
        try:
            run(a.leadlag_autoresume())
            self.assertEqual(started, [])
            LL.save_state(d, {'running': True, 'until': 4_000_000_000, 'symbols': ['AAA']})
            _common.fresh_state()
            run(a.leadlag_autoresume())
            self.assertEqual(started, [True])
            LL.save_state(d, {'running': True, 'until': 1.0, 'symbols': ['AAA']})
            run(a.leadlag_autoresume())
            self.assertEqual(started, [True])                                     # срок истёк — не продолжается
        finally:
            a._ll_launch = orig
            os.environ.pop('LEADLAG_DIR')

    def test_leadlag_start_refuses_over_existing_data_without_new_or_resume(self):
        d = tempfile.mkdtemp()
        os.environ['LEADLAG_DIR'] = d
        make_series(d, lambda i: (0.0, 0.0, 0.0), n=50)
        _db, sent = _common.fresh_state()
        launched = []
        orig = a._ll_launch

        async def fake(resume):
            launched.append(resume)
            return {'symbols': ['AAA'], 'ref': {}, 'step': 2.0, 'est_bytes': 1}
        a._ll_launch = fake
        try:
            run(a.leadlag_cmd('/leadlag_start', []))
            self.assertEqual(launched, [])
            self.assertTrue(any('/leadlag_start new' in m for m in sent))
            self.assertTrue(os.path.exists(LL.ts_path(d)))                         # данные не удалены
            run(a.leadlag_cmd('/leadlag_start', ['new']))
            self.assertEqual(launched, [False])
            self.assertFalse(os.path.exists(LL.ts_path(d)))                        # удалены только по явному new
        finally:
            a._ll_launch = orig
            os.environ.pop('LEADLAG_DIR')


class DiskAndStatus(unittest.TestCase):
    def test_collector_stops_when_free_space_below_200mb_and_notifies(self):
        d = tempfile.mkdtemp()
        clk = Clock()
        sent = []

        async def notify(m):
            sent.append(m)

        async def gb():
            return snaps({'AAA': 0.0})[0]

        async def go():
            return snaps({'AAA': 0.0})[1]
        orig, old_f = LL.free_mb, LL.FLUSH_TICKS
        LL.free_mb = lambda _d: 150.0
        LL.FLUSH_TICKS = 10
        try:
            col = LL.Collector(d, gb, go, step=2.0, sleep=clk.sleep, now=clk.now, notify=notify)
            st = run(col.run())
        finally:
            LL.free_mb, LL.FLUSH_TICKS = orig, old_f
        self.assertFalse(st['running'])
        self.assertIn('мало места', st['stopped'])
        self.assertTrue(any('150' in m and '200' in m for m in sent))
        self.assertTrue(col.ticks <= LL.DISK_CHECK_S / 2 + 5)                    # остановка на первой 10-минутной проверке
        self.assertEqual(os.path.getsize(LL.ts_path(d)) // 16, col.ticks)         # записанное до остановки сохранено

    def test_enough_space_does_not_stop_before_check_and_checks_every_10_minutes(self):
        d = tempfile.mkdtemp()
        clk = Clock()
        checks = []

        async def gb():
            return snaps({'AAA': 0.0})[0]

        async def go():
            return snaps({'AAA': 0.0})[1]
        orig = LL.free_mb

        def fake(_d):
            checks.append(clk.t)
            return 5000.0
        LL.free_mb = fake
        try:
            col = LL.Collector(d, gb, go, step=2.0, sleep=clk.sleep, now=clk.now)

            async def both():
                async def stopper():
                    while clk.t < 1_800_000_000.0 + 3 * 600 + 30:
                        await asyncio.sleep(0)
                    col.stop = True
                await asyncio.gather(col.run(), stopper())
            run(both())
        finally:
            LL.free_mb = orig
        self.assertEqual(len(checks), 3)
        self.assertTrue(all(abs((b - a_) - 600) < 5 for a_, b in zip(checks, checks[1:])))

    def test_start_refused_when_free_space_low_and_status_shows_space(self):
        d = tempfile.mkdtemp()
        os.environ['LEADLAG_DIR'] = d
        _db, sent = _common.fresh_state()
        launched = []
        orig_l, orig_f = a._ll_launch, LL.free_mb

        async def fake(resume):
            launched.append(resume)
            return {'symbols': [], 'ref': {}, 'step': 2.0, 'est_bytes': 1}
        a._ll_launch = fake
        LL.free_mb = lambda _d: 120.0
        try:
            run(a.leadlag_cmd('/leadlag_start', []))
            self.assertEqual(launched, [])
            self.assertTrue(any('120' in m and '200' in m for m in sent))
            LL.free_mb = lambda _d: 900.0
            run(a.leadlag_cmd('/leadlag_start', []))
            self.assertEqual(launched, [False])
            LL.save_state(d, {'running': True, 'until': 4_000_000_000, 'step': 2.0, 'symbols': ['AAA'], 'ticks': 5})
            txt = LL.status_text(d)
            self.assertIn('занято сборщиком', txt)
            self.assertIn('свободно на томе 900', txt)
        finally:
            a._ll_launch, LL.free_mb = orig_l, orig_f
            os.environ.pop('LEADLAG_DIR')

    def test_free_mb_missing_dir_uses_parent(self):
        self.assertTrue(LL.free_mb(os.path.join(tempfile.mkdtemp(), 'a', 'b')) > 0)

    def test_status_line_with_scanner_flags(self):
        _common.fresh_state()
        txt = a.status_text()
        self.assertIn('Сканеры (ENV): все выключены', txt)
        saved = (a.SMC2_ENABLED, a.RB_ENABLED)
        a.SMC2_ENABLED, a.RB_ENABLED = True, True
        try:
            txt = a.status_text()
            self.assertIn('⚠️ Сканеры включены (ENV): SMC2, RB', txt)
            self.assertNotIn('все выключены', txt)
        finally:
            a.SMC2_ENABLED, a.RB_ENABLED = saved
        a._ll_state['task'] = type('T', (), {'done': lambda self: False})()
        try:
            self.assertIn('сбор /leadlag идёт', a.status_text())
        finally:
            a._ll_state['task'] = None


if __name__ == '__main__':
    unittest.main()
