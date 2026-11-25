"""v131 (§2.61): Bitget — дефект запроса ≠ «пусто», перебор форм запроса, `/venue_probe`; конец кэша BingX/OKX — «конец данных», не «делистинг»; маркер исключения. Сеть не используется."""
import asyncio
import os
import tempfile
import unittest

import numpy as np

import _common
import research_bsim as S
import research_h2repl as H2R
import research_venues as V
import research_xdata as X
import test_v116_h2x1 as T
import test_v129_h2ext_leadlag as T9
from _common import a

ROOT = _common.ROOT
DAY = X.DAY
NOW = int(np.datetime64('2025-01-01', 'ms').astype(np.int64))
FEE = {'taker': 0.0005, 'maker': 0.0002}
E40017 = '{"code":"40017","msg":"Parameter verification failed startTime || endTime","requestTime":1}'


def run(c):
    return asyncio.run(c)


def strict_bitget(max_window_bars=200, only_forms=None, calls=None):
    """Bitget с ограничением окна: history-candles принимает окно не шире max_window_bars суток; иначе HTTP 400 / 40017."""
    def get(path, params):
        if calls is not None:
            calls.append((path, dict(params)))
        if path.endswith('/contracts'):
            return {'code': '00000', 'data': [{'symbol': 'BTCUSDT', 'baseCoin': 'BTC', 'quoteCoin': 'USDT', 'symbolType': 'perpetual', 'launchTime': '1600000000000', 'symbolStatus': 'normal'}]}
        if '/candles' in path or path.endswith('history-candles'):
            ep = path.rsplit('/', 1)[-1]
            if only_forms is not None and ep not in only_forms:
                raise V.VenueError(f'bitget HTTP 400: {E40017}')
            lo, hi = params.get('startTime'), params.get('endTime')
            if lo is not None and hi is not None and (int(hi) - int(lo)) / DAY > max_window_bars:
                raise V.VenueError(f'bitget HTTP 400: {E40017}')
            if lo is None and hi is None and ep == 'history-candles':
                raise V.VenueError(f'bitget HTTP 400: {E40017}')
            hi = int(hi) if hi is not None else NOW
            lo = int(lo) if lo is not None else hi - int(params['limit']) * DAY
            ts = list(range(((lo // DAY) + 1) * DAY, hi + 1, DAY))[-int(params['limit']):]
            return {'code': '00000', 'data': [[str(t), '1', '2', '0.5', '1.5', '10', '777'] for t in ts]}
        if path.endswith('history-fund-rate'):
            if int(params.get('pageSize', 0) or 0) == 100:
                raise V.VenueError(f'bitget HTTP 400: {E40017}')
            page = int(params.get('pageNo', 1))
            data = [{'symbol': 'BTCUSDT', 'fundingRate': '0.0001', 'fundingTime': str(1_700_000_000_000 + 28_800_000 * (page * 20 - k))} for k in range(20)] if page <= 2 else []
            return {'code': '00000', 'data': data}
        raise AssertionError(path)

    async def aget(path, params):
        return get(path, params)
    return V.BitgetPublic(get=aget, sleep=T9.nosleep, pause=0.0, now=lambda: NOW)


class Defect(unittest.TestCase):
    def test_regex(self):
        for t in (f'bitget HTTP 400: {E40017}', 'gateio HTTP 400: {"label":"INVALID_PARAM_VALUE"}', 'bitget history-candles: {\'code\': \'40018\'}', 'x HTTP 422: y'):
            self.assertTrue(V.is_request_defect(V.VenueError(t)), t)
            self.assertTrue(X.is_request_defect(V.VenueError(t)), t)
        for t in ('bitget HTTP 500: x', 'bitget HTTP 429 too many requests: y', 'mexc HTTP 403: forbidden', 'bitget history-candles: {"code":"00001"}'):
            self.assertFalse(V.is_request_defect(V.VenueError(t)), t)
        self.assertFalse(V.is_request_defect(RuntimeError('HTTP 400')))               # только исключения клиентов прямого REST
        self.assertFalse(X.is_transient(V.VenueError(f'bitget HTTP 400: {E40017}')))   # не повторяется

    def test_forms_fall_back_to_working_window_and_remember(self):
        calls = []
        ex = strict_bitget(max_window_bars=30, calls=calls)
        run(ex.load_markets())
        f = X.Fetcher(ex, sleep=T9.nosleep, pause=0.0)
        since = NOW - 400 * DAY
        ts, o, h, l, c, v, comp = run(X.fetch_bars_back(f, 'bitget', 'BTC/USDT:USDT', since, NOW))
        self.assertTrue(len(ts) >= 390 and np.all(np.diff(ts) == DAY))
        self.assertEqual(ex.CANDLE_FORMS[ex._cform], ('history-candles', 30, 'window'))
        n_first = len(calls)
        run(X.fetch_bars_back(f, 'bitget', 'BTC/USDT:USDT', since, NOW))             # форма запомнена: отклонённые формы больше не запрашиваются
        later = calls[n_first:]
        self.assertTrue(all(p['limit'] == 30 for _p, p in later if 'candles' in _p))

    def test_all_forms_rejected_error_cached_no_hammering(self):
        calls = []
        ex = strict_bitget(max_window_bars=0, only_forms=('nothing',), calls=calls)
        run(ex.load_markets())
        with self.assertRaises(V.VenueError) as cm:
            run(ex.fetch_ohlcv('BTC/USDT:USDT', '1d', None, 1000, {'endTime': NOW}))
        self.assertIn('все' if False else 'отклонён', str(cm.exception))
        self.assertIn('40017', str(cm.exception))
        n = len(calls)
        with self.assertRaises(V.VenueError):
            run(ex.fetch_ohlcv('ETH/USDT:USDT', '1d', None, 1000, {'endTime': NOW}))
        self.assertEqual(len(calls), n)                                                # без HTTP: ошибка запомнена на прогон

    def test_funding_forms(self):
        ex = strict_bitget()
        run(ex.load_markets())
        rows = run(ex.fetch_funding_rate_history('BTC/USDT:USDT', None, 1000, {}))
        self.assertTrue(rows)
        self.assertEqual(ex.FUND_FORMS[ex._fform], (20, True))                         # pageSize=100 отклонён, 20 принят
        ex2 = strict_bitget()
        ex2.FUND_FORMS = ((100, True),)
        run(ex2.load_markets())
        with self.assertRaises(V.VenueError):
            run(ex2.fetch_funding_rate_history('BTC/USDT:USDT', None, 1000, {}))


class Classify(unittest.TestCase):
    def loader(self, make_error=None, empty=False, n=30, d=None, idx_pre=None):
        d = d or tempfile.mkdtemp()
        bdir = tempfile.mkdtemp()
        T9.BF.write_battery(bdir, n_syms=6)
        first = int(np.datetime64('2023-01-01', 'D').astype(np.int64))
        bars = T.mk_bars(first, 140, 7, vol=0.06, qv=6e7)
        calls = []

        class Ex:
            async def load_markets(self):
                return {f'C{i:03d}/USDT:USDT': {'swap': True, 'linear': True, 'quote': 'USDT', 'settle': 'USDT', 'base': f'C{i:03d}', 'active': True, 'created': None, 'info': {}} for i in range(n)}

            async def fetch_ohlcv(self, sym, tf, since=None, limit=None, params=None):
                calls.append(sym)
                if make_error:
                    raise make_error()
                if empty:
                    return []
                return T.ohlcv_window(bars, since, limit or 1000, params)

            async def fetch_funding_rate_history(self, sym, since=None, limit=None, params=None):
                return []

            async def close(self):
                pass
        txt = run(X.run_xload({'xdir': d, 'bdir': bdir, 'venues': ('bitget',), 'ext': True, 'hourly': False, 'max_minutes': 5}, None, exchanges={'bitget': Ex()},
                              now_ms=int(np.datetime64('2023-06-01', 'ms').astype(np.int64)), sleep=T9.nosleep))
        return d, txt, calls

    def test_40017_stays_unprocessed_not_empty(self):
        d, txt, calls = self.loader(lambda: V.VenueError(f'bitget HTTP 400: {E40017}'))
        idx = X.load_index(d)
        self.assertEqual(idx['bars']['bitget'], {})
        self.assertFalse([k for k in idx['neg'] if k.startswith('bitget|')])             # в отрицательный кэш не записано (ни «пусто», ни «постоянная ошибка»)
        fails = [v for k, v in idx['fail'].items() if k.startswith('bitget|')]
        self.assertTrue(fails and all(v.startswith('ДЕФЕКТ ЗАПРОСА') for v in fails))
        self.assertIn('дефект запроса', txt)
        self.assertIn('40017', txt)
        self.assertNotIn('✅ загрузка завершена', txt)
        self.assertNotIn('подтверждённо пусто/постоянная ошибка: 30', txt)
        self.assertEqual(len(calls), X.DEFECT_STOP)                                       # защита: нет успехов и 10 дефектов подряд — остальные не запрашивались
        self.assertIn('остальные символы биржи', txt)
        # повторная загрузка (после «исправления») запрашивает их снова — кэша нет
        d2, txt2, calls2 = self.loader(None, d=d)
        self.assertEqual(len(set(calls2)), 30)
        self.assertEqual(len(X.load_index(d)['bars']['bitget']), 30)

    def test_defect_symbols_are_uncovered_in_gate(self):
        d, txt, calls = self.loader(lambda: V.VenueError(f'bitget HTTP 400: {E40017}'))
        cov = X.coverage_report(d, 'h2', None, venues=('bitget',), minimum=0.9)
        self.assertFalse(cov['ok'])
        self.assertIn('свечи: 0 из 30', ' '.join(cov['lines']))

    def test_empty_successful_answer_is_confirmed_empty(self):
        d, txt, calls = self.loader(empty=True)
        idx = X.load_index(d)
        negs = [r for k, r in idx['neg'].items() if k.startswith('bitget|') and k.endswith('|bars')]
        self.assertEqual(len(negs), 30)
        self.assertTrue(all(r['why'] == 'empty' for r in negs))
        self.assertNotIn('ДЕФЕКТ', str(idx['fail']))
        self.assertIn('подтверждённо пусто/постоянная ошибка: 30', txt)

    def test_other_errors_unchanged(self):
        d, txt, calls = self.loader(lambda: V.VenueError('bitget HTTP 403: forbidden'), n=3)
        idx = X.load_index(d)
        negs = [r for k, r in idx['neg'].items() if k.startswith('bitget|')]
        self.assertEqual(len(negs), 3)                                                   # прежнее поведение: постоянная ошибка символа (не дефект запроса)
        self.assertTrue(all(r['why'] == 'VenueError' for r in negs))

    def test_migration_resets_only_bitget_errors_once(self):
        d = tempfile.mkdtemp()
        idx = X.load_index(d)
        X.put(d, idx, 'neg', 'bitget', 'A/USDT:USDT|bars', {'t': 1, 'why': 'VenueError', 'msg': 'HTTP 400 40017'})
        X.put(d, idx, 'neg', 'bitget', 'B/USDT:USDT|bars', {'t': 1, 'why': 'empty'})
        X.put(d, idx, 'neg', 'gateio', 'C/USDT:USDT|bars', {'t': 1, 'why': 'VenueError', 'msg': 'HTTP 403'})
        self.assertEqual(X.migrate_v131(d, idx), 1)
        idx2 = X.load_index(d)
        self.assertNotIn('bitget|A/USDT:USDT|bars', idx2['neg'])
        self.assertIn('bitget|B/USDT:USDT|bars', idx2['neg'])
        self.assertIn('gateio|C/USDT:USDT|bars', idx2['neg'])
        self.assertEqual(X.migrate_v131(d, idx2), 0)


class Exclude(unittest.TestCase):
    def spec(self, lines):
        f = tempfile.NamedTemporaryFile('w', suffix='.md', delete=False, encoding='utf-8')
        f.write('\n'.join(lines) + '\n')
        f.close()
        return f.name

    def test_exclude_marker(self):
        sp = self.spec(['H2EXT-VENUES: gateio,mexc,bitget'])
        self.assertEqual(X.ext_venues_from_spec(sp), ('bingx', 'okx', 'gateio', 'mexc', 'bitget'))
        sp2 = self.spec(['H2EXT-VENUES: gateio,mexc,bitget', 'H2EXT-EXCLUDE: bitget успешный пустой ответ истории (тест)'])
        self.assertEqual(X.excluded_venues_from_spec(sp2), ('bitget',))
        self.assertEqual(X.ext_venues_from_spec(sp2), ('bingx', 'okx', 'gateio', 'mexc'))
        self.assertEqual(X.excluded_venues_from_spec(self.spec(['нет'])), ())
        # в BOT_SPEC маркера исключения НЕТ (запишется отдельным коммитом после проверки): Bitget остаётся во вселенной
        self.assertEqual(X.excluded_venues_from_spec(), ())
        self.assertIn('bitget', X.ext_venues_from_spec())

    def test_gate_counts_remaining_exchanges_only(self):
        d = tempfile.mkdtemp()
        bdir = tempfile.mkdtemp()
        T9.BF.write_battery(bdir, n_syms=18)
        T9.write_xdata_ext(d)
        idx = X.load_index(d)
        idx['markets']['bitget'] = {f'Z{i}/USDT:USDT': {'base': f'Z{i}', 'norm': f'Z{i}', 'active': True, 'created': None, 'excl': None, 'cat': ''} for i in range(40)}
        X.save_index(d, idx)
        self.assertFalse(X.coverage_report(d, 'h2', bdir, venues=('bingx', 'okx', 'gateio', 'mexc', 'bitget'), minimum=0.9)['ok'])
        self.assertTrue(X.coverage_report(d, 'h2', bdir, venues=('bingx', 'okx', 'gateio', 'mexc'), minimum=0.9)['ok'])


# ── конец кэша BingX/OKX ──────────────────────────────────────────────────────────────────────────
D0 = T.D0


def series(first, n, seed, vol=0.02):
    b = T.mk_bars(first, n, seed, vol=vol, qv=6e7)
    return {k: b[k] for k in ('ts', 'o', 'h', 'l', 'c', 'qv')}


class DataEnd(unittest.TestCase):
    def panel(self):
        sd = {'gateio:ZZZ': series(D0, 300, 1), 'bingx:AAA': series(D0 + 40, 20, 2), 'bingx:INACT': series(D0 + 40, 20, 3), 'gateio:BBB': series(D0 + 40, 200, 4)}
        P = X.panel_from_series(sd, {})
        return P, S.make_ctx(P)

    def test_numbers_unchanged_only_reason_label(self):
        P, ctx = self.panel()
        ev = S.h2_events(ctx, 3)
        names = P['syms']
        self.assertTrue({'bingx:AAA', 'bingx:INACT'} <= {names[s] for _e, s in ev})
        flag = np.array([n == 'bingx:AAA' for n in names])                             # INACT — неактивен в markets → делистинг
        r0 = S.sim_h2(ctx, ev, FEE)
        r1 = S.sim_h2(ctx, ev, FEE, data_end=flag)
        self.assertTrue(np.array_equal(r0['r'], r1['r']))                              # числа симуляции те же
        self.assertEqual(r0['meta']['delist'], 2)
        self.assertEqual((r1['meta']['data_end'], r1['meta']['delist']), (1, 1))
        self.assertNotIn('data_end', r0['meta'])                                       # без параметра meta прежний
        flag_all = np.array([n.startswith('bingx:') for n in names])
        r2 = S.sim_h2(ctx, ev, FEE, data_end=flag_all)
        self.assertEqual((r2['meta']['data_end'], r2['meta']['delist']), (2, 0))

    def test_gateio_symbol_stopping_early_is_still_delist(self):
        P, ctx = self.panel()
        sd = {'gateio:ZZZ': series(D0, 300, 1), 'gateio:GG': series(D0 + 40, 20, 5)}
        P2 = X.panel_from_series(sd, {})
        ctx2 = S.make_ctx(P2)
        flag = np.array([False] * len(P2['syms']))
        r = S.sim_h2(ctx2, S.h2_events(ctx2, 3), FEE, data_end=flag)
        self.assertEqual((r['meta']['data_end'], r['meta']['delist']), (0, 1))

    def test_panel_end_data_flags_by_exchange_and_markets_activity(self):
        d = tempfile.mkdtemp()
        bdir = tempfile.mkdtemp()
        T9.BF.write_battery(bdir, n_syms=18)
        T9.write_xdata_ext(d)
        idx = X.load_index(d)
        bn = sorted(idx['markets']['bingx'])
        inactive = next(s for s in bn if s.startswith('NEW'))
        idx['markets']['bingx'][inactive]['active'] = False
        X.save_index(d, idx)
        P, uni, ref = H2R.build_repl_panel(d, bdir, ('bingx', 'okx', 'gateio', 'mexc'))
        ed = dict(zip(P['syms'], P['end_data']))
        self.assertTrue(any(k.startswith('bingx:') and v for k, v in ed.items()))
        self.assertTrue(any(k.startswith('okx:') and v for k, v in ed.items()))
        self.assertFalse(any(k.startswith(('gateio:', 'mexc:')) and v for k, v in ed.items()))
        self.assertFalse(ed.get(f'bingx:{inactive}', True) if f'bingx:{inactive}' in ed else False)

    def test_h2ext_sim_uses_flag_and_report_counts(self):
        import research_h2ext as HX
        import research_battery as RB
        d, bdir = tempfile.mkdtemp(), tempfile.mkdtemp()
        T9.BF.write_battery(bdir, n_syms=18)
        T9.write_xdata_ext(d)
        P, uni, ref = H2R.build_repl_panel(d, bdir, T9.VEN)
        env = H2R.make_env(P)
        fees = RB.fees_of({'fee_taker': 0.0005, 'fee_maker': 0.0002})
        ev = HX.all_events(env)
        r_new = HX.sim(env, fees, ev)
        r_old = RB.run_cell(('H2', 'D3'), env, fees)
        self.assertTrue(np.array_equal(r_new['r'], r_old['r']))                         # доходности побитово прежние
        self.assertEqual(r_new['meta']['delist'] + r_new['meta']['data_end'], r_old['meta']['delist'])
        seg = np.arange(P['nd']) >= HX.first_day(ev)
        ref_ = HX.reference(env, r_new['r'], fees, ev, seg)
        self.assertEqual(ref_['data_end'], r_new['meta']['data_end'])
        txt = '\n'.join(HX.final_lines(HX.evaluate(env['days'], r_new['r'], r_new['r'], seg, 1.0, {}), ref_))
        self.assertIn('конец данных', txt)

    def test_coverage_lines_cache_end_vs_stale(self):
        d = tempfile.mkdtemp()
        T9.write_xdata_ext(d)
        idx = X.load_index(d)
        now_ms = int(np.datetime64('2026-11-25', 'ms').astype(np.int64))
        txt = '\n'.join(X.coverage_lines(d, idx, now_ms))
        self.assertIn('bingx: свечей', txt)
        self.assertRegex(txt, r'bingx: .*конец кэша \d{4}-\d\d-\d\d')
        self.assertRegex(txt, r'okx: .*конец кэша')
        self.assertRegex(txt, r'gateio: .*не обновлявшихся > 3 суток')
        bingx_line = [l for l in txt.split('\n') if l.strip().startswith('bingx: свечей')][0]
        self.assertNotIn('не обновлявшихся', bingx_line)


# ── /venue_probe ────────────────────────────────────────────────────────────────────────────────────
class Probe(unittest.TestCase):
    def test_bitget_probe_lists_every_form_with_http_code_rows_and_dates(self):
        ex = strict_bitget(max_window_bars=30)
        txt = run(V.probe_venue('bitget', 'BTC', {'bitget': ex}))
        for needle in ('/venue_probe bitget BTC', 'контрактов USDT-перпетуалов: 1', 'свечи[0] history-candles', 'свечи[5] candles', 'фандинг[0]', 'фандинг[2]', 'HTTP 400', '40017', 'HTTP 200', 'строк 30',
                       'первая 20', 'последняя 2025-01-01', 'GET https://api.bitget.com/api/v2/mix/market/history-candles?symbol=BTCUSDT&productType=USDT-FUTURES&granularity=1D'):
            self.assertIn(needle, txt)
        self.assertNotRegex(txt, r'(?i)api[_-]?key|secret|signature|passphrase')
        self.assertIn('НЕТ в списке', run(V.probe_venue('bitget', 'ZZZ', {'bitget': strict_bitget()})))
        self.assertIn('не из списка', run(V.probe_venue('binance', 'BTC')))

    def test_generic_probe_for_gateio(self):
        async def aget(path, params):
            return T9.gate_get(path, params)
        g = V.GatePublic(get=aget, sleep=T9.nosleep, pause=0.0, now=lambda: T9.NOW)
        txt = run(V.probe_venue('gateio', 'BTC', {'gateio': g}))
        self.assertIn('свечи 1d: HTTP 200; строк', txt)
        self.assertIn('фандинг: строк', txt)

    def test_command_and_task_registered(self):
        pool, allv = a.command_pools()
        self.assertIn('/venue_probe', allv)
        self.assertIsNone(a.command_hint('/venue_probe'))
        import research_runner as R
        self.assertIn('venue_probe', R.TASKS)
        self.assertIn('/venue_probe', open(os.path.join(ROOT, 'async_bot.py'), encoding='utf-8').read())


class Docs(unittest.TestCase):
    def test_records(self):
        src = open(os.path.join(ROOT, 'BOT_SPEC.md'), encoding='utf-8').read()
        for needle in ('### 2.61.', '#### 2.61.1', '#### 2.61.2', '#### 2.61.3', '#### 2.61.4', 'H2EXT-EXCLUDE:', '| v131 |'):
            self.assertIn(needle, src)
        self.assertIn('2026-11-25-v131', [x[0] for x in a.CHANGELOG])
        self.assertIn('| v131 |', src)
        self.assertEqual([l for l in src.split('\n') if l.startswith('H2EXT-EXCLUDE:')], [])        # маркер исключения пока не записан


if __name__ == '__main__':
    unittest.main()
