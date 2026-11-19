"""v118: шаг 0 — проба доступа Bybit/OKX (§2.50), причина сбоя эквити в /status без секретов, загрузка не падает из-за одной биржи,
X1 на паре (BingX, Binance): ставки из архива по фактическим отметкам времени, 1h-свечи архива."""
import io
import os
import tempfile
import unittest
import zipfile

import numpy as np

import _common
import access_probe as AP
import battery_fixtures as BF
import research_bdata as B
import research_vision as V
import research_xdata as X
import research_xfund as XF
from _common import a, fresh_state
from test_v116_h2x1 import mk_bars, mk_fund, x1_inputs, D0, END, DAY, nosleep, run

SECRET_KEY = 'AbCdEf0123456789AbCdEf0123456789XyZ'


class StatusReason(unittest.TestCase):
    def setUp(self):
        fresh_state()
        a.BINGX_KEY, a.BINGX_SECRET = SECRET_KEY, 'SeCrEtSeCrEtSeCrEt0123'
        self._ex, self._pauses = a.exchange_priv, a.EQUITY_RETRY_PAUSES
        a.EQUITY_RETRY_PAUSES = ()

    def tearDown(self):
        a.exchange_priv, a.EQUITY_RETRY_PAUSES = self._ex, self._pauses
        a.BINGX_KEY = a.BINGX_SECRET = ''

    def test_class_and_exchange_code(self):
        class AuthenticationError(Exception):
            pass
        e = AuthenticationError('bingx {"code":100413,"msg":"Incorrect apiKey","timestamp":1}')
        self.assertEqual(a.safe_exc_reason(e), 'AuthenticationError 100413 Incorrect apiKey')

    def test_secrets_are_cut(self):
        e = RuntimeError(f'bingx GET https://open-api.bingx.com/x?timestamp=1&signature=deadbeef01234567890123456789012345 apiKey={SECRET_KEY} '
                         f'header X-BX-APIKEY: {SECRET_KEY} secret SeCrEtSeCrEtSeCrEt0123 code 100001')
        out = a.safe_exc_reason(e)
        for bad in (SECRET_KEY, 'SeCrEtSeCrEtSeCrEt0123', 'deadbeef0123', 'signature=de'):
            self.assertNotIn(bad, out)
        self.assertTrue(out.startswith('RuntimeError'))
        self.assertLessEqual(len(out), 160)

    def test_status_and_alert_show_reason_then_clear(self):
        class Boom(Exception):
            pass

        class Ex:
            async def fetch_balance(self_):
                raise Boom('{"code":100413,"msg":"Incorrect apiKey"} ' + SECRET_KEY)
        a.exchange_priv = Ex()
        sent = []

        async def tg(t):
            sent.append(t)
        a.tg = tg
        for _ in range(3):
            self.assertFalse(run(a.dd_update()))
        self.assertEqual(a._dd['fails'], 3)
        self.assertIn('Boom 100413 Incorrect apiKey', a.dd_status_text())
        self.assertIn('Boom 100413 Incorrect apiKey', a.health_status_line())
        alert = [t for t in sent if 'DD-контроль' in t and '3 сбоя' in t]
        self.assertEqual(len(alert), 1)
        self.assertIn('Boom 100413 Incorrect apiKey', alert[0])
        for t in sent + [a.dd_status_text(), a.health_status_line()]:
            self.assertNotIn(SECRET_KEY, t)

        class Ok:
            async def fetch_balance(self_):
                return {'USDT': {'total': 75.0}}
        a.exchange_priv = Ok()
        self.assertTrue(run(a.dd_update()))
        self.assertEqual(a._dd['last_err'], '')
        self.assertNotIn('причина', a.dd_status_text())


def resp(status, body='', **hdr):
    return {'dt': 0.1, 'status': status, 'headers': {k.replace('_', '-'): v for k, v in hdr.items()}, 'body': body, 'err': None}


class Probe(unittest.TestCase):
    def test_verdicts(self):
        ok = resp(200, '{"retCode":0,"retMsg":"OK","result":{}}')
        geo = resp(403, '<html>The Amazon CloudFront distribution is configured to block access from your country.</html>', server='CloudFront', x_cache='Error from cloudfront')
        lim = resp(429, '{"retCode":10006}', retry_after='5')
        self.assertEqual(AP.classify_http(ok), 'доступно')
        self.assertEqual(AP.classify_http(resp(200, '{"code":"0","data":[]}')), 'доступно')
        self.assertEqual(AP.classify_http(geo), 'гео-блок')
        self.assertEqual(AP.classify_http(resp(451, '')), 'гео-блок')
        self.assertEqual(AP.classify_http(lim), 'лимит частоты')
        self.assertEqual(AP.classify_http(resp(500, 'x')), 'не определено')
        self.assertEqual(AP.classify_http({'status': None, 'headers': {}, 'body': '', 'err': 'timeout'}), 'не определено')
        self.assertEqual(AP.classify_exchange([geo, geo]), 'гео-блок')
        self.assertEqual(AP.classify_exchange([geo, ok]), 'доступно')
        self.assertEqual(AP.classify_exchange([lim]), 'лимит частоты')
        self.assertEqual(AP.classify_exchange([]), 'не определено')

    def test_probe_all_with_fakes_headers_body_and_ccxt_class(self):
        class Resp:
            def __init__(self, st, body, hdr):
                self.status, self._b, self.headers = st, body, hdr

            async def read(self):
                return self._b

            async def __aenter__(self):
                return self

            async def __aexit__(self, *x):
                return False

        class Sess:
            def get(self_, url, timeout=None):
                if 'bybit' in url:
                    return Resp(403, ('<html>' + 'x' * 500 + 'block access from your country</html>').encode(), {'Server': 'CloudFront', 'X-Cache': 'Error from cloudfront', 'Date': 'x'})
                return Resp(200, b'{"code":"0","data":[{"last":"1"}]}', {'Server': 'cloudflare'})

        async def ccxt_fn(ex_id):
            return {'dt': 0.2, 'ok': ex_id == 'okx', 'exc': None if ex_id == 'okx' else 'RateLimitExceeded: bybit 403', 'n': 5 if ex_id == 'okx' else 0}
        res = run(AP.probe_all(session=Sess(), ccxt_fn=ccxt_fn))
        self.assertEqual(res['bybit']['verdict'], 'гео-блок')
        self.assertEqual(res['okx']['verdict'], 'доступно')
        r0 = res['bybit']['http'][0]
        self.assertEqual(r0['status'], 403)
        self.assertEqual(len(r0['body']), 300)                                        # первые 300 байт
        self.assertEqual(sorted(r0['headers']), ['server', 'x-cache'])                 # только нужные заголовки
        txt = '\n'.join(AP.format_probe(res))
        self.assertIn('bybit: гео-блок', txt)
        self.assertIn('RateLimitExceeded', txt)                                        # имя класса исключения ccxt
        self.assertIn('okx: доступно', txt)

    def test_probe_never_raises(self):
        class Sess:
            def get(self_, *x, **k):
                raise OSError('нет сети')

        async def bad(ex_id):
            raise RuntimeError('x')
        res = run(AP.probe_all(session=Sess(), ccxt_fn=bad))
        self.assertEqual(res['bybit']['verdict'], 'не определено')
        self.assertIn('ИСКЛЮЧЕНИЕ', '\n'.join(AP.format_probe(res)))

    def test_net_probe_includes_access_block(self):
        async def fake_all(*x, **k):
            return {'bybit': {'http': [], 'ccxt': {'dt': 0, 'ok': False, 'exc': 'RateLimitExceeded', 'n': 0}, 'verdict': 'гео-блок'}}
        saved = a._ap.probe_all
        a._ap.probe_all = fake_all
        try:
            self.assertIn('bybit: гео-блок', a.net_probe_access_text({'access': run(fake_all())}))
            self.assertEqual(a.net_probe_access_text({}), '')
        finally:
            a._ap.probe_all = saved


class Load(unittest.TestCase):
    def make_ex(self, markets, bars, funding, fail=None):
        class Ex:
            async def load_markets(self_):
                if fail:
                    raise fail
                return markets

            async def fetch_ohlcv(self_, sym, tf, since, limit):
                b = bars[sym]
                if tf == '1h':
                    return []
                return [[int(t), o, h, l, c, v] for t, o, h, l, c, v in zip(b['ts'], b['o'], b['h'], b['l'], b['c'], b['v']) if t >= since][:limit]

            async def fetch_funding_rate_history(self_, sym, since=None, limit=None, params=None):
                ts, r = funding[sym]
                end = (params or {}).get('until', (params or {}).get('endTime'))
                sel = [(int(t), float(x)) for t, x in zip(ts, r) if end is None or t <= end][-limit:]
                return [{'timestamp': t, 'fundingRate': x} for t, x in sel]

            async def close(self_):
                return None
        return Ex()

    def test_unavailable_exchange_does_not_stop_the_load(self):
        class RateLimitExceeded(Exception):
            pass
        bdir = tempfile.mkdtemp()
        BF.write_battery(bdir, n_syms=18)
        mk = {'NEW02/USDT:USDT': {'swap': True, 'linear': True, 'quote': 'USDT', 'settle': 'USDT', 'base': 'NEW02', 'active': True}}
        b2 = mk_bars(D0 + 120, 300, 6, qv=6e7)
        f2 = mk_fund(D0 + 120, 300, 8)
        by = self.make_ex({}, {}, {}, fail=RateLimitExceeded('bybit 403'))
        bn = self.make_ex(mk, {'NEW02/USDT:USDT': b2}, {'NEW02/USDT:USDT': f2})
        xdir = tempfile.mkdtemp()
        os.environ['HTF_FETCH_PAUSE_SEC'] = '0'
        try:
            txt = run(X.run_xload({'xdir': xdir, 'bdir': bdir, 'max_minutes': 5}, None, exchanges={'bybit': by, 'bingx': bn}, now_ms=(END + 1) * DAY, sleep=nosleep))
        finally:
            os.environ.pop('HTF_FETCH_PAUSE_SEC', None)
        self.assertIn('bybit: недоступна', txt)
        self.assertIn('RateLimitExceeded', txt)
        self.assertNotIn('load_markets не удался', txt)
        idx = X.load_index(xdir)
        self.assertIn('NEW02/USDT:USDT', idx['bars']['bingx'])                        # BingX загружен
        self.assertEqual(idx['bars']['bybit'], {})
        self.assertEqual(idx['unavailable'], {'bybit': 'RateLimitExceeded'})

    def test_all_unavailable_reports_and_returns(self):
        class Boom(Exception):
            pass
        bdir = tempfile.mkdtemp()
        BF.write_battery(bdir, n_syms=18)
        e1, e2 = self.make_ex({}, {}, {}, fail=Boom()), self.make_ex({}, {}, {}, fail=Boom())
        txt = run(X.run_xload({'xdir': tempfile.mkdtemp(), 'bdir': bdir}, None, exchanges={'bybit': e1, 'bingx': e2}, now_ms=(END + 1) * DAY, sleep=nosleep))
        self.assertIn('все биржи недоступны', txt)

    def test_venues_param_skips_bybit_and_never_calls_it(self):
        called = []

        class Spy:
            async def load_markets(self_):
                called.append(1)
                return {}

            async def close(self_):
                return None
        bdir = tempfile.mkdtemp()
        BF.write_battery(bdir, n_syms=18)
        mk = {'NEW02/USDT:USDT': {'swap': True, 'linear': True, 'quote': 'USDT', 'settle': 'USDT', 'base': 'NEW02', 'active': True}}
        bn = self.make_ex(mk, {'NEW02/USDT:USDT': mk_bars(D0 + 120, 300, 6, qv=6e7)}, {'NEW02/USDT:USDT': mk_fund(D0 + 120, 300, 8)})
        os.environ['HTF_FETCH_PAUSE_SEC'] = '0'
        try:
            txt = run(X.run_xload({'xdir': tempfile.mkdtemp(), 'bdir': bdir, 'venues': ('bingx',)}, None, exchanges={'bybit': Spy(), 'bingx': bn}, now_ms=(END + 1) * DAY, sleep=nosleep))
        finally:
            os.environ.pop('HTF_FETCH_PAUSE_SEC', None)
        self.assertEqual(called, [])
        self.assertIn('пропущена по пре-регистрации', txt)


class SpecMarkers(unittest.TestCase):
    def test_markers_gate_the_amendments(self):
        d = tempfile.mkdtemp()
        p = os.path.join(d, 'spec.md')
        open(p, 'w', encoding='utf-8').write('текст\n')
        self.assertEqual(X.x1_pair_from_spec(p), 'bybit')
        self.assertEqual(X.venues_from_spec(p), X.EXCHANGES)
        open(p, 'w', encoding='utf-8').write('X1-MAIN-PAIR: BingX/Binance\nH2REPL-VENUES: bingx\n')
        self.assertEqual(X.x1_pair_from_spec(p), 'binance')
        self.assertEqual(X.venues_from_spec(p), ('bingx',))
        self.assertEqual(X.x1_pair_from_spec(os.path.join(d, 'нет.md')), 'bybit')
        # в самом BOT_SPEC маркеров ещё нет: условные поправки применяются только после вердикта пробы (§2.50)
        self.assertEqual(X.x1_pair_from_spec(), 'bybit')
        self.assertEqual(X.venues_from_spec(), X.EXCHANGES)


def make_zip(name, text):
    bio = io.BytesIO()
    with zipfile.ZipFile(bio, 'w') as z:
        z.writestr(name, text)
    return bio.getvalue()


class BinanceX1(unittest.TestCase):
    def test_sync_h1_from_archive_and_resume(self):
        d = tempfile.mkdtemp()
        calls = []

        async def fetch(url, params=None, method='GET'):
            calls.append(url)
            if url == V.S3:
                pre = params['prefix']
                self.assertTrue(pre.endswith('/1h/'))
                keys = [f'{pre}NEWAUSDT-1h-2021-01.zip', f'{pre}NEWAUSDT-1h-2021-02.zip']
                return {'status': 200, 'headers': {}, 'body': ('<ListBucketResult>' + ''.join(f'<Key>{k}</Key>' for k in keys) + '<IsTruncated>false</IsTruncated></ListBucketResult>').encode(), 'error': ''}
            if url.endswith('.CHECKSUM'):
                return {'status': 404, 'headers': {}, 'body': b'', 'error': ''}
            name = url.split('/')[-1][:-4]
            y, m = name[-7:].split('-')
            t0 = int(np.datetime64(f'{y}-{m}-01', 'D').astype(np.int64)) * DAY
            text = ''.join(f'{t0 + i * B.HOUR_MS},1,2,0.5,1.5,10,0,{5e6},3,1,1,0\n' for i in range(48))
            return {'status': 200, 'headers': {}, 'body': make_zip(name + '.csv', text), 'error': ''}
        r = run(B.sync_h1(fetch, d, ['NEWAUSDT']))
        self.assertEqual((r['loaded'], r['failed']), (2, 0))
        z = B.load_h1(d, 'NEWAUSDT')
        self.assertEqual(len(z['ts']), 96)
        self.assertEqual(z['ts'][1] - z['ts'][0], B.HOUR_MS)
        self.assertEqual(z['h'].dtype, np.float32)
        n = len(calls)
        run(B.sync_h1(fetch, d, ['NEWAUSDT']))
        self.assertEqual(len(calls), n)                                              # повтор: загруженные месяцы не запрашиваются

    def _archive(self, rates_ts, rates):
        """Архив Binance: символ NEWAUSDT — дневные свечи, 1h-свечи, фандинг по заданным (нерегулярным) отметкам времени."""
        bdir = tempfile.mkdtemp()
        nd = 120
        day0 = 20000
        base = np.exp(np.cumsum(np.random.default_rng(5).normal(0, 0.01, nd)))
        ts = (day0 + np.arange(nd)) * DAY
        hc = np.empty((nd, 24))
        hc[:, 0] = 100.0 * base
        hc[:, 1:] = (100.0 * base)[:, None]
        hh, hl = hc * 1.0005, hc * 0.9995
        B.save_d1(bdir, 'NEWAUSDT', ts, {'o': hc[:, 0], 'h': hh.max(axis=1), 'l': hl.min(axis=1), 'c': hc[:, 23].copy(), 'qv': np.full(nd, 1e8)}, ['2024-10'])
        hts = (day0 * 24 + np.arange(nd * 24)) * B.HOUR_MS
        B.save_h1(bdir, 'NEWAUSDT', hts, {'h': hh.reshape(-1), 'l': hl.reshape(-1), 'c': hc.reshape(-1)}, ['2024-10'])
        B.save_fund(bdir, 'NEWAUSDT', np.asarray(rates_ts, np.int64), {'rate': np.asarray(rates)}, ['2024-10'])
        idx = B.load_index(bdir)
        idx['symbols'] = ['NEWAUSDT']
        B.save_index(bdir, idx)
        return bdir, day0, nd, base

    def test_binance_funding_read_by_actual_timestamps(self):
        day0, nd = 20000, 120
        # Binance: нерегулярные отметки (4 события в сутки в разное время), ставка 0.0001; BingX: 3 события в сутки, 0
        rts = np.sort(np.concatenate([(day0 + np.arange(nd)) * DAY + h * B.HOUR_MS + 1000 for h in (1, 5, 13, 22)])).astype(np.int64)
        bdir, day0, nd, base = self._archive(rts, np.full(len(rts), 0.0001))
        bn = XF.binance_venue(bdir, {'NEWA'}, 0.0005, with_h1=True)
        self.assertEqual(list(bn['fund']['NEWA'][0][:4]), list(rts[:4]))             # отметки времени — как в архиве, без пересчёта на сетку
        self.assertEqual(len(bn['hbars']['NEWA']['ts']), nd * 24)
        self.assertEqual(bn['fee'], 0.0005)
        # прямая проверка начислений: нога Binance получает ставки строго по отметкам (t0, t1]
        got = XF.leg_funding(bn['fund']['NEWA'], day0 * DAY, (day0 + 3) * DAY, True, 1.0)
        self.assertEqual(len(got), 12)                                               # 3 суток × 4 события (а не 3 × 3 из 8-ч сетки)
        self.assertAlmostEqual(sum(x for _d, x in got), 12 * 0.0001, places=12)
        self.assertEqual(sorted({d for d, _x in got}), [day0, day0 + 1, day0 + 2])

    def test_x1_on_binance_pair_runs_and_collects_funding(self):
        day0, nd = 20000, 120
        rts = np.sort(np.concatenate([(day0 + np.arange(nd)) * DAY + h * B.HOUR_MS + 1000 for h in (1, 5, 13, 22)])).astype(np.int64)
        bdir, day0, nd, base = self._archive(rts, np.full(len(rts), 0.0))            # Binance: ставка 0
        bn = XF.binance_venue(bdir, {'NEWA'}, 0.0005, with_h1=True)
        vA, _vB, _p = x1_inputs(nd=nd, rate_a=0.0007, rate_b=0.0)                    # BingX: 0.07% за расчёт, 3 в сутки
        vA['bars'] = {'NEWA_BX': vA['bars']['sa0']}
        vA['fund'] = {'NEWA_BX': vA['fund']['sa0']}
        vA['hbars'] = {'NEWA_BX': vA['hbars']['sa0']}
        vA['markets'] = {'NEWA_BX': {'norm': 'NEWA'}}
        pairs = XF.main_pairs_binance(vA, bn)
        self.assertEqual(pairs, [('NEWA', 'NEWA_BX', 'NEWA')])
        inp, rej = XF.build_inputs(vA, bn, pairs)
        self.assertIsNotNone(inp, rej)
        self.assertEqual(inp['fund'][0][1][0].tolist()[:2], rts[:2].tolist())         # ставки Binance в расчёт идут из архива как есть
        res = XF.simulate_x1(inp, 0.0002)
        self.assertGreater(len(res['trades']) + res['open_end'], 0)
        self.assertGreater(float(res['fund'].sum()), 0)                              # шорт на BingX получает фандинг


class SyncBinanceH1(unittest.TestCase):
    def test_h1_only_for_bases_loaded_from_bingx(self):
        xdir, bdir = tempfile.mkdtemp(), tempfile.mkdtemp()
        BF.write_battery(bdir, n_syms=18)                                            # в архиве: H16 + COIN00, COIN01
        idx = X.load_index(xdir)
        idx['markets']['bingx'] = {'COIN00/USDT:USDT': {'norm': 'COIN00'}, 'XYZ/USDT:USDT': {'norm': 'XYZ'}}
        idx['fund']['bingx'] = {'COIN00/USDT:USDT': {'n': 1}, 'XYZ/USDT:USDT': {'n': 1}}
        X.save_index(xdir, idx)
        seen = []

        async def fetch(url, params=None, method='GET'):
            if url == V.S3:
                seen.append(params['prefix'])
                return {'status': 200, 'headers': {}, 'body': b'<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>', 'error': ''}
            return {'status': 404, 'headers': {}, 'body': b'', 'error': ''}
        txt = run(XF.sync_binance_h1({'xdir': xdir, 'bdir': bdir}, None, fetch))
        self.assertEqual(len(seen), 1)                                               # XYZ нет в архиве, остальные базы BingX не загружались
        self.assertIn('/COIN00USDT/1h/', seen[0])
        self.assertIn('символов 1/1', txt)


class SourcesGuard(unittest.TestCase):
    def test_no_order_methods_in_new_code(self):
        for fn in ('access_probe.py', 'research_xfund.py', 'research_xdata.py', 'research_bdata.py'):
            src = open(os.path.join(_common.ROOT, fn), encoding='utf-8').read().lower()
            for w in ('create_order', 'create_market', 'create_limit', 'cancel_order'):
                self.assertNotIn(w, src, fn)


if __name__ == '__main__':
    unittest.main()
