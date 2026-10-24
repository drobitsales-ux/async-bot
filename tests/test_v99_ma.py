"""v99 (этап 1 §2.41): парсеры Stooq/Dukascopy на фикстурах, проба источников, подсчёт N по библиотеке, защита LOCKBOX."""
import asyncio
import lzma
import os
import struct
import unittest
from datetime import datetime, timezone

import numpy as np

from _common import ROOT
import research_ma as M
import research_ma_lib as L
import research_runner as RR


def run(coro):
    return asyncio.run(coro)


def ms(y, m, d, h=0):
    return int(datetime(y, m, d, h, tzinfo=timezone.utc).timestamp() * 1000)


STOOQ_OK = """Date,Open,High,Low,Close,Volume
2018-12-31,1280.5,1285.0,1275.0,1282.2,0
2019-01-02,1282.2,1300.0,1280.0,1296.0,0
2019-01-03,1296.0,1298.0,1285.0,1290.5,0
2019-01-04,1290.5,1295.0,1288.0,1291.0,0
2019-01-05,1291.0,1291.0,1291.0,1291.0,0
2019-01-07,1291.0,1296.0,1285.0,1294.0,0
"""
STOOQ_NOVOL = "Date,Open,High,Low,Close\n2024-01-02,1.10,1.11,1.09,1.105\n2024-01-03,1.105,1.12,1.10,1.115\n"


class StooqParser(unittest.TestCase):
    def test_parse_ok(self):
        p = M.parse_stooq_csv(STOOQ_OK)
        self.assertEqual((p['n'], p['first'], p['last']), (6, '2018-12-31', '2019-01-07'))
        self.assertEqual(p['cols'], ['Date', 'Open', 'High', 'Low', 'Close', 'Volume'])
        self.assertEqual(p['weekend_rows'], 1)                                       # 2019-01-05 — суббота
        self.assertEqual(p['missing_weekdays'], 1)                                   # 2019-01-01 (праздник) отсутствует
        self.assertEqual(int(p['ts'][1]), ms(2019, 1, 2))                           # 00:00 UTC даты
        self.assertAlmostEqual(float(p['c'][1]), 1296.0)

    def test_unsorted_duplicates_and_missing_weekdays(self):
        txt = "Date,Open,High,Low,Close\n2024-01-05,1,1,1,1\n2024-01-02,1,1,1,1\n2024-01-02,1,1,1,1\n"
        p = M.parse_stooq_csv(txt)
        self.assertTrue(p['unsorted'])
        self.assertEqual(p['duplicates'], 1)
        self.assertEqual(p['missing_weekdays'], 2)                                   # 03.01 и 04.01 отсутствуют
        self.assertFalse(p['has_volume'])

    def test_no_volume_column(self):
        p = M.parse_stooq_csv(STOOQ_NOVOL)
        self.assertEqual(p['n'], 2)
        self.assertTrue(np.isnan(p['v']).all())

    def test_bad_responses(self):
        for body, needle in (('', 'пустой'), ('No data', 'нет данных'), ('Get your apikey: https://stooq.com/q/d/?s=xauusd&get_apikey', 'apikey'),
                             ('<html><body>Oops</body></html>', 'не CSV'), ('Date,Open\n2024-01-01,1', 'не CSV')):
            with self.assertRaises(M.ParseError) as cm:
                M.parse_stooq_csv(body)
            self.assertIn(needle, str(cm.exception))

    def test_verdict(self):
        now = ms(2026, 10, 25)
        rows = ['Date,Open,High,Low,Close'] + [f'{y}-{m:02d}-{d:02d},1,1,1,1' for y in range(2018, 2027) for m in range(1, 13) for d in range(1, 29)
                                              if datetime(y, m, d).weekday() < 5 and datetime(y, m, d) <= datetime(2026, 10, 24)]
        p = M.parse_stooq_csv('\n'.join(rows))
        ok, why = M.stooq_verdict(p, now)
        self.assertTrue(ok, why)
        p2 = dict(p, first='2021-01-04')
        self.assertFalse(M.stooq_verdict(p2, now)[0])
        p3 = dict(p, last='2026-09-01')
        self.assertIn('старше 10 дней', M.stooq_verdict(p3, now)[1])
        p4 = dict(p, weekday_coverage=0.5)
        self.assertIn('покрытие', M.stooq_verdict(p4, now)[1])


def duka_month_bytes(year, month, order='ocLH', price=1.10, scale=1e5, skip_weekend=True, absolute=False):
    """Фикстура: месячный файл часовых свечей Dukascopy (LZMA alone), записи >IIIIIf; неделя FX: пятница 21:00 UTC — воскресенье 22:00 UTC закрыто."""
    start = datetime(year, month, 1, tzinfo=timezone.utc)
    recs = b''
    t = 0
    rnd = np.random.default_rng(1)
    px = price
    while True:
        dt = datetime.fromtimestamp(start.timestamp() + t, timezone.utc)
        if dt.month != month:
            break
        wd, h = dt.weekday(), dt.hour
        closed = (wd == 5) or (wd == 4 and h >= 22) or (wd == 6 and h < 22)
        if not (skip_weekend and closed):
            o = px
            c = px * (1 + rnd.normal(0, 0.0005))
            hi = max(o, c) * (1 + abs(rnd.normal(0, 0.0003)))
            lo = min(o, c) * (1 - abs(rnd.normal(0, 0.0003)))
            raw = [int(round(x * scale)) for x in ((o, c, lo, hi) if order == 'ocLH' else (o, hi, lo, c))]
            stamp = int(start.timestamp() + t) if absolute else t
            recs += struct.pack('>IIIIIf', stamp, *raw, 12.5)
            px = c
        t += 3600
    return lzma.compress(recs, format=lzma.FORMAT_ALONE), start


class DukascopyParser(unittest.TestCase):
    def test_roundtrip_ocLH(self):
        body, start = duka_month_bytes(2026, 9)
        raw = M.decode_bi5(body)
        self.assertEqual(len(raw) % 24, 0)
        p = M.parse_duka_candles(raw, int(start.timestamp() * 1000), 'ocLH')
        self.assertTrue(p['rel_time'])
        self.assertGreater(p['n'], 400)
        self.assertEqual(M.ohlc_consistency(p), 1.0)
        # неверный порядок полей распознаётся по согласованности OHLC
        wrong = M.parse_duka_candles(raw, int(start.timestamp() * 1000), 'ohlc')
        self.assertLess(M.ohlc_consistency(wrong), 0.9)
        self.assertEqual(M.pick_scale(float(np.median(p['c'])), *M.DUKA_PRICE_RANGE['EURUSD']), 1e5)

    def test_roundtrip_ohlc_order_and_absolute_time(self):
        body, start = duka_month_bytes(2026, 9, order='ohlc', absolute=True)
        p = M.parse_duka_candles(M.decode_bi5(body), int(start.timestamp() * 1000), 'ohlc')
        self.assertFalse(p['rel_time'])
        self.assertEqual(M.ohlc_consistency(p), 1.0)
        self.assertEqual(int(p['ts'][0]), int(start.timestamp() * 1000))

    def test_analysis_weekend_and_reopen_hour(self):
        body, start = duka_month_bytes(2026, 9)
        ps = int(start.timestamp() * 1000)
        p = M.parse_duka_candles(M.decode_bi5(body), ps)
        an = M.duka_analyze(p, 'EURUSD', ps)
        self.assertTrue(an['monotonic'])
        self.assertEqual(an['weekend_bars'], 8)                                     # воскресные бары 22–23 ч UTC (открытие недели) × 4 воскресенья сентября 2026
        self.assertGreater(an['hourly_share'], 0.9)
        self.assertEqual(an['reopen_hours_utc'], [22])                              # открытие недели в воскресенье 22:00 UTC
        self.assertEqual(an['reopen_weekdays'], [6])
        self.assertTrue(an['in_period'])

    def test_bad_inputs(self):
        with self.assertRaises(M.ParseError):
            M.decode_bi5(b'not lzma at all')
        with self.assertRaises(M.ParseError):
            M.parse_duka_candles(b'\x00' * 25, 0)
        with self.assertRaises(M.ParseError):
            M.parse_duka_candles(b'', 0)
        with self.assertRaises(M.ParseError):
            M.parse_duka_candles(b'\x00' * 24, 0, 'zzz')

    def test_pick_scale(self):
        self.assertEqual(M.pick_scale(1850.5 * 1e3, 300, 8000), 1e3)
        self.assertEqual(M.pick_scale(150.2 * 1e3, 50, 220), 1e3)
        self.assertIsNone(M.pick_scale(5.0e9, 50, 220))

    def test_urls(self):
        self.assertEqual(M.duka_hour_url('XAUUSD', 2024, 0), 'https://datafeed.dukascopy.com/datafeed/XAUUSD/2024/00/BID_candles_hour_1.bi5')
        self.assertEqual(M.duka_tick_url('EURUSD', 2024, 11, 5, 7), 'https://datafeed.dukascopy.com/datafeed/EURUSD/2024/11/05/07h_ticks.bi5')


def fake_fetch_factory(stooq_ok=True):
    """Поддельный fetch: Stooq отдаёт фикстуру (для xauusd — apikey-заглушку, для xptusd — 404), Dukascopy — месячные файлы с 2012 года."""
    calls = []
    full_rows = ['Date,Open,High,Low,Close,Volume'] + [f'{y}-{m:02d}-{d:02d},1,1,1,1,0' for y in range(2010, 2027) for m in range(1, 13) for d in range(1, 29)
                                                      if datetime(y, m, d).weekday() < 5 and datetime(y, m, d) <= datetime(2026, 10, 24)]
    full_csv = '\n'.join(full_rows).encode()

    async def fetch(url, params=None, method='GET'):
        calls.append(url)
        ok = lambda body: {'status': 200, 'headers': {}, 'body': body, 'error': ''}
        if 'stooq.com' in url:
            if 's=xauusd' in url:
                return ok(b'Get your apikey: https://stooq.com/q/d/?s=xauusd&get_apikey')
            if 's=xptusd' in url:
                return {'status': 404, 'headers': {}, 'body': b'', 'error': ''}
            if 's=cl.f' in url or 's=zn.f' in url:
                return ok(full_csv)
            if 's=' in url and any(x in url for x in ('cb.f', 'ng.f', 'zb.f', 'zf.f', 'zt.f', 'gc.f')):
                return ok(b'No data')
            return ok(full_csv)
        if 'dukascopy' in url:
            parts = url.split('/datafeed/')[1].split('/')
            sym, year = parts[0], int(parts[1])
            if url.endswith('BID_candles_hour_1.bi5'):
                if year < 2012:
                    return {'status': 404, 'headers': {}, 'body': b'', 'error': ''}
                month0 = int(parts[2])
                body, _ = duka_month_bytes(year, month0 + 1, price={'XAUUSD': 1850.0, 'XAGUSD': 24.0, 'EURUSD': 1.10, 'GBPUSD': 1.27, 'USDJPY': 150.0}[sym],
                                           scale=1e5 if sym in ('EURUSD', 'GBPUSD') else 1e3)
                return ok(body)
            return ok(b'\x01' * 40)
        return {'status': 404, 'headers': {}, 'body': b'', 'error': ''}
    fetch.calls = calls
    return fetch


class ProbeFlow(unittest.TestCase):
    NOW = ms(2026, 10, 25)

    def test_stooq_probe(self):
        res = run(M.probe_stooq(fake_fetch_factory(), self.NOW, pause=0))
        self.assertEqual(set(res), set(M.STOOQ_CLASS) | set(M.STOOQ_EXTRA))
        self.assertFalse(res['xauusd']['ok'])
        self.assertIn('apikey', res['xauusd']['why'])
        self.assertFalse(res['xptusd']['ok'])
        self.assertIn('HTTP 404', res['xptusd']['why'])
        self.assertTrue(res['eurusd']['ok'], res['eurusd'])
        self.assertTrue(res['cl.f']['ok'])
        self.assertFalse(res['cb.f']['ok'])
        self.assertEqual(res['eurusd']['class'], 'FX')

    def test_dukascopy_probe(self):
        res = run(M.probe_dukascopy(fake_fetch_factory(), self.NOW, pause=0))
        self.assertEqual(set(res), set(M.DUKA_SYMS))
        for sym, r in res.items():
            self.assertTrue(r['ok'], (sym, r['why']))
            self.assertEqual(r['earliest_year'], 2012)
            self.assertEqual(r['depth'][2010][0], 404)
            smp = r['sample']
            self.assertEqual(smp['order'], 'ocLH')
            self.assertEqual(smp['scale'], 1e5 if sym in ('EURUSD', 'GBPUSD') else 1e3)
            self.assertEqual(r['tick']['status'], 200)
            self.assertTrue(smp['url'].endswith('BID_candles_hour_1.bi5'))

    def test_dukascopy_depth_rule(self):
        base = fake_fetch_factory()

        async def late(url, params=None, method='GET'):
            if 'dukascopy' in url and url.endswith('BID_candles_hour_1.bi5') and int(url.split('/datafeed/')[1].split('/')[1]) < 2021:
                return {'status': 404, 'headers': {}, 'body': b'', 'error': ''}
            return await base(url, params, method)
        res = run(M.probe_dukascopy(late, self.NOW, pause=0))
        self.assertFalse(res['EURUSD']['ok'])
        self.assertIn('глубина', res['EURUSD']['why'])

    def test_last_complete_month_in_january(self):
        # в январе последний завершённый месяц — декабрь прошлого года (month0 = 11)
        urls = []

        async def spy(url, params=None, method='GET'):
            urls.append(url)
            return {'status': 404, 'headers': {}, 'body': b'', 'error': ''}
        run(M.probe_dukascopy(spy, ms(2027, 1, 15), pause=0))
        self.assertTrue(any('/EURUSD/2026/11/BID_candles_hour_1.bi5' in u for u in urls))
        urls.clear()
        run(M.probe_dukascopy(spy, ms(2026, 2, 15), pause=0))
        self.assertTrue(any('/EURUSD/2026/00/BID_candles_hour_1.bi5' in u for u in urls))

    def test_report_and_proposal(self):
        f = fake_fetch_factory()
        stooq = run(M.probe_stooq(f, self.NOW, pause=0))
        duka = run(M.probe_dukascopy(f, self.NOW, pause=0))
        markets = {'GOLD/USDT:USDT': {'swap': True, 'base': 'NCCOGOLD2USD', 'id': 'NCCOGOLD2USD-USDT', 'created': ms(2025, 3, 1), 'taker': 0.0005, 'maker': 0.0002,
                                      'contractSize': 1, 'active': True, 'info': {'x': 1}},
                   'BTC/USDT:USDT': {'swap': True, 'base': 'BTC', 'id': 'BTC-USDT', 'info': {}}}
        bingx = {'rows': [M.market_row(s, markets[s]) for s in M.bingx_candidates(markets)], 'n_markets': 2}
        bingx['rows'][0]['spread_bps'] = [3.1, 2.9, 3.0, 3.4, 2.8]
        text = M.build_report(stooq, duka, bingx, self.NOW, 5000.0, 'v99')
        for needle in ('Stooq, дневные CSV', 'Dukascopy, часовые BID-свечи', 'BingX: перпетуалы', 'спред 3.0 б.п.', 'листинг 2025-03-01', 'Оценка объёма и диска',
                       'Число тестов N по формуле §2.41.5', 'ВЕРДИКТ источников', 'apikey', 'Нефть/облигации'):
            self.assertIn(needle, text)
        prop = M.proposal(stooq, duka)
        self.assertEqual(prop['classes']['METALS'], ['xagusd'])                      # xauusd — apikey, xptusd — 404
        self.assertEqual(len(prop['classes']['FX']), 5)
        self.assertEqual(len(prop['classes']['INDICES']), 4)
        cells = prop['cells']
        self.assertIn(('METALS', '1D', 1), cells)
        self.assertIn(('FX', '4h', 3), cells)                                         # Dukascopy: EUR, GBP, JPY
        self.assertIn(('METALS', '4h', 2), cells)
        self.assertEqual(prop['N'], L.count_tests(cells))

    def test_bingx_candidates_and_prepare(self):
        mk = {'A/USDT:USDT': {'swap': True, 'base': 'NCCOGOLD2USD', 'id': 'NCCOGOLD2USD-USDT'}, 'B/USDT:USDT': {'swap': True, 'base': 'NCSKSPX2USD', 'id': 'X'},
              'C/USDT:USDT': {'swap': True, 'base': 'XAU', 'id': 'XAU-USDT'}, 'D/USDT:USDT': {'swap': True, 'base': 'DOGE', 'id': 'DOGE-USDT'},
              'E/USDT': {'swap': False, 'type': 'spot', 'base': 'NCCO', 'id': 'E'}}
        self.assertEqual(M.bingx_candidates(mk), ['A/USDT:USDT', 'B/USDT:USDT', 'C/USDT:USDT'])

        class Ex:
            markets = mk

            async def fetch_ticker(self, sym):
                if sym.startswith('B'):
                    raise RuntimeError('нет тикера')
                return {'bid': 1999.0, 'ask': 2001.0}
        payload = run(M.prepare_ma_bingx(Ex(), samples=5, pause=0))
        by = {r['symbol']: r for r in payload['rows']}
        self.assertAlmostEqual(float(np.median(by['A/USDT:USDT']['spread_bps'])), 10.0, places=6)             # (2001−1999)/2000 = 10 б.п.
        self.assertEqual(by['B/USDT:USDT']['spread_bps'], [])
        self.assertIn('нет тикера', by['B/USDT:USDT']['ticker_error'])

    def test_market_row_listing_date_sources(self):
        r = M.market_row('X', {'created': ms(2024, 5, 6)})
        self.assertEqual(r['created'], '2024-05-06')
        r = M.market_row('X', {'created': None, 'info': {'launchTime': str(ms(2023, 1, 2))}})
        self.assertEqual(r['created'], '2023-01-02')
        self.assertIsNone(M.market_row('X', {'info': {}})['created'])


class Library(unittest.TestCase):
    def test_library_exact_parameters(self):
        lib = L.library()
        ids = [c.id for c in lib]
        self.assertEqual(ids, ['T-MA-10-50', 'T-MA-20-100', 'T-MA-50-200', 'T-DC-20', 'T-DC-55', 'T-DC-100', 'T-MOM-21', 'T-MOM-63', 'T-MOM-126', 'T-MOM-252',
                               'R-RSI2-plain', 'R-RSI2-SMA200', 'R-BB-2.0', 'R-BB-2.5', 'B-VC-NR7', 'X-MOM-21', 'X-MOM-63'])
        self.assertEqual(len(ids), len(set(ids)))
        fam = {}
        for c in lib:
            fam[c.family] = fam.get(c.family, 0) + 1
        self.assertEqual(fam, {'T-MA': 3, 'T-DC': 3, 'T-MOM': 4, 'R-RSI2': 2, 'R-BB': 2, 'B-VC': 1, 'X-MOM': 2})
        self.assertEqual({c.id: dict(c.params)['exit_n'] for c in lib if c.family == 'T-DC'}, {'T-DC-20': 10, 'T-DC-55': 27, 'T-DC-100': 50})
        self.assertEqual([c.id for c in lib if c.cross_sectional], ['X-MOM-21', 'X-MOM-63'])

    def test_count_matches_library(self):
        lib = L.library()
        non_xs = [c for c in lib if not c.cross_sectional]
        xs = [c for c in lib if c.cross_sectional]
        self.assertEqual((len(non_xs), len(xs)), (15, 2))
        self.assertEqual(len(L.configs_for(5)), len(non_xs))
        self.assertEqual(len(L.configs_for(6)), len(non_xs) + len(xs))                 # X-MOM — только при ≥ 6 инструментах
        self.assertEqual(L.count_tests([('CRYPTO', '1D', 16)]), 17)
        self.assertEqual(L.count_tests([('FX', '1D', 5)]), 15)
        self.assertEqual(L.count_tests([('CRYPTO', tf, 16) for tf in L.TIMEFRAMES]), 51)

    def test_n_bounds_equal_pre_registration(self):
        lo, hi = L.n_bounds()
        self.assertEqual((lo, hi), (96, 156))                                           # §2.41.5
        self.assertEqual(L.count_tests(L.cells_from_spec(L.SPEC_MIN)), lo)
        spec = {'CRYPTO': {'tfs': ['1D', '4h'], 'n': {'1D': 16, '4h': 16}}, 'FX': {'tfs': ['1D', '1h'], 'n': {'1D': 7, '1h': 3}}}
        self.assertEqual(L.count_tests(L.cells_from_spec(spec)), 17 + 17 + 17 + 15)

    def test_constants_match_pre_registration(self):
        self.assertEqual((L.RISK, L.STOP_ATR, L.ATR_N, L.NW_LAG, L.FDR_Q), (0.005, 3.0, 20, 5, 0.05))
        self.assertEqual((L.VALID_MIN_T, L.FINAL_MAX, L.FINAL_MIN_SHARPE, L.FINAL_MIN_T, L.FINAL_MAX_DD), (1.5, 3, 0.5, 2.0, 0.15))
        self.assertEqual((L.CALIB_N, L.CALIB_MAX_FRAC, L.XMOM_MIN_INSTRUMENTS, L.REBALANCE_BARS), (200, 0.05, 6, 5))
        self.assertEqual(L.PERIODS, {'TRAIN': ('2020-01-01', '2023-12-31'), 'VALID': ('2024-01-01', '2024-12-31'), 'LOCKBOX': ('2025-01-01', None)})
        self.assertEqual((L.spread_bps('METALS'), L.spread_bps('FX'), L.spread_bps('INDICES')), (3.0, 1.0, 2.0))
        os.environ['SPREAD_BPS_FX'] = '1.5'
        try:
            self.assertEqual(L.spread_bps('FX'), 1.5)
        finally:
            del os.environ['SPREAD_BPS_FX']


class Lockbox(unittest.TestCase):
    def setUp(self):
        self.saved = dict(L._lockbox_state)
        L._lockbox_state['read'] = False
        self.ts = np.array([ms(2023, 12, 31), ms(2024, 6, 1), ms(2024, 12, 31), ms(2025, 1, 1), ms(2026, 3, 1)], dtype=np.int64)
        self.x = np.arange(5.0)

    def tearDown(self):
        L._lockbox_state.update(self.saved)

    def test_refuses_without_final_flag(self):
        with self.assertRaises(L.LockboxError):
            L.read_lockbox(self.ts, self.x)
        with self.assertRaises(L.LockboxError):
            L.read_lockbox(self.ts, self.x, final=False)
        with self.assertRaises(L.LockboxError):
            L.read_lockbox(self.ts, self.x, final=1)                                    # только настоящий True
        self.assertFalse(L._lockbox_state['read'])                                      # отказ не «тратит» единственное чтение

    def test_slice_period_refuses_lockbox(self):
        with self.assertRaises(L.LockboxError):
            L.slice_period(self.ts, 'LOCKBOX')
        self.assertEqual(list(L.slice_period(self.ts, 'TRAIN')), [True, False, False, False, False])
        self.assertEqual(list(L.slice_period(self.ts, 'VALID')), [False, True, True, False, False])

    def test_final_reads_once(self):
        t, x = L.read_lockbox(self.ts, self.x, final=True)
        self.assertEqual(list(t), [ms(2025, 1, 1), ms(2026, 3, 1)])
        self.assertEqual(list(x), [3.0, 4.0])
        with self.assertRaises(L.LockboxError):
            L.read_lockbox(self.ts, self.x, final=True)                                 # повтор запрещён

    def test_period_bounds(self):
        lo, hi = L.period_bounds_ms('VALID')
        self.assertEqual((lo, hi), (ms(2024, 1, 1), ms(2025, 1, 1)))
        self.assertIsNone(L.period_bounds_ms('LOCKBOX')[1])


class Wiring(unittest.TestCase):
    def test_runner_task_and_command(self):
        self.assertIn('data_probe_ma', RR.TASKS)
        src = open(os.path.join(ROOT, 'async_bot.py'), encoding='utf-8').read()
        self.assertIn("elif cmd == '/data_probe_ma':", src)
        self.assertIn("_research_job('/data_probe_ma', 'data_probe_ma'", src)
        self.assertIn('research_ma.prepare_ma_bingx(exchange', src)                    # BingX через общий exchange — в процессе бота
        self.assertIn('/data_probe_ma —', src)
        self.assertNotIn('research_ma.run_probe_ma(', src)                             # расчёт/сеть Stooq и Dukascopy — только через runner

    def test_stage1_has_no_calculation_or_lockbox_access(self):
        src = open(os.path.join(ROOT, 'research_ma.py'), encoding='utf-8').read()
        self.assertNotIn('read_lockbox', src)
        self.assertNotIn('slice_period', src)


if __name__ == '__main__':
    unittest.main()
