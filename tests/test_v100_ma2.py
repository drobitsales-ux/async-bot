"""v100: проба v2 (§2.41.14) — правило выходных, часовой пояс, спред BID/ASK, агрегация дневок, стакан BingX, Stooq-диагностика; все на фикстурах."""
import asyncio
import calendar
import lzma
import os
import struct
import unittest
from datetime import datetime, timezone

import numpy as np

from _common import ROOT
import research_ma as M
import research_ma2 as M2
import research_ma_lib as L
import research_runner as RR


def run(coro):
    return asyncio.run(coro)


def ms(y, m, d, h=0):
    return int(datetime(y, m, d, h, tzinfo=timezone.utc).timestamp() * 1000)


# ── фикстуры: месячные файлы часовых свечей по расписанию рынка ─────────────────────────────────────
def is_closed(t_ms, schedule):
    dt = datetime.fromtimestamp(t_ms / 1000, timezone.utc)
    wd, h = dt.weekday(), dt.hour
    if schedule == 'fx':                       # закрыт: пт ≥ 22 (зимой) / 21 (летом) … вс до 22 / 21 UTC
        k = 21 if M2.us_dst(t_ms) else 22
        return (wd == 4 and h >= k) or wd == 5 or (wd == 6 and h < k)
    if schedule == 'fx_fixed':                 # без сезонного сдвига: всегда 22
        return (wd == 4 and h >= 22) or wd == 5 or (wd == 6 and h < 22)
    if schedule == 'fx_shifted':               # сезонный сдвиг есть, но часы на час раньше ожидаемых (21 зимой / 20 летом)
        k = 20 if M2.us_dst(t_ms) else 21
        return (wd == 4 and h >= k) or wd == 5 or (wd == 6 and h < k)
    if schedule == 'gold':                     # закрыт: пт ≥ k … вс < k+1 UTC, ежедневный перерыв в час k в будни (k = 21 летом / 22 зимой, 17:00 Нью-Йорка)
        k = 21 if M2.us_dst(t_ms) else 22
        return (wd == 4 and h >= k) or wd == 5 or (wd == 6 and h < k + 1) or (wd < 4 and h == k)
    raise ValueError(schedule)


def month_bars(year, month, schedule='fx', price=1.10, seed=1, filler=True, spread_active=0.0, spread_night=0.0, spread_closed=0.0, side='BID', flat_extra=()):
    """Список баров (смещение_с, open, close, low, high, volume) bid-ряда + (для ASK добавляется спред). Закрытые часы — плоские бары-заполнители с нулевым объёмом (filler) или пропуски."""
    rnd = np.random.default_rng(seed)
    n_days = calendar.monthrange(year, month)[1]
    start = ms(year, month, 1)
    px, out = price, []
    for k in range(n_days * 24):
        t = start + k * 3_600_000
        closed = is_closed(t, schedule)
        if closed and not filler:
            continue
        dt = datetime.fromtimestamp(t / 1000, timezone.utc)
        if closed:
            o = c = h = l = px
            vol = 0.0
        else:
            o = px
            c = px * (1 + rnd.normal(0, 0.0005))
            h = max(o, c) * (1 + abs(rnd.normal(0, 0.0003)))
            l = min(o, c) * (1 - abs(rnd.normal(0, 0.0003)))
            vol = 10.0 + float(rnd.random())
            px = c
            if (dt.day, dt.hour) in flat_extra:
                o = c = h = l = o
        if side == 'ASK':
            sp = spread_closed if closed else (spread_active if (dt.weekday() < 5 and 7 <= dt.hour <= 20) else spread_night)
            o, c, h, l = o + sp, c + sp, h + sp, l + sp
        out.append((k * 3600, o, c, l, h, vol))
    return out


def encode(bars, scale):
    recs = b''.join(struct.pack('>IIIIIf', t, int(round(o * scale)), int(round(c * scale)), int(round(l * scale)), int(round(h * scale)), v) for t, o, c, l, h, v in bars)
    return lzma.compress(recs, format=lzma.FORMAT_ALONE)


def parsed_month(year, month, **kw):
    scale = kw.pop('scale', 1e5)
    body = encode(month_bars(year, month, **kw), scale)
    p = M.parse_duka_candles(M.decode_bi5(body), ms(year, month, 1), 'ocLH')
    return M2.scale_prices(p, scale)


# ── время ─────────────────────────────────────────────────────────────────────────────────────────────
class Dst(unittest.TestCase):
    def test_us_dst_boundaries(self):
        for t, exp in ((ms(2024, 3, 10, 6), False), (ms(2024, 3, 10, 7), True), (ms(2024, 11, 3, 5), True), (ms(2024, 11, 3, 6), False),
                       (ms(2025, 3, 9, 7), True), (ms(2025, 11, 2, 6), False), (ms(2024, 1, 15), False), (ms(2024, 7, 15), True)):
            self.assertEqual(M2.us_dst(t), exp, datetime.fromtimestamp(t / 1000, timezone.utc))
        self.assertEqual(M2.ny_offset_ms(ms(2024, 1, 15)), -5 * 3_600_000)
        self.assertEqual(M2.ny_offset_ms(ms(2024, 7, 15)), -4 * 3_600_000)


# ── 1.1 выходные ──────────────────────────────────────────────────────────────────────────────────────
class Weekend(unittest.TestCase):
    def test_closed_window_detected_and_rule_excludes_only_filler(self):
        p = parsed_month(2024, 1)                                             # январь 2024: 4 пятницы/субботы/воскресенья
        ws = M2.weekend_stats(p)
        self.assertEqual(ws['n'], 31 * 24)
        self.assertEqual(ws['ranges'], [(4 * 24 + 22, 48)])                   # пт 22:00 → вс 22:00, 48 ч
        self.assertEqual(M2.fmt_range(*ws['ranges'][0]), 'Пт 22:00 → Вс 22:00 UTC (48 ч)')
        self.assertEqual(ws['live_in_closed'], 0)
        self.assertEqual(ws['zero_vol_closed'], 1.0)
        self.assertEqual(ws['zero_vol_open'], 0.0)
        self.assertGreater(ws['mean_vol_open'], 10)
        self.assertEqual(ws['mean_vol_closed'], 0.0)
        self.assertAlmostEqual(ws['flat_weekend'], 46 / 48, places=6)         # Сб 24 ч + Вс 0–21 ч плоские; Вс 22–23 ч — живые
        excl = M2.exclusion_mask(p, ws)
        self.assertEqual(int(excl.sum()), 4 * (2 + 24 + 22))                  # ровно бары-заполнители
        self.assertEqual(int(np.sum(excl & ~M2.flat_mask(p))), 0)             # живые бары не исключаются

    def test_genuine_flat_bar_outside_window_is_kept(self):
        p = parsed_month(2024, 1, flat_extra={(9, 3)})                        # вторник 9 января 03:00 — реальный плоский бар в открытом рынке
        ws = M2.weekend_stats(p)
        self.assertEqual(ws['flat_outside_closed'], 1)
        excl = M2.exclusion_mask(p, ws)
        i = int(np.where(p['ts'] == ms(2024, 1, 9, 3))[0][0])
        self.assertTrue(M2.flat_mask(p)[i])
        self.assertFalse(excl[i])                                             # O=H=L=C, но вне окна закрытия — не исключается

    def test_dst_year_window_cells_drift_but_run_rule_is_stable(self):
        """Находка на годовом образце: ячейки «день×час» (≥ 90% плоских) теряют час, плоский только в одном из сезонов; серия ≥ 12 плоских подряд — нет."""
        p = year_series('fx')
        ws = M2.weekend_stats(p)
        self.assertEqual(ws['ranges'], [(4 * 24 + 22, 47)])                                   # 47 ч вместо 48: зимний вс 21:00 и летний пт 21:00 выпали из окна
        cell_rule = ws['flat'] & ws['closed_mask']
        run_rule = M2.exclusion_mask(p)
        self.assertLess(int(cell_rule.sum()), int(run_rule.sum()))                            # окно по ячейкам недоисключает плоские бары
        self.assertEqual(int(run_rule.sum()), 52 * 48)
        self.assertEqual(int(np.sum(run_rule & ~M2.flat_mask(p))), 0)

    def test_closure_mask_runs(self):
        ts = np.arange(0, 40) * 3_600_000
        flat = np.zeros(40, bool)
        flat[3] = True                                                                          # одиночный
        flat[10:14] = True                                                                      # серия 4
        flat[20:34] = True                                                                      # серия 14 ≥ 12
        px = np.arange(40, dtype=float) + 1
        p = {'ts': ts.astype(np.int64), 'o': px, 'h': px + 1, 'l': px - 1, 'c': px + 0.5, 'v': np.ones(40)}
        for i in np.where(flat)[0]:
            p['o'][i] = p['h'][i] = p['l'][i] = p['c'][i] = px[i]
        m = M2.closure_mask(p)
        self.assertEqual(list(np.where(m)[0]), list(range(20, 34)))
        # пропуск баров (разрыв > 1 ч) обрывает серию
        p2 = {k: np.concatenate([v[:27], v[28:]]) for k, v in p.items()}
        self.assertFalse(M2.closure_mask(p2)[20:33].all())

    def test_no_filler_means_no_window(self):
        p = parsed_month(2024, 1, filler=False)
        ws = M2.weekend_stats(p)
        self.assertEqual(ws['ranges'], [])
        self.assertEqual(ws['flat_share'], 0.0)
        self.assertEqual(int(M2.exclusion_mask(p, ws).sum()), 0)


# ── 1.2 часовой пояс ──────────────────────────────────────────────────────────────────────────────────
class Timezone(unittest.TestCase):
    def edges(self, schedule, year=2024):
        return [M2.week_edges(parsed_month(year, mo, schedule=schedule)) for mo in (1, 7)]

    def test_utc_confirmed_for_us_linked_schedule(self):
        jan, jul = self.edges('fx')
        self.assertEqual(M2._mode(jan[0]), (6, 22))                           # вс 22:00
        self.assertEqual(M2._mode(jan[1]), (4, 21))                           # пт 21:00 (последний бар)
        self.assertEqual(M2._mode(jul[0]), (6, 21))
        self.assertEqual(M2._mode(jul[1]), (4, 20))
        v = M2.tz_verdict(jan, jul)
        self.assertIs(v['utc_confirmed'], True)
        self.assertIn('UTC подтверждён', v['text'])

    def test_no_seasonal_shift_is_not_utc(self):
        v = M2.tz_verdict(*self.edges('fx_fixed'))
        self.assertIs(v['utc_confirmed'], False)
        self.assertIn('сезонного сдвига нет', v['text'])

    def test_shift_with_unexpected_hours(self):
        v = M2.tz_verdict(*self.edges('fx_shifted'))
        self.assertIsNone(v['utc_confirmed'])
        self.assertIn('отличаются от ожидаемых', v['text'])

    def test_insufficient_data(self):
        self.assertIsNone(M2.tz_verdict(([], []), ([], []))['utc_confirmed'])


# ── 1.3 спред ─────────────────────────────────────────────────────────────────────────────────────────
class Spread(unittest.TestCase):
    def test_median_p90_active_night_and_exclusions(self):
        kw = dict(price=1.10, spread_active=0.0001, spread_night=0.0003, spread_closed=0.01)
        bid = parsed_month(2024, 2, side='BID', **kw)
        ask = parsed_month(2024, 2, side='ASK', **kw)
        ts, spr, fl = M2.spread_bps_series(bid, ask)
        self.assertEqual(len(ts), len(bid['ts']))
        st = M2.spread_stats(ts, spr, fl)
        mid_bps = 1e4 / 1.10
        self.assertAlmostEqual(st['active']['median'], 0.0001 * mid_bps, delta=0.05 * mid_bps * 0.0001 * 20)      # ≈ 0.909 б.п.
        self.assertAlmostEqual(st['night']['median'], 0.0003 * mid_bps, delta=0.05)                            # ≈ 2.727 б.п.
        self.assertLess(st['active']['median'], st['night']['median'])
        self.assertLess(st['night']['p90'], 4.0)                                                                # огромный спред закрытых часов (0.01) не попал в выборку
        self.assertGreaterEqual(st['active']['p90'], st['active']['median'])
        self.assertGreater(st['active']['n'], 200)
        self.assertGreater(st['night']['n'], 100)

    def test_alignment_with_missing_bars(self):
        bid = parsed_month(2024, 2, side='BID', spread_active=0.0001)
        ask = parsed_month(2024, 2, side='ASK', spread_active=0.0001)
        keep = np.ones(len(ask['ts']), bool)
        keep[10:20] = False
        ask = {k: v[keep] for k, v in ask.items() if hasattr(v, '__len__') and len(v) == len(keep)} | {'n': int(keep.sum())}
        ts, spr, fl = M2.spread_bps_series(bid, ask)
        self.assertEqual(len(ts), len(bid['ts']) - 10)

    def test_pooled(self):
        a = M2.spread_bps_series(parsed_month(2024, 2, side='BID', spread_active=0.0001, spread_night=0.0001), parsed_month(2024, 2, side='ASK', spread_active=0.0001, spread_night=0.0001))
        b = M2.spread_bps_series(parsed_month(2024, 3, side='BID', spread_active=0.0003, spread_night=0.0003), parsed_month(2024, 3, side='ASK', spread_active=0.0003, spread_night=0.0003))
        st = M2.pooled_spread([a, b])
        sa, sb = M2.spread_stats(*a), M2.spread_stats(*b)
        self.assertEqual(st['active']['n'], sa['active']['n'] + sb['active']['n'])
        self.assertTrue(sa['active']['median'] < st['active']['median'] < sb['active']['median'])
        self.assertIsNone(M2.pooled_spread([]))


# ── 1.5 дневки ────────────────────────────────────────────────────────────────────────────────────────
def year_series(schedule, year=2024, price=1.10, scale=1e5):
    parts = [parsed_month(year, mo, schedule=schedule, price=price, scale=scale, seed=mo) for mo in range(1, 13)]
    return {k: np.concatenate([x[k] for x in parts]) for k in ('ts', 'o', 'h', 'l', 'c', 'v')}


class Daily(unittest.TestCase):
    def test_aggregate_ohlcv_hand_made(self):
        ts = np.array([ms(2024, 1, 2, h) for h in (1, 2, 3)] + [ms(2024, 1, 3, h) for h in (0, 1)], dtype=np.int64)
        p = {'ts': ts, 'o': np.array([1.0, 1.1, 1.2, 2.0, 2.1]), 'h': np.array([1.5, 1.6, 1.4, 2.5, 2.2]), 'l': np.array([0.9, 1.0, 1.1, 1.9, 1.8]),
             'c': np.array([1.1, 1.2, 1.3, 2.1, 2.0]), 'v': np.array([1.0, 2.0, 3.0, 4.0, 5.0])}
        d = M2.aggregate_daily(p, 'utc00')
        self.assertEqual(list(d['nbars']), [3, 2])
        self.assertEqual((d['o'][0], d['h'][0], d['l'][0], d['c'][0], d['v'][0]), (1.0, 1.6, 0.9, 1.3, 6.0))
        self.assertEqual((d['o'][1], d['h'][1], d['l'][1], d['c'][1], d['v'][1]), (2.0, 2.5, 1.8, 2.0, 9.0))
        d2 = M2.aggregate_daily(p, 'utc00', exclude=np.array([False, True, False, False, False]))
        self.assertEqual(list(d2['nbars']), [2, 2])
        self.assertEqual(d2['v'][0], 4.0)
        with self.assertRaises(ValueError):
            M2.aggregate_daily(p, 'zzz')

    def test_ny17_day_boundary_winter_and_summer(self):
        # зима: 17:00 NY = 22:00 UTC; лето: 17:00 NY = 21:00 UTC
        w = M2.day_labels(np.array([ms(2024, 1, 10, 21), ms(2024, 1, 10, 22)], dtype=np.int64), 'ny17')
        self.assertEqual(int(w[1] - w[0]), 1)
        s = M2.day_labels(np.array([ms(2024, 7, 10, 20), ms(2024, 7, 10, 21)], dtype=np.int64), 'ny17')
        self.assertEqual(int(s[1] - s[0]), 1)
        self.assertEqual(int(s[0]), ms(2024, 7, 10) // M2.DAY_MS)                              # до границы — день 10 июля
        u = M2.day_labels(np.array([ms(2024, 7, 10, 23), ms(2024, 7, 11, 0)], dtype=np.int64), 'utc00')
        self.assertEqual(int(u[1] - u[0]), 1)

    def test_fx_year_ny17_has_no_stubs_and_utc00_has_sunday_stubs(self):
        p = year_series('fx')
        ws = M2.weekend_stats(p)
        excl = M2.exclusion_mask(p, ws)
        st = {r: M2.daily_stats(M2.aggregate_daily(p, r, excl)) for r in ('utc00', 'ny17')}
        self.assertEqual(int(excl.sum()), 52 * 48)                                            # серии закрытия по 48 ч и зимой, и летом (окно сдвигается на час)
        self.assertTrue(M2.DAYS_RANGE[0] <= st['ny17']['days'] <= M2.DAYS_RANGE[1], st)
        self.assertEqual(st['ny17']['stubs'], 0)
        self.assertGreaterEqual(st['utc00']['stubs'], 50)                                     # воскресные 2 часа (22–23 UTC) — обрубки
        self.assertGreater(st['utc00']['days'], M2.DAYS_RANGE[1])
        rule, why = M2.choose_rule(st, 'ny17')
        self.assertEqual((rule, why), ('ny17', 'подходит правило по умолчанию'))

    def test_gold_like_schedule_default_utc00_rejected(self):
        p = year_series('gold', price=1900.0, scale=1e3)
        excl = M2.exclusion_mask(p)
        st = {r: M2.daily_stats(M2.aggregate_daily(p, r, excl)) for r in ('utc00', 'ny17')}
        self.assertGreaterEqual(st['utc00']['stubs'], 50)                                     # воскресный бар 23:00 UTC — обрубок при границе 00:00 UTC
        self.assertEqual(st['ny17']['stubs'], 0)
        rule, why = M2.choose_rule(st, 'utc00')
        self.assertEqual(rule, 'ny17')
        self.assertIn('по умолчанию не подходит', why)

    def test_choose_rule_none(self):
        bad = {'utc00': {'days': 313, 'stubs': 52}, 'ny17': {'days': 240, 'stubs': 0}}
        self.assertEqual(M2.choose_rule(bad, 'ny17')[0], None)


# ── стакан BingX ──────────────────────────────────────────────────────────────────────────────────────
class Book(unittest.TestCase):
    def test_book_stats(self):
        smp = [{'bids': [[99.9, 10.0], [99.8, 5.0]], 'asks': [[100.1, 8.0]]}, {'bids': [[99.95, 10.0]], 'asks': [[100.05, 10.0]]}, {'bids': [], 'asks': []}]
        st = M2.book_stats(smp)
        self.assertEqual(st['n'], 2)
        self.assertAlmostEqual(st['spread_median'], np.median([0.2 / 100.0 * 1e4, 0.1 / 100.0 * 1e4]), places=6)
        self.assertAlmostEqual(st['bid_depth_usdt'], np.median([999.0, 999.5]), places=6)
        self.assertAlmostEqual(st['ask_depth_usdt'], np.median([800.8, 1000.5]), places=6)
        self.assertIsNone(M2.book_stats([])['spread_median'])

    def test_prepare_skips_paused_and_missing(self):
        calls = []

        class Ex:
            markets = {
                'G/USDT:USDT': {'swap': True, 'id': 'NCCOGOLD2USD-USDT', 'symbol': 'G/USDT:USDT', 'active': True, 'contractSize': 0.0001},
                'S/USDT:USDT': {'swap': True, 'id': 'NCCOXAG2USD-USDT', 'symbol': 'S/USDT:USDT', 'active': False},                     # приостановлен
                'X/USDT:USDT': {'swap': True, 'id': 'NCFXEUR2USD-USDT', 'symbol': 'X/USDT:USDT', 'active': False},
                'B/USDT:USDT': {'swap': True, 'id': 'BTC-USDT', 'symbol': 'B/USDT:USDT', 'active': True},
            }

            async def fetch_order_book(self, sym, limit=5):
                calls.append((sym, limit))
                return {'bids': [[1999.0, 2.0]], 'asks': [[2001.0, 3.0]]}
        payload = run(M2.prepare_ma_bingx_book(Ex(), samples=5, pause=0))
        self.assertEqual(calls, [('G/USDT:USDT', 5)] * 5)                                      # только золото, 5 замеров, limit=5
        b = payload['books']
        self.assertEqual(len(b['золото']['samples']), 5)
        self.assertIn('не активен', b['серебро']['error'])
        self.assertIn('не найден', b['платина']['error'])
        self.assertEqual((payload['n_active'], payload['n_paused']), (1, 2))
        self.assertAlmostEqual(M2.book_stats(b['золото']['samples'])['spread_median'], 10.0, places=6)

    def test_prepare_records_errors(self):
        class Ex:
            markets = {'G/USDT:USDT': {'swap': True, 'id': 'NCCOGOLD2USD-USDT', 'symbol': 'G/USDT:USDT', 'active': True}}

            async def fetch_order_book(self, sym, limit=5):
                raise RuntimeError('нет стакана')
        b = run(M2.prepare_ma_bingx_book(Ex(), samples=2, pause=0))['books']['золото']
        self.assertEqual(b['samples'], [])
        self.assertIn('нет стакана', b['error'])


# ── Stooq ─────────────────────────────────────────────────────────────────────────────────────────────
class FakeResp:
    def __init__(self, status, headers, body):
        self.status, self.headers, self._b = status, headers, body

    async def read(self):
        return self._b

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False


class FakeSession:
    def __init__(self, resp=None, exc=None):
        self.resp, self.exc, self.kw = resp, exc, None

    def get(self, url, **kw):
        self.kw = kw
        if self.exc:
            raise self.exc
        return self.resp


class StooqDiag(unittest.TestCase):
    def test_timeout_is_reported(self):
        d = run(M2.stooq_diagnose(FakeSession(exc=asyncio.TimeoutError()), dns=False))
        self.assertIn('TimeoutError', d['error'])
        self.assertEqual(d['timeout_s'], 15)

    def test_redirect_not_followed_and_headers(self):
        s = FakeSession(FakeResp(301, {'Location': 'https://stooq.pl/x', 'Server': 'nginx'}, b''))
        d = run(M2.stooq_diagnose(s, dns=False))
        self.assertEqual(d['status'], 301)
        self.assertEqual(d['redirect'], 'https://stooq.pl/x')
        self.assertIs(s.kw['allow_redirects'], False)
        self.assertEqual(d['headers']['server'], 'nginx')

    def test_captcha_and_apikey_flags(self):
        d = run(M2.stooq_diagnose(FakeSession(FakeResp(200, {'Content-Type': 'text/html'}, b'<html>Get your apikey: ...</html>')), dns=False))
        self.assertTrue(d['apikey'])
        self.assertFalse(d['captcha'])
        d = run(M2.stooq_diagnose(FakeSession(FakeResp(403, {}, b'Attention Required! | Cloudflare')), dns=False))
        self.assertTrue(d['captcha'])


# ── поиск инструментов, поток пробы, предложение ─────────────────────────────────────────────────────
def fake_duka(known, price, scale, schedule='fx', first_year=2005, calls=None):
    """Поддельный fetch Dukascopy: known — множество папок, существующих в datafeed; BID/ASK-файлы по фикстурам; годы < first_year — 404."""
    async def fetch(url, params=None, method='GET'):
        if calls is not None:
            calls.append(url)
        parts = url.split('/datafeed/')[1].split('/')
        folder, year, month0, fname = parts[0], int(parts[1]), int(parts[2]), parts[3]
        if folder not in known or year < first_year:
            return {'status': 404, 'headers': {}, 'body': b'', 'error': ''}
        side = 'ASK' if fname.startswith('ASK') else 'BID'
        bars = month_bars(year, month0 + 1, schedule=schedule, price=price[folder] if isinstance(price, dict) else price, seed=month0 + 1, side=side,
                          spread_active=0.0001, spread_night=0.0002)
        return {'status': 200, 'headers': {}, 'body': encode(bars, scale), 'error': ''}
    return fetch


NOW = ms(2026, 10, 25)


class Discovery(unittest.TestCase):
    def test_variant_fallback(self):
        calls = []
        fx = M2.Fetcher(fake_duka({'WTICMDUSD'}, 80.0, 1e3, calls=calls), 0)
        info = run(M2.discover(fx, 'WTI', NOW))
        self.assertEqual(info['folder'], 'WTICMDUSD')                                          # первый вариант LIGHTCMDUSD — 404, второй найден
        self.assertEqual(info['tried'][0][:2], ('LIGHTCMDUSD', 404))
        self.assertEqual(info['tried'][1][1], 200)
        self.assertEqual(info['scale'], 1e3)
        self.assertTrue(info['ok_format'])
        self.assertEqual(info['order'], 'ocLH')

    def test_not_found(self):
        fx = M2.Fetcher(fake_duka(set(), 80.0, 1e3), 0)
        info = run(M2.discover(fx, 'BRENT', NOW))
        self.assertIsNone(info['folder'])
        self.assertFalse(info['ok_format'])
        self.assertEqual(info['tried'], [('BRENTCMDUSD', 404, 0)])

    def test_cache_does_not_refetch(self):
        calls = []
        fx = M2.Fetcher(fake_duka({'EURUSD'}, 1.10, 1e5, calls=calls), 0)
        run(fx.month('EURUSD', 'BID', 2024, 0))
        run(fx.month('EURUSD', 'BID', 2024, 0))
        self.assertEqual(len(calls), 1)
        self.assertEqual(fx.requests, 1)


class InstrumentFlow(unittest.TestCase):
    def setUp(self):
        self.fx = M2.Fetcher(fake_duka({'EURUSD', 'XAUUSD'}, {'EURUSD': 1.10, 'XAUUSD': 1900.0}, 1e5), 0)

    def test_probe_instrument(self):
        # EURUSD: масштаб 1e5 ⇒ цена 1.10 в диапазоне FX
        info = run(M2.discover(self.fx, 'EURUSD', NOW))
        r = run(M2.probe_instrument(self.fx, info, NOW))
        self.assertEqual(r['earliest_year'], 2008)
        self.assertEqual(r['depth'][2008], (200, r['depth'][2008][1]))
        self.assertTrue(r['weekend']['ranges'])
        self.assertEqual(r['weekend']['live_in_closed'], 0)
        sp = r['spread_pooled']
        self.assertAlmostEqual(sp['active']['median'], 0.0001 / 1.10 * 1e4, delta=0.1)
        self.assertAlmostEqual(sp['night']['median'], 0.0002 / 1.10 * 1e4, delta=0.1)
        self.assertEqual(len(r['spread']), 3)
        ok, why = M2.instrument_ok(r)
        self.assertTrue(ok, why)

    def test_tz_and_daily_probe(self):
        infos = {k: run(M2.discover(self.fx, k, NOW)) for k in ('EURUSD', 'XAUUSD')}
        tz = run(M2.probe_tz(self.fx, infos))
        self.assertIs(tz['EURUSD']['utc_confirmed'], True)
        self.assertIs(tz['XAUUSD']['utc_confirmed'], True)
        daily = run(M2.probe_daily_rules(self.fx, {'EURUSD': infos['EURUSD']}))
        self.assertEqual(daily['FX']['rule'], 'ny17')
        self.assertEqual(daily['FX']['stats']['ny17']['stubs'], 0)
        self.assertIn('образец SPX не найден', daily['INDICES']['error'])
        self.assertIn('образец XAUUSD не найден', daily['METALS']['error'])

    def test_instrument_ok_reasons(self):
        base = {'folder': 'X', 'ok_format': True, 'earliest_year': 2012, 'spread_pooled': {'active': {'median': 1.0}}}
        self.assertTrue(M2.instrument_ok(base)[0])
        self.assertIn('не найден', M2.instrument_ok({'folder': None})[1])
        self.assertIn('история с 2021', M2.instrument_ok(dict(base, earliest_year=2021))[1])
        self.assertIn('формат/масштаб', M2.instrument_ok(dict(base, ok_format=False))[1])
        self.assertIn('спред', M2.instrument_ok(dict(base, spread_pooled=None))[1])


class Proposal(unittest.TestCase):
    def inst(self, cls, key, ok=True):
        return {'class': cls, 'folder': key if ok else None, 'ok_format': True, 'earliest_year': 2010, 'spread_pooled': {'active': {'median': 1.0}}, 'tried': []}

    def test_cells_and_n(self):
        ins = {}
        for k in ('XAUUSD', 'XAGUSD', 'XPTUSD'):
            ins[k] = self.inst('METALS', k)
        for k in ('EURUSD', 'GBPUSD', 'USDJPY', 'AUDUSD', 'USDCHF', 'USDCAD', 'NZDUSD'):
            ins[k] = self.inst('FX', k)
        for k in ('SPX', 'NDX', 'DAX', 'NIKKEI', 'FTSE'):
            ins[k] = self.inst('INDICES', k)
        ins['BRENT'] = self.inst('ENERGY', 'BRENT'); ins['WTI'] = self.inst('ENERGY', 'WTI', ok=False)
        prop = M2.proposal2(ins, {})
        self.assertEqual(prop['classes']['METALS'], ['XAUUSD', 'XAGUSD', 'XPTUSD'])
        self.assertEqual(len(prop['classes']['FX']), 7)
        self.assertEqual(prop['extra'], {'ENERGY': 1, 'BONDS': 0})
        self.assertIn(('FX', '1D', 7), prop['cells'])
        self.assertIn(('INDICES', '4h', 5), prop['cells'])
        self.assertNotIn(('INDICES', '1h', 5), prop['cells'])                                  # индексы — только 1D и 4h
        # N: CRYPTO 3×17 + METALS 3×15 + FX 3×17 (7 инструментов ≥ 6 → X-MOM) + INDICES 2×15 (5 < 6)
        self.assertEqual(prop['N'], 51 + 45 + 51 + 30)
        self.assertEqual(prop['N'], L.count_tests(prop['cells']))

    def test_empty_classes_drop_out(self):
        prop = M2.proposal2({'EURUSD': self.inst('FX', 'EURUSD')}, {})
        self.assertEqual([c for c in prop['cells'] if c[0] != 'CRYPTO'], [('FX', '1D', 1), ('FX', '4h', 1), ('FX', '1h', 1)])
        self.assertEqual(prop['N'], 51 + 45)


class Report(unittest.TestCase):
    def test_report_contains_all_sections(self):
        fx = M2.Fetcher(fake_duka({'EURUSD', 'XAUUSD'}, {'EURUSD': 1.10, 'XAUUSD': 1900.0}, 1e5), 0)
        infos = {k: run(M2.discover(fx, k, NOW)) for k in M2.TARGETS}
        instruments = {k: (run(M2.probe_instrument(fx, i, NOW)) if i['folder'] else i) for k, i in infos.items()}
        tz = run(M2.probe_tz(fx, infos))
        daily = run(M2.probe_daily_rules(fx, infos))
        book = {'books': {'золото': {'symbol': 'G', 'contract_size': 1, 'samples': [{'bids': [[1999.0, 2.0]], 'asks': [[2001.0, 3.0]]}], 'error': None},
                          'серебро': {'symbol': 'S', 'error': 'контракт не активен (приостановлен) — не опрашивался'}}, 'n_active': 10, 'n_paused': 600}
        stooq = {'error': 'TimeoutError: ', 'elapsed_s': 15.0, 'dns': ['1.2.3.4']}
        text = M2.build_report2(stooq, instruments, tz, daily, book, NOW, 800.0, fx.requests, 'v100')
        for needle in ('ПРОБА v2', 'доходностей стратегий не считалось', 'Stooq недоступен', '1.1 Выходные', 'Пт 21:00 → Вс 21:00 UTC (48 ч)', 'ПРЕДЛОЖЕНИЕ правила исключения',
                       '1.2 Часовой пояс', 'UTC подтверждён', '1.3 Спред', '1.5 Дневки', 'ПРЕДЛАГАЕТСЯ ny17', '2. BingX', 'не опрашивался', 'спред медиана 10.00 б.п.',
                       'Ячейки (класс, таймфрейм', 'N по формуле §2.41.5', 'ВЕРДИКТ пробы v2', 'НЕ НАЙДЕН'):
            self.assertIn(needle, text)
        self.assertNotIn('ПРОЙДЕНА', text)


class Wiring(unittest.TestCase):
    def test_runner_and_command_use_v2(self):
        src = open(os.path.join(ROOT, 'async_bot.py'), encoding='utf-8').read()
        self.assertIn('research_ma2.prepare_ma_bingx_book(exchange', src)                      # стакан BingX — через общий exchange в процессе бота
        self.assertIn("_research_job('/data_probe_ma', 'data_probe_ma'", src)
        self.assertNotIn('research_ma.prepare_ma_bingx(exchange', src)
        rsrc = open(os.path.join(ROOT, 'research_runner.py'), encoding='utf-8').read()
        self.assertIn('research_ma2.run_probe_ma2', rsrc)
        self.assertIn('data_probe_ma', RR.TASKS)

    def test_probe_has_no_strategy_calculation_or_lockbox(self):
        src = open(os.path.join(ROOT, 'research_ma2.py'), encoding='utf-8').read()
        for forbidden in ('read_lockbox', 'slice_period', 'simulate', 'sharpe', 'Sharpe', 'mean_r'):
            self.assertNotIn(forbidden, src)

    def test_active_only_for_bingx_book(self):
        src = open(os.path.join(ROOT, 'research_ma2.py'), encoding='utf-8').read()
        self.assertIn("if not m.get('active'):", src)
        self.assertEqual(sorted(M2.BINGX_BOOK), sorted(['золото', 'серебро', 'платина', 'палладий', 'Brent', 'WTI', 'газ', 'медь', 'S&P 500', 'Nasdaq 100']))


if __name__ == '__main__':
    unittest.main()
