"""v95: /edge_audit — R0 на случайном блуждании, R1 против _smc2_eval/_shadow_exit_eval бота, варианты V0/V1/V2/F, фандинг, критерии."""
import asyncio
import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone

import numpy as np

from _common import a, fresh_state
import research_edge as E
import research_vision as V

T0 = int(datetime(2024, 6, 1, tzinfo=timezone.utc).timestamp() * 1000)
FEES = (0.0002, 0.0005)


def walk(n, seed, sd=0.0035, t0=T0, vol_spikes=True):
    """Случайное блуждание 15m со всплесками объёма; ATR ≈ 0.4% цены (внутри границ SL [0.3%; 3%])."""
    rng = np.random.default_rng(seed)
    c = 100 * np.exp(np.cumsum(rng.normal(0, sd, n)))
    o = np.r_[c[0], c[:-1]]
    h = np.maximum(o, c) * (1 + np.abs(rng.normal(0, 0.0012, n)))
    l = np.minimum(o, c) * (1 - np.abs(rng.normal(0, 0.0012, n)))
    v = rng.lognormal(5, 0.5, n) * (1 + 2.5 * (rng.random(n) < 0.08) if vol_spikes else 1)
    ts = t0 + np.arange(n, dtype=np.int64) * E.TF_MS
    return {'ts': ts, 'o': o, 'h': h, 'l': l, 'c': c, 'v': v, 'tbv': v * 0.5}


def run(coro):
    return asyncio.run(coro)


class R0Calibration(unittest.TestCase):
    def test_random_walk_gives_zero_edge(self):
        trades, diags = [], {}
        for i in range(8):
            tr, dg = E.r0_trades(f'S{i}USDT', i, walk(12000, 100 + i), FEES, n_target=600)
            trades += tr; diags[i] = dg
        s = E.stats(trades)
        self.assertGreater(s['n'], 4000)
        self.assertLess(abs(s['t_g']), 2.0, s)                 # mean R до издержек ≈ 0
        self.assertLess(s['mean'], s['mean_g'])                # издержки уменьшают R
        status, _ = E.r0_verdict(trades)
        self.assertIn(status, ('OK', 'WARN'))
        self.assertAlmostEqual(s['sl'] + s['tp'] + s['to'], 100.0, places=6)

    def test_many_seeds_never_stop(self):
        stops = 0
        for k in range(10):
            trades = []
            for i in range(4):
                trades += E.r0_trades(f'S{i}USDT', i, walk(8000, 1000 + 10 * k + i), FEES, n_target=500)[0]
            stops += E.r0_verdict(trades)[0] == 'STOP'
        self.assertEqual(stops, 0)

    def test_deterministic_seed(self):
        arr = walk(6000, 7)
        t1 = E.r0_trades('XUSDT', 3, arr, FEES, n_target=300)[0]
        t2 = E.r0_trades('XUSDT', 3, arr, FEES, n_target=300)[0]
        self.assertEqual([(t['entry_ts'], t['long'], t['R']) for t in t1], [(t['entry_ts'], t['long'], t['R']) for t in t2])
        t3 = E.r0_trades('XUSDT', 4, arr, FEES, n_target=300)[0]   # другой номер символа → другой розыгрыш
        self.assertNotEqual([t['entry_ts'] for t in t1], [t['entry_ts'] for t in t3])

    def test_detects_biased_measurement(self):
        # систематически ошибочная система (напр. SL всегда смещён) → |t| >= 3 → STOP
        rng = np.random.default_rng(5)
        fake = [{'Rg': float(rng.normal(0.08, 1.0)), 'R': 0.0, 'reason': 'SL', 'long': True, 'entry_ts': T0, 'exit_ts': T0 + 1000} for _ in range(8000)]
        self.assertEqual(E.r0_verdict(fake)[0], 'STOP')

    def test_trades_respect_gaps(self):
        arr = walk(3000, 9)
        keep = np.r_[np.arange(0, 1500), np.arange(1600, 3000)]          # разрыв 100 баров
        gap = {k: v[keep] for k, v in arr.items()}
        self.assertEqual(len(E.segments(gap['ts'])), 2)
        tr, _ = E.r0_trades('GUSDT', 0, gap, FEES, n_target=400)
        edge = int(gap['ts'][1499])
        for t in tr:
            self.assertFalse(t['entry_ts'] <= edge < t['exit_ts'] - E.TF_MS, 'сделка пересекла разрыв')


class ExitEquivalence(unittest.TestCase):
    def test_matches_shadow_exit_eval(self):
        arr = walk(4000, 21, sd=0.004)
        ts, h, l, c = arr['ts'], arr['h'], arr['l'], arr['c']
        rng = np.random.default_rng(3)
        n_checked, reasons = 0, set()
        for _ in range(400):
            eb = int(rng.integers(30, 3900)); is_long = bool(rng.integers(0, 2))
            entry = float(c[eb - 1]); atr = float(E.atr14(h, l, c)[eb - 1])
            sl = entry - atr if is_long else entry + atr
            tp = entry + 2 * atr if is_long else entry - 2 * atr
            mine = E.sim_exit(h, l, c, eb, is_long, sl, tp, len(ts))
            bars = [[int(ts[k]), 0, float(h[k]), float(l[k]), float(c[k]), 0] for k in range(eb, min(eb + 40, len(ts)))]
            row = {'direction': 'Long' if is_long else 'Short', 'entry_price': entry, 'tp_price': tp, 'sl_price': sl, 'tp2_price': 0,
                   'open_time': datetime.fromtimestamp(ts[eb] / 1000, timezone.utc).isoformat()}
            closed, px, why, held = a._shadow_exit_eval('SMC2_MKT', row, bars, now_ts=ts[-1] / 1000 + 10 ** 7)
            self.assertTrue(closed and mine is not None)
            self.assertEqual((mine[2], mine[0] - eb), (why, held))
            self.assertAlmostEqual(mine[1], px, places=9)
            reasons.add(why); n_checked += 1
        self.assertEqual(reasons, {'SL', 'TP', 'TIMEOUT'})

    def test_unclosed_trade_dropped(self):
        arr = walk(60, 2)
        self.assertIsNone(E.sim_exit(arr['h'], arr['l'], arr['c'], 50, True, 0.0, 1e9, 60))

    def test_costs_by_order_type(self):
        mk, tk, sl = 0.0002, 0.0005, E.SLIP
        self.assertAlmostEqual(E.trade_costs('MKT', 'TP', mk, tk), tk + mk + sl)          # вход рынком: taker + проскальз.; TP maker
        self.assertAlmostEqual(E.trade_costs('MKT', 'SL', mk, tk), tk + tk + 2 * sl)
        self.assertAlmostEqual(E.trade_costs('RT', 'TP', mk, tk), mk + mk)                # лимит+лимит, без проскальзывания
        self.assertAlmostEqual(E.trade_costs('RT', 'TIMEOUT', mk, tk), mk + tk + sl)


def naive_shadow_events(arr, vol_mult=None, window=E.WINDOW):
    """Эталон: цикл scan_smc2 «в лоб» — окно из `window` баров, pivot-ы через get_pivots на ОКНЕ, `_smc2_eval` бота, запись как _smc2_persist."""
    ts, o, h, l, c, v = (arr[k] for k in ('ts', 'o', 'h', 'l', 'c', 'v'))
    rec = {}
    for e in range(window - 1, len(c)):
        s = e - window + 1
        w = [x[s:e + 1] for x in (ts, o, h, l, c, v)]
        ph, pl = a.get_pivots(w[2], w[3], order=a.SMC_PIVOT_ORDER)
        for mode in ('Long', 'Short'):
            kw = {} if vol_mult is None else {'vol_mult': vol_mult}
            evs, _ = a._smc2_eval(*w, mode, ph, pl, **kw)
            for ev in evs:
                r = rec.setdefault((mode, int(ev['ts_b'])), {'mkt': None, 'rt': None, 'b': s + ev['b']})
                if r['mkt'] is None and ev['mkt'] is not None:
                    r['mkt'] = (ev['mkt']['entry'], ev['mkt']['sl'], ev['mkt']['tp'])
                if ev['rt']['state'] == 'filled' and r['rt'] is None:
                    r['rt'] = (s + ev['rt']['f'], ev['rt']['entry'], ev['rt']['sl'], ev['rt']['tp'])
    return rec


class R1VsBot(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.arr = walk(2500, 33, sd=0.004)
        cls.ph, cls.pl = a.get_pivots(cls.arr['h'], cls.arr['l'], order=3)
        cls.ref_v0 = naive_shadow_events(cls.arr, None)
        cls.ref_v1 = naive_shadow_events(cls.arr, 0.0)
        a_ = cls.arr
        cls.ev_v1 = E.scan_segment(a_['ts'], a_['o'], a_['h'], a_['l'], a_['c'], a_['v'], cls.ph, cls.pl, a._smc2_eval, vol_mult=0.0)
        cls.ev_def = E.scan_segment(a_['ts'], a_['o'], a_['h'], a_['l'], a_['c'], a_['v'], cls.ph, cls.pl, a._smc2_eval, vol_mult=None)

    def _same(self, mine, ref):
        self.assertEqual(set(mine), set(ref))
        for k, r in ref.items():
            m = mine[k]
            self.assertEqual(m['b'], r['b'])
            self.assertEqual(m['mkt'], r['mkt'])
            self.assertEqual(m['rt'], r['rt'])

    def test_reference_has_events(self):
        self.assertGreater(len(self.ref_v0), 5)
        self.assertGreater(len(self.ref_v1), len(self.ref_v0))
        self.assertTrue(any(r['rt'] for r in self.ref_v0.values()) and any(r['mkt'] for r in self.ref_v0.values()))

    def test_default_scan_equals_bot_loop(self):
        self._same(self.ev_def, self.ref_v0)

    def test_v1_scan_equals_bot_loop_without_volume_filter(self):
        self._same(self.ev_v1, self.ref_v1)

    def test_v0_is_v1_filtered_by_break_volume(self):
        """V0 из надмножества V1 (флаг v0) = события, которые бот находит с фильтром объёма."""
        v0 = {k for k, r in self.ev_v1.items() if r['v0']}
        self.assertEqual(v0, set(self.ev_def))
        for k in v0:
            self.assertEqual((self.ev_v1[k]['mkt'], self.ev_v1[k]['rt']), (self.ev_def[k]['mkt'], self.ev_def[k]['rt']))

    def test_vol_mult_none_is_bot_default(self):
        a_ = self.arr
        w = [x[100:163] for x in (a_['ts'], a_['o'], a_['h'], a_['l'], a_['c'], a_['v'])]
        for m in ('Long', 'Short'):
            e1, s1 = a._smc2_eval(*w, m)
            e2, s2 = a._smc2_eval(*w, m, vol_mult=a.SMC2_BREAK_VOL_MULT)
            self.assertEqual((len(e1), s1), (len(e2), s2))

    def test_trades_match_shadow_exit_eval(self):
        arr = self.arr
        tr, _dg = E.variant_trades('XUSDT', self.ev_def, 'V0', (arr['ts'], arr['h'], arr['l'], arr['c'], len(arr['c'])), FEES)
        self.assertGreater(len(tr), 3)
        ts = arr['ts']
        for t in tr:
            eb = int(np.searchsorted(ts, t['entry_ts']))
            strat = 'SMC2_MKT' if t['etype'] == 'MKT' else 'SMC2_RT'
            rec = self.ev_def[('Long' if t['long'] else 'Short', t['ts_b'])]
            entry, sl, tp = rec['mkt'] if t['etype'] == 'MKT' else rec['rt'][1:]
            bars = [[int(ts[k]), 0, float(arr['h'][k]), float(arr['l'][k]), float(arr['c'][k]), 0] for k in range(eb, min(eb + 40, len(ts)))]
            row = {'direction': 'Long' if t['long'] else 'Short', 'entry_price': entry, 'tp_price': tp, 'sl_price': sl, 'tp2_price': 0,
                   'open_time': datetime.fromtimestamp(ts[eb] / 1000, timezone.utc).isoformat()}
            closed, px, why, held = a._shadow_exit_eval(strat, row, bars, now_ts=ts[-1] / 1000 + 10 ** 7)
            self.assertEqual((t['reason'], t['held']), (why, held))
            self.assertAlmostEqual(t['exit'], px, places=9)


class Variants(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.arr = walk(6000, 51, sd=0.004)
        x = cls.arr
        ph, pl = a.get_pivots(x['h'], x['l'], order=3)
        cls.events = E.scan_segment(x['ts'], x['o'], x['h'], x['l'], x['c'], x['v'], ph, pl, a._smc2_eval, vol_mult=0.0)
        cls.ts = x['ts']

    def _keys(self, variant, funding=None):
        c, _ = E.variant_candidates(self.events, variant, self.ts, funding)
        return {(r['mode'], r['ts_b']) for r in c}

    def test_volume_variants_differ_only_by_volume_condition(self):
        v0, v1, v2 = self._keys('V0'), self._keys('V1'), self._keys('V2')
        self.assertEqual(v1, {k for k, r in self.events.items() if r['rec_e'] is not None})
        self.assertEqual(v0, {k for k in v1 if self.events[k]['v0']})
        self.assertEqual(v2, {k for k in v1 if self.events[k]['v2']})
        self.assertTrue(v0 < v1 and v2 < v1 and v0 != v2)
        for k in v0 | v2:                                  # параметры сделок не зависят от варианта
            self.assertIs(self.events[k], self.events[k])

    def test_prev_volume_condition_excludes_break_candle(self):
        x = self.arr
        for k, r in self.events.items():
            b = r['b']
            med = float(np.median(x['v'][b - 20:b - 1]))
            self.assertEqual(r['v2'], not (med <= 0 or x['v'][b - 1] < 1.5 * med))

    def test_funding_variant_is_v0_plus_sign_rule(self):
        v0 = self._keys('V0')
        ft = np.arange(int(self.ts[0]), int(self.ts[-1]) + 1, 8 * 3_600_000, dtype=np.int64)       # расчёты каждые 8 ч на всём ряду
        const = lambda x: (ft, np.full(len(ft), x))
        pos = self._keys('F', const(0.0001))
        neg = self._keys('F', const(-0.0001))
        self.assertTrue(pos and neg)
        self.assertEqual(pos | neg, v0)
        self.assertEqual(pos & neg, set())
        self.assertTrue(all(m == 'Short' for m, _ in pos) and all(m == 'Long' for m, _ in neg))
        self.assertEqual(self._keys('F', const(0.0)), set())               # нулевой фандинг — ни лонг, ни шорт
        _, nofund = E.variant_candidates(self.events, 'F', self.ts, None)
        self.assertEqual(nofund, len(v0))                                   # без ставок событие исключается и считается

    def test_dedup_two_hours(self):
        """Два события одного направления в пределах 2 ч: второе блокируется (после фильтра варианта); другое направление и ≥ 2 ч — нет."""
        arr = self.arr
        base = [r for r in self.events.values() if r['mode'] == 'Long' and r['mkt'] is not None]
        r0 = dict(base[0]); b = r0['b']
        near = dict(r0, ts_b=r0['ts_b'] + 7 * E.TF_MS, b=b + 7, rec_e=r0['rec_e'] + 7, rt=None)          # 1 ч 45 мин — блок
        far = dict(r0, ts_b=r0['ts_b'] + 8 * E.TF_MS, b=b + 8, rec_e=r0['rec_e'] + 8, rt=None)           # ровно 2 ч — не блок (строго < 2 ч)
        other = dict(r0, mode='Short', rec_e=r0['rec_e'] + 1, rt=None)
        r0['rt'] = None
        evs = {('Long', r0['ts_b']): r0, ('Long', near['ts_b']): near, ('Long', far['ts_b']): far, ('Short', other['ts_b']): other}
        for r in evs.values():
            r['v0'] = r['v2'] = True
        tr, dg = E.variant_trades('XUSDT', evs, 'V1', (arr['ts'], arr['h'], arr['l'], arr['c'], len(arr['c'])), FEES)
        self.assertEqual(dg['dedup'], 1)
        self.assertEqual(sorted((t['long'], t['ts_b'] - r0['ts_b']) for t in tr), sorted([(True, 0), (True, 8 * E.TF_MS), (False, 0)]))


class FundingAndData(unittest.TestCase):
    def test_parse_funding_header(self):
        txt = 'calc_time,funding_interval_hours,last_funding_rate\n1704067200000,8,0.00010000\n1704096000000,8,-0.00005000\n'
        ts, r = E.parse_funding_csv(txt)
        self.assertEqual(list(ts), [1704067200000, 1704096000000])
        self.assertAlmostEqual(r[1], -0.00005)

    def test_parse_funding_no_header_and_alt_names(self):
        ts, r = E.parse_funding_csv('1704067200000,8,0.0002\n')
        self.assertAlmostEqual(r[0], 0.0002)
        ts, r = E.parse_funding_csv('fundingTime,fundingRate\n2024-01-01 00:00:00,0.0003\n')
        self.assertAlmostEqual(r[0], 0.0003)
        with self.assertRaises(V.VisionError):
            E.parse_funding_csv('foo,bar\n1,2\n')

    def test_fund_at_last_settlement(self):
        ft = np.array([1000, 2000, 3000], dtype=np.int64); fr = np.array([0.1, -0.2, 0.3])
        self.assertIsNone(E.fund_at(ft, fr, 999))
        self.assertEqual(E.fund_at(ft, fr, 1000), 0.1)         # расчёт ровно в момент входа относится к «≤ момента входа»
        self.assertEqual(E.fund_at(ft, fr, 2999), -0.2)
        self.assertIsNone(E.fund_at(ft, fr, 3000 + E.FUND_MAX_AGE_MS + 1))
        self.assertIsNone(E.fund_at(None, None, 5))

    def test_tail_days(self):
        now = int(datetime(2026, 10, 5, 13, tzinfo=timezone.utc).timestamp() * 1000)
        self.assertEqual(E.tail_days(['2026-08', '2026-09'], now), ['2026-10-01', '2026-10-02', '2026-10-03', '2026-10-04'])
        self.assertEqual(E.tail_days(['2025-12'], int(datetime(2026, 1, 3, tzinfo=timezone.utc).timestamp() * 1000)), ['2026-01-01', '2026-01-02'])
        self.assertEqual(E.tail_days([], now), [])

    def test_atr_matches_bot(self):
        x = walk(300, 4)
        atr = E.atr14(x['h'], x['l'], x['c'])
        for b in (14, 50, 299):
            self.assertAlmostEqual(atr[b], a.calc_atr(x['h'][:b + 1], x['l'][:b + 1], x['c'][:b + 1]), places=9)
        self.assertTrue(np.isnan(atr[13]))


def mk_trade(month_ms, r, long=True, day=0):
    return {'entry_ts': month_ms, 'exit_ts': month_ms + day * 86_400_000 + 3_600_000, 'R': r, 'Rg': r, 'reason': 'TP' if r > 0 else 'SL', 'long': long}


class Verdict(unittest.TestCase):
    def _trades(self, up_r, down_r, n_days=150, per_day=2):
        """OOS-сделки: чётные месяцы — рост BTC, нечётные — падение; IS — 40 сделок с положительным R."""
        out, btc = [], {}
        for m in range(1, 13):
            btc[f'2025-{m:02d}'] = 0.05 if m % 2 == 0 else -0.05
        base = int(datetime(2025, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
        for d in range(n_days):
            ms = base + d * 86_400_000
            month = datetime.fromtimestamp(ms / 1000, timezone.utc).month
            for j in range(per_day):
                r = (up_r if month % 2 == 0 else down_r) + (0.9 if j else -0.9) * (1 if d % 2 else -1) * 0.5 + (d % 7) * 0.01
                out.append(mk_trade(ms + j * 1000, r))
        is_base = int(datetime(2024, 3, 1, tzinfo=timezone.utc).timestamp() * 1000)
        out += [mk_trade(is_base + i * 86_400_000, 0.2) for i in range(40)]
        for m in range(1, 13):
            btc[f'2024-{m:02d}'] = 0.05 if m % 2 == 0 else -0.05
        return out, btc

    def test_pass(self):
        tr, btc = self._trades(0.6, 0.6, n_days=300)
        v = E.cell_verdict(tr, btc)
        self.assertEqual(v['status'], 'PASS', v['checks'])

    def test_beta_fails_even_if_total_is_great(self):
        tr, btc = self._trades(1.6, -0.2, n_days=300)       # прибыль только в месяцы роста BTC
        v = E.cell_verdict(tr, btc)
        self.assertEqual(v['status'], 'FAIL')
        self.assertFalse(v['checks']['mean R&gt;0 в месяцы падения BTC'])
        self.assertTrue(v['checks']['mean R&gt;0 в месяцы роста BTC'])

    def test_nodata(self):
        tr, btc = self._trades(0.6, 0.6, n_days=40)
        self.assertEqual(E.cell_verdict(tr, btc)['status'], 'NODATA')

    def test_low_t_fails(self):
        tr, btc = self._trades(0.01, 0.01, n_days=300)
        v = E.cell_verdict(tr, btc)
        self.assertEqual(v['status'], 'FAIL')

    def test_btc_month_dirs(self):
        x = walk(3000, 8)
        d = E.btc_month_dirs(x)
        self.assertTrue(all(isinstance(v, float) for v in d.values()))
        self.assertIn('2024-06', d)


class ShadowCompare(unittest.TestCase):
    def test_compare(self):
        db, _ = fresh_state()
        t = int(datetime(2026, 10, 3, 12, tzinfo=timezone.utc).timestamp() * 1000)
        con = sqlite3.connect(db)
        def ins(sym, d, ms):
            con.execute("INSERT INTO shadow_signals (symbol,direction,strategy,open_time,status) VALUES (?,?,?,?,?)",
                        (sym, d, 'SMC2_MKT', datetime.fromtimestamp(ms / 1000, timezone.utc).isoformat(), 'closed'))
        ins('BTC/USDT:USDT', 'Short', t + E.TF_MS)            # совпадает с событием R1
        ins('BTC/USDT:USDT', 'Long', t + 50 * E.TF_MS)         # только в shadow
        ins('DOGE/USDT:USDT', 'Long', t)                       # не из вселенной → игнор
        con.commit(); con.close()
        r1 = {'BTCUSDT': [('Short', t), ('Long', t + 10 * E.TF_MS)]}
        res = E.compare_shadow(db, r1, {'BTCUSDT': 'BTC/USDT:USDT'}, data_end_ms=t + 100 * E.TF_MS)
        self.assertTrue(res['ok'])
        self.assertEqual((res['r1'], res['shadow'], res['r1_in_sh'], res['sh_in_r1']), (2, 2, 1, 1))
        res = E.compare_shadow(db, r1, {'BTCUSDT': 'BTC/USDT:USDT'}, data_end_ms=T0)
        self.assertFalse(res['ok'])


class EndToEnd(unittest.TestCase):
    def _cache(self, d, n=1800):
        for i, sym in enumerate(['BTCUSDT', 'ETHUSDT', 'SOLUSDT']):
            x = walk(n, 70 + i, sd=0.004)
            V.save_klines(d, sym, '15m', x['ts'], x, ['2024-06'])

    def test_full_run_offline(self):
        db, sent = fresh_state()
        d = tempfile.mkdtemp(prefix='vision_')
        self._cache(d)
        uni = ['BTC/USDT:USDT', 'ETH/USDT:USDT', 'SOL/USDT:USDT']
        text, res = run(E.run_edge_audit(None, db, uni, *FEES[:2], a._smc2_eval, a.get_pivots, progress=a.tg, d=d, sync=False))
        self.assertTrue(text.index('R0') < text.index('R1'))
        self.assertIn('ВЕРДИКТ R0', text)
        self.assertIn('ВЕРДИКТ R1', text)
        self.assertEqual(res['r0'] in ('OK', 'WARN'), True)
        self.assertIn('НЕПРОВЕРЯЕМ', text)                    # фандинг не загружен → F непроверяем, правила не меняются
        self.assertEqual(res['passed'], [])
        self.assertTrue(any('R1 BTCUSDT' in m for m in sent))

    def test_stop_on_measurement_error_ends_at_r0(self):
        db, sent = fresh_state()
        d = tempfile.mkdtemp(prefix='vision_')
        self._cache(d, 1200)
        orig = E.r0_trades

        def biased(sym, idx, arr, fees, n_target=E.N_RANDOM, seed_base=E.SEED):
            tr, dg = orig(sym, idx, arr, fees, n_target=n_target, seed_base=seed_base)
            for t in tr:
                t['Rg'] = t['Rg'] + 0.25                     # искусственная ошибка измерения
            return tr, dg
        E.r0_trades = biased
        try:
            text, res = run(E.run_edge_audit(None, db, ['BTC/USDT:USDT', 'ETH/USDT:USDT'], *FEES[:2], a._smc2_eval, a.get_pivots, progress=a.tg, d=d, sync=False))
        finally:
            E.r0_trades = orig
        self.assertEqual(res['r0'], 'STOP')
        self.assertIn('ОШИБКА ИЗМЕРЕНИЯ', text)
        self.assertNotIn('R1: SMC2', text)
        self.assertNotIn('ВЕРДИКТ R1', text)
        self.assertFalse(any(m.startswith('R1 ') for m in sent))


if __name__ == '__main__':
    unittest.main()
