"""v111: поиск по данным канала (BOT_SPEC §2.45) — признаки только из данных ДО публикации, HOLDOUT закрыт без флага, пороги фильтров — только квантили 20/80 DISCOVERY,
обратная торговля вне отбора, симуляция вариантов «его вход + наш стоп», статистика (Манн–Уитни, BH), сквозной прогон на синтетике (флаг финала, повтор запрещён)."""
import asyncio
import json
import math
import os
import tempfile
import unittest
import zlib

import numpy as np

import _common  # noqa: F401
import audit_db as AD
import audit_study as ST

H, MIN, DAY = ST.H, 60_000, 86_400_000
FEES0 = {'taker': 0.0, 'maker': 0.0}
NOW = 10 ** 13


def run(c):
    return asyncio.run(c)


def walk_bars(n, seed, t0=0, p0=100.0):
    rng = np.random.default_rng(seed)
    c = p0 * np.exp(np.cumsum(0.004 * rng.standard_normal(n)))
    o = np.r_[p0, c[:-1]]
    return {'ts': t0 + H * np.arange(n, dtype=np.int64), 'o': o, 'h': np.maximum(o, c) * 1.002, 'l': np.minimum(o, c) * 0.998, 'c': c, 'v': 1000 + 100 * rng.random(n)}


def same(a, b):
    for k in a:
        np.testing.assert_equal(a[k], b[k])


class Features(unittest.TestCase):
    def setUp(self):
        self.b = walk_bars(500, 7)
        self.pub = int(self.b['ts'][300]) + 20 * MIN                    # t_h = ts[300]; последний закрытый бар — индекс 299

    def future_scrambled(self, b, from_idx):
        b2 = {k: v.copy() for k, v in b.items()}
        for k in ('o', 'h', 'l', 'c', 'v'):
            b2[k][from_idx:] = b2[k][from_idx:] * 7.3 + 11
        return b2

    def test_hourly_features_ignore_everything_from_the_publication_hour(self):
        f0 = ST.hourly_features(self.b, self.pub)
        self.assertTrue(math.isfinite(f0['ret7d']) and math.isfinite(f0['atr']) and math.isfinite(f0['rsi']) and math.isfinite(f0['vol_ratio']) and math.isfinite(f0['hi7']))
        same(f0, ST.hourly_features(self.future_scrambled(self.b, 300), self.pub))        # бар публикации и всё после — не влияют
        b3 = {k: v.copy() for k, v in self.b.items()}
        b3['c'][299] *= 1.1
        self.assertNotEqual(ST.hourly_features(b3, self.pub)['ret4h'], f0['ret4h'])        # а последний закрытый бар — влияет

    def test_btc_features_ignore_future(self):
        b = walk_bars(900, 3, t0=0)
        pub = int(b['ts'][700]) + 5 * MIN
        f0 = ST.btc_features(b, pub)
        self.assertTrue(math.isfinite(f0['btc_ret24h']) and math.isfinite(f0['btc_slope']))
        same(f0, ST.btc_features(self.future_scrambled(b, 700), pub))

    def test_turnover_funding_and_channel_counts_use_only_the_past(self):
        b = walk_bars(1200, 5)
        pub = int(b['ts'][1000]) + 7 * MIN
        t0 = ST.turnover30(ST.make_turn(b), pub)
        self.assertTrue(math.isfinite(t0) and t0 > 0)
        self.assertEqual(t0, ST.turnover30(ST.make_turn(self.future_scrambled(b, 1000)), pub))
        fund = (np.array([pub - 3 * H, pub + H], dtype=np.int64), np.array([0.0001, 0.5]))
        self.assertEqual(ST.last_funding(fund, pub), 0.0001)
        pub_s = pub // 1000
        ctx = [('c', pub_s - 600, 'short'), ('c', pub_s - 3600, 'long'), ('c', pub_s - 3601, 'short'), ('c', pub_s + 600, 'short'), ('x', pub_s - 10, 'short'), ('c', pub_s, 'short')]
        n, share = ST.channel_counts(ctx, 'c', 'short', pub_s)
        self.assertEqual(n, 2)                                                            # будущий (+10 мин), одновременный, чужой канал и старше часа не считаются
        self.assertEqual(share, 0.5)

    def test_wilder_atr_rsi_known_values(self):
        self.assertAlmostEqual(ST.rsi14(np.arange(1.0, 40.0)), 100.0)
        self.assertAlmostEqual(ST.rsi14(np.arange(40.0, 1.0, -1.0)), 0.0)
        h = np.full(30, 11.0)
        l = np.full(30, 9.0)
        c = np.full(30, 10.0)
        self.assertAlmostEqual(float(ST.atr14(h, l, c)[-1]), 2.0)

    def test_signed_features_follow_trade_direction(self):
        f = {'ret24h': 0.1, 'hi24': 0.5, 'lo24': 3.0, 'hi7': 1.0, 'lo7': 9.0, 'rsi': 70.0, 'funding': 0.001, 'btc_ret24h': 0.02}
        sh, lg = ST.signed_features(f, 'short'), ST.signed_features(f, 'long')
        self.assertEqual((sh['near24'], sh['far24'], lg['near24'], lg['far24']), (0.5, 3.0, 3.0, 0.5))
        self.assertAlmostEqual(sh['ret24h_s'], -0.1)
        self.assertAlmostEqual(lg['ret24h_s'], 0.1)
        self.assertAlmostEqual(sh['rsi_s'], 30.0)
        self.assertEqual(lg['rsi_s'], 70.0)
        self.assertEqual(len(ST.FEATURES), 20)

    def test_portrait_classes(self):
        self.assertEqual(ST.portrait_class('short', {'ret24h': 0.1, 'hi24': 0.4}), 'против движения у экстремума')
        self.assertEqual(ST.portrait_class('short', {'ret24h': -0.1, 'hi24': 0.4}), 'по тренду')
        self.assertEqual(ST.portrait_class('long', {'ret24h': -0.1, 'lo24': 0.4}), 'против движения у экстремума')
        self.assertEqual(ST.portrait_class('long', {'ret24h': 0.1, 'lo24': 5}), 'по тренду')
        self.assertEqual(ST.portrait_class('long', {}), 'нет данных')


class Stats(unittest.TestCase):
    def test_mannwhitney_known(self):
        _u, p = ST.mannwhitney_p([1, 2, 3, 4, 5], [6, 7, 8, 9, 10])
        self.assertAlmostEqual(p, 0.01219, delta=5e-4)
        self.assertEqual(ST.mannwhitney_p([1, 2], [3, 4, 5])[1], 1.0)
        self.assertAlmostEqual(ST.mannwhitney_p([1, 2, 3, 4], [1, 2, 3, 4])[1], 1.0)

    def test_bh(self):
        self.assertEqual(ST.bh([0.5, 0.001, 0.03, 0.01]), [False, True, True, True])
        self.assertEqual(ST.bh([0.9, 0.8]), [False, False])


def rec(R, kinds, ts=0, closed=True):
    return {'R': R, 'closed': closed, 'kinds': kinds, 'exit_ts': ts}


class Simulation(unittest.TestCase):
    def candles(self, rows):
        a = np.array(rows, float)
        n = len(a)
        return {'ts': MIN * (1 + np.arange(n, dtype=np.int64)), 'o': a[:, 0], 'h': a[:, 1], 'l': a[:, 2], 'c': a[:, 3]}

    def sig(self, **kw):
        d = {'side': 'short', 'leverage': 10, 'entry_lo': 100.0, 'entry_hi': 100.0, 'targets': [97.0, 94.0], 'stop': 105.0, 'pub_ts': 0}
        d.update(kw)
        return d

    def days(self, rows):
        return rows + [(rows[-1][3],) * 4] * (8 * 24 * 60)

    def test_two_r_target_and_stop_with_our_stop(self):
        up = self.days([(100, 100.1, 99.9, 100), (100, 100, 95.5, 96), (96, 96, 96, 96)])
        r = ST.sim_signal(self.sig(), self.candles(up), FEES0, NOW, 1.0)
        self.assertEqual(r['status'], 'filled')
        exp = -(96 * 1.0005 / (100 * 0.9995) - 1) / 0.02                                 # TP 2R = 96, издержки только проскальзывание
        self.assertAlmostEqual(r['V']['P2|R2']['R'], exp, places=6)
        self.assertEqual(r['V']['P2|R2']['kinds'], ['tp'])
        dn = self.days([(100, 100.1, 99.9, 100), (100, 102.5, 99.9, 102), (102, 102, 102, 102)])
        r2 = ST.sim_signal(self.sig(), self.candles(dn), FEES0, NOW, 1.0)
        self.assertAlmostEqual(r2['V']['P2|R2']['R'], -(102 * 1.0005 / (100 * 0.9995) - 1) / 0.02, places=6)
        self.assertEqual(r2['V']['P2|R2']['kinds'], ['stop'])
        self.assertEqual(r2['V']['P5|R2']['kinds'], ['timeout'])                         # стоп 5% (105) не задет → таймаут 7 суток
        self.assertIn('A1.5|T', r2['V'])                                                  # ATR задан → 12 вариантов
        self.assertEqual(len(r2['V']), 12)

    def test_his_targets_with_breakeven_under_our_stop(self):
        rows = self.days([(100, 100.1, 99.9, 100), (100, 100, 96.9, 97), (97, 100.5, 97, 100.2)])
        r = ST.sim_signal(self.sig(), self.candles(rows), FEES0, NOW, None)
        self.assertEqual(r['V']['P3|T']['kinds'], ['tp', 'be'])
        e = 100 * 0.9995
        exp = (0.5 * -(97 * 1.0005 / e - 1) + 0.5 * -(100 * 1.0005 / e - 1)) / 0.03
        self.assertAlmostEqual(r['V']['P3|T']['R'], exp, places=6)
        self.assertNotIn('A3|T', r['V'])                                                  # нет ATR → ATR-варианты пропущены, остальные 8 есть
        self.assertEqual(len(r['V']), 8)

    def test_reverse_trade_is_reference_with_his_stop_as_target(self):
        rows = self.days([(100, 100.1, 99.9, 100), (100, 100, 96.9, 97), (97, 97, 97, 97)])
        r = ST.sim_signal(self.sig(), self.candles(rows), FEES0, NOW, None)
        self.assertEqual(r['REV']['kinds'], ['stop'])                                     # длинная против сигнала: стоп = его цель 1 (97)
        self.assertLess(r['REV']['R'], -1.0)
        self.assertNotIn('REV', ST.VARIANTS)

    def test_not_filled_and_mae(self):
        rows = self.days([(90, 91, 89, 90)])
        self.assertEqual(ST.sim_signal(self.sig(), self.candles(rows), FEES0, NOW, 1.0)['status'], 'not_filled')
        rows = self.days([(100, 100.1, 99.9, 100), (100, 103, 99, 99), (99, 99, 96.9, 97)])
        r = ST.sim_signal(self.sig(), self.candles(rows), FEES0, NOW, 1.0)
        self.assertAlmostEqual(r['MAE']['pct'], 3.0)
        self.assertTrue(r['MAE']['reached'])


def mk_row(i, near, far=float('nan'), stop=False, side='short'):
    fs = {k: float('nan') for k in ST.FEATURES}
    fs['near24'], fs['far24'] = near, far
    return {'status': 'filled', 'side': side, 'pub_ms': i * 1000, 'fs': fs, 'f': {}, 'V': {}, 'B': rec(-1.0 if stop else 0.5, ['stop'] if stop else ['tp', 'be'], i * DAY)}


class Selection(unittest.TestCase):
    def test_filter_thresholds_are_only_discovery_quantiles_20_80(self):
        rows = [mk_row(i, near=float(i), far=float(100 - i) + 0.01 * (i % 3), stop=(i >= 70)) for i in range(100)]
        p3 = ST.part3_features(rows)
        passed = {f['name'] for f in p3['features'] if f['bh']}
        self.assertEqual(passed, {'near24', 'far24'})                                      # остальные признаки пусты → p = 1
        flts = ST.build_filters(p3, rows)
        self.assertEqual([f['id'] for f in flts], ['F1', 'F2', 'F3'])
        near = np.array([r['fs']['near24'] for r in rows])
        far = np.array([r['fs']['far24'] for r in rows])
        for fl in flts:
            self.assertLessEqual(len(fl['conds']), 2)
            for c in fl['conds']:
                vals = near if c['name'] == 'near24' else far
                self.assertAlmostEqual(c['thr'], float(np.quantile(vals, 0.8 if c['tail'] == 'gt' else 0.2)))
                self.assertEqual(c['tail'], 'gt' if c['name'] == 'near24' else 'lt')      # у стопов near24 выше, far24 ниже → хвосты gt / lt
        self.assertEqual(len(flts[2]['conds']), 2)

    def test_no_bh_features_means_no_filters(self):
        rows = [mk_row(i, near=float(i % 5), stop=(i % 2 == 0)) for i in range(60)]
        self.assertEqual(ST.build_filters(ST.part3_features(rows), rows), [])

    def test_filter_drops_tail_and_keeps_missing(self):
        conds = [{'name': 'near24', 'tail': 'gt', 'thr': 10.0}]
        self.assertFalse(ST.passes_filter({'near24': 10.5}, conds))
        self.assertTrue(ST.passes_filter({'near24': 10.0}, conds))
        self.assertTrue(ST.passes_filter({'near24': float('nan')}, conds))
        self.assertTrue(ST.passes_filter({}, None))

    def test_reverse_trade_never_enters_selection(self):
        rows = []
        for i in range(120):
            r = mk_row(i, near=1.0)
            r['REV'] = rec(5.0, ['tp'], i * DAY)                                          # обратная торговля «блестящая»
            r['V'] = {k: rec(-0.2 if i % 2 else 0.1, ['stop'], i * DAY) for k in ST.VARIANTS}
            rows.append(r)
        p1 = ST.part1(rows)
        self.assertEqual(len(ST.VARIANTS), 12)
        self.assertEqual(len(p1['variants']), 12)
        self.assertGreater(p1['rev']['mean'], 4.0)
        self.assertNotIn('REV', p1['passed'])
        self.assertNotIn('REV', p1['best'] or '')
        cands = ST.pick_candidates(p1, [], [])
        self.assertTrue(all('REV' not in c['id'] and c['variant'] != 'REV' for c in cands))

    def test_pick_candidates_limits_and_requires_positive_t(self):
        v = {k: {'t_day': 3.0 - 0.1 * i, 'bh': True} for i, k in enumerate(ST.VARIANTS)}
        v[ST.VARIANTS[0]]['t_day'] = -2.0
        p1 = {'variants': v, 'passed': [k for k in ST.VARIANTS if v[k]['t_day'] > 0], 'best': ST.VARIANTS[1], 'rev': {}}
        c = ST.pick_candidates(p1, [], [])
        self.assertEqual(len(c), 3)
        self.assertEqual(c[0]['variant'], ST.VARIANTS[1])
        self.assertNotIn(ST.VARIANTS[0], [x['variant'] for x in c])

    def test_final_eval_is_strict_about_btc_months(self):
        rows = [{'status': 'filled', 'pub_ms': int(i * 0.5 * DAY), 'fs': {}, 'B': rec(0.4 if i % 3 else -0.5, ['tp'], int(i * 0.5 * DAY) + H), 'V': {}} for i in range(400)]
        ev = ST.final_eval(rows, {'filter': None, 'variant': 'B'}, {})
        self.assertFalse(ev['pass'])
        self.assertIsNone(ev['crit']['up'] or None)
        self.assertTrue(ev['crit']['n'])


class FakeStudySource:
    """Синтетика: часовые бары и 1m-пути детерминированы по символу; запросы записываются (проверка, что HOLDOUT не запрашивался)."""

    def __init__(self):
        self.candle_calls, self.hourly_calls = [], []

    async def hourly(self, base, lo, hi, ref, now_ms):
        self.hourly_calls.append((base, lo, hi))
        t0 = lo // H * H
        n = int((hi - t0) // H) + 1
        return walk_bars(n, zlib.crc32(base.encode()), t0=t0, p0=(ref or 100.0))

    async def candles(self, base, start_ms, end_ms, ref, now_ms):
        self.candle_calls.append((base, start_ms))
        rng = np.random.default_rng(zlib.crc32(f'{base}{start_ms}'.encode()))
        n = 9 * 24 * 60
        c = ref * np.exp(np.cumsum(0.0006 * rng.standard_normal(n)))
        o = np.r_[ref, c[:-1]]
        t0 = (start_ms // MIN + 1) * MIN
        return {'ts': t0 + MIN * np.arange(n, dtype=np.int64), 'o': o, 'h': np.maximum(o, c) * 1.0003, 'l': np.minimum(o, c) * 0.9997, 'c': c}, 'BingX ' + base

    async def funding_all(self, base, s, e):
        return None

    async def btc_daily(self, since, now):
        ts = [since + i * DAY for i in range(400)]
        return ts, list(100 * np.exp(np.cumsum(0.02 * np.random.default_rng(1).standard_normal(400))))


def make_db(n=120):
    path = os.path.join(tempfile.mkdtemp(), 'audit.db')
    con = AD.connect(path)
    t0 = 1_760_000_000
    rng = np.random.default_rng(11)
    for i in range(n):
        sym = ('AAA', 'BBB', 'CCC')[i % 3]
        side = 'short' if rng.random() < 0.7 else 'long'
        px = 10.0 * (1 + i % 3)
        if side == 'short':
            text = f'🔴 SHORT #{sym}/USDT\nПлечо: 10x\nВход: {px:.4f}\nЦели: {px * 0.99:.4f} / {px * 0.98:.4f}\nСтоп: {px * 1.03:.4f}'
        else:
            text = f'🟢 LONG {sym}\nВход: {px:.4f}\nТейки: {px * 1.01:.4f}, {px * 1.02:.4f}\nSL: {px * 0.97:.4f}\nx10'
        AD.ingest(con, 'ch', i + 1, t0 + i * 5 * 3600, text, 'live' if i % 2 else 'catchup')
    return path, con, t0


class Gate(unittest.TestCase):
    def test_holdout_unavailable_without_flag(self):
        path, con, _t0 = make_db()
        sp = ST.freeze_split(con)
        self.assertEqual(sp['n_total'], 120)
        self.assertEqual(sp['n_disc'], 72)
        disc = ST.load_discovery(con, sp)
        self.assertEqual(len(disc), 72)
        self.assertTrue(all(s['pub_ts'] < sp['t_split'] for s in disc))
        with self.assertRaises(PermissionError):
            ST.load_holdout(con, sp)
        with self.assertRaises(PermissionError):
            ST.context_light(con, sp, True)
        self.assertTrue(all(t < sp['t_split'] for _c, t, _s in ST.context_light(con, sp, False)))
        AD.meta_set(con, ST.K_FLAG, '1')
        hold = ST.load_holdout(con, sp)
        self.assertEqual(len(hold), 48)
        self.assertTrue(all(s['pub_ts'] >= sp['t_split'] for s in hold))

    def test_split_is_frozen_and_ignores_new_signals(self):
        path, con, t0 = make_db()
        sp = ST.freeze_split(con)
        AD.ingest(con, 'ch', 9999, t0 + 10 ** 7, '🔴 SHORT #AAA/USDT\nВход: 10.0\nЦели: 9.9\nСтоп: 10.3', 'live')
        self.assertEqual(ST.freeze_split(con), sp)

    def test_study_never_requests_holdout_period_and_final_is_single_shot(self):
        path, con, t0 = make_db()
        src = FakeStudySource()
        params = {'adb': path, 'version': 'vT', 'now_ms': (t0 + 60 * 86400) * 1000, 'fee_taker': 0.0005, 'fee_maker': 0.0005}
        text = run(ST.run_study(params, source=src))
        sp = ST.freeze_split(con)
        for h in ('ЧАСТЬ 1', 'ЧАСТЬ 2', 'ЧАСТЬ 3', 'Кандидаты на финал', 'обратная торговля', 'MAE'):
            self.assertIn(h, text)
        self.assertEqual(len(src.candle_calls), 72)
        self.assertTrue(all(st < sp['t_split'] * 1000 for _b, st in src.candle_calls))   # цены HOLDOUT не запрашивались
        last_disc = max(s['pub_ts'] for s in ST.load_discovery(con, sp)) * 1000
        self.assertTrue(all(hi <= last_disc for _b, _lo, hi in src.hourly_calls))
        self.assertIsNone(ST.final_flag(con))
        self.assertIsNotNone(ST.load_cands(con))
        # финал: без confirm флаг не берётся; кандидаты подменяем на заведомо непустой (проверка механики)
        AD.meta_set(con, ST.K_CANDS, json.dumps({'cands': [{'id': 'V:P3|T', 'filter': None, 'variant': 'P3|T', 't_day': 1.0}]}))
        info = run(ST.run_final(dict(params), source=src))
        self.assertIn('свободен', info)
        self.assertIsNone(ST.final_flag(con))
        self.assertEqual(len(src.candle_calls), 72)
        rep = run(ST.run_final(dict(params, confirm=True), source=FakeStudySource()))
        self.assertIn('/signal_final', rep)
        self.assertIn('справочно: модель B «как есть» на HOLDOUT', rep)
        self.assertIn('НЕ ПРОЙДЕНО', rep)                                                  # на шуме и с недостаточным n критерии не выполняются
        self.assertIsNotNone(AD.meta_get(con, ST.K_DONE))
        again = run(ST.run_final(dict(params, confirm=True), source=FakeStudySource()))
        self.assertIn('отказ', again)
        # после флага повторный /signal_study не перезаписывает кандидатов
        before = AD.meta_get(con, ST.K_CANDS)
        run(ST.run_study(params, source=FakeStudySource()))
        self.assertEqual(AD.meta_get(con, ST.K_CANDS), before)

    def test_final_refuses_without_study_and_with_no_candidates_reads_nothing(self):
        path, con, t0 = make_db()
        self.assertIn('отказ', run(ST.run_final({'adb': path, 'confirm': True}, source=FakeStudySource())))
        ST.freeze_split(con)
        AD.meta_set(con, ST.K_CANDS, json.dumps({'cands': []}))
        out = run(ST.run_final({'adb': path, 'confirm': True}, source=FakeStudySource()))
        self.assertIn('закрыты окончательно', out)
        self.assertIsNone(ST.final_flag(con))


class Wiring(unittest.TestCase):
    def test_runner_tasks_and_commands_registered(self):
        import research_runner as RR
        self.assertIn('signal_study', RR.TASKS)
        self.assertIn('signal_final', RR.TASKS)
        src = open(os.path.join(_common.ROOT, 'async_bot.py'), encoding='utf-8').read()
        self.assertIn("cmd == '/signal_study'", src)
        self.assertIn("cmd.startswith('/signal_final ')", src)
        self.assertIn("CODE_VERSION = '2026-11-10-v111'", src)

    def test_worker_untouched(self):
        import subprocess
        out = subprocess.run(['git', 'diff', '--name-only', 'a325b34', '--', 'bybit_worker.py'], cwd=_common.ROOT, capture_output=True, text=True).stdout
        self.assertEqual(out.strip(), '')


if __name__ == '__main__':
    unittest.main()
