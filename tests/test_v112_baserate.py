"""v112: проверка базовой ставки в /audit (BOT_SPEC §2.44.10): на синтетическом случайном блуждании с a = 1.2%, b = 13% фактическая доля первой цели ≈ ожидаемой (|z| < 2);
формулы p_i и z; строки-выводы; блок в отчёте секции."""
import unittest

import numpy as np

import _common
import audit_sim as AS

MIN = 60_000
ROOT = _common.ROOT
FEES0 = {'taker': 0.0, 'maker': 0.0}
NOW = 10 ** 14


def fake(a, b, hit, closed=True, status='filled'):
    return {'status': status, 'B': {'closed': closed, 'R': 0.0}, 'geo': (a, b) if a is not None else None, 'hit': [1] if hit else []}


class Formula(unittest.TestCase):
    def test_expected_and_z(self):
        rows = [fake(1.0, 3.0, True)] * 3 + [fake(1.0, 3.0, False)]            # p = 0.75, 3 из 4
        br = AS.base_rate(rows)
        self.assertEqual(br['n'], 4)
        self.assertAlmostEqual(br['exp'], 0.75)
        self.assertAlmostEqual(br['act'], 0.75)
        self.assertAlmostEqual(br['z'], 0.0)
        self.assertIn('не чаще случайного входа с той же геометрией', br['verdict'])

    def test_more_often_and_less_often(self):
        up = AS.base_rate([fake(5.0, 5.0, True)] * 100)                          # p = 0.5, достигнуто 100 из 100
        self.assertGreaterEqual(up['z'], 2.0)
        self.assertIn('чаще случайного входа на 50.0 п. п.', up['verdict'])
        dn = AS.base_rate([fake(1.0, 9.0, False)] * 100)                         # p = 0.9, достигнуто 0
        self.assertLessEqual(dn['z'], -2.0)
        self.assertIn('реже случайного входа на 90.0 п. п.', dn['verdict'])

    def test_skips_open_unfilled_and_bad_geometry(self):
        rows = [fake(1.0, 3.0, True), fake(1.0, 3.0, True, closed=False), fake(None, None, True), {'status': 'not_filled'}, fake(1.0, 3.0, False, status='filled')]
        br = AS.base_rate(rows)
        self.assertEqual(br['n'], 2)
        self.assertEqual(br['n_skip'], 1)
        self.assertIsNone(AS.base_rate([{'status': 'not_filled'}]))

    def test_geometry_sides(self):
        self.assertEqual(AS.geometry('short', 100.0, [99.0], 103.0), (1.0, 3.0))
        self.assertEqual(AS.geometry('long', 100.0, [101.0], 97.0), (1.0, 3.0))
        self.assertIsNone(AS.geometry('short', 100.0, [101.0], 103.0))         # цель не в сторону прибыли
        self.assertIsNone(AS.geometry('long', 100.0, [101.0], 103.0))          # стоп не в сторону убытка


class RandomWalk(unittest.TestCase):
    def test_actual_share_matches_expected_on_driftless_walk_a12_b13(self):
        """Шорт: вход 100, цель 98.8 (a = 1.2%), стоп 113 (b = 13%). Блуждание без сноса, срок удержания продлён, чтобы таймаут не искажал долю."""
        old = AS.HOLD_MS
        AS.HOLD_MS = 60 * 86_400_000
        try:
            rng = np.random.default_rng(2024)
            n_min = 61 * 24 * 60
            ts = MIN * (1 + np.arange(n_min, dtype=np.int64))
            rows = []
            for _ in range(400):
                c = 100.0 * np.exp(np.cumsum(0.001 * rng.standard_normal(n_min)) - 0.0)
                o = np.r_[100.0, c[:-1]]
                cd = {'ts': ts, 'o': o, 'h': np.maximum(o, c) * 1.0004, 'l': np.minimum(o, c) * 0.9996, 'c': c}
                sig = {'side': 'short', 'leverage': None, 'entry_lo': 100.0, 'entry_hi': 100.0, 'targets': [98.8, 97.0], 'stop': 113.0, 'pub_ms': 0}
                r = AS.simulate(sig, cd, FEES0, NOW)
                if r['status'] == 'filled':
                    rows.append(r)
        finally:
            AS.HOLD_MS = old
        self.assertGreater(len(rows), 380)
        br = AS.base_rate(rows)
        self.assertAlmostEqual(br['exp'], 13.0 / 14.2, places=3)
        self.assertLess(abs(br['z']), 2.0, br)
        self.assertIn('не чаще', br['verdict'])


class Report(unittest.TestCase):
    def test_section_contains_base_rate_block(self):
        rows = []
        for i in range(30):
            r = fake(1.2, 13.0, i % 12 != 0)
            r.update({'pub_ms': 1_700_000_000_000 + i * 86_400_000, 'exit_ts': 1_700_000_000_000 + i * 86_400_000 + 3600_000, 'symbol': 'AAA', 'side': 'short', 'source': 'live', 'A': None, 'note': 'BingX'})
            r['B'].update({'R': 0.1, 'kinds': ['tp']})
            rows.append(r)
        text = '\n'.join(AS.section('ФОРВАРД', rows, {}))
        self.assertIn('проверка базовой ставки (§2.44.10)', text)
        self.assertIn('не учитывает таймаут (7 суток) и тренд', text)


if __name__ == '__main__':
    unittest.main()
