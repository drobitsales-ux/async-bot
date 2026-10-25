"""v98: C1 (сбор фандинга) по BOT_SPEC §2.40; досрочная остановка PO3/ORB_RT (дефолты false)."""
import asyncio
import os
import tempfile
import unittest
from datetime import datetime, timezone

import numpy as np

from _common import a, fresh_state, ROOT
import research_carry as C
import research_runner as RR
import research_vision as V

T0 = C.monday_ceil(int(datetime(2024, 1, 1, tzinfo=timezone.utc).timestamp() * 1000))      # понедельник
H8 = 8 * 3_600_000


def run(coro):
    return asyncio.run(coro)


def flat(sym_rates, start, weeks, step=H8):
    """{sym: (ts, rate)} — начисления каждые 8 ч от start; sym_rates: {sym: rate или функция(ts)->rate}."""
    ts = np.arange(start, start + weeks * C.WEEK_MS + 1, step, dtype=np.int64)
    out = {}
    for s, r in sym_rates.items():
        out[s] = (ts, np.array([r(t) if callable(r) else r for t in ts], dtype=np.float64))
    return out


SYMS5 = ['A', 'B', 'C', 'D', 'E']
C0_FOR_1000 = 1000 / C.EFF * C.SLOTS                      # номинал позиции ровно 1000 USDT


class Funding(unittest.TestCase):
    def test_one_accrual_on_notional_1000_is_ten_cents(self):
        ts = np.array([T0 + H8], dtype=np.int64)
        self.assertAlmostEqual(C.accrue(ts, np.array([0.0001]), T0, T0 + C.WEEK_MS, 1000.0), 0.10, places=12)
        self.assertAlmostEqual(C.slot_notional(C0_FOR_1000), 1000.0, places=9)
        self.assertEqual(C.EFF, 0.75)

    def test_accrual_interval_is_open_closed(self):
        ts = np.array([T0, T0 + H8, T0 + 2 * H8], dtype=np.int64)
        r = np.array([0.0001] * 3)
        # начисление ровно в момент открытия не получаем, в момент закрытия — получаем
        self.assertAlmostEqual(C.accrue(ts, r, T0, T0 + 2 * H8, 1000.0), 0.20, places=12)

    def test_negative_funding_reduces_income(self):
        pos = C.accrue(np.array([T0 + H8, T0 + 2 * H8]), np.array([0.0001, 0.0001]), T0, T0 + C.WEEK_MS, 1000.0)
        mixed = C.accrue(np.array([T0 + H8, T0 + 2 * H8]), np.array([0.0001, -0.00004]), T0, T0 + C.WEEK_MS, 1000.0)
        self.assertAlmostEqual(pos, 0.20, places=12)
        self.assertAlmostEqual(mixed, 0.06, places=12)
        self.assertLess(mixed, pos)
        self.assertLess(C.accrue(np.array([T0 + H8]), np.array([-0.0001]), T0, T0 + C.WEEK_MS, 1000.0), 0)       # шорт платит

    def test_simulation_funding_per_week(self):
        f = flat({s: 0.0001 for s in SYMS5}, T0 - C.WEEK_MS, 4)             # неделя истории до старта + 3 недели
        sim = C.simulate_c1(f, C0_FOR_1000, 0.0, 0.0, 0.0, t_start=T0, t_end=T0 + 3 * C.WEEK_MS)
        self.assertEqual(len(sim['weeks']), 3)
        for w in sim['weeks']:
            self.assertEqual(len(w['held']), 5)
            self.assertAlmostEqual(w['funding'], 5 * 21 * 0.10, places=9)      # 21 начисление в неделю × 0.10 USDT × 5 позиций

    def test_negative_funding_in_simulation_lowers_week(self):
        f = flat({s: 0.0001 for s in SYMS5}, T0 - C.WEEK_MS, 3)
        base = C.simulate_c1(f, C0_FOR_1000, 0.0, 0.0, 0.0, t_start=T0, t_end=T0 + 2 * C.WEEK_MS)
        f['A'] = (f['A'][0], np.where((f['A'][0] > T0 + 2 * H8) & (f['A'][0] <= T0 + 5 * H8), -0.0003, f['A'][1]))   # 3 отрицательных начисления в 1-й неделе
        neg = C.simulate_c1(f, C0_FOR_1000, 0.0, 0.0, 0.0, t_start=T0, t_end=T0 + 2 * C.WEEK_MS)
        self.assertAlmostEqual(base['weeks'][0]['funding'] - neg['weeks'][0]['funding'], 3 * (0.0001 + 0.0003) * 1000, places=9)
        self.assertLess(neg['weeks'][0]['funding'], base['weeks'][0]['funding'])


class Selection(unittest.TestCase):
    def test_window_and_threshold(self):
        ts = np.arange(T0 - C.WEEK_MS + H8, T0 + 1, H8, dtype=np.int64)           # ровно 21 начисление в (t−7д, t]
        m, n = C.window_avg(ts, np.full(len(ts), 0.0002), T0 - C.WEEK_MS, T0)
        self.assertEqual(n, 21)
        self.assertAlmostEqual(m, 0.0002, places=12)
        self.assertEqual(C.window_avg(ts, np.full(len(ts), 0.0002), T0 - C.WEEK_MS - H8, T0 - C.WEEK_MS)[1], 0)

    def test_top5_positive_min_settlements_and_ties(self):
        ts = np.arange(T0 - C.WEEK_MS + H8, T0 + 1, H8, dtype=np.int64)
        rates = {'S1': 0.0005, 'S2': 0.0004, 'S3': 0.0003, 'S4': 0.0002, 'S5': 0.0001, 'S6': 0.00005, 'NEG': -0.0009, 'ZERO': 0.0, 'TIE': 0.0004}
        f = {s: (ts, np.full(len(ts), r)) for s, r in rates.items()}
        f['FEW'] = (ts[-3:], np.full(3, 0.01))                                    # 3 начисления < 6 — не участвует, хоть ставка рекордная
        self.assertEqual(C.select_top(f, T0), ['S1', 'S2', 'TIE', 'S3', 'S4'])     # ничья S2/TIE — по имени
        f2 = {s: f[s] for s in ('S1', 'NEG', 'ZERO')}
        self.assertEqual(C.select_top(f2, T0), ['S1'])                            # положительных меньше пяти — держим меньше

    def test_all_negative_means_no_positions(self):
        ts = np.arange(T0 - C.WEEK_MS + H8, T0 + 1, H8, dtype=np.int64)
        self.assertEqual(C.select_top({'X': (ts, np.full(len(ts), -0.0001))}, T0), [])


class Fees(unittest.TestCase):
    FEES = dict(spot=0.001, perp=0.0005, slip=0.0003)

    def sim(self, f, weeks, **kw):
        return C.simulate_c1(f, C0_FOR_1000, self.FEES['spot'], self.FEES['perp'], self.FEES['slip'], t_start=T0, t_end=T0 + weeks * C.WEEK_MS)

    def test_side_cost(self):
        self.assertAlmostEqual(C.side_cost(1000, 0.001, 0.0005, 0.0003), 1000 * (0.001 + 0.0005 + 0.0006), places=12)

    def test_symbol_staying_in_top5_pays_no_repeat_fee(self):
        f = flat({s: 0.0001 for s in SYMS5}, T0 - C.WEEK_MS, 5)
        sim = self.sim(f, 4)
        side = C.side_cost(1000.0, 0.001, 0.0005, 0.0003)
        w = sim['weeks']
        self.assertEqual(len(w), 4)
        self.assertAlmostEqual(w[0]['fees'], 5 * side, places=9)                      # открытие пяти позиций
        self.assertEqual(w[0]['opened'], SYMS5)
        for k in (1, 2):
            self.assertEqual((w[k]['fees'], w[k]['opened'], w[k]['closed']), (0.0, [], []))        # повторных комиссий нет
        self.assertAlmostEqual(w[3]['fees'], 5 * side, places=9)                      # закрытие всех в конце периода
        self.assertAlmostEqual(sum(x['fees'] for x in w), 5 * 2 * side, places=9)      # ровно круг по каждой позиции
        self.assertEqual(len(sim['runs']), 5)
        self.assertTrue(all(r['close'] == T0 + 4 * C.WEEK_MS for r in sim['runs']))

    def test_replaced_symbol_pays_close_and_open(self):
        # F слабее остальных, пока A (самый сильный) не уйдёт в минус → A закрывается, F открывается
        f = flat({'A': 0.0, 'B': 0.0002, 'C': 0.0002, 'D': 0.0002, 'E': 0.0002, 'F': 0.00005}, T0 - C.WEEK_MS, 5)
        f['A'] = (f['A'][0], np.array([0.0003 if t <= T0 + C.WEEK_MS else -0.0005 for t in f['A'][0]]))
        sim = self.sim(f, 4)
        side = C.side_cost(1000.0, 0.001, 0.0005, 0.0003)
        w = sim['weeks']
        self.assertEqual(w[0]['opened'], sorted(['A', 'B', 'C', 'D', 'E']))
        self.assertEqual((w[1]['closed'], w[1]['opened']), ([], []))                  # на 2-й неделе среднее A ещё положительное
        self.assertEqual((w[2]['closed'], w[2]['opened']), (['A'], ['F']))             # затем A уходит, F входит в топ-5
        self.assertAlmostEqual(w[2]['fees'], 2 * side, places=9)                      # закрытие A + открытие F, остальные не тронуты
        self.assertNotIn('B', w[2]['opened'] + w[2]['closed'])

    def test_costs_make_pnl(self):
        f = flat({s: 0.0001 for s in SYMS5}, T0 - C.WEEK_MS, 3)
        sim = self.sim(f, 2)
        for w in sim['weeks']:
            self.assertAlmostEqual(w['pnl'], w['funding'] - w['fees'], places=9)


class Metrics(unittest.TestCase):
    def weeks(self, pnls, start=T0):
        return [{'t': start + i * C.WEEK_MS, 'end': start + (i + 1) * C.WEEK_MS, 'held': ['A'], 'opened': [], 'closed': [],
                 'funding': p, 'fees': 0.0, 'pnl': p} for i, p in enumerate(pnls)]

    def test_apr_and_capital_for_50_per_day(self):
        w = self.weeks([10.0] * 52)
        b = C._block(w, 10_000.0)
        self.assertAlmostEqual(b['apr'], 520 / 10_000 * 365 / 364, places=12)
        self.assertAlmostEqual(b['capital_50'], 50 * 365 / b['apr'], places=6)
        self.assertEqual(b['neg_share'], 0.0)
        self.assertEqual(b['max_dd'], 0.0)

    def test_negative_apr_has_no_capital(self):
        b = C._block(self.weeks([-5.0, 1.0, -2.0]), 10_000.0)
        self.assertIsNone(b['capital_50'])
        self.assertLess(b['apr'], 0)

    def test_neg_share_worst_month_drawdown(self):
        b = C._block(self.weeks([10.0, -4.0, -6.0, 5.0, 10.0]), 1000.0)
        self.assertAlmostEqual(b['neg_share'], 2 / 5)
        self.assertAlmostEqual(b['max_dd'], 10 / 1000)                                 # пик 10, дно 0
        self.assertEqual(b['worst_month'][0], '2024-01')
        self.assertAlmostEqual(b['worst_month'][1], 15 / 1000)

    def test_cost_share(self):
        w = self.weeks([10.0, 10.0])
        w[0]['fees'] = 2.0; w[0]['pnl'] = 8.0
        b = C._block(w, 1000.0)
        self.assertAlmostEqual(b['cost_share'], 2.0 / 20.0)

    def test_years_and_oos_split(self):
        start = int(datetime(2024, 12, 16, tzinfo=timezone.utc).timestamp() * 1000)
        w = self.weeks([1.0] * 6, start=start)                                          # недели 16.12 … 20.01
        s = C.summarize(w, 1000.0)
        self.assertEqual(sorted(s['years']), [2024, 2025])
        self.assertEqual(s['years'][2024]['weeks'] + s['years'][2025]['weeks'], 6)
        self.assertEqual(s['oos']['weeks'], s['years'][2025]['weeks'])


class Liquidation(unittest.TestCase):
    def test_hourly_cache_detects_rise_daily_fallback_and_threshold(self):
        d = tempfile.mkdtemp(prefix='v_')
        n = 24 * 20
        ts = np.arange(T0, T0 + n * 3_600_000, 3_600_000, dtype=np.int64)
        o = np.full(n, 100.0); h = np.full(n, 101.0); l = np.full(n, 99.0); c = np.full(n, 100.0)
        h[100] = 140.0                                                                  # +40% внутри 8 часов
        V.save_klines(d, 'AAAUSDT', '1h', ts, {'o': o, 'h': h, 'l': l, 'c': c, 'v': np.ones(n), 'tbv': np.ones(n)}, ['2024-01'])
        runs = [{'sym': 'AAA/USDT:USDT', 'open': T0, 'close': T0 + 10 * C.DAY_MS},
                {'sym': 'AAA/USDT:USDT', 'open': T0 + 12 * C.DAY_MS, 'close': T0 + 19 * C.DAY_MS},
                {'sym': 'BBB/USDT:USDT', 'open': T0, 'close': T0 + 3 * C.DAY_MS}]
        daily = {'BBB/USDT:USDT': [[T0, 10.0, 13.5, 9.0, 11.0, 1.0], [T0 + C.DAY_MS, 11.0, 11.5, 10.0, 11.0, 1.0]]}
        res = C.liquidation_check(runs, daily, {'AAA/USDT:USDT': 'AAAUSDT', 'BBB/USDT:USDT': 'BBBUSDT'}, d)
        by = {(i['sym'], i['open']): i for i in res['items']}
        self.assertAlmostEqual(by[('AAA/USDT:USDT', T0)]['rise'], 0.40, places=9)
        self.assertEqual(by[('AAA/USDT:USDT', T0)]['source'], 'Binance 1h')
        self.assertAlmostEqual(by[('AAA/USDT:USDT', T0 + 12 * C.DAY_MS)]['rise'], 0.01, places=9)
        self.assertEqual(by[('BBB/USDT:USDT', T0)]['source'], 'BingX 1D (high/open дня)')
        self.assertAlmostEqual(by[('BBB/USDT:USDT', T0)]['rise'], 0.35, places=9)
        hits = {(i['sym'], i['open']) for i in res['hits']}
        self.assertEqual(hits, {('AAA/USDT:USDT', T0), ('BBB/USDT:USDT', T0)})

    def test_max_rise_window(self):
        o = np.array([100.0] * 10); h = np.array([100, 100, 100, 130, 100, 100, 100, 100, 100, 100.0])
        self.assertAlmostEqual(C.max_rise(o, h, 8), 0.30)
        self.assertIsNone(C.max_rise(o[:5], h[:5], 8))


def synth_payload(n_syms=6, weeks=14):
    data = {}
    start = T0 - 2 * C.WEEK_MS
    for i in range(n_syms):
        ts = np.arange(start, start + weeks * C.WEEK_MS, H8, dtype=np.int64)
        rate = [0.0001 * (1 + i % 3) * (1 if (k // 21) % 5 else -0.3) for k in range(len(ts))]
        days = np.arange(start, start + weeks * C.WEEK_MS, C.DAY_MS, dtype=np.int64)
        bars = [[int(t), 100.0, 103.0, 98.0, 101.0, 10.0] for t in days]
        data[f'S{i}/USDT:USDT'] = {'1d': bars, '4h': [], 'funding': [[int(t), float(r)] for t, r in zip(ts, rate)]}
    return data, {'funding_failed': {}}


class EndToEnd(unittest.TestCase):
    def test_run_carry_report(self):
        text = C.run_carry(synth_payload(), {'fee_spot': 0.001, 'fee_perp': 0.0005, 'vision_dir': None})
        for needle in ('C1: сбор фандинга', 'ОШИБКА ВЫЖИВШЕГО', 'НЕ УЧТЕНЫ', 'базис', 'APR', 'недель с отрицательным фандингом', 'худший месяц',
                       'макс. просадка', 'издержки/валовый фандинг', 'капитал для 50 USDT/день', 'OOS с 2025-01-01', 'Покрытие фандинга BingX',
                       'Риск ликвидации шорта', 'решение за владельцем', 'ВЕРДИКТ C1'):
            self.assertIn(needle, text)
        self.assertNotIn('ПРОЙДЕНА', text)                       # критериев «пройдено/не пройдено» нет

    def test_no_data_report(self):
        text = C.run_carry(({'S0/USDT:USDT': {'1d': [], '4h': [], 'funding': []}}, {}), {'fee_spot': 0.001, 'fee_perp': 0.0005, 'vision_dir': None})
        self.assertIn('данных недостаточно', text)

    def test_runs_in_child_process_via_runner(self):
        base = tempfile.mkdtemp(prefix='jobs_')
        r = run(RR.run_job('carry_test', {'fee_spot': 0.001, 'fee_perp': 0.0005, 'vision_dir': None}, payload=synth_payload(), poll_s=0.2, base_dir=base))
        self.assertTrue(r['ok'], r)
        self.assertEqual(r['text'], C.run_carry(synth_payload(), {'fee_spot': 0.001, 'fee_perp': 0.0005, 'vision_dir': None}))

    def test_command_wiring(self):
        src = open(os.path.join(ROOT, 'async_bot.py'), encoding='utf-8').read()
        self.assertIn("elif cmd == '/carry_test':", src)
        self.assertIn("_research_job('/carry_test', 'carry_test'", src)
        self.assertIn('research_carry.prepare_carry(exchange', src)           # сеть через общий exchange — в процессе бота
        self.assertIn('/carry_test — C1', src)
        self.assertIn('carry_test', RR.TASKS)
        self.assertEqual(a.FEE_SPOT, float(os.getenv('FEE_SPOT', '0.001')))


class EarlyStop(unittest.TestCase):
    def test_defaults_false(self):
        if os.getenv('PO3_ENABLED') is None:
            self.assertIs(a.PO3_ENABLED, False)
        if os.getenv('ORB_RT_ENABLED') is None:
            self.assertIs(a.ORB_RT_ENABLED, False)
        src = open(os.path.join(ROOT, 'async_bot.py'), encoding='utf-8').read()
        self.assertIn("os.getenv('PO3_ENABLED', 'false')", src)
        self.assertIn("os.getenv('ORB_RT_ENABLED', 'false')", src)

    def test_scanners_do_not_run_when_disabled(self):
        fresh_state()
        saved = (a.PO3_ENABLED, a.ORB_RT_ENABLED, a._scan_universe)

        async def boom(*_a, **_k):
            raise AssertionError('сканер запустился при выключенном флаге')
        a.PO3_ENABLED = a.ORB_RT_ENABLED = False
        a._scan_universe = boom
        try:
            self.assertIsNone(run(a.scan_po3()))
            self.assertIsNone(run(a.scan_orb_rt()))
        finally:
            a.PO3_ENABLED, a.ORB_RT_ENABLED, a._scan_universe = saved

    def test_startup_message_without_po3_orb_rt_by_default(self):
        fresh_state()
        saved = (a.PO3_ENABLED, a.ORB_RT_ENABLED, a.SMC_SIG_ENABLED)
        a.PO3_ENABLED = a.ORB_RT_ENABLED = False
        a.SMC_SIG_ENABLED = True                                    # v107: по умолчанию выключен — включаем явно
        try:
            live, shadow = a.active_strategies()
            self.assertNotIn('PO3', shadow)
            self.assertNotIn('ORB_RT', shadow)
            self.assertTrue(any(x.startswith('SMC_SIG') for x in shadow) or a.SMC_LIVE)    # SMC_SIG сохранён
        finally:
            a.PO3_ENABLED, a.ORB_RT_ENABLED, a.SMC_SIG_ENABLED = saved


if __name__ == '__main__':
    unittest.main()
