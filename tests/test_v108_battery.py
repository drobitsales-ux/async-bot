"""v108: серия «батарея» (BOT_SPEC §2.43): PIT-ликвидность без заглядывания вперёд, H2 включает делистингованные, H5 — худший порядок внутри бара, проверка мощности исключает заведомо
бессильную гипотезу, загрузчик (листинг, продолжение с места остановки), H1/H3 по правилам U1/U3, шаги 0–3 и ворота финала (флаг серии до чтения LOCKBOX, повтор запрещён)."""
import asyncio
import io
import math
import os
import tempfile
import unittest
import zipfile

import numpy as np

import _common  # noqa: F401
import battery_fixtures as BF
import research_bdata as B
import research_battery as RB
import research_bsim as S
import research_factory as RF
import research_fengine as E
import research_ma_lib as L
import research_vision as V

DAY = B.DAY_MS
FEES0 = {'taker': 0.0, 'maker': 0.0}


def run(c):
    return asyncio.run(c)


def mini_panel(nd=80, ns=12, day0=0, qv=1e9, first=None, last=None, price=100.0):
    """Ручная дневная панель: плоские цены ±1% диапазон, оборот qv; first/last — индексы жизни символов."""
    P = {}
    base = np.full((nd, ns), price, np.float32)
    P['C'] = base.copy()
    P['O'] = base.copy()
    P['H'] = base * 1.01
    P['L'] = base * 0.99
    P['QV'] = np.full((nd, ns), qv, np.float32)
    P['F'] = np.zeros((nd, ns), np.float32)
    P['G'] = np.zeros((nd, ns), np.float32)
    P['GC'] = np.full((nd, ns), 3, np.float32)
    P['first'] = np.array(first if first is not None else [0] * ns, np.int64)
    P['last'] = np.array(last if last is not None else [nd - 1] * ns, np.int64)
    for j in range(ns):
        for k in ('O', 'H', 'L', 'C', 'QV'):
            P[k][:P['first'][j], j] = np.nan
            P[k][P['last'][j] + 1:, j] = np.nan
    P.update({'syms': [f'S{j:02d}USDT' for j in range(ns)], 'day0': day0, 'nd': nd, 'excluded_stable': [], 'has_fund': np.ones(ns, bool)})
    return P


def trend(P, j, per_day):
    c = P['C'][:, j].astype(np.float64)
    ok = np.isfinite(c)
    n = int(ok.sum())
    px = 100.0 * (1 + per_day) ** np.arange(n)
    o = np.r_[100.0, px[:-1]]
    P['C'][ok, j] = px
    P['O'][ok, j] = o
    P['H'][ok, j] = np.maximum(o, px) * 1.005
    P['L'][ok, j] = np.minimum(o, px) * 0.995


class PitLiquidity(unittest.TestCase):
    def test_adv_atr_and_signal_do_not_look_forward(self):
        rng = np.random.default_rng(1)
        QV = rng.uniform(1e6, 1e8, (120, 5)).astype(np.float32)
        adv, cnt = B.pit_adv(QV)
        D = 70
        QV2 = QV.copy()
        QV2[D:] = 1e12                                                            # меняем день D и всё после него
        adv2, _ = B.pit_adv(QV2)
        np.testing.assert_allclose(adv[:D + 1], adv2[:D + 1])                      # adv[D] использует дни < D
        self.assertFalse(np.allclose(adv[D + 1], adv2[D + 1]))                     # а следующий день уже видит изменённый D
        # окно строго 30 предшествующих суток
        self.assertAlmostEqual(float(adv[60, 0]), float(QV[30:60, 0].astype(np.float64).mean()), places=0)
        P = mini_panel(nd=60)
        P['H'][40:] *= 5.0
        a1 = B.atr_panel(mini_panel(nd=60)['H'], mini_panel(nd=60)['L'], mini_panel(nd=60)['C'])
        a2 = B.atr_panel(P['H'], P['L'], P['C'])
        np.testing.assert_allclose(a1[:41], a2[:41], equal_nan=True)               # ATR[d] не видит день d и позже
        P['G'][:, 0] = 0.001
        ctx = S.make_ctx(P)
        avg1, _ = S.h1_signal(ctx, 30, 7)
        P['G'][30:, 0] = -0.5
        avg2, _ = S.h1_signal(ctx, 30, 7)
        np.testing.assert_allclose(avg1, avg2)                                     # фандинг ≥ дня сигнала в сигнал не входит

    def test_liquidity_threshold_and_top30(self):
        P = mini_panel(nd=40, ns=3)
        P['QV'][:, 0] = 19e6
        P['QV'][:, 1] = 21e6
        P['QV'][:, 2] = np.nan
        adv, _ = B.pit_adv(P['QV'])
        liq = B.liquid_mask(adv)
        self.assertFalse(liq[20, 0])
        self.assertTrue(liq[20, 1])
        self.assertFalse(liq[20, 2])                                               # нет оборота — не участвует
        self.assertFalse(liq[0, 1])                                                # в день 0 нет предшествующей истории
        top = B.top_n_mask(adv, 1)
        self.assertTrue(top[20, 1] and not top[20, 0])


class H2NewList(unittest.TestCase):
    def test_delisted_symbol_is_included_and_closed_at_last_bar(self):
        P = mini_panel(nd=100, ns=3, first=[20, 0, 0], last=[40, 99, 99])
        ctx = S.make_ctx(P)
        ev = S.h2_events(ctx, 3)
        self.assertIn((23, 0), [(e, s) for e, s in ev])                            # делистингованный символ — в выборке (ошибка выжившего исключена)
        res = S.sim_h2(ctx, [(23, 0)], FEES0, costs_on=False)
        self.assertEqual(res['meta']['delist'], 1)
        self.assertEqual(res['meta']['opened'], 1)
        self.assertTrue(np.isfinite(res['r']).all())

    def test_preexisting_symbols_are_not_listings(self):
        P = mini_panel(nd=100, ns=3, first=[0, 10, 0])
        ctx = S.make_ctx(P)
        self.assertEqual([(e, s) for e, s in S.h2_events(ctx, 3)], [(13, 1)])        # символы с первым баром в первый день архива — не листинги (§2.43.8)

    def test_fixture_with_delisted_symbols(self):
        d = tempfile.mkdtemp()
        BF.write_battery(d, n_syms=19, delist_n=3)
        P = B.load_panel(d, 'pre')
        ctx = S.make_ctx(P)
        self.assertGreaterEqual(int((P['last'] < P['nd'] - 1).sum()), 3)
        names = {P['syms'][s] for _e, s in S.h2_events(ctx, 3)}
        self.assertTrue({'COIN00USDT', 'COIN01USDT', 'COIN02USDT'} <= names)

    def test_stop_gap_and_cap(self):
        P = mini_panel(nd=120, ns=30, first=[0] * 30)
        ctx = S.make_ctx(P)
        ev = [(10, j) for j in range(30)]
        res = S.sim_h2(ctx, ev, FEES0, costs_on=False)
        self.assertEqual((res['meta']['opened'], res['meta']['skipped_cap']), (20, 10))   # риск ≤ 10% → не более 20 позиций
        # стоп +50% с гэпом: бар открылся за уровнем → выход по open
        P = mini_panel(nd=60, ns=2)
        P['O'][15:, 0] = 160.0
        P['H'][15:, 0] = 161.0
        P['C'][15:, 0] = 160.0
        res = S.sim_h2(S.make_ctx(P), [(10, 0)], FEES0, costs_on=False)
        w = S.RISK / S.H2_STOP
        self.assertEqual(res['meta']['stops'], 1)
        self.assertAlmostEqual(float(res['r'].sum()), -w * 0.6, places=6)           # убыток по open 160 (а не по стопу 150)

    def test_short_profits_when_price_falls_and_funding_is_received(self):
        P = mini_panel(nd=80, ns=2)
        trend(P, 0, -0.01)
        P['F'][:, 0] = 0.0001
        ctx = S.make_ctx(P)
        r1 = S.sim_h2(ctx, [(10, 0)], FEES0, fund_on=False, costs_on=False)['r'].sum()
        r2 = S.sim_h2(ctx, [(10, 0)], FEES0, fund_on=True, costs_on=False)['r'].sum()
        self.assertGreater(r1, 0)
        self.assertAlmostEqual(float(r2 - r1), S.RISK / S.H2_STOP * 0.0001 * S.H2_HOLD, places=6)


class H1FundContra(unittest.TestCase):
    def setup_panel(self):
        P = mini_panel(nd=80, ns=12)
        trend(P, 0, -0.01)
        trend(P, 1, 0.01)
        P['G'][:, 0] = P['F'][:, 0] = 0.001                                         # самый высокий положительный → шорт
        P['G'][:, 1] = P['F'][:, 1] = -0.001                                        # самый отрицательный → лонг
        return P

    def test_shorts_positive_longs_negative_and_funding_in_pnl(self):
        P = self.setup_panel()
        ctx = S.make_ctx(P)
        a = S.sim_h1(ctx, 3, FEES0, fund_on=False, costs_on=False)
        b = S.sim_h1(ctx, 3, FEES0, fund_on=True, costs_on=False)
        self.assertEqual(a['meta']['positions'], 2 * a['meta']['rebalances'])      # только два символа проходят знак фандинга
        self.assertGreater(a['r'].sum(), 0)
        self.assertGreater(b['r'].sum(), a['r'].sum())                              # шорт при положительном фандинге и лонг при отрицательном — получают
        self.assertEqual(a['meta']['stops'], 0)

    def test_flat_symbol_with_zero_atr_is_not_eligible(self):
        P = self.setup_panel()
        P['H'][:, 5] = P['L'][:, 5] = P['O'][:, 5] = P['C'][:, 5] = 100.0              # неподвижный ряд: ATR = 0 (ZeroDivisionError в /battery 2026-10-26)
        ctx = S.make_ctx(P)
        avg, ok = S.h1_signal(ctx, 40, 3)
        self.assertFalse(bool(ok[5]))
        for seed in range(5):
            S.sim_h1(ctx, 3, FEES0, rand=True, rng=np.random.default_rng(seed), costs_on=False)   # не падает

    def test_random_strategy_is_independent_of_funding(self):
        P = self.setup_panel()
        ctx = S.make_ctx(P)
        r = S.sim_h1(ctx, 3, FEES0, rand=True, rng=np.random.default_rng(3), costs_on=False)
        self.assertEqual(r['meta']['positions'], 10 * r['meta']['rebalances'])

    def test_costs_reduce_pnl(self):
        P = self.setup_panel()
        ctx = S.make_ctx(P)
        a = S.sim_h1(ctx, 3, {'taker': 0.0005, 'maker': 0.0}, costs_on=True)['r'].sum()
        b = S.sim_h1(ctx, 3, {'taker': 0.0005, 'maker': 0.0}, costs_on=False)['r'].sum()
        self.assertLess(a, b)


class H3FundTime(unittest.TestCase):
    def hc(self, rates):
        """Один символ, 6 суток часовых баров; расчёты в 00/08/16 со ставками rates[(день, час)]."""
        nh = 24 * 6
        t0 = 24 * 100
        g = {'t0': t0, 'nh': nh, 'names': ['AAAUSDT']}
        for k, v in (('O', 100.0), ('H', 101.0), ('L', 99.0), ('C', 100.5)):
            g[k] = np.full((nh, 1), v)
        ev_ts = np.array([(t0 + 24 * d + h) * B.HOUR_MS for (d, h) in sorted(rates)], np.int64)
        ev_r = np.array([rates[k] for k in sorted(rates)])
        fund = {'AAAUSDT': (ev_ts, ev_r)}
        FB = B.hourly_funding(g, fund)
        P = mini_panel(nd=110, ns=1, day0=0)
        P['QV'][:] = 1e9
        ctx = S.make_ctx(P)
        return {'grid': g, 'FB': FB, 'fund': fund, 'pmap': [0]}, ctx, t0

    def test_before_uses_previous_settlement_and_after_uses_current_one(self):
        rates = {(d, h): 0.0 for d in range(6) for h in (0, 8, 16)}
        rates[(2, 8)] = 0.001                       # положительная в 08:00 дня 2
        rates[(2, 16)] = -0.002                     # отрицательная в 16:00 дня 2
        hc, ctx, t0 = self.hc({k: (v if v else 1e-9) for k, v in rates.items()})
        pre = S.h3_prepare(hc, ctx, 'before')
        hod = (t0 + pre['row']) % 24
        day = (t0 + pre['row']) // 24 - 100
        pick = lambda p, d, h: p['sig'][np.flatnonzero((((t0 + p['row']) // 24 - 100) == d) & (((t0 + p['row']) % 24) == h))]
        # T=16: бар [15:00,16:00]; последняя ставка ≤ 15:00 — 08:00 (положительная) → шорт (−1); ставка 16:00 в знак НЕ входит
        self.assertEqual(float(pick(pre, 2, 15)[0]), -1.0)
        # T=00 следующих суток: бар [23:00, 24:00]; последняя ставка ≤ 23:00 — 16:00 (отрицательная) → лонг (+1)
        self.assertEqual(float(pick(pre, 2, 23)[0]), 1.0)
        post = S.h3_prepare(hc, ctx, 'after')
        self.assertEqual(float(pick(post, 2, 8)[0]), 1.0)       # ставка 08:00 положительная → лонг «по знаку»
        self.assertEqual(float(pick(post, 2, 16)[0]), -1.0)     # отрицательная → шорт
        # смена ставки в T не меняет знак «до» для бара, который заканчивается в T
        rates2 = dict(rates)
        rates2[(2, 16)] = +0.005
        hc2, ctx2, _ = self.hc({k: (v if v else 1e-9) for k, v in rates2.items()})
        pre2 = S.h3_prepare(hc2, ctx2, 'before')
        self.assertEqual(float(pick(pre2, 2, 15)[0]), -1.0)
        self.assertTrue(set(np.unique(hod[:3])) <= {7, 15, 23})
        self.assertGreater(len(day), 0)

    def test_funding_of_settlement_is_in_pnl_for_before_variant_only(self):
        rates = {(d, h): (0.001 if h == 16 else 1e-9) for d in range(6) for h in (0, 8, 16)}
        hc, ctx, t0 = self.hc(rates)
        pre = S.h3_prepare(hc, ctx, 'before')
        days = np.arange(0, 110, dtype=np.int64)
        a = S.sim_h3(pre, FEES0, days, fund_on=False, costs_on=False)['r'].sum()
        b = S.sim_h3(pre, FEES0, days, fund_on=True, costs_on=False)['r'].sum()
        self.assertNotAlmostEqual(float(a), float(b))
        post = S.h3_prepare(hc, ctx, 'after')
        a2 = S.sim_h3(post, FEES0, days, fund_on=False, costs_on=False)['r'].sum()
        b2 = S.sim_h3(post, FEES0, days, fund_on=True, costs_on=False)['r'].sum()
        self.assertAlmostEqual(float(a2), float(b2))                                  # после расчёта фандинг в бар не попадает


class H5Grid(unittest.TestCase):
    @staticmethod
    def win(o, h, l, c, nxt=100.0):
        T = len(o)
        row = lambda x: np.array([x], float)
        return {'O': row(o), 'H': row(h), 'L': row(l), 'C': row(c), 'FB': np.zeros((1, T)), 'P0': np.array([100.0]), 'atr': np.array([100.0 * 0.05 / 1.5]),
                'valid': np.array([True]), 'top': np.array([True]), 'nxt': np.array([nxt]), 'm': 1.5}

    Q = (1.0 / S.HXS_N / 10.0) / 100.0                                       # количество на уровень; шаг s = 1.0, уровни 95…99 и 101…105, стоп 107.5 / 92.5

    def test_worst_order_inside_bar(self):
        w = self.win([100, 100, 100], [100.2, 100.4, 100.1], [98.9, 99.4, 99.5], [99.5, 100.0, 100.0])
        worst = S.grid_sim(w, FEES0, 1.0, False, False, 'worst')['ret'][0]
        best = S.grid_sim(w, FEES0, 1.0, False, False, 'best')['ret'][0]
        # худший порядок: покупка на 99 исполнена, закрывающая продажа на 100 в ЭТОМ же баре не исполняется (бар дошёл до 100.2, но порядок неизвестен)
        self.assertAlmostEqual(worst[0], self.Q * 0.5, places=12)
        self.assertAlmostEqual(worst[1], self.Q * 0.5, places=12)            # закрытие — в следующем баре: итого s = 1.0
        self.assertAlmostEqual(best[0], self.Q * 1.0, places=12)             # оптимистичный порядок (диагностика) — прибыль сразу

    def test_stop_bar_fills_all_openings_and_no_closings_then_flat(self):
        w = self.win([100, 100, 100], [108, 100.5, 100.4], [99.0, 99.6, 99.6], [107, 100, 100])
        res = S.grid_sim(w, FEES0, 1.0, False, False)
        self.assertAlmostEqual(res['ret'][0][0], -14.0 * self.Q, places=12)  # лонг 99 → 107.5: +8.5; шорты 101…105 → 107.5: −22.5; закрытий в баре выхода нет
        self.assertEqual(res['n_stop'], 1)
        self.assertEqual(float(np.abs(res['ret'][0][1:]).sum()), 0.0)         # после выхода — flat до следующего понедельника
        self.assertEqual(float(res['tail'][0]), 0.0)

    def test_both_edges_in_one_bar_take_the_worse_path_and_costs(self):
        w = self.win([100, 100], [108, 100], [92.0, 100], [100, 100])
        res = S.grid_sim(w, FEES0, 1.0, False, False)
        self.assertEqual(res['n_stop'], 1)
        # путь «вверх, затем стоп»: исполнены только продажи 101…105, закрытие по 107.5 → −22.5·Q; путь «вниз» симметричен (−22.5·Q); все 10 уровней сразу (+30·Q) не допускается
        self.assertAlmostEqual(res['ret'][0][0], -22.5 * self.Q, places=12)
        paid = S.grid_sim(w, {'taker': 0.001, 'maker': 0.0002}, 1.0, False, True)['ret'][0][0]
        self.assertLess(paid, res['ret'][0][0])                                     # комиссии maker на открытиях и taker + проскальзывание на выходе уменьшают результат

    def test_maker_fee_on_limit_fills(self):
        w = self.win([100, 100, 100], [100.2, 100.4, 100.1], [98.9, 99.4, 99.5], [99.5, 100.0, 100.0])
        a = S.grid_sim(w, {'taker': 0.0, 'maker': 0.001}, 1.0, False, True)['ret'][0].sum()
        b = S.grid_sim(w, FEES0, 1.0, False, False)['ret'][0].sum()
        self.assertAlmostEqual(b - a, self.Q * (99.0 + 100.0) * 0.001, places=12)  # открытие 99 и закрытие 100 — обе по FEE_MAKER


class PowerGate(unittest.TestCase):
    def test_s_min_formula_and_exclusion_rule(self):
        self.assertAlmostEqual(S.t_bh(8), 2.4977, places=3)
        self.assertAlmostEqual(S.s_min(8, 1461), S.t_bh(8) * math.sqrt(365 / 1461), places=9)
        self.assertLess(S.s_min(8, 1461), 1.3)
        self.assertGreater(S.s_min(8, 150), S.MAX_S_MIN)                             # 150 дней покрытия — заведомо бессильная гипотеза
        self.assertEqual(S.s_min(8, 0), float('inf'))

    def test_step0_excludes_h2_with_few_listings(self):
        d, v = tempfile.mkdtemp(), tempfile.mkdtemp()
        BF.write_battery(d, n_syms=19, delist_n=0)                                    # вне H16 — всего 3 листинга
        BF.write_h16_15m(v)
        env = RB.build_env({'bdir': d, 'vdir': v, 'syms': BF.H16}, 'pre')
        s0 = RB.step0(env)
        self.assertTrue(s0['H2-D3']['excluded'] and s0['H2-D7']['excluded'])
        self.assertFalse(s0['H1-L3']['excluded'])
        self.assertLess(s0['H2-D3']['D'], 570)


def make_zip(name, text):
    b = io.BytesIO()
    with zipfile.ZipFile(b, 'w') as z:
        z.writestr(name, text)
    return b.getvalue()


class Loader(unittest.TestCase):
    def setUp(self):
        self.old = (V._sleep, os.environ.get('VISION_PAUSE_SEC'))

        async def nosleep(_s):
            return None
        V._sleep = nosleep
        os.environ['VISION_PAUSE_SEC'] = '0'

    def tearDown(self):
        V._sleep = self.old[0]
        if self.old[1] is None:
            os.environ.pop('VISION_PAUSE_SEC', None)
        else:
            os.environ['VISION_PAUSE_SEC'] = self.old[1]

    def test_parsers(self):
        d1 = B.parse_d1('open_time,open,high,low,close,volume,close_time,quote_volume,count,tbv,tbq,ignore\n1577836800000,1,2,0.5,1.5,10,0,15.5,3,1,1,0\n')
        self.assertEqual(d1['ts'][0], 1577836800000)
        self.assertAlmostEqual(float(d1['qv'][0]), 15.5)
        us = B.parse_d1('1735689600000000,1,2,1,1,1,0,9,1,1,1,0\n')
        self.assertEqual(us['ts'][0], 1735689600000)                               # микросекунды → мс
        fr = B.parse_funding('calc_time,funding_interval_hours,last_funding_rate\n1577836807012,8,0.0001\n1577865600000,8,-0.0002\n')
        self.assertEqual(list(fr['ts']), [1577836807000, 1577865600000])           # округление до секунды
        self.assertAlmostEqual(float(fr['rate'][1]), -0.0002)
        nh = B.parse_funding('1577836800000,0.0003\n')
        self.assertAlmostEqual(float(nh['rate'][0]), 0.0003)                        # без заголовка: время — первая, ставка — последняя колонка

    def fake(self, symbols, months, fail=None):
        """Фейковый архив: листинг S3 и zip-файлы; fail — множество ключей, на которые отвечаем 500."""
        calls = []
        fail = fail if fail is not None else set()

        async def fetch(url, params=None, method='GET'):
            calls.append(url)
            if url == V.S3:
                pre = params['prefix']
                if params.get('delimiter'):
                    body = '<ListBucketResult><Prefix>%s</Prefix>' % pre + ''.join(f'<CommonPrefixes><Prefix>{pre}{s}/</Prefix></CommonPrefixes>' for s in symbols) + '<IsTruncated>false</IsTruncated></ListBucketResult>'
                else:
                    kind = '1d' if '/klines/' in pre else 'fundingRate'
                    s = [x for x in symbols if f'/{x}/' in pre][0]
                    ks = [f'{pre}{s}-{kind}-{m}.zip' for m in months[s][kind]]
                    body = '<ListBucketResult>' + ''.join(f'<Key>{k}</Key>' for k in ks) + '<IsTruncated>false</IsTruncated></ListBucketResult>'
                return {'status': 200, 'headers': {}, 'body': body.encode(), 'error': ''}
            if url.endswith('.CHECKSUM'):
                return {'status': 404, 'headers': {}, 'body': b'', 'error': ''}
            key = url[len(V.ARCH) + 1:]
            if key in fail:
                return {'status': 500, 'headers': {}, 'body': b'', 'error': ''}
            name = key.split('/')[-1][:-4]
            if '-1d-' in name:
                y, m = name[-7:].split('-')
                ts0 = int(np.datetime64(f'{y}-{m}-01', 'D').astype(np.int64)) * DAY
                text = ''.join(f'{ts0 + i * DAY},1,2,0.5,1.5,10,0,{50e6},3,1,1,0\n' for i in range(3))
            else:
                y, m = name[-7:].split('-')
                ts0 = int(np.datetime64(f'{y}-{m}-01', 'D').astype(np.int64)) * DAY
                text = 'calc_time,funding_interval_hours,last_funding_rate\n' + ''.join(f'{ts0 + i * 8 * B.HOUR_MS + 5},8,0.0001\n' for i in range(3))
            return {'status': 200, 'headers': {'content-type': 'application/zip'}, 'body': make_zip(name + '.csv', text), 'error': ''}
        return fetch, calls

    def test_list_symbols_filters_delivery_contracts(self):
        fetch, _ = self.fake(['BTCUSDT', 'ETHUSDT', 'BTCUSDT_230331', 'ETHBUSD'], {})
        self.assertEqual(run(B.list_symbols(fetch)), ['BTCUSDT', 'ETHUSDT'])

    def test_sync_resumes_and_marks_gaps_and_delisted(self):
        d = tempfile.mkdtemp()
        months = {'AAAUSDT': {'1d': ['2020-01', '2020-02', '2020-03'], 'fundingRate': ['2020-01', '2020-02', '2020-03']},
                  'OLDUSDT': {'1d': ['2020-01'], 'fundingRate': ['2020-01']}}
        # ключ листинга kind: 'fr' ↔ fundingRate
        for s in months:
            months[s]['fr'] = months[s]['fundingRate']
        bad = {f'{B.KL_PREFIX}AAAUSDT/1d/AAAUSDT-1d-2020-02.zip'}
        fetch, calls = self.fake(['AAAUSDT', 'OLDUSDT'], months, bad)
        for i in range(B.GAP_ATTEMPTS):                                            # 3 запуска: месяц 2020-02 не получен → постоянный разрыв
            run(B.sync_all(fetch, d, refresh=(i == 0)))
        cov = B.coverage(d)
        self.assertEqual(cov['gaps'], 1)
        self.assertEqual(cov['not_started'], [])
        self.assertEqual(cov['n_done'], 2)                                          # символ завершён (разрыв исчерпал попытки)
        self.assertEqual(cov['delisted'], ['OLDUSDT'])                              # последний месяц OLD — на 2 месяца раньше AAA
        z = B.load_d1(d, 'AAAUSDT')
        self.assertEqual(z['months'], ['2020-01', '2020-03'])
        n = len(calls)
        run(B.sync_all(fetch, d))                                                    # повтор: завершённые символы пропускаются, запросов нет
        self.assertEqual(len(calls), n)

    def test_deadline_stops_and_resume_continues(self):
        d = tempfile.mkdtemp()
        syms = [f'S{i}USDT' for i in range(4)]
        months = {s: {'1d': ['2020-01'], 'fundingRate': ['2020-01'], 'fr': ['2020-01']} for s in syms}
        fetch, _ = self.fake(syms, months)
        r = run(B.sync_all(fetch, d, deadline=0.0))                                  # дедлайн в прошлом — ничего не загружено, остановлено
        self.assertTrue(r['stopped'])
        self.assertEqual(B.coverage(d)['n_done'], 0)
        self.assertEqual(len(B.coverage(d)['not_started']), 4)                        # «не обработано» отличается от «нет файлов»
        run(B.sync_all(fetch, d))
        self.assertEqual(B.coverage(d)['n_done'], 4)

    def test_stable_like_symbols_are_excluded_from_panel(self):
        d = tempfile.mkdtemp()
        BF.write_battery(d, n_syms=20, stable=2)
        P = B.load_panel(d, 'pre')
        self.assertEqual(len(P['excluded_stable']), 2)
        self.assertEqual(len(P['syms']), 18)

    def test_panel_before_lockbox_has_no_lockbox_days(self):
        d = tempfile.mkdtemp()
        BF.write_battery(d, n_syms=18)
        P = B.load_panel(d, 'pre')
        self.assertLess((P['day0'] + P['nd']) * DAY, L.lockbox_start_ms() + 1)
        with self.assertRaises(L.LockboxError):
            B.load_panel(d, 'full', None)


class Pipeline(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.old = (RB.BIAS_REPS, RB.CALIB_N, RB.CALIB_MAX)
        RB.BIAS_REPS, RB.CALIB_N = 2, 6
        cls.bdir, cls.vdir, cls.fdir = tempfile.mkdtemp(), tempfile.mkdtemp(), tempfile.mkdtemp()
        BF.write_battery(cls.bdir, n_syms=34, delist_n=3)
        BF.write_h16_15m(cls.vdir)
        cls.params = {'bdir': cls.bdir, 'vdir': cls.vdir, 'fdir': cls.fdir, 'syms': BF.H16, 'fee_taker': 0.0005, 'fee_maker': 0.0002, 'version': 'vTEST'}

    @classmethod
    def tearDownClass(cls):
        RB.BIAS_REPS, RB.CALIB_N, RB.CALIB_MAX = cls.old

    def reset_flags(self):
        con = RF._con(self.fdir)
        con.execute('DELETE FROM kv')
        con.commit()
        con.close()
        L._lockbox_session_state['opened'] = False

    @property
    def fdir(self):
        return self.__class__.fdir

    def test_1_refuses_with_incomplete_data(self):
        self.reset_flags()
        d = tempfile.mkdtemp()
        BF.write_battery(d, n_syms=18)
        idx = B.load_index(d)
        idx['done'][idx['symbols'][0]] = False
        B.save_index(d, idx)
        txt = run(RB.run_battery(dict(self.params, bdir=d)))
        self.assertIn('данные неполны', txt)
        self.assertIsNone(RF.kv_get(RB.KV_STATE, self.fdir))

    def test_2_battery_steps_without_lockbox_and_candidates_stored(self):
        self.reset_flags()
        txt = run(RB.run_battery(self.params))
        for needle in ('0. МОЩНОСТЬ', '1. СМЕЩЕНИЕ ПРАВИЛ', 'Данные D1'):
            self.assertIn(needle, txt)
        self.assertNotIn('ОШИБКА ВЫЖИВШЕГО', txt)                                    # в фикстуре делистингованные символы есть
        self.assertFalse(L._lockbox_session_state['opened'])                         # LOCKBOX не открывался
        st = RF.kv_get(RB.KV_STATE, self.fdir)
        self.assertIsNotNone(st)
        self.assertEqual(set(st['step0']), {S.cid(*c) for c in S.CELLS})
        self.assertIsInstance(RF.kv_get(RB.KV_CANDS, self.fdir), list)

    def test_3_calibration_stop_hides_hypothesis_results(self):
        self.reset_flags()
        old = RB.CALIB_MAX
        RB.CALIB_MAX = -1
        try:
            txt = run(RB.run_battery(self.params))
        finally:
            RB.CALIB_MAX = old
        if '2. КАЛИБРОВКА' in txt:
            self.assertIn('СТОП', txt)
            self.assertNotIn('3. TRAIN 2020–2023', txt)
        self.assertEqual(RF.kv_get(RB.KV_CANDS, self.fdir), [])

    def test_4_final_flag_before_lockbox_and_single_run(self):
        self.reset_flags()
        shown = run(RB.run_battery_final(dict(self.params, confirm=False)))
        self.assertIn('свободен', shown)
        self.assertIsNone(RF.kv_get(RB.KV_FLAG, self.fdir))                          # без confirm флаг не берётся
        refused = run(RB.run_battery_final(dict(self.params, confirm=True)))
        self.assertIn('кандидатов нет', refused)
        self.assertIsNone(RF.kv_get(RB.KV_FLAG, self.fdir))
        RF.kv_set(RB.KV_CANDS, ['H1-L3'], self.fdir)
        txt = run(RB.run_battery_final(dict(self.params, confirm=True)))
        self.assertIsNotNone(RF.kv_get(RB.KV_FLAG, self.fdir))
        self.assertTrue(RF.kv_get(RB.KV_DONE, self.fdir))
        self.assertIn('H1-L3', txt)
        self.assertIn('mean', txt)
        again = run(RB.run_battery_final(dict(self.params, confirm=True)))
        self.assertIn('флаг финала серии уже взят', again)                           # повтор запрещён
        after = run(RB.run_battery(self.params))
        self.assertIn('заморожены', after)                                           # шаги 0–3 после финала не перезаписываются
        self.assertEqual(RF.kv_get(RB.KV_CANDS, self.fdir), ['H1-L3'])

    def test_5_old_final_flag_is_untouched(self):
        self.reset_flags()
        RF.kv_set(RB.KV_CANDS, ['H1-L3'], self.fdir)
        run(RB.run_battery_final(dict(self.params, confirm=True)))
        self.assertIsNone(RF.kv_get('final_started', self.fdir))                      # прежний флаг (§2.41/§2.42) не затронут

    def test_5b_survivorship_warning_when_no_delisted(self):
        d, v = tempfile.mkdtemp(), tempfile.mkdtemp()
        BF.write_battery(d, n_syms=18, delist_n=0)
        BF.write_h16_15m(v)
        env = RB.build_env({'bdir': d, 'vdir': v, 'syms': BF.H16}, 'pre')
        cov = B.coverage(d)
        self.assertEqual(cov['delisted'], [])
        txt = '\n'.join(RB.cov_lines(env, cov, {'taker': 0.0005, 'maker': 0.0005}, 'vT'))
        self.assertIn('ОШИБКА ВЫЖИВШЕГО', txt)
        self.assertIn('ВНИМАНИЕ: FEE_MAKER = FEE_TAKER', txt)

    def test_6_evaluate_final_criteria(self):
        lb = E.day_bounds('LOCKBOX')[0]
        days = np.arange(lb, lb + 200, dtype=np.int64)
        rng = np.random.default_rng(5)
        r = rng.normal(0.003, 0.002, 200)
        base, stress = {'days': days, 'r': r}, {'days': days, 'r': r - 0.0005}
        ev = RB.evaluate_final(base, stress, {}, days)
        self.assertTrue(ev['crit']['t'] and ev['crit']['sharpe'] and ev['crit']['dd'] and ev['crit']['stress'])
        self.assertTrue(ev['pass'])
        ev2 = RB.evaluate_final(base, {'days': days, 'r': r - 0.01}, {}, days)
        self.assertFalse(ev2['pass'])                                                 # стресс ×2: mean ≤ 0 блокирует
        bad = r.copy()
        bad[10:50] = -0.01
        self.assertFalse(RB.evaluate_final({'days': days, 'r': bad}, stress, {}, days)['crit']['dd'])


class Wiring(unittest.TestCase):
    def test_commands_and_tasks_registered(self):
        import research_runner as RR
        for t in ('battery_load', 'battery', 'battery_final'):
            self.assertIn(t, RR.TASKS)
        src = open(os.path.join(_common.ROOT, 'async_bot.py'), encoding='utf-8').read()
        for c in ("cmd == '/battery_load'", "cmd == '/battery'", "cmd == '/battery_final'", 'async def battery_final_gate', 'fee_maker=FEE_MAKER'):
            self.assertIn(c, src)

    def test_cells_are_eight_and_pre_registered(self):
        self.assertEqual(S.N_PLANNED, 8)
        self.assertEqual([S.cid(*c) for c in S.CELLS], ['H1-L3', 'H1-L7', 'H2-D3', 'H2-D7', 'H3-before', 'H3-after', 'H5-m1.5', 'H5-m3'])
        self.assertEqual((S.RISK, S.H1_HOLD, S.H2_HOLD, S.H2_STOP, S.H2_RISK_CAP, S.GRID_LEVELS, S.GRID_STOP_MULT), (0.005, 3, 30, 0.5, 0.10, 5, 1.5))
        self.assertEqual((B.LIQ_MIN, B.ADV_WINDOW, B.TOP_N), (20_000_000.0, 30, 30))


if __name__ == '__main__':
    unittest.main()
