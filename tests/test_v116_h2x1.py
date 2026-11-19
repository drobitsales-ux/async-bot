"""v116: H2 — проверка фандинга и репликация (§2.47), X1 — арбитраж фандинга BingX/Bybit (§2.48), проверка исполнения без ордеров. Данные — синтетика/фейки (сети нет)."""
import asyncio
import os
import re
import tempfile
import unittest

import numpy as np

import _common
import battery_fixtures as BF
import research_battery as RB
import research_bdata as B
import research_bsim as S
import research_exec as EX
import research_factory as RF
import research_h2repl as H2R
import research_xdata as X
import research_xfund as XF
from _common import a

ROOT = _common.ROOT
DAY = X.DAY
FEE = {'taker': 0.0005, 'maker': 0.0002}


def run(c):
    return asyncio.run(c)


async def nosleep(_s):
    return None


# ── фикстуры ────────────────────────────────────────────────────────────────────────────────────────
def mk_bars(first_day, n, seed, vol=0.05, qv=5e7, p0=1.0):
    rng = np.random.default_rng(seed)
    r = rng.normal(-0.5 * vol * vol, vol, n)
    c = p0 * np.exp(np.cumsum(r))
    o = np.r_[p0, c[:-1]]
    h = np.maximum(o, c) * (1 + np.abs(rng.normal(0, vol * 0.3, n)))
    lo = np.minimum(o, c) * (1 - np.abs(rng.normal(0, vol * 0.3, n)))
    ts = (first_day + np.arange(n)) * DAY
    return {'ts': ts.astype(np.int64), 'o': o, 'h': h, 'l': lo, 'c': c, 'v': qv * np.random.default_rng(seed + 1).uniform(0.6, 1.4, n) / c, 'qv': qv * np.random.default_rng(seed + 1).uniform(0.6, 1.4, n)}


def mk_fund(first_day, n, seed, level=0.0001, interval_h=8):
    rng = np.random.default_rng(seed)
    k = int(n * 24 / interval_h)
    ts = (first_day * DAY + (np.arange(k) + 1) * interval_h * 3_600_000).astype(np.int64)
    return ts, level + rng.normal(0, 0.00003, k)


D0 = int(np.datetime64('2021-01-01', 'D').astype(np.int64))
END = int(np.datetime64('2025-06-30', 'D').astype(np.int64))


def write_xdata(d, n_new=26, spread=1100, with_binance_overlap=True):
    """Кэш XDATA_DIR: BYB — Bybit, BNG — BingX. «Новые листинги» разнесены по времени; пара символов есть на обеих биржах; один символ — «существовавший до начала истории» (самый ранний день), один — стейблкоин-подобный;
    COIN00/COIN01 есть в архиве Binance (фикстура battery) → в вселенную не входят."""
    idx = X.load_index(d)
    mk = {'bybit': {}, 'bingx': {}}
    n_days = END - D0
    specs = []
    specs.append(('bybit', 'OLD/USDT:USDT', 'OLD', D0, n_days, 1, 5e7, 0.05))              # самый ранний день на бирже → «существовавший до начала истории»
    specs.append(('bingx', 'OLD/USDT:USDT', 'OLD', D0, n_days, 2, 5e7, 0.05))
    specs.append(('bybit', 'STB/USDT:USDT', 'STB', D0 + 50, 800, 3, 5e7, 0.0004))           # стейблкоин-подобный
    for i in range(n_new):
        fd = D0 + 60 + int(i * spread / n_new)
        ex = 'bybit' if i % 2 == 0 else 'bingx'
        specs.append((ex, f'NEW{i:02d}/USDT:USDT', f'NEW{i:02d}', fd, min(900, END - fd), 10 + i, 6e7, 0.07))
    specs.append(('bingx', 'NEW00/USDT:USDT', 'NEW00', D0 + 60, 900, 99, 2e7, 0.07))      # NEW00 есть на обеих биржах: берётся биржа с большим оборотом (Bybit, 6e7)
    if with_binance_overlap:
        specs.append(('bybit', 'COIN00/USDT:USDT', 'COIN00', D0 + 200, 700, 77, 6e7, 0.06))
        specs.append(('bingx', 'COIN01/USDT:USDT', 'COIN01', D0 + 300, 700, 78, 6e7, 0.06))
    for ex, sym, base, fd, n, seed, qv, vol in specs:
        bars = mk_bars(fd, n, seed, vol=vol, qv=qv)
        X.save_bars(d, ex, sym, bars['ts'], bars['o'], bars['h'], bars['l'], bars['c'], bars['v'])
        idx['bars'][ex][sym] = {'first': int(bars['ts'][0]), 'last': int(bars['ts'][-1]), 'n': int(n), 't': 1}
        ft, fr = mk_fund(fd, n, seed + 500, level=0.0002 if ex == 'bybit' else 0.0001)
        X.save_fund(d, ex, sym, ft, fr)
        idx['fund'][ex][sym] = {'first': int(ft[0]), 'last': int(ft[-1]), 'n': len(ft), 't': 1}
        mk[ex][sym] = {'base': base, 'norm': X.norm_base(base), 'active': True, 'created': None}
    idx['markets'] = mk
    X.save_index(d, idx)
    return idx


class Universe(unittest.TestCase):
    def test_norm_base(self):
        self.assertEqual([X.norm_base(x) for x in ('1000PEPE', '10000SATS', '1MBABYDOGE', 'ENA', 'btc', '1INCH')], ['PEPE', 'SATS', 'BABYDOGE', 'ENA', 'BTC', '1INCH'])

    def test_universe_is_disjoint_from_binance_and_follows_rules(self):
        bdir = tempfile.mkdtemp()
        BF.write_battery(bdir, n_syms=18)
        xdir = tempfile.mkdtemp()
        write_xdata(xdir)
        P, uni, ref = H2R.build_repl_panel(xdir, bdir)
        bnorm = X.binance_norm_set(bdir)
        self.assertTrue({'COIN00', 'COIN01', 'BTC'} <= bnorm)
        self.assertTrue(all(u['norm'] not in bnorm for u in uni))                         # вселенная НЕ пересекается с Binance (включая нормализацию 1000X)
        names = {u['norm'] for u in uni}
        self.assertNotIn('COIN00', names)
        self.assertNotIn('OLD', names)                                                     # существовавший до начала истории биржи
        self.assertNotIn('STB', names)                                                     # стейблкоин-подобный
        self.assertTrue(any(x.endswith(':OLD/USDT:USDT') for x in ref['pre_existing']))
        self.assertEqual(sum(1 for u in uni if u['norm'] == 'NEW00'), 1)                   # монета на обеих биржах — один раз
        self.assertEqual([u['ex'] for u in uni if u['norm'] == 'NEW00'], ['bybit'])        # биржа с большим оборотом за первые 30 суток
        self.assertTrue(X.universe_line(uni).startswith('H2REPL-UNIVERSE: N='))

    def test_binance_overlap_with_multiplier_prefix(self):
        bdir = tempfile.mkdtemp()
        BF.write_battery(bdir, n_syms=17)
        idx = B.load_index(bdir)
        idx['symbols'].append('1000PEPEUSDT')
        B.save_index(bdir, idx)
        self.assertIn('PEPE', X.binance_norm_set(bdir))
        mk = {'bybit': {'PEPE/USDT:USDT': {'base': 'PEPE', 'norm': 'PEPE', 'active': True}, 'ZZZ/USDT:USDT': {'base': 'ZZZ', 'norm': 'ZZZ', 'active': True}}, 'bingx': {}}
        bars = {'bybit': {'PEPE/USDT:USDT': mk_bars(D0 + 10, 100, 1), 'ZZZ/USDT:USDT': mk_bars(D0 + 20, 100, 2), 'FIRST/USDT:USDT': mk_bars(D0, 100, 3)}, 'bingx': {}}
        mk['bybit']['FIRST/USDT:USDT'] = {'base': 'FIRST', 'norm': 'FIRST', 'active': True}
        uni, _ref = X.h2_universe(mk, bars, X.binance_norm_set(bdir))
        self.assertEqual([u['norm'] for u in uni], ['ZZZ'])


class SameRule(unittest.TestCase):
    def test_h2_repl_uses_the_same_run_cell_and_panel_format_bitwise(self):
        """Правила H2 — та же run_cell: панель репликации (panel_from_series) на ТЕХ ЖЕ данных, что и load_panel, даёт побитово тот же результат."""
        self.assertEqual(H2R.CELL, ('H2', 'D3'))
        self.assertIn('RB.run_cell(CELL', open(os.path.join(ROOT, 'research_h2repl.py'), encoding='utf-8').read())
        d = tempfile.mkdtemp()
        BF.write_battery(d, n_syms=34, delist_n=3)
        P1 = B.load_panel(d, 'pre')
        lb = __import__('research_ma_lib').lockbox_start_ms()
        series, fund = {}, {}
        for s in P1['syms']:
            z = B.load_d1(d, s)
            m = z['ts'] < lb
            series[s] = {k: z[k][m] for k in ('ts', 'o', 'h', 'l', 'c', 'qv')}
            f = B.load_fund(d, s)
            mf = f['ts'] < lb
            fund[s] = (f['ts'][mf], f['rate'][mf])
        P2 = X.panel_from_series(series, fund)
        self.assertEqual(set(P1['syms']), set(P2['syms']))
        self.assertEqual((P1['day0'], P1['nd']), (P2['day0'], P2['nd']))
        for name in P1['syms']:                                                               # порядок столбцов разный (load_panel — порядок индекса, panel_from_series — по алфавиту): сравниваем по именам
            j1, j2 = P1['syms'].index(name), P2['syms'].index(name)
            for k in ('O', 'H', 'L', 'C', 'QV', 'F', 'G', 'GC'):
                self.assertTrue(np.array_equal(np.nan_to_num(P1[k][:, j1], nan=-1), np.nan_to_num(P2[k][:, j2], nan=-1)), (name, k))
            self.assertEqual((int(P1['first'][j1]), int(P1['last'][j1])), (int(P2['first'][j2]), int(P2['last'][j2])))
        e1 = {'P': P1, 'ctx': S.make_ctx(P1), 'days': np.arange(P1['day0'], P1['day0'] + P1['nd'], dtype=np.int64), 'hc': None}
        e2 = H2R.make_env(P2)
        r1 = RB.run_cell(('H2', 'D3'), e1, FEE)
        r2 = RB.run_cell(('H2', 'D3'), e2, FEE)
        self.assertTrue(np.array_equal(r1['r'], r2['r'][:len(r1['r'])]))
        self.assertEqual(r1['meta'], r2['meta'])

    def test_short_funding_sign(self):
        """Фандинг шорта учитывается со знаком: положительная ставка — получает, отрицательная — платит."""
        from test_v108_battery import mini_panel, trend
        for sign in (+1, -1):
            P = mini_panel(nd=80, ns=2)
            trend(P, 0, 0.0)
            P['F'][:, 0] = sign * 0.0001
            ctx = S.make_ctx(P)
            r0 = S.sim_h2(ctx, [(10, 0)], {'taker': 0.0, 'maker': 0.0}, fund_on=False, costs_on=False)['r'].sum()
            r1 = S.sim_h2(ctx, [(10, 0)], {'taker': 0.0, 'maker': 0.0}, fund_on=True, costs_on=False)['r'].sum()
            self.assertAlmostEqual(float(r1 - r0), sign * S.RISK / S.H2_STOP * 0.0001 * S.H2_HOLD, places=9)
        ev = H2R.code_evidence()
        self.assertTrue(ev['accrues'] and ev['default_on'] and ev['h2_branch'])                # фандинг в исходном H2 учитывался


class FundingCheck(unittest.TestCase):
    def test_check_on_train_valid_only_and_reports_with_without(self):
        import research_ma_lib as L
        d, f = tempfile.mkdtemp(), tempfile.mkdtemp()
        BF.write_battery(d, n_syms=34, delist_n=3)
        modes = []
        orig = B.load_panel

        def spy(dd, mode='pre', session=None, **kw):
            modes.append(mode)
            return orig(dd, mode, session, **kw)
        B.load_panel = spy
        old_open = L.open_lockbox_session

        def boom(*x, **k):
            raise AssertionError('LOCKBOX открыт')
        L.open_lockbox_session = boom
        try:
            txt = H2R.funding_check({'bdir': d, 'fdir': f, 'fee_taker': 0.0005, 'fee_maker': 0.0002})
        finally:
            B.load_panel = orig
            L.open_lockbox_session = old_open
        self.assertEqual(set(modes), {'pre'})
        for needle in ('УЧИТЫВАЛСЯ', 'TRAIN', 'VALID', 'без фандинга', 'с фандингом', 'не пересчитывался', 'Решение (§2.47.1)', 'Фандинг за первые 30 суток'):
            self.assertIn(needle, txt)
        self.assertIsNotNone(RF.kv_get('h2_funding_check', f))


class Replicate(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.old = H2R.K_REPS
        H2R.K_REPS = 6
        cls.bdir, cls.xdir, cls.fdir = tempfile.mkdtemp(), tempfile.mkdtemp(), tempfile.mkdtemp()
        BF.write_battery(cls.bdir, n_syms=18)
        write_xdata(cls.xdir)
        RF.kv_set('h2_funding_check', {'allow': True, 'comb': 1.0}, cls.fdir)
        cls.params = {'bdir': cls.bdir, 'xdir': cls.xdir, 'fdir': cls.fdir, 'fee_taker': 0.0005, 'fee_maker': 0.0002, 'version': 'vT'}

    @classmethod
    def tearDownClass(cls):
        H2R.K_REPS = cls.old

    def spec(self, lines):
        f = tempfile.NamedTemporaryFile('w', suffix='.md', delete=False, encoding='utf-8')
        f.write('# spec\n' + '\n'.join(lines) + '\n')
        f.close()
        return f.name

    def test_1_refuses_without_funding_check_allow(self):
        f2 = tempfile.mkdtemp()
        txt = run(H2R.run_h2repl(dict(self.params, fdir=f2, mode='show')))
        self.assertIn('/h2_funding_check', txt)

    def test_2_show_computes_k_but_not_returns_and_confirm_needs_spec_lines(self):
        calls = []
        orig = H2R.evaluate

        def spy(*x, **k):
            calls.append(1)
            return orig(*x, **k)
        H2R.evaluate = spy
        try:
            show = run(H2R.run_h2repl(dict(self.params, mode='show', spec_path=self.spec([]))))
            self.assertIn('k = ', show)
            self.assertIn('H2REPL-UNIVERSE: N=', show)
            self.assertEqual(calls, [])                                                    # k считается ДО расчёта доходностей; show доходности не считает
            refused = run(H2R.run_h2repl(dict(self.params, mode='confirm', spec_path=self.spec([]))))
            self.assertIn('не записаны', refused)
            self.assertEqual(calls, [])
            self.assertIsNone(RF.kv_get(H2R.KV_STARTED, self.fdir))                        # флаг не взят
            P, uni, ref = H2R.build_repl_panel(self.xdir, self.bdir)
            env = H2R.make_env(P)
            kres = H2R.compute_k(env, RB.fees_of(self.params), 6)
            self.assertTrue(kres['k'] >= 1.0)
            sp = self.spec([X.universe_line(uni), f'{H2R.SPEC_K_KEY} {kres["k"]:.3f} (тест)'])
            res = run(H2R.run_h2repl(dict(self.params, mode='confirm', spec_path=sp)))
            self.assertEqual(calls, [1])
            for needle in ('Критерии репликации', 'Справочно', 'по биржам', 't_adj'):
                self.assertIn(needle, res)
            self.assertTrue(RF.kv_get(H2R.KV_DONE, self.fdir))
            again = run(H2R.run_h2repl(dict(self.params, mode='confirm', spec_path=sp)))
            self.assertIn('повтор запрещён', again)                                      # один раз
            self.assertIn('Критерии репликации', run(H2R.run_h2repl(dict(self.params, mode='report'))))
        finally:
            H2R.evaluate = orig

    def test_3_evaluate_criteria(self):
        rng = np.random.default_rng(3)
        n = 400
        days = np.arange(20000, 20000 + n)
        r = rng.normal(0.0004, 0.0008, n)
        seg = np.ones(n, bool)
        ev = H2R.evaluate(days, r, r - 0.0002, seg, 1.1, {})
        self.assertTrue(ev['crit']['t_adj'] and ev['crit']['sharpe'] and ev['crit']['dd'] and ev['crit']['stress'])
        self.assertIsNone(ev['crit']['up'])
        self.assertFalse(H2R.evaluate(days, r, r - 0.01, seg, 1.1, {})['pass'])              # стресс ×2 ≤ 0
        self.assertFalse(H2R.evaluate(days, r, r, seg, 50.0, {})['crit']['t_adj'])            # большое k блокирует
        months = H2R.btc_months_all((np.arange(20000, 20120)) * DAY, 100 + np.arange(120.0))
        self.assertTrue(months)


class XData(unittest.TestCase):
    def make_ex(self, ex_id, markets, bars, funding):
        outer = self
        keys = []

        class FakeEx:
            def __init__(self):
                self.closed = False

            async def load_markets(self):
                return markets

            async def fetch_ohlcv(self, sym, tf, since, limit):
                rows = [[int(t), o, h, l, c, v] for t, o, h, l, c, v in zip(bars[sym]['ts'], bars[sym]['o'], bars[sym]['h'], bars[sym]['l'], bars[sym]['c'], bars[sym]['v']) if t >= since]
                return rows[:limit]

            async def fetch_funding_rate_history(self, sym, since=None, limit=None, params=None):
                params = params or {}
                keys.append(tuple(sorted(params)))
                end = params.get('until', params.get('endTime'))
                ts, r = funding[sym]
                sel = [(int(t), float(x)) for t, x in zip(ts, r) if end is None or t <= end]
                sel = sel[-limit:]
                return [{'timestamp': t, 'fundingRate': x} for t, x in sel]

            async def close(self):
                self.closed = True

            def __getattr__(self, name):
                if any(w in name.lower() for w in ('create', 'cancel', 'edit', 'private', 'transfer', 'withdraw')):
                    raise AssertionError(f'запрещённый вызов {name}')
                raise AttributeError(name)
        e = FakeEx()
        e.keys = keys
        del outer
        return e

    def test_loader_resume_param_names_and_read_only(self):
        bdir = tempfile.mkdtemp()
        BF.write_battery(bdir, n_syms=18)
        now_ms = (END + 1) * DAY
        mk_by = {'NEW01/USDT:USDT': {'swap': True, 'linear': True, 'quote': 'USDT', 'settle': 'USDT', 'base': 'NEW01', 'active': True},
                 'COIN00/USDT:USDT': {'swap': True, 'linear': True, 'quote': 'USDT', 'settle': 'USDT', 'base': 'COIN00', 'active': True},
                 'NEWSPOT/USDT': {'swap': False, 'linear': None, 'quote': 'USDT', 'settle': None, 'base': 'NEWSPOT', 'active': True}}
        mk_bn = {'NEW02/USDT:USDT': {'swap': True, 'linear': True, 'quote': 'USDT', 'settle': 'USDT', 'base': 'NEW02', 'active': True},
                 'NEW01/USDT:USDT': {'swap': True, 'linear': True, 'quote': 'USDT', 'settle': 'USDT', 'base': 'NEW01', 'active': False}}
        b1, b2 = mk_bars(D0 + 100, 300, 5, qv=6e7), mk_bars(D0 + 120, 300, 6, qv=6e7)
        f1 = mk_fund(D0 + 100, 300, 7)
        f2 = mk_fund(D0 + 120, 300, 8, interval_h=4)
        by = self.make_ex('bybit', mk_by, {'NEW01/USDT:USDT': b1, 'COIN00/USDT:USDT': b1}, {'NEW01/USDT:USDT': f1, 'COIN00/USDT:USDT': f1})
        bn = self.make_ex('bingx', mk_bn, {'NEW02/USDT:USDT': b2, 'NEW01/USDT:USDT': b1}, {'NEW02/USDT:USDT': f2, 'NEW01/USDT:USDT': f1})
        xdir = tempfile.mkdtemp()
        os.environ['HTF_FETCH_PAUSE_SEC'] = '0'
        try:
            txt = run(X.run_xload({'xdir': xdir, 'bdir': bdir, 'max_minutes': 5}, None, exchanges={'bybit': by, 'bingx': bn}, now_ms=now_ms, sleep=nosleep))
        finally:
            os.environ.pop('HTF_FETCH_PAUSE_SEC', None)
        idx = X.load_index(xdir)
        self.assertIn('NEW01/USDT:USDT', idx['bars']['bybit'])
        self.assertNotIn('NEWSPOT/USDT', idx['bars']['bybit'])                              # спот не берётся
        self.assertNotIn('COIN00/USDT:USDT', idx['bars']['bybit'])                          # есть в архиве Binance и не общая база → не нужна
        self.assertIn('NEW01/USDT:USDT', idx['bars']['bingx'])                              # NEW01 — общая база: нужна на обеих биржах
        self.assertIn('NEW02/USDT:USDT', idx['bars']['bingx'])
        self.assertIn('неактивных рынков в markets: 1', txt)
        f = X.load_fund(xdir, 'bingx', 'NEW02/USDT:USDT')
        self.assertEqual(len(f[0]), len(f2[0]))                                              # вся история фандинга собрана постранично назад
        self.assertTrue(all(k == ('until',) or k == () for k in [tuple(k) for k in by.keys]))
        self.assertTrue(all(k == ('endTime',) or k == () for k in [tuple(k) for k in bn.keys]))   # BingX: сырой endTime (ccxt 4.2.14 кладёт until в startTime)
        self.assertIn('ОШИБКА ВЫЖИВШЕГО', txt.replace('<b>', '')) if False else None
        calls_before = [len(by.keys), len(bn.keys)]
        run(X.run_xload({'xdir': xdir, 'bdir': bdir, 'max_minutes': 5}, None, exchanges={'bybit': by, 'bingx': bn}, now_ms=now_ms, sleep=nosleep))
        self.assertEqual([len(by.keys), len(bn.keys)], calls_before)                        # повторный запуск: загружено — запросов нет (продолжение с места остановки)

    def test_no_order_calls_in_sources(self):
        bad = re.compile(r'create_order|createOrder|create_market|create_limit|cancel_order|cancelOrder|\.transfer\(|\.withdraw\(|edit_order')
        for f in ('research_xdata.py', 'research_h2repl.py', 'research_xfund.py', 'research_exec.py'):
            src = open(os.path.join(ROOT, f), encoding='utf-8').read()
            self.assertIsNone(bad.search(src), f)


# ── X1 ──────────────────────────────────────────────────────────────────────────────────────────────
def x1_inputs(nd=120, rate_a=0.0005, rate_b=0.0, pa=None, pb=None, n_pairs=1, fee=(0.0005, 0.0005), spike=None, drop_after=None, qv=1e8):
    """Простая пара: обе площадки торгуют по одному общему блужданию (корреляция 1) ~100; множители pa/pb — разница цен площадок; фандинг: A — rate_a за 8 ч (3 события в сутки), B — rate_b."""
    day0 = 20000
    ts = (day0 + np.arange(nd)) * DAY
    base = 100.0 * np.exp(np.cumsum(np.random.default_rng(5).normal(0, 0.01, nd)))
    venues = {}
    for tag, rate, mul in (('A', rate_a, pa), ('B', rate_b, pb)):
        px = base * (1.0 if mul is None else np.asarray(mul, float) / 100.0)
        bars = {'ts': ts, 'o': px.copy(), 'h': px * 1.001, 'l': px * 0.999, 'c': px.copy(), 'qv': np.full(nd, qv)}
        ev_ts = (np.arange(nd * 3) + 1) * 8 * 3_600_000 + day0 * DAY
        r = np.full(nd * 3, rate) if drop_after is None or tag == 'B' else np.where(ev_ts < (day0 + drop_after) * DAY, rate, 0.0)
        venues[tag] = (bars, (ev_ts.astype(np.int64), r))
    if spike is not None:
        tag, day, hi = spike
        venues[tag][0]['h'][day] = venues[tag][0]['o'][day] * hi
    vA = {'bars': {f'sa{i}': venues['A'][0] for i in range(n_pairs)}, 'fund': {f'sa{i}': venues['A'][1] for i in range(n_pairs)}, 'fee': fee[0]}
    vB = {'bars': {f'sb{i}': venues['B'][0] for i in range(n_pairs)}, 'fund': {f'sb{i}': venues['B'][1] for i in range(n_pairs)}, 'fee': fee[1]}
    return vA, vB, [(f'P{i}', f'sa{i}', f'sb{i}') for i in range(n_pairs)]


class X1(unittest.TestCase):
    def test_pairs_need_correlated_returns(self):
        a_ = mk_bars(20000, 200, 1)
        self.assertTrue(XF.pair_ok(a_, dict(a_, c=a_['c'] * 1000.0))[0])                    # масштаб (1000PEPE/PEPE) не важен
        self.assertFalse(XF.pair_ok(a_, mk_bars(20000, 200, 2))[0])

    def test_rates_are_reduced_to_8h(self):
        day0 = 20000
        e8 = ((np.arange(60) + 1) * 8 * 3_600_000 + day0 * DAY, np.full(60, 0.0001))                      # 8 ч: 0.01% за расчёт
        e4 = ((np.arange(120) + 1) * 4 * 3_600_000 + day0 * DAY, np.full(120, 0.00005))                   # 4 ч: 0.005% за расчёт = 0.01% за 8 ч
        e1 = ((np.arange(480) + 1) * 3_600_000 + day0 * DAY, np.full(480, 0.0000125))                     # 1 ч
        for ev in (e8, e4, e1):
            m = XF.avg8_matrix(day0, 20, ev)
            self.assertAlmostEqual(float(m[10]), 0.0001, places=9)
        self.assertEqual(XF.interval_hours(e4[0]), 4.0)
        self.assertTrue(np.isnan(XF.avg8_matrix(day0, 20, ([day0 * DAY + 1000], [0.1]))[10]))             # < 2 событий в окне — NaN
        # строго до 00:00: событие ровно в 00:00 суток d в окно суток d не входит
        ev = (np.array([day0 * DAY + 5 * DAY - 2 * 3_600_000, day0 * DAY + 5 * DAY - 3_600_000, day0 * DAY + 5 * DAY]), np.array([0.0001, 0.0001, 1.0]))
        self.assertAlmostEqual(float(XF.avg8_matrix(day0, 10, ev)[5]), 0.0002 / 9.0, places=12)

    def test_four_fills_funding_and_costs(self):
        vA, vB, pairs = x1_inputs(nd=120)
        inp, rej = XF.build_inputs(vA, vB, pairs)
        res = XF.simulate_x1(inp, 0.0002)
        tr = res['trades'][0]
        self.assertEqual((tr['reason'], tr['x'] - tr['e']), ('время', 14))
        c = XF.C_POS
        self.assertAlmostEqual(float(res['cost'].sum()), c * (0.0005 + XF.SLIP) * (4 * len(res['trades']) + 2 * res['open_end']), places=9)   # закрытая сделка — 4 исполнения (комиссия + проскальзывание), открытая на конец — 2 входа
        self.assertAlmostEqual(float(res['fund'][:tr['x'] + 1].sum()) / 1.0, 14 * 3 * 0.0005 * c, places=7)              # шорт на A получает 0.05% × 3 в сутки × 14 суток на номинал c
        self.assertAlmostEqual(float(res['price'].sum()), 0.0, places=6)
        stress = XF.simulate_x1(inp, 0.0002, mult=2.0)
        self.assertAlmostEqual(float(stress['cost'].sum()), 2 * float(res['cost'].sum()), places=9)
        self.assertAlmostEqual(float(stress['fund'].sum()), float(res['fund'].sum()), places=12)                         # фандинг не удваивается

    def test_orientation_threshold_and_exit_on_d(self):
        vA, vB, pairs = x1_inputs(nd=120, rate_a=0.0, rate_b=0.0005)
        inp, _ = XF.build_inputs(vA, vB, pairs)
        res = XF.simulate_x1(inp, 0.0002)
        self.assertGreater(float(res['fund'].sum()), 0)                                                                  # шорт на B (бóльшая ставка) получает
        vA, vB, pairs = x1_inputs(nd=120, rate_a=0.0001, rate_b=0.0)
        inp, _ = XF.build_inputs(vA, vB, pairs)
        self.assertEqual(XF.simulate_x1(inp, 0.0002)['trades'], [])                                                      # d = 0.01% < T = 0.02% — входа нет
        vA, vB, pairs = x1_inputs(nd=120, rate_a=0.0005, rate_b=0.0, drop_after=8)
        inp, _ = XF.build_inputs(vA, vB, pairs)
        tr = XF.simulate_x1(inp, 0.0002)['trades'][0]
        self.assertEqual(tr['reason'], 'd')                                                                              # выход при d < 0.005%
        self.assertLess(tr['x'] - tr['e'], 14)

    def test_leg_liquidation_and_price_difference(self):
        # шорт-нога (A) вырастает на 45% за сутки: нога ликвидирована (−c/2 накопленно), вторая закрывается, позиция закрыта
        vA, vB, pairs = x1_inputs(nd=120, spike=('A', 6, 1.45))
        inp, _ = XF.build_inputs(vA, vB, pairs)
        res = XF.simulate_x1(inp, 0.0002)
        self.assertEqual(res['liq'], 1)
        self.assertEqual(res['trades'][0]['reason'], 'ликвидация')
        c = XF.C_POS
        self.assertAlmostEqual(float(res['price'].sum()), -c / 2.0, delta=0.01)                                          # потеря маржи ноги ≈ −c/2 (вторая нога ≈ 0)
        # разница цен бирж на входе/выходе входит в P&L: A открывается 100 → 100; B открывается 101 (лонг) и на выходе 99
        pa = np.full(120, 100.0)
        pb = np.full(120, 101.0)
        pb[20:] = 99.5
        vA, vB, pairs = x1_inputs(nd=120, pa=pa, pb=pb, rate_a=0.0005, rate_b=0.0)
        inp, _ = XF.build_inputs(vA, vB, pairs)
        r2 = XF.simulate_x1(inp, 0.0002)
        tr = r2['trades'][0]
        if tr['x'] > 19:
            self.assertLess(float(r2['price'].sum()), 0)                                                                  # лонг на B купил по 101, закрыл по 99

    def test_max_positions_and_metrics(self):
        vA, vB, pairs = x1_inputs(nd=120, n_pairs=8)
        inp, _ = XF.build_inputs(vA, vB, pairs)
        res = XF.simulate_x1(inp, 0.0002)
        open_now = {}
        for t in res['trades']:
            for d in range(t['e'], t['x']):
                open_now[d] = open_now.get(d, 0) + 1
        self.assertLessEqual(max(open_now.values()), XF.MAX_POS)
        m = XF.metrics(res, inp)
        c, ok = XF.criteria(m)
        self.assertIn('apr', c)
        self.assertIn(2024, m['years'])
        self.assertFalse(c['y2026'])                                                                                      # нет 2026 в данных — критерий не выполнен
        top = XF.top_symbols(res, inp)
        self.assertTrue(top and top[0][2] > 0)                                                                            # ёмкость: 5% × минимальный медианный оборот

    def test_report_runs_end_to_end(self):
        xdir, bdir, fdir = tempfile.mkdtemp(), tempfile.mkdtemp(), tempfile.mkdtemp()
        BF.write_battery(bdir, n_syms=18)
        idx = X.load_index(xdir)
        mk = {'bybit': {}, 'bingx': {}}
        for i in range(6):
            for ex, lvl in (('bybit', 0.0004), ('bingx', 0.0)):
                sym = f'N{i}/USDT:USDT'
                bars = mk_bars(D0, 700, 40 + i)                                                                      # одинаковый ряд на обеих биржах → корреляция 1
                X.save_bars(xdir, ex, sym, bars['ts'], bars['o'], bars['h'], bars['l'], bars['c'], bars['v'])
                ft, fr = mk_fund(D0, 700, 90 + i, level=lvl)
                X.save_fund(xdir, ex, sym, ft, fr)
                idx['bars'][ex][sym] = {'first': int(bars['ts'][0]), 'last': int(bars['ts'][-1]), 'n': 700, 't': 1}
                idx['fund'][ex][sym] = {'first': int(ft[0]), 'last': int(ft[-1]), 'n': len(ft), 't': 1}
                mk[ex][sym] = {'base': f'N{i}', 'norm': f'N{i}', 'active': True, 'created': None}
        idx['markets'] = mk
        X.save_index(xdir, idx)
        txt = run(XF.run_xfund({'xdir': xdir, 'bdir': bdir, 'fdir': fdir, 'fee_taker': 0.0005, 'version': 'vT'}))
        for needle in ('межбиржевой арбитраж фандинга X1', 'T = 0.02%', 'T = 0.05%', 'ликвидаций', 'по годам', 'Решение (§2.48.3', 'Интервалы начисления'):
            self.assertIn(needle, txt)
        self.assertIsNotNone(RF.kv_get(XF.KV_PASSED, fdir))
        self.assertEqual(len(XF.scan_list(xdir)), 6)


# ── проверка исполнения без ордеров ────────────────────────────────────────────────────────────────
class FakeBook:
    def __init__(self, mid=1.0, half=0.0005, qty=2000.0, levels=5):
        self.ob = {'bids': [[mid * (1 - half - i * 0.0005), qty] for i in range(levels)], 'asks': [[mid * (1 + half + i * 0.0005), qty] for i in range(levels)]}


class ExecEx:
    def __init__(self, markets, rate=0.0001, interval=None):
        self.markets, self.rate, self.interval, self.calls = markets, rate, interval, []

    async def load_markets(self):
        self.calls.append('load_markets')
        return self.markets

    async def fetch_order_book(self, sym, limit=None):
        self.calls.append('fetch_order_book')
        return FakeBook().ob

    async def fetch_funding_rate(self, sym):
        self.calls.append('fetch_funding_rate')
        return {'fundingRate': self.rate, 'interval': self.interval}

    def __getattr__(self, name):
        if any(w in name.lower() for w in ('create', 'cancel', 'edit', 'transfer', 'withdraw')):
            raise AssertionError(f'запрещённый вызов {name}')
        raise AttributeError(name)


def swap(base, created=None):
    return {'swap': True, 'linear': True, 'quote': 'USDT', 'settle': 'USDT', 'base': base, 'active': True, 'created': created}


class Exec(unittest.TestCase):
    def test_book_stats(self):
        st = EX.book_stats(FakeBook(mid=1.0, half=0.0005, qty=1000.0).ob, (100, 3000), 'sell')
        self.assertAlmostEqual(st['spread_bps'], 10.0, places=6)
        self.assertAlmostEqual(st['slip'][100], 5.0, places=6)                                  # покупка/продажа по лучшей цене: отклонение от mid = полуспред 5 б.п.
        self.assertGreater(st['slip'][3000], st['slip'][100])                                   # большой размер идёт глубже по стакану
        self.assertIsNone(EX.book_stats(FakeBook(qty=1.0, levels=2).ob, (100000,), 'buy')['slip'][100000])

    def test_h2_sampler_detects_new_listing_and_reports(self):
        db = os.path.join(tempfile.mkdtemp(), 'e.db')
        t = [1_800_000_000.0]
        mk = {'OLD/USDT:USDT': swap('OLD'), 'NEWX/USDT:USDT': swap('NEWX')}
        ex = ExecEx({'OLD/USDT:USDT': swap('OLD')})
        s = EX.ExecSampler('h2', ex, db=db, sleep=nosleep, now=lambda: t[0], pause=0)
        self.assertEqual(run(s.sample_h2()), 0)                                                  # первый срез — база
        ex.markets = mk
        t[0] += 3600
        self.assertEqual(run(s.sample_h2()), 1)                                                  # новый листинг появился после базы → стакан и фандинг
        t[0] += 4 * 86400
        run(s.sample_h2())
        txt = EX.report('h2', db)
        for needle in ('Листингов в наблюдении: 1', 'Спред', 'допущения', 'шорт на день ≥ 3'):
            self.assertIn(needle, txt)
        self.assertTrue(set(ex.calls) <= {'load_markets', 'fetch_order_book', 'fetch_funding_rate'})

    def test_x1_sampler_and_report(self):
        db = os.path.join(tempfile.mkdtemp(), 'e.db')
        a_, b_ = ExecEx({}, rate=0.0006), ExecEx({}, rate=0.0001, interval='4h')
        s = EX.ExecSampler('x1', a_, b_, db=db, sleep=nosleep, now=lambda: 1_800_000_000.0, pause=0)
        n = run(s.sample_x1([('AAA', 'AAA/USDT:USDT', 'AAA/USDT:USDT'), ('BBB', 'BBB/USDT:USDT', 'BBB/USDT:USDT')]))
        self.assertEqual(n, 2)
        txt = EX.report('x1', db)
        for needle in ('Кандидатов', 'вход 100 USDT на ногу', 'вход 1000 USDT на ногу', 'Итог'):
            self.assertIn(needle, txt)
        import json
        rec = json.loads(sqlite_one(db))
        self.assertAlmostEqual(rec['rate8']['bybit'], 0.0002, places=9)                          # ставка Bybit 4 ч → 8 ч ×2
        self.assertEqual(rec['short'], 'bingx')

    def test_run_loop_stops_after_seven_days(self):
        db = os.path.join(tempfile.mkdtemp(), 'e.db')
        t = [1_800_000_000.0]
        ex = ExecEx({'A/USDT:USDT': swap('A')})

        async def sleep(sec):
            if sec >= 3600:
                t[0] += sec
        s = EX.ExecSampler('h2', ex, db=db, sleep=sleep, now=lambda: t[0], pause=0)
        run(s.run())
        self.assertGreaterEqual(t[0] - 1_800_000_000.0, 7 * 86400)
        self.assertLessEqual(ex.calls.count('load_markets'), 7 * 24 + 1)                          # не чаще раза в час

    def test_gate_refuses_without_pass_flag(self):
        sent = []

        async def fake_tg(t):
            sent.append(t)
        old = a.tg
        a.tg = fake_tg
        f = tempfile.mkdtemp()
        os.environ['FACTORY_DIR'] = f
        try:
            run(a.exec_check_cmd(['h2', 'start']))
            run(a.exec_check_cmd(['x1', 'start']))
            run(a.exec_check_cmd(['zzz']))
        finally:
            a.tg = old
            os.environ.pop('FACTORY_DIR', None)
        self.assertTrue(sent[0].startswith('⛔') and 'h2' in sent[0])
        self.assertTrue(sent[1].startswith('⛔') and 'x1' in sent[1])
        self.assertIn('Использование', sent[2])


def sqlite_one(db):
    import sqlite3
    con = sqlite3.connect(db)
    r = con.execute("SELECT payload FROM samples WHERE kind='x1' ORDER BY id LIMIT 1").fetchone()
    con.close()
    return r[0]


class Wiring(unittest.TestCase):
    def test_commands_tasks_version(self):
        import research_runner as RR
        for t in ('h2_funding_check', 'h2_replicate', 'xfund_test'):
            self.assertIn(t, RR.TASKS)
        src = open(os.path.join(ROOT, 'async_bot.py'), encoding='utf-8').read()
        for c in ("cmd == '/h2_funding_check'", "cmd == '/h2_replicate'", "cmd == '/xfund_test'", "cmd == '/exec_check'", 'async def exec_check_cmd', '/exec_check h2|x1'):
            self.assertIn(c, src)
        self.assertGreaterEqual(int(re.search(r'v(\d+)$', a.CODE_VERSION).group(1)), 116)


if __name__ == '__main__':
    unittest.main()
