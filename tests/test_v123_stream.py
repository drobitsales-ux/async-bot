"""v123 (BOT_SPEC §2.55): потоковая загрузка и расчёты — пики памяти под лимитом на синтетике прод-размера; результаты X1 и H2 побитово совпадают с эталоном v122."""
import tempfile
import unittest

import numpy as np

import _common
import memprobe
import ref_xfund_v122 as REF
import research_h2repl as H2R
import research_xdata as X
import research_xfund as XF
import synth_prod as SP

ROOT = _common.ROOT                                                    # импорт _common добавляет корень репозитория в sys.path (модули research_*)
LIMIT_LOAD_MB = 120.0
LIMIT_CALC_MB = 200.0


def same(a, b, path='x'):
    """Побитовое равенство вложенных структур (массивы — np.array_equal с NaN; числа — точное равенство, NaN == NaN)."""
    if isinstance(a, np.ndarray) or isinstance(b, np.ndarray):
        a_, b_ = np.asarray(a), np.asarray(b)
        assert a_.dtype == b_.dtype and a_.shape == b_.shape, f'{path}: dtype/shape {a_.dtype}{a_.shape} != {b_.dtype}{b_.shape}'
        assert np.array_equal(a_, b_, equal_nan=a_.dtype.kind == 'f'), f'{path}: массивы различаются'
    elif isinstance(a, dict):
        assert isinstance(b, dict) and set(a) == set(b), f'{path}: ключи {sorted(map(str, a))[:5]} != {sorted(map(str, b))[:5]}'
        for k in a:
            same(a[k], b[k], f'{path}.{k}')
    elif isinstance(a, (list, tuple)):
        assert len(a) == len(b), f'{path}: длина {len(a)} != {len(b)}'
        for i, (x, y) in enumerate(zip(a, b)):
            same(x, y, f'{path}[{i}]')
    elif isinstance(a, float):
        assert (a == b) or (a != a and b != b), f'{path}: {a!r} != {b!r}'
    else:
        assert a == b, f'{path}: {a!r} != {b!r}'


class Bitwise(unittest.TestCase):
    """Эталонная малая панель: 10 пар, ~2 года; X1 — build_inputs + simulate_x1 + metrics + top_symbols; H2 — build_repl_panel. Эталон — модуль v122 (tests/ref_xfund_v122.py)."""

    @classmethod
    def setUpClass(cls):
        cls.xdir, cls.bdir = tempfile.mkdtemp(), tempfile.mkdtemp()
        cls.info = SP.build(cls.xdir, cls.bdir, scale=0.025, days=(700, 730), fund_gap=0.0008, h1_n=3)

    def _inputs(self, mod):
        ven = mod.load_venues(self.xdir, self.bdir, 0.0005)
        norms = {X.norm_base(s.split('/')[0]) for s in ven['bingx']['bars']} | {v.get('norm') for v in ven['bingx']['markets'].values() if v.get('norm')}
        bn = mod.binance_venue(self.bdir, norms, mod.FEE_DEFAULT, with_h1=True)
        pairs = mod.main_pairs_binance(ven['bingx'], bn)
        inp, rej = mod.build_inputs(ven['bingx'], bn, pairs)
        return inp, rej, pairs

    def test_x1_results_are_bitwise_equal_to_v122(self):
        ni, nrej, npairs = self._inputs(XF)
        ri, rrej, rpairs = self._inputs(REF)
        self.assertGreaterEqual(len(ri['labels']), 9)                                          # ~10 пар
        same(npairs, rpairs)
        same(nrej, rrej)
        for k in ('day0', 'nd', 'labels', 'syms', 'fees', 'elig', 'present'):
            same(ni[k], ri[k], k)
        same(ni['avg'], ri['avg'], 'avg')
        for j in range(len(ri['labels'])):
            for key in ri['M']:
                same(np.asarray(ni['M'][key][:, j]), np.asarray(ri['M'][key][:, j]), f'M.{key}[{j}]')
            for t in 'AB':
                a_, b_ = ni['hs'][t][j], ri['hs'][t][j]
                self.assertEqual(a_ is None, b_ is None)
                if a_ is not None:
                    same(a_, b_, f'hs.{t}[{j}]')
            for vi in (0, 1):
                fa, fb = ni['fund'][j][vi], ri['fund'][j][vi]
                self.assertEqual(fa is None, fb is None)
                if fa is not None:
                    same(fa, fb, f'fund[{j}][{vi}]')
        n_trades = 0
        for T in XF.THRESHOLDS:
            for mult in (1.0, 2.0):
                a_, b_ = XF.simulate_x1(ni, T, mult), REF.simulate_x1(ri, T, mult)
                same(a_, b_, f'sim[T={T},x{mult}]')
                n_trades += len(a_['trades'])
                same(XF.metrics(a_, ni), REF.metrics(b_, ri), f'metrics[T={T},x{mult}]')
                same(XF.top_symbols(a_, ni), REF.top_symbols(b_, ri), 'top_symbols')
        self.assertGreater(n_trades, 0)                                                        # сделки были: сравнение не пустое
        self.assertGreater(sum(1 for j in range(len(ri['labels'])) if ni['hs']['A'][j] is not None), 0)     # и часовой режим затронут

    def test_h2_panel_is_bitwise_equal_to_eager_loading(self):
        P, uni, ref = H2R.build_repl_panel(self.xdir, self.bdir, ('bingx', 'okx'))
        # эталон: прежняя загрузка всех рядов в память (код v122)
        idx = X.load_index(self.xdir)
        mk = {ex: {s: dict(v) for s, v in idx['markets'].get(ex, {}).items()} for ex in X.EXCHANGES}
        bars = {ex: {} for ex in X.EXCHANGES}
        for ex in ('bingx', 'okx'):
            for s in idx['bars'].get(ex, {}):
                b = X.load_bars(self.xdir, ex, s)
                if b is not None and len(b['ts']):
                    bars[ex][s] = b
        uni2, ref2 = X.h2_universe({ex: (mk[ex] if ex in ('bingx', 'okx') else {}) for ex in X.EXCHANGES}, bars, X.binance_norm_set(self.bdir))
        series, fund = {}, {}
        for u in uni2:
            name = f'{u["ex"]}:{u["sym"]}'
            series[name] = bars[u['ex']][u['sym']]
            f = X.load_fund(self.xdir, u['ex'], u['sym'])
            if f is not None:
                fund[name] = f
        P2 = X.panel_from_series(series, fund)
        self.assertGreaterEqual(len(uni2), 3)
        same(uni, uni2, 'uni')
        same(ref, ref2, 'ref')
        for k in P2:
            same(P[k], P2[k], f'P.{k}')


class Memory(unittest.TestCase):
    """Пики памяти на синтетике ПРОД-размера (BingX 1295 рынков / 552 символа свечей / 523 фандинга, Binance 414 общих баз + 1h по 9 символам, OKX 89). RSS — VmHWM дочернего процесса
    с окружением бота (потоки BLAS = 1), без tracemalloc. Прод-лимит RLIMIT_AS здесь не воспроизводится (адресное пространство зависит от сборки Python/numpy), поэтому проверяется RSS."""

    @classmethod
    def setUpClass(cls):
        cls.xdir, cls.bdir = tempfile.mkdtemp(), tempfile.mkdtemp()
        cls.info = SP.build(cls.xdir, cls.bdir, scale=1.0)
        cls.small_x, cls.small_b = tempfile.mkdtemp(), tempfile.mkdtemp()
        SP.build(cls.small_x, cls.small_b, scale=0.05)

    def _stage(self, stage, xdir=None, bdir=None):
        import shutil
        d = tempfile.mkdtemp()
        shutil.rmtree(d)
        shutil.copytree(xdir or self.xdir, d)                                                   # этап пишет в индекс — каждый раз чистая копия
        r = memprobe.run_subprocess(stage, d, bdir or self.bdir, trace=False)
        shutil.rmtree(d, ignore_errors=True)
        self.assertNotIn('error', r, r.get('error'))
        return r

    def test_sizes_are_prod_like(self):
        c = self.info['counts']
        self.assertEqual((c['bingx_markets'], c['bingx_bars'], c['bingx_fund'], c['okx'], c['h1']), (1295, 552, 523, 89, 9))

    def test_load_peak_does_not_depend_on_number_of_done_symbols(self):
        big, small = self._stage('xfund_load'), self._stage('xfund_load', self.small_x, self.small_b)
        self.assertLessEqual(big['vm']['hwm_mb'], LIMIT_LOAD_MB, big['vm'])                      # ≤ 120 МБ при 552 готовых символах
        self.assertLessEqual(small['vm']['hwm_mb'], LIMIT_LOAD_MB)
        self.assertLess(big['vm']['hwm_mb'] - small['vm']['hwm_mb'], 12.0)                       # и не растёт с их числом (27 → 552)
        self.assertIn('✅ загрузка завершена', big['text_tail'])
        h2 = self._stage('h2_load')
        self.assertLessEqual(h2['vm']['hwm_mb'], LIMIT_LOAD_MB, h2['vm'])

    def test_done_symbols_series_are_never_opened_by_loader(self):
        import asyncio
        d = tempfile.mkdtemp()
        b = tempfile.mkdtemp()
        info = SP.build(d, b, scale=0.05)
        opened = []
        orig = (X.load_bars, X.load_fund, X.load_hbars)
        X.load_bars = lambda *a, **k: (opened.append(('bars', a[2])), orig[0](*a, **k))[1]
        X.load_fund = lambda *a, **k: (opened.append(('fund', a[2])), orig[1](*a, **k))[1]
        X.load_hbars = lambda *a, **k: (opened.append(('h', a[2])), orig[2](*a, **k))[1]
        try:
            async def nosleep(_s):
                return None
            idx = X.load_index(d)
            mk = {s: {'swap': True, 'linear': True, 'quote': 'USDT', 'settle': 'USDT', 'base': v['base'], 'active': v['active'], 'info': {}} for s, v in idx['markets']['bingx'].items()}

            class Ex:
                async def load_markets(self_):
                    return mk

                async def fetch_ohlcv(self_, *a, **k):
                    raise AssertionError('готовый символ запрошен повторно')

                async def fetch_funding_rate_history(self_, *a, **k):
                    raise AssertionError('готовый символ запрошен повторно')

                async def close(self_):
                    return None
            now = (SP.END_DAY + 1) * SP.DAY
            txt = asyncio.run(X.run_xload({'xdir': d, 'bdir': b, 'venues': ('bingx',), 'x1_main': True, 'hourly': False}, None, exchanges={'bingx': Ex()}, now_ms=now, sleep=nosleep))
        finally:
            X.load_bars, X.load_fund, X.load_hbars = orig
        done = set(X.load_index(d)['bars']['bingx'])
        self.assertGreater(len(done), 10)
        self.assertEqual([o for o in opened if o[1] in done], [])                               # «что осталось» — только по индексу: ряд готового символа не открывается
        self.assertEqual(txt.splitlines()[-1], '✅ загрузка завершена')
        self.assertGreater(len(info['bingx_bars']), 10)

    def test_x1_calc_peak_under_limit(self):
        pre = self._stage('xfund_prelim')
        self.assertLessEqual(pre['vm']['hwm_mb'], LIMIT_CALC_MB, pre['vm'])
        self.assertIn('пар 414', pre['text_tail'])
        calc = self._stage('xfund_test')
        self.assertLessEqual(calc['vm']['hwm_mb'], LIMIT_CALC_MB, calc['vm'])
        print(f"\n[X1 прод-размер] пик RSS: предварительный прогон {pre['vm']['hwm_mb']:.0f} МБ, расчёт {calc['vm']['hwm_mb']:.0f} МБ (до v123: 331 / 343 МБ)")

    def test_h2_calc_peak_under_limit(self):
        r = self._stage('h2_show')
        self.assertLessEqual(r['vm']['hwm_mb'], LIMIT_CALC_MB, r['vm'])


if __name__ == '__main__':
    unittest.main()
