"""v132 (§2.62): лёгкая панель H2 (память), равенство полной панели, /disk_usage (оценка по не загруженным, audit_cache/vrp), отчёт загрузки. Сеть не используется."""
import asyncio
import json
import os
import subprocess
import sys
import tempfile
import unittest

import numpy as np

import _common
import disk_usage as DU
import research_battery as RB
import research_bsim as S
import research_h2ext as HX
import research_h2repl as H2R
import research_xdata as X
import test_v116_h2x1 as T
import test_v129_h2ext_leadlag as T9
from _common import a

ROOT = _common.ROOT
HERE = os.path.dirname(os.path.abspath(__file__))


def run(c):
    return asyncio.run(c)


class Equality(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.bdir, cls.xdir = tempfile.mkdtemp(), tempfile.mkdtemp()
        T9.BF.write_battery(cls.bdir, n_syms=18)
        T9.write_xdata_ext(cls.xdir)
        cls.full_P, cls.uni_f, cls.ref_f = H2R.build_repl_panel(cls.xdir, cls.bdir, T9.VEN)
        cls.full_env = H2R.make_env(cls.full_P)
        cls.P, cls.uni, cls.ref, cls.liq, cls.top = HX.build_lean(cls.xdir, cls.bdir, T9.VEN)
        cls.env = HX.make_lean_env(cls.P, cls.liq, cls.top)
        cls.fees = RB.fees_of({'fee_taker': 0.0005, 'fee_maker': 0.0002})

    def test_panel_arrays_masks_and_universe_equal(self):
        F, P = self.full_P, self.P
        self.assertEqual(F['syms'], P['syms'])
        self.assertEqual((F['day0'], F['nd']), (P['day0'], P['nd']))
        for k in ('O', 'H', 'C', 'F'):
            self.assertTrue(np.array_equal(np.nan_to_num(F[k], nan=-7.0), np.nan_to_num(P[k], nan=-7.0)), k)
            self.assertEqual(P[k].dtype, np.float32)
        for k in ('first', 'last', 'fund_first', 'has_fund', 'end_data'):
            self.assertTrue(np.array_equal(F[k], P[k]), k)
        self.assertTrue(np.array_equal(self.full_env['ctx']['liq'], self.liq))
        self.assertTrue(np.array_equal(self.full_env['ctx']['top'], self.top))
        self.assertEqual(self.uni, self.uni_f)
        for k in ('QV', 'L', 'adv', 'atr'):                                               # то, что не нужно симуляции, в памяти не держится
            self.assertNotIn(k, P)
            self.assertNotIn(k, self.env['ctx'])

    def test_events_funding_cells_and_h2f_filter_equal(self):
        ev_f, ev_l = S.h2_events(self.full_env['ctx'], 3), S.h2_events(self.env['ctx'], 3)
        self.assertEqual(ev_f, ev_l)
        self.assertTrue(ev_l)
        for e, s in ev_l:
            self.assertEqual(float(self.P['G'][e, s]), float(self.full_P['G'][e, s]))
            self.assertEqual(float(self.P['GC'][e, s]), float(self.full_P['GC'][e, s]))
        with self.assertRaises(KeyError):
            self.P['G'][ev_l[0][0] + 1, ev_l[0][1]]                                         # только ячейки события
        seen = HX.old_norms(self.xdir)
        names = HX.new_symbols(self.uni, T9.VEN, seen)
        self.assertEqual(HX.f_events(self.full_P, ev_f, names), HX.f_events(self.P, ev_l, names))

    def test_returns_bitwise_equal_to_run_cell(self):
        ev = HX.all_events(self.env)
        for mult, fo, co in ((1.0, True, True), (2.0, True, True), (1.0, False, False)):
            r1 = RB.run_cell(('H2', 'D3'), self.full_env, self.fees, mult, fo, co)
            r2 = HX.sim(self.env, self.fees, ev, mult, fo, co)
            self.assertTrue(np.array_equal(r1['r'], r2['r']))
            self.assertEqual(r2['meta']['delist'] + r2['meta']['data_end'], r1['meta']['delist'])

    def test_synthetic_and_k_identical(self):
        for seed in (1, 2):
            f = S.synth_daily_panel(self.full_P, np.random.default_rng(seed))
            l = HX.synth_panel(self.P, np.random.default_rng(seed))
            for k in ('O', 'H', 'C'):
                self.assertTrue(np.array_equal(np.nan_to_num(f[k], nan=-7.0), np.nan_to_num(l[k], nan=-7.0)), (seed, k))
            self.assertNotIn('L', l)
        ev = HX.all_events(self.env)
        k_full = HX.compute_k(self.full_env, self.fees, ev, 'ext', 4)
        k_lean = HX.compute_k(self.env, self.fees, ev, 'ext', 4)
        self.assertEqual(k_full['k'], k_lean['k'])
        self.assertEqual(k_full['m0'], k_lean['m0'])

    def test_run_h2ext_show_uses_lean_panel(self):
        fdir = tempfile.mkdtemp()
        old = HX.K_REPS
        HX.K_REPS = 3
        called = []
        orig = H2R.build_repl_panel
        H2R.build_repl_panel = lambda *x, **k: called.append(1) or orig(*x, **k)
        try:
            txt = run(HX.run_h2ext({'xdir': self.xdir, 'bdir': self.bdir, 'fdir': fdir, 'fee_taker': 0.0005, 'fee_maker': 0.0002, 'version': 'vT', 'venues': T9.VEN, 'mode': 'show',
                                    'hyp': 'ext', 'coverage_min': 0.0, 'spec_path': '/dev/null'}))
        finally:
            H2R.build_repl_panel = orig
            HX.K_REPS = old
        self.assertEqual(called, [])                                                        # полная панель не строится
        self.assertIn('H2EXT-UNIVERSE: N=', txt)


class Universe(unittest.TestCase):
    def test_h2_universe_does_not_keep_series_and_same_result(self):
        """h2_universe хранит только оборот и первый бар; выбор биржи и результат прежние (правила §2.47.2)."""
        d = tempfile.mkdtemp()
        bdir = tempfile.mkdtemp()
        T9.BF.write_battery(bdir, n_syms=18)
        T9.write_xdata_ext(d)
        series, fund, uni, ref, where, mk = H2R.repl_inputs(d, bdir, T9.VEN)
        dup = [u for u in uni if u['norm'] == 'DUP00']
        self.assertEqual([u['ex'] for u in dup], ['gateio'])
        self.assertTrue(all(u['first_day'] == int(series[f'{u["ex"]}:{u["sym"]}']['ts'][0]) // X.DAY for u in uni))


# ── память под лимитом дочернего процесса ──────────────────────────────────────────────────────────
PROBE = r'''
import sys, os, re, json, asyncio, resource
sys.path.insert(0, sys.argv[1]); sys.path.insert(0, sys.argv[1] + "/tests")
resource.setrlimit(resource.RLIMIT_AS, (int(float(sys.argv[4]) * 1048576),) * 2)
def vm():
    s = open("/proc/self/status").read(); g = lambda k: int(re.search(k + r":\s+(\d+) kB", s).group(1)) / 1024
    return {"vmpeak": round(g("VmPeak")), "hwm": round(g("VmHWM"))}
mode = sys.argv[5]
try:
    if mode == "lean":
        import research_h2ext as HX, research_factory as RF
        HX.K_REPS = 3
        txt = asyncio.run(HX.run_h2ext({"xdir": sys.argv[2], "bdir": sys.argv[3], "fdir": sys.argv[2] + "/fdir", "fee_taker": 0.0005, "fee_maker": 0.0002, "version": "p",
                                        "venues": ("bingx", "okx", "gateio", "mexc", "bitget"), "mode": "show", "hyp": "ext", "coverage_min": 0.0, "spec_path": "/dev/null"}))
        print(json.dumps({"ok": "k = " in txt, "vm": vm()}))
    else:
        import research_h2repl as H, research_bsim as S
        P, uni, ref = H.build_repl_panel(sys.argv[2], sys.argv[3], ("bingx", "okx", "gateio", "mexc", "bitget"))
        env = H.make_env(P)
        print(json.dumps({"ok": True, "vm": vm()}))
except MemoryError:
    print(json.dumps({"ok": False, "err": "MemoryError", "vm": vm()}))
'''


def gen_prod_size(out, bdir):
    """Синтетика прод-размера: 1812 символов на 5 биржах, 2463 суток (как отчёт загрузки 2026-11-25)."""
    T9.BF.write_battery(bdir, n_syms=18)
    rng = np.random.default_rng(5)
    D0 = int(np.datetime64('2020-02-24', 'D').astype(np.int64))
    d = lambda s: int(np.datetime64(s, 'D').astype(np.int64))
    ends = {'gateio': d('2026-11-23'), 'mexc': d('2026-11-23'), 'bitget': d('2026-11-23'), 'bingx': d('2026-11-19'), 'okx': d('2026-11-18')}
    spec = [('gateio', 351, D0), ('mexc', 557, d('2020-10-16')), ('bitget', 263, d('2024-03-12')), ('bingx', 552, d('2021-05-14')), ('okx', 89, d('2021-04-07'))]
    idx = X.load_index(out)
    mk = {ex: {} for ex in X.EXCHANGES}
    k = 0
    for ex, n, start in spec:
        end = ends[ex]
        for i in range(n):
            k += 1
            fd = start + int(rng.uniform(0, (end - start) - 40)) if i > 0 else start
            nn = end - fd + 1
            b = T.mk_bars(fd, nn, k, vol=0.07, qv=float(rng.uniform(2e7, 2e8)))
            sym = f'T{k:04d}/USDT:USDT'
            X.save_bars(out, ex, sym, b['ts'], b['o'], b['h'], b['l'], b['c'], b['v'])
            idx['bars'][ex][sym] = {'first': int(b['ts'][0]), 'last': int(b['ts'][-1]), 'n': int(nn), 't': 1, 'complete': True}
            ft, fr = T.mk_fund(fd, min(nn, 90), k + 9)
            X.save_fund(out, ex, sym, ft, fr)
            idx['fund'][ex][sym] = {'first': int(ft[0]), 'last': int(ft[-1]), 'n': len(ft), 't': 1, 'complete': True}
            mk[ex][sym] = {'base': f'T{k:04d}', 'norm': f'T{k:04d}', 'active': True, 'created': None, 'excl': None, 'cat': ''}
    idx['markets'] = mk
    X.save_index(out, idx)


class MemoryLimit(unittest.TestCase):
    """Прод-размер под RLIMIT_AS = 300 МБ (как RESEARCH_MEM_MB): полная панель падает (MemoryError — как на проде 2026-11-25), лёгкая проходит с запасом."""

    @classmethod
    def setUpClass(cls):
        cls.xd, cls.bd = tempfile.mkdtemp(), tempfile.mkdtemp()
        gen_prod_size(cls.xd, cls.bd)
        cls.script = os.path.join(tempfile.mkdtemp(), 'probe.py')
        with open(cls.script, 'w', encoding='utf-8') as f:
            f.write(PROBE)
        sys.path.insert(0, ROOT)
        import research_runner as RR
        cls.env = RR.child_env(300)

    def probe(self, mode, limit=300):
        r = subprocess.run([sys.executable, '-I', cls_script(self), ROOT, self.xd, self.bd, str(limit), mode], capture_output=True, text=True, env=self.env, timeout=900)
        self.assertEqual(r.returncode, 0, r.stderr[-400:])
        return json.loads(r.stdout.strip().splitlines()[-1])

    def test_full_panel_fails_lean_passes_under_limit(self):
        old = self.probe('full')
        self.assertFalse(old['ok'])                                                          # воспроизведена прод-ошибка
        self.assertEqual(old['err'], 'MemoryError')
        new = self.probe('lean')
        self.assertTrue(new['ok'], new)
        self.assertLess(new['vm']['vmpeak'], 290)                                            # запас к лимиту 300 МБ


def cls_script(t):
    return t.script


class DiskReport(unittest.TestCase):
    def test_need_counts_only_not_yet_loaded(self):
        bdir, d = tempfile.mkdtemp(), tempfile.mkdtemp()
        T9.BF.write_battery(bdir, n_syms=6)

        class C:
            async def load_markets(self):
                return {f'N{i}/USDT:USDT': {'swap': True, 'linear': True, 'quote': 'USDT', 'settle': 'USDT', 'base': f'N{i}', 'active': True, 'created': None, 'info': {}} for i in range(10)}

            async def close(self):
                pass
        idx = X.load_index(d)
        for i in range(6):                                                                   # 6 загружены, 1 подтверждённо пуст, 3 осталось
            X.put(d, idx, 'bars', 'gateio', f'N{i}/USDT:USDT', {'first': 1, 'last': 2, 'n': 2, 't': 1, 'complete': True})
        X.put(d, idx, 'neg', 'gateio', 'N6/USDT:USDT|bars', {'t': 1, 'why': 'empty'})
        X.put(d, idx, 'neg', 'gateio', 'N7/USDT:USDT|bars', {'t': 1, 'why': 'VenueError', 'msg': 'x'})
        out = run(HX.count_contracts(('gateio',), bdir, {'gateio': C()}, d))
        self.assertEqual(out['gateio'], {'markets': 10, 'want': 10, 'todo': 3})
        root = tempfile.mkdtemp()
        txt = DU.format_report(root, [], 425.0, 1021.0, {}, 0.0, 200.0, out, ('gateio',))
        self.assertIn('ещё не загружено 3', txt)
        self.assertIn('3 ещё не загруженных символов', txt)
        done = {'gateio': {'markets': 10, 'want': 10, 'todo': 0}}
        txt2 = DU.format_report(root, [], 425.0, 1021.0, {}, 0.0, 200.0, done, ('gateio',))
        self.assertIn('0 МБ (все символы уже загружены', txt2)
        self.assertIn('хватает', txt2)
        self.assertNotIn('не хватает', txt2)

    def test_audit_cache_and_vrp_are_archives(self):
        root = tempfile.mkdtemp()
        for rel, n in (('audit_cache/x.npz', 3000), ('audit.db', 80), ('vrp/dvol.json', 40), ('battery/b.npz', 500)):
            p = os.path.join(root, rel)
            os.makedirs(os.path.dirname(p), exist_ok=True)
            open(p, 'wb').write(b'x' * n)
        items = {x['key']: x for x in DU.layout(root)}
        for k in ('audit_cache', 'vrp'):
            self.assertEqual(items[k]['cls'], 'архив закрытого исследования', k)
            self.assertTrue(items[k]['cleanable'] and '§' in items[k]['note'])
        self.assertEqual(items['audit_cache']['size'], 3000)
        for k in ('audit.db', 'battery'):
            self.assertFalse(items[k]['cleanable'])
        ok, _m, paths, size = DU.cleanup_plan(root, list(items.values()), 'audit_cache')
        self.assertTrue(ok)
        self.assertEqual([os.path.basename(p) for p in paths], ['audit_cache'])
        self.assertFalse(DU.cleanup_plan(root, list(items.values()), 'audit.db')[0])
        self.assertNotIn('не классифицировано', {x['cls'] for x in items.values()})


class LoadReport(unittest.TestCase):
    def test_bitget_form_info_and_depth_flag(self):
        from test_v131_bitget_classify_dataend import strict_bitget
        ex = strict_bitget(max_window_bars=30)
        self.assertIsNone(ex.form_info())
        run(ex.load_markets())
        run(ex.fetch_ohlcv('BTC/USDT:USDT', '1d', None, 1000, {'endTime': T9.NOW}))
        info = ex.form_info()
        self.assertIn('history-candles', info)
        self.assertIn('30 баров', info)
        self.assertIn('форма не определена', info)                                           # фандинг ещё не запрашивался
        d = tempfile.mkdtemp()
        idx = X.load_index(d)
        for i in range(5):
            idx['bars']['bitget'][f'S{i}/USDT:USDT'] = {'first': 1, 'last': 2 * X.DAY, 'n': 1000 if i < 3 else 200, 't': 1, 'complete': True}
        X.save_index(d, idx)
        txt = '\n'.join(X.coverage_lines(d, X.load_index(d), 3 * X.DAY))
        self.assertIn('символов с ≥ 990 барами истории: 3 из 5', txt)

    def test_xload_report_prints_form_info(self):
        d, bdir = tempfile.mkdtemp(), tempfile.mkdtemp()
        T9.BF.write_battery(bdir, n_syms=6)
        ex = strict_bitget_with_one_contract()
        txt = run(X.run_xload({'xdir': d, 'bdir': bdir, 'venues': ('bitget',), 'ext': True, 'hourly': False, 'max_minutes': 5}, None, exchanges={'bitget': ex},
                              now_ms=T9.NOW, sleep=T9.nosleep))
        self.assertIn('Bitget: принятая форма запроса — свечи: history-candles', txt)


def strict_bitget_with_one_contract():
    from test_v131_bitget_classify_dataend import strict_bitget
    ex = strict_bitget(max_window_bars=30)
    orig = ex.load_markets

    async def lm():
        m = await orig()
        for v in m.values():
            v['base'] = 'ZNEW'
        return {'ZNEW/USDT:USDT': dict(next(iter(m.values())), base='ZNEW')}
    ex.load_markets = lm
    return ex


class Docs(unittest.TestCase):
    def test_records(self):
        src = open(os.path.join(ROOT, 'BOT_SPEC.md'), encoding='utf-8').read()
        for needle in ('### 2.62.', '#### 2.62.1', '#### 2.62.2', '#### 2.62.3', '| v132 |'):
            self.assertIn(needle, src)
        self.assertIn('2026-11-25-v132', [x[0] for x in a.CHANGELOG])


if __name__ == '__main__':
    unittest.main()
