"""v130: маркер H2EXT-VENUES, диск до /h2_ext load и /disk_usage, /disk_cleanup, возобновление lead-lag после перезапуска и разрывы, правило двух тиков B3 (§2.59.10–§2.59.11, §2.60.6–§2.60.7)."""
import asyncio
import os
import re
import tempfile
import unittest

import numpy as np

import _common
import disk_usage as DU
import research_h2ext as HX
import research_leadlag as LL
import research_xdata as X
import test_v116_h2x1 as T
import test_v129_h2ext_leadlag as T9
from _common import a

ROOT = _common.ROOT


def run(c):
    return asyncio.run(c)


class Marker(unittest.TestCase):
    def test_bot_spec_marker_and_union_with_bingx_okx(self):
        self.assertEqual(X.spec_line('H2EXT-VENUES:'), 'gateio,mexc,bitget')
        self.assertEqual(X.ext_venues_from_spec(), ('bingx', 'okx', 'gateio', 'mexc', 'bitget'))
        f = tempfile.NamedTemporaryFile('w', suffix='.md', delete=False, encoding='utf-8')
        f.write('H2EXT-VENUES: mexc\n')
        f.close()
        self.assertEqual(X.ext_venues_from_spec(f.name), ('bingx', 'okx', 'mexc'))

    def test_load_requests_only_new_venues(self):
        """/h2_ext load обращается только к биржам маркера; BingX/OKX не запрашиваются (кэш §2.47)."""
        d, bdir = tempfile.mkdtemp(), tempfile.mkdtemp()
        T9.BF.write_battery(bdir, n_syms=6)
        now = int(np.datetime64('2023-06-01', 'ms').astype(np.int64))
        first = int(np.datetime64('2023-01-01', 'D').astype(np.int64))
        bars = T.mk_bars(first, 140, 7, vol=0.06, qv=6e7)

        class Ex:
            def __init__(self, base):
                self.base, self.called = base, []

            async def load_markets(self):
                self.called.append('m')
                return {f'{self.base}/USDT:USDT': {'swap': True, 'linear': True, 'quote': 'USDT', 'settle': 'USDT', 'base': self.base, 'active': True, 'created': None, 'info': {}}}

            async def fetch_ohlcv(self, sym, tf, since=None, limit=None, params=None):
                self.called.append('o')
                return T.ohlcv_window(bars, since, limit or 1000, params)

            async def fetch_funding_rate_history(self, sym, since=None, limit=None, params=None):
                ft, fr = T.mk_fund(first, 140, 3)
                end = (params or {}).get('endTime')
                return [{'timestamp': int(t), 'fundingRate': float(r)} for t, r in zip(ft, fr) if end is None or t <= end][-(limit or 100):]

            async def close(self):
                pass
        exs = {'bingx': Ex('AAA'), 'okx': Ex('BBB'), 'gateio': Ex('CCC'), 'mexc': Ex('DDD'), 'bitget': Ex('EEE')}
        txt = run(X.run_xload({'xdir': d, 'bdir': bdir, 'venues': ('gateio', 'mexc', 'bitget'), 'ext': True, 'hourly': False, 'max_minutes': 5}, None, exchanges=exs, now_ms=now, sleep=T9.nosleep))
        self.assertIn('✅ загрузка завершена', txt)
        self.assertEqual(exs['bingx'].called, [])
        self.assertEqual(exs['okx'].called, [])
        for ex in ('gateio', 'mexc', 'bitget'):
            self.assertTrue(exs[ex].called)
        idx = X.load_index(d)
        self.assertEqual({ex for ex in X.EXCHANGES if idx['bars'][ex]}, {'gateio', 'mexc', 'bitget'})


# ── диск до /h2_ext load ────────────────────────────────────────────────────────────────────────
class SpaceGuard(unittest.TestCase):
    ST = {'running': True, 'until': 1_800_000_000 + 5.1 * 86400, 'step': 6.75, 'symbols': [f'S{i}' for i in range(33)]}
    NOW = 1_800_000_000.0

    def test_numbers_for_the_prod_case(self):
        g = DU.leadlag_remaining_mb(self.ST, self.NOW)
        self.assertAlmostEqual(g, 5.1 * 86400 / 6.75 * (12 * 33 + 16) / 1e6, places=6)
        self.assertTrue(26 < g < 28)
        ok, d = DU.space_check(259.0, 10, self.ST, self.NOW, 200.0)
        self.assertFalse(ok)                                                       # 259 − 1.4 − 26.9 = 230.7 < 250
        self.assertAlmostEqual(d['need_after'], 250.0)
        self.assertTrue(18 < d['short'] < 21)
        ok2, _ = DU.space_check(400.0, 100, self.ST, self.NOW, 200.0)
        self.assertTrue(ok2)
        ok3, d3 = DU.space_check(259.0, 10, {'running': False}, self.NOW, 200.0)   # замер не идёт — нет роста, но проверка в guard не применяется
        self.assertEqual(d3['growth'], 0.0)

    def test_guard_refuses_before_download_only_while_collecting(self):
        d, ll, bdir = tempfile.mkdtemp(), tempfile.mkdtemp(), tempfile.mkdtemp()
        T9.BF.write_battery(bdir, n_syms=6)
        now_s = 1_685_600_000.0
        LL.save_state(ll, dict(self.ST, until=now_s + 5.1 * 86400))
        first = int(np.datetime64('2023-01-01', 'D').astype(np.int64))
        bars = T.mk_bars(first, 140, 7, vol=0.06, qv=6e7)

        class Ex:
            def __init__(self, n):
                self.n, self.called = n, []

            async def load_markets(self):
                return {f'C{i:03d}/USDT:USDT': {'swap': True, 'linear': True, 'quote': 'USDT', 'settle': 'USDT', 'base': f'C{i:03d}', 'active': True, 'created': None, 'info': {}} for i in range(self.n)}

            async def fetch_ohlcv(self, sym, tf, since=None, limit=None, params=None):
                self.called.append('o')
                return T.ohlcv_window(bars, since, limit or 1000, params)

            async def fetch_funding_rate_history(self, sym, since=None, limit=None, params=None):
                self.called.append('f')
                return []

            async def close(self):
                pass
        ex = Ex(400)
        guard = HX.space_guard(d, ll, {'free_mb': 259.0})
        orig = LL.load_state
        LL.load_state = lambda p: dict(self.ST, until=float(__import__('time').time()) + 5.1 * 86400)      # «сейчас» — реальное время теста
        try:
            txt = run(X.run_xload({'xdir': d, 'bdir': bdir, 'venues': ('gateio',), 'ext': True, 'hourly': False, 'space_guard': guard}, None, exchanges={'gateio': ex},
                                  now_ms=int(np.datetime64('2023-06-01', 'ms').astype(np.int64)), sleep=T9.nosleep))
            self.assertIn('load отказ', txt)
            self.assertIn('Не хватает', txt)
            self.assertIn('/disk_usage', txt)
            self.assertEqual(ex.called, [])                                          # до скачивания свечей
            self.assertFalse(X.load_index(d)['bars']['gateio'])
            # места достаточно → загрузка идёт
            g2 = HX.space_guard(d, ll, {'free_mb': 5000.0})
            txt2 = run(X.run_xload({'xdir': d, 'bdir': bdir, 'venues': ('gateio',), 'ext': True, 'hourly': False, 'space_guard': g2, 'max_minutes': 5}, None, exchanges={'gateio': Ex(3)},
                                   now_ms=int(np.datetime64('2023-06-01', 'ms').astype(np.int64)), sleep=T9.nosleep))
            self.assertNotIn('load отказ', txt2)
            self.assertTrue(X.load_index(d)['bars']['gateio'])
            # замер не идёт → проверка не применяется даже при малом месте
            LL.load_state = lambda p: {'running': False}
            g3 = HX.space_guard(tempfile.mkdtemp(), ll, {'free_mb': 1.0})
            self.assertIsNone(g3(['x'] * 1000, {'bars': {}}))
        finally:
            LL.load_state = orig


# ── /disk_usage и /disk_cleanup ───────────────────────────────────────────────────────────────────
def mk_root():
    root = tempfile.mkdtemp()

    def w(rel, n):
        p = os.path.join(root, rel)
        os.makedirs(os.path.dirname(p), exist_ok=True)
        with open(p, 'wb') as f:
            f.write(b'x' * n)
    for rel, n in (('trades.db', 100), ('trades.db-wal', 20), ('bot.db', 50), ('logs/digest.txt', 30), ('vision/a.npz', 5000), ('vision/b.npz', 3000), ('edge.db', 400), ('battery/x.npz', 700),
                   ('xdata/bingx/A.d1.npz', 60), ('xdata/bingx/A.h1.npz', 900), ('xdata/index.json', 10), ('factory/factory.db', 200), ('factory/factory.db-wal', 10), ('factory/duka/EURUSD.npz', 2000),
                   ('audit.db', 80), ('tg.session', 5), ('leadlag/ts.bin', 160), ('mystery.bin', 77)):
        w(rel, n)
    return root


class Disk(unittest.TestCase):
    def test_layout_classes_and_sizes(self):
        root = mk_root()
        items = {x['key']: x for x in DU.layout(root)}
        self.assertEqual(items['vision']['size'], 8000)
        self.assertEqual(items['vision']['cls'], 'архив закрытого исследования')
        self.assertEqual(items['edge']['cls'], 'архив закрытого исследования')
        self.assertEqual(items['factory_data']['size'], 2000)                       # данные «фабрики» без factory.db
        self.assertEqual(items['factory.db']['size'], 210)
        self.assertEqual(items['trades.db']['size'], 120)                           # с -wal
        for k in ('battery', 'xdata', 'leadlag', 'logs', 'trades.db', 'bot.db', 'audit.db', 'tg', 'factory.db'):
            self.assertEqual(items[k]['cls'], 'активно', k)
            self.assertFalse(items[k]['cleanable'], k)
        self.assertEqual(items['mystery.bin']['cls'], 'не классифицировано')
        for x in items.values():
            self.assertTrue(x['note'])
            if x['cls'] == 'архив закрытого исследования':
                self.assertRegex(x['note'], r'§')                                    # раздел BOT_SPEC указан
        self.assertEqual(DU.h1_bytes(items['xdata']['path']), 900)

    def test_cleanup_plan_only_closed_archives(self):
        root = mk_root()
        items = DU.layout(root)
        for key in ('battery', 'xdata', 'audit.db', 'factory.db', 'trades.db', 'leadlag', 'mystery.bin', 'tg'):
            ok, msg, paths, _ = DU.cleanup_plan(root, items, key)
            self.assertFalse(ok, key)
            self.assertEqual(paths, [])
        self.assertFalse(DU.cleanup_plan(root, items, 'нет-такого')[0])
        ok, _m, paths, size = DU.cleanup_plan(root, items, 'factory_data')
        self.assertTrue(ok)
        self.assertTrue(all('factory.db' not in os.path.basename(p) for p in paths))
        ok, _m, paths, size = DU.cleanup_plan(root, items, 'edge')
        self.assertTrue(ok)
        self.assertEqual([os.path.basename(p) for p in paths], ['edge.db'])

    def test_report_totals(self):
        root = mk_root()
        items = DU.layout(root)
        st = dict(SpaceGuard.ST)
        txt = DU.format_report(root, items, 259.0, 1000.0, st, SpaceGuard.NOW, 200.0, {'gateio': {'markets': 600, 'want': 300}, 'mexc': {'markets': None, 'want': None, 'err': 'HTTP 403'}}, ('gateio', 'mexc'))
        for needle in ('/disk_usage', 'активно', 'архив закрытого исследования', 'свободно 259', 'нужно для /h2_ext load', 'нужно до конца lead-lag', 'gateio: всего 600, к загрузке 300', 'mexc: нет данных', 'не хватает'):
            self.assertIn(needle, txt)
        txt2 = DU.format_report(root, items, 259.0, 1000.0, {}, SpaceGuard.NOW, 200.0, None, ())
        self.assertIn('нет данных (число контрактов не получено)', txt2)

    def test_count_contracts_and_disk_job_without_changes(self):
        bdir = tempfile.mkdtemp()
        T9.BF.write_battery(bdir, n_syms=6)
        root = mk_root()
        before = sorted(os.listdir(root))

        class C:
            async def load_markets(self):
                return {'NEWA/USDT:USDT': {'swap': True, 'linear': True, 'quote': 'USDT', 'settle': 'USDT', 'base': 'NEWA', 'active': True, 'created': None, 'info': {}},
                        'NCSK/USDT:USDT': {'swap': True, 'linear': True, 'quote': 'USDT', 'settle': 'USDT', 'base': 'NCSKAPPL', 'active': True, 'created': None, 'info': {}}}

            async def close(self):
                pass

        class Bad(C):
            async def load_markets(self):
                raise RuntimeError('HTTP 403')
        out = run(HX.count_contracts(('gateio', 'mexc'), bdir, {'gateio': C(), 'mexc': Bad()}))
        self.assertEqual(out['gateio'], {'markets': 2, 'want': 1})
        self.assertIsNone(out['mexc']['markets'])
        txt = run(HX.disk_job({'root': root, 'bdir': bdir, 'venues': ('bingx', 'okx', 'gateio', 'mexc'), 'clients': {'gateio': C(), 'mexc': Bad()}, 'll_dir': tempfile.mkdtemp()}))
        self.assertIn('Итог', txt)
        self.assertEqual(sorted(os.listdir(root)), before)                          # /disk_usage ничего не меняет

    def test_cleanup_command_requires_confirm_and_never_automatic(self):
        root = mk_root()
        _db, sent = _common.fresh_state()
        prm = {'root': root, 'vision': os.path.join(root, 'vision'), 'factory': os.path.join(root, 'factory'), 'edge': os.path.join(root, 'edge.db'), 'battery': os.path.join(root, 'battery'),
               'xdata': os.path.join(root, 'xdata'), 'leadlag': os.path.join(root, 'leadlag'), 'jobs': os.path.join(root, 'research_jobs'), 'll_dir': os.path.join(root, 'leadlag')}
        orig = a._disk_params
        a._disk_params = lambda: prm
        try:
            run(a.disk_cleanup_cmd([]))
            run(a.disk_cleanup_cmd(['vision']))
            self.assertTrue(os.path.exists(os.path.join(root, 'vision', 'a.npz')))        # без confirm — только показ
            self.assertTrue(any('/disk_cleanup vision confirm' in m for m in sent))
            run(a.disk_cleanup_cmd(['battery', 'confirm']))
            run(a.disk_cleanup_cmd(['audit.db', 'confirm']))
            self.assertTrue(os.path.exists(os.path.join(root, 'battery', 'x.npz')) and os.path.exists(os.path.join(root, 'audit.db')))
            a._htf_running[0] = True
            try:
                run(a.disk_cleanup_cmd(['vision', 'confirm']))
                self.assertTrue(os.path.exists(os.path.join(root, 'vision', 'a.npz')))    # идёт задача — отказ
            finally:
                a._htf_running[0] = False
            run(a.disk_cleanup_cmd(['factory_data', 'confirm']))
            self.assertTrue(os.path.exists(os.path.join(root, 'factory', 'factory.db')))     # флаги финалов сохранены
            self.assertFalse(os.path.exists(os.path.join(root, 'factory', 'duka')))
            run(a.disk_cleanup_cmd(['vision', 'confirm']))
            self.assertFalse(os.path.exists(os.path.join(root, 'vision')))
            self.assertTrue(os.path.exists(os.path.join(root, 'xdata', 'bingx', 'A.d1.npz')) and os.path.exists(os.path.join(root, 'trades.db')))
        finally:
            a._disk_params = orig
        src = open(os.path.join(ROOT, 'async_bot.py'), encoding='utf-8').read()
        self.assertEqual(len(re.findall(r'DU\.cleanup_run', src)), 1)                      # единственное место вызова — disk_cleanup_cmd
        seg = src[src.index('async def disk_cleanup_cmd'):src.index('def _ll_running')]
        self.assertIn('DU.cleanup_run', seg)
        for f in ('research_h2ext.py', 'research_leadlag.py', 'research_xdata.py'):
            self.assertNotIn('cleanup_run', open(os.path.join(ROOT, f), encoding='utf-8').read())

    def test_commands_registered(self):
        pool, allv = a.command_pools()
        for c in ('/disk_usage', '/disk_cleanup'):
            self.assertIn(c, allv)
            self.assertIsNone(a.command_hint(c))
        import research_runner as R
        self.assertIn('disk_usage', R.TASKS)
        p = a._disk_params()
        for k in ('root', 'vision', 'factory', 'edge', 'battery', 'xdata', 'leadlag', 'jobs', 'll_dir'):
            self.assertTrue(p[k])


# ── возобновление lead-lag после перезапуска ──────────────────────────────────────────────────────
class Resume(unittest.TestCase):
    def collector(self, d, clk, **kw):
        async def gb():
            return T9.snaps({'AAA': 0.001, 'BBB': -0.001})[0]

        async def go():
            return T9.snaps({'AAA': 0.001, 'BBB': -0.001})[1]
        return LL.Collector(d, gb, go, step=2.0, sleep=clk.sleep, now=clk.now, **kw)

    async def run_until(self, col, ticks, **kw):
        task = asyncio.create_task(col.run(**kw))
        while col.ticks + len(col.buf_ts) < ticks and not task.done():
            await asyncio.sleep(0)
        return task

    def test_resume_keeps_ticks_marks_gap_and_status_shows_it(self):
        d = tempfile.mkdtemp()
        clk = T9.Clock()
        old = LL.FLUSH_TICKS
        LL.FLUSH_TICKS = 1000                                                         # сброс только по времени (60 с)

        async def scenario():
            col = self.collector(d, clk)
            task = await self.run_until(col, 200)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            return col

        try:
            run(scenario())
            n1 = LL.load_ts(d).shape[0]
            self.assertTrue(n1 >= 200 - 1)                                           # при отмене буфер сброшен
            st = LL.load_state(d)
            self.assertTrue(st['running'] and st['warm_done'])
            clk.t += 900.0                                                            # сервис «лежал» 15 минут
            col2 = self.collector(d, clk)

            async def second():
                task = await self.run_until(col2, n1 + 60, resume=True)
                col2.stop = True
                await task
            run(second())
        finally:
            LL.FLUSH_TICKS = old
        ts = LL.load_ts(d)
        self.assertTrue(len(ts) >= n1 + 60)                                           # накопленные тики не потеряны
        self.assertTrue(np.array_equal(ts[:n1], LL.load_ts(d)[:n1]))
        for b in ('AAA', 'BBB'):
            self.assertEqual(len(LL.load_sym(d, b, 10 ** 9)), len(ts))               # файлы символов выровнены
        st = LL.load_state(d)
        self.assertEqual(len(st['resumes']), 1)
        self.assertTrue(880 < st['resumes'][0]['gap_s'] < 940)                       # длительность разрыва записана
        g = LL.gaps_info(ts, 2.0)
        self.assertEqual(len(g), 1)
        self.assertTrue(850 < g[0][1] < 940)
        txt = LL.status_text(d)
        self.assertIn('разрывов 1', txt)
        self.assertIn('возобновлений после перезапуска 1', txt)
        self.assertIn(f'тиков {len(ts)}', txt)

    def test_hard_kill_loses_at_most_one_flush_interval(self):
        d = tempfile.mkdtemp()
        clk = T9.Clock()
        old = LL.FLUSH_TICKS
        LL.FLUSH_TICKS = 100000

        async def scenario():
            col = self.collector(d, clk)
            task = asyncio.create_task(col.run())
            while clk.t < 1_800_000_000.0 + 250:
                await asyncio.sleep(0)
            col.flush = lambda: None                                                   # «убийство» процесса: финальный сброс не происходит
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            return col
        try:
            run(scenario())
        finally:
            LL.FLUSH_TICKS = old
        n = LL.load_ts(d).shape[0]
        self.assertTrue(250 / 2 - LL.FLUSH_MAX_S / 2 - 3 <= n <= 250 / 2 + 3, n)       # потеря не больше интервала сброса (60 с)
        col2 = self.collector(d, clk)
        col2.syms = LL.load_state(d)['symbols']
        self.assertEqual(col2.repair(), n)

    def test_time_based_flush_and_sigterm_handler_registered(self):
        async def scenario():
            d = tempfile.mkdtemp()
            clk = T9.Clock()
            col = self.collector(d, clk)
            self.assertTrue(col.install_sigterm())
            import signal
            loop = asyncio.get_running_loop()
            self.assertIn(signal.SIGTERM, loop._signal_handlers)
            loop.remove_signal_handler(signal.SIGTERM)
        run(scenario())

    def test_autoresume_launches_and_reports_gap(self):
        d = tempfile.mkdtemp()
        os.environ['LEADLAG_DIR'] = d
        _db, sent = _common.fresh_state()
        import time as _t
        LL.save_state(d, {'running': True, 'until': _t.time() + 3 * 86400, 'step': 6.75, 'symbols': ['AAA']})
        orig = a._ll_launch

        async def fake(resume):
            return {'resumes': [{'gap_s': 600.0}]}
        a._ll_launch = fake
        try:
            run(a.leadlag_autoresume())
        finally:
            a._ll_launch = orig
            os.environ.pop('LEADLAG_DIR')
        self.assertTrue(any('разрыв 10.0 мин' in m for m in sent))


# ── B3: правило двух тиков ─────────────────────────────────────────────────────────────────────────
class TwoTicks(unittest.TestCase):
    STEP = 6.75

    def series(self, ticks_above):
        d = tempfile.mkdtemp()
        arr, ts = T9.make_series(d, lambda i: (0.004, 0.0, 0.0) if 100 <= i < 100 + ticks_above else (0.0, 0.0, 0.0), n=400, step=self.STEP)
        return d, arr, ts

    def test_single_tick_not_in_criterion_two_ticks_are(self):
        d, arr, ts = self.series(1)
        r = LL.analyse_symbol(arr, ts, self.STEP, 0.0025, 0.0005)
        self.assertEqual((r['events'], r['single'], r['multi'], r['exec']), (1, 1, 0, 0))
        d, arr, ts = self.series(2)
        r = LL.analyse_symbol(arr, ts, self.STEP, 0.0025, 0.0005)
        self.assertEqual((r['events'], r['single'], r['multi'], r['exec']), (1, 0, 1, 1))           # 0.4% − 0.04% − 0.10% = 0.26% > 0.10%
        self.assertAlmostEqual(r['dur_exec'][0], self.STEP, places=2)
        d, arr, ts = self.series(5)
        self.assertEqual(LL.analyse_symbol(arr, ts, self.STEP, 0.0025, 0.0005)['exec'], 1)

    def test_entry_is_second_tick(self):
        """Вход — второй тик: если на втором тике gap уже ниже издержек, событие не исполнимо, даже если на первом был большой."""
        d = tempfile.mkdtemp()

        def spec(i):
            return {100: (0.004, 0, 0), 101: (0.0027, 0, 0)}.get(i, (0.0, 0.0, 0.0))
        arr, ts = T9.make_series(d, spec, n=300, step=self.STEP, hs=0.001)
        r = LL.analyse_symbol(arr, ts, self.STEP, 0.0025, 0.0005)
        self.assertEqual((r['multi'], r['exec']), (1, 0))                                          # 0.27% − 0.1% − 0.1% − 0.1% < 0.10%

    def test_report_lines_single_and_actual_step(self):
        d, arr, ts = self.series(1)
        txt = LL.report(d, 0.0005)
        self.assertIn('события в одном тике (отдельно, в критерий не входят)', txt)
        self.assertIn('Фактический шаг по времени', txt)
        self.assertIn('6.75', txt)
        self.assertIn('≥ 2 подряд идущих тиках', txt)
        self.assertIn('Разрывы данных', txt)
        d, arr, ts = self.series(2)
        txt2 = LL.report(d, 0.0005)
        self.assertIn('событий в ≥ 2 тиках подряд 1', txt2)

    def test_b3_uses_only_multi_tick_events(self):
        d = tempfile.mkdtemp()
        old = (LL.EXEC_PER_DAY_MIN, LL.EXEC_SYMS_MIN)
        try:
            T9.make_series(d, lambda i: (0.004, 0.0, 0.0) if i % 20 == 0 else (0.0, 0.0, 0.0), n=1200, step=self.STEP)        # только одиночные тики
            LL.EXEC_PER_DAY_MIN, LL.EXEC_SYMS_MIN = 1.0, 1
            txt = LL.report(d, 0.0005)
            self.assertIn('направление закрыто', txt.split('ИТОГ B3')[1])
            T9.make_series(d, lambda i: (0.004, 0.0, 0.0) if i % 20 < 2 else (0.0, 0.0, 0.0), n=1200, step=self.STEP)          # пары тиков
            txt = LL.report(d, 0.0005)
            self.assertIn('критерий выполнен', txt.split('ИТОГ B3')[1])
        finally:
            LL.EXEC_PER_DAY_MIN, LL.EXEC_SYMS_MIN = old


class Docs(unittest.TestCase):
    def test_spec_has_v130_records(self):
        src = open(os.path.join(ROOT, 'BOT_SPEC.md'), encoding='utf-8').read()
        for needle in ('#### 2.59.10', '#### 2.59.11', '#### 2.60.6', '#### 2.60.7', 'H2EXT-VENUES: gateio,mexc,bitget', '| v130 |'):
            self.assertIn(needle, src)
        self.assertIn('v130', src.split('\n', 3)[1])


if __name__ == '__main__':
    unittest.main()
