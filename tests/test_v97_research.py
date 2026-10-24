"""v97: исследования в отдельном процессе (лимит памяти), R1 по символам с контрольными точками и SQL-агрегацией, стартовое сообщение, тревога по памяти."""
import asyncio
import os
import random
import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone

from _common import a, fresh_state, ROOT
import research_edge as E
import research_runner as RR
import research_vision as V
from test_v95_edge import walk, FEES

START = int(datetime(2023, 6, 1, tzinfo=timezone.utc).timestamp() * 1000)


def run(coro):
    return asyncio.run(coro)


class ChildProcess(unittest.TestCase):
    def setUp(self):
        self.db, self.sent = fresh_state()
        self.base = tempfile.mkdtemp(prefix='jobs_')
        self.saved_env = os.environ.get('RESEARCH_MEM_MB')

    def tearDown(self):
        if self.saved_env is None:
            os.environ.pop('RESEARCH_MEM_MB', None)
        else:
            os.environ['RESEARCH_MEM_MB'] = self.saved_env

    def test_ok_job_returns_text_and_batches_progress(self):
        msgs = []

        async def pr(t):
            msgs.append(t)
        r = run(RR.run_job('_selftest_ok', {'x': 7}, progress=pr, poll_s=0.1, base_dir=self.base))
        self.assertTrue(r['ok'], r)
        self.assertEqual(r['text'], 'selftest ok 7')
        self.assertIn('selftest: шаг 2', '\n'.join(msgs))
        self.assertEqual(os.listdir(self.base), [])                 # рабочая папка убрана

    def test_memory_limit_kills_only_child_and_main_survives(self):
        os.environ['RESEARCH_MEM_MB'] = '80'
        text = run(a._research_job('/edge_audit', '_selftest_mem', {'alloc_mb': 600}))
        self.assertIsNone(text)                                     # основной процесс жив (этот тест) и получил отказ, а не исключение
        msg = self.sent[-1]
        self.assertIn('расчёт прерван', msg)
        self.assertIn('MemoryError', msg)
        self.assertIn('RESEARCH_MEM_MB=80', msg)
        self.assertIn('selftest: старт', msg)                       # последний прогресс
        self.assertIn('бот работает', msg)
        self.assertTrue(any('selftest: старт' in m for m in self.sent[:-1]))     # прогресс был переслан в Telegram
        # после падения бот продолжает: следующий запуск в этом же процессе работает
        r = run(RR.run_job('_selftest_ok', {}, base_dir=self.base, poll_s=0.1))
        self.assertTrue(r['ok'])

    def test_run_job_memory_result_fields(self):
        r = run(RR.run_job('_selftest_mem', {'alloc_mb': 600}, mem_mb=80, poll_s=0.1, base_dir=self.base))
        self.assertFalse(r['ok'])
        self.assertEqual(r['code'], RR.EXIT_MEMORY)
        self.assertIn('MemoryError', r['error'])

    def test_signal_kill_is_reported(self):
        r = run(RR.run_job('_selftest_kill', {}, poll_s=0.1, base_dir=self.base))
        self.assertFalse(r['ok'])
        self.assertEqual(r['code'], -9)
        self.assertIn('SIGKILL', r['error'])
        self.assertEqual(r['last_progress'], 'selftest: перед SIGKILL')
        text = run(a._research_job('/edge_audit', '_selftest_kill', {}))
        self.assertIsNone(text)
        self.assertIn('SIGKILL', self.sent[-1])

    def test_unknown_task_and_exit_code(self):
        r = run(RR.run_job('no_such_task', {}, poll_s=0.1, base_dir=self.base))
        self.assertFalse(r['ok'])
        self.assertIn('KeyError', r['error'])
        self.assertIn('исключение в расчёте', r['error'])

    def test_describe_exit(self):
        self.assertIn('код возврата 7', RR.describe_exit(7, None, 300, 'x\nboom'))
        self.assertIn('boom', RR.describe_exit(7, None, 300, 'x\nboom'))
        self.assertIn('RESEARCH_MEM_MB=300', RR.describe_exit(RR.EXIT_MEMORY, None, 300))

    def test_registry_covers_all_research_commands(self):
        for t in ('edge_audit', 'of_backtest', 'htf_backtest', 'htf_wide', 'htf_xsmom', 'wide_stop', 'data_probe'):
            self.assertIn(t, RR.TASKS)

    def test_wrappers_use_runner_and_exchange_stays_in_main(self):
        src = open(os.path.join(ROOT, 'async_bot.py'), encoding='utf-8').read()
        for task in ('edge_audit', 'of_backtest', 'htf_backtest', 'htf_wide', 'htf_xsmom', 'wide_stop', 'data_probe'):
            self.assertRegex(src, rf"_research_job\('/[a-z_]+', '{task}'")
        for old in ('research_edge.run_edge_audit(', 'research_of.run_of(', 'research_htf.run_backtest(', 'research_htf.run_wide(',
                    'research_htf.run_xsmom_backtest(', 'research_ws.run_wide_stop(', 'research_probe.run_probe('):
            self.assertNotIn(old, src)
        for prep in ('research_htf.prepare_backtest(exchange', 'research_htf.prepare_wide(exchange', 'research_htf.prepare_xs(exchange',
                     'research_ws.prepare_wide_stop(exchange'):
            self.assertIn(prep, src)                                 # сеть через общий exchange — ДО запуска расчёта, в процессе бота
        self.assertIn('if _htf_running[0]:', src)                    # не больше одного исследовательского процесса

    def test_status_line(self):
        saved = RR.STATE.copy()
        try:
            RR.STATE.update(task=None)
            self.assertEqual(RR.status_line(210), 'исследование: нет, RSS бота 210 МБ')
            RR.STATE.update(task='/edge_audit', started=__import__('time').time() - 180, rss_mb=140.0)
            line = RR.status_line(210)
            self.assertIn('исследование: /edge_audit (3 мин, RSS дочернего 140 МБ', line)
            self.assertIn('RSS бота 210 МБ', line)
        finally:
            RR.STATE.update(saved)


def mem_cache(d, syms, n=2400):
    for i, sym in enumerate(syms):
        x = walk(n, 500 + i, sd=0.004, t0=START)
        V.save_klines(d, sym, '15m', x['ts'], x, ['2023-06'])


def counts(edb, run_id):
    con = sqlite3.connect(edb)
    try:
        return dict(((s, v, e, dr), n) for s, v, e, dr, n in con.execute(
            'SELECT symbol, variant, entry, direction, COUNT(*) FROM edge_trades WHERE run_id=? GROUP BY 1,2,3,4', (run_id,)))
    finally:
        con.close()


class EdgeResume(unittest.TestCase):
    def setUp(self):
        self.db, self.sent = fresh_state()
        self.d = tempfile.mkdtemp(prefix='vision_')
        self.syms = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT']
        mem_cache(self.d, self.syms)
        self.uni = [s[:-4] + '/USDT:USDT' for s in self.syms]

    def _go(self, edb, **kw):
        return run(E.run_edge_audit(None, self.db, self.uni, *FEES[:2], a._smc2_eval, a.get_pivots, progress=a.tg, d=self.d, sync=False, edge_db=edb, **kw))

    def test_interrupted_run_resumes_without_duplicates(self):
        edb_ref = os.path.join(tempfile.mkdtemp(), 'ref.db')
        text_ref, res_ref = self._go(edb_ref, inline=True)
        ref = counts(edb_ref, res_ref['run_id'])
        self.assertGreater(sum(ref.values()), 50)

        edb = os.path.join(tempfile.mkdtemp(), 'edge.db')
        orig_stream, calls = E.r1_symbol_stream, {'n': 0}

        def flaky(sym, arr, fees, ev, gp, funding, sink):
            calls['n'] += 1
            if calls['n'] == 2:                                      # 2-й символ: часть сделок уже записана — и прерывание
                def half(var, tr):
                    sink(var, tr)
                    if var == 'V1':
                        raise RuntimeError('прерывание посреди символа')
                return orig_stream(sym, arr, fees, ev, gp, funding, half)
            return orig_stream(sym, arr, fees, ev, gp, funding, sink)
        E.r1_symbol_stream = flaky
        try:
            with self.assertRaises(RuntimeError):
                self._go(edb, inline=True)
        finally:
            E.r1_symbol_stream = orig_stream
        con = sqlite3.connect(edb)
        run_id = con.execute("SELECT run_id FROM edge_runs WHERE kind='r1'").fetchone()[0]
        done = [r[0] for r in con.execute('SELECT symbol FROM edge_progress WHERE run_id=? AND done=1', (run_id,))]
        partial = con.execute('SELECT COUNT(*) FROM edge_trades WHERE run_id=? AND symbol=?', (run_id, 'ETHUSDT')).fetchone()[0]
        con.close()
        self.assertEqual(done, ['BTCUSDT'])                          # контрольная точка только у завершённого символа
        self.assertEqual(partial, 0)                                 # частичные сделки прерванного символа откатились

        # повторный запуск: R0 не пересчитывается, R1 продолжает с ETHUSDT
        r0_calls, orig_r0 = [], E.process_symbol_r0
        E.process_symbol_r0 = lambda *x, **k: (r0_calls.append(x[2]), orig_r0(*x, **k))[1]
        n_before = len(self.sent)
        try:
            text, res = self._go(edb, inline=True)
        finally:
            E.process_symbol_r0 = orig_r0
        self.assertEqual(r0_calls, [])
        self.assertEqual(res['run_id'], run_id)
        self.assertTrue(any('продолжаю прогон' in m and run_id in m for m in self.sent[n_before:]))
        self.assertFalse(any(m.startswith('R1 BTCUSDT') for m in self.sent[n_before:]))     # BTC не пересчитывался
        self.assertTrue(any(m.startswith('R1 ETHUSDT') for m in self.sent[n_before:]))
        got = counts(edb, run_id)
        self.assertEqual(got, ref)                                   # те же сделки, что без прерывания
        con = sqlite3.connect(edb)
        dup = con.execute('SELECT COUNT(*) - (SELECT COUNT(*) FROM (SELECT DISTINCT run_id,symbol,variant,entry,direction,open_ts,close_ts,r_net FROM edge_trades)) '
                          'FROM edge_trades').fetchone()[0]
        status = con.execute('SELECT status FROM edge_runs WHERE run_id=?', (run_id,)).fetchone()[0]
        con.close()
        self.assertEqual(dup, 0)
        self.assertEqual(status, 'done')
        self.assertEqual(text.split('ВЕРДИКТ R1')[1], text_ref.split('ВЕРДИКТ R1')[1])        # итоговый отчёт идентичен

    def test_r0_reused_for_same_seed(self):
        edb = os.path.join(tempfile.mkdtemp(), 'edge.db')
        self._go(edb, inline=True)
        calls, orig = [], E.process_symbol_r0
        E.process_symbol_r0 = lambda *x, **k: (calls.append(1), orig(*x, **k))[1]
        try:
            self._go(edb, inline=True)
        finally:
            E.process_symbol_r0 = orig
        self.assertEqual(calls, [])
        con = sqlite3.connect(edb)
        self.assertEqual(con.execute("SELECT COUNT(*) FROM edge_trades WHERE variant='R0'").fetchone()[0], len(self.syms) * E.N_RANDOM)
        con.close()

    def test_new_run_after_finished_run(self):
        edb = os.path.join(tempfile.mkdtemp(), 'edge.db')
        _, r1 = self._go(edb, inline=True)
        _, r2 = self._go(edb, inline=True)
        self.assertNotEqual(r1['run_id'], r2['run_id'])              # завершённый прогон не «продолжается» — новые данные дают новый прогон

    def test_stop_at_low_disk_before_run(self):
        orig = V.free_mb
        V.free_mb = lambda path: 120.0
        try:
            edb = os.path.join(tempfile.mkdtemp(), 'edge.db')
            text, res = self._go(edb, inline=True)
        finally:
            V.free_mb = orig
        self.assertEqual(res['r0'], 'DISK')
        self.assertIn('свободно 120 МБ < 200 МБ', text)
        self.assertIn('оценка размера edge_trades', text)
        self.assertFalse(os.path.exists(edb))

    def test_size_estimate(self):
        self.assertAlmostEqual(E.estimate_edge_mb(16), 16 * (E.EST_ROWS_PER_SYMBOL + E.N_RANDOM) * E.EST_BYTES_PER_ROW / 1e6)
        self.assertLess(E.estimate_edge_mb(16), 100)

    def test_lean_loader_equals_old_values(self):
        old = E.load_series(self.d, 'BTCUSDT')
        new = E.load_series_lean(self.d, 'BTCUSDT')
        for k in ('ts', 'o', 'h', 'l', 'c', 'v'):
            self.assertEqual(list(old[k]), list(new[k]), k)
        self.assertNotIn('tbv', new)
        self.assertEqual(str(new['ts'].dtype), 'int64')
        self.assertEqual(str(new['o'].dtype), 'float32')

    def test_no_trades_accumulation_in_run_edge_audit(self):
        """В run_edge_audit нет списков сделок по всем символам: сделки уходят в SQLite внутри process_symbol_*."""
        src = open(os.path.join(ROOT, 'research_edge.py'), encoding='utf-8').read()
        body = src[src.index('async def run_edge_audit('):]
        self.assertNotIn('all_tr', body)
        self.assertNotIn('.extend(', body)
        self.assertNotIn('arrs[', body)


def synth_trades(n=3000, seed=4):
    rnd = random.Random(seed)
    out = []
    t0 = int(datetime(2021, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
    for i in range(n):
        et = t0 + rnd.randrange(0, 5 * 365 * 86_400_000)
        r = rnd.choice([-1.0, 2.0, rnd.uniform(-1, 2)])
        why = 'SL' if r == -1.0 else ('TP' if r == 2.0 else 'TIMEOUT')
        out.append({'etype': rnd.choice(['MKT', 'RT']), 'long': rnd.random() < 0.5, 'entry_ts': et, 'exit_ts': et + rnd.randrange(900_000, 4 * 3_600_000),
                    'Rg': r + 0.05, 'R': r - 0.04, 'reason': why})
    return out


class SqlVsMemory(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.trades = synth_trades()
        cls.edb = os.path.join(tempfile.mkdtemp(), 'e.db')
        E.init_edge_db(cls.edb)
        cls.con = sqlite3.connect(cls.edb, isolation_level=None)
        E.insert_trades(cls.con, 'rX', 'XUSDT', 'V0', cls.trades)
        E.insert_trades(cls.con, 'rX', 'XUSDT', 'R0', cls.trades)
        cls.btc = {f'{y}-{m:02d}': (0.04 if (y + m) % 2 else -0.04) for y in range(2021, 2027) for m in range(1, 13)}

    @classmethod
    def tearDownClass(cls):
        cls.con.close()

    def _close(self, x, y, key):
        if isinstance(x, float) and isinstance(y, float) and (x == float('inf') or y == float('inf')):
            self.assertEqual(x, y, key)
        else:
            self.assertAlmostEqual(x, y, places=9, msg=key)

    def test_stats_all_filters(self):
        cut = E._ms(E.OOS_START)
        sel = [
            (dict(), lambda t: True),
            (dict(entry='MKT'), lambda t: t['etype'] == 'MKT'),
            (dict(direction='Long'), lambda t: t['long']),
            (dict(entry='RT', direction='Short'), lambda t: t['etype'] == 'RT' and not t['long']),
            (dict(ts_hi=cut), lambda t: t['entry_ts'] < cut),
            (dict(ts_lo=cut, direction='Long'), lambda t: t['entry_ts'] >= cut and t['long']),
        ]
        for kw, f in sel:
            sql = E.sql_stats(self.con, 'rX', 'V0', **kw)
            mem = E.stats([t for t in self.trades if f(t)])
            self.assertEqual(set(sql), set(mem))
            for k in mem:
                self._close(sql[k], mem[k], f'{kw}:{k}')

    def test_years_and_months(self):
        mem = E.by_year(self.trades)
        sql = E.sql_by_year(self.con, 'rX', 'V0')
        self.assertEqual(set(sql), set(mem))
        for y in mem:
            self.assertEqual(sql[y][0], mem[y][0])
            self.assertAlmostEqual(sql[y][1], mem[y][1], places=9)
        for ts_lo in (None, E._ms(E.OOS_START)):
            ms_mem = E.month_split([t for t in self.trades if ts_lo is None or t['entry_ts'] >= ts_lo], self.btc)
            ms_sql = E.sql_month_split(self.con, 'rX', 'V0', None, None, self.btc, ts_lo=ts_lo)
            for side in ('up', 'down'):
                self.assertEqual(ms_sql[side][0], ms_mem[side][0])
                self.assertAlmostEqual(ms_sql[side][1], ms_mem[side][1], places=9)

    def test_cell_verdicts_equal(self):
        for et in E.ENTRIES:
            for side in E.SIDES:
                tr = [t for t in self.trades if t['etype'] == et and t['long'] == (side == 'Long')]
                m = E.cell_verdict(tr, self.btc)
                q = E.cell_verdict_sql(self.con, 'rX', 'V0', et, side, self.btc)
                self.assertEqual(q['status'], m['status'])
                self.assertEqual(list(q['checks']), list(m['checks']))
                self.assertEqual(list(q['checks'].values()), list(m['checks'].values()))
                for part in ('is', 'oos'):
                    for k in m[part]:
                        self._close(q[part][k], m[part][k], f'{et}{side}{part}{k}')
                self.assertEqual(q['years'].keys(), E.by_year(tr).keys())

    def test_r0_summary_equal(self):
        mem = E.summary_from_trades(self.trades)
        sql = E.r0_summary_sql(self.con, 'rX')
        for key in ('all', 'long', 'short'):
            for k in mem[key]:
                self._close(sql[key][k], mem[key][k], f'{key}:{k}')
        self.assertEqual(sql['years'].keys(), mem['years'].keys())
        for y in mem['years']:
            for i in range(3):
                self.assertAlmostEqual(sql['years'][y][i], mem['years'][y][i], places=9)
        self.assertEqual(E.build_r0(mem, {}, [], FEES)[1], E.build_r0(sql, {}, [], FEES)[1])
        self.assertEqual(E.build_r0(mem, {}, [], FEES)[0], E.build_r0(sql, {}, [], FEES)[0])      # один и тот же текст отчёта

    def test_insert_chunks(self):
        edb = os.path.join(tempfile.mkdtemp(), 'c.db')
        E.init_edge_db(edb)
        con = sqlite3.connect(edb, isolation_level=None)
        saved = E.INSERT_CHUNK
        E.INSERT_CHUNK = 7
        try:
            E.insert_trades(con, 'r', 'S', 'V1', self.trades[:100])
        finally:
            E.INSERT_CHUNK = saved
        self.assertEqual(con.execute('SELECT COUNT(*) FROM edge_trades').fetchone()[0], 100)


class StartupAndMemory(unittest.TestCase):
    FLAGS = ('SMC_LIVE', 'SMC_SHADOW_ENABLED', 'SA_ENABLED', 'SA_LIVE', 'RSI_ENABLED', 'PB_ENABLED', 'PB_LIVE', 'MOMENTUM_ENABLED', 'MOMENTUM_LIVE',
             'RB_ENABLED', 'ORB_ENABLED', 'PO3_ENABLED', 'SMC2_ENABLED', 'ORB_RT_ENABLED')

    def setUp(self):
        self.db, self.sent = fresh_state()
        self.saved = {f: getattr(a, f) for f in self.FLAGS}

    def tearDown(self):
        for f, v in self.saved.items():
            setattr(a, f, v)

    def _flags(self, **on):
        for f in self.FLAGS:
            setattr(a, f, f in on)

    def test_shows_code_version_and_only_enabled(self):
        self._flags(PO3_ENABLED=1, ORB_RT_ENABLED=1)
        t = a.startup_text()
        self.assertIn(a.CODE_VERSION, t)
        self.assertIn('Живые стратегии: нет (живые входы выключены)', t)
        self.assertIn('Shadow (виртуальные): SMC_SIG (замер вместо живых входов, SMC_LIVE=false), PO3, ORB_RT', t)
        for dead in ('RSI', 'SA', 'SMC2', 'RB', 'ORB,', 'PB', 'MOM'):
            self.assertNotIn(dead, t.split('Shadow')[0] + t.split('Shadow')[1].split('\n')[0].replace('ORB_RT', ''))
        for old in ('v92', 'PROP', 'Mean Reversion', 'VWAP MR', 'Smart Money Concepts', 'Unified'):
            self.assertNotIn(old, t)
        self.assertIn(f'Риск: {a.RISK_PER_TRADE * 100:.2f}%/сделку', t)
        self.assertIn(f'Плечо: {a.LEVERAGE}x', t)
        self.assertIn(f'{a.SESSION_START_UTC}–{a.SESSION_END_UTC} UTC', t)
        self.assertIn(f'дневной DD > {a.DAILY_DD_LIMIT_PCT:.1f}%', t)
        self.assertIn(f'общий > {a.MAX_DD_LIMIT_PCT:.1f}%', t)

    def test_live_and_shadow_separated(self):
        self._flags(SMC_LIVE=1, SA_ENABLED=1, RSI_ENABLED=1, PB_ENABLED=1, RB_ENABLED=1)
        live, shadow = a.active_strategies()
        self.assertEqual(live, ['SMC', 'RSI'])
        self.assertEqual(shadow, ['SA', 'PB', 'RB'])
        self._flags(SMC_LIVE=1, SA_ENABLED=1, SA_LIVE=1, RSI_ENABLED=1, PB_ENABLED=1, PB_LIVE=1)
        live, shadow = a.active_strategies()
        self.assertEqual((live, shadow), (['SMC', 'SA', 'RSI', 'PB'], []))
        t = a.startup_text()
        self.assertIn('Живые стратегии: SMC, SA, RSI, PB', t)
        self.assertIn('Shadow (виртуальные): нет', t)

    def test_no_old_version_constant(self):
        import ast
        tree = ast.parse(open(os.path.join(ROOT, 'async_bot.py'), encoding='utf-8').read())
        names = {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)}
        self.assertNotIn('BOT_VERSION', names)                       # версия везде — CODE_VERSION
        self.assertIn('CODE_VERSION', names)
        literals = [n.value for n in ast.walk(tree) if isinstance(n, ast.Constant) and isinstance(n.value, str)]
        self.assertFalse([x for x in literals if 'Unified SMC+RSI' in x])

    def test_session_constants_unchanged_behavior(self):
        self.assertEqual((a.SESSION_START_UTC, a.SESSION_END_UTC), ('06:30', '17:00'))

    def test_memory_alert_once_per_hour(self):
        saved = a._rr.rss_mb
        a._rr.rss_mb = lambda pid=None: 450.0
        t0 = 1_800_000_000.0
        try:
            self.assertTrue(any(k == 'mem' for k, _ in a.health_conditions(t0)))
            self.assertEqual(run(a.health_alert_tick(t0)).count('mem'), 1)
            self.assertEqual(run(a.health_alert_tick(t0 + 600)), [])
            self.assertEqual(run(a.health_alert_tick(t0 + 3601)).count('mem'), 2 - 1)
            self.assertEqual(sum('RSS процесса бота 450 МБ' in m for m in self.sent), 2)
            a._rr.rss_mb = lambda pid=None: 399.0
            self.assertFalse(any(k == 'mem' for k, _ in a.health_conditions(t0 + 9999)))
        finally:
            a._rr.rss_mb = saved

    def test_mem_alert_env_default(self):
        self.assertEqual(a.MEM_ALERT_MB, float(os.getenv('MEM_ALERT_MB', '400')))

    def test_status_command_shows_research_and_rss(self):
        from test_v96_channels import FakeHttp
        saved = (a.TOKEN, a.CHAT_ID, a.http, a._tg_offset, a._rr.rss_mb)
        a.TOKEN, a.CHAT_ID, a._tg_offset = 'x', 123, 0
        a._rr.rss_mb = lambda pid=None: 212.0
        a.http = FakeHttp(['/status'], 123)
        try:
            run(a.check_tg_commands())
        finally:
            a.TOKEN, a.CHAT_ID, a.http, a._tg_offset, a._rr.rss_mb = saved
        self.assertTrue(any('исследование: нет, RSS бота 212 МБ' in m for m in self.sent), self.sent)


if __name__ == '__main__':
    unittest.main()
