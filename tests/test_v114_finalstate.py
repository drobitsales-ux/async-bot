"""v114: состояния флага финала (BOT_SPEC §2.43.17) и проверка утечки (§2.43.15): free → running → done | tech_fail_no_output, ровно одна повторная попытка, legacy-ключ v113 как зависший running,
audit находит маркеры итогового отчёта и не загрязняет сам себя, при сбое после открытия LOCKBOX в БД не попадает ни одна величина расчёта, память финала при RESEARCH_MEM_MB=300."""
import asyncio
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest

import _common
import battery_fixtures as BF
import research_factory as RF
import research_finalstate as FS
import research_h2final as H2
import research_ma_lib as L
import research_runner as RR

ROOT = _common.ROOT
NAME = 'final_h2'


def run(c):
    return asyncio.run(c)


def spec_file(k_text):
    p = os.path.join(tempfile.mkdtemp(), 'SPEC.md')
    with open(p, 'w', encoding='utf-8') as f:
        f.write(H2.SPEC_K_KEY + ' ' + k_text + '\n')
    return p


class StateMachine(unittest.TestCase):
    def setUp(self):
        self.d = tempfile.mkdtemp()

    def test_happy_path_and_done_is_final(self):
        self.assertEqual(FS.get(NAME, self.d)['state'], FS.FREE)
        self.assertIsNone(FS.begin(NAME, self.d, 2))                        # повтор до первой попытки невозможен
        self.assertEqual(FS.begin(NAME, self.d, 1)['state'], FS.RUNNING)
        self.assertIsNone(FS.begin(NAME, self.d, 1))                        # второй запуск параллельно — отказ (атомарно)
        self.assertEqual(FS.finish_done(NAME, self.d)['state'], FS.DONE)
        self.assertIsNone(FS.begin(NAME, self.d, 1))
        self.assertIsNone(FS.begin(NAME, self.d, 2))

    def test_exactly_one_retry_then_final_failure(self):
        FS.begin(NAME, self.d, 1)
        self.assertEqual(FS.fail_no_output(NAME, self.d, error='MemoryError')['state'], FS.TECH_FAIL_NO_OUTPUT)
        self.assertEqual(FS.begin(NAME, self.d, 2)['state'], FS.RUNNING)
        self.assertIsNone(FS.begin(NAME, self.d, 2))
        self.assertEqual(FS.fail_no_output(NAME, self.d, error='MemoryError')['state'], FS.TECH_FAIL_FINAL)
        self.assertIsNone(FS.begin(NAME, self.d, 2))                        # третьей попытки нет
        self.assertIsNone(FS.begin(NAME, self.d, 1))
        h = FS.get(NAME, self.d)['history']
        self.assertEqual([x['to'] for x in h], ['running', 'tech_fail_no_output', 'running', 'tech_fail_final'])

    def test_second_attempt_can_finish_done(self):
        FS.begin(NAME, self.d, 1)
        FS.fail_no_output(NAME, self.d)
        FS.begin(NAME, self.d, 2)
        self.assertEqual(FS.finish_done(NAME, self.d)['state'], FS.DONE)

    def test_legacy_keys_map_to_states(self):
        leg = {'started': 'final_h2_started', 'done': 'final_h2_done'}
        RF.kv_set('final_h2_started', {'t': time.time() - 7200, 'kind': 'final_h2'}, self.d)
        st = FS.get(NAME, self.d, leg)
        self.assertEqual((st['state'], st['legacy'], st['attempt']), (FS.RUNNING, True, 1))
        self.assertTrue(FS.is_stale(st))                                    # старше 30 мин, pid неизвестен
        self.assertFalse(FS.is_stale(dict(st, t=time.time())))              # свежий — не зависший
        self.assertFalse(FS.is_stale(dict(st, t=time.time() - 7200, pid=os.getpid())))   # процесс жив — не зависший
        moved = FS.reconcile_stale(NAME, self.d, leg, note='тест')
        self.assertEqual(moved['state'], FS.TECH_FAIL_NO_OUTPUT)
        self.assertEqual(FS.begin(NAME, self.d, 2, leg)['state'], FS.RUNNING)
        d2 = tempfile.mkdtemp()
        RF.kv_set('final_h2_done', True, d2)
        self.assertEqual(FS.get(NAME, d2, leg)['state'], FS.DONE)


class H2Flow(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.old_reps = H2.K_REPS
        H2.K_REPS = 3
        cls.bdir = tempfile.mkdtemp()
        BF.write_battery(cls.bdir, n_syms=90, delist_n=3, list_hi=1900)
        cls.k = H2.compute_k({'bdir': cls.bdir}, reps=3)['k']
        cls.spec = spec_file('%.3f' % cls.k)

    @classmethod
    def tearDownClass(cls):
        H2.K_REPS = cls.old_reps

    def setUp(self):
        self.fdir = tempfile.mkdtemp()
        self.scan = tempfile.mkdtemp()
        L._lockbox_session_state['opened'] = False
        self.orig_eval = H2.lockbox_eval

    def tearDown(self):
        H2.lockbox_eval = self.orig_eval

    def p(self, **kw):
        return dict({'bdir': self.bdir, 'fdir': self.fdir, 'spec_path': self.spec, 'scan_dirs': [self.scan]}, **kw)

    def boom(self):
        def f(*a, **kw):
            raise MemoryError('имитация')
        H2.lockbox_eval = f

    def test_first_failure_then_exactly_one_retry_success_report_saved_before_done(self):
        self.boom()
        t1 = run(H2.run_final_h2(self.p(mode='confirm')))
        self.assertIn('tech_fail_no_output', t1)
        self.assertEqual(FS.get(NAME, self.fdir)['state'], FS.TECH_FAIL_NO_OUTPUT)
        self.assertIn('уже взят', run(H2.run_final_h2(self.p(mode='confirm'))))     # confirm повторно нельзя
        self.assertIn('ОДНА повторная попытка', run(H2.run_final_h2(self.p(mode='show'))))
        H2.lockbox_eval = self.orig_eval
        L._lockbox_session_state['opened'] = False
        t2 = run(H2.run_final_h2(self.p(mode='retry')))
        self.assertIn('РЕЗУЛЬТАТ', t2)
        st = FS.get(NAME, self.fdir)
        self.assertEqual((st['state'], st['attempt']), (FS.DONE, 2))
        self.assertEqual(RF.kv_get(H2.KV_REPORT, self.fdir), t2)            # отчёт в БД записан до done
        self.assertEqual(run(H2.run_final_h2(self.p(mode='report'))), t2)
        self.assertIn('отказ', run(H2.run_final_h2(self.p(mode='retry'))))  # после done повтора нет

    def test_two_failures_close_the_final(self):
        self.boom()
        run(H2.run_final_h2(self.p(mode='confirm')))
        L._lockbox_session_state['opened'] = False
        t2 = run(H2.run_final_h2(self.p(mode='retry')))
        self.assertIn('закрыт как технически непроверяемый', t2)
        self.assertEqual(FS.get(NAME, self.fdir)['state'], FS.TECH_FAIL_FINAL)
        t3 = run(H2.run_final_h2(self.p(mode='retry')))
        self.assertIn('отказ', t3)                                           # третьей попытки нет
        self.assertIn('технически непроверяемый', t3)
        self.assertIsNone(RF.kv_get(H2.KV_RESULT, self.fdir))
        self.assertIsNone(RF.kv_get(H2.KV_EXHAUSTED, self.fdir))

    def test_legacy_v113_flag_is_stale_running_and_retry_goes_through(self):
        RF.kv_set('final_h2_started', {'t': time.time() - 7200, 'kind': 'final_h2', 'k': self.k}, self.fdir)     # состояние прода после сбоя 2026-10-29
        self.assertIn('уже взят', run(H2.run_final_h2(self.p(mode='confirm'))))
        self.assertIn('ОДНА повторная попытка', run(H2.run_final_h2(self.p(mode='show'))))
        t = run(H2.run_final_h2(self.p(mode='retry')))
        self.assertIn('РЕЗУЛЬТАТ', t)
        h = FS.get(NAME, self.fdir)['history']
        self.assertEqual([x['to'] for x in h], ['tech_fail_no_output', 'running', 'done'])

    def test_fresh_running_blocks_retry(self):
        FS.begin(NAME, self.fdir, 1)                                         # pid текущего процесса жив, время свежее
        self.assertIn('подождите', run(H2.run_final_h2(self.p(mode='retry'))))
        self.assertEqual(FS.get(NAME, self.fdir)['state'], FS.RUNNING)

    def test_retry_refused_when_audit_finds_output(self):
        self.boom()
        run(H2.run_final_h2(self.p(mode='confirm')))
        H2.lockbox_eval = self.orig_eval
        with open(os.path.join(self.scan, 'leak.log'), 'w', encoding='utf-8') as f:
            f.write('что-то H2-D3\n' + H2.MARKERS[1] + '\n')
        t = run(H2.run_final_h2(self.p(mode='retry')))
        self.assertIn('УТЕЧКА', t)
        self.assertIn('повтор ЗАПРЕЩЁН', t)
        self.assertEqual(FS.get(NAME, self.fdir)['state'], FS.TECH_FAIL_NO_OUTPUT)    # состояние не тронуто

    def test_audit_scans_sqlite_and_files_and_does_not_contaminate_itself(self):
        clean = H2.leak_audit(self.p(), self.fdir, self.bdir)
        self.assertTrue(clean['clean'])
        txt = H2.audit_text(clean)
        self.assertIn('утечки нет', txt)
        for m in H2.MARKERS:
            self.assertNotIn(m, txt)                                         # отчёт audit не содержит маркеров целиком
        for m in H2.MARKERS:
            self.assertNotIn(m, '\n'.join(H2.spec_summary()))                # показ k/критериев не содержит маркеров
        db = os.path.join(self.scan, 'x.db')
        con = sqlite3.connect(db)
        con.execute('CREATE TABLE htf_reports (id INTEGER, text TEXT)')
        con.execute('INSERT INTO htf_reports VALUES (1, ?)', ('шапка\n' + H2.MARKERS[3] + ' 5',))
        con.commit()
        con.close()
        res = H2.leak_audit(self.p(), self.fdir, self.bdir)
        self.assertFalse(res['clean'])
        self.assertTrue(any('htf_reports.text' in w for w, _i, _n in res['hits']))
        self.assertIn('УТЕЧКА', H2.audit_text(res))

    def test_crypto4h_report_format_is_not_a_hit_but_h2_report_is(self):
        """Первый audit на проде (2026-10-29 20:30) дал ложные находки в отчёте /final_crypto4h от 2026-10-25 (§2.43.18): общие маркеры считаются находкой только вместе с «H2-D3»."""
        c4 = ('шапка\n<b>РЕЗУЛЬТАТ: НЕ ПРОЙДЕНО</b> (дней 600, k = 1.151)\n  mean при издержках ×2 = 1.55 б.п./день > 0: да\n'
              '  месяцы BTC (индекс месяца UTC → доходность): 2025-01: +11.8%')
        with open(os.path.join(self.scan, 'htf_backtest_2026-10-25_2046.txt'), 'w', encoding='utf-8') as f:
            f.write(c4)
        db = os.path.join(self.scan, 'trades.db')
        con = sqlite3.connect(db)
        con.execute('CREATE TABLE htf_reports (id INTEGER, text TEXT)')
        con.execute('INSERT INTO htf_reports VALUES (1, ?)', (c4,))
        con.commit()
        con.close()
        res = H2.leak_audit(self.p(), self.fdir, self.bdir)
        self.assertTrue(res['clean'], res['hits'])
        con = sqlite3.connect(db)
        con.execute('INSERT INTO htf_reports VALUES (2, ?)', ('<b>РЕЗУЛЬТАТ: ПРОЙДЕНО</b> H2-D3 (дней 90)',))
        con.commit()
        con.close()
        res2 = H2.leak_audit(self.p(), self.fdir, self.bdir)
        self.assertFalse(res2['clean'])
        self.assertEqual([i for _w, i, _n in res2['hits']], [1])
        with open(os.path.join(self.scan, 'spec.log'), 'w', encoding='utf-8') as f:
            f.write('финал H2-D3 на LOCKBOX (2025+)')                       # специфичный маркер — находка без контекста
        self.assertIn(6, [i for _w, i, _n in H2.leak_audit(self.p(), self.fdir, self.bdir)['hits']])

    def test_specific_markers_do_not_occur_in_other_finals_source(self):
        specific = [m for i, m in enumerate(H2.MARKERS) if i not in H2.GENERIC]
        for f in ('research_ffinal.py', 'research_factory.py', 'research_battery.py'):
            src = open(os.path.join(ROOT, f), encoding='utf-8').read()
            for m in specific:
                self.assertNotIn(m, src, f)
        ffinal = open(os.path.join(ROOT, 'research_ffinal.py'), encoding='utf-8').read()
        for i in H2.GENERIC:
            self.assertIn(H2.MARKERS[i].split(':')[0], ffinal)                  # общие маркеры действительно есть в формате crypto4h

    def test_audit_lists_leftover_job_dirs(self):
        jd = os.path.join(self.scan, 'final_h2_abc123')
        os.makedirs(jd)
        with open(os.path.join(jd, 'progress.jsonl'), 'w', encoding='utf-8') as f:
            f.write('{"msg": "LOCKBOX открыт"}\n')
        txt = H2.audit_text(H2.leak_audit(self.p(), self.fdir, self.bdir))
        self.assertIn('final_h2_abc123', txt)
        self.assertIn('progress.jsonl', txt)

    def test_audit_after_success_sees_the_result_itself(self):
        run(H2.run_final_h2(self.p(mode='confirm')))
        res = H2.leak_audit(self.p(), self.fdir, self.bdir)
        self.assertTrue(res['kv'][H2.KV_RESULT] and res['kv'][H2.KV_REPORT])

    def test_crash_after_lockbox_opened_writes_no_computed_value(self):
        """Шаг 0 (статически подтверждённое): сбой сразу после открытия LOCKBOX — в БД только переходы состояния и текст ошибки, ни одной величины расчёта."""
        def kv_all():
            con = RF._con(self.fdir)
            try:
                return dict(con.execute('SELECT k, v FROM kv').fetchall())
            finally:
                con.close()
        seen = []

        async def prog(m):
            seen.append(m)
        orig = H2.B.load_panel

        def hit_after_open(d, mode='pre', session=None, *a, **kw):
            if mode == 'full':
                raise MemoryError('превышен лимит памяти')
            return orig(d, mode, session, *a, **kw)
        H2.B.load_panel = hit_after_open
        try:
            run(H2.run_final_h2(self.p(mode='confirm'), progress=prog))
        finally:
            H2.B.load_panel = orig
        kv = kv_all()
        self.assertEqual(set(kv), {'final_h2_k', 'final_h2_started', 'finalstate:final_h2', 'final_h2_crash'})
        blob = ' '.join(kv.values())
        for token in ('mean', 'sharpe', 't_adj', 'dd', 'stress', 'groups', 'years', 'listings'):
            self.assertNotIn(token, blob)
        self.assertEqual(seen[-1].split(' (')[0], 'LOCKBOX открыт')            # последний прогресс — то же, что видел владелец
        self.assertEqual(json.loads(kv['final_h2_crash'])['error'].split(':')[0], 'MemoryError')


class Memory(unittest.TestCase):
    def _run(self, n):
        d = tempfile.mkdtemp()
        BF.write_battery(d, n_syms=n, first_day='2020-01-01', last_day='2026-09-30', delist_n=20, list_hi=2400)
        code = (
            "import resource, sys\n"
            "lim = 300 * 1024 * 1024\n"
            "resource.setrlimit(resource.RLIMIT_AS, (lim, lim))\n"
            f"sys.path[:0] = [{ROOT!r}]\n"
            "import research_h2final as H2, research_battery as RB, research_ma_lib as L\n"
            f"out = H2.lockbox_eval({d!r}, RB.fees_of({{}}), 1.122, L.open_lockbox_session(final=True))\n"
            "st = dict(l.split(':') for l in open('/proc/self/status').read().splitlines() if l.startswith(('VmPeak', 'VmHWM')))\n"
            "print('OK', out['ev']['n'], st['VmPeak'].split()[0], st['VmHWM'].split()[0])\n")
        env = dict(os.environ, **RR.THREAD_ENV)
        r = subprocess.run([sys.executable, '-c', code], capture_output=True, text=True, env=env, timeout=600)
        self.assertIn('OK 638', r.stdout, r.stderr[-400:])
        _ok, _n, peak_kb, hwm_kb = r.stdout.split()
        return int(peak_kb) / 1024, int(hwm_kb) / 1024

    def test_391_symbols_fit_300mb(self):
        peak, rss = self._run(391)
        print(f'\n[391 символов] пик адресного пространства {peak:.0f} МБ, пик RSS {rss:.0f} МБ (лимит 300)')
        self.assertLess(peak, 250)

    def test_650_symbols_fit_300mb(self):
        peak, rss = self._run(650)                                           # воспроизведение прод-падения (старый код: MemoryError при RSS ≈ 214 МБ)
        print(f'\n[650 символов] пик адресного пространства {peak:.0f} МБ, пик RSS {rss:.0f} МБ (лимит 300)')
        self.assertLess(peak, 250)

    def test_thread_env_limits_are_applied_to_child(self):
        for k in ('OPENBLAS_NUM_THREADS', 'OMP_NUM_THREADS', 'MKL_NUM_THREADS', 'NUMEXPR_NUM_THREADS'):
            self.assertEqual(RR.THREAD_ENV[k], '1')
        self.assertEqual(RR.THREAD_ENV['MALLOC_ARENA_MAX'], '2')
        src = open(os.path.join(ROOT, 'research_runner.py'), encoding='utf-8').read()
        self.assertIn('env = dict(os.environ, **THREAD_ENV', src)           # дочерний процесс запускается с этим окружением


class Gate(unittest.TestCase):
    def test_command_modes_are_parsed(self):
        a = _common.a
        calls = []

        async def fake(cmd, task, extra=None, need_universe=True):
            calls.append(extra['mode'])
        old = a.battery_job
        a.battery_job = fake
        try:
            for args in ([], ['confirm'], ['retry', 'confirm'], ['retry'], ['audit'], ['report'], ['CONFIRM'], ['что-то']):
                run(a.final_h2_gate(args))
        finally:
            a.battery_job = old
        self.assertEqual(calls, ['show', 'confirm', 'retry', 'show', 'audit', 'report', 'confirm', 'show'])
        src = open(os.path.join(ROOT, 'async_bot.py'), encoding='utf-8').read()
        self.assertIn("cmd == '/final_h2' or cmd.startswith('/final_h2 ')", src)

    def test_completed_finals_untouched(self):
        for f in ('research_ffinal.py',):
            self.assertNotIn('research_finalstate', open(os.path.join(ROOT, f), encoding='utf-8').read())
        self.assertNotIn('finalstate', open(os.path.join(ROOT, 'research_factory.py'), encoding='utf-8').read())


if __name__ == '__main__':
    unittest.main()
