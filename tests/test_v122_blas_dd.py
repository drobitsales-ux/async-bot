"""v122: потоки BLAS в дочерних процессах (5 переменных = 1 ДО запуска интерпретатора), сообщение о нехватке адресного пространства для BLAS, строка «Окружение»,
dd_update на «чистом» состоянии `_dd` без KeyError и без WARNING при equity 0 (BOT_SPEC §2.54)."""
import asyncio
import json
import os
import subprocess
import sys
import tempfile
import unittest

import _common
import research_runner as RR
from _common import a

ROOT = _common.ROOT


def run(c):
    return asyncio.run(c)


class ChildEnv(unittest.TestCase):
    def test_five_blas_variables_are_one_even_if_service_env_says_otherwise(self):
        saved = {k: os.environ.get(k) for k in RR.BLAS_VARS}
        try:
            for k in RR.BLAS_VARS:
                os.environ[k] = '8'                                                        # ENV сервиса (Render) с другим значением
            env = RR.child_env(300)
        finally:
            for k, v in saved.items():
                os.environ.pop(k, None) if v is None else os.environ.__setitem__(k, v)
        self.assertEqual(len(RR.BLAS_VARS), 5)
        for k in ('OPENBLAS_NUM_THREADS', 'OMP_NUM_THREADS', 'MKL_NUM_THREADS', 'NUMEXPR_NUM_THREADS', 'VECLIB_MAXIMUM_THREADS'):
            self.assertEqual(env[k], '1', k)
        self.assertEqual((env['RESEARCH_MEM_MB'], env['BOT_RESEARCH_CHILD']), ('300', '1'))

    def test_real_child_process_sees_ones_before_numpy(self):
        base = tempfile.mkdtemp()
        os.environ['OPENBLAS_NUM_THREADS'] = '8'
        try:
            r = run(RR.run_job('_selftest_env', {}, base_dir=base, poll_s=0.1))
        finally:
            os.environ.pop('OPENBLAS_NUM_THREADS', None)
        self.assertTrue(r['ok'], r['error'])
        got = json.loads(r['text'])
        for k in RR.BLAS_VARS:
            self.assertEqual(got[k], '1', k)
        self.assertEqual(got['MALLOC_ARENA_MAX'], '2')

    def test_numpy_in_child_uses_one_thread_and_fits_address_space(self):
        code = ("import numpy as np, re\n"
                "a = np.random.rand(1500, 1500); b = a @ a\n"
                "s = open('/proc/self/status').read()\n"
                "print(re.search(r'VmPeak:\\s+(\\d+)', s).group(1))\n")
        r = subprocess.run([sys.executable, '-c', code], capture_output=True, text=True, env=RR.child_env(300), timeout=120)
        self.assertEqual(r.returncode, 0, r.stderr[-300:])
        self.assertLess(int(r.stdout.strip()) / 1024, 250)                                   # с 1 потоком BLAS адресное пространство укладывается в лимит

    def test_status_line_and_text(self):
        self.assertEqual(RR.blas_threads_text(), 'потоки BLAS в дочерних процессах: 1')
        self.assertIn('потоки BLAS в дочерних процессах: 1', a.runtime_versions_text())
        self.assertIn('потоки BLAS в дочерних процессах: 1', a.status_text())


class BlasMessage(unittest.TestCase):
    def test_describe_exit_replaces_raw_openblas_text(self):
        tail = 'OpenBLAS error: Memory allocation still failed after 10 retries, giving up.\n'
        msg = RR.describe_exit(1, None, 300.0, tail)
        self.assertEqual(msg, 'нехватка адресного пространства для BLAS; потоки BLAS: 1; лимит RESEARCH_MEM_MB=300 (код возврата 1)')
        self.assertNotIn('OpenBLAS error', msg)
        self.assertIn('нехватка адресного пространства для BLAS', RR.describe_exit(1, None, 300.0, 'foo\nMemory allocation failed\n'))
        self.assertTrue(RR.describe_exit(1, None, 300.0, 'Traceback\nValueError: x\n').startswith('код возврата 1; stderr:'))      # прочие ошибки — как раньше

    def test_child_crash_end_to_end(self):
        r = run(RR.run_job('_selftest_blas', {}, base_dir=tempfile.mkdtemp(), poll_s=0.1))
        self.assertFalse(r['ok'])
        self.assertIn('нехватка адресного пространства для BLAS; потоки BLAS: 1; лимит RESEARCH_MEM_MB=300', r['error'])
        self.assertNotIn('OpenBLAS error', r['error'])


FIXTURE = """
import ccxt.async_support as cx
NET_PROBE_BALANCE = {'code': 0, 'msg': '', 'data': {'balance': {'userId': '1', 'asset': 'USDT', 'balance': '0.0000', 'equity': '0.0000', 'unrealizedProfit': '0.0000',
                     'realisedProfit': '0.0000', 'availableMargin': '0.0000', 'usedMargin': '0.0000', 'freezedMargin': '0.0000'}}}
_x = cx.bingx({'options': {'defaultType': 'swap'}})
BAL = _x.parse_balance(NET_PROBE_BALANCE)
"""


class PristineDd(unittest.TestCase):
    """dd_update на «чистом» `_dd` из литерала модуля (в процессе тестов `fresh_state` дополняет `_dd` недостающими ключами и маскирует такую ошибку — поэтому отдельный процесс)."""

    SCRIPT = FIXTURE + """
import asyncio, json, logging, sys
sys.path.insert(0, %r)
import async_bot as a
recs = []
class H(logging.Handler):
    def emit(self, r):
        if r.levelno >= logging.WARNING:
            recs.append((r.levelname, r.getMessage()[:200]))
logging.getLogger().addHandler(H())
sent = []
async def tg(t):
    sent.append(t)
a.tg = tg
class Ex:
    async def fetch_balance(self):
        return BAL
a.exchange_priv = Ex()
a.all_positions = lambda: []
a.active_strategies = lambda: ([], [])
keys0 = sorted(a._dd)
res = [asyncio.run(a.dd_update()) for _ in range(3)]
print(json.dumps({'keys0': keys0, 'res': res, 'empty': a._dd['empty_account'], 'fails': a._dd['fails'], 'recs': [r for r in recs if 'update fail' in r[1] or 'KeyError' in r[1] or r[0] == 'WARNING'],
                  'sent': sent, 'line': a.health_status_line()}))
""" % ROOT

    def _run(self):
        r = subprocess.run([sys.executable, '-c', self.SCRIPT], capture_output=True, text=True, timeout=180, cwd=ROOT,
                           env=dict(os.environ, TRADES_DB=os.path.join(tempfile.mkdtemp(), 't.db')))
        self.assertEqual(r.returncode, 0, r.stderr[-600:])
        return json.loads(r.stdout.strip().splitlines()[-1])

    def test_pristine_dd_has_keys_and_zero_balance_is_quiet(self):
        out = self._run()
        for k in ('last_status', 'empty_account', 'last_err'):
            self.assertIn(k, out['keys0'])                                                   # ключи есть в литерале модуля (в v120 их там не было)
        self.assertEqual(out['res'], [False, False, False])
        self.assertTrue(out['empty'])
        self.assertEqual(out['fails'], 0)
        self.assertEqual(out['recs'], [])                                                    # ни одного WARNING (и ни KeyError) при equity "0.0000"
        self.assertEqual(out['sent'], [])
        self.assertIn('счёт пуст — режим ожидания', out['line'])

    def test_module_literal_contains_state_keys_in_process(self):
        src = open(os.path.join(ROOT, 'async_bot.py'), encoding='utf-8').read()
        lit = src[src.index('_dd = {'):src.index('_mdd = {')]
        for k in ("'last_err'", "'last_status'", "'empty_account'"):
            self.assertIn(k, lit)


if __name__ == '__main__':
    unittest.main()
