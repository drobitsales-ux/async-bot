"""v107: режим ожидания — все сканеры выключены по умолчанию (в чистом окружении), сканеры не делают сетевых вызовов, /status и старт показывают режим ожидания, цикл ожидания,
breaker'ы и мониторинг эквити работают без сканеров."""
import asyncio
import json
import os
import subprocess
import sys
import unittest

from _common import a, ROOT, fresh_state

FLAGS = ('SMC_LIVE', 'SMC_SIG_ENABLED', 'SMC_SHADOW_ENABLED', 'SMC2_ENABLED', 'PO3_ENABLED', 'ORB_RT_ENABLED', 'RB_ENABLED', 'ORB_ENABLED', 'SA_ENABLED', 'SA_LIVE',
         'PB_ENABLED', 'PB_LIVE', 'MOMENTUM_ENABLED', 'MOMENTUM_LIVE', 'RSI_ENABLED')


def run(c):
    return asyncio.run(c)


class Defaults(unittest.TestCase):
    def test_all_flags_false_in_clean_environment(self):
        env = {k: v for k, v in os.environ.items() if k not in FLAGS and k != 'IDLE_CYCLE_SEC'}
        code = ("import json, async_bot as a\n"
                "print('FLAGS' + json.dumps({k: getattr(a, k) for k in %r}))\n"
                "print('IDLE' + json.dumps([a.idle_mode(), list(map(list, a.active_strategies())), a.IDLE_CYCLE_SEC]))" % (FLAGS,))
        out = subprocess.run([sys.executable, '-c', code], cwd=ROOT, env=env, capture_output=True, text=True, timeout=120)
        self.assertEqual(out.returncode, 0, out.stderr[-500:])
        flags = json.loads(next(ln for ln in out.stdout.splitlines() if ln.startswith('FLAGS'))[5:])
        idle = json.loads(next(ln for ln in out.stdout.splitlines() if ln.startswith('IDLE'))[4:])
        self.assertEqual({k: v for k, v in flags.items() if v}, {})                 # ни один флаг не включён по умолчанию
        self.assertEqual(idle, [True, [[], []], 300.0])                              # режим ожидания, интервал по умолчанию 300 с

    def test_source_defaults_are_false(self):
        src = open(os.path.join(ROOT, 'async_bot.py'), encoding='utf-8').read()
        self.assertIn("SMC_SIG_ENABLED = os.getenv('SMC_SIG_ENABLED', 'false')", src)
        self.assertIn("IDLE_CYCLE_SEC = max(60.0, float(os.getenv('IDLE_CYCLE_SEC', '300')", src)


class Scanners(unittest.TestCase):
    def setUp(self):
        self.saved = {k: getattr(a, k) for k in FLAGS}
        for k in FLAGS:
            setattr(a, k, False)
        self.saved_fn = (a.get_btc_context, a._scan_universe, a.get_tickers_cached, a.is_session)

    def tearDown(self):
        for k, v in self.saved.items():
            setattr(a, k, v)
        a.get_btc_context, a._scan_universe, a.get_tickers_cached, a.is_session = self.saved_fn

    def boom(self):
        calls = []

        async def f(*aa, **kk):
            calls.append(1)
            raise AssertionError('сетевой вызов в режиме ожидания')

        def sync_boom(*aa, **kk):
            calls.append(1)
            raise AssertionError('вызов в режиме ожидания')
        a.get_btc_context, a._scan_universe, a.get_tickers_cached, a.is_session = f, f, f, sync_boom
        return calls

    def test_every_scanner_returns_without_network_calls(self):
        calls = self.boom()
        for fn in (a.scan_smc, a.scan_rsi, a.scan_rb, a.scan_orb, a.scan_po3, a.scan_smc2, a.scan_orb_rt):
            run(fn())
        self.assertEqual(calls, [])

    def test_smc_scan_gate_opens_only_with_live_or_sig_flag(self):
        for flag in ('SMC_SIG_ENABLED', 'SMC_LIVE'):
            setattr(a, flag, True)
            calls = []
            a.is_session = lambda: calls.append(1) or False                            # вне сессии скан молча выходит — но ворота открыты (is_session вызван)
            run(a.scan_smc())
            self.assertEqual(calls, [1], flag)
            setattr(a, flag, False)

    def test_active_strategies_and_texts(self):
        self.assertEqual(a.active_strategies(), ([], []))
        self.assertTrue(a.idle_mode())
        self.assertIn(a.IDLE_TEXT, a.startup_text())
        self.assertNotIn('Shadow (виртуальные)', a.startup_text())
        self.assertIn('Режим ожидания: торговых и shadow-стратегий нет (поиск закрыт, BOT_SPEC §3)', a.status_text())
        a.SMC_SIG_ENABLED = True
        live, shadow = a.active_strategies()
        self.assertTrue(any('SMC_SIG' in x for x in shadow))
        self.assertFalse(a.idle_mode())
        self.assertNotIn(a.IDLE_TEXT, a.status_text())
        self.assertIn('Shadow (виртуальные)', a.startup_text())
        a.SMC_SIG_ENABLED = False
        a.RB_ENABLED = True
        self.assertFalse(a.idle_mode())
        a.RB_ENABLED = False
        a.SMC_LIVE = True
        self.assertEqual(a.active_strategies()[0], ['SMC'])
        self.assertFalse(a.idle_mode())

    def test_status_keeps_breaker_and_monitoring_lines(self):
        txt = a.status_text()
        for needle in ('Circuit:', 'DD по эквити:', 'Позиций:', 'БД сделок'):
            self.assertIn(needle, txt)


class IdleCycle(unittest.TestCase):
    def setUp(self):
        self.old = (a._idle_state['last_full'], a.all_positions, a.IDLE_CYCLE_SEC, a.RB_ENABLED)
        a._idle_state['last_full'] = 0.0
        a.IDLE_CYCLE_SEC = 300.0
        a.RB_ENABLED = False

    def tearDown(self):
        a._idle_state['last_full'], a.all_positions, a.IDLE_CYCLE_SEC, a.RB_ENABLED = self.old

    def test_tick_only_between_full_cycles(self):
        a.all_positions = lambda: []
        t0 = 10_000.0
        self.assertFalse(a.idle_tick_only(t0))                 # первый — полный цикл
        self.assertTrue(a.idle_tick_only(t0 + 10))             # дальше только команды
        self.assertTrue(a.idle_tick_only(t0 + 299))
        self.assertFalse(a.idle_tick_only(t0 + 300))           # раз в IDLE_CYCLE_SEC — полный
        self.assertTrue(a.idle_tick_only(t0 + 310))

    def test_open_position_or_enabled_scanner_means_full_cycle_every_iteration(self):
        t0 = 20_000.0
        a.all_positions = lambda: [{'x': 1}]
        a.idle_tick_only(t0)
        self.assertFalse(a.idle_tick_only(t0 + 5))             # открытая позиция — мониторинг каждую итерацию
        a.all_positions = lambda: []
        a.RB_ENABLED = True
        self.assertFalse(a.idle_tick_only(t0 + 6))             # сканер включён — не режим ожидания

    def test_main_loop_guard_is_before_cycle_and_polls_commands(self):
        src = open(os.path.join(ROOT, 'async_bot.py'), encoding='utf-8').read()
        i = src.index('if idle_tick_only():')
        self.assertLess(i, src.index('cycle += 1', i))
        self.assertIn('await check_tg_commands()', src[i:i + 400])
        self.assertIn('asyncio.sleep(IDLE_TICK_SEC)', src[i:i + 500])


class BreakersWithoutScanners(unittest.TestCase):
    """Эквити и breaker'ы работают в режиме ожидания (сканеры выключены по умолчанию)."""

    def setUp(self):
        _db, self.sent = fresh_state()
        a.circuit_open = False
        a._mdd.update({'peak': 0.0, 'tripped': False})
        self.old_bal = a.exchange_priv.fetch_balance

    def tearDown(self):
        a.exchange_priv.fetch_balance = self.old_bal
        a.circuit_open = False
        a._mdd.update({'peak': 0.0, 'tripped': False})

    def check(self):
        """check_circuit_breaker шлёт уведомление через create_task — нужен работающий цикл событий."""
        async def f():
            r = a.check_circuit_breaker()
            await asyncio.sleep(0.05)
            return r
        return run(f())

    def balance(self, eq):
        async def f(*aa, **kk):
            return {'info': {'data': {'balance': {'equity': str(eq)}}}}
        a.exchange_priv.fetch_balance = f

    def test_daily_and_total_breakers_trip_without_any_scanner(self):
        self.assertTrue(a.idle_mode())
        self.balance(100.0)
        self.assertTrue(run(a.dd_update(force_rebase=True)))
        self.assertTrue(self.check())                                     # торговля разрешена
        self.balance(98.0)                                                # −2% за день > лимит 1.5%
        self.assertTrue(run(a.dd_update()))
        self.assertFalse(self.check())
        self.assertTrue(a.circuit_open)
        self.assertIn('Circuit: 🔴 СТОП', a.status_text())
        # общий breaker по просадке от пика (6%)
        self.balance(93.0)
        run(a.dd_update())
        self.assertTrue(a._mdd['tripped'])
        self.assertFalse(self.check())
        self.assertIn('ОБЩИЙ breaker', a.status_text())
        self.assertTrue(any('ОБЩИЙ BREAKER' in m for m in self.sent))

    def test_equity_failure_does_not_trip_or_reset(self):
        self.balance(100.0)
        run(a.dd_update(force_rebase=True))

        async def fail(*aa, **kk):
            raise TimeoutError('нет ответа')
        a.exchange_priv.fetch_balance = fail
        old = a.EQUITY_RETRY_PAUSES
        a.EQUITY_RETRY_PAUSES = ()
        try:
            self.assertFalse(run(a.dd_update()))
        finally:
            a.EQUITY_RETRY_PAUSES = old
        self.assertFalse(a.circuit_open)
        self.assertFalse(a._mdd['tripped'])
        self.assertEqual(a._dd['equity'], 100.0)                         # последнее известное значение сохранено


if __name__ == '__main__':
    unittest.main()
