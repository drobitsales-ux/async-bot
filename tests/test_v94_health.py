"""v94: тревоги самодиагностики, вердикт дайджеста, /net_probe, повторы эквити."""
import asyncio
import sqlite3
import time
import unittest
from datetime import datetime, timezone

from _common import a, fresh_state


def run(coro):
    return asyncio.run(coro)


class HealthAlerts(unittest.TestCase):
    def setUp(self):
        self.db, self.sent = fresh_state()
        self.t0 = 1_800_000_000.0

    def _types(self, now):
        return run(a.health_alert_tick(now))

    def test_equity_not_received_30min(self):
        a._dd.update({'fails': 7, 'fail_since': self.t0 - 31 * 60, 'ok_ts': self.t0 - 3600})
        self.assertEqual(self._types(self.t0), ['equity'])

    def test_equity_under_30min_silent(self):
        a._dd.update({'fails': 7, 'fail_since': self.t0 - 10 * 60, 'ok_ts': self.t0 - 3600})
        self.assertEqual(self._types(self.t0), [])

    def test_ctx_fail_over_half(self):
        for i in range(10):
            a._health_note_scan('SMC', {'ctx_fail': 1 if i < 6 else 0}, self.t0 - 100 + i)
        self.assertEqual(self._types(self.t0), ['ctx_fail'])

    def test_ctx_fail_half_or_less_silent(self):
        for i in range(10):
            a._health_note_scan('SMC', {'ctx_fail': 1 if i < 5 else 0}, self.t0 - 100 + i)
        self.assertEqual(self._types(self.t0), [])

    def test_fetch_err_over_half_po3_and_orb_rt(self):
        a._health_note_scan('PO3', {'total': 60, 'fetch_err': 54.4}, self.t0 - 60)      # прод: 54.4 из 60
        a._health_note_scan('ORB_RT', {'total': 80, 'fetch_err': 15.4}, self.t0 - 60)   # 19% — не тревога
        self.assertEqual(self._types(self.t0), ['fetch_err:PO3'])

    def test_errors_of_one_type_over_100(self):
        for i in range(101):
            a._health_note_error('GET /openApi/swap/v2/user/balance RequestTimeout 10000ms', self.t0 - 600 + i)
        self.assertEqual(self._types(self.t0), ['errtype:RequestTimeout'])

    def test_errors_exactly_100_silent(self):
        for i in range(100):
            a._health_note_error('RequestTimeout x', self.t0 - 600 + i)
        self.assertEqual(self._types(self.t0), [])

    def test_old_window_expires(self):
        for i in range(150):
            a._health_note_error('RequestTimeout x', self.t0 - 2 * 3600 + i)
        self.assertEqual(self._types(self.t0), [])

    def test_no_repeat_within_hour_then_repeat(self):
        a._dd.update({'fails': 7, 'fail_since': self.t0 - 40 * 60, 'ok_ts': self.t0 - 3600})
        self.assertEqual(self._types(self.t0), ['equity'])
        self.assertEqual(self._types(self.t0 + 60), [])
        self.assertEqual(self._types(self.t0 + 3599), [])
        self.assertEqual(self._types(self.t0 + 3601), ['equity'])
        self.assertEqual(len(self.sent), 2)

    def test_types_throttled_independently(self):
        a._dd.update({'fails': 7, 'fail_since': self.t0 - 40 * 60, 'ok_ts': self.t0 - 3600})
        self.assertEqual(self._types(self.t0), ['equity'])
        a._health_note_scan('PO3', {'total': 60, 'fetch_err': 54}, self.t0 + 100)
        self.assertEqual(self._types(self.t0 + 200), ['fetch_err:PO3'])

    def test_status_line(self):
        a._dd.update({'ok_ts': time.time() - 120})
        a._health_note_scan('SMC', {'ctx_fail': 1})
        a._health_note_scan('PO3', {'total': 60, 'fetch_err': 30})
        line = a.health_status_line()
        self.assertIn('2 мин назад', line)
        self.assertIn('ctx_fail 100%', line)
        self.assertIn('PO3 50%', line)


class DigestVerdict(unittest.TestCase):
    def setUp(self):
        self.db, self.sent = fresh_state()

        class _Ex:
            async def fetch_balance(self_):
                return {'USDT': {'total': 75.0}}
        self._ex = a.exchange_priv
        a.exchange_priv = _Ex()

        async def _ws():
            return 'н/д'
        a._fetch_worker_status = _ws

    def tearDown(self):
        a.exchange_priv = self._ex

    def _fill_errors(self, n):
        con = sqlite3.connect(self.db)
        con.execute('DELETE FROM log_events')
        ts = datetime.now(timezone.utc).isoformat()
        con.executemany('INSERT INTO log_events (ts,level,message) VALUES (?,?,?)',
                        [(ts, 'WARNING', '[DD] эквити не получено: RequestTimeout: bingx GET user/balance') for _ in range(n)])
        con.commit(); con.close()

    def test_2060_errors_is_anomaly(self):
        self._fill_errors(2060)
        _f, verdict = run(a.build_daily_digest())
        self.assertIn('АНОМАЛИЯ', verdict)
        self.assertIn('2060', verdict)
        self.assertNotIn('Аномалий нет', verdict)

    def test_few_errors_no_anomaly(self):
        self._fill_errors(5)
        _f, verdict = run(a.build_daily_digest())
        self.assertIn('Аномалий нет', verdict)

    def test_health_condition_is_anomaly_even_with_few_errors(self):
        self._fill_errors(5)
        for _ in range(10):
            a._health_note_scan('SMC', {'ctx_fail': 1})
        _f, verdict = run(a.build_daily_digest())
        self.assertIn('АНОМАЛИЯ', verdict)
        self.assertIn('ctx_fail', verdict)


class NetProbe(unittest.TestCase):
    def setUp(self):
        self.db, self.sent = fresh_state()

    def test_unreachable_network_no_exceptions(self):
        async def boom(*_a, **_k):
            raise ConnectionError('network unreachable')

        class _Ex:
            fetch_ticker = staticmethod(boom)
            fetch_balance = staticmethod(boom)
        saved = (a.exchange, a.exchange_priv, a._np_dns, a._np_http_get)
        a.exchange, a.exchange_priv, a._np_dns, a._np_http_get = _Ex(), _Ex(), boom, boom
        try:
            run(a.net_probe_run())
        finally:
            a.exchange, a.exchange_priv, a._np_dns, a._np_http_get = saved
        self.assertFalse(a._net_probe_running[0])
        self.assertTrue(any('СЕТЬ НЕДОСТУПНА' in m for m in self.sent), self.sent)

    def test_real_dns_failure_no_exceptions(self):
        async def boom(*_a, **_k):
            raise ConnectionError('network unreachable')

        class _Ex:
            fetch_ticker = staticmethod(boom)
            fetch_balance = staticmethod(boom)
        saved = (a.exchange, a.exchange_priv, a.NET_PROBE_HOST, a.NET_PROBE_N)
        a.exchange, a.exchange_priv, a.NET_PROBE_HOST, a.NET_PROBE_N = _Ex(), _Ex(), 'unreachable.invalid', 1
        try:
            run(a.net_probe_run())
        finally:
            a.exchange, a.exchange_priv, a.NET_PROBE_HOST, a.NET_PROBE_N = saved
        self.assertTrue(any('СЕТЬ НЕДОСТУПНА' in m for m in self.sent), self.sent)

    def _ok(self, dt=0.3):
        return {'dt': dt, 'status': 200, 'api_code': 0, 'msg': '', 'err': None}

    def test_verdicts(self):
        base = {'dns': {'ok': True, 'dt': 0.01, 'ips': ['1.2.3.4']},
                'direct_pub': [self._ok()] * 5, 'direct_priv': [self._ok()] * 5}
        # доступ в норме
        res = dict(base, ccxt_pub=[{'dt': 0.4, 'err': None, 'queue': 0}] * 5,
                   ccxt_priv=[{'dt': 3.2, 'err': None, 'queue': 0}] * 5)
        self.assertIn('ДОСТУП В НОРМЕ', a._net_probe_verdict(res))
        # очередь перегружена: прямой быстрый, ccxt таймаутит, очередь глубокая
        res = dict(base, ccxt_pub=[{'dt': 10.0, 'err': 'RequestTimeout', 'queue': 40}] * 5, ccxt_priv=[])
        self.assertIn('ОЧЕРЕДЬ ПЕРЕГРУЖЕНА', a._net_probe_verdict(res))
        # отказ BingX
        den = {'dt': 0.2, 'status': 403, 'api_code': None, 'msg': 'forbidden', 'err': None}
        res = dict(base, direct_pub=[den] * 5, ccxt_pub=[], ccxt_priv=[])
        self.assertIn('ОТКАЗ BINGX: HTTP 403', a._net_probe_verdict(res))
        # медленная сеть
        res = dict(base, direct_pub=[self._ok(6.0)] * 5, ccxt_pub=[], ccxt_priv=[])
        self.assertIn('МЕДЛЕННАЯ', a._net_probe_verdict(res))
        # все прямые упали
        dead = {'dt': 15.0, 'status': None, 'api_code': None, 'msg': '', 'err': 'TimeoutError'}
        res = dict(base, direct_pub=[dead] * 5, direct_priv=[dead] * 5, ccxt_pub=[], ccxt_priv=[])
        self.assertIn('СЕТЬ НЕДОСТУПНА', a._net_probe_verdict(res))
        # DNS
        res = dict(base, dns={'ok': False, 'err': 'gaierror'}, ccxt_pub=[], ccxt_priv=[])
        self.assertIn('DNS', a._net_probe_verdict(res))


class EquityRetry(unittest.TestCase):
    def setUp(self):
        self.db, self.sent = fresh_state()
        self.pauses = []
        self._sleep, self._ex = a.asyncio.sleep, a.exchange_priv

        async def fake_sleep(sec, *_a, **_k):
            self.pauses.append(sec)
        a.asyncio.sleep = fake_sleep

    def tearDown(self):
        a.asyncio.sleep, a.exchange_priv = self._sleep, self._ex

    def _exchange(self, fails):
        calls = {'n': 0}

        class _Ex:
            async def fetch_balance(self_):
                calls['n'] += 1
                if calls['n'] <= fails:
                    raise a.ccxt_async.RequestTimeout('bingx GET user/balance')
                return {'info': {'data': {'balance': {'equity': '76.5'}}}}
        a.exchange_priv = _Ex()
        return calls

    def test_recovers_on_third_attempt(self):
        calls = self._exchange(2)
        self.assertEqual(run(a._fetch_equity()), 76.5)
        self.assertEqual(self.pauses, [5, 15])
        self.assertEqual(calls['n'], 3)

    def test_gives_up_after_pauses_5_15_30(self):
        calls = self._exchange(99)
        self.assertIsNone(run(a._fetch_equity()))
        self.assertEqual(self.pauses, [5, 15, 30])
        self.assertEqual(calls['n'], 4)

    def test_no_retry_mode_single_attempt(self):
        calls = self._exchange(99)
        self.assertIsNone(run(a._fetch_equity(retry=False)))
        self.assertEqual(self.pauses, [])
        self.assertEqual(calls['n'], 1)

    def test_dd_update_retries_only_after_success(self):
        calls = self._exchange(99)
        self.assertFalse(run(a.dd_update()))          # fails был 0 → полный набор попыток
        self.assertEqual(calls['n'], 4)
        self.assertEqual(a._dd['fails'], 1)
        self.assertGreater(a._dd['fail_since'], 0)
        calls['n'] = 0
        self.assertFalse(run(a.dd_update()))          # затяжной сбой → одна попытка, цикл не растягивается
        self.assertEqual(calls['n'], 1)


if __name__ == '__main__':
    unittest.main()
