"""v117: развязка публичных данных BingX и ключа (BOT_SPEC §2.49): exchange_pub без ключей не делает приватных запросов; 100413 на приватном канале даёт один алерт в час и не останавливает публичные данные;
дайджест не опрашивает выключенный воркер; версии закреплены."""
import asyncio
import glob
import os
import re
import unittest

import ccxt.async_support as ccxt_async

import _common
from _common import a

ROOT = _common.ROOT


def run(c):
    return asyncio.run(c)


MARKET = {'id': 'BTC-USDT', 'symbol': 'BTC/USDT:USDT', 'base': 'BTC', 'quote': 'USDT', 'settle': 'USDT', 'baseId': 'BTC', 'quoteId': 'USDT', 'settleId': 'USDT', 'type': 'swap', 'spot': False, 'margin': False, 'swap': True,
          'future': False, 'option': False, 'contract': True, 'linear': True, 'inverse': False, 'active': True, 'precision': {'amount': 0.0001, 'price': 0.1}, 'limits': {}, 'info': {}}


class PublicInstance(unittest.TestCase):
    def test_exchange_pub_has_no_keys_in_source(self):
        src = open(os.path.join(ROOT, 'async_bot.py'), encoding='utf-8').read()
        block = src[src.index('exchange_pub = ccxt_async.bingx({'):]
        block = block[:block.index('})')]
        self.assertNotIn('apiKey', block)
        self.assertNotIn('secret', block)
        self.assertFalse(a.exchange_pub.apiKey)
        self.assertFalse(a.exchange_pub.secret)
        self.assertIsNot(a.exchange_pub, a.exchange_priv)

    def test_load_markets_makes_no_private_request_without_keys_but_does_with_keys(self):
        """Причина инцидента: с ключом load_markets → fetch_currencies → приватный walletsV1PrivateGetCapitalConfigGetall; без ключа — None, приватного запроса нет."""
        for keyed, expect in ((False, 0), (True, 1)):
            ex = ccxt_async.bingx({'apiKey': 'k', 'secret': 's', 'options': {'defaultType': 'swap'}} if keyed else {'options': {'defaultType': 'swap'}})
            calls = []

            async def private(params={}, calls=calls):
                calls.append('walletsV1PrivateGetCapitalConfigGetall')
                return {'code': 0, 'data': []}

            async def fetch_markets(params={}):
                return [dict(MARKET)]

            async def any_fetch(*x, **k):
                calls.append('fetch')
                raise AssertionError('сетевой запрос')
            ex.walletsV1PrivateGetCapitalConfigGetall = private
            ex.fetch_markets = fetch_markets
            ex.fetch = any_fetch
            mk = run(ex.load_markets(True))
            self.assertIn('BTC/USDT:USDT', mk)
            self.assertEqual(calls.count('walletsV1PrivateGetCapitalConfigGetall'), expect, keyed)
            self.assertNotIn('fetch', calls)

    def test_pub_instance_of_the_bot_does_not_call_private(self):
        calls = []

        async def private(params={}):
            calls.append('private')
            return {'code': 0, 'data': []}

        async def fetch_markets(params={}):
            return [dict(MARKET)]
        old = (a.exchange_pub.walletsV1PrivateGetCapitalConfigGetall, a.exchange_pub.fetch_markets)
        saved_state = (a.exchange_pub.markets, a.exchange_pub.markets_by_id, a.exchange_pub.currencies)
        a.exchange_pub.markets_loading, a.exchange_pub.reloading_markets = None, False         # состояние load_markets привязано к циклу событий предыдущего теста
        a.exchange_pub.walletsV1PrivateGetCapitalConfigGetall = private
        a.exchange_pub.fetch_markets = fetch_markets
        try:
            run(a.exchange_pub.load_markets(True))
        finally:
            a.exchange_pub.walletsV1PrivateGetCapitalConfigGetall, a.exchange_pub.fetch_markets = old
            a.exchange_pub.markets_loading, a.exchange_pub.reloading_markets = None, False
            a.exchange_pub.markets, a.exchange_pub.markets_by_id, a.exchange_pub.currencies = saved_state
        self.assertEqual(calls, [])

    def test_public_calls_use_exchange_pub_and_keyed_exchange_only_in_net_probe(self):
        src = open(os.path.join(ROOT, 'async_bot.py'), encoding='utf-8').read()
        bad = []
        for i, ln in enumerate(src.split('\n'), 1):
            st = ln.lstrip()
            if st.startswith('#') or st.startswith("('20") or st.startswith('"""'):
                continue
            for m in re.finditer(r'(?<![\w.\'"_])exchange\.([a-z_]+)', ln):
                if m.group(1) not in ('close', 'fetch_balance'):
                    bad.append((i, ln.strip()[:80]))
        self.assertEqual(bad, [])
        for f in glob.glob(os.path.join(ROOT, 'research_*.py')) + glob.glob(os.path.join(ROOT, 'audit_*.py')):
            t = open(f, encoding='utf-8').read()
            self.assertNotIn("'apiKey'", t, f)                      # исследовательские модули ключей не используют


class KeyError100413(unittest.TestCase):
    def setUp(self):
        self.sent = []
        self.old_tg = a.tg

        async def fake_tg(t):
            self.sent.append(t)
        a.tg = fake_tg
        a._key_alert['t'] = 0.0

    def tearDown(self):
        a.tg = self.old_tg

    def test_one_alert_per_hour(self):
        e = Exception('bingx {"code":100413,"msg":"Incorrect apiKey"}')

        async def go():
            r1 = a.key_error_note(e, now=10_000.0)
            r2 = a.key_error_note(e, now=10_000.0 + 3599)
            r3 = a.key_error_note(e, now=10_000.0 + 3601)
            r4 = a.key_error_note(Exception('timeout'), now=9e9)
            await asyncio.sleep(0)
            return r1, r2, r3, r4
        self.assertEqual(run(go()), (True, False, True, False))
        self.assertEqual(len(self.sent), 2)
        self.assertIn('Ключ BingX недействителен', self.sent[0])

    def test_private_wrapper_alerts_and_public_is_untouched(self):
        class Dummy:
            async def fetch_balance(self):
                raise Exception('bingx 100413 Incorrect apiKey')

            async def fetch_ticker(self, s):
                return {'last': 1.0}
        d = Dummy()
        wp = a._lat_make(d, 'fetch_balance', d.fetch_balance)

        async def go():
            try:
                await wp()
            except Exception as e:
                err = str(e)
            await asyncio.sleep(0)
            # публичный вызов на том же процессе идёт через exchange_pub и от ключа не зависит
            old = a.exchange_pub.fetch_ticker

            async def ok(sym):
                return {'last': 2.0}
            a.exchange_pub.fetch_ticker = ok
            try:
                r = await a._np_ccxt_call('pub')
            finally:
                a.exchange_pub.fetch_ticker = old
            return err, r
        err, r = run(go())
        self.assertIn('100413', err)
        self.assertEqual(len(self.sent), 1)
        self.assertIsNone(r['err'])                                                  # публичный путь работает при недействительном ключе

    def test_candle_loader_independent_of_priv_key(self):
        """Загрузчик свечей исследований создаёт свои keyless-клиенты и не использует ключевые экземпляры бота."""
        import research_xdata as X
        src = open(os.path.join(ROOT, 'research_xdata.py'), encoding='utf-8').read()
        self.assertNotIn('exchange_priv', src)
        self.assertNotIn('apiKey', src)
        old = a.exchange_priv.fetch_balance

        async def boom(*x, **k):
            raise Exception('100413 Incorrect apiKey')
        a.exchange_priv.fetch_balance = boom
        try:
            self.assertTrue(callable(X.run_xload))
        finally:
            a.exchange_priv.fetch_balance = old


class Digest(unittest.TestCase):
    def test_worker_disabled_by_default_and_no_poll(self):
        self.assertFalse(a.WORKER_ENABLED)
        self.assertEqual(run(a._fetch_worker_status()), 'воркер выключен (WORKER_ENABLED=false)')
        old = (a.WORKER_ENABLED, a._worker_urls)
        a.WORKER_ENABLED, a._worker_urls = True, []
        try:
            self.assertIn('WORKER_URLS не задан', run(a._fetch_worker_status()))
        finally:
            a.WORKER_ENABLED, a._worker_urls = old


class Versions(unittest.TestCase):
    def test_pins_and_warning(self):
        self.assertEqual(open(os.path.join(ROOT, '.python-version')).read().strip(), '3.13.18')
        self.assertIn('ccxt==4.2.14', open(os.path.join(ROOT, 'requirements.txt'), encoding='utf-8').read())
        self.assertIn('Окружение: Python', a.runtime_versions_text())
        import ccxt
        old = ccxt.__version__
        ccxt.__version__ = '4.9.9'
        try:
            self.assertIn('⚠️', a.runtime_versions_text())
            self.assertIn('4.9.9', a.runtime_versions_text())
        finally:
            ccxt.__version__ = old
        self.assertIn('Окружение: Python', a.startup_text())


if __name__ == '__main__':
    unittest.main()
