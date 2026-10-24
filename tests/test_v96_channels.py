"""v96: отдельный канал приватных вызовов (exchange_priv), неизвестные команды, экранирование HTML и /csv."""
import ast
import asyncio
import os
import re
import sqlite3
import time
import unittest

from _common import a, fresh_state, ROOT

PRIVATE_METHODS = {
    'fetch_balance', 'fetch_positions', 'fetch_position', 'fetch_open_orders', 'fetch_order', 'fetch_orders', 'fetch_closed_orders',
    'create_order', 'edit_order', 'cancel_order', 'cancel_all_orders', 'fetch_my_trades', 'set_leverage', 'set_margin_mode',
    'set_position_mode', 'fetch_leverage', 'fetch_trading_fees', 'transfer', 'withdraw',
}
# сравнение «общий канал против приватного» в /net_probe — единственное место, где приватный метод намеренно идёт через общий exchange
ALLOWED_SHARED_PRIVATE = {'_np_ccxt_call'}


def run(coro):
    return asyncio.run(coro)


def _parse(path):
    with open(path, encoding='utf-8') as f:
        return ast.parse(f.read())


class PrivateRouting(unittest.TestCase):
    def test_no_private_method_on_shared_exchange(self):
        """AST: ни один приватный метод ccxt не вызывается на общем `exchange` (в async_bot и research_*), кроме сравнения в /net_probe."""
        bad = []
        for fn in ['async_bot.py'] + sorted(f for f in os.listdir(ROOT) if f.startswith('research_') and f.endswith('.py')):
            tree = _parse(os.path.join(ROOT, fn))
            for func in ast.walk(tree):
                if not isinstance(func, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    continue
                for node in ast.walk(func):
                    if (isinstance(node, ast.Attribute) and node.attr in PRIVATE_METHODS and isinstance(node.value, ast.Name)
                            and node.value.id == 'exchange' and func.name not in ALLOWED_SHARED_PRIVATE):
                        bad.append(f'{fn}:{node.lineno} {func.name}: exchange.{node.attr}')
        self.assertEqual(bad, [])

    def test_module_level_private_calls_also_checked(self):
        tree = _parse(os.path.join(ROOT, 'async_bot.py'))
        for node in tree.body:                      # вызовы вне функций
            for sub in ast.walk(node) if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)) else []:
                if isinstance(sub, ast.Attribute) and sub.attr in PRIVATE_METHODS and isinstance(sub.value, ast.Name):
                    self.assertNotEqual(sub.value.id, 'exchange')

    def test_every_expected_private_method_uses_priv(self):
        tree = _parse(os.path.join(ROOT, 'async_bot.py'))
        used = {}
        for node in ast.walk(tree):
            if isinstance(node, ast.Attribute) and node.attr in PRIVATE_METHODS and isinstance(node.value, ast.Name) and node.value.id == 'exchange_priv':
                used[node.attr] = used.get(node.attr, 0) + 1
        for m in ('fetch_balance', 'fetch_positions', 'create_order', 'cancel_order', 'set_leverage', 'set_margin_mode', 'fetch_my_trades'):
            self.assertGreater(used.get(m, 0), 0, m)
        self.assertGreaterEqual(sum(used.values()), 40)

    def test_net_probe_shared_private_call_is_comparison_only(self):
        src = open(os.path.join(ROOT, 'async_bot.py'), encoding='utf-8').read()
        self.assertEqual(len(re.findall(r'\bexchange\.fetch_balance\(', src)), 1)       # только сравнение в _np_ccxt_call

    def test_instance_config_matches_and_throttlers_are_separate(self):
        e, p = a.exchange, a.exchange_priv
        self.assertIsNot(e, p)
        self.assertEqual((p.apiKey, p.secret), (e.apiKey, e.secret))
        self.assertEqual(p.options['defaultType'], e.options['defaultType'])
        self.assertEqual(p.options.get('marginMode'), e.options.get('marginMode'))
        self.assertEqual((p.rateLimit, p.enableRateLimit, p.timeout), (e.rateLimit, e.enableRateLimit, e.timeout))
        self.assertIsNot(p.throttle, e.throttle)

    def test_priv_cost_unchanged(self):
        """Темп приватных запросов не растёт: стоимость запросов и rateLimit те же (3 токена на 1000 мс)."""
        api = a.exchange_priv.describe()['api']['swap']['v2']['private']
        self.assertEqual(api['get']['user/balance'], 3)
        self.assertEqual(api['post']['trade/order'], 3)
        self.assertEqual(a.exchange_priv.rateLimit, 1000)

    def test_markets_synced_to_priv(self):
        class _E:
            markets = {'BTC/USDT:USDT': {'id': 'BTC-USDT', 'symbol': 'BTC/USDT:USDT', 'base': 'BTC', 'quote': 'USDT', 'settle': 'USDT',
                                         'type': 'swap', 'swap': True, 'spot': False, 'future': False, 'option': False, 'contract': True,
                                         'linear': True, 'inverse': False, 'active': True, 'precision': {'amount': 0.0001, 'price': 0.1},
                                         'limits': {}, 'info': {}}}
            currencies = {}

            async def load_markets(self):
                return self.markets
        saved = (a.exchange, a.markets_cache, a.markets_ts, a.exchange_priv.markets, a.exchange_priv.markets_by_id)
        a.exchange, a.markets_cache, a.markets_ts = _E(), None, 0
        try:
            run(a.get_markets())
            self.assertIn('BTC/USDT:USDT', a.exchange_priv.markets)
        finally:
            a.exchange, a.markets_cache, a.markets_ts, a.exchange_priv.markets, a.exchange_priv.markets_by_id = saved


class QueueIndependence(unittest.TestCase):
    """Приватный вызов не ждёт очередь публичного экземпляра (на проде: 47–120 с за сканерами)."""

    def setUp(self):
        self.saved = (a.exchange.throttle, a.exchange_priv.fetch, a.exchange_priv.apiKey, a.exchange_priv.secret, a.exchange_priv.markets,
                      a.exchange_priv.markets_by_id, a.exchange_priv.throttle, a.exchange.apiKey, a.exchange.secret)
        self.release = None

    def tearDown(self):
        (a.exchange.throttle, a.exchange_priv.fetch, a.exchange_priv.apiKey, a.exchange_priv.secret, a.exchange_priv.markets,
         a.exchange_priv.markets_by_id, a.exchange_priv.throttle, a.exchange.apiKey, a.exchange.secret) = self.saved

    def _prepare(self):
        class FakeThrottle:                              # «100 запросов в очереди»: вызовы ждут, пока тест не отпустит
            queue = list(range(100))

            def __init__(s, ev):
                s.ev = ev

            async def __call__(s, cost=None):
                await s.ev.wait()
        priv = a.exchange_priv
        priv.apiKey, priv.secret = 'k', 's'
        # throttler ccxt привязан к циклу событий, в котором создан (в боте цикл один; тесты запускают asyncio.run несколько раз) —
        # свежий throttler на текущем цикле с теми же настройками токен-бакета
        from ccxt.async_support.base.throttler import Throttler
        priv.throttle = Throttler(priv.tokenBucket, loop=asyncio.get_running_loop())

        async def fake_fetch(url, method='GET', headers=None, body=None):
            return {'code': 0, 'data': {'balance': {'balance': '76.5', 'equity': '76.5', 'unrealizedProfit': '0'}}}
        priv.fetch = fake_fetch
        priv.set_markets({'BTC/USDT:USDT': {'id': 'BTC-USDT', 'symbol': 'BTC/USDT:USDT', 'base': 'BTC', 'quote': 'USDT', 'settle': 'USDT',
                                            'type': 'swap', 'swap': True, 'spot': False, 'future': False, 'option': False, 'contract': True,
                                            'linear': True, 'inverse': False, 'active': True, 'precision': {'amount': 0.0001, 'price': 0.1},
                                            'limits': {}, 'info': {}}})
        return FakeThrottle

    def test_private_call_does_not_wait_for_public_queue(self):
        async def main():
            ev = asyncio.Event()
            a.exchange.throttle = self._prepare()(ev)
            self.assertEqual(a._lat_qlen(a.exchange), 100)                       # публичная очередь забита
            public = [asyncio.create_task(a.exchange.throttle(1)) for _ in range(100)]   # 100 запросов стоят в очереди
            await asyncio.sleep(0)
            t0 = time.time()
            bal = await asyncio.wait_for(a.exchange_priv.fetch_balance(), 2)         # приватный уходит сразу
            dt = time.time() - t0
            self.assertEqual(a._extract_equity(bal), 76.5)
            self.assertLess(dt, 1.0)
            self.assertFalse(any(t.done() for t in public))                           # публичные всё ещё ждут
            # контроль: тот же вызов через ОБЩИЙ экземпляр застревает в этой очереди
            a.exchange.apiKey, a.exchange.secret = 'k', 's'
            with self.assertRaises(asyncio.TimeoutError):
                await asyncio.wait_for(a.exchange.fetch_balance(), 0.3)
            ev.set()
            await asyncio.gather(*public)
        run(main())

    def test_equity_fetch_uses_priv_and_ignores_public_queue(self):
        async def main():
            ev = asyncio.Event()
            a.exchange.throttle = self._prepare()(ev)
            eq = await asyncio.wait_for(a._fetch_equity(retry=False), 3)
            self.assertEqual(eq, 76.5)
            ev.set()
        run(main())


class LatChannels(unittest.TestCase):
    def setUp(self):
        self.db, _ = fresh_state()
        del a._lat_buf[:]
        a._lat_sample_n.clear()

    def test_sampling_of_public_ohlcv(self):
        class _Ex:
            throttle = type('T', (), {'queue': []})()

            async def fetch_ohlcv(self, *args, **kw):
                return [[1]]
        ex = _Ex()
        w = a._lat_make(ex, 'fetch_ohlcv', ex.fetch_ohlcv)

        async def main():
            for _ in range(25):
                self.assertEqual(await w('BTC/USDT:USDT', '15m'), [[1]])
        run(main())
        recs = [r for r in a._lat_buf if r[1] == 'ohlcv']
        self.assertEqual(len(recs), 2)                      # каждый 10-й вызов

    def test_report_has_separate_channels(self):
        con = sqlite3.connect(self.db)
        now = time.time()
        rows = [(now, 'equity', '', 3.1, 1, 0), (now, 'equity', '', 3.3, 1, 1), (now, 'sl_place', 'BTC', 3.0, 1, 0),
                (now, 'tickers', 'ALL', 9.0, 1, 40), (now, 'ohlcv', 'BTC', 1.5, 1, 44)]
        con.executemany("INSERT INTO latency_log (ts,op,symbol,dur,ok,qlen) VALUES (?,?,?,?,?,?)", rows)
        con.commit(); con.close()
        text = a.lat_report_text(1)
        i_priv, i_pub = text.index('Приватный канал'), text.index('Публичный канал')
        self.assertLess(i_priv, i_pub)
        priv_part, pub_part = text[i_priv:i_pub], text[i_pub:]
        self.assertIn('equity', priv_part); self.assertIn('sl_place', priv_part)
        self.assertNotIn('tickers', priv_part)
        self.assertIn('tickers', pub_part); self.assertIn('ohlcv', pub_part)
        self.assertIn('макс 1', priv_part)                  # очередь приватного канала: макс 1
        self.assertIn('макс 44', pub_part)                  # очередь публичного: макс 44


class FakeHttp:
    """Минимальная замена aiohttp.ClientSession для check_tg_commands: getUpdates с заданными сообщениями."""
    def __init__(self, texts, chat):
        self.updates = [{'update_id': i + 1, 'message': {'text': t, 'chat': {'id': chat}}} for i, t in enumerate(texts)]

    def get(self, *args, **kw):
        outer = self

        class _Resp:
            status = 200

            async def json(s):
                return {'result': outer.updates}

            async def __aenter__(s):
                return s

            async def __aexit__(s, *x):
                return False
        return _Resp()


class Commands(unittest.TestCase):
    def setUp(self):
        self.db, self.sent = fresh_state()
        self.saved = (a.TOKEN, a.CHAT_ID, a.http, a._tg_offset)
        a.TOKEN, a.CHAT_ID, a._tg_offset = 'x', 123, 0

    def tearDown(self):
        a.TOKEN, a.CHAT_ID, a.http, a._tg_offset = self.saved

    def _run(self, texts):
        a.http = FakeHttp(texts, 123)
        run(a.check_tg_commands())

    def test_unknown_command_reply_has_version(self):
        self._run(['/foobar'])
        self.assertEqual(len(self.sent), 1)
        self.assertEqual(self.sent[0], f'Неизвестная команда /foobar. Версия бота: {a.CODE_VERSION}. /help — список команд')

    def test_unknown_command_with_args_and_case(self):
        self._run(['/FooBar 1 2'])
        self.assertIn('Неизвестная команда /foobar.', self.sent[0])
        self.assertNotIn('1 2', self.sent[0])

    def test_html_in_command_is_escaped(self):
        self._run(['/<b>x&y'])
        self.assertIn('/&lt;b&gt;x&amp;y', self.sent[0])
        self.assertNotIn('<b>', self.sent[0])

    def test_plain_text_and_foreign_bot_commands_ignored(self):
        self._run(['привет', '/start@some_other_bot', 'ok /status'])
        self.assertEqual(self.sent, [])

    def test_known_command_not_answered_as_unknown(self):
        self._run(['/lat_status', '/help'])
        self.assertTrue(self.sent and not any('Неизвестная команда' in m for m in self.sent))

    def test_is_unknown_helper(self):
        self.assertTrue(a._is_unknown_command('/x'))
        self.assertFalse(a._is_unknown_command('x'))
        self.assertFalse(a._is_unknown_command('/x@bot'))
        self.assertFalse(a._is_unknown_command(''))


def tags_balanced(text):
    """В сообщении допустимы только <b>, </b>, <code>, </code>, остальное — экранировано; теги закрыты в правильном порядке."""
    stack = []
    for m in re.finditer(r'<(/?)([a-z]*)>|<|>|&(?!(?:amp|lt|gt);)', text):
        t = m.group(0)
        if t in ('<', '>') or t.startswith('&'):
            return False
        name, closing = m.group(2), bool(m.group(1))
        if name not in ('b', 'code'):
            return False
        if closing:
            if not stack or stack.pop() != name:
                return False
        else:
            stack.append(name)
    return not stack


class HtmlEscaping(unittest.TestCase):
    def setUp(self):
        self.db, self.sent = fresh_state()

    def test_esc(self):
        self.assertEqual(a.esc('A<B>&C'), 'A&lt;B&gt;&amp;C')
        self.assertEqual(a.esc(ValueError('x < y & z')), 'x &lt; y &amp; z')
        self.assertEqual(a.esc('"q"'), '"q"')

    def test_code_blocks_balanced_and_lossless(self):
        csv = 'id,sym,comment\n' + '\n'.join(f'{i},BTC/USDT:USDT,"a<b> & c>{"x" * 60}"' for i in range(300))
        blocks = a._code_blocks(csv)
        self.assertGreater(len(blocks), 1)
        for b in blocks:
            self.assertLessEqual(len(b), 3500 + len('<code></code>'))
            self.assertTrue(tags_balanced(b), b[:80])
        joined = '\n'.join(b[len('<code>'):-len('</code>')] for b in blocks)
        self.assertEqual(joined, a.esc(csv))

    def test_code_blocks_long_line(self):
        blocks = a._code_blocks('&' * 10000 + '\nok')
        self.assertTrue(all(tags_balanced(b) for b in blocks))
        self.assertEqual(''.join(b[6:-7] for b in blocks).replace('\n', ''), a.esc('&' * 10000 + 'ok'))

    def _csv_run(self, csv):
        saved = a.export_trades_csv
        a.export_trades_csv = lambda: csv
        saved_http = (a.TOKEN, a.CHAT_ID, a.http, a._tg_offset)
        a.TOKEN, a.CHAT_ID, a._tg_offset = 'x', 123, 0
        a.http = FakeHttp(['/csv'], 123)
        try:
            run(a.check_tg_commands())
        finally:
            a.export_trades_csv = saved
            a.TOKEN, a.CHAT_ID, a.http, a._tg_offset = saved_http

    def test_csv_command_small(self):
        self._csv_run('id,comment\n1,a<b & c>d')
        self.assertEqual(len(self.sent), 1)
        self.assertTrue(tags_balanced(self.sent[0]), self.sent[0])
        self.assertIn('a&lt;b &amp; c&gt;d', self.sent[0])

    def test_csv_command_large_every_message_balanced(self):
        """Прод-баг: <code> с CSV на десятки строк резался tg() по строкам → незакрытый тег → 400 → запасной путь без parse_mode."""
        head = ','.join(f'col{i}' for i in range(40))
        rows = [','.join(f'v<{r}>&{i}' for i in range(40)) for r in range(60)]
        self._csv_run('\n'.join([head] + rows))
        self.assertGreaterEqual(len(self.sent), 2)
        for m in self.sent:
            self.assertLessEqual(len(m), a.TG_MAX_LEN)
            self.assertTrue(tags_balanced(m), m[:120])
        self.assertIn('Всего строк: 60', self.sent[-1])

    def test_tg_would_split_unwrapped_code(self):
        """Регрессия на причину: прежняя схема (<code> вокруг всего CSV) действительно давала несбалансированные куски."""
        body = '\n'.join(f'row{i},' + 'x' * 300 for i in range(40))
        parts = a._split_tg_text(f'<code>{body}</code>')
        self.assertFalse(all(tags_balanced(p) for p in parts))

    def test_error_sites_escape_exception_text(self):
        src = open(os.path.join(ROOT, 'async_bot.py'), encoding='utf-8').read()
        for m in re.finditer(r"tg\(f'❌ /[a-z_]+: ([^\n]*)'\)", src):
            self.assertIn('esc(', m.group(1), m.group(0))

    def test_symbols_in_trade_notifications_escaped(self):
        src = open(os.path.join(ROOT, 'async_bot.py'), encoding='utf-8').read()
        bad = [l.strip() for l in src.split('\n') if '<b>[' in l and '{sym}' in l and 'logging.' not in l]
        self.assertEqual(bad, [])

    def test_net_probe_text_escapes_errors(self):
        res = {'dns': {'ok': False, 'err': 'gaierror <x> & y'}, 'direct_pub': [{'dt': 1.0, 'status': None, 'api_code': None, 'msg': '', 'err': 'E<1>&'}],
               'direct_priv': [], 'ccxt_pub': [], 'ccxt_priv': [], 'ccxt_priv_shared': []}
        text = a.net_probe_text(res)
        self.assertTrue(tags_balanced(text), text)
        self.assertIn('gaierror &lt;x&gt; &amp; y', text)

    def test_health_alert_escapes_error_sample(self):
        sent = []

        async def fake(t):
            sent.append(t)
        a.tg = fake
        t0 = 1_800_000_000.0
        for i in range(101):
            a._health_note_error('RequestTimeout <html> & co', t0 - 100 + i)
        run(a.health_alert_tick(t0))
        self.assertTrue(sent and all(tags_balanced(m) for m in sent), sent)


class NetProbeChannels(unittest.TestCase):
    def _ok(self, dt=0.2):
        return {'dt': dt, 'status': 200, 'api_code': 0, 'msg': '', 'err': None}

    def test_prod_case_verdict_private_channel_ok_public_overloaded(self):
        res = {'dns': {'ok': True, 'dt': 0.01, 'ips': ['1.1.1.1']}, 'direct_pub': [self._ok()] * 5, 'direct_priv': [self._ok()] * 5,
               'ccxt_pub': [{'dt': 60.0, 'err': None, 'queue': 50}] * 5,
               'ccxt_priv': [{'dt': 3.0, 'err': None, 'queue': 0}] * 5,
               'ccxt_priv_shared': [{'dt': 63.0, 'err': None, 'queue': 50}] * 5}
        v = a._net_probe_verdict(res)
        self.assertIn('ПРИВАТНЫЙ КАНАЛ В НОРМЕ', v)
        self.assertIn('ОЧЕРЕДЬ ПЕРЕГРУЖЕНА только у публичного', v)
        self.assertIn('через общий канал приватный запрос — медиана 63.0', v)

    def test_private_channel_itself_overloaded(self):
        res = {'dns': {'ok': True, 'dt': 0.01, 'ips': []}, 'direct_pub': [self._ok()] * 5, 'direct_priv': [self._ok()] * 5,
               'ccxt_pub': [{'dt': 0.5, 'err': None, 'queue': 0}] * 5,
               'ccxt_priv': [{'dt': 40.0, 'err': None, 'queue': 30}] * 5, 'ccxt_priv_shared': []}
        self.assertIn('ОЧЕРЕДЬ ПЕРЕГРУЖЕНА (ПРИВАТНЫЙ КАНАЛ', a._net_probe_verdict(res))

    def test_everything_fine(self):
        res = {'dns': {'ok': True, 'dt': 0.01, 'ips': []}, 'direct_pub': [self._ok()] * 5, 'direct_priv': [self._ok()] * 5,
               'ccxt_pub': [{'dt': 0.4, 'err': None, 'queue': 0}] * 5, 'ccxt_priv': [{'dt': 3.1, 'err': None, 'queue': 0}] * 5,
               'ccxt_priv_shared': [{'dt': 3.2, 'err': None, 'queue': 0}] * 5}
        self.assertIn('ДОСТУП В НОРМЕ', a._net_probe_verdict(res))


if __name__ == '__main__':
    unittest.main()
