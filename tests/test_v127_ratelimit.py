"""v127 (BOT_SPEC §2.58): отчёт /xfund_1h_errors — все 48 ошибок = лимит запросов BingX (код 100410 «请稍后重试»): временные ошибки распознаются (is_transient, классификация, приоритет в над а1/а2), не кэшируются как постоянные;
частичная глубина 1h (≈ 22 500 баров) показывается по первому 1h-бару."""
import asyncio
import tempfile
import unittest

import _common
import research_h1err as H
import research_xdata as X
from test_v125_classify import DAY, HOUR, NOW, Fake, make_idx, mk, rows, run_cmd

ROOT = _common.ROOT
RL = '{"code":100410,"msg":"请稍后重试","timestamp":1}'                                 # ответ BingX из отчёта


def rl_err():
    return type('ExchangeError', (Exception,), {})('bingx ' + RL)


class Transient(unittest.TestCase):
    def test_is_transient_by_code_and_chinese_text(self):
        Ex = type('ExchangeError', (Exception,), {})
        self.assertTrue(X.is_transient(Ex('bingx {"code":100410,"msg":"请稍后重试"}')))       # код 100410 при классе ExchangeError
        self.assertTrue(X.is_transient(Ex('bingx 请稍后重试')))                                # только китайский текст
        self.assertTrue(X.is_transient(Ex('Rate limit exceeded, try again later')))
        self.assertFalse(X.is_transient(Ex('bingx {"code":109400,"msg":"invalid symbol"}')))   # постоянные — как раньше
        self.assertFalse(X.is_transient(Ex('bingx {"code":100500,"msg":"weird"}')))             # 100500 не считается (нет данных о смысле)
        self.assertTrue(X.is_transient(type('RequestTimeout', (type('NetworkError', (Exception,), {}),), {})('x')))

    def test_fetcher_retries_rate_limit_instead_of_failing_immediately(self):
        n = {'k': 0}

        class Ex:
            async def fetch_ohlcv(self, sym, tf, since=None, limit=None, params=None):
                n['k'] += 1
                if n['k'] <= 2:
                    raise rl_err()
                return [[NOW, 1, 1, 1, 1, 1]]
        sleeps = []

        async def sl(x):
            sleeps.append(x)
        f = X.Fetcher(Ex(), pause=0, sleep=sl)
        got = asyncio.run(f.call('fetch_ohlcv', 'A', '1h', None, 10, {}))
        self.assertEqual(len(got), 1)
        self.assertEqual([x for x in sleeps if x > 0], [3, 8])                                  # лимит запросов: повторы той же страницы 3/8 с (раньше ExchangeError поднималась сразу и кэшировалась на 7 суток)

    def test_loader_does_not_cache_rate_limit_as_permanent_error(self):
        d = tempfile.mkdtemp()
        idx = X.load_index(d)

        class Ex:
            async def fetch_ohlcv(self, sym, tf, since=None, limit=None, params=None):
                raise rl_err()

        async def nosleep(_s):
            return None
        fx = {'bingx': X.Fetcher(Ex(), pause=0, sleep=nosleep)}
        out = asyncio.run(X.load_hourly(d, idx, fx, 'bingx', ['A/USDT:USDT'], NOW, 10 ** 14))
        self.assertEqual(out[:3], (0, 0, 0))                                                    # ни «пусто», ни «ошибка биржи»
        self.assertNotIn('bingx|A/USDT:USDT|h1', idx['neg'])                                    # постоянной записи нет
        self.assertIn('bingx|A/USDT:USDT|h1', idx['fail'])                                      # временная ошибка: символ остаётся «не обработан» и будет запрошен снова
        st = X.hourly_status(idx, [('bingx', 'A/USDT:USDT')])
        self.assertEqual(st['pending'], ['A/USDT:USDT'])


class PauseRetryMigration(unittest.TestCase):
    def test_rate_limit_retry_schedule_3_8_20_45_then_pending(self):
        n = {'k': 0}

        class Ex:
            async def fetch_ohlcv(self, sym, tf, since=None, limit=None, params=None):
                n['k'] += 1
                raise rl_err()
        sleeps = []

        async def sl(x):
            sleeps.append(x)
        f = X.Fetcher(Ex(), pause=0, sleep=sl)
        with self.assertRaises(Exception):
            asyncio.run(f.call('fetch_ohlcv', 'A', '1h', None, 10, {}))
        self.assertEqual(n['k'], 5)                                                              # 1 + 4 повтора той же страницы
        self.assertEqual([x for x in sleeps if x > 0], [3, 8, 20, 45])
        # другая временная ошибка (сеть) — прежнее расписание 2/6/15
        Net = type('RequestTimeout', (type('NetworkError', (Exception,), {}),), {})

        class Ex2:
            async def fetch_ohlcv(self, *a, **k):
                raise Net('x')
        sleeps.clear()
        with self.assertRaises(Exception):
            asyncio.run(X.Fetcher(Ex2(), pause=0, sleep=sl).call('fetch_ohlcv', 'A', '1h', None, 10, {}))
        self.assertEqual([x for x in sleeps if x > 0], [2, 6, 15])
        # режим диагностики без повторов — ничего не повторяется
        n['k'] = 0
        with self.assertRaises(Exception):
            asyncio.run(X.Fetcher(Ex(), pause=0, sleep=sl, delays=()).call('fetch_ohlcv', 'A', '1h', None, 10, {}))
        self.assertEqual(n['k'], 1)

    def test_page_pause_between_bingx_pages_env(self):
        import os

        class Ex:
            def __init__(self):
                self.k = 0

            async def fetch_ohlcv(self, sym, tf, since=None, limit=None, params=None):
                self.k += 1
                return rows((params or {}).get('endTime') or NOW, 1000) if self.k <= 3 else []
        sleeps = []

        async def sl(x):
            sleeps.append(x)
        old = os.environ.get('BINGX_PAGE_PAUSE_SEC')
        try:
            os.environ['BINGX_PAGE_PAUSE_SEC'] = '1.0'
            f = X.Fetcher(Ex(), pause=0, sleep=sl)
            asyncio.run(X.fetch_bars_back(f, 'bingx', 'A', X.SINCE_MS, NOW + HOUR, tf='1h'))
            self.assertEqual([x for x in sleeps if x > 0], [1.0, 1.0, 1.0, 1.0])               # после каждой из 4 страниц (3 с данными + пустая)
            self.assertEqual(X.page_pause('bingx'), 1.0)
            self.assertEqual(X.page_pause('okx'), 0.0)                                            # только BingX
            sleeps.clear()
            os.environ['BINGX_PAGE_PAUSE_SEC'] = '0.25'
            asyncio.run(X.fetch_bars_back(X.Fetcher(Ex(), pause=0, sleep=sl), 'bingx', 'A', X.SINCE_MS, NOW + HOUR, tf='1h'))
            self.assertEqual({x for x in sleeps if x > 0}, {0.25})
            os.environ.pop('BINGX_PAGE_PAUSE_SEC')
            self.assertEqual(X.page_pause('bingx'), 1.0)                                           # по умолчанию 1.0 с
        finally:
            if old is None:
                os.environ.pop('BINGX_PAGE_PAUSE_SEC', None)
            else:
                os.environ['BINGX_PAGE_PAUSE_SEC'] = old

    def test_migration_resets_errors_and_classification_once(self):
        d = tempfile.mkdtemp()
        idx = X.load_index(d)
        idx['neg']['bingx|A/USDT:USDT|h1'] = {'t': 3, 'why': 'ExchangeError'}
        idx['neg']['bingx|B/USDT:USDT|bars'] = {'t': 3, 'why': 'ExchangeError', 'msg': RL}
        idx['neg']['bingx|C/USDT:USDT|h1'] = {'t': 3, 'why': 'empty'}                            # «пусто» — подтверждённый ответ, не сбрасывается
        idx['neg']['bingx|D/USDT:USDT|h1'] = {'t': 3, 'why': 'unavailable'}
        idx['h1class']['bingx|A/USDT:USDT'] = {'cls': 'а1'}
        X.save_index(d, idx)
        idx = X.load_index(d)
        self.assertEqual(X.migrate_v127(d, idx), 3)                                              # 2 ошибки + 1 классификация
        re_ = X.load_index(d)                                                                    # переживает перезапуск (журнал)
        self.assertEqual(sorted(re_['neg']), ['bingx|C/USDT:USDT|h1', 'bingx|D/USDT:USDT|h1'])
        self.assertEqual(re_['h1class'], {})
        self.assertEqual(X.migrate_v127(d, re_), 0)                                              # повторно не выполняется
        st = X.hourly_status(re_, [('bingx', 'A/USDT:USDT')])
        self.assertEqual(st['pending'], ['A/USDT:USDT'])                                         # «не обработано»

    def test_load_reports_reset_line_once(self):
        d = tempfile.mkdtemp()
        idx = X.load_index(d)
        idx['neg']['bingx|A/USDT:USDT|h1'] = {'t': 3, 'why': 'ExchangeError'}
        X.save_index(d, idx)

        class Ex:
            async def load_markets(self):
                return {}

            async def close(self):
                return None

        async def nosleep(_s):
            return None
        txt = asyncio.run(X.run_xload({'xdir': d, 'bdir': tempfile.mkdtemp(), 'venues': ('bingx',)}, None, exchanges={'bingx': Ex()}, now_ms=NOW, sleep=nosleep))
        self.assertIn('↺ v127 (§2.58): сброшено 1 записей-ошибок', txt)
        txt2 = asyncio.run(X.run_xload({'xdir': d, 'bdir': tempfile.mkdtemp(), 'venues': ('bingx',)}, None, exchanges={'bingx': Ex()}, now_ms=NOW, sleep=nosleep))
        self.assertNotIn('↺ v127', txt2)


class Classify(unittest.TestCase):
    def test_report_cases_are_temporary_not_depth_limit(self):
        daily = (NOW - 1500 * DAY, NOW - DAY)
        exc = rl_err()
        # ошибка ПОСЛЕ 15 успешных страниц, 1h за 7 суток есть — раньше класс а1 («ограничение глубины»), теперь в
        mid = {'rows': {}, 'pages_ok': 15, 'end_fail': NOW - 600 * DAY, 'exc': exc, 'complete': False}
        self.assertEqual(H.classify(mid, (168, None), 'активен', daily, [])[0], 'в')
        # ошибка на первой странице при работающей пробе 7 суток (ONT) — раньше г, теперь в
        first = {'rows': {}, 'pages_ok': 0, 'end_fail': None, 'exc': exc, 'complete': False}
        self.assertEqual(H.classify(first, (169, None), 'активен', daily, [])[0], 'в')
        # даже если и проба 7 суток вернула лимит (DIA)
        self.assertEqual(H.classify(mid, (None, rl_err()), 'активен', daily, [])[0], 'в')
        # временная проверяется ПЕРЕД а2/б
        closed = {'rows': {}, 'pages_ok': 0, 'end_fail': None, 'exc': exc, 'complete': False}
        self.assertEqual(H.classify(closed, (None, exc), 'неактивен', daily, [])[0], 'в')
        self.assertEqual(H.classify(closed, (None, exc), 'нет', daily, [('NEW/USDT:USDT', None)])[0], 'в')

    def test_a1_only_with_explicit_no_data_text(self):
        Ex = type('ExchangeError', (Exception,), {})
        daily = (NOW - 1500 * DAY, NOW - DAY)
        mid = lambda msg: {'rows': {}, 'pages_ok': 5, 'end_fail': NOW - 300 * DAY, 'exc': Ex(msg), 'complete': False}
        self.assertEqual(H.classify(mid('bingx {"code":109400,"msg":"startTime out of range"}'), (168, None), 'активен', daily, [])[0], 'а1')
        self.assertEqual(H.classify(mid('bingx {"code":100999,"msg":"weird thing"}'), (168, None), 'активен', daily, [])[0], 'г')    # ошибка без явного «нет данных» — не а1
        self.assertEqual(H.classify(mid('bingx ' + RL), (168, None), 'активен', daily, [])[0], 'в')                                  # лимит запросов — всегда в

    def test_real_depth_error_still_classified_a1(self):
        Ex = type('ExchangeError', (Exception,), {})
        daily = (NOW - 1500 * DAY, NOW - DAY)
        mid = {'rows': {}, 'pages_ok': 5, 'end_fail': NOW - 300 * DAY, 'exc': Ex('bingx {"code":109400,"msg":"startTime out of range"}'), 'complete': False}
        self.assertEqual(H.classify(mid, (168, None), 'активен', daily, [])[0], 'а1')            # не-временная ошибка у границы — по-прежнему а1


class EndToEnd(unittest.TestCase):
    def test_apply_loads_full_history_for_rate_limited_symbols(self):
        syms = ['MID/USDT:USDT', 'FIRST/USDT:USDT']
        d = make_idx(syms)
        state = {}

        def script_factory(first_fail_k):
            def script(k, since, limit, params):
                state[k] = True
                if since is not None:
                    return rows(NOW, 200)
                end = params.get('endTime') or NOW
                if k == first_fail_k:                           # лимит запросов на одном из запросов диагностики; дальше — норма
                    return rl_err()
                if end < NOW - 3000 * HOUR:                     # история заканчивается пустой страницей (глубина BingX)
                    return []
                return rows(end, limit)
            return script
        ex = Fake({s: mk(s.split('/')[0]) for s in syms}, {'MID/USDT:USDT': script_factory(3), 'FIRST/USDT:USDT': script_factory(1)})
        sleeps = []
        txt = run_cmd(d, ex, True, sleeps)
        idx = X.load_index(d)
        self.assertEqual({idx['h1class'][f'bingx|{s}']['cls'] for s in syms}, {'в'})
        for s in syms:
            h = idx['hbars']['bingx'][s]
            self.assertNotIn('limit', h)                        # никакой «ограниченной глубины» по ошибке
            self.assertTrue(h['complete'])
            self.assertNotIn(f'bingx|{s}|h1', idx['neg'])
        self.assertIn('в (временные): загружено после повторов 3/8/20/45 с — 2', txt)
        self.assertIn('итого по классам: в: 2', txt)
        self.assertTrue(ROOT)


class Partial(unittest.TestCase):
    def test_partial_depth_by_first_hour_bar(self):
        d = tempfile.mkdtemp()
        idx = X.load_index(d)
        for s, first_h in (('SHORT/USDT:USDT', NOW - 937 * DAY), ('FULL/USDT:USDT', NOW - 1000 * DAY + 2 * DAY)):
            idx['bars']['bingx'][s] = {'first': NOW - 1000 * DAY, 'last': NOW - DAY, 'n': 999, 't': 3, 'complete': True}
            idx['hbars']['bingx'][s] = {'first': first_h, 'last': NOW, 'n': 10, 't': 3, 'complete': True}
        st = X.hourly_status(idx, [('bingx', 'SHORT/USDT:USDT'), ('bingx', 'FULL/USDT:USDT')])
        self.assertEqual(st['partial'], ['SHORT/USDT:USDT'])                                    # 1h начинается на 63 суток позже дневной истории; у FULL — ≤ 3 суток
        self.assertIn('частичной глубины (§2.57 а1 / §2.58 п. 4, остальной период — дневной базис): 1', X.hourly_line(st))


if __name__ == '__main__':
    unittest.main()
