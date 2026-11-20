"""v125 (BOT_SPEC §2.57): /xfund_1h_errors — диагностика и классификация ошибок 1h BingX: каждый класс (а1, а2, б, в, г) ведёт к своему исходу; «подтверждённо недоступно» расчёт не блокирует, ошибка класса г — блокирует."""
import asyncio
import os
import tempfile
import unittest

import _common
import battery_fixtures as BF
import research_h1err as H
import research_xdata as X

ROOT = _common.ROOT
HOUR, DAY = 3_600_000, 86_400_000
NOW = 1_800_000_000_000


def rows(end, n):
    return [[end - i * HOUR, 1.0, 1.1, 0.9, 1.0, 1.0] for i in range(n)]


def mk(base, active=True, created=None):
    return {'swap': True, 'linear': True, 'quote': 'USDT', 'settle': 'USDT', 'base': base, 'active': active, 'created': created, 'info': {}}


def Err(msg, cls='ExchangeError'):
    return type(cls, (Exception,), {})(msg)


class Fake:
    """BingX-подобный клиент со сценариями по символам: script[sym](k, since, limit, params) → строки либо исключение; k — номер вызова символа."""

    def __init__(self, markets, script):
        self.markets, self.script, self.calls = markets, script, {}

    async def load_markets(self):
        return self.markets

    async def fetch_ohlcv(self, sym, tf, since=None, limit=None, params=None):
        k = self.calls[sym] = self.calls.get(sym, 0) + 1
        r = self.script[sym](k, since, limit, params or {})
        if isinstance(r, Exception):
            raise r
        return r

    async def close(self):
        return None


def depth(k, since, limit, params):                       # а1: первая страница есть, вторая — ошибка границы; за 7 суток данные есть
    if since is not None:
        return rows(NOW, 200)
    if params.get('endTime') is None:
        return rows(NOW, limit)
    return Err('bingx {"code":109400,"msg":"startTime/endTime out of range"}')


def closed(k, since, limit, params):                      # а2: символа нет
    return Err('bingx {"code":109400,"msg":"symbol does not exist"}')


def make_idx(syms):
    d = tempfile.mkdtemp()
    idx = X.load_index(d)
    for s in syms:
        idx['bars']['bingx'][s] = {'first': NOW - 400 * DAY, 'last': NOW - DAY, 'n': 399, 't': 3, 'complete': True}
        idx['neg'][f'bingx|{s}|h1'] = {'t': 3, 'why': 'ExchangeError', 'msg': 'старое', 'pages': 0, 'rows': 0}
    idx['migr']['bingx|v127_reset'] = {'t': 1, 'n': 0}                                     # разовый сброс v127 уже выполнен (иначе записи-ошибки были бы сброшены в «не обработано»)
    X.save_index(d, idx)
    return d


SYMS = ['DEPTH/USDT:USDT', 'CLOSED/USDT:USDT', '1000RENAME/USDT:USDT', 'TRANS/USDT:USDT', 'TRANS2/USDT:USDT', 'ODD/USDT:USDT']


def scenario():
    live = {'DEPTH/USDT:USDT': mk('DEPTH', True, NOW - 300 * DAY), 'CLOSED/USDT:USDT': mk('CLOSED', False), 'RENAME/USDT:USDT': mk('RENAME', True, NOW - 100 * DAY),
            'TRANS/USDT:USDT': mk('TRANS'), 'TRANS2/USDT:USDT': mk('TRANS2'), 'ODD/USDT:USDT': mk('ODD')}

    def renamed_old(k, since, limit, params):
        return Err('bingx {"code":109400,"msg":"invalid symbol"}')

    def renamed_new(k, since, limit, params):
        return rows(NOW, limit) if params.get('endTime') is None else []

    def trans(k, since, limit, params):                   # первые 5 вызовов — лимит запросов, дальше нормально
        if k <= 5:
            return Err('bingx {"code":100410,"msg":"rate limit exceeded"}')
        return rows(NOW, limit) if params.get('endTime') is None else []

    def trans2(k, since, limit, params):
        return Err('bingx {"code":100410,"msg":"rate limit exceeded"}')

    def odd(k, since, limit, params):                     # г: ошибка на первой странице при том, что запрос за 7 суток работает, текст нестандартный
        if since is not None:
            return rows(NOW, 200)
        return Err('bingx {"code":100999,"msg":"weird thing"}')
    script = {'DEPTH/USDT:USDT': depth, 'CLOSED/USDT:USDT': closed, '1000RENAME/USDT:USDT': renamed_old, 'RENAME/USDT:USDT': renamed_new,
              'TRANS/USDT:USDT': trans, 'TRANS2/USDT:USDT': trans2, 'ODD/USDT:USDT': odd}
    return Fake(live, script)


def run_cmd(d, ex, apply_, sleeps=None):
    async def sl(x):
        if sleeps is not None:
            sleeps.append(x)
    os.environ['HTF_FETCH_PAUSE_SEC'] = '0'
    try:
        return asyncio.run(H.run_h1_errors({'xdir': d, 'apply': apply_}, None, {'bingx': ex}, now_ms=NOW, sleep=sl))
    finally:
        os.environ.pop('HTF_FETCH_PAUSE_SEC', None)


class Classes(unittest.TestCase):
    def test_diagnostics_classifies_each_symbol_without_changing_data(self):
        d = make_idx(SYMS)
        txt = run_cmd(d, scenario(), False)
        idx = X.load_index(d)
        cls = {s: idx['h1class'][f'bingx|{s}']['cls'] for s in SYMS}
        self.assertEqual(cls, {'DEPTH/USDT:USDT': 'а1', 'CLOSED/USDT:USDT': 'а2', '1000RENAME/USDT:USDT': 'б', 'TRANS/USDT:USDT': 'в', 'TRANS2/USDT:USDT': 'в', 'ODD/USDT:USDT': 'г'})
        self.assertEqual(idx['hbars']['bingx'], {})                                           # диагностика данные не меняет
        self.assertTrue(all(idx['neg'][f'bingx|{s}|h1']['why'] == 'ExchangeError' for s in SYMS))
        self.assertIn('Данные не менялись', txt)
        for need in ('DEPTH/USDT:USDT: startTime=—, endTime=', 'код 109400', 'symbol does not exist', 'markets: неактивен', 'markets: нет', '7 суток: 200 свечей', 'листинг ', 'markets.created',
                     'первый дневной бар', 'класс а1', 'класс а2', 'класс б', 'класс в', 'класс г', 'Сводка по коду ошибки', 'код 109400 «', 'итого по классам: а1: 1, а2: 1, б: 1, в: 2, г: 1'):
            self.assertIn(need, txt)
        self.assertIn('кандидат по базе: RENAME/USDT:USDT', txt)
        self.assertIn('страниц до ошибки 1, строк 1000', txt)                                  # а1: ошибка после успешной страницы

    def test_apply_leads_each_class_to_its_own_outcome(self):
        d = make_idx(SYMS)
        sleeps = []
        txt = run_cmd(d, scenario(), True, sleeps)
        idx = X.load_index(d)
        st = X.hourly_status(idx, [('bingx', s) for s in SYMS])
        # а1: загружена доступная часть, помечена depth, complete=False
        h = idx['hbars']['bingx']['DEPTH/USDT:USDT']
        self.assertEqual((h['n'], h['complete'], h['limit']), (1000, False, 'depth'))
        self.assertIn('DEPTH/USDT:USDT', st['partial'])
        # а2: подтверждённо недоступны — в гейт не идёт
        self.assertEqual(idx['neg']['bingx|CLOSED/USDT:USDT|h1']['why'], 'unavailable')
        self.assertEqual(st['unavailable'], ['CLOSED/USDT:USDT'])
        # б: ряд новой биржевой записи сохранён под СТАРЫМ ключом, соответствие записано
        self.assertIn('1000RENAME/USDT:USDT', idx['hbars']['bingx'])
        self.assertEqual(idx['alias']['bingx|1000RENAME/USDT:USDT']['new'], 'RENAME/USDT:USDT')
        self.assertEqual(st['alias'], ['1000RENAME/USDT:USDT'])
        # в: после повторов 3/8/20/45 с — TRANS загрузилась, TRANS2 снова ошибка (в гейт)
        self.assertIn('TRANS/USDT:USDT', idx['hbars']['bingx'])
        self.assertEqual(idx['neg']['bingx|TRANS2/USDT:USDT|h1']['why'], 'ExchangeError')
        self.assertEqual([x for x in sleeps if x > 0][:7], [3, 8, 20, 3, 8, 20, 45])        # TRANS: 3, 8, 20 → успех; TRANS2: 3, 8, 20, 45 → ошибка
        # г: остаётся ошибкой
        self.assertEqual(idx['neg']['bingx|ODD/USDT:USDT|h1']['msg'], 'старое')
        self.assertEqual(sorted(st['error']), ['ODD/USDT:USDT', 'TRANS2/USDT:USDT'])
        for need in ('а1 (ограничение глубины): загружена доступная часть 1h у 1', 'а2 (подтверждённо недоступны): 1', 'б (переименованы): 1 — соответствие: 1000RENAME/USDT:USDT → RENAME/USDT:USDT',
                     'в (временные): загружено после повторов 3/8/20/45 с — 1, снова ошибка (в гейт) — 1', 'г (прочее): 1'):
            self.assertIn(need, txt)

    def test_gate_unavailable_does_not_block_but_class_g_does(self):
        base = ['BTC', 'ETH', 'SOL', 'BNB', 'XRP', 'ADA', 'DOGE', 'AVAX', 'LINK', 'DOT']
        d, b = tempfile.mkdtemp(), tempfile.mkdtemp()
        BF.write_battery(b, n_syms=18)
        idx = X.load_index(d)
        syms = []
        for bs in base:
            s = f'{bs}/USDT:USDT'
            syms.append(('bingx', s))
            idx['markets']['bingx'][s] = {'base': bs, 'norm': bs, 'active': True, 'created': None, 'excl': None}
            idx['bars']['bingx'][s] = {'first': 1, 'last': 2, 'n': 2, 't': 3, 'complete': True}
            idx['fund']['bingx'][s] = {'first': 1, 'last': 2, 'n': 2, 't': 3, 'complete': True}
        # 5 загружены (в т. ч. частичная глубина и alias), 5 подтверждённо недоступны (а2) → расчёт не блокируется
        for k, (_e, s) in enumerate(syms):
            if k < 5:
                idx['hbars']['bingx'][s] = {'first': 1, 'last': 2, 'n': 2, 't': 3, 'complete': k != 0, **({'limit': 'depth'} if k == 0 else {})}
            else:
                idx['neg'][f'bingx|{s}|h1'] = {'t': 3, 'why': 'unavailable', 'msg': 'нет данных'}
        idx['alias']['bingx|ETH/USDT:USDT'] = {'new': 'ETH2/USDT:USDT', 't': 3}
        X.save_index(d, idx)
        c = X.coverage_report(d, 'x1', b, hourly_syms=syms)
        self.assertTrue(c['ok'], c['lines'])
        line = X.hourly_line(c['hourly'])
        self.assertIn('подтверждённо недоступны (§2.57 а2, дневной запасной базис): 5', line)
        self.assertIn('частичной глубины (§2.57 а1 / §2.58 п. 4, остальной период — дневной базис): 1', line)
        self.assertIn('через соответствие переименованию (§2.57 б): 1', line)
        # два символа класса г (ошибка в гейте) → 8/10 < 90% — расчёт блокируется
        for _e, s in syms[5:7]:
            idx['neg'][f'bingx|{s}|h1'] = {'t': 3, 'why': 'ExchangeError', 'msg': 'weird thing', 'pages': 0, 'rows': 0}
        X.save_index(d, idx)
        c = X.coverage_report(d, 'x1', b, hourly_syms=syms)
        self.assertFalse(c['ok'])

    def test_rule_order_a2_yields_to_rename_candidate(self):
        p = {'rows': {}, 'pages_ok': 0, 'end_fail': None, 'exc': Err('bingx {"code":109400,"msg":"invalid symbol"}'), 'complete': False}
        daily = (NOW - 400 * DAY, NOW - DAY)
        self.assertEqual(H.classify(p, (None, p['exc']), 'нет', daily, [])[0], 'а2')
        self.assertEqual(H.classify(p, (None, p['exc']), 'нет', daily, [('NEW/USDT:USDT', None)])[0], 'б')       # есть преемник — не «закрыт», а переименован
        p2 = dict(p, exc=Err('x', 'RequestTimeout'))
        self.assertEqual(H.classify(p2, (None, p2['exc']), 'активен', daily, [])[0], 'в')                          # сетевой класс — временная
        self.assertEqual(H.classify(dict(p, exc=None), (5, None), 'активен', daily, [])[0], 'в')                    # не воспроизвелась — временная
        self.assertEqual(H.classify(dict(p, exc=Err('weird')), (5, None), 'активен', daily, [])[0], 'г')

    def test_no_errors_message_and_command_wiring(self):
        d = tempfile.mkdtemp()
        self.assertIn('записей-ошибок загрузки 1h BingX нет', run_cmd(d, scenario(), False))
        src = open(os.path.join(ROOT, 'async_bot.py'), encoding='utf-8').read()
        self.assertIn("{'apply': 'apply' in _args}", src)
        self.assertIn("'/xfund_1h_errors [apply]", src)
        import research_runner as RR
        self.assertIn('xfund_1h_errors', RR.TASKS)


if __name__ == '__main__':
    unittest.main()
