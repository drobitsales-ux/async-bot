"""v125 (BOT_SPEC §2.56): диагностика ошибок загрузки 1h — сообщение биржи, число страниц до ошибки, сводка по классам; `/xfund_test load retry` сбрасывает записи-ошибки (не «пусто»)."""
import asyncio
import os
import tempfile
import unittest

import _common
import research_xdata as X
import research_xfund as XF

ROOT = _common.ROOT
HOUR = 3_600_000
NOW = 1_800_000_000_000


class Ex:
    """Свечи 1h: sym 'GOOD' — 1500 баров назад от endTime, затем пусто; 'LATE' — первая страница есть, вторая — ошибка биржи; 'EARLY' — ошибка сразу; 'EMPTY' — пусто."""

    def __init__(self):
        self.calls = {}

    async def fetch_ohlcv(self, sym, tf, since=None, limit=None, params=None):
        k = self.calls[sym] = self.calls.get(sym, 0) + 1
        Bad = type('ExchangeError', (Exception,), {})
        if sym == 'EARLY/USDT:USDT':
            raise Bad('bingx {"code":109400,"msg":"invalid symbol or interval"}')
        if sym == 'EMPTY/USDT:USDT':
            return []
        end = (params or {}).get('endTime') or NOW
        if sym == 'LATE/USDT:USDT':
            if k >= 2:
                raise Bad('bingx {"code":109400,"msg":"startTime must be after listing"}')
            return [[end - i * HOUR, 1.0, 1.1, 0.9, 1.0, 1.0] for i in range(limit)][::-1]
        if k > 2:
            return []
        n = limit if k == 1 else 500
        return [[end - i * HOUR, 1.0, 1.1, 0.9, 1.0, 1.0] for i in range(n)][::-1]


def load(d, idx, syms, ex=None):
    async def nosleep(_s):
        return None
    fx = {'bingx': X.Fetcher(ex or Ex(), pause=0, sleep=nosleep)}
    return asyncio.run(X.load_hourly(d, idx, fx, 'bingx', syms, NOW + HOUR, 10 ** 13))


SYMS = ['GOOD/USDT:USDT', 'LATE/USDT:USDT', 'EARLY/USDT:USDT', 'EMPTY/USDT:USDT']


class Diagnostics(unittest.TestCase):
    def test_neg_record_has_message_pages_and_rows(self):
        d = tempfile.mkdtemp()
        idx = X.load_index(d)
        out = load(d, idx, SYMS)
        self.assertEqual(out[:3], (1, 1, 2))                                                   # загружено 1, пусто 1, ошибок 2
        late, early = idx['neg']['bingx|LATE/USDT:USDT|h1'], idx['neg']['bingx|EARLY/USDT:USDT|h1']
        self.assertEqual((late['why'], late['pages'], late['rows']), ('ExchangeError', 1, 1000))   # ошибка ПОСЛЕ успешной страницы (у границы истории?)
        self.assertIn('startTime must be after listing', late['msg'])
        self.assertEqual((early['why'], early['pages'], early['rows']), ('ExchangeError', 0, 0))   # ошибка на первой странице
        self.assertIn('109400', early['msg'])
        self.assertEqual(idx['neg']['bingx|EMPTY/USDT:USDT|h1']['why'], 'empty')

    def test_error_summary_groups_and_counts(self):
        d = tempfile.mkdtemp()
        idx = X.load_index(d)
        load(d, idx, SYMS)
        lines = X.error_lines(idx, 'h1')
        txt = '\n'.join(lines)
        self.assertEqual(len(lines), 2)                                                        # два разных сообщения → две группы
        self.assertIn('ExchangeError ×1', lines[0])
        self.assertIn('ошибка на первой странице: 0, после успешных страниц: 1, собрано строк до ошибки: 1000', txt)
        self.assertIn('ошибка на первой странице: 1, после успешных страниц: 0', txt)
        self.assertNotIn('EMPTY', txt)                                                         # «подтверждённо пусто» — не ошибка
        self.assertEqual(X.error_lines(idx, 'h1', [('bingx', 'GOOD/USDT:USDT')]), [])

    def test_coverage_refusal_lists_exchange_errors(self):
        import battery_fixtures as BF
        d, b = tempfile.mkdtemp(), tempfile.mkdtemp()
        BF.write_battery(b, n_syms=18)
        idx = X.load_index(d)
        syms = []
        for base in ['BTC', 'ETH', 'SOL', 'BNB', 'XRP']:
            s = f'{base}/USDT:USDT'
            syms.append(('bingx', s))
            idx['markets']['bingx'][s] = {'base': base, 'norm': base, 'active': True, 'created': None, 'excl': None}
            idx['bars']['bingx'][s] = {'first': 1, 'last': 2, 'n': 2, 't': 3, 'complete': True}
            idx['fund']['bingx'][s] = {'first': 1, 'last': 2, 'n': 2, 't': 3, 'complete': True}
        idx['hbars']['bingx']['BTC/USDT:USDT'] = {'first': 1, 'last': 2, 'n': 2, 't': 3, 'complete': True}
        for _ex, s in syms[1:]:
            idx['neg'][f'bingx|{s}|h1'] = {'t': 3, 'why': 'ExchangeError', 'msg': 'bingx {"code":109400}', 'pages': 0, 'rows': 0}
        X.save_index(d, idx)
        c = X.coverage_report(d, 'x1', b, hourly_syms=syms)
        self.assertFalse(c['ok'])
        self.assertIn('ошибки h1: ExchangeError ×4', '; '.join(c['lines']))                    # в отказе видно, КАКАЯ ошибка
        self.assertIn('109400', '; '.join(c['lines']))


class Retry(unittest.TestCase):
    def test_clear_drops_errors_keeps_empty_and_symbols_are_requested_again(self):
        d = tempfile.mkdtemp()
        idx = X.load_index(d)
        ex = Ex()
        load(d, idx, SYMS, ex)
        before = dict(ex.calls)
        load(d, idx, SYMS, ex)
        self.assertEqual(ex.calls, before)                                                     # без retry постоянные ошибки НЕ перезапрашиваются (кэш 7 суток)
        n = X.clear_neg_errors(d, idx)
        self.assertEqual(n, 2)
        self.assertIn('bingx|EMPTY/USDT:USDT|h1', idx['neg'])                                  # «подтверждённо пусто» не сбрасывается
        self.assertNotIn('bingx|LATE/USDT:USDT|h1', idx['neg'])
        reloaded = X.load_index(d)                                                             # сброс переживает перезапуск (журнал)
        self.assertNotIn('bingx|EARLY/USDT:USDT|h1', reloaded['neg'])
        self.assertIn('bingx|EMPTY/USDT:USDT|h1', reloaded['neg'])
        load(d, reloaded, SYMS, ex)
        self.assertGreater(ex.calls['EARLY/USDT:USDT'], before['EARLY/USDT:USDT'])             # запрошен снова
        self.assertEqual(ex.calls['EMPTY/USDT:USDT'], before['EMPTY/USDT:USDT'])

    def test_xfund_load_retry_reports_cleared_count(self):
        d = tempfile.mkdtemp()
        idx = X.load_index(d)
        load(d, idx, SYMS)
        X.save_index(d, idx)
        saved = (X.run_xload, XF.prelim_traded, XF.sync_binance_h1, XF.load_traded_hourly)
        seen = {}

        async def fake_load(params, progress=None, **k):
            seen['n_neg_errors'] = sum(1 for r in X.load_index(d)['neg'].values() if r.get('why') != 'empty')
            return 'ЗАГРУЗКА'

        async def fake_async(*a, **k):
            return 'x'
        X.run_xload, XF.prelim_traded, XF.sync_binance_h1, XF.load_traded_hourly = fake_load, (lambda *a, **k: ([], 0)), fake_async, fake_async
        try:
            txt = asyncio.run(XF.run_xfund({'mode': 'load', 'retry': True, 'xdir': d, 'bdir': tempfile.mkdtemp(), 'fdir': tempfile.mkdtemp(), 'x1_pair': 'binance'}))
            txt2 = asyncio.run(XF.run_xfund({'mode': 'load', 'xdir': d, 'bdir': tempfile.mkdtemp(), 'fdir': tempfile.mkdtemp(), 'x1_pair': 'binance'}))
        finally:
            X.run_xload, XF.prelim_traded, XF.sync_binance_h1, XF.load_traded_hourly = saved
        self.assertTrue(txt.startswith('↺ сброшено отрицательных записей-ошибок: 2'), txt)
        self.assertEqual(seen['n_neg_errors'], 0)                                              # к моменту загрузки записи-ошибки уже сброшены
        self.assertFalse(txt2.startswith('↺'))

    def test_command_parsing_and_help(self):
        src = open(os.path.join(ROOT, 'async_bot.py'), encoding='utf-8').read()
        self.assertIn("'retry': _a == 'load' and 'retry' in _args[1:]", src)
        self.assertIn('/xfund_test [load [retry]]', src)


if __name__ == '__main__':
    unittest.main()
