"""v137 (§2.67): полный список баз архива Binance (monthly ∪ daily, однобуквенные базы), фильтры по нему, safe() для не-ASCII имён, миграция v137, запрос Gate.io с не-ASCII именем. Сеть не используется."""
import asyncio
import os
import tempfile
import time
import unittest

import numpy as np

import _common
import research_bdata as BD
import research_h2ext as HX
import research_h2repl as H2R
import research_venues as RV
import research_xdata as X
import test_v116_h2x1 as T
import test_v129_h2ext_leadlag as T9
import test_v134_ext_filter as T34
from _common import a

ROOT = _common.ROOT
DAY = X.DAY


def run(c):
    return asyncio.run(c)


def xml_pages(pages_by_prefix):
    """Фейковый S3-листинг: {prefix: [страница1(list имён), страница2…]} → fetch(url, params, method)."""
    async def fetch(url, params=None, method='GET'):
        pages = pages_by_prefix.get(params['prefix'])
        if pages is None:
            return {'status': 500, 'headers': {}, 'body': b'', 'error': 'boom'}
        k = int(params.get('marker', '0') or 0) if params.get('marker') else 0
        names = pages[k]
        body = ''.join(f'<CommonPrefixes><Prefix>{params["prefix"]}{n}/</Prefix></CommonPrefixes>' for n in names)
        more = k + 1 < len(pages)
        body += f'<IsTruncated>{"true" if more else "false"}</IsTruncated>' + (f'<NextMarker>{k + 1}</NextMarker>' if more else '')
        return {'status': 200, 'headers': {}, 'body': body.encode(), 'error': ''}
    return fetch


MONTHLY = [['BTCUSDT', 'ETHUSDT', 'LUNAUSDT', 'WUSDT'], ['SUSDT', 'DELISTEDUSDT', 'BTCUSDT_250627', 'BTCUSD', 'ab', '1000PEPEUSDT']]
DAILY = [['COREUSDT', 'WUSDT', 'NEWLISTUSDT', 'ETHUSDT_250926']]
FEED = {BD.KL_PREFIX: MONTHLY, BD.DL_PREFIX: DAILY}


class Listing(unittest.TestCase):
    def test_full_list_is_union_with_single_letter_bases_and_delisted(self):
        res = run(BD.list_symbols_full(xml_pages(FEED)))
        self.assertEqual(res['monthly'], ['1000PEPEUSDT', 'BTCUSDT', 'DELISTEDUSDT', 'ETHUSDT', 'LUNAUSDT', 'SUSDT', 'WUSDT'])
        self.assertEqual(res['daily'], ['COREUSDT', 'NEWLISTUSDT', 'WUSDT'])
        self.assertEqual(res['symbols'], sorted(set(res['monthly']) | set(res['daily'])))
        for s in ('WUSDT', 'SUSDT', 'COREUSDT', 'DELISTEDUSDT', 'NEWLISTUSDT', '1000PEPEUSDT'):
            self.assertIn(s, res['symbols'])
        for s in ('BTCUSDT_250627', 'ETHUSDT_250926', 'BTCUSD', 'ab'):                    # поставочные контракты, не USDT, без суффикса
            self.assertNotIn(s, res['symbols'])

    def test_old_battery_listing_misses_single_letters_and_daily_only(self):
        """Документирует дефект списка батареи (§2.67.1): однобуквенные базы и символы только из daily в нём отсутствовали; сам список батареи не менялся."""
        old = run(BD.list_symbols(xml_pages(FEED)))
        self.assertNotIn('WUSDT', old)
        self.assertNotIn('SUSDT', old)
        self.assertNotIn('COREUSDT', old)
        self.assertIn('DELISTEDUSDT', old)
        self.assertTrue(BD.SYMBOL_RE.pattern.startswith('^[A-Z0-9]{2,20}'))                # регулярка списка батареи не изменена (запечатанные анализы)

    def test_listing_error_is_reported_and_no_file(self):
        d = tempfile.mkdtemp()
        txt = run(BD.run_binance_bases({'bdir': d, 'fetch': xml_pages({BD.KL_PREFIX: MONTHLY})}))              # daily — HTTP 500
        self.assertIn('недоступен', txt)
        self.assertFalse(os.path.exists(BD.full_path(d)))
        self.assertIsNone(BD.load_full(d))

    def test_command_writes_file_and_reports_checks(self):
        d = tempfile.mkdtemp()
        BD.save_index(d, dict(BD.load_index(d), symbols=['BTCUSDT', 'ETHUSDT', 'LUNAUSDT', 'DELISTEDUSDT', '1000PEPEUSDT']))
        txt = run(BD.run_binance_bases({'bdir': d, 'fetch': xml_pages(FEED), 'now': 1_700_000_000}))
        fl = BD.load_full(d)
        self.assertEqual(fl['date'], '2023-11-14')
        self.assertEqual(len(fl['symbols']), 9)
        for needle in ('monthly/klines: 7; daily/klines: 3; объединение: 9', 'в списке батареи: 5', 'в полном, но не в списке батареи: 4', 'W: полный да / батарея нет', 'CORE: полный да / батарея нет',
                       'S: полный да / батарея нет', 'M: полный НЕТ / батарея нет', 'BTC' if False else 'NEWLIST'):
            self.assertIn(needle, txt)

    def test_filters_use_union_but_battery_index_untouched(self):
        d = tempfile.mkdtemp()
        BD.save_index(d, dict(BD.load_index(d), symbols=['BTCUSDT', 'ETHUSDT']))
        self.assertEqual(X.binance_norm_set(d), {'BTC', 'ETH'})                            # файла полного списка нет — только батарея
        BD.save_full(d, {'monthly': ['BTCUSDT'], 'daily': ['WUSDT', 'COREUSDT', '1000PEPEUSDT'], 'symbols': ['1000PEPEUSDT', 'BTCUSDT', 'COREUSDT', 'WUSDT']})
        self.assertEqual(X.binance_norm_set(d), {'BTC', 'ETH', 'W', 'CORE', 'PEPE'})
        self.assertEqual(X.binance_norm_set(d, full=False), {'BTC', 'ETH'})
        self.assertEqual(X.binance_raw_bases(d), ['1000PEPE', 'BTC', 'CORE', 'ETH', 'W'])
        self.assertEqual(BD.load_index(d)['symbols'], ['BTCUSDT', 'ETHUSDT'])              # индекс батареи не менялся


class Universe(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.bdir, cls.xdir = tempfile.mkdtemp(), tempfile.mkdtemp()
        names = T9.BF.write_battery(cls.bdir, n_syms=18)
        BD.save_full(cls.bdir, {'monthly': names, 'daily': ['COREUSDT'], 'symbols': sorted(names + ['WUSDT', 'COREUSDT', 'TRUMPUSDT', 'SUSDT'])})
        T9.write_xdata_ext(cls.xdir, n_new=12)
        T34.add_symbols(cls.xdir, {'mexc': ['SEEDM', 'CRYPB', 'W', 'CORE', 'S'], 'gateio': ['SEEDG', 'CRYPA', 'TRUMPOFFICIAL']})
        cls.ven = T9.VEN

    def test_symbols_from_full_list_are_excluded_and_listed(self):
        series, fund, uni, ref, where, mk = H2R.repl_inputs(self.xdir, self.bdir, self.ven, {'coins': {}, 'meta': {}})
        names = {(u['ex'], u['sym'].split('/')[0]) for u in uni}
        for k in (('mexc', 'W'), ('mexc', 'CORE'), ('mexc', 'S'), ('gateio', 'TRUMPOFFICIAL')):
            self.assertNotIn(k, names, k)
        for k in (('mexc', 'CRYPB'), ('gateio', 'CRYPA')):
            self.assertIn(k, names)
        fo = {(ex, sym.split('/')[0]): (b, kind) for ex, sym, b, kind in ref['full_list_only']}
        self.assertEqual(fo[('mexc', 'W')], ('W', 'exact'))
        self.assertEqual(fo[('mexc', 'CORE')], ('CORE', 'exact'))
        self.assertEqual(fo[('mexc', 'S')], ('S', 'exact'))
        self.assertEqual(fo[('gateio', 'TRUMPOFFICIAL')], ('TRUMP', 'alias'))
        self.assertNotIn(('mexc', 'CRYPB'), fo)
        why = {(ex, sym.split('/')[0]): r for ex, sym, r in ref['excluded_syms']}
        self.assertEqual(why[('mexc', 'W')], 'binance-архив=W')
        self.assertEqual(why[('gateio', 'TRUMPOFFICIAL')], 'binance-alias=TRUMP')

    def test_old_battery_list_alone_would_keep_them(self):
        d = tempfile.mkdtemp()
        T9.BF.write_battery(d, n_syms=18)
        os.remove(BD.full_path(d))                                                          # только список батареи (как до v137)
        series, fund, uni, ref, where, mk = H2R.repl_inputs(self.xdir, d, self.ven, {'coins': {}, 'meta': {}})
        names = {(u['ex'], u['sym'].split('/')[0]) for u in uni}
        for k in (('mexc', 'W'), ('mexc', 'CORE'), ('mexc', 'S')):
            self.assertIn(k, names)                                                         # воспроизведение прод-дефекта
        self.assertEqual(ref['full_list_only'], [])

    def test_report_line_file_section_and_confirm_refusal(self):
        fdir = tempfile.mkdtemp()
        old = HX.K_REPS
        HX.K_REPS = 3
        base = {'xdir': self.xdir, 'bdir': self.bdir, 'fdir': fdir, 'fee_taker': 0.0005, 'fee_maker': 0.0002, 'version': 'vT', 'venues': self.ven, 'coverage_min': 0.0, 'spec_path': '/dev/null',
                'coins_override': {}, 'meta_override': {}}
        try:
            txt = run(HX.run_h2ext(dict(base, mode='show', hyp='ext')))
            self.assertIn('Полный список баз Binance USDT-M (§2.67.2, файл от', txt)
            self.assertIn('исключено благодаря полному списку Binance (база есть в полном списке и нет в списке батареи): 4', txt)
            self.assertIn('mexc:W→W', txt)
            self.assertIn('gateio:TRUMPOFFICIAL→TRUMP', txt)
            self.assertIn('binance (полный список) 3', txt)
            body = open(os.path.join(self.xdir, 'h2ext_universe_review.txt'), encoding='utf-8').read()
            sec = body.split('## исключены благодаря полному списку Binance')[1].split('## binance-alias')[0]
            self.assertIn('mexc\tW/USDT:USDT\tW\texact', sec)
            self.assertIn('gateio\tTRUMPOFFICIAL/USDT:USDT\tTRUMP\talias', sec)
            d2 = tempfile.mkdtemp()
            T9.BF.write_battery(d2, n_syms=18)
            os.remove(BD.full_path(d2))                                                     # нет полного списка: показ — предупреждение, confirm — отказ
            warn = run(HX.run_h2ext(dict(base, bdir=d2, mode='show', hyp='ext')))
            self.assertIn('полный список баз Binance не получен', warn)
            ref = run(HX.run_h2ext(dict(base, bdir=d2, fdir=tempfile.mkdtemp(), mode='confirm', hyp='ext')))
            self.assertIn('нет полного списка баз Binance', ref)
            self.assertIsNone(__import__('research_factory').kv_get('h2_ext_started', fdir))
        finally:
            HX.K_REPS = old


class Safe(unittest.TestCase):
    def test_non_ascii_names_get_distinct_paths_ascii_unchanged(self):
        names = ['哈基米/USDT:USDT', '币安人生/USDT:USDT', '牛来/USDT:USDT', '牛逼/USDT:USDT', '龙虾/USDT:USDT']
        self.assertEqual(len({X.old_safe(n) for n in names}), 1)                           # прежняя safe(): один путь на всех (причина идентичных рядов)
        self.assertEqual(len({X.safe(n) for n in names}), 5)
        self.assertEqual(len({X.bars_path('/d', 'gateio', n) for n in names}), 5)
        self.assertEqual(len({X.fund_path('/d', 'gateio', n) for n in names}), 5)
        for n in ('BTC/USDT:USDT', '1000PEPE/USDT:USDT', 'ZK/USDT:USDT', 'W/USDT:USDT'):
            self.assertEqual(X.safe(n), X.old_safe(n))                                     # ASCII — без изменений
        self.assertEqual(X.safe('ABC币/USDT:USDT').split('~')[0], 'ABC_USDT_USDT')
        self.assertNotEqual(X.safe('ABC币/USDT:USDT'), X.safe('ABC/USDT:USDT'))
        self.assertEqual(X.safe('哈基米/USDT:USDT'), X.safe('哈基米/USDT:USDT'))


def put_old(d, ex, sym, seed, idx):
    b = T.mk_bars(T.D0 + 50, 80, seed, vol=0.05, qv=6e7)
    for path in (os.path.join(d, ex, X.old_safe(sym) + '.d1.npz'),):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        np.savez_compressed(path, ts=b['ts'], o=b['o'], h=b['h'], l=b['l'], c=b['c'], v=b['v'])
    ft, fr = T.mk_fund(T.D0 + 50, 80, seed + 1)
    np.savez_compressed(os.path.join(d, ex, X.old_safe(sym) + '.fr.npz'), ts=ft, rate=fr)
    idx['bars'][ex][sym] = {'first': int(b['ts'][0]), 'last': int(b['ts'][-1]), 'n': 80, 't': 1, 'complete': True}
    idx['fund'][ex][sym] = {'first': int(ft[0]), 'last': int(ft[-1]), 'n': len(ft), 't': 1, 'complete': True}


class Migration(unittest.TestCase):
    def test_v137_resets_only_affected_symbols_once(self):
        d = tempfile.mkdtemp()
        idx = X.load_index(d)
        for i, sym in enumerate(['哈基米/USDT:USDT', '币安人生/USDT:USDT', 'ABC币/USDT:USDT', 'ABC/USDT:USDT', 'XYZ/USDT:USDT']):
            put_old(d, 'gateio', sym, 10 + i, idx)
        put_old(d, 'mexc', 'ABC/USDT:USDT', 99, idx)                                       # другая биржа — не затронута
        X.save_index(d, idx)
        idx = X.load_index(d)
        n = X.migrate_v137(d, idx)
        self.assertEqual(n, 4)
        idx2 = X.load_index(d)
        for sym in ('哈基米/USDT:USDT', '币安人生/USDT:USDT', 'ABC币/USDT:USDT', 'ABC/USDT:USDT'):
            self.assertNotIn(sym, idx2['bars']['gateio'], sym)
            self.assertNotIn(sym, idx2['fund']['gateio'], sym)
        self.assertIn('XYZ/USDT:USDT', idx2['bars']['gateio'])
        self.assertIn('ABC/USDT:USDT', idx2['bars']['mexc'])
        self.assertFalse(os.path.exists(os.path.join(d, 'gateio', '_USDT_USDT.d1.npz')))   # общий файл прежнего пути удалён
        self.assertFalse(os.path.exists(os.path.join(d, 'gateio', 'ABC_USDT_USDT.d1.npz')))
        self.assertTrue(os.path.exists(os.path.join(d, 'gateio', 'XYZ_USDT_USDT.d1.npz')))
        self.assertTrue(os.path.exists(os.path.join(d, 'mexc', 'ABC_USDT_USDT.d1.npz')))
        self.assertEqual(X.migrate_v137(d, idx2), 0)                                        # один раз


class Reload(unittest.TestCase):
    def test_chinese_symbols_load_into_separate_files_with_different_series(self):
        d, bdir = tempfile.mkdtemp(), tempfile.mkdtemp()
        T9.BF.write_battery(bdir, n_syms=6)
        first = int(np.datetime64('2023-01-01', 'D').astype(np.int64))
        names = ['哈基米', '币安人生', '牛来', '牛逼', '龙虾']
        data = {f'{n}/USDT:USDT': T.mk_bars(first, 100, 200 + i, vol=0.06, qv=6e7) for i, n in enumerate(names)}

        class Ex:
            async def load_markets(self):
                return {s: {'swap': True, 'linear': True, 'quote': 'USDT', 'settle': 'USDT', 'base': s.split('/')[0], 'active': True, 'created': None, 'info': {}} for s in data}

            async def fetch_ohlcv(self, sym, tf, since=None, limit=None, params=None):
                return T.ohlcv_window(data[sym], since, limit or 1000, params)

            async def fetch_funding_rate_history(self, sym, since=None, limit=None, params=None):
                ft, fr = T.mk_fund(first, 100, abs(hash(sym)) % 1000)
                end = (params or {}).get('endTime')
                return [{'timestamp': int(t), 'fundingRate': float(r)} for t, r in zip(ft, fr) if end is None or t <= end][-(limit or 100):]

            async def close(self):
                pass
        now = int(np.datetime64('2023-06-01', 'ms').astype(np.int64))
        txt = run(X.run_xload({'xdir': d, 'bdir': bdir, 'venues': ('gateio',), 'ext': True, 'hourly': False, 'max_minutes': 5}, None, exchanges={'gateio': Ex()}, now_ms=now, sleep=T9.nosleep))
        self.assertIn('✅ загрузка завершена', txt)
        series = {s: X.load_bars(d, 'gateio', s)['c'] for s in data}
        self.assertEqual(len({X.bars_path(d, 'gateio', s) for s in data}), 5)
        cs = list(series.values())
        for i in range(5):
            for j in range(i + 1, 5):
                self.assertFalse(np.array_equal(cs[i], cs[j]), (i, j))                      # ряды разные
            self.assertTrue(np.allclose(cs[i], data[list(data)[i]]['c']))
        # запись индекса без файла — считается не загруженной и загружается заново
        os.remove(X.bars_path(d, 'gateio', '龙虾/USDT:USDT'))
        txt2 = run(X.run_xload({'xdir': d, 'bdir': bdir, 'venues': ('gateio',), 'ext': True, 'hourly': False, 'max_minutes': 5}, None, exchanges={'gateio': Ex()}, now_ms=now, sleep=T9.nosleep))
        self.assertTrue(os.path.exists(X.bars_path(d, 'gateio', '龙虾/USDT:USDT')))
        self.assertIn('✅ загрузка завершена', txt2)


class GateEncoding(unittest.TestCase):
    def test_contract_name_passed_as_is_and_utf8_percent_encoded(self):
        seen = []

        async def get(path, params):
            seen.append((path, dict(params)))
            if path.endswith('/contracts'):
                return [{'name': '哈基米_USDT', 'type': 'direct', 'create_time': 1700000000, 'quanto_multiplier': '1'}, {'name': '币安人生_USDT', 'type': 'direct', 'create_time': 1700000000, 'quanto_multiplier': '1'}]
            return []
        g = RV.GatePublic(get=get, pause=0.0, now=lambda: 1_800_000_000_000)
        mk = run(g.load_markets())
        self.assertEqual(set(mk), {'哈基米/USDT:USDT', '币安人生/USDT:USDT'})
        run(g.fetch_ohlcv('哈基米/USDT:USDT', '1d', None, 100, {'endTime': 1_800_000_000_000}))
        run(g.fetch_ohlcv('币安人生/USDT:USDT', '1d', None, 100, {'endTime': 1_800_000_000_000}))
        contracts = [p['contract'] for path, p in seen if path.endswith('/candlesticks')]
        self.assertEqual(contracts, ['哈基米_USDT', '币安人生_USDT'])                        # каждому символу — своё имя контракта
        import yarl
        u = yarl.URL('https://api.gateio.ws/api/v4/futures/usdt/candlesticks').with_query({'contract': '哈基米_USDT'})
        self.assertIn('%E5%93%88%E5%9F%BA%E7%B1%B3_USDT', str(u))                             # UTF-8 percent-encoding (как делает aiohttp)


class Wiring(unittest.TestCase):
    def test_prestep_command_and_docs(self):
        calls = []
        _db, sent = _common.fresh_state()

        async def fake_job(cmd, task, params, payload=None):
            calls.append(task)
            return f'{task}: ок'

        async def fake_battery(cmd, task, extra=None, need_universe=True):
            calls.append('battery:' + extra['mode'])
        orig = (a._research_job, a.battery_job, BD.load_full)
        a._research_job, a.battery_job = fake_job, fake_battery
        try:
            BD.load_full = lambda d: None
            run(a.h2ext_cmd('show'))
            self.assertEqual(calls[:2], ['binance_bases', 'h2_coins'])
            calls.clear()
            BD.load_full = lambda d: {'t': time.time() - 8 * 86400, 'symbols': ['BTCUSDT']}             # старше 7 суток
            run(a.h2ext_cmd('show'))
            self.assertEqual(calls[0], 'binance_bases')
            calls.clear()
            BD.load_full = lambda d: {'t': time.time() - 86400, 'symbols': ['BTCUSDT']}
            run(a.h2ext_cmd('show'))
            self.assertEqual(calls[0], 'h2_coins')
            calls.clear()
            run(a.h2ext_cmd('confirm'))
            self.assertEqual(calls, ['battery:confirm'])
        finally:
            a._research_job, a.battery_job, BD.load_full = orig
        pool, allv = a.command_pools()
        self.assertIn('/binance_bases', allv)
        self.assertIsNone(a.command_hint('/binance_bases'))
        import research_runner as R
        self.assertIn('binance_bases', R.TASKS)
        src = open(os.path.join(ROOT, 'BOT_SPEC.md'), encoding='utf-8').read()
        for needle in ('### 2.67.', '#### 2.67.1', '#### 2.67.2', '#### 2.67.3', '#### 2.67.4', '| v137 |', 'binance_bases.json'):
            self.assertIn(needle, src)
        self.assertTrue(a.CODE_VERSION.endswith('v137'))
        self.assertEqual(a.CHANGELOG[0][0], '2026-11-27-v137')
        for ln in src.split('\n'):
            self.assertFalse(ln.startswith('H2EXT-UNIVERSE:') or ln.startswith('H2EXT-K:'))


if __name__ == '__main__':
    unittest.main()
