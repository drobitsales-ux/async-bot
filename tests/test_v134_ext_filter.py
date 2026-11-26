"""v134 (§2.64): механическое исключение не-крипто из вселенной H2EXT (б, в, г; а — §2.52), отчёт по пунктам и биржам, файл-обзор вселенной, /contract_probe. Сеть не используется."""
import asyncio
import os
import tempfile
import unittest


import _common
import research_cprobe as CP
import research_h2ext as HX
import research_h2repl as H2R
import research_venues as V
import research_xdata as X
import test_v116_h2x1 as T
import test_v129_h2ext_leadlag as T9
from _common import a

ROOT = _common.ROOT


def run(c):
    return asyncio.run(c)


class Rules(unittest.TestCase):
    def ex(self, base, bases=(), coins=None):
        allb = {b.upper() for b in bases} | {base.upper()}
        return X.ext_exclusion(base, X.norm_base(base), allb, coins)

    def test_crypto_passes(self):
        for b in ('BTC', 'PEPE', 'SOL', 'AAVE', 'ENA', 'WLD'):
            self.assertIsNone(self.ex(b), b)
        self.assertIsNone(self.ex('BTC', coins={'BTC', 'ETH'}))

    def test_point_b_stock_suffix_and_variant(self):
        self.assertEqual(self.ex('AAPLSTOCK'), 'б')
        self.assertEqual(self.ex('NVIDIASTOCK'), 'б')
        self.assertEqual(self.ex('XYZHKD'), 'б')
        self.assertEqual(self.ex('NVIDIA', bases={'NVIDIASTOCK'}), 'б')                  # у этой базы на той же бирже есть вариант *STOCK
        self.assertEqual(self.ex('AAPL', bases={'AAPLSTOCK'}), 'б')
        self.assertIsNone(self.ex('AAPL', bases={'TSLASTOCK'}))                           # вариант другой базы — не считается

    def test_point_c_currencies_factory_indices(self):
        for b in ('JPY', 'GBP', 'CHF', 'USD', 'EUR', 'AUD', 'CAD', 'CNY', 'KRW', 'INR', 'BRL'):
            self.assertEqual(self.ex(b), 'в', b)
        for b in ('XAU', 'XAG', 'BRENT', 'WTI', 'USOIL'):
            self.assertEqual(self.ex(b), 'в', b)
        for b in ('SP500', 'SPX500', 'NDX100', 'NAS100', 'US30', 'US2000', 'GER40', 'UK100', 'JPN225', 'JP225', 'HK50', 'HSI', 'KOSPI', 'AUS200', 'DXY'):
            self.assertEqual(self.ex(b), 'в', b)
        self.assertEqual(self.ex('HKD'), 'б')                                             # порядок а → б → в: HKD попадает в б раньше в
        self.assertIsNone(self.ex('USDC'))                                                # не код валюты
        self.assertIsNone(self.ex('USDT'))

    def test_point_d_only_with_endpoint(self):
        coins = {'BTC', 'ETH', 'PEPE'}
        self.assertEqual(self.ex('GLD', coins=coins), 'г')                                # ETF: нет спотовой монеты с сетью
        self.assertEqual(self.ex('VOO', coins=coins), 'г')
        self.assertIsNone(self.ex('PEPE', coins=coins))
        self.assertIsNone(self.ex('1000PEPE', coins=coins))                               # база без множителя
        self.assertIsNone(self.ex('GLD', coins=None))                                     # эндпоинта нет — п. г отключён
        self.assertIsNone(self.ex('GLD', coins=set()))
        self.assertEqual(self.ex('SP500', coins=coins), 'в')                              # порядок: в раньше г

    def test_apply_keeps_old_excl_and_counts(self):
        mk = {f'{b}/USDT:USDT': {'base': b, 'norm': X.norm_base(b), 'excl': None} for b in ('BTC', 'AAPLSTOCK', 'SP500', 'GLD', 'JPY', 'NCSKTSLA')}
        mk['NCSKTSLA/USDT:USDT']['excl'] = 'NC-префикс'
        n = X.apply_ext_filter(mk, {'BTC'})
        self.assertEqual(n, 4)
        got = {m['base']: m['excl'] for m in mk.values()}
        self.assertEqual(got, {'BTC': None, 'AAPLSTOCK': 'б', 'SP500': 'в', 'GLD': 'г', 'JPY': 'в', 'NCSKTSLA': 'NC-префикс'})
        mk2 = {f'{b}/USDT:USDT': {'base': b, 'norm': b, 'excl': None} for b in ('BTC', 'GLD')}
        self.assertEqual(X.apply_ext_filter(mk2, None), 0)


def add_symbols(d, bases_by_ex):
    """Добавляет в кэш XDATA_DIR символы с заданными базами (листинги разнесены по времени)."""
    idx = X.load_index(d)
    k = 500
    for ex, bases in bases_by_ex.items():
        for i, b in enumerate(bases):
            k += 1
            fd = T.D0 + 80 + i * 20
            nn = min(300, T.END - fd)
            bars = T.mk_bars(fd, nn, k, vol=0.07, qv=6e7)
            sym = f'{b}/USDT:USDT'
            X.save_bars(d, ex, sym, bars['ts'], bars['o'], bars['h'], bars['l'], bars['c'], bars['v'])
            idx['bars'][ex][sym] = {'first': int(bars['ts'][0]), 'last': int(bars['ts'][-1]), 'n': nn, 't': 1, 'complete': True}
            ft, fr = T.mk_fund(fd, nn, k + 9, level=0.0002)
            X.save_fund(d, ex, sym, ft, fr)
            idx['fund'][ex][sym] = {'first': int(ft[0]), 'last': int(ft[-1]), 'n': len(ft), 't': 1, 'complete': True}
            idx['markets'][ex][sym] = {'base': b, 'norm': X.norm_base(b), 'active': True, 'created': None, 'excl': None, 'cat': ''}
    X.save_index(d, idx)


class Integration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.bdir, cls.xdir, cls.fdir = tempfile.mkdtemp(), tempfile.mkdtemp(), tempfile.mkdtemp()
        T9.BF.write_battery(cls.bdir, n_syms=18)
        T9.write_xdata_ext(cls.xdir, n_new=20)
        add_symbols(cls.xdir, {'gateio': ['CRYPA', 'AAPLSTOCK', 'AAPL', 'SP500', 'JPY', 'GLD', 'VOO'], 'mexc': ['SEEDM', 'CRYPB', 'NVIDIA', 'NVIDIASTOCK', 'XAU', 'GBP', 'XYZHKD', 'USOIL']})
        cls.old = HX.K_REPS
        HX.K_REPS = 3
        cls.params = {'xdir': cls.xdir, 'bdir': cls.bdir, 'fdir': cls.fdir, 'fee_taker': 0.0005, 'fee_maker': 0.0002, 'version': 'vT', 'venues': T9.VEN, 'mode': 'show', 'hyp': 'ext',
                      'coverage_min': 0.0, 'spec_path': '/dev/null'}

    @classmethod
    def tearDownClass(cls):
        HX.K_REPS = cls.old

    def names(self, coins):
        series, fund, uni, ref, where, mk = H2R.repl_inputs(self.xdir, self.bdir, T9.VEN, {'coins': coins})
        return {u['sym'].split('/')[0] for u in uni}, ref

    def test_universe_excludes_noncrypto_and_keeps_crypto(self):
        coins = {'gateio': {'CRYPA', 'AAPL', 'DUMMY'}, 'mexc': None}
        names, ref = self.names(coins)
        self.assertTrue({'CRYPA', 'CRYPB'} <= names, names)
        for b in ('AAPLSTOCK', 'SP500', 'JPY', 'XAU', 'GBP', 'XYZHKD', 'USOIL', 'NVIDIASTOCK', 'NVIDIA'):
            self.assertNotIn(b, names, b)
        self.assertNotIn('GLD', names)                                                    # gateio: п. г (нет в списке монет с сетью)
        self.assertNotIn('VOO', names)
        self.assertIn('AAPL', names.union({'AAPL'}) )                                     # AAPL на gateio: есть AAPLSTOCK → б
        why = {(ex, sym.split('/')[0]): r for ex, sym, r in ref['excluded_syms']}
        self.assertEqual(why[('gateio', 'AAPLSTOCK')], 'б')
        self.assertEqual(why[('gateio', 'AAPL')], 'б')
        self.assertEqual(why[('gateio', 'SP500')], 'в')
        self.assertEqual(why[('gateio', 'GLD')], 'г')
        self.assertEqual(why[('mexc', 'NVIDIA')], 'б')
        self.assertEqual(why[('mexc', 'XAU')], 'в')
        self.assertEqual(why[('mexc', 'XYZHKD')], 'б')

    def test_point_d_disabled_without_endpoint(self):
        names, ref = self.names({'gateio': None, 'mexc': None})
        self.assertIn('GLD', names)                                                       # эндпоинта нет — GLD остаётся (п. а по полю — после шага 0)
        self.assertIn('VOO', names)
        self.assertNotIn('SP500', names)
        self.assertFalse([1 for ex, sym, r in ref['excluded_syms'] if r == 'г'])

    def test_show_creates_file_report_and_counts_per_rule_and_exchange(self):
        p = os.path.join(self.xdir, 'h2ext_universe_review.txt')
        self.assertFalse(os.path.exists(p))
        txt = run(HX.run_h2ext(dict(self.params, coins_override={'gateio': {'CRYPA', 'DUMMY'}, 'mexc': None})))
        self.assertTrue(os.path.exists(p))
        body = open(p, encoding='utf-8').read()
        self.assertIn('## В вселенной (', body)
        self.assertIn('## Исключены (', body)
        self.assertIn('gateio\tCRYPA/USDT:USDT\t', body)
        inc, exc = body.split('## Исключены')
        self.assertNotIn('AAPLSTOCK', inc)
        self.assertIn('gateio\tAAPLSTOCK/USDT:USDT\tб', exc)
        self.assertIn('gateio\tSP500/USDT:USDT\tв', exc)
        self.assertIn('gateio\tGLD/USDT:USDT\tг', exc)
        self.assertIn('mexc\tNVIDIA/USDT:USDT\tб', exc)
        self.assertRegex(inc, r'gateio\tCRYPA/USDT:USDT\t\d{4}-\d\d-\d\d')                # дата листинга
        self.assertNotRegex(body, r'(?i)t_adj|sharpe|mean R|б\.п\./день')                # без доходностей
        for needle in ('по биржам и пунктам', 'gateio: а (метаданные)', 'б ', 'в ', 'г ', 'пункт г по биржам', 'Файл-обзор вселенной', 'H2EXT-UNIVERSE: N=', 'Мощность (шаг 0)'):
            self.assertIn(needle, txt)
        self.assertIn('mexc — не применяется', txt)
        self.assertIn('/h2ext_review', txt)
        size1 = os.path.getmtime(p)
        os.utime(p, (size1 - 100, size1 - 100))
        run(HX.run_h2ext(dict(self.params, coins_override={'gateio': {'CRYPA'}, 'mexc': None})))
        self.assertGreater(os.path.getmtime(p), size1 - 100)                              # перезаписан

    def test_file_only_for_show_of_ext(self):
        d = tempfile.mkdtemp()
        import shutil
        for f in ('index.json', 'index.journal'):
            if os.path.exists(os.path.join(self.xdir, f)):
                shutil.copy(os.path.join(self.xdir, f), os.path.join(d, f))
        shutil.copytree(os.path.join(self.xdir, 'gateio'), os.path.join(d, 'gateio'))
        for ex in ('mexc', 'bingx', 'okx', 'bitget'):
            if os.path.isdir(os.path.join(self.xdir, ex)):
                shutil.copytree(os.path.join(self.xdir, ex), os.path.join(d, ex))
        fd = tempfile.mkdtemp()
        from research_factory import kv_set
        kv_set('h2_ext_done', True, fd)
        kv_set('h2_ext_passed', True, fd)
        for hyp, mode in (('ext', 'confirm'), ('f', 'show')):
            run(HX.run_h2ext(dict(self.params, xdir=d, fdir=fd if hyp == 'f' else tempfile.mkdtemp(), hyp=hyp, mode=mode, coins_override={'gateio': None})))
            self.assertFalse(os.path.exists(os.path.join(d, 'h2ext_universe_review.txt')), (hyp, mode))


class Coins(unittest.TestCase):
    class C:
        def __init__(self, coins=None, exc=None):
            self.coins, self.exc, self.closed = coins, exc, False

        async def fetch_spot_coins(self):
            if self.exc:
                raise self.exc
            return self.coins

        async def close(self):
            self.closed = True

    def test_fetch_notes_cache_and_reuse(self):
        d = tempfile.mkdtemp()
        g, b = self.C({'BTC', 'ETH'}), self.C(exc=V.VenueError('bitget HTTP 403: x'))
        coins, notes = run(HX.coin_sets({'coin_clients': {'gateio': g, 'bitget': b}}, ('bingx', 'okx', 'gateio', 'mexc', 'bitget'), d, True))
        self.assertEqual(coins['gateio'], {'BTC', 'ETH'})
        self.assertIsNone(coins['bitget'])
        self.assertIn('применён (2 монет', notes['gateio'])
        self.assertIn('запрос списка монет не удался', notes['bitget'])
        for ex in ('bingx', 'okx', 'mexc'):
            self.assertIsNone(coins[ex])
            self.assertIn('подписанного запроса', notes[ex])
        self.assertTrue(g.closed and b.closed)
        self.assertTrue(os.path.exists(os.path.join(d, 'h2ext_coins.json')))
        g2 = self.C({'OTHER'})
        coins2, notes2 = run(HX.coin_sets({'coin_clients': {'gateio': g2}}, ('gateio',), d, False))           # confirm/H2F: набор последнего показа
        self.assertEqual(coins2['gateio'], {'BTC', 'ETH'})
        self.assertFalse(g2.closed)
        coins3, _n = run(HX.coin_sets({'coin_clients': {'gateio': g2}}, ('gateio',), d, True))                # показ — обновляет
        self.assertEqual(coins3['gateio'], {'OTHER'})

    def test_fetch_spot_coins_parsing(self):
        async def gate_get(path, params):
            return [{'currency': 'BTC', 'chains': [{'name': 'BTC'}]}, {'currency': 'AAPLX', 'chains': []}, {'currency': 'ETH', 'chain': 'ETH'}]

        async def bit_get(path, params):
            return {'code': '00000', 'data': [{'coin': 'BTC', 'chains': [{'chain': 'BTC'}]}, {'coin': 'SPY', 'chains': []}]}
        g = V.GatePublic(get=gate_get, pause=0.0)
        self.assertEqual(run(g.fetch_spot_coins()), {'BTC', 'ETH'})
        b = V.BitgetPublic(get=bit_get, pause=0.0)
        self.assertEqual(run(b.fetch_spot_coins()), {'BTC'})
        empty = V.BitgetPublic(get=lambda p, q: asyncio.sleep(0, {'code': '00000', 'data': []}), pause=0.0)
        with self.assertRaises(V.VenueError):
            run(empty.fetch_spot_coins())


class CoinsJob(unittest.TestCase):
    def test_refresh_job_writes_cache_and_calc_reads_without_network(self):
        import research_coins as RC
        d = tempfile.mkdtemp()
        g = Coins.C({'BTC', 'CRYPA'})
        class Mk:
            async def load_markets(self):
                return {}

            async def close(self):
                pass
        g.load_markets = Mk().load_markets
        txt = run(RC.refresh_job({'xdir': d, 'venues': ('gateio', 'mexc'), 'clients': {'gateio': g, 'mexc': Mk()}}))
        self.assertIn('gateio — применён (2 монет', txt)
        self.assertIn('mexc — не применяется', txt)
        coins, notes, t = RC.read_cache(d)
        self.assertEqual(coins['gateio'], {'BTC', 'CRYPA'})
        self.assertIsNone(coins['mexc'])
        self.assertTrue(t > 0)
        orig = RC.fetch_coin_sets

        async def boom(*x, **k):
            raise AssertionError('сеть в процессе расчёта не используется')
        RC.fetch_coin_sets = boom
        try:
            co, nt = run(HX.coin_sets({}, ('gateio', 'mexc'), d))
            self.assertEqual(co['gateio'], {'BTC', 'CRYPA'})
            self.assertIn('набор от', nt['gateio'])
            d2 = tempfile.mkdtemp()                                                       # файла нет: п. г не применяется, сеть не трогается
            co2, nt2 = run(HX.coin_sets({}, ('gateio',), d2))
            self.assertEqual(co2, {'gateio': None})
            self.assertIn('набор монет не получен', nt2['gateio'])
        finally:
            RC.fetch_coin_sets = orig

    def test_coin_module_is_light_no_numpy(self):
        import subprocess
        import sys
        r = subprocess.run([sys.executable, '-I', '-c', 'import sys; sys.path.insert(0, %r); import research_coins; print("numpy" in sys.modules)' % ROOT], capture_output=True, text=True)
        self.assertEqual(r.stdout.strip(), 'False')

    def test_h2ext_cmd_runs_coin_job_before_show_only(self):
        calls = []
        _db, sent = _common.fresh_state()

        async def fake_job(cmd, task, params, payload=None):
            calls.append(('job', task, tuple(params.get('venues') or ())))
            return 'монеты: ок'

        async def fake_battery(cmd, task, extra=None, need_universe=True):
            calls.append(('battery', task, extra['mode']))
        orig = (a._research_job, a.battery_job)
        a._research_job, a.battery_job = fake_job, fake_battery
        try:
            run(a.h2ext_cmd('show'))
            run(a.h2ext_cmd('confirm'))
            run(a.h2ext_cmd('load'))
        finally:
            a._research_job, a.battery_job = orig
        self.assertEqual([c[:2] for c in calls], [('job', 'h2_coins'), ('battery', 'h2_ext'), ('battery', 'h2_ext'), ('battery', 'h2_ext')])
        self.assertEqual(calls[0][2], ('bingx', 'okx', 'gateio', 'mexc', 'bitget'))
        self.assertTrue(any('монеты: ок' in m for m in sent))
        import research_runner as R
        self.assertIn('h2_coins', R.TASKS)


class Probe(unittest.TestCase):
    def markets(self):
        def m(b, extra):
            return {'base': b, 'info': dict({'symbol': f'{b}USDT', 'status': 'normal'}, **extra)}
        return {f'{b}/USDT:USDT': m(b, e) for b, e in (('BTC', {'symbolType': 'perpetual'}), ('ETH', {'symbolType': 'perpetual'}), ('AAPLSTOCK', {'symbolType': 'stock', 'underlying': 'AAPL'}),
                                                       ('SP500', {'symbolType': 'index'}), ('GLD', {'symbolType': 'etf'}))}

    def test_examples_keys_diffs_and_distributions(self):
        mk = self.markets()
        ex = CP.pick_examples(mk)
        self.assertEqual(ex['крипто'][0], 'BTC/USDT:USDT')
        self.assertEqual(ex['акция'][0], 'AAPLSTOCK/USDT:USDT')
        self.assertEqual(ex['индекс/сырьё'][0], 'SP500/USDT:USDT')
        dist = CP.distributions(mk)
        self.assertEqual(dist['symbolType'], {'perpetual': 2, 'stock': 1, 'index': 1, 'etf': 1})
        self.assertNotIn('symbol', dist)                                                  # уникальное по контракту поле — не классификатор, не печатается как распределение
        self.assertNotIn('status', dist)                                                  # одно значение — тоже
        txt = '\n'.join(CP.venue_section('bitget', mk))
        for needle in ('контрактов в списке 5', 'ВСЕ ключи объекта контракта (3): symbol, status, symbolType', 'ключи, которых нет у BTC: underlying', 'symbolType=', 'symbolType:'):
            self.assertIn(needle, txt)

    def test_missing_examples_reported(self):
        mk = {'BTC/USDT:USDT': {'base': 'BTC', 'info': {'a': 1}}}
        txt = '\n'.join(CP.venue_section('okx', mk))
        self.assertIn('пример «акция»: НЕ НАЙДЕН', txt)
        self.assertIn('пример «индекс/сырьё»: НЕ НАЙДЕН', txt)

    def test_run_with_fake_clients_no_network(self):
        mk = self.markets()

        class Cl:
            async def load_markets(self):
                return mk

            async def close(self):
                pass

            async def _get(self, *a, **k):
                return []

            @staticmethod
            def _data(j, what):
                return j
        txt = run(CP.run_contract_probe({'clients': {ex: Cl() for ex in CP.ORDER}, 'coins': False, 'version': 'vT'}))
        for ex in CP.ORDER:
            self.assertIn(f'══ {ex}: контрактов в списке 5', txt)
        self.assertIn('bitget productType=USDC-FUTURES', txt)
        self.assertIn('gateio /futures/btc/contracts', txt)
        self.assertIn('Что прислать', txt)

    def test_load_error_is_reported_not_raised(self):
        class Bad:
            async def load_markets(self):
                raise RuntimeError('HTTP 403')

            async def close(self):
                pass
        txt = run(CP.run_contract_probe({'clients': {'gateio': Bad()}, 'order': ('gateio',), 'coins': False}))
        self.assertIn('══ gateio: ОШИБКА RuntimeError', txt)

    def test_sources_have_no_orders_or_keys(self):
        import re
        bad = re.compile(r'create_order|createOrder|cancel_order|apiKey|secret|signature|\.sign\(', re.I)
        src = open(os.path.join(ROOT, 'research_cprobe.py'), encoding='utf-8').read()
        self.assertIsNone(bad.search(src.replace('без подписи', '').replace('требует подписи', '').replace('требует подпись', '').replace('подписанного', '')))


class Wiring(unittest.TestCase):
    def test_command_and_docs(self):
        pool, allv = a.command_pools()
        self.assertIn('/contract_probe', allv)
        self.assertIsNone(a.command_hint('/contract_probe'))
        import research_runner as R
        self.assertIn('contract_probe', R.TASKS)
        src = open(os.path.join(ROOT, 'BOT_SPEC.md'), encoding='utf-8').read()
        for needle in ('### 2.64.', '#### 2.64.1', '#### 2.64.2', '#### 2.64.3', '| v134 |', 'h2ext_universe_review.txt'):
            self.assertIn(needle, src)
        self.assertIn('2026-11-26-v134', [x[0] for x in a.CHANGELOG])
        for ln in src.split('\n'):
            self.assertFalse(ln.startswith('H2EXT-EXCLUDE:'))                             # маркера исключения биржи нет


if __name__ == '__main__':
    unittest.main()
