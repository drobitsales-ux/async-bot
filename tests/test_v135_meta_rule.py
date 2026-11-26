"""v135 (§2.65): пункт а — поля метаданных по выводу /contract_probe (Gate contract_type, Bitget isRwa, MEXC type/conceptPlate, OKX instCategory, BingX — NC). Объекты — из реального вывода шага 0."""
import asyncio
import os
import tempfile
import unittest

import _common
import research_coins as RC
import research_h2ext as HX
import research_h2repl as H2R
import research_venues as V
import research_xdata as X
import test_v129_h2ext_leadlag as T9
import test_v134_ext_filter as T34
from _common import a

ROOT = _common.ROOT


def run(c):
    return asyncio.run(c)


# значения полей — из реального вывода /contract_probe 2026-11-26 (урезаны до различающих полей)
GATE_BTC = {'name': 'BTC_USDT', 'type': 'direct', 'status': 'trading', 'contract_type': ''}
GATE_AAPL = {'name': 'AAPL_USDT', 'type': 'direct', 'status': 'trading', 'contract_type': 'stocks'}
GATE_SPX = {'name': 'SPX500_USDT', 'type': 'direct', 'status': 'trading', 'contract_type': 'indices'}
BIT_BTC = {'symbol': 'BTCUSDT', 'symbolType': 'perpetual', 'isRwa': 'NO'}
BIT_AAPL = {'symbol': 'AAPLUSDT', 'symbolType': 'perpetual', 'isRwa': 'YES'}
MEXC_BTC = {'symbol': 'BTC_USDT', 'type': 1, 'typeLabel': 0, 'conceptPlate': ['mc-trade-zone-mainly', 'mc-trade-zone-layer2', 'mc-trade-zone-pow']}
MEXC_AAPL = {'symbol': 'AAPLSTOCK_USDT', 'type': 2, 'typeLabel': 2, 'conceptPlate': ['mc-trade-zone-Stock', 'mc-trade-zone-tradfi', 'mc-trade-zone-usstocks']}
MEXC_SPX = {'symbol': 'SPX500_USDT', 'type': 2, 'typeLabel': 0, 'conceptPlate': ['mc-trade-zone-Stock', 'mc-trade-zone-0fees', 'mc-trade-zone-tradfi', 'mc-trade-zone-stockindex']}
OKX_BTC = {'instId': 'BTC-USDT-SWAP', 'category': '1', 'instCategory': '1'}
OKX_AAPL = {'instId': 'AAPL-USDT-SWAP', 'category': '1', 'instCategory': '3'}
OKX_XAU = {'instId': 'XAU-USDT-SWAP', 'category': '1', 'instCategory': '4'}
BINGX_BTC = {'symbol': 'BTC-USDT', 'status': '1', 'displayName': 'BTC-USDT'}


class Fields(unittest.TestCase):
    def test_gate(self):
        self.assertIsNone(V.meta_exclusion(GATE_BTC))
        self.assertEqual(V.meta_exclusion(GATE_AAPL), 'поле contract_type=stocks')
        self.assertEqual(V.meta_exclusion(GATE_SPX), 'поле contract_type=indices')
        for v in ('metals', 'forex', 'commodities', 'anything'):
            self.assertEqual(V.meta_exclusion(dict(GATE_BTC, contract_type=v)), f'поле contract_type={v}')

    def test_bitget(self):
        self.assertIsNone(V.meta_exclusion(BIT_BTC))
        self.assertEqual(V.meta_exclusion(BIT_AAPL), 'поле isRwa=YES')
        self.assertEqual(V.meta_exclusion(dict(BIT_BTC, isRwa='yes')), 'поле isRwa=YES')

    def test_mexc(self):
        self.assertIsNone(V.meta_exclusion(MEXC_BTC))
        self.assertEqual(V.meta_exclusion(MEXC_AAPL), 'поле type=2')
        self.assertEqual(V.meta_exclusion(MEXC_SPX), 'поле type=2')
        self.assertEqual(V.meta_exclusion(dict(MEXC_AAPL, type=1)), 'поле conceptPlate=mc-trade-zone-Stock')             # без type — по зоне
        self.assertEqual(V.meta_exclusion(dict(MEXC_BTC, conceptPlate=['mc-trade-zone-tradfi'])), 'поле conceptPlate=mc-trade-zone-tradfi')
        self.assertEqual(V.meta_exclusion(dict(MEXC_BTC, type='2')), 'поле type=2')
        self.assertIsNone(V.meta_exclusion(dict(MEXC_BTC, conceptPlate=['mc-trade-zone-layer2'])))
        self.assertIsNone(V.meta_exclusion({'type': 2}))                                                                # без conceptPlate — не MEXC: тип не трогаем

    def test_okx_and_bingx_through_noncrypto_reason(self):
        self.assertIsNone(X.noncrypto_reason('BTC', OKX_BTC))
        self.assertEqual(X.noncrypto_reason('AAPL', OKX_AAPL), 'поле instCategory=3')
        self.assertEqual(X.noncrypto_reason('XAU', OKX_XAU), 'поле instCategory=4')
        self.assertIsNone(X.noncrypto_reason('BTC', BINGX_BTC))                                                        # у BingX полей нет
        self.assertEqual(X.noncrypto_reason('NCSKAAPL2USD', BINGX_BTC), 'NC-префикс')
        self.assertEqual(X.noncrypto_reason('AAPL', GATE_AAPL), 'поле contract_type=stocks')                          # загрузка больше не запрашивает такие символы
        self.assertEqual(X.noncrypto_reason('AAPL', BIT_AAPL), 'поле isRwa=YES')
        self.assertEqual(X.noncrypto_reason('AAPLSTOCK', MEXC_AAPL), 'поле type=2')
        m = X.usdt_swaps({'AAPL/USDT:USDT': {'swap': True, 'linear': True, 'quote': 'USDT', 'settle': 'USDT', 'base': 'AAPL', 'active': True, 'created': None, 'info': GATE_AAPL}})
        self.assertEqual(m['AAPL/USDT:USDT']['excl'], 'поле contract_type=stocks')


class FakeClient:
    def __init__(self, markets):
        self.markets, self.closed = markets, False

    async def load_markets(self):
        return {f'{b}/USDT:USDT': {'base': b, 'info': i} for b, i in self.markets.items()}

    async def close(self):
        self.closed = True


class Job(unittest.TestCase):
    def test_fetch_meta_and_cache(self):
        d = tempfile.mkdtemp()
        g = FakeClient({'BTC': GATE_BTC, 'AAPL': GATE_AAPL, 'SP500': GATE_SPX})
        m = FakeClient({'BTC': MEXC_BTC, 'AAPLSTOCK': MEXC_AAPL})
        b = FakeClient({'BTC': BIT_BTC, 'ABBV': BIT_AAPL})

        class Bad:
            async def load_markets(self):
                raise RuntimeError('HTTP 403')

            async def close(self):
                pass
        meta, notes = run(RC.fetch_meta(('gateio', 'mexc', 'bitget', 'bingx', 'okx'), {'gateio': g, 'mexc': m, 'bitget': Bad()}))
        self.assertEqual(meta['gateio'], {'AAPL/USDT:USDT': 'поле contract_type=stocks', 'SP500/USDT:USDT': 'поле contract_type=indices'})
        self.assertEqual(meta['mexc'], {'AAPLSTOCK/USDT:USDT': 'поле type=2'})
        self.assertIsNone(meta['bitget'])
        self.assertIn('не получены', notes['bitget'])
        self.assertIn('2 из 3', notes['gateio'])
        self.assertIsNone(meta['bingx'])
        self.assertTrue(g.closed and m.closed)
        txt = run(RC.refresh_job({'xdir': d, 'venues': ('gateio', 'bitget'), 'clients': {'gateio': g, 'bitget': b}}))
        self.assertIn('поля метаданных (п. а §2.65.2)', txt)
        got, mnotes = RC.read_meta(d)
        self.assertEqual(got['bitget'], {'ABBV/USDT:USDT': 'поле isRwa=YES'})
        self.assertIn('gateio', got)
        self.assertIsNone(RC.read_meta(tempfile.mkdtemp()))

    def test_meta_sets_fallback_without_file(self):
        meta, notes = run(HX.meta_sets({}, ('gateio',), tempfile.mkdtemp()))
        self.assertEqual(meta, {})
        self.assertIn('действует только §2.52', notes['gateio'])


class Universe(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.bdir, cls.xdir = tempfile.mkdtemp(), tempfile.mkdtemp()
        T9.BF.write_battery(cls.bdir, n_syms=18)
        T9.write_xdata_ext(cls.xdir, n_new=20)
        T34.add_symbols(cls.xdir, {'gateio': ['SEEDG', 'CRYPA', 'AAPLSTOCK', 'TSLA', 'METALX'], 'mexc': ['SEEDM', 'CRYPB', 'ETFM'], 'bitget': ['SEEDB', 'CRYPC', 'ABBV']})

    def test_rule_a_first_and_report(self):
        meta = {'gateio': {'TSLA/USDT:USDT': 'поле contract_type=stocks', 'METALX/USDT:USDT': 'поле contract_type=metals', 'AAPLSTOCK/USDT:USDT': 'поле contract_type=stocks'},
                'mexc': {'ETFM/USDT:USDT': 'поле type=2'}, 'bitget': {'ABBV/USDT:USDT': 'поле isRwa=YES'}}
        series, fund, uni, ref, where, mk = H2R.repl_inputs(self.xdir, self.bdir, ('bingx', 'okx', 'gateio', 'mexc', 'bitget'), {'coins': {}, 'meta': meta})
        names = {(u['ex'], u['sym'].split('/')[0]) for u in uni}
        for k in (('gateio', 'CRYPA'), ('mexc', 'CRYPB'), ('bitget', 'CRYPC')):
            self.assertIn(k, names)
        for k in (('gateio', 'TSLA'), ('gateio', 'METALX'), ('mexc', 'ETFM'), ('bitget', 'ABBV')):
            self.assertNotIn(k, names)
        why = {(ex, sym.split('/')[0]): r for ex, sym, r in ref['excluded_syms']}
        self.assertEqual(why[('gateio', 'AAPLSTOCK')], 'поле contract_type=stocks')                  # а раньше б (в базе есть STOCK)
        self.assertEqual(why[('gateio', 'TSLA')], 'поле contract_type=stocks')
        self.assertEqual(why[('mexc', 'ETFM')], 'поле type=2')
        self.assertEqual(why[('bitget', 'ABBV')], 'поле isRwa=YES')

    def test_end_to_end_job_file_then_show(self):
        """h2_coins (поля + монеты) → файл → /h2_ext show читает его без сети: исключения по п. а в отчёте и в файле-обзоре с полем и значением."""
        fdir = tempfile.mkdtemp()
        clients = {'gateio': FakeClient({'CRYPA': GATE_BTC, 'TSLA': GATE_AAPL, 'AAPLSTOCK': GATE_AAPL, 'METALX': dict(GATE_BTC, contract_type='metals'), 'SEEDG': GATE_BTC}),
                   'mexc': FakeClient({'CRYPB': MEXC_BTC, 'ETFM': MEXC_SPX, 'SEEDM': MEXC_BTC}),
                   'bitget': FakeClient({'CRYPC': BIT_BTC, 'ABBV': BIT_AAPL, 'SEEDB': BIT_BTC})}
        for c in clients.values():
            c.fetch_spot_coins = None
        class Coin:
            def __init__(self, s):
                self.s = s
            async def fetch_spot_coins(self):
                return self.s
            async def load_markets(self):
                return await clients_orig[self.key].load_markets()
            async def close(self):
                pass
        clients_orig = clients
        wrapped = {}
        for k, v in clients.items():
            w = Coin({'CRYPA', 'CRYPC', 'ABBV', 'SEEDG', 'SEEDB'})
            w.key = k
            wrapped[k] = w
        txt = run(RC.refresh_job({'xdir': self.xdir, 'venues': ('bingx', 'okx', 'gateio', 'mexc', 'bitget'), 'clients': wrapped}))
        self.assertIn('поля метаданных (п. а §2.65.2)', txt)
        old = HX.K_REPS
        HX.K_REPS = 3
        orig = RC.fetch_coin_sets

        async def boom(*x, **k):
            raise AssertionError('сеть в процессе расчёта не используется')
        RC.fetch_coin_sets = boom
        try:
            rep = run(HX.run_h2ext({'xdir': self.xdir, 'bdir': self.bdir, 'fdir': fdir, 'fee_taker': 0.0005, 'fee_maker': 0.0002, 'version': 'vT', 'venues': T9.VEN + ('bitget',), 'mode': 'show',
                                    'hyp': 'ext', 'coverage_min': 0.0, 'spec_path': '/dev/null'}))
        finally:
            RC.fetch_coin_sets = orig
            HX.K_REPS = old
        self.assertIn('пункт а (метаданные, §2.65.2 + §2.52): bingx', rep)
        self.assertIn('gateio — по полям метаданных отмечено 3 из 5', rep)
        body = open(os.path.join(self.xdir, 'h2ext_universe_review.txt'), encoding='utf-8').read()
        inc, exc = body.split('## Исключены')
        for sym in ('CRYPA', 'CRYPB', 'CRYPC'):
            self.assertIn(f'{sym}/USDT:USDT', inc)
        self.assertIn('gateio\tTSLA/USDT:USDT\tа (метаданные) (поле contract_type=stocks)', exc)
        self.assertIn('gateio\tMETALX/USDT:USDT\tа (метаданные) (поле contract_type=metals)', exc)
        self.assertIn('mexc\tETFM/USDT:USDT\tа (метаданные) (поле type=2)', exc)
        self.assertIn('bitget\tABBV/USDT:USDT\tа (метаданные) (поле isRwa=YES)', exc)
        self.assertIn('gateio\tAAPLSTOCK/USDT:USDT\tа (метаданные) (поле contract_type=stocks)', exc)


class Docs(unittest.TestCase):
    def test_records(self):
        src = open(os.path.join(ROOT, 'BOT_SPEC.md'), encoding='utf-8').read()
        for needle in ('### 2.65.', '#### 2.65.1', '#### 2.65.2', '#### 2.65.3', '| v135 |', 'contract_type', 'isRwa', 'conceptPlate'):
            self.assertIn(needle, src)
        self.assertIn('2026-11-26-v135', [x[0] for x in a.CHANGELOG])


if __name__ == '__main__':
    unittest.main()
