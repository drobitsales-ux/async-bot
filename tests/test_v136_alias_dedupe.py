"""v136 (§2.66): MEXC conceptPlate точно, «binance-alias» (нормализованное сравнение с архивом Binance), дедупликация одной монеты под разными тикерами. Сеть не используется."""
import asyncio
import os
import tempfile
import unittest

import numpy as np

import _common
import research_h2ext as HX
import research_h2repl as H2R
import research_venues as V
import research_xdata as X
import test_v116_h2x1 as T
import test_v129_h2ext_leadlag as T9
import test_v134_ext_filter as T34
from _common import a

ROOT = _common.ROOT
DAY = X.DAY


def run(c):
    return asyncio.run(c)


class MexcZones(unittest.TestCase):
    BASE = {'symbol': 'X_USDT', 'type': 1, 'conceptPlate': ['mc-trade-zone-mainly']}

    def test_exact_zone_only(self):
        self.assertIsNone(V.meta_exclusion(dict(self.BASE, conceptPlate=['mc-trade-zone-stockmeme'])))            # мем-монеты NIUBI, SI
        self.assertIsNone(V.meta_exclusion(dict(self.BASE, conceptPlate=['mc-trade-zone-usstocks'])))
        self.assertIsNone(V.meta_exclusion(dict(self.BASE, conceptPlate=['mc-trade-zone-stockindex'])))
        self.assertIsNone(V.meta_exclusion(dict(self.BASE, conceptPlate=['mc-trade-zone-stock'])))                # регистр как в API: точное совпадение
        self.assertEqual(V.meta_exclusion(dict(self.BASE, conceptPlate=['x', 'mc-trade-zone-Stock'])), 'поле conceptPlate=mc-trade-zone-Stock')
        self.assertEqual(V.meta_exclusion(dict(self.BASE, conceptPlate=['mc-trade-zone-tradfi'])), 'поле conceptPlate=mc-trade-zone-tradfi')
        self.assertEqual(V.meta_exclusion(dict(self.BASE, type=2, conceptPlate=['mc-trade-zone-stockmeme'])), 'поле type=2')    # условие type=2 сохранено (ответ владельца)


class Normalize(unittest.TestCase):
    def test_prefixes_and_suffixes(self):
        n = X.norm_alias
        self.assertEqual((n('1000PEPE'), n('PEPE')), ('PEPE', 'PEPE'))
        self.assertEqual((n('10000SATS'), n('1MBABYDOGE'), n('100000X'.replace('X', 'ABC')), n('1000000MOG')), ('SATS', 'BABYDOGE', 'ABC', 'MOG'))
        self.assertEqual(n('100PEPE'), '100PEPE')                                                                   # префикс 100 в списке нет
        self.assertEqual(n('1INCH'), '1INCH')
        self.assertEqual(n('ABCNEWTOKEN'), 'ABC')                                                                   # повторно до стабилизации
        self.assertEqual(n('XYONETH'), 'XY')                                                                        # ONETH раньше ETH (по убыванию длины)
        self.assertEqual(n('FUN'), 'FUN')                                                                           # остаток < 2 символов — не снимается
        self.assertEqual(n('XAI'), 'XAI')
        self.assertEqual(n('GRAI'), 'GR')                                                                           # остаток ровно 2 — снимается
        self.assertEqual(n('abcdai'), 'ABCD')
        self.assertEqual(n(''), '')

    def test_known_renames_apply_only_to_symbol_side(self):
        for src, dst in (('ZKSYNC', 'ZK'), ('LUNANEW', 'LUNA2'), ('TRUMPOFFICIAL', 'TRUMP'), ('PUMPFUN', 'PUMP')):
            self.assertEqual(X.ALIAS_RENAMES[src], dst)
            self.assertEqual(X.norm_alias(src), X.norm_alias(dst, rename=False))
        self.assertEqual(X.norm_alias('LUNANEW'), 'LUNA2')
        self.assertEqual(X.norm_alias('LUNANEW', rename=False), 'LUNA')                                             # на стороне Binance переименований нет

    def test_alias_marks_only_matching_and_keeps_old_excl(self):
        amap = X.binance_alias_map(['TRUMP', 'PUMP', 'ZK', 'LUNA2', 'PEPE', 'SATS', 'BTC'])
        bases = ['TRUMPOFFICIAL', 'PUMPFUN', 'ZKSYNC', 'LUNANEW', '1000PEPE', '10000SATS', 'RANDOM', 'STOCKMEME', 'AAPLSTOCK']
        mk = {f'{b}/USDT:USDT': {'base': b, 'norm': X.norm_base(b), 'excl': None} for b in bases}
        mk['AAPLSTOCK/USDT:USDT']['excl'] = 'б'
        n = X.apply_alias(mk, amap)
        got = {m['base']: m['excl'] for m in mk.values()}
        self.assertEqual(n, 6)
        self.assertEqual(got['TRUMPOFFICIAL'], 'binance-alias=TRUMP')
        self.assertEqual(got['PUMPFUN'], 'binance-alias=PUMP')
        self.assertEqual(got['ZKSYNC'], 'binance-alias=ZK')
        self.assertEqual(got['LUNANEW'], 'binance-alias=LUNA2')
        self.assertEqual(got['1000PEPE'], 'binance-alias=PEPE')
        self.assertEqual(got['10000SATS'], 'binance-alias=SATS')
        self.assertIsNone(got['RANDOM'])
        self.assertIsNone(got['STOCKMEME'])
        self.assertEqual(got['AAPLSTOCK'], 'б')                                                                     # прежняя причина сохранена (порядок а → б → в → г → binance-alias)

    def test_binance_raw_bases_from_archive_index(self):
        bd = tempfile.mkdtemp()
        T9.BF.write_battery(bd, n_syms=6)
        bases = X.binance_raw_bases(bd)
        self.assertTrue(bases and all(not b.endswith('USDT') for b in bases))


def mkbars(first_day, n, seed, vol=0.06, qv=5e7, scale=1.0):
    b = T.mk_bars(first_day, n, seed, vol=vol, qv=qv)
    for k in ('o', 'h', 'l', 'c'):
        b[k] = b[k] * scale
    return b


class Dedupe(unittest.TestCase):
    D = T.D0 + 100

    def run_dd(self, items):
        """items: [(ex, sym, bars)] → (группы, убранные)."""
        uni = [{'ex': ex, 'sym': sym, 'norm': sym, 'first_day': int(b['ts'][0]) // DAY} for ex, sym, b in items]
        bars = {}
        for ex, sym, b in items:
            bars.setdefault(ex, {})[sym] = b
        return X.dedupe_universe(uni, bars)

    def test_same_coin_two_tickers_collapses_keeping_bigger_turnover(self):
        base = mkbars(self.D, 120, 11, qv=3e7)
        twin = dict(base, qv=base['qv'] * 2.0)
        other = mkbars(self.D, 120, 12)
        groups, dropped = self.run_dd([('gateio', 'HAJIMI/USDT:USDT', base), ('mexc', '哈基米/USDT:USDT', twin), ('bitget', 'OTHER/USDT:USDT', other)])
        self.assertEqual(len(groups), 1)
        self.assertEqual(groups[0]['keep'], 'mexc:哈基米/USDT:USDT')                                                 # оборот вдвое больше
        self.assertEqual([d['name'] for d in groups[0]['drop']], ['gateio:HAJIMI/USDT:USDT'])
        self.assertAlmostEqual(groups[0]['drop'][0]['corr'], 1.0, places=6)
        self.assertAlmostEqual(groups[0]['drop'][0]['ratio'], 1.0, places=6)
        self.assertEqual(dropped, {('gateio', 'HAJIMI/USDT:USDT')})                                                 # разные монеты не схлопываются

    def test_multiplier_1000_10000_and_tie_by_exchange_order(self):
        b1 = mkbars(self.D, 120, 21)
        for scale in (1000.0, 1e-3, 10000.0, 1e-4):
            groups, dropped = self.run_dd([('bingx', 'PEPE/USDT:USDT', b1), ('okx', '1000PEPE/USDT:USDT', mkbars(self.D, 120, 21, scale=scale))])
            self.assertEqual(len(dropped), 1, scale)
            self.assertEqual(groups[0]['keep'], 'bingx:PEPE/USDT:USDT')                                              # равный оборот — порядок бирж bybit, bingx, okx…

    def test_different_coins_not_merged_and_conditions(self):
        b = mkbars(self.D, 120, 31)
        self.assertEqual(self.run_dd([('gateio', 'A/USDT:USDT', b), ('mexc', 'B/USDT:USDT', mkbars(self.D, 120, 32))])[1], set())            # низкая корреляция
        self.assertEqual(self.run_dd([('gateio', 'A/USDT:USDT', b), ('mexc', 'B/USDT:USDT', mkbars(self.D, 120, 31, scale=5.0))])[1], set())   # корреляция 1, цена в 5 раз — не одна монета
        late = mkbars(self.D + 31, 100, 31)
        self.assertEqual(self.run_dd([('gateio', 'A/USDT:USDT', b), ('mexc', 'B/USDT:USDT', late)])[1], set())                                # листинги дальше 30 суток
        short = mkbars(self.D, 8, 31)
        self.assertEqual(self.run_dd([('gateio', 'A/USDT:USDT', short), ('mexc', 'B/USDT:USDT', short)])[1], set())                            # < 10 общих суток
        flat = dict(mkbars(self.D, 120, 41), c=np.ones(120), o=np.ones(120), h=np.ones(120), l=np.ones(120))
        self.assertEqual(self.run_dd([('gateio', 'F1/USDT:USDT', flat), ('mexc', 'F2/USDT:USDT', flat)])[1], set())                            # постоянный ряд

    def test_group_of_three_is_transitive_and_listing_offset_ok(self):
        b = mkbars(self.D, 120, 51)
        shifted = mkbars(self.D + 10, 110, 51)                                                                        # тот же ряд, листинг позже на 10 суток (первый общий день — D+10)
        shifted['c'] = b['c'][10:]
        shifted['o'], shifted['h'], shifted['l'] = b['o'][10:], b['h'][10:], b['l'][10:]
        groups, dropped = self.run_dd([('gateio', 'A/USDT:USDT', b), ('mexc', 'B/USDT:USDT', dict(b)), ('bitget', 'C/USDT:USDT', shifted)])
        self.assertEqual(len(groups), 1)
        self.assertEqual(len(dropped), 2)


class Integration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.bdir, cls.xdir = tempfile.mkdtemp(), tempfile.mkdtemp()
        T9.BF.write_battery(cls.bdir, n_syms=18)
        T9.write_xdata_ext(cls.xdir, n_new=20)
        T34.add_symbols(cls.xdir, {'gateio': ['SEEDG', 'CRYPA', 'TRUMPOFFICIAL', 'PUMPFUN', 'ZKSYNC', 'LUNANEW', '1000PEPE'], 'mexc': ['SEEDM', 'CRYPB', 'HAJIMI']})
        idx = X.load_index(cls.xdir)                                                                                   # 哈基米 на bitget — те же цены, что HAJIMI на mexc, оборот меньше
        src = X.load_bars(cls.xdir, 'mexc', 'HAJIMI/USDT:USDT')
        X.save_bars(cls.xdir, 'bitget', '哈基米/USDT:USDT', src['ts'], src['o'], src['h'], src['l'], src['c'], src['v'] * 0.5)
        idx['bars']['bitget']['哈基米/USDT:USDT'] = dict(idx['bars']['mexc']['HAJIMI/USDT:USDT'])
        idx['markets']['bitget']['哈基米/USDT:USDT'] = {'base': '哈基米', 'norm': '哈基米', 'active': True, 'created': None, 'excl': None, 'cat': ''}
        seed = X.load_bars(cls.xdir, 'mexc', 'SEEDM/USDT:USDT')
        X.save_bars(cls.xdir, 'bitget', 'SEEDB/USDT:USDT', seed['ts'], seed['o'], seed['h'], seed['l'], seed['c'], seed['v'])
        idx['bars']['bitget']['SEEDB/USDT:USDT'] = dict(idx['bars']['mexc']['SEEDM/USDT:USDT'], first=int(seed['ts'][0]) - DAY * 5)
        idx['markets']['bitget']['SEEDB/USDT:USDT'] = {'base': 'SEEDB', 'norm': 'SEEDB', 'active': True, 'created': None, 'excl': None, 'cat': ''}
        X.save_index(cls.xdir, idx)
        cls.ven = T9.VEN + ('bitget',)

    def inputs(self):
        return H2R.repl_inputs(self.xdir, self.bdir, self.ven, {'coins': {}, 'meta': {}, 'binance_bases': ['TRUMP', 'PUMP', 'ZK', 'LUNA2', 'PEPE']})

    def test_universe_alias_and_duplicates(self):
        series, fund, uni, ref, where, mk = self.inputs()
        names = {(u['ex'], u['sym'].split('/')[0]) for u in uni}
        for b in ('TRUMPOFFICIAL', 'PUMPFUN', 'ZKSYNC', 'LUNANEW', '1000PEPE'):
            self.assertNotIn(('gateio', b), names, b)
        why = {(ex, sym.split('/')[0]): r for ex, sym, r in ref['excluded_syms']}
        self.assertEqual(why[('gateio', 'TRUMPOFFICIAL')], 'binance-alias=TRUMP')
        self.assertEqual(why[('gateio', 'PUMPFUN')], 'binance-alias=PUMP')
        self.assertEqual(why[('gateio', 'ZKSYNC')], 'binance-alias=ZK')
        self.assertEqual(why[('gateio', 'LUNANEW')], 'binance-alias=LUNA2')
        self.assertEqual(why[('gateio', '1000PEPE')], 'binance-alias=PEPE')
        self.assertIn(('gateio', 'CRYPA'), names)
        # дубликаты: HAJIMI (mexc, оборот больше) остаётся, 哈基米 (bitget) убран; прочие не тронуты
        self.assertIn(('mexc', 'HAJIMI'), names)
        self.assertNotIn(('bitget', '哈基米'), names)
        self.assertEqual(len(ref['duplicates']), 1)
        self.assertEqual(ref['duplicates'][0]['keep'], 'mexc:HAJIMI/USDT:USDT')
        self.assertEqual(why[('bitget', '哈基米')], 'дубликат=mexc:HAJIMI/USDT:USDT')
        self.assertEqual(len({u['norm'] for u in uni}), len(uni))

    def test_report_file_sections_and_counts(self):
        fdir = tempfile.mkdtemp()
        old = HX.K_REPS
        HX.K_REPS = 3
        try:
            txt = run(HX.run_h2ext({'xdir': self.xdir, 'bdir': self.bdir, 'fdir': fdir, 'fee_taker': 0.0005, 'fee_maker': 0.0002, 'version': 'vT', 'venues': self.ven, 'mode': 'show', 'hyp': 'ext',
                                    'coverage_min': 0.0, 'spec_path': '/dev/null', 'coins_override': {}, 'meta_override': {}, 'binance_bases': ['TRUMP', 'PUMP', 'ZK', 'LUNA2', 'PEPE']}))
        finally:
            HX.K_REPS = old
        body = open(os.path.join(self.xdir, 'h2ext_universe_review.txt'), encoding='utf-8').read()
        self.assertIn('## binance-alias (', body)
        self.assertIn('## дубликаты (1 групп)', body)
        self.assertIn('gateio\tTRUMPOFFICIAL/USDT:USDT\tTRUMP', body.split('## binance-alias')[1].split('## дубликаты')[0])
        self.assertIn('mexc:HAJIMI/USDT:USDT\tbitget:哈基米/USDT:USDT\t1.000', body)
        self.assertIn('binance-alias (', txt)
        self.assertIn('дубликаты одной монеты под разными тикерами', txt)
        self.assertIn('оставлен mexc:HAJIMI/USDT:USDT', txt)
        self.assertIn('binance-alias 5', txt)                                                                           # по пунктам и биржам
        self.assertIn('дубликат 1', txt)


class Docs(unittest.TestCase):
    def test_records(self):
        src = open(os.path.join(ROOT, 'BOT_SPEC.md'), encoding='utf-8').read()
        for needle in ('### 2.66.', '#### 2.66.1', '#### 2.66.2', '#### 2.66.3', '#### 2.66.4', '| v136 |', 'ZKSYNC → ZK', 'LUNANEW → LUNA2', 'TRUMPOFFICIAL → TRUMP', 'PUMPFUN → PUMP'):
            self.assertIn(needle, src)
        self.assertIn('2026-11-27-v136', [c[0] for c in a.CHANGELOG])
        for ln in src.split('\n'):
            self.assertFalse(ln.startswith('H2EXT-UNIVERSE:') or ln.startswith('H2EXT-K:'))                              # строки пре-регистрации не записаны


if __name__ == '__main__':
    unittest.main()
