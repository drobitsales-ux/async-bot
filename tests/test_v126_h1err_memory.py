"""v126: /xfund_1h_errors не копит ряды всех символов в памяти (в v125 — MemoryError на 8-м из 48 при RESEARCH_MEM_MB=300, RSS 204 МБ): пик памяти не растёт с числом символов."""
import asyncio
import os
import tempfile
import tracemalloc
import unittest

import _common
import research_h1err as H
import research_xdata as X
from test_v125_classify import DAY, HOUR, NOW, Err, Fake, mk, rows

ROOT = _common.ROOT


def build(n_syms, pages=30):
    syms = [f'S{i:03d}/USDT:USDT' for i in range(n_syms)]
    d = tempfile.mkdtemp()
    idx = X.load_index(d)
    for s in syms:
        idx['bars']['bingx'][s] = {'first': NOW - 2500 * DAY, 'last': NOW - DAY, 'n': 2400, 't': 3, 'complete': True}
        idx['neg'][f'bingx|{s}|h1'] = {'t': 3, 'why': 'ExchangeError', 'msg': 'старое', 'pages': 0, 'rows': 0}
    idx['migr']['bingx|v127_reset'] = {'t': 1, 'n': 0}
    X.save_index(d, idx)

    def script(k, since, limit, params):                      # 30 страниц по 1000 баров, затем ошибка границы; за 7 суток данные есть → класс а1 (загружается доступная часть)
        if since is not None:
            return rows(NOW, 200)
        if k <= pages:
            return rows(params.get('endTime') or NOW, limit)
        return Err('bingx {"code":109400,"msg":"startTime/endTime out of range"}')
    return d, Fake({s: mk(s.split('/')[0]) for s in syms}, {s: script for s in syms})


def peak_mb(n_syms, apply_):
    d, ex = build(n_syms)
    os.environ['HTF_FETCH_PAUSE_SEC'] = '0'

    async def sl(_x):
        return None
    tracemalloc.start()
    try:
        txt = asyncio.run(H.run_h1_errors({'xdir': d, 'apply': apply_}, None, {'bingx': ex}, now_ms=NOW, sleep=sl))
        peak = tracemalloc.get_traced_memory()[1] / 1048576
    finally:
        tracemalloc.stop()
        os.environ.pop('HTF_FETCH_PAUSE_SEC', None)
    return peak, txt, d


class Memory(unittest.TestCase):
    def test_peak_does_not_grow_with_number_of_symbols(self):
        p3, _t3, _d3 = peak_mb(3, True)
        p12, txt, d = peak_mb(12, True)
        self.assertLess(p12 - p3, 8.0, (p3, p12))                                                # при 12 символах — не больше, чем при 3 (ряды освобождаются по символу)
        self.assertLess(p12, 40.0, p12)                                                          # v125: ≈ 12 × 30 000 баров × 250 байт ≈ 90 МБ
        self.assertIn('а1 (ограничение глубины): загружена доступная часть 1h у 12 символов', txt)
        idx = X.load_index(d)
        self.assertEqual(len(idx['hbars']['bingx']), 12)
        h = idx['hbars']['bingx']['S000/USDT:USDT']
        self.assertEqual((h['n'], h['complete'], h['limit']), (30000, False, 'depth'))           # данные сохранены полностью (все 30 страниц)
        got = X.load_hbars(d, 'bingx', 'S000/USDT:USDT')
        self.assertEqual(len(got['ts']), 30000)
        self.assertTrue((got['ts'][1:] > got['ts'][:-1]).all())                                    # по возрастанию, без дублей
        self.assertEqual(int(got['ts'][-1]), NOW)

    def test_diagnostics_only_also_stays_small(self):
        p12, txt, d = peak_mb(12, False)
        self.assertLess(p12, 40.0, p12)
        self.assertEqual(X.load_index(d)['hbars']['bingx'], {})                                   # без apply данные не меняются
        self.assertIn('Данные не менялись', txt)
        self.assertIn('страниц до ошибки 30, строк 30000', txt)
        self.assertTrue(ROOT and HOUR == 3_600_000)


if __name__ == '__main__':
    unittest.main()
