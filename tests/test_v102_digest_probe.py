"""v102: группировка ошибок дайджеста; повторный проход пробы по «не определённым» инструментам; замер «плоский бар × объём»."""
import unittest


from _common import a
import research_ma2 as M2
from test_v100_ma2 import fake_duka, run, NOW


class LogGroups(unittest.TestCase):
    def test_signature_and_timestamp_are_collapsed(self):
        base = 'эквити не получено: RequestTimeout: bingx GET https://open-api.bingx.com/openApi/swap/v2/user/balance?timestamp=%d&signature=%s'
        rows = [(base % (1792800111599 + i * 143000, ('ab%02x' % i) * 16), 'WARNING') for i in range(8)]
        rows += [('[DD] старт процесса без сохранённой базы дня: база = текущее эквити 76.46', 'WARNING')]
        g = a._group_log_events(rows)
        self.assertEqual([c for _m, _l, c in g], [8, 1])
        self.assertIn('RequestTimeout', g[0][0])

    def test_levels_kept_separate_and_top_limit(self):
        rows = [('x 1', 'ERROR'), ('x 2', 'WARNING'), ('x 3', 'ERROR')] + [(f'msg{chr(97 + i)}', 'ERROR') for i in range(15)]
        g = a._group_log_events(rows, top=3)
        self.assertEqual(len(g), 3)
        self.assertEqual(g[0][1:], ('ERROR', 2))

    def test_empty(self):
        self.assertEqual(a._group_log_events([]), [])


class SecondPass(unittest.TestCase):
    def test_forget_transient_keeps_definitive(self):
        state = {'n': 0}
        inner = fake_duka({'EURUSD'}, 1.10, 1e5)

        async def fetch(url, params=None, method='GET'):
            state['n'] += 1
            if state['n'] <= 1:
                return {'status': 503, 'headers': {}, 'body': b'', 'error': ''}
            return await inner(url, params, method)
        fx = M2.Fetcher(fetch, 0, delays=())
        i1 = run(M2.discover(fx, 'EURUSD', NOW))
        self.assertIsNone(i1['folder'])
        self.assertTrue(i1['undetermined'])
        run(fx.month('NOPE', 'BID', 2024, 0))                              # 404 — определённый ответ
        fx.forget_transient()
        self.assertEqual([k for k in fx.cache if k[0] == 'NOPE'], [('NOPE', 'BID', 2024, 0)])
        self.assertFalse([k for k in fx.cache if k[0] == 'EURUSD'])
        i2 = run(M2.discover(fx, 'EURUSD', NOW))
        self.assertEqual(i2['folder'], 'EURUSD')

    def test_run_probe_has_second_pass(self):
        src = open(M2.__file__, encoding='utf-8').read()
        self.assertIn('повторный проход', src)
        self.assertIn('fx.forget_transient()', src)


class FlatVolume(unittest.TestCase):
    def test_measure_in_probe(self):
        fx = M2.Fetcher(fake_duka({'EURUSD'}, 1.10, 1e5), 0, delays=())
        info = run(M2.discover(fx, 'EURUSD', NOW))
        r = run(M2.probe_instrument(fx, info, NOW))
        w = r['weekend']
        # заполнители выходных — серия 48 ч с нулевым объёмом: вне серий плоских нулевых нет
        self.assertEqual(w['flat_zero_outside_run'], 0)
        self.assertEqual(w['flat_live_outside_run'], 0)
        self.assertEqual(w['zero_total'], w['flat_total'])
        txt = M2.build_report2({'error': 'x', 'elapsed_s': 1}, {'EURUSD': r}, {}, {}, {}, NOW, 100.0, 5)
        self.assertIn('Плоский бар и объём', txt)
        self.assertIn('плоских с нулевым объёмом вне серий ≥ 12 ч 0', txt)


if __name__ == '__main__':
    unittest.main()
