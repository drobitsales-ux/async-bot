"""v103: «фабрика» — сквозной прогон на синтетических данных (малая вселенная): /factory (калибровка, A, B), провал калибровки, /factory_pairs, /factory_final (один раз, флаг в БД),
отказы по условиям, проводка команд бота, HTML-санитайзер Telegram."""
import asyncio
import os
import tempfile
import unittest

import numpy as np

from _common import a, ROOT
import factory_fixtures as F
import research_fdata as FD
import research_fengine as E
import research_factory as RF
import research_ma_lib as L
import research_runner as RR

SYMS = ['AAAUSDT', 'BTCUSDT', 'CCCUSDT', 'DDDUSDT', 'EEEUSDT', 'FFFUSDT']
KEYS = ('XAUUSD', 'XAGUSD', 'EURUSD', 'GBPUSD', 'USDJPY', 'AUDUSD', 'USDCHF', 'USDCAD')
NOW = F.ms(2025, 3, 10)


def run(coro):
    return asyncio.run(coro)


class Base(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._inst, cls._cm, cls._cells, cls._calib, cls._bias = dict(FD.INSTRUMENTS), dict(FD.CLASS_MEMBERS), list(FD.CELLS), RF.CALIB_N, RF.BIAS_REPS
        FD.INSTRUMENTS.clear()
        FD.INSTRUMENTS.update({k: (v[0], v[1], v[2], (2019, 1)) for k, v in cls._inst.items() if k in KEYS})
        FD.CLASS_MEMBERS.clear()
        FD.CLASS_MEMBERS.update({c: [k for k, v in FD.INSTRUMENTS.items() if v[0] == c] for c in ('COMMODITIES', 'FX', 'INDICES', 'BONDS')})
        FD.CLASS_MEMBERS['MULTI'] = list(FD.INSTRUMENTS)
        FD.CELLS[:] = [('CRYPTO', '1D'), ('CRYPTO', '4h'), ('COMMODITIES', '1D'), ('FX', '1D'), ('FX', '4h'), ('MULTI', '1D')]
        RF.CALIB_N = 30
        RF.BIAS_REPS = 4
        cls.d, cls.vd = tempfile.mkdtemp(), tempfile.mkdtemp()
        for i, k in enumerate(FD.INSTRUMENTS):
            F.write_instrument(cls.d, k, seed=i + 1, p0={'XAUUSD': 1500, 'XAGUSD': 20, 'USDJPY': 110}.get(k, 1.0))
        for i, s in enumerate(SYMS):
            F.write_crypto(cls.vd, s, seed=50 + i)
        cls.params = {'fdir': cls.d, 'vdir': cls.vd, 'syms': SYMS, 'now_ms': NOW}

    @classmethod
    def tearDownClass(cls):
        FD.INSTRUMENTS.clear(); FD.INSTRUMENTS.update(cls._inst)
        FD.CLASS_MEMBERS.clear(); FD.CLASS_MEMBERS.update(cls._cm)
        FD.CELLS[:] = cls._cells
        RF.CALIB_N = cls._calib
        RF.BIAS_REPS = cls._bias

    def reset(self, keep=('spread:XAGUSD',)):
        con = RF._con(self.d)
        con.execute('DELETE FROM kv WHERE k NOT IN (%s)' % ','.join('?' * len(keep)), keep) if keep else con.execute('DELETE FROM kv')
        con.commit(); con.close()
        L._lockbox_session_state['opened'] = False

    def write_spec(self, pairs_line):
        f = tempfile.NamedTemporaryFile('w', suffix='.md', delete=False, encoding='utf-8')
        f.write('# spec\n' + pairs_line + '\n')
        f.close()
        return f.name

    def ready(self):
        RF.kv_set('spread:XAGUSD', {'median': 2.0}, self.d)
        txt = run_pairs(self.params)
        RF.kv_set('pairs_ack', True, self.d)
        pairs = RF.kv_get('pairs', self.d)
        self.params = dict(self.__class__.params, spec_path=self.write_spec('PAIRS-FROZEN: ' + ', '.join(f'{a}/{b}' for a, b, _c, _n in pairs)))
        return txt


def run_pairs(params):
    return RF.run_pairs(params)


class Preflight(Base):
    def test_refuses_without_pairs_xag_and_with_gap(self):
        self.reset(keep=())
        txt = run(RF.run_factory(self.params))
        self.assertIn('/factory отказ', txt)
        self.assertNotIn('спред XAGUSD не измерен', txt)                       # v104: запасное значение 10.99 б.п. (§2.41.21 п. 3.4)
        self.assertIn('пары P-PAIR не заморожены', txt)
        RF.run_pairs(self.params)
        txt = run(RF.run_factory(self.params))
        self.assertIn('не подтверждён', txt)
        # подтверждение без записи в BOT_SPEC (строка-заглушка) — отказ (§2.41.21 п. 3.5)
        RF.kv_set('pairs_ack', True, self.d)
        txt = run(RF.run_factory(dict(self.params, spec_path=self.write_spec('PAIRS-FROZEN: (не записан)'))))
        self.assertIn('список пар не записан в BOT_SPEC', txt)
        pairs = RF.kv_get('pairs', self.d)
        good = ', '.join(f'{a}/{b}' for a, b, _c, _n in pairs)
        bad = ', '.join(f'{a}/{b}' for a, b, _c, _n in pairs[::-1])
        txt = run(RF.run_factory(dict(self.params, spec_path=self.write_spec('PAIRS-FROZEN: ' + bad))))
        self.assertIn('не совпадает с замороженным в БД', txt)
        self.params = dict(self.params, spec_path=self.write_spec('PAIRS-FROZEN: ' + good))
        # пропуск месяца между имеющимися — «данные неполны» (хронические 503)
        p = FD.month_path(self.d, 'EURUSD', 2022, 5)
        os.rename(p, p + '.bak')
        try:
            txt = run(RF.run_factory(self.params))
            self.assertIn('EURUSD: не получено 1 мес.', txt)
        finally:
            os.rename(p + '.bak', p)

    def test_xag_spread_fallback_and_measured(self):
        """BOT_SPEC §2.41.21 п. 3.4: измеренный спред XAGUSD, иначе 10.99 б.п. (значение владельца) с указанием источника."""
        self.reset(keep=())
        ctx = RF.build_ctx(self.params)
        probs, spreads, pairs, excl = RF.preflight(ctx)
        self.assertEqual(spreads['XAGUSD'], 10.99)
        self.assertIn('10.99', ctx['xag_src'])
        self.assertIn('не измерено', ctx['xag_src'])
        RF.kv_set('spread:XAGUSD', {'median': 7.5}, self.d)
        ctx = RF.build_ctx(self.params)
        probs, spreads, pairs, excl = RF.preflight(ctx)
        self.assertEqual(spreads['XAGUSD'], 7.5)
        self.assertIn('измерено', ctx['xag_src'])
        self.assertNotIn('не измерено', ctx['xag_src'])

    def test_pairs_top10_sorted_and_not_acked(self):
        self.reset()
        txt = RF.run_pairs(self.params)
        pairs = RF.kv_get('pairs', self.d)
        self.assertEqual(len(pairs), 10)
        cs = [c for _a, _b, c, _n in pairs]
        self.assertEqual(cs, sorted(cs, reverse=True))
        self.assertTrue(all(a < b for a, b, _c, _n in pairs))
        self.assertFalse(RF.kv_get('pairs_ack', self.d))
        self.assertIn('/factory_pairs ok', txt)
        # корреляция считается только по TRAIN: изменение данных 2024 не меняет список
        again = RF.select_pairs(self.vd, SYMS)
        self.assertEqual(again, pairs)


class Pipeline(Base):
    def test_factory_report_and_resume(self):
        self.reset()
        self.ready()
        msgs = []

        async def prog(m):
            msgs.append(m)
        txt = run(RF.run_factory(self.params, prog))
        self.assertIn('КАЛИБРОВКА', txt)
        self.assertIn('ШАГ A (TRAIN', txt)
        self.assertIn('ШАГ B (VALID', txt)
        self.assertIn('ТАБЛИЦА ВСЕХ N ТЕСТОВ', txt)
        import re
        n_all = RF.count_n({'CRYPTO': 6, 'COMMODITIES': 2, 'FX': 6, 'INDICES': 0, 'BONDS': 0, 'MULTI': 8})
        n_rep = int(re.search(r'N = (\d+)', txt).group(1))
        self.assertLessEqual(n_rep, n_all)
        rows = [ln for ln in txt.splitlines() if ' | ' in ln and ln.split(' | ')[0].split(' ')[0] in ('CRYPTO', 'COMMODITIES', 'FX', 'MULTI') and 'нет данных' not in ln
                and not ln.startswith('ячейка')]
        self.assertEqual(len(rows), n_rep)
        # первым разделом идёт проверка смещения (до калибровки)
        self.assertIn('0. ПРОВЕРКА СМЕЩЕНИЯ ПРАВИЛ НА СИНТЕТИКЕ', txt)
        self.assertLess(txt.index('0. ПРОВЕРКА СМЕЩЕНИЯ'), txt.index('КАЛИБРОВКА'))
        self.assertIn('Спред XAGUSD:', txt)
        self.assertTrue(any('результаты скрыты до калибровки' in m for m in msgs))     # реальные результаты не показываются до калибровки
        self.assertIsNotNone(RF.kv_get('candidates', self.d))
        self.assertLessEqual(len(RF.kv_get('candidates', self.d)), 3)
        # калибровка на случайном блуждании не должна «проходить» процедуру отбора чаще 5%
        excl = RF.kv_get('excluded_families', self.d)
        tag = ','.join(sorted(excl)) or '-'
        for ci in range(len(FD.CELLS)):
            cal = RF.kv_get(f'cell:{RF.REV}:{ci}:{tag}', self.d)['calib']
            self.assertFalse(cal['fail'], (FD.CELLS[ci], cal))
        # продолжение: сохранённые ячейки не пересчитываются
        calls = []
        orig = RF.run_config
        RF.run_config = lambda *aa, **kk: (calls.append(1), orig(*aa, **kk))[1]
        try:
            txt2 = run(RF.run_factory(self.params))
        finally:
            RF.run_config = orig
        self.assertEqual(calls, [])
        self.assertEqual(txt2.split('Время:')[0], txt.split('Время:')[0])

    def test_calibration_failure_stops_and_hides_results(self):
        self.reset()
        self.ready()
        orig = E.nw_t
        E.nw_t = lambda x, lag=5: (0.001, 6.0, len(x))                          # все случайные «проходят» шаг A
        try:
            txt = run(RF.run_factory(self.params))
        finally:
            E.nw_t = orig
        self.assertIn('ПРОЦЕДУРА ОТБОРА НЕНАДЁЖНА', txt)
        self.assertNotIn('ТАБЛИЦА ВСЕХ N ТЕСТОВ', txt)
        self.assertNotIn('КАНДИДАТЫ НА ФИНАЛ', txt)
        self.assertIsNone(RF.kv_get('candidates', self.d))


class Final(Base):
    def test_final_once_and_flag(self):
        self.reset()
        self.ready()
        RF.kv_set('candidates', ['FX|1D|T-MA-10-50', 'COMMODITIES|1D|T-DC-20'], self.d)
        # не из утверждённых кандидатов — отказ, флаг НЕ взят
        txt = run(RF.run_final(dict(self.params, ids=['FX|1D|T-MA-20-100'])))
        self.assertIn('/factory_final отказ', txt)
        self.assertIsNone(RF.kv_get('final_started', self.d))
        # не выполнены условия (пары не подтверждены) — отказ, флаг НЕ взят
        RF.kv_set('pairs_ack', False, self.d)
        txt = run(RF.run_final(self.params))
        self.assertIn('флаг финала НЕ взят', txt)
        self.assertIsNone(RF.kv_get('final_started', self.d))
        RF.kv_set('pairs_ack', True, self.d)
        txt = run(RF.run_final(self.params))
        self.assertIn('шаг C на LOCKBOX', txt)
        self.assertIn('FX|1D|T-MA-10-50', txt)
        self.assertTrue(('ПРОЙДЕНО' in txt) or ('НЕ ПРОЙДЕНО' in txt))
        self.assertTrue(RF.kv_get('final_started', self.d))
        self.assertTrue(RF.kv_get('final_done', self.d))
        res = RF.kv_get('final_result', self.d)
        self.assertEqual([r[0] for r in res['rows']], ['FX|1D|T-MA-10-50', 'COMMODITIES|1D|T-DC-20'])
        for _t, m in res['rows']:
            self.assertGreater(m['n'], 30)                                       # LOCKBOX 2025-01…2025-02: дней > 30
        # повтор запрещён
        L._lockbox_session_state['opened'] = False
        txt2 = run(RF.run_final(self.params))
        self.assertIn('финал уже запускался', txt2)
        self.assertIn('повтор запрещён', txt2)

    def test_more_than_three_finalists_refused(self):
        self.reset()
        self.ready()
        ids = [f'FX|1D|{c}' for c in ('T-MA-10-50', 'T-MA-20-100', 'T-MA-50-200', 'T-DC-20')]
        RF.kv_set('candidates', ids, self.d)
        txt = run(RF.run_final(self.params))
        self.assertIn('больше 3', txt)
        self.assertIsNone(RF.kv_get('final_started', self.d))

    def test_stress_costs_double_and_lockbox_metrics(self):
        days = np.arange(20_000, 20_200)
        r = np.full(200, 0.001)
        m = RF.lockbox_metrics(days, r, 252, days, r - 0.0005)
        self.assertTrue(m['stress_mean'] > 0)
        m2 = RF.lockbox_metrics(days, r, 252, days, r - 0.002)                       # при ×2 издержек mean < 0 → не пройдено
        self.assertFalse(m2['pass'])
        self.assertLess(m2['stress_mean'], 0)
        noisy = np.random.default_rng(1).normal(0.002, 0.003, 200)
        m3 = RF.lockbox_metrics(days, noisy, 252, days, noisy - 0.0005)
        self.assertTrue(m3['pass'], m3)
        self.assertFalse(RF.lockbox_metrics(days, -noisy, 252, days, -noisy)['pass'])


class Wiring(unittest.TestCase):
    def test_runner_tasks_and_commands(self):
        for t in ('factory_load', 'factory_pairs', 'factory', 'factory_final', 'data_probe_ma', 'carry_test'):
            self.assertIn(t, RR.TASKS)
        src = open(os.path.join(ROOT, 'async_bot.py'), encoding='utf-8').read()
        for needle in ("cmd == '/factory_load'", "cmd == '/factory'", "cmd == '/factory_pairs' or cmd == '/factory_pairs ok'", "cmd == '/factory_final' or cmd.startswith('/factory_final ')",
                       "'factory_final'", '/factory_load —'):
            self.assertIn(needle, src)

    def test_factory_final_requires_confirm(self):
        sent, jobs = [], []

        async def fake_tg(text):
            sent.append(text)

        async def fake_job(cmd, task, extra=None, need_universe=True):
            jobs.append((cmd, task, extra))
        old_tg, old_job = a.tg, a.factory_job
        a.tg, a.factory_job = fake_tg, fake_job
        try:
            run(a.factory_final_gate([]))
            self.assertEqual(jobs, [])
            self.assertIn('confirm', sent[-1])
            run(a.factory_final_gate(['CONFIRM', 'FX|1D|T-MA-10-50']))
            self.assertEqual(jobs, [('/factory_final', 'factory_final', {'ids': ['FX|1D|T-MA-10-50']})])
            run(a.factory_final_gate(['confirm']))
            self.assertEqual(jobs[-1][2], {'ids': None})
        finally:
            a.tg, a.factory_job = old_tg, old_job

    def test_html_safe(self):
        self.assertEqual(a._html_safe('R0: |t| = 0.58 < 2 & x'), 'R0: |t| = 0.58 &lt; 2 &amp; x')
        self.assertEqual(a._html_safe('<b>S&amp;P 500</b> <code>a<b</code>'), '<b>S&amp;P 500</b> <code>a&lt;b</code>')
        self.assertEqual(a._html_safe('<a href="https://x.y/?a=1&b=2">ссылка</a>'), '<a href="https://x.y/?a=1&amp;b=2">ссылка</a>')
        self.assertEqual(a._html_safe('без разметки'), 'без разметки')
        self.assertEqual(a._html_safe(a.esc('<x> & y')), a.esc('<x> & y'))              # уже экранированное не ломается

    def test_digest_alert_line_plain(self):
        self.assertEqual(a._plain_alert_line('🟢 <b>Бот v103</b> S&amp;P 500\nвторая строка'), '🟢 Бот v103 S&P 500')
        self.assertEqual(a._plain_alert_line(''), '')


if __name__ == '__main__':
    unittest.main()
