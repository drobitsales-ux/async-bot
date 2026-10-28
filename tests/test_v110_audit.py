"""v110: аудит — backfill (без дублей, FloodWait, продолжение с места остановки), привязка отчётов к сигналам (reply_to / символ ≤ 14 суток), расшифровка ожидания модели B,
/audit_stop и /audit_start (флаг в БД, «сессия активна с»)."""
import datetime
import os
import sqlite3
import tempfile
import unittest

import numpy as np

import _common
import audit_bot as AB
import audit_db as AD
import audit_parse as AP
import audit_sim as AS
import audit_tg as AT
from test_v109_audit import ENA, FakeClient, FakeMsg, FakeSource, MIN, candles, run

ROOT = _common.ROOT
DAY = 86_400


class FloodWaitError(Exception):
    def __init__(self, seconds):
        super().__init__(f'A wait of {seconds} seconds is required')
        self.seconds = seconds


class BFClient(FakeClient):
    """Фейк с историей 1..N, порядком reverse/offset_id как в Telethon и однократным FloodWait после k выданных сообщений."""

    def __init__(self, *a, n=250, flood_after=None, **kw):
        super().__init__(*a, **kw)
        self.n, self.flood_after, self.flooded = n, flood_after, False
        self.calls_iter = []

    async def iter_messages(self, ent, limit=None, reverse=False, offset_id=0):
        self.calls_iter.append((limit, reverse, offset_id))
        ids = [i for i in range(1, self.n + 1) if (i > offset_id if reverse else (offset_id == 0 or i < offset_id))]
        if not reverse:
            ids.reverse()
        if limit is not None:
            ids = ids[:limit]
        for k, i in enumerate(ids):
            if self.flood_after is not None and not self.flooded and k == self.flood_after:
                self.flooded = True
                raise FloodWaitError(7)
            yield FakeMsg(i, ENA if i % 10 == 0 else f'привет {i}', ts=1_760_000_000 + i * 60)


class Base(unittest.TestCase):
    def make(self, **kw):
        d = tempfile.mkdtemp()
        self.session = os.path.join(d, 'tg')
        self.db = os.path.join(d, 'audit.db')
        self.client = BFClient(self.session, '+380000000000', 'hash', **kw)
        self.client.authorized = True
        self.a = AT.AuditTG('1', 'hash', ['@chan'], self.session, self.db, '+380000000000', None, client_factory=lambda: self.client)
        self.said, self.slept = [], []

        async def say(m):
            self.said.append(m)

        async def sleep(sec):
            self.slept.append(sec)
        self.say, self.sleep = say, sleep
        return self.a

    def bf(self, mx=1000):
        return run(self.a.backfill(mx, self.say, sleep=self.sleep, batch=100))

    def con(self):
        return AD.connect(self.db)


class Backfill(Base):
    def test_full_history_no_duplicates_and_resume(self):
        self.make(n=250)
        con = self.con()
        for i in range(1, 51):                                              # 50 сообщений уже есть (live) — не дублируются
            AD.ingest(con, '@chan', i, 1_760_000_000 + i * 60, f'привет {i}', 'live')
        out = self.bf()
        self.assertIn('история прочитана до конца', out)
        n_new = con.execute("SELECT COUNT(*) FROM audit_events WHERE ev='new'").fetchone()[0]
        self.assertEqual(n_new, 250)                                          # ключ канал + id: ни одного дубля
        self.assertEqual(AD.counts(con)['signals'], {'catchup': 20})           # сигналы (id кратные 10 среди 51…250; 1…50 уже были) — источник catchup, не форвард
        self.assertEqual(con.execute("SELECT source FROM audit_events WHERE msg_id=1").fetchone()[0], 'live')   # ранее сохранённое не перезаписано
        self.assertEqual(AD.progress_get(con, '@chan'), (250, 250, 1))
        self.client.calls_iter.clear()
        out2 = self.bf()                                                      # повторный запуск: продолжает с последнего id, нового нет
        self.assertEqual(self.client.calls_iter[0][2], 250)
        self.assertEqual(con.execute("SELECT COUNT(*) FROM audit_events WHERE ev='new'").fetchone()[0], 250)
        self.assertIn('новых 0', out2)

    def test_survives_floodwait(self):
        self.make(n=250, flood_after=120)
        out = self.bf()
        self.assertEqual(self.slept, [8])                                      # ждём указанное время (+1 с) и продолжаем
        self.assertTrue(any('FloodWait' in m for m in self.said))
        con = self.con()
        self.assertEqual(con.execute("SELECT COUNT(*) FROM audit_events WHERE ev='new'").fetchone()[0], 250)
        self.assertIn('история прочитана до конца', out)
        self.assertEqual(self.client.calls_iter[1][2], 120)                    # после паузы — с id последнего обработанного сообщения

    def test_limit_then_resume_and_stop(self):
        self.make(n=250)
        out = self.bf(100)
        self.assertIn('достигнут лимит 100', out)
        con = self.con()
        self.assertEqual(AD.progress_get(con, '@chan'), (100, 100, 0))
        out = self.bf(1000)                                                     # поднятый лимит: продолжение с id 100
        self.assertEqual(self.client.calls_iter[-1][2], 100)
        self.assertEqual(AD.progress_get(con, '@chan'), (250, 250, 1))
        # остановка во время чтения (как /audit_stop): прогресс сохраняется, повторный запуск продолжает
        self.make(n=250)

        async def say_stop(m):
            self.a._stop = True
        out = run(self.a.backfill(1000, say_stop, sleep=self.sleep, batch=50))
        self.assertIn('остановлено', out)
        con = self.con()
        last, n, done = AD.progress_get(con, '@chan')
        self.assertEqual((n, done), (50, 0))                                       # остановились сразу после первого прогресс-сообщения (сообщение 51 не обработано)
        out = run(self.a.backfill(1000, self.say, sleep=self.sleep, batch=100))
        self.assertEqual(AD.progress_get(con, '@chan')[1:], (250, 1))

    def test_read_only_and_progress_table_mutable_but_data_immutable(self):
        self.make(n=30)
        self.bf()
        self.assertTrue(set(self.client.calls) <= {'get_entity', 'catch_up'})
        con = self.con()
        con.execute('UPDATE audit_progress SET n = n')                           # служебная таблица изменяема
        with self.assertRaises(sqlite3.IntegrityError):
            con.execute('UPDATE audit_signals SET stop = 1')


def sig(mid, sym='INJ', side='long', pub=1_000_000, ch='c', lev=25, entry=(8.0, 8.0), targets=(8.2, 8.4), stop=7.0):
    return {'channel': ch, 'msg_id': mid, 'symbol': sym, 'side': side, 'leverage': lev, 'entry_lo': entry[0], 'entry_hi': entry[1], 'targets': list(targets), 'stop': stop, 'pub_ts': pub, 'source': 'catchup'}


def rep(mid, reply=None, sym=None, side=None, pub=1_100_000, k=1, pct=50.0, ch='c'):
    return {'channel': ch, 'msg_id': mid, 'source': 'catchup', 'pub_ts': pub, 'symbol': sym, 'reply_to': reply, 'side': side, 'target_no': k, 'claimed_pct': pct}


class Linking(unittest.TestCase):
    def test_by_reply_chain_and_by_symbol_14_days(self):
        sigs = [sig(1, pub=1_000_000), sig(2, 'PEPE', 'short', pub=1_000_000 + 5 * DAY)]
        reports = [rep(10, reply=1),                                            # прямой ответ на сигнал
                   rep(11, reply=10),                                           # ответ на отчёт → по цепочке до сигнала
                   rep(12, sym='PEPE', pub=1_000_000 + 5 * DAY + 3600),         # по символу
                   rep(13, sym='INJ', pub=1_000_000 + 14 * DAY),                # ровно 14 суток — ещё привязывается
                   rep(14, sym='INJ', pub=1_000_000 + 14 * DAY + 1),            # старше 14 суток — нет
                   rep(15, sym='INJ', side='short', pub=1_000_000 + DAY),       # направление названо и не совпало
                   rep(16, sym='INJ', pub=1_000_000 + DAY, ch='other'),         # другой канал
                   rep(17)]                                                     # ни reply_to, ни символа
        links = AS.link_reports(reports, sigs, {('c', 11): 10, ('c', 10): 1})
        got = {l['report']['msg_id']: (l['signal']['msg_id'] if l['signal'] else None, l['how']) for l in links}
        self.assertEqual(got[10], (1, 'reply'))
        self.assertEqual(got[11], (1, 'reply'))
        self.assertEqual(got[12], (2, 'symbol'))
        self.assertEqual(got[13], (1, 'symbol'))
        for k in (14, 15, 16, 17):
            self.assertEqual(got[k], (None, None), k)

    def test_symbol_picks_latest_signal(self):
        sigs = [sig(1, pub=1_000_000), sig(2, pub=1_000_000 + DAY)]
        links = AS.link_reports([rep(10, sym='INJ', pub=1_000_000 + 2 * DAY)], sigs)
        self.assertEqual(links[0]['signal']['msg_id'], 2)

    def test_report_without_symbol_parses_and_links_by_reply(self):
        p = AP.parse('Ціль 2 (45%) ✅')
        self.assertEqual((p['kind'], p['symbol'], p['target_no'], p['claimed_pct']), ('report', None, 2, 45.0))

    def test_check_reached_match_and_after_liquidation(self):
        s = sig(1, entry=(8.0, 8.0), targets=(8.2, 8.4), lev=25)                 # цель 1: +2.5% цены → +62.5% на маржу при 25x
        links = AS.link_reports([rep(10, reply=1, k=1, pct=60.0), rep(11, reply=1, k=2, pct=100.0), rep(12, reply=1, k=1, pct=20.0, pub=1_000_050)], [s])
        res = {('c', 1): {'status': 'filled', 'A': {'liq': True, 'liq_ts': 1_000_100_000, 'ret': -1.0}, 'hit_A': [1], 'hit': [1, 2]}}
        rows, sm = AS.report_check(links, res)
        self.assertAlmostEqual(rows[0]['computed'], 62.5, places=6)
        self.assertTrue(rows[0]['match'])                                         # 60 против 62.5: в пределах ±10% от расчётного
        self.assertFalse(rows[2]['match'])
        self.assertTrue(rows[0]['reached'])                                       # цель 1 достигнута до ликвидации
        self.assertFalse(rows[1]['reached'])                                      # цель 2 после ликвидации модели A — нет
        self.assertTrue(rows[0]['after_liq'])                                     # отчёт (pub 1_100_000 с) позже ликвидации (1_000_100 с)
        self.assertFalse(rows[2]['after_liq'])                                    # отчёт опубликован ДО ликвидации (1_000_050 < 1_000_100 с)
        self.assertEqual((sm['n_linked'], sm['reached'], sm['n_eval']), (3, 2, 3))


class Expectancy(unittest.TestCase):
    def rows(self):
        out = []
        for i in range(100):
            if i < 90:
                out.append({'B': {'R': 0.1, 'kinds': ['tp', 'be'] if i < 30 else ['tp', 'tp']}})
            else:
                out.append({'B': {'R': -1.5, 'kinds': ['stop']}})
        return out

    def test_wr_90_small_wins_big_losses_gives_negative_mean(self):
        ex = AS.expectancy(self.rows())
        self.assertAlmostEqual(ex['wr'], 0.9)
        self.assertAlmostEqual(ex['mean'], 0.9 * 0.1 + 0.1 * -1.5)
        self.assertLess(ex['mean'], 0)                                            # WR 90%, но ожидание отрицательно
        self.assertAlmostEqual(ex['ratio'], 15.0)
        self.assertIn('средний проигрыш в 15.0 раза больше среднего выигрыша', ex['text'])
        self.assertEqual((ex['n_be'], ex['n_stop']), (30, 10))
        self.assertAlmostEqual(ex['be_share'], 0.30)
        self.assertAlmostEqual(ex['sum_stop'], -15.0)                              # вклад полных стопов
        self.assertAlmostEqual(ex['sum_all'], 9.0 - 15.0)

    def test_section_prints_decomposition(self):
        rows = []
        for i, r in enumerate(self.rows()):
            rows.append({'status': 'filled', 'pub_ms': (1_700_000_000 + i * 3600) * 1000, 'exit_ts': (1_700_000_000 + i * 3600) * 1000 + 600_000, 'B': dict(r['B'], closed=True),
                         'A': None, 'side': 'long', 'symbol': 'X', 'source': 'live', 'note': 'BingX'})
        txt = '\n'.join(AS.section('t', rows, {}))
        for needle in ('расшифровка ожидания', 'средний проигрыш в 15.0 раза больше', 'в безубыток после первой цели: 30 (30%)', 'полные стопы (без единой цели): 10 шт.', 'месяцы роста BTC'):
            self.assertIn(needle, txt)


class Hygiene(Base):
    def test_stop_flag_persists_start_refuses_resume_works_and_status(self):
        os.environ['AUDIT_DB'] = self.make(n=10) and self.db
        os.environ.update({'TG_API_ID': '1', 'TG_API_HASH': 'hash', 'AUDIT_CHANNELS': '@chan'})
        AB.STATE['tg'] = self.a
        try:
            msg = run(self.a.start())
            self.assertIn('запущено', msg)
            st = AB.status_text()
            self.assertIn('Сессия активна с ' + datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d'), st)
            said = []

            async def tg(m):
                said.append(m)
            run(AB.stop(tg))
            self.assertIn('TG_SESSION_STRING', said[0])
            self.assertIn('Устройства', said[0])
            self.assertFalse(self.client.connected)
            self.assertFalse(any('log_out' in c or 'sign_out' in c for c in self.client.calls))     # из аккаунта не выходим
            self.assertIn('ОСТАНОВЛЕНО', AB.status_text())
            self.assertNotIn('Сессия активна с', AB.status_text())
            # после «перезапуска» (новый объект) чтение не стартует
            b = AT.AuditTG('1', 'hash', ['@chan'], self.session, self.db, None, None, client_factory=lambda: self.client)
            self.assertIn('остановлено', run(b.start()))
            self.assertFalse(b.started)
            self.assertIn('остановлено', run(b.backfill(10, self.say, sleep=self.sleep)).lower())
            AD.meta_set(AD.connect(self.db), 'stopped', None)
            self.assertIn('запущено', run(AT.AuditTG('1', 'hash', ['@chan'], self.session, self.db, None, None, client_factory=lambda: self.client).start()))
        finally:
            AB.STATE['tg'] = None
            for k in ('AUDIT_DB', 'TG_API_ID', 'TG_API_HASH', 'AUDIT_CHANNELS'):
                os.environ.pop(k, None)

    def test_backfill_max_env_and_wiring(self):
        os.environ['AUDIT_BACKFILL_MAX'] = '123'
        try:
            self.assertEqual(AB.backfill_max(), 123)
        finally:
            os.environ.pop('AUDIT_BACKFILL_MAX')
        self.assertEqual(AB.backfill_max(), 20000)
        src = open(os.path.join(ROOT, 'async_bot.py'), encoding='utf-8').read()
        for c in ("cmd == '/audit_backfill'", "cmd == '/audit_stop'", "cmd == '/audit_start'", 'audit_bot.backfill(tg)'):
            self.assertIn(c, src)


class EndToEnd(unittest.TestCase):
    def test_report_has_new_sections(self):
        path = os.path.join(tempfile.mkdtemp(), 'audit.db')
        con = AD.connect(path)
        t0 = 1_760_000_000
        AD.ingest(con, 'ch', 1, t0, ENA, 'live')
        AD.ingest(con, 'ch', 2, t0 + 3600, 'Ціль 1 (40%) ✅', 'live', reply_to=1)                 # отчёт без символа: привязка по reply_to
        px = [(0.284, 0.2851, 0.2839, 0.2845)] + [(0.2845, 0.2846, 0.2744, 0.2745)] + [(0.2745, 0.2746, 0.2640, 0.2641)] + [(0.2641, 0.2641, 0.2640, 0.2640)] * (3 * 24 * 60)
        c = candles(px)
        c['ts'] = (t0 // 60 + 1) * MIN + MIN * np.arange(len(px), dtype=np.int64)
        txt = run(AS.run_audit({'adb': path, 'fee_taker': 0.0005, 'fee_maker': 0.0002, 'now_ms': (t0 + 30 * DAY) * 1000, 'version': 'vT'}, source=FakeSource(c)))
        for needle in ('Сверка отчётов о целях с расчётом', 'по reply_to 1', '(1) цель реально достигнута', '(2) заявленный %', '(3) отчёт опубликован', 'расшифровка ожидания'):
            self.assertIn(needle, txt)


if __name__ == '__main__':
    unittest.main()
