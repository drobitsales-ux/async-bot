"""v109: аудит внешнего источника сигналов (BOT_SPEC §2.44): парсер на фикстурах (форматы — по описанию владельца, НЕ со скриншотов), ликвидация/безубыток/неисполненный вход в симуляции,
неизменяемость таблиц, чтение каналов (фейковый клиент: только чтение, секреты не в логах и не в чате, права сессии 600, сессия вне git)."""
import asyncio
import json
import logging
import os
import re
import sqlite3
import subprocess
import tempfile
import unittest

import numpy as np

import _common
import audit_bot as AB
import audit_db as AD
import audit_parse as AP
import audit_sim as AS
import audit_tg as AT

ROOT = _common.ROOT
FEES = {'taker': 0.0005, 'maker': 0.0002}
FEES0 = {'taker': 0.0, 'maker': 0.0}
MIN = 60_000


def run(c):
    return asyncio.run(c)


ENA = """🔴 SHORT #ENA/USDT
Плечо: 25x
Вход: 0.2850 - 0.2900
Цели: 0.2800 / 0.2750 / 0.2700 / 0.2650
Стоп: 0.3200"""
WLD = """🟢 LONG WLD
Вхід: 2.35
Тейки: 2.40, 2.45, 2.50, 2.60
SL: 2.20
x10"""
WIF = """#WIF/USDT SHORT
Leverage: Cross 20x
Entry: 1.8500
Target 1: 1.80
Target 2: 1.75
Target 3: 1.70
Target 4: 1.60
Stop Loss: 2.05"""
DEEP = """🔻ШОРТ DEEP
Точка входа: 0,1250
Цель 1 - 0,1220
Цель 2 - 0,1190
Цель 3 - 0,1150
Цель 4 - 0,1100
Стоп - 0,1400
Плечо 15х"""


class Parser(unittest.TestCase):
    def test_fixture_formats(self):
        p = AP.parse(ENA)
        self.assertEqual((p['kind'], p['symbol'], p['side'], p['leverage'], p['entry_lo'], p['entry_hi'], p['stop']), ('signal', 'ENA', 'short', 25, 0.285, 0.29, 0.32))
        self.assertEqual(p['targets'], [0.28, 0.275, 0.27, 0.265])
        p = AP.parse(WLD)
        self.assertEqual((p['symbol'], p['side'], p['leverage'], p['entry_lo'], p['entry_hi'], p['stop']), ('WLD', 'long', 10, 2.35, 2.35, 2.2))
        self.assertEqual(p['targets'], [2.4, 2.45, 2.5, 2.6])
        p = AP.parse(WIF)
        self.assertEqual((p['symbol'], p['side'], p['leverage'], p['entry_lo'], p['stop']), ('WIF', 'short', 20, 1.85, 2.05))
        self.assertEqual(p['targets'], [1.8, 1.75, 1.7, 1.6])
        p = AP.parse(DEEP)
        self.assertEqual((p['symbol'], p['side'], p['leverage'], p['entry_lo'], p['stop']), ('DEEP', 'short', 15, 0.125, 0.14))
        self.assertEqual(len(p['targets']), 4)

    def test_other_shapes(self):
        p = AP.parse('SHORT BTCUSDT\nВход: 67 500\nЦели:\n1) 66 800\n2) 66 000\nСтоп: 69 000\n25x')
        self.assertEqual((p['kind'], p['symbol'], p['entry_lo'], p['targets'], p['stop']), ('signal', 'BTC', 67500.0, [66800.0, 66000.0], 69000.0))
        p = AP.parse('$PEPEUSDT LONG\nEntry zone: 0.0000120 - 0.0000125\nTP1: 0.0000130\nTP2: 0.0000135\nSL: 0.0000110')
        self.assertEqual((p['kind'], p['symbol'], p['entry_lo'], p['entry_hi'], p['leverage']), ('signal', 'PEPE', 1.2e-05, 1.25e-05, None))

    def test_unparsed_reasons_and_report_and_other(self):
        p = AP.parse('SHORT #ENA/USDT\nEntry: 0.2850\nTargets: 0.28, 0.27\nStop: 12%')
        self.assertEqual(p['kind'], 'unparsed')
        self.assertIn('стоп', p['reason'])
        p = AP.parse('🔴 SHORT #ENA/USDT\nВход: 0.2850\nЦели: 0.2900 / 0.3000\nСтоп: 0.3200')
        self.assertEqual(p['kind'], 'unparsed')
        self.assertIn('геометрия', p['reason'])                                    # нет «правдоподобной геометрии» — не додумываем
        p = AP.parse('LONG SHORT ENA\nВход: 1\nЦель: 2\nСтоп: 0.5')
        self.assertEqual(p['kind'], 'unparsed')
        self.assertIn('оба направления', p['reason'])
        r = AP.parse('ENA цель 2 достигнута +85% 🔥')
        self.assertEqual((r['kind'], r['symbol'], r['target_no'], r['claimed_pct']), ('report', 'ENA', 2, 85.0))
        self.assertEqual(AP.parse('Всем доброго утра, рынок волатильный')['kind'], 'other')
        self.assertEqual(AP.parse('')['kind'], 'other')

    def test_number_formats(self):
        self.assertEqual(AP.to_float('0,2345'), 0.2345)
        self.assertEqual(AP.to_float('1 234.5'), 1234.5)
        self.assertEqual(AP.to_float('1,234,567'), 1234567.0)


def candles(rows, ts0=0):
    a = np.array(rows, float)
    n = len(a)
    return {'ts': (ts0 // MIN) * MIN + MIN * np.arange(n, dtype=np.int64) + 0, 'o': a[:, 0], 'h': a[:, 1], 'l': a[:, 2], 'c': a[:, 3]}


def flat(price, n, hi=0.0, lo=0.0):
    return [(price, price + hi, price - lo, price)] * n


def sig(side='short', lev=25, lo=100.0, hi=100.0, targets=(95.0, 90.0), stop=112.0, pub_ms=0):
    return {'side': side, 'leverage': lev, 'entry_lo': lo, 'entry_hi': hi, 'targets': list(targets), 'stop': stop, 'pub_ms': pub_ms}


NOW = 10 ** 13


class Simulation(unittest.TestCase):
    def sim(self, rows, **kw):
        c = candles(rows)
        c['ts'] = c['ts'] + MIN                               # первая свеча — следующая минута после публикации (pub_ms = 0)
        return AS.simulate(sig(**kw), c, FEES0, NOW)

    def test_liquidation_at_25x_before_12pct_stop_gives_minus_100_percent_margin(self):
        # шорт 100, плечо 25x: ликвидация ≈ 100·(1 + 1/25 − 0.5%) = 103.5, стоп 112 дальше → модель A: −100% маржи
        rows = flat(99.0, 2) + [(99.0, 100.0, 99.0, 100.0)] + [(100.0, 104.0, 100.0, 104.0)] + flat(95.0, 3, lo=1.0)
        r = self.sim(rows)
        self.assertEqual(r['status'], 'filled')
        self.assertTrue(r['A']['liq'])
        self.assertEqual(r['A']['ret'], -1.0)
        self.assertGreater(r['B']['R'], 0)                    # модель B: стопа 112 не было, цена вернулась к целям

    def test_stop_closer_than_liquidation_is_normal_stop(self):
        rows = flat(99.0, 1) + [(99.0, 100.0, 99.0, 100.0)] + [(100.0, 106.0, 100.0, 106.0)]
        r = self.sim(rows, lev=5, stop=105.0)                 # ликвидация 5x: 119.5, стоп 105 ближе
        self.assertFalse(r['A']['liq'])
        self.assertAlmostEqual(r['A']['ret'], 5 * -0.05, delta=0.01)          # 5 × (−5% стоп) плюс издержки
        self.assertAlmostEqual(r['B']['R'], -1.0, delta=0.05)

    def test_breakeven_after_first_target(self):
        rows = [(99.0, 100.0, 99.0, 100.0)] + [(100.0, 100.0, 94.0, 95.0)] + [(95.0, 100.0, 95.0, 99.0)] + flat(99.0, 3, hi=0.5)
        c = candles(rows)
        c['ts'] = c['ts'] + MIN
        w = AS.walk('short', c, 0, 100.0, [95.0, 90.0], 112.0, None, NOW)
        self.assertEqual([f[0] for f in w['fills']], ['tp', 'be'])               # первая цель, затем стоп в безубытке по цене входа
        self.assertEqual((w['fills'][1][1], w['fills'][1][2]), (100.0, 0.5))
        r = AS.simulate(sig(), c, FEES0, NOW)
        self.assertAlmostEqual(r['B']['R'], 0.5 * 0.05 / 0.12, delta=0.02)          # половина позиции по цели 95 (+5%), вторая половина — в безубыток; риск 12% до стопа
        self.assertGreater(r['B']['R'], 0)
        self.assertLess(r['B']['R'], 1.0)

    def test_targets_not_in_entry_candle_and_stop_has_priority(self):
        # в свече входа цель не засчитывается; в одной свече стоп и цель — стоп раньше
        rows = [(99.0, 100.0, 90.0, 92.0)] + [(92.0, 113.0, 89.0, 100.0)]
        c = candles(rows)
        c['ts'] = c['ts'] + MIN
        w = AS.walk('short', c, 0, 100.0, [95.0], 112.0, None, NOW)
        self.assertEqual([f[0] for f in w['fills']], ['stop'])
        self.assertEqual(w['hit'], [])

    def test_unfilled_entry_is_not_in_r(self):
        rows = flat(90.0, 50)
        r = self.sim(rows)
        self.assertEqual(r['status'], 'not_filled')
        self.assertNotIn('B', r)
        sims = [dict(r, pub_ms=0, source='live', symbol='X', side='short', lev=25, note='BingX')]
        sec = '\n'.join(AS.section('t', sims, {}))
        self.assertIn('не исполнено 1', sec)
        self.assertIn('нет закрытых исполненных', sec)
        v = AS.verdict(sims, {})
        self.assertEqual(v['stats']['n'], 0)

    def test_entry_window_and_publication_candle(self):
        # цена касается входа ТОЛЬКО в минуту публикации (не используется) и спустя 25 ч (вне окна)
        n = 26 * 60
        rows = flat(90.0, n)
        c = candles(rows)
        c['ts'] = c['ts'] + 0                                  # свеча 0 начинается в момент публикации
        c['h'][0] = 101.0
        c['h'][25 * 60 + 5] = 101.0
        i, level = AS.find_entry(c, 30_000, 'short', 100.0, 100.0)    # pub внутри минуты 0 → ищем со свечи 1
        self.assertIsNone(i)
        c['h'][100] = 100.0
        i, level = AS.find_entry(c, 30_000, 'short', 100.0, 100.0)
        self.assertEqual(i, 100)

    def test_zone_worst_boundary(self):
        c = candles(flat(98.0, 20))
        c['ts'] = c['ts'] + MIN
        c['h'][5] = 100.5
        i, level = AS.find_entry(c, 0, 'short', 100.0, 102.0)                   # шорт: нижняя граница зоны
        self.assertEqual((i, level), (5, 100.0))
        c2 = candles(flat(105.0, 20))
        c2['ts'] = c2['ts'] + MIN
        c2['l'][7] = 101.9
        i, level = AS.find_entry(c2, 0, 'long', 100.0, 102.0)                    # лонг: верхняя граница зоны
        self.assertEqual((i, level), (7, 102.0))

    def test_costs_and_funding(self):
        rows = [(99.0, 100.0, 99.0, 100.0)] + [(100.0, 100.0, 94.0, 95.0)] + [(95.0, 95.0, 89.0, 90.0)]
        c = candles(rows)
        c['ts'] = c['ts'] + MIN
        a = AS.simulate(sig(), c, FEES0, NOW)['B']['R']
        b = AS.simulate(sig(), c, FEES, NOW)['B']['R']
        self.assertLess(b, a)                                                    # комиссии и проскальзывание уменьшают R
        f = (np.array([c['ts'][0] + 1000]), np.array([0.001]))                   # шорт при положительной ставке получает
        r = AS.simulate(sig(), c, FEES0, NOW, funding=f)['B']['R']
        self.assertGreater(r, a)

    def test_timeout_after_7_days_closes_at_close(self):
        n = 8 * 24 * 60
        rows = flat(99.0, 2) + [(99.0, 100.0, 99.0, 100.0)] + flat(99.0, n)
        c = candles(rows)
        c['ts'] = c['ts'] + MIN
        w = AS.walk('short', c, 2, 100.0, [90.0], 112.0, None, NOW)
        self.assertEqual(w['fills'][-1][0], 'timeout')
        self.assertTrue(w['closed'])
        open_ = AS.walk('short', candles(flat(99.0, 100)), 50, 100.0, [90.0], 112.0, None, 0)
        self.assertFalse(open_['closed'])                                        # данных меньше срока и время не вышло — позиция ещё открыта (в R не входит)


class Stats(unittest.TestCase):
    def test_stats_and_verdict(self):
        st = AS.r_stats([1, -1, 1, 1, -1, -1, -1])
        self.assertEqual((st['n'], st['streak']), (7, 3))
        self.assertAlmostEqual(st['pf'], 3 / 4)
        rng = np.random.default_rng(1)
        rows = []
        for i in range(150):
            r = float(rng.normal(0.4, 1.0))
            rows.append({'status': 'filled', 'pub_ms': (1_700_000_000 + i * 43200) * 1000, 'exit_ts': (1_700_000_000 + i * 43200) * 1000 + 3_600_000, 'B': {'R': r, 'closed': True}})
        v = AS.verdict(rows, {})
        self.assertTrue(v['crit']['n'])
        self.assertIsNone(v['crit']['up'])                                       # нет месяцев BTC — группа не применима
        few = AS.verdict(rows[:10], {})
        self.assertFalse(few['crit']['n'])
        self.assertFalse(few['pass'])

    def test_claimed_vs_computed(self):
        sigs = [{'channel': 'c', 'msg_id': 1, 'symbol': 'ENA', 'side': 'short', 'leverage': 25, 'entry_lo': 0.285, 'entry_hi': 0.29, 'targets': [0.28, 0.275], 'pub_ts': 100}]
        reps = [{'channel': 'c', 'msg_id': 2, 'symbol': 'ENA', 'reply_to': None, 'pub_ts': 200, 'payload': {'target_no': 1, 'claimed_pct': 85.0}}]
        rows = AS.claimed_vs_computed(reps, sigs)
        self.assertAlmostEqual(rows[0]['computed'], (1 - 0.28 / 0.285) * 100 * 25, places=6)


class Store(unittest.TestCase):
    def setUp(self):
        self.path = os.path.join(tempfile.mkdtemp(), 'audit.db')
        self.con = AD.connect(self.path)

    def test_tables_are_immutable_and_edits_are_events(self):
        self.assertEqual(AD.ingest(self.con, 'ch', 1, 1_700_000_000, ENA, 'live'), 'signal')
        AD.ingest(self.con, 'ch', 2, 1_700_000_100, 'просто текст', 'live')                                  # строка в audit_other (триггер срабатывает по строкам)
        self.assertEqual(AD.ingest(self.con, 'ch', 1, 1_700_000_000, 'другой текст', 'live'), 'dup')       # повтор не меняет исходный текст
        AD.record_event(self.con, 'ch', 1, 'edit', 'правка', 'live')
        AD.record_event(self.con, 'ch', 1, 'delete', None, 'live')
        for sql in ('UPDATE audit_signals SET stop = 1', 'DELETE FROM audit_signals', "UPDATE audit_events SET text = 'x'", 'DELETE FROM audit_events', "UPDATE audit_other SET kind = 'x'", 'DELETE FROM audit_other'):
            with self.assertRaises(sqlite3.IntegrityError):
                self.con.execute(sql)
        row = self.con.execute("SELECT text FROM audit_events WHERE msg_id=1 AND ev='new'").fetchone()
        self.assertEqual(row[0], ENA)
        self.assertEqual(AD.counts(self.con)['events'], {'new': 2, 'edit': 1, 'delete': 1})

    def test_import_telegram_desktop_format(self):
        data = {'name': 'Канал', 'id': 5, 'messages': [
            {'id': 1, 'type': 'message', 'date': '2026-01-02T10:00:00', 'date_unixtime': '1767348000', 'text': [{'type': 'bold', 'text': '🔴 SHORT #ENA/USDT\n'}, 'Вход: 0.2850 - 0.2900\nЦели: 0.28 / 0.27\nСтоп: 0.32']},
            {'id': 2, 'type': 'message', 'date': '2026-01-02T11:00:00', 'date_unixtime': '1767351600', 'text': 'ENA цель 1 достигнута +40%', 'reply_to_message_id': 1},
            {'id': 3, 'type': 'service', 'date': '2026-01-02T12:00:00', 'text': ''},
            {'id': 4, 'type': 'message', 'date': '2026-01-02T12:00:00', 'date_unixtime': '1767355200', 'text': 'шорт без стопа ENA вход 1'}]}
        res = AD.import_export(self.con, data)
        self.assertEqual((res['signal'], res['report'], res['unparsed'], res['skipped']), (1, 1, 1, 1))
        row = self.con.execute('SELECT source, pub_ts, symbol FROM audit_signals').fetchone()
        self.assertEqual(row, ('import', 1767348000, 'ENA'))
        self.assertEqual(len(AD.unparsed(self.con)), 1)
        self.assertEqual(AD.import_export(self.con, data)['dup'], 3)                        # повторный импорт ничего не дублирует


class FakeEntity:
    id = 777


class FakeMsg:
    def __init__(self, mid, text, ts=1_760_000_000):
        import datetime
        self.id, self.raw_text, self.reply_to_msg_id = mid, text, None
        self.date = datetime.datetime.fromtimestamp(ts, datetime.timezone.utc)


class SessionPasswordNeededError(Exception):
    pass


class FakeClient:
    """Фейк Telethon-клиента: ничего не отправляет; записывает все вызовы."""

    def __init__(self, session_path, phone, api_hash, password_needed=False, history=()):
        self.session_path, self.phone, self.api_hash = session_path, phone, api_hash
        self.calls, self.connected, self.authorized, self.handlers = [], False, False, []
        self.password_needed, self.history = password_needed, list(history)

    def is_connected(self):
        return self.connected

    async def connect(self):
        self.connected = True
        with open(self.session_path + '.session', 'a'):
            pass
        os.chmod(self.session_path + '.session', 0o644)

    async def disconnect(self):
        self.connected = False

    async def is_user_authorized(self):
        return self.authorized

    async def send_code_request(self, phone):
        self.calls.append('send_code_request')
        if phone == 'bad':
            raise RuntimeError(f'The phone number {phone} is invalid {self.api_hash}')
        return type('S', (), {'phone_code_hash': 'HASH_SECRET_123'})()

    async def sign_in(self, phone=None, code=None, phone_code_hash=None, password=None):
        self.calls.append('sign_in')
        if password is None and self.password_needed:
            raise SessionPasswordNeededError('2FA')
        if password is None and code != '12345':
            raise RuntimeError(f'bad code {code} for {phone}')
        self.authorized = True

    async def get_entity(self, key):
        self.calls.append('get_entity')
        return FakeEntity()

    def add_event_handler(self, cb, ev):
        self.handlers.append(cb)

    async def catch_up(self):
        self.calls.append('catch_up')

    async def iter_messages(self, ent, limit=None):
        for m in self.history:
            yield m


class LogCapture(logging.Handler):
    def __init__(self):
        super().__init__(logging.DEBUG)
        self.lines = []

    def emit(self, rec):
        self.lines.append(rec.getMessage())


class Telegram(unittest.TestCase):
    PHONE, HASH, PWD = '+380501234567', 'a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6', 's3cr3t-pass'

    def make(self, **kw):
        d = tempfile.mkdtemp()
        self.session = os.path.join(d, 'tg')
        self.db = os.path.join(d, 'audit.db')
        self.client = FakeClient(self.session, self.PHONE, self.HASH, **kw)
        self.a = AT.AuditTG('12345', self.HASH, ['@chan'], self.session, self.db, self.PHONE, self.PWD, client_factory=lambda: self.client)
        return self.a

    def test_login_flow_secrets_not_in_logs_or_chat_and_session_600(self):
        a = self.make(password_needed=True)
        sess1 = self.session
        cap = LogCapture()
        root = logging.getLogger()
        root.addHandler(cap)
        old = root.level
        root.setLevel(logging.DEBUG)
        try:
            out = [run(a.request_code()), run(a.sign_in('1-2-3-4-5'))]
            out.append(run(a.sign_in('1 2 3 4 5')))                       # повтор после успешного входа: код уже использован
            bad = self.make()
            bad.phone = 'bad'
            out.append(run(bad.request_code()))                           # ошибка содержит телефон и api_hash — они не должны попасть в текст
            out.append(run(bad.sign_in('12345')))
        finally:
            root.removeHandler(cap)
            root.setLevel(old)
        blob = '\n'.join(out) + '\n'.join(cap.lines)
        for secret in (self.PHONE, self.HASH, self.PWD, 'HASH_SECRET_123', '12345'):
            self.assertNotIn(secret, blob)
        self.assertTrue(out[1].startswith('✅'))
        self.assertEqual(os.stat(sess1 + '.session').st_mode & 0o777, 0o600)          # права сессии 600

    def test_password_from_env_only(self):
        a = self.make(password_needed=True)
        a.password = None
        run(a.request_code())
        self.assertIn('TG_2FA_PASSWORD', run(a.sign_in('12345')))

    def test_code_parsing(self):
        self.assertEqual(AT.code_from_text('1-2-3-4-5'), '12345')
        self.assertEqual(AT.code_from_text('12 345'), '12345')
        self.assertIsNone(AT.code_from_text('12'))

    def test_listener_is_read_only_and_records_live_edit_delete_catchup(self):
        a = self.make(history=[FakeMsg(5, ENA), FakeMsg(4, 'привет')])
        self.client.authorized = True
        msg = run(a.start())
        self.assertIn('только чтение', msg)
        con = AD.connect(self.db)
        cn = AD.counts(con)
        self.assertEqual(cn['signals'], {'catchup': 1})                                   # догонка — не форвард
        ev = type('E', (), {'message': FakeMsg(10, WLD), 'chat_id': -100777})()
        run(a._on_new(ev))
        self.assertEqual(AD.counts(con)['signals'].get('live'), 1)
        ev2 = type('E', (), {'message': FakeMsg(10, 'правка'), 'chat_id': -100777})()
        run(a._on_edit(ev2))
        ev3 = type('E', (), {'deleted_ids': [10], 'chat_id': -100777})()
        run(a._on_deleted(ev3))
        evs = AD.counts(con)['events']
        self.assertEqual((evs.get('edit'), evs.get('delete')), (1, 1))
        row = con.execute("SELECT text FROM audit_events WHERE msg_id=10 AND ev='new'").fetchone()
        self.assertEqual(row[0], WLD)                                                      # исходный текст сохранён
        self.assertTrue(set(self.client.calls) <= {'get_entity', 'catch_up', 'send_code_request', 'sign_in'})

    def test_source_has_no_write_calls(self):
        bad = re.compile(r'\.(send_message|send_file|forward_messages|send_read_acknowledge|send_reaction|delete_messages|edit_message|pin_message|kick_participant|action)\(')
        for f in ('audit_tg.py', 'audit_bot.py', 'audit_db.py', 'audit_parse.py', 'audit_sim.py'):
            src = open(os.path.join(ROOT, f), encoding='utf-8').read()
            self.assertIsNone(bad.search(src), f)

    def test_session_not_in_git_and_cmd_not_logged(self):
        gi = open(os.path.join(ROOT, '.gitignore'), encoding='utf-8').read()
        for pat in ('*.session', '*.session-journal'):
            self.assertIn(pat, gi)
        r = subprocess.run(['git', 'check-ignore', '-q', 'tg.session'], cwd=ROOT)
        self.assertEqual(r.returncode, 0)
        self.assertEqual(AB.redact_cmd('/tg_login 1-2-3-4-5'), '/tg_login ***')
        self.assertEqual(AB.redact_cmd('/status'), '/status')
        src = open(os.path.join(ROOT, 'async_bot.py'), encoding='utf-8').read()
        self.assertIn('audit_bot.redact_cmd(cmd)', src)
        tracked = subprocess.run(['git', 'ls-files'], cwd=ROOT, capture_output=True, text=True).stdout
        self.assertNotRegex(tracked, r'\.session')
        self.assertIn('telethon', open(os.path.join(ROOT, 'requirements.txt'), encoding='utf-8').read())

    def test_config_does_not_expose_secrets_in_status(self):
        os.environ.update({'TG_API_ID': '1', 'TG_API_HASH': self.HASH, 'AUDIT_CHANNELS': '@a, @b', 'TG_PHONE': self.PHONE})
        old = AB.STATE['tg']
        AB.STATE['tg'] = None
        try:
            txt = AB.status_text()
        finally:
            AB.STATE['tg'] = old
            for k in ('TG_API_ID', 'TG_API_HASH', 'AUDIT_CHANNELS', 'TG_PHONE'):
                os.environ.pop(k, None)
        self.assertNotIn(self.HASH, txt)
        self.assertNotIn(self.PHONE, txt)
        self.assertIn('каналов в AUDIT_CHANNELS: 2', txt)


class FakeSource:
    def __init__(self, c):
        self.c = c

    async def candles(self, base, start_ms, end_ms, ref, now_ms):
        return self.c, 'BingX ' + base

    async def funding(self, base, s, e):
        return None

    async def btc_daily(self, since, now):
        return None


class EndToEnd(unittest.TestCase):
    def test_audit_report_sections_and_early_verdict(self):
        path = os.path.join(tempfile.mkdtemp(), 'audit.db')
        con = AD.connect(path)
        t0 = 1_760_000_000
        AD.ingest(con, 'ch', 1, t0, ENA.replace('Вход: 0.2850 - 0.2900', 'Вход: 0.2850 - 0.2900'), 'live')
        AD.ingest(con, 'ch', 2, t0 + 100, 'ENA цель 1 достигнута +40%', 'live')
        AD.ingest(con, 'ch', 3, t0 + 200, 'SHORT ENA вход 1', 'live')
        AD.ingest(con, 'old', 9, t0 - 10 ** 6, WLD, 'import')
        n = 3 * 24 * 60
        px = [(0.284, 0.2851, 0.2839, 0.2845)] + [(0.2845, 0.2846, 0.2744, 0.2745)] + [(0.2745, 0.2746, 0.2640, 0.2641)] + [(0.2641, 0.2641, 0.2640, 0.2640)] * n
        c = candles(px)
        c['ts'] = (t0 // 60 + 1) * MIN + MIN * np.arange(len(px), dtype=np.int64)
        txt = run(AS.run_audit({'adb': path, 'fee_taker': 0.0005, 'fee_maker': 0.0002, 'now_ms': (t0 + 30 * 86400) * 1000, 'version': 'vT'}, source=FakeSource(c)))
        for needle in ('ФОРВАРД (live)', 'РЕТРОСПЕКТИВА', 'модель A', 'модель B', 'Критерии §2.44.2', 'Вердикт: рано', 'нераспознано 1', 'Сверка'):
            self.assertIn(needle, txt)

    def test_wiring(self):
        import research_runner as RR
        self.assertIn('audit', RR.TASKS)
        self.assertIn('audit_import', RR.TASKS)
        src = open(os.path.join(ROOT, 'async_bot.py'), encoding='utf-8').read()
        for c in ("cmd == '/tg_login'", "cmd == '/audit'", "cmd == '/audit_status'", "cmd == '/audit_unparsed'", "'/audit_import'", 'audit_bot.start(tg)', "msg.get('caption')"):
            self.assertIn(c, src)

    def test_import_task_ingests_and_removes_file(self):
        import research_runner as RR
        d = tempfile.mkdtemp()
        f = os.path.join(d, 'result.json')
        json.dump({'name': 'K', 'messages': [{'id': 1, 'type': 'message', 'date': '2026-01-01T00:00:00', 'date_unixtime': '1767225600', 'text': ENA}]}, open(f, 'w'))
        db = os.path.join(d, 'a.db')

        class Ctx:
            params = {'path': f, 'adb': db, 'label': None}
        res = run(RR._task_audit_import(Ctx()))
        self.assertIn('сигналов 1', res['text'])
        self.assertFalse(os.path.exists(f))
        self.assertEqual(AD.counts(AD.connect(db))['signals'], {'import': 1})


if __name__ == '__main__':
    unittest.main()
