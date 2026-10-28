"""audit_db.py — хранилище аудита внешнего источника сигналов (BOT_SPEC §2.44): неизменяемые таблицы audit_events / audit_signals / audit_other (UPDATE и DELETE запрещены триггерами),
приём сообщений (ingest), события правок/удалений, импорт экспорта Telegram Desktop (result.json). Секретов (сессия, api_hash, телефон, код) здесь нет."""
import json
import os
import sqlite3
import time
from datetime import datetime, timezone

import audit_parse as AP

SOURCES = ('live', 'catchup', 'import')


def db_path():
    return os.getenv('AUDIT_DB') or ('/data/audit.db' if os.path.exists('/data') else '/tmp/audit.db')


SCHEMA = '''
CREATE TABLE IF NOT EXISTS audit_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT, channel TEXT NOT NULL, msg_id INTEGER NOT NULL, ev TEXT NOT NULL,
    msg_ts INTEGER, recv_ts REAL NOT NULL, source TEXT NOT NULL, text TEXT);
CREATE UNIQUE INDEX IF NOT EXISTS ux_audit_new ON audit_events(channel, msg_id) WHERE ev = 'new';
CREATE TABLE IF NOT EXISTS audit_signals (
    id INTEGER PRIMARY KEY AUTOINCREMENT, channel TEXT NOT NULL, msg_id INTEGER NOT NULL, source TEXT NOT NULL, pub_ts INTEGER NOT NULL, recv_ts REAL NOT NULL,
    symbol TEXT NOT NULL, side TEXT NOT NULL, leverage INTEGER, entry_lo REAL NOT NULL, entry_hi REAL NOT NULL, targets TEXT NOT NULL, stop REAL NOT NULL, text TEXT NOT NULL,
    UNIQUE(channel, msg_id));
CREATE TABLE IF NOT EXISTS audit_other (
    id INTEGER PRIMARY KEY AUTOINCREMENT, channel TEXT NOT NULL, msg_id INTEGER NOT NULL, source TEXT NOT NULL, kind TEXT NOT NULL, pub_ts INTEGER, recv_ts REAL NOT NULL,
    symbol TEXT, reply_to INTEGER, payload TEXT, text TEXT, UNIQUE(channel, msg_id));
'''
MUTABLE_SCHEMA = '''
CREATE TABLE IF NOT EXISTS audit_progress (channel TEXT PRIMARY KEY, last_id INTEGER NOT NULL, n INTEGER NOT NULL, done INTEGER NOT NULL, updated REAL NOT NULL);
CREATE TABLE IF NOT EXISTS audit_meta (k TEXT PRIMARY KEY, v TEXT);
'''
IMMUTABLE = ('audit_events', 'audit_signals', 'audit_other')


def connect(path=None):
    path = path or db_path()
    os.makedirs(os.path.dirname(path) or '.', exist_ok=True)
    con = sqlite3.connect(path)
    con.executescript(SCHEMA)
    con.executescript(MUTABLE_SCHEMA)                       # служебные (изменяемые) таблицы: прогресс backfill и флаги; данные аудита — только в неизменяемых
    for t in IMMUTABLE:
        for op in ('UPDATE', 'DELETE'):
            con.execute(f"CREATE TRIGGER IF NOT EXISTS {t}_no_{op.lower()} BEFORE {op} ON {t} BEGIN SELECT RAISE(ABORT, 'audit: таблица неизменяема'); END")
    con.commit()
    return con


def ingest(con, channel, msg_id, pub_ts, text, source, reply_to=None, recv_ts=None):
    """Новое сообщение: событие 'new' (исходный текст, один раз на канал+id) + разбор. → 'signal'|'report'|'unparsed'|'other'|'dup'."""
    if source not in SOURCES:
        raise ValueError(source)
    recv = time.time() if recv_ts is None else recv_ts
    channel = str(channel)
    cur = con.execute('INSERT OR IGNORE INTO audit_events (channel, msg_id, ev, msg_ts, recv_ts, source, text) VALUES (?,?,?,?,?,?,?)',
                      (channel, int(msg_id), 'new', int(pub_ts) if pub_ts is not None else None, recv, source, text))
    if cur.rowcount == 0:
        con.commit()
        return 'dup'
    p = AP.parse(text or '')
    kind = p['kind']
    if kind == 'signal':
        con.execute('INSERT OR IGNORE INTO audit_signals (channel, msg_id, source, pub_ts, recv_ts, symbol, side, leverage, entry_lo, entry_hi, targets, stop, text) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)',
                    (channel, int(msg_id), source, int(pub_ts), recv, p['symbol'], p['side'], p['leverage'], p['entry_lo'], p['entry_hi'], json.dumps(p['targets']), p['stop'], text))
    else:
        payload = {k: v for k, v in p.items() if k not in ('kind', 'symbol')}
        con.execute('INSERT OR IGNORE INTO audit_other (channel, msg_id, source, kind, pub_ts, recv_ts, symbol, reply_to, payload, text) VALUES (?,?,?,?,?,?,?,?,?,?)',
                    (channel, int(msg_id), source, kind, int(pub_ts) if pub_ts is not None else None, recv, p.get('symbol'), reply_to, json.dumps(payload, ensure_ascii=False), text))
    con.commit()
    return kind


def record_event(con, channel, msg_id, ev, text=None, source='live', ts=None):
    """Правка ('edit') или удаление ('delete'): событие добавляется, исходный текст не меняется."""
    if ev not in ('edit', 'delete'):
        raise ValueError(ev)
    con.execute('INSERT INTO audit_events (channel, msg_id, ev, msg_ts, recv_ts, source, text) VALUES (?,?,?,?,?,?,?)', (str(channel), int(msg_id), ev, ts, time.time(), source, text))
    con.commit()


def progress_get(con, channel):
    """Прогресс backfill по каналу: (last_id, n, done); нет записи → (0, 0, 0)."""
    r = con.execute('SELECT last_id, n, done FROM audit_progress WHERE channel=?', (str(channel),)).fetchone()
    return r if r else (0, 0, 0)


def progress_set(con, channel, last_id, n, done):
    con.execute('INSERT OR REPLACE INTO audit_progress (channel, last_id, n, done, updated) VALUES (?,?,?,?,?)', (str(channel), int(last_id), int(n), int(done), time.time()))
    con.commit()


def meta_get(con, k, default=None):
    r = con.execute('SELECT v FROM audit_meta WHERE k=?', (k,)).fetchone()
    return r[0] if r else default


def meta_set(con, k, v):
    if v is None:
        con.execute('DELETE FROM audit_meta WHERE k=?', (k,))
    else:
        con.execute('INSERT OR REPLACE INTO audit_meta (k, v) VALUES (?, ?)', (k, str(v)))
    con.commit()


def known(con, channel, msg_id):
    return con.execute("SELECT 1 FROM audit_events WHERE channel=? AND msg_id=? AND ev='new'", (str(channel), int(msg_id))).fetchone() is not None


def channels_with_msg(con, msg_id):
    return [r[0] for r in con.execute("SELECT channel FROM audit_events WHERE msg_id=? AND ev='new'", (int(msg_id),))]


def counts(con):
    out = {'signals': {}, 'other': {}, 'events': {}}
    for src, n in con.execute('SELECT source, COUNT(*) FROM audit_signals GROUP BY source'):
        out['signals'][src] = n
    for kind, src, n in con.execute('SELECT kind, source, COUNT(*) FROM audit_other GROUP BY kind, source'):
        out['other'].setdefault(kind, {})[src] = n
    for ev, n in con.execute('SELECT ev, COUNT(*) FROM audit_events GROUP BY ev'):
        out['events'][ev] = n
    return out


def unparsed(con, limit=200):
    return con.execute("SELECT channel, msg_id, source, pub_ts, payload, text FROM audit_other WHERE kind='unparsed' ORDER BY id DESC LIMIT ?", (limit,)).fetchall()


# ── импорт экспорта Telegram Desktop ────────────────────────────────────────────────────────────────
def flatten_text(t):
    """text в result.json — строка либо список (строки и сущности {'type', 'text'})."""
    if isinstance(t, str):
        return t
    if isinstance(t, list):
        return ''.join(x if isinstance(x, str) else str(x.get('text', '')) for x in t)
    return ''


def msg_unix(m):
    """date_unixtime (UTC) если есть, иначе date как UTC (в экспорте это локальное время владельца — часовой пояс неизвестен, поправка не вводится)."""
    if m.get('date_unixtime'):
        try:
            return int(m['date_unixtime'])
        except (TypeError, ValueError):
            pass
    try:
        return int(datetime.fromisoformat(m['date']).replace(tzinfo=timezone.utc).timestamp())
    except Exception:
        return None


def import_export(con, data, label=None):
    """result.json одного чата или всей выгрузки; source='import'. → dict(messages, signal, report, unparsed, other, dup, skipped)."""
    chats = []
    if isinstance(data, dict) and 'messages' in data:
        chats = [data]
    elif isinstance(data, dict) and isinstance(data.get('chats'), dict):
        chats = data['chats'].get('list', [])
    res = {'messages': 0, 'signal': 0, 'report': 0, 'unparsed': 0, 'other': 0, 'dup': 0, 'skipped': 0, 'chats': len(chats)}
    for ch in chats:
        name = label or str(ch.get('name') or ch.get('id') or 'import')
        for m in ch.get('messages', []):
            if m.get('type') != 'message':
                res['skipped'] += 1
                continue
            ts = msg_unix(m)
            if ts is None:
                res['skipped'] += 1
                continue
            res['messages'] += 1
            k = ingest(con, name, m.get('id'), ts, flatten_text(m.get('text')), 'import', reply_to=m.get('reply_to_message_id'))
            res[k] = res.get(k, 0) + 1
    return res
