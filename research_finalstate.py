"""research_finalstate.py — состояния флага ФИНАЛА (BOT_SPEC §2.43.17): free → running → done | tech_fail_no_output; из tech_fail_no_output — РОВНО ОДНА повторная попытка по подтверждению владельца;
после неё — только done или tech_fail_final. Состояние — ключ `finalstate:<имя>` в kv factory.db; все переходы атомарны (транзакция SQLite). Для финалов, у которых уже есть старые ключи
(`<имя>_started` / `<имя>_done`), состояние выводится из них (legacy). Завершённые финалы (/final_crypto4h, /factory_final) этим модулем не затрагиваются."""
import json
import os
import time

import research_factory as RF

FREE, RUNNING, DONE, TECH_FAIL_NO_OUTPUT, TECH_FAIL_FINAL = 'free', 'running', 'done', 'tech_fail_no_output', 'tech_fail_final'
STATES = (FREE, RUNNING, DONE, TECH_FAIL_NO_OUTPUT, TECH_FAIL_FINAL)
MAX_ATTEMPTS = 2
STALE_S = 30 * 60


def key(name):
    return f'finalstate:{name}'


def _kv(con, k):
    r = con.execute('SELECT v FROM kv WHERE k=?', (k,)).fetchone()
    return json.loads(r[0]) if r else None


def _read(con, name, legacy):
    cur = _kv(con, key(name))
    if cur is not None:
        return cur
    if legacy:
        if _kv(con, legacy.get('done', '')) is not None:
            return {'state': DONE, 'attempt': 1, 'legacy': True, 'history': []}
        st = _kv(con, legacy.get('started', ''))
        if st is not None:
            return {'state': RUNNING, 'attempt': 1, 'legacy': True, 't': (st or {}).get('t') if isinstance(st, dict) else None, 'pid': None, 'history': []}
    return {'state': FREE, 'attempt': 0, 'legacy': False, 'history': []}


def get(name, fdir, legacy=None):
    con = RF._con(fdir)
    try:
        return _read(con, name, legacy)
    finally:
        con.close()


def transition(name, fdir, frm, to, legacy=None, attempt=None, **fields):
    """Атомарно: если текущее состояние ∈ frm — записать `to` (и attempt/поля), добавить запись в историю. → новое состояние (dict) | None (переход не разрешён)."""
    con = RF._con(fdir)
    con.isolation_level = None
    try:
        con.execute('BEGIN IMMEDIATE')
        cur = _read(con, name, legacy)
        if cur['state'] not in frm:
            con.execute('ROLLBACK')
            return None
        new = dict(cur, state=to, legacy=False)
        if attempt is not None:
            new['attempt'] = attempt
        new.update(fields)
        new['history'] = list(cur.get('history') or []) + [{'t': time.time(), 'from': cur['state'], 'to': to, 'attempt': new.get('attempt'), **{k: v for k, v in fields.items() if k in ('note', 'error')}}]
        con.execute('INSERT OR REPLACE INTO kv (k, v) VALUES (?, ?)', (key(name), json.dumps(new, ensure_ascii=False)))
        con.execute('COMMIT')
        return new
    except Exception:
        try:
            con.execute('ROLLBACK')
        except Exception:
            pass
        raise
    finally:
        con.close()


def begin(name, fdir, attempt, legacy=None, **fields):
    """free → running (попытка 1) либо tech_fail_no_output → running (попытка 2, только если использована ровно одна). → состояние | None."""
    if attempt == 1:
        return transition(name, fdir, (FREE,), RUNNING, legacy, attempt=1, t=time.time(), pid=os.getpid(), **fields)
    con = RF._con(fdir)
    try:
        cur = _read(con, name, legacy)
    finally:
        con.close()
    if attempt != 2 or cur.get('attempt') != 1:
        return None
    return transition(name, fdir, (TECH_FAIL_NO_OUTPUT,), RUNNING, legacy, attempt=2, t=time.time(), pid=os.getpid(), **fields)


def fail_no_output(name, fdir, legacy=None, error=''):
    """running → tech_fail_no_output (попытка 1) | tech_fail_final (попытка 2): сбой до записи любого вывода по LOCKBOX."""
    cur = get(name, fdir, legacy)
    to = TECH_FAIL_NO_OUTPUT if (cur.get('attempt') or 1) < MAX_ATTEMPTS else TECH_FAIL_FINAL
    return transition(name, fdir, (RUNNING,), to, legacy, error=str(error)[:300])


def finish_done(name, fdir, legacy=None, note=''):
    return transition(name, fdir, (RUNNING,), DONE, legacy, note=note)


def alive(pid):
    if not pid:
        return False
    try:
        os.kill(int(pid), 0)
        return True
    except (OSError, ValueError):
        return False


def is_stale(st, now=None, min_age_s=STALE_S):
    """running считается зависшим: возраст ≥ min_age_s (нет времени старта — legacy без метки — тоже) и процесс (если известен pid) не жив."""
    if st.get('state') != RUNNING:
        return False
    now = now or time.time()
    t = st.get('t')
    old = (t is None) or (now - float(t) >= min_age_s)
    return bool(old and not alive(st.get('pid')))


def reconcile_stale(name, fdir, legacy=None, note=''):
    """Зависший running → tech_fail_no_output (попытка 1) | tech_fail_final (попытка 2). Вызывать только после проверки is_stale и чистого audit. → состояние | None."""
    cur = get(name, fdir, legacy)
    to = TECH_FAIL_NO_OUTPUT if (cur.get('attempt') or 1) < MAX_ATTEMPTS else TECH_FAIL_FINAL
    return transition(name, fdir, (RUNNING,), to, legacy, note=note)
