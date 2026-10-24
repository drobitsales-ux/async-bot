"""Общая подготовка: импорт async_bot из корня репозитория, временная БД, заглушки."""
import os
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import async_bot as a  # noqa: E402


def fresh_state():
    """Чистая временная БД и обнулённые счётчики здоровья. Возвращает (путь, список отправленных tg)."""
    d = tempfile.mkdtemp(prefix='ab_test_')
    a.TRADES_DB = os.path.join(d, 'trades.db')
    a.DIGEST_DIR = os.path.join(d, 'logs')
    a._init_trades_db()
    a._health_scans.clear(); a._health_errs.clear(); a._health_err_sample.clear(); a._health_last_alert.clear()
    a._dd.update({'start_equity': 0.0, 'start_date': '', 'equity': 0.0, 'ok_ts': 0.0, 'fails': 0,
                  'min_dd': 0.0, 'fail_since': 0.0})
    sent = []

    async def fake_tg(text):
        sent.append(text)
    a.tg = fake_tg
    return a.TRADES_DB, sent
