"""audit_bot.py — склейка аудита внешнего источника сигналов (BOT_SPEC §2.44) с ботом: конфигурация из ENV, запуск чтения каналов (Telethon, только чтение), /tg_login, /audit_status,
/audit_unparsed, приём файла /audit_import. Секреты (TG_API_HASH, TG_PHONE, TG_2FA_PASSWORD, сессия, код) в логи и чат не попадают."""
import asyncio
import logging
import os
import re
import time

import audit_db as AD
import audit_tg as AT

IMPORT_MAX_BYTES = 20 * 1024 * 1024          # лимит скачивания файла ботом (Bot API)
IMPORT_WAIT_S = 900
STATE = {'tg': None, 'import_wait': 0.0, 'start_msg': '', 'bf_task': None}
BACKFILL_DEFAULT = 20000


def config():
    """ENV → dict. Значения секретов наружу не выводятся (status показывает только «задано/нет»)."""
    ch = [x.strip() for x in re.split(r'[,\n;]+', os.getenv('AUDIT_CHANNELS', '')) if x.strip()]
    return {'api_id': os.getenv('TG_API_ID', ''), 'api_hash': os.getenv('TG_API_HASH', ''), 'channels': ch, 'phone': os.getenv('TG_PHONE', ''), 'password': os.getenv('TG_2FA_PASSWORD', ''),
            'session': os.getenv('TG_SESSION_PATH') or ('/data/tg' if os.path.exists('/data') else '/tmp/tg'), 'db': AD.db_path()}


def configured():
    c = config()
    return bool(c['api_id'] and c['api_hash'] and c['channels'])


def get_client(factory=None):
    c = config()
    if STATE['tg'] is None:
        STATE['tg'] = AT.AuditTG(c['api_id'], c['api_hash'], c['channels'], c['session'], c['db'], c['phone'] or None, c['password'] or None, client_factory=factory)
    return STATE['tg']


async def start(tg):
    """Фоновый запуск чтения каналов (не падает: любая ошибка — строка в лог/чат без секретов)."""
    if not configured():
        return
    try:
        a = get_client()
        msg = await a.start()
        STATE['start_msg'] = msg
        logging.info('[AUDIT] %s', re.sub(r'<[^>]+>', '', msg))
        if msg.startswith(('⚠️', '❌')):
            await tg(msg)
    except Exception as e:
        logging.warning('[AUDIT] запуск: %s', type(e).__name__)
        await tg('❌ Аудит: запуск чтения каналов не удался: ' + type(e).__name__)


async def login(args, tg):
    """/tg_login — запрос кода; /tg_login КОД — вход. Код в логи не попадает; сообщение с кодом пытаемся удалить (если у бота есть право)."""
    if not configured():
        await tg('⛔ Аудит не настроен: нужны TG_API_ID, TG_API_HASH, AUDIT_CHANNELS (и TG_PHONE для входа) в ENV')
        return
    a = get_client()
    if not args or [x.lower() for x in args] == ['new']:
        await tg(await a.request_code(force=bool(args)))
        return
    text = await a.sign_in(' '.join(args))
    await tg(text)
    if text.startswith('✅'):
        asyncio.ensure_future(start(tg))


def status_text():
    c = config()
    a = STATE['tg']
    L = ['📋 <b>Аудит внешнего источника (BOT_SPEC §2.44)</b>',
         f'Настройка: TG_API_ID {"задан" if c["api_id"] else "НЕТ"}, TG_API_HASH {"задан" if c["api_hash"] else "НЕТ"}, TG_PHONE {"задан" if c["phone"] else "нет"}, каналов в AUDIT_CHANNELS: {len(c["channels"])}']
    sf = AT.session_file(c['session'])
    if os.getenv('TG_SESSION_STRING'):
        L.append('Сессия: строка из ENV TG_SESSION_STRING (значение не показывается)')
    elif os.path.exists(sf):
        mode = os.stat(sf).st_mode & 0o777
        L.append(f'Файл сессии: есть, права {mode:o}' + ('' if mode == 0o600 else ' ⚠️ должны быть 600'))
    else:
        L.append('Файл сессии: нет (нужен /tg_login)')
    if a is not None:
        L.append(f'Чтение каналов: {"работает" if a.started else "не запущено"}; получено live {a.stats["live"]}, правок {a.stats["edit"]}, удалений {a.stats["delete"]}, догонка {a.stats["catchup"]}')
    if STATE['start_msg']:
        L.append(re.sub(r'<[^>]+>', '', STATE['start_msg']))
    try:
        if os.path.exists(c['db']):
            _con = AD.connect(c['db'])
            since = AD.meta_get(_con, 'listening_since')
            if AD.meta_get(_con, 'stopped') == '1':
                L.append('Сессия: чтение ОСТАНОВЛЕНО (/audit_stop; возобновить — /audit_start)')
            elif since:
                from datetime import datetime, timezone
                L.append('Сессия активна с ' + datetime.fromtimestamp(int(since), timezone.utc).strftime('%Y-%m-%d %H:%M UTC'))
            for ch, last_id, n, done, _u in _con.execute('SELECT channel, last_id, n, done, updated FROM audit_progress'):
                L.append(f'backfill {ch}: обработано {n}, последний id {last_id}, {"прочитано до конца" if done else "не завершено"}')
    except Exception as e:
        L.append('Сессия/backfill: ошибка чтения ' + type(e).__name__)
    try:
        if os.path.exists(c['db']):
            cn = AD.counts(AD.connect(c['db']))
            L.append(f'БД: сигналов {cn["signals"]}; прочее {cn["other"]}; события {cn["events"]}')
        else:
            L.append('БД: пока пуста')
    except Exception as e:
        L.append('БД: ошибка чтения ' + type(e).__name__)
    return '\n'.join(L)


def unparsed_text(limit=40):
    c = config()
    if not os.path.exists(c['db']):
        return 'Нераспознанных нет (БД пуста)'
    rows = AD.unparsed(AD.connect(c['db']), limit)
    if not rows:
        return 'Нераспознанных сообщений нет'
    out = [f'Нераспознанных (последние {len(rows)}); исправление парсера — только до набора форварда (§2.44.4):']
    for ch, mid, src, ts, payload, text in rows:
        out.append(f'— {ch} #{mid} [{src}] {payload}\n   {(text or "").strip()[:300]}')
    return '\n'.join(out)


def redact_cmd(cmd):
    """Для логов: команда с кодом входа не логируется."""
    return '/tg_login ***' if cmd.startswith('/tg_login') else cmd


async def import_document(msg, token, http, run_job, tg, label=None):
    """Файл result.json документом → скачивание (≤ 20 МБ) → импорт в дочернем процессе. run_job(cmd, task, params) → текст отчёта или None."""
    doc = msg.get('document') or {}
    if int(doc.get('file_size') or 0) > IMPORT_MAX_BYTES:
        await tg('⛔ /audit_import: файл больше 20 МБ — Bot API не позволяет боту его скачать. Разбейте экспорт по датам (Telegram Desktop → экспорт чата с выбором периода).')
        return
    import aiohttp
    async with http.get(f'https://api.telegram.org/bot{token}/getFile', params={'file_id': doc.get('file_id')}, timeout=aiohttp.ClientTimeout(total=20)) as r:
        info = (await r.json()).get('result') if r.status == 200 else None
    if not info or not info.get('file_path'):
        await tg('❌ /audit_import: файл не получен от Telegram (возможно, больше 20 МБ)')
        return
    base = os.path.dirname(AD.db_path()) or '/tmp'
    path = os.path.join(base, f'audit_import_{int(time.time())}.json')
    async with http.get(f'https://api.telegram.org/file/bot{token}/{info["file_path"]}', timeout=aiohttp.ClientTimeout(total=120)) as r:
        if r.status != 200:
            await tg(f'❌ /audit_import: скачивание не удалось (HTTP {r.status})')
            return
        data = await r.read()
    with open(path, 'wb') as f:
        f.write(data)
    del data
    text = await run_job('/audit_import', 'audit_import', {'path': path, 'adb': AD.db_path(), 'label': label})
    if text:
        await tg(text)


def backfill_max():
    try:
        return max(1, int(os.getenv('AUDIT_BACKFILL_MAX', str(BACKFILL_DEFAULT))))
    except ValueError:
        return BACKFILL_DEFAULT


async def backfill(tg):
    """/audit_backfill: вся история каналов (только чтение), прогресс в Telegram, продолжение с места остановки."""
    if not configured():
        await tg('⛔ Аудит не настроен (TG_API_ID, TG_API_HASH, AUDIT_CHANNELS)')
        return
    a = get_client()
    await tg(f'📥 /audit_backfill: читаю историю каналов (до {backfill_max()} сообщений на канал, от старых к новым; только чтение; повторный запуск продолжит с места остановки)')
    try:
        await tg(await a.backfill(backfill_max(), tg))
    except Exception as e:
        logging.warning('[AUDIT] backfill: %s', type(e).__name__)
        await tg('❌ backfill: ' + a.safe_err(e))


async def stop(tg):
    """/audit_stop: остановить чтение и backfill, отключить клиент Telethon (без выхода из аккаунта), поставить флаг; напомнить про ENV и сессию."""
    a = get_client() if configured() else STATE['tg']
    if a is not None:
        await a.stop(persist=True)
    else:
        AD.meta_set(AD.connect(config()['db']), 'stopped', 1)
    await tg('⏹ Чтение каналов остановлено, клиент Telethon отключён (из аккаунта не выходил). После перезапуска бота чтение само не стартует (флаг в БД; возобновить — /audit_start).\n'
             'Гигиена сессии — сделайте сами: 1) удалите <code>TG_SESSION_STRING</code> из ENV Render (и при желании TG_API_HASH/TG_PHONE); 2) в Telegram: Настройки → Устройства → завершите сессию, '
             'созданную для бота. Накопленная БД аудита (/audit) остаётся.')


async def resume(tg):
    """/audit_start: снять флаг остановки и запустить чтение каналов."""
    if not configured():
        await tg('⛔ Аудит не настроен')
        return
    AD.meta_set(AD.connect(config()['db']), 'stopped', None)
    a = get_client()
    a.stopped = False
    await start(tg)
    await tg(re.sub(r'<[^>]+>', '', STATE['start_msg']) or 'запуск выполнен')
