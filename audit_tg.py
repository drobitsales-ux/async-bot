"""audit_tg.py — чтение канала(ов) через аккаунт владельца (Telethon), BOT_SPEC §2.44.1 п. 3. ТОЛЬКО ЧТЕНИЕ: никаких ответов, пересылок, реакций, подтверждений прочтения.
Секреты (api_hash, телефон, код, пароль 2FA, файл сессии) не пишутся в логи и не отправляются в чат; ошибки показываются без значений секретов (safe_err).
Файл сессии — SESSION.session (права 600), вне git (.gitignore). Клиент создаётся фабрикой — в тестах подменяется фейком (сети нет)."""
import asyncio
import logging
import os
import re
import time

import audit_db as AD

log = logging.getLogger('audit')
CATCHUP_LIMIT = 200
CODE_REUSE_S = 120                         # повторный /tg_login без аргументов в течение 2 минут код НЕ пересылает (новый код обнуляет прежний)


def session_file(session_path):
    return session_path + '.session'


def chmod_session(session_path):
    """Права 600 на файл сессии (и журнал SQLite, если есть)."""
    for suffix in ('.session', '.session-journal'):
        p = session_path + suffix
        if os.path.exists(p):
            try:
                os.chmod(p, 0o600)
            except OSError:
                pass


def code_from_text(s):
    """Код подтверждения из текста команды: допускаются разделители («1-2-3-4-5», «1 2 3 4 5»); берутся только цифры, ожидается 4–8."""
    d = re.sub(r'\D', '', s or '')
    return d if 4 <= len(d) <= 8 else None


class AuditTG:
    def __init__(self, api_id, api_hash, channels, session_path, db_path=None, phone=None, password=None, client_factory=None):
        self.api_id, self.api_hash, self.phone, self.password = api_id, api_hash, phone, password
        self.channels = list(channels)
        self.session_path = session_path
        self.db_path = db_path
        self.client_factory = client_factory or self._default_factory
        self.client = None
        self._hash = None
        self._code_ts = 0.0
        self.labels = {}
        self.started = False
        self.stopped = False
        self._stop = False
        self.backfilling = False
        self.con = None
        self.stats = {'live': 0, 'edit': 0, 'delete': 0, 'catchup': 0}

    # ── служебное ─────────────────────────────────────────────────────────────────────────────
    def _secrets(self):
        return [x for x in (self.api_hash, self.phone, self.password, self._hash, os.getenv('TG_SESSION_STRING') or None, str(self.api_id) if self.api_id else None) if x]

    def safe_err(self, e):
        """Текст ошибки без значений секретов (и без длинных цифровых последовательностей — телефон/код)."""
        s = f'{type(e).__name__}: {e}'
        for x in self._secrets():
            s = s.replace(str(x), '***')
        s = re.sub(r'\+?\d[\d\-\s]{6,}\d', '***', s)
        return s[:200]

    def _default_factory(self):
        from telethon import TelegramClient
        logging.getLogger('telethon').setLevel(logging.WARNING)
        sess = os.getenv('TG_SESSION_STRING', '').strip()
        if sess:                                                      # сессия, созданная на компьютере владельца (tg_make_session.py): код входа в чат не передаётся
            from telethon.sessions import StringSession
            return TelegramClient(StringSession(sess), int(self.api_id), self.api_hash)
        return TelegramClient(self.session_path, int(self.api_id), self.api_hash)

    def _db(self):
        if self.con is None:
            self.con = AD.connect(self.db_path)
        return self.con

    async def _client(self):
        if self.client is None:
            self.client = self.client_factory()
        if not self.client.is_connected():
            await self.client.connect()
        return self.client

    # ── вход ──────────────────────────────────────────────────────────────────────────────────
    async def authorized(self):
        try:
            c = await self._client()
            return bool(await c.is_user_authorized())
        except Exception as e:
            log.warning('[AUDIT] проверка авторизации: %s', self.safe_err(e))
            return False

    async def request_code(self, force=False):
        """Запрос кода подтверждения. Код приходит В ПРИЛОЖЕНИЕ Telegram владельца (служебный чат «Telegram», аккаунт 777000), а при неактивном приложении — по SMS. Каждый запрос
        делает прежний код недействительным, поэтому повторный запрос в течение CODE_REUSE_S без force не отправляется. → текст для чата (без секретов)."""
        if not self.phone:
            return '❌ TG_PHONE не задан в ENV (телефон аккаунта владельца в международном формате)'
        if self._hash and not force and time.time() - self._code_ts < CODE_REUSE_S:
            return (f'ℹ️ Код уже запрошен {int(time.time() - self._code_ts)} с назад — берите ПОСЛЕДНЕЕ сообщение от «Telegram» в приложении. Новый код (старый станет недействительным): '
                    '<code>/tg_login new</code>')
        try:
            c = await self._client()
            if await c.is_user_authorized():
                return '✅ Сессия уже авторизована'
            sent = await c.send_code_request(self.phone)
            self._hash = getattr(sent, 'phone_code_hash', None)
            self._code_ts = time.time()
        except Exception as e:
            return '❌ Не удалось запросить код: ' + self.safe_err(e)
        return ('📨 Код отправлен в приложение Telegram: чат «Telegram» (служебные уведомления) — не в этот чат; без приложения — SMS. Пришлите <code>/tg_login КОД</code>, цифры через дефис '
                '(например 1-2-3-4-5), в течение пары минут и НЕ запрашивайте код повторно — каждый новый обнуляет прежний. Если Telegram считает код «опубликованным в чате», он блокирует его '
                '(по моим сведениям, не проверено) — тогда используйте сессию, созданную на компьютере: tg_make_session.py → ENV TG_SESSION_STRING. Облачный пароль — в ENV TG_2FA_PASSWORD.')

    async def sign_in(self, code_text):
        code = code_from_text(code_text)
        if not code:
            return '❌ Код не распознан (нужны 4–8 цифр)'
        if not self._hash:
            return '❌ Сначала /tg_login без аргументов (запрос кода)'
        try:
            c = await self._client()
            try:
                await c.sign_in(phone=self.phone, code=code, phone_code_hash=self._hash)
            except Exception as e:
                if type(e).__name__ == 'SessionPasswordNeededError':
                    if not self.password:
                        return '❌ Нужен облачный пароль: задайте TG_2FA_PASSWORD в ENV и повторите /tg_login'
                    await c.sign_in(password=self.password)
                else:
                    raise
        except Exception as e:
            name = type(e).__name__
            hint = ''
            if name == 'PhoneCodeInvalidError':
                hint = ' — код неверен: возьмите ПОСЛЕДНИЙ код из чата «Telegram» (старые недействительны; каждый /tg_login new отправляет новый) либо Telegram заблокировал код, написанный в чате: тогда сессия с компьютера (TG_SESSION_STRING)'
            elif name == 'PhoneCodeExpiredError':
                hint = ' — код истёк: /tg_login new'
            return '❌ Вход не выполнен: ' + self.safe_err(e) + hint
        self._hash = None
        chmod_session(self.session_path)
        return '✅ Вход выполнен, сессия сохранена (права 600). Запускаю чтение каналов.'

    # ── чтение ────────────────────────────────────────────────────────────────────────────────
    async def start(self):
        """Подключение, регистрация обработчиков (только чтение), догонка последних сообщений. → текст статуса (без секретов)."""
        if not self.channels:
            return '⚠️ AUDIT_CHANNELS не задан — чтение каналов не запущено'
        if AD.meta_get(self._db(), 'stopped') == '1':
            return '⏹ Чтение каналов остановлено командой /audit_stop (флаг в БД) — после перезапуска оно само не стартует; возобновить: /audit_start'
        self.stopped, self._stop = False, False
        if not await self.authorized():
            return '⚠️ Сессия Telegram не авторизована — выполните /tg_login'
        try:
            from telethon import events
        except Exception:
            events = None
        c = await self._client()
        chmod_session(self.session_path)
        ents = []
        for ch in self.channels:
            try:
                key = int(ch) if re.fullmatch(r'-?\d+', ch.strip()) else ch.strip()
                ent = await c.get_entity(key)
                ents.append(ent)
                self.labels[getattr(ent, 'id', ch)] = ch
            except Exception as e:
                log.warning('[AUDIT] канал недоступен: %s', self.safe_err(e))
        if not ents:
            return '❌ Ни один канал из AUDIT_CHANNELS не открылся'
        if events is not None:
            c.add_event_handler(self._on_new, events.NewMessage(chats=ents))
            c.add_event_handler(self._on_edit, events.MessageEdited(chats=ents))
            c.add_event_handler(self._on_deleted, events.MessageDeleted(chats=ents))
        await self.catchup(ents)
        self.started = True
        if AD.meta_get(self._db(), 'listening_since') is None:
            AD.meta_set(self._db(), 'listening_since', int(time.time()))
        return f'✅ Чтение каналов запущено: {len(ents)} (только чтение); догнано сообщений: {self.stats["catchup"]}'

    def _label(self, chat_id):
        return str(self.labels.get(abs_id(chat_id), chat_id))

    async def catchup(self, ents):
        con = self._db()
        c = await self._client()
        try:
            await c.catch_up()
        except Exception:
            pass
        for ent in ents:
            label = str(self.labels.get(getattr(ent, 'id', None), getattr(ent, 'id', '?')))
            try:
                async for m in c.iter_messages(ent, limit=CATCHUP_LIMIT):
                    if AD.known(con, label, m.id):
                        continue
                    AD.ingest(con, label, m.id, int(m.date.timestamp()), m.raw_text or '', 'catchup', reply_to=getattr(m, 'reply_to_msg_id', None))
                    self.stats['catchup'] += 1
            except Exception as e:
                log.warning('[AUDIT] догонка: %s', self.safe_err(e))

    async def _on_new(self, ev):
        try:
            m = ev.message
            label = self._label(ev.chat_id)
            AD.ingest(self._db(), label, m.id, int(m.date.timestamp()), m.raw_text or '', 'live', reply_to=getattr(m, 'reply_to_msg_id', None))
            self.stats['live'] += 1
        except Exception as e:
            log.warning('[AUDIT] обработка нового сообщения: %s', self.safe_err(e))

    async def _on_edit(self, ev):
        try:
            m = ev.message
            AD.record_event(self._db(), self._label(ev.chat_id), m.id, 'edit', m.raw_text or '', 'live', int(m.date.timestamp()))
            self.stats['edit'] += 1
        except Exception as e:
            log.warning('[AUDIT] обработка правки: %s', self.safe_err(e))

    async def _on_deleted(self, ev):
        try:
            con = self._db()
            for mid in getattr(ev, 'deleted_ids', []) or []:
                chans = [self._label(ev.chat_id)] if getattr(ev, 'chat_id', None) else AD.channels_with_msg(con, mid)
                for ch in chans:
                    AD.record_event(con, ch, mid, 'delete', None, 'live')
                    self.stats['delete'] += 1
        except Exception as e:
            log.warning('[AUDIT] обработка удаления: %s', self.safe_err(e))

    async def stop(self, persist=True):
        """/audit_stop: остановить backfill и чтение, отключить клиент (БЕЗ выхода из аккаунта); флаг «остановлено» — в БД (после перезапуска бота чтение не стартует)."""
        self._stop = True
        self.stopped = True
        self.started = False
        if self.client is not None:
            try:
                await self.client.disconnect()
            except Exception:
                pass
        if persist:
            con = self._db()
            AD.meta_set(con, 'stopped', 1)
            AD.meta_set(con, 'listening_since', None)

    # ── полная история (/audit_backfill) ──────────────────────────────────────────────────────
    async def _entities(self):
        c = await self._client()
        ents = []
        for ch in self.channels:
            try:
                key = int(ch) if re.fullmatch(r'-?\d+', ch.strip()) else ch.strip()
                ent = await c.get_entity(key)
                self.labels.setdefault(getattr(ent, 'id', ch), ch)
                ents.append(ent)
            except Exception as e:
                log.warning('[AUDIT] канал недоступен: %s', self.safe_err(e))
        return ents

    async def backfill(self, max_per_channel, say, sleep=asyncio.sleep, batch=500):
        """Вся доступная история каналов (от СТАРЫХ к новым, до max_per_channel сообщений на канал), ТОЛЬКО чтение. FloodWait — ждём указанное время и продолжаем с последнего id;
        прогресс (last_id, n, done) хранится в audit_progress → повторный запуск продолжает; уже сохранённые сообщения не дублируются (канал + id); источник 'catchup' (не форвард)."""
        if self.backfilling:
            return '⏳ backfill уже выполняется'
        if AD.meta_get(self._db(), 'stopped') == '1' or self.stopped:
            return '⏹ Чтение остановлено (/audit_stop) — сначала /audit_start'
        if not await self.authorized():
            return '⚠️ Сессия Telegram не авторизована'
        self.backfilling, self._stop = True, False
        con = self._db()
        c = await self._client()
        lines = []
        try:
            for ent in await self._entities():
                label = str(self.labels.get(getattr(ent, 'id', None), getattr(ent, 'id', '?')))
                last, n, done = AD.progress_get(con, label)
                new = dup = 0
                exhausted = False
                while n < max_per_channel and not self._stop:
                    try:
                        async for m in c.iter_messages(ent, limit=max_per_channel - n, reverse=True, offset_id=last):
                            if self._stop:
                                break
                            n += 1
                            last = m.id
                            if (m.raw_text or '').strip():
                                k = AD.ingest(con, label, m.id, int(m.date.timestamp()), m.raw_text, 'catchup', reply_to=getattr(m, 'reply_to_msg_id', None))
                                if k == 'dup':
                                    dup += 1
                                else:
                                    new += 1
                            if n % 100 == 0:
                                AD.progress_set(con, label, last, n, 0)
                            if n % batch == 0:
                                await say(f'📥 backfill {label}: обработано {n}, новых {new}, уже были {dup} (id до {last})')
                        else:
                            exhausted = n < max_per_channel            # итератор закончился: история прочитана до конца, если не упёрлись в лимит
                        break
                    except Exception as e:
                        if type(e).__name__ == 'FloodWaitError':
                            sec = int(getattr(e, 'seconds', 60) or 60)
                            AD.progress_set(con, label, last, n, 0)
                            await say(f'⏳ backfill {label}: Telegram просит подождать {sec} с (FloodWait) — жду и продолжаю с id {last}')
                            await sleep(sec + 1)
                            continue
                        log.warning('[AUDIT] backfill: %s', self.safe_err(e))
                        await say('❌ backfill ' + label + ': ' + self.safe_err(e))
                        break
                AD.progress_set(con, label, last, n, 1 if exhausted else 0)
                st = 'история прочитана до конца' if exhausted else ('остановлено' if self._stop else f'достигнут лимит {max_per_channel} (поднимите AUDIT_BACKFILL_MAX)')
                lines.append(f'{label}: обработано {n} (новых {new}, уже были {dup}), последний id {last} — {st}')
        finally:
            self.backfilling = False
        return '✅ backfill завершён (источник catchup — ретроспектива, в форвард не входит)\n' + '\n'.join(lines)


def abs_id(chat_id):
    """Telethon отдаёт id канала как -100XXXXXXXXXX, а entity.id — XXXXXXXXXX."""
    try:
        s = str(int(chat_id))
    except (TypeError, ValueError):
        return chat_id
    if s.startswith('-100'):
        return int(s[4:])
    return abs(int(s))
