"""tg_make_session.py — ЛОКАЛЬНЫЙ скрипт владельца (на ботe не запускается): создаёт строку сессии Telethon на вашем компьютере, чтобы код входа вводился в терминале, а не в чате Telegram.
Использование:  pip install "telethon>=1.34,<2"  →  python tg_make_session.py  →  ввести api_id, api_hash, телефон, код (и облачный пароль, если есть)
→ скрипт напечатает строку; положите её в ENV Render как TG_SESSION_STRING. Строка = полный доступ к аккаунту: не публиковать, не коммитить, не пересылать в чат.
Бот использует сессию только для чтения указанных каналов (BOT_SPEC §2.44.1)."""
import asyncio
import getpass

from telethon import TelegramClient
from telethon.sessions import StringSession


async def main():
    api_id = int(input('api_id: ').strip())
    api_hash = getpass.getpass('api_hash (ввод скрыт): ').strip()
    client = TelegramClient(StringSession(), api_id, api_hash)
    await client.start()                           # спросит телефон, код и пароль 2FA в терминале
    print('\nTG_SESSION_STRING (секрет, только в ENV Render):\n')
    print(client.session.save())
    await client.disconnect()


if __name__ == '__main__':
    asyncio.run(main())
