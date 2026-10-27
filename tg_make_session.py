"""tg_make_session.py — ЛОКАЛЬНЫЙ скрипт владельца (на боте не запускается): создаёт строку сессии Telethon БЕЗ кода подтверждения — по QR-коду, который сканируется из уже
авторизованного приложения Telegram (Настройки → Устройства → Подключить устройство). Подходит, когда код не приходит (SendCodeUnavailableError) или Telegram блокирует код, написанный в чате.
Запуск: Google Colab (браузер, ничего ставить не нужно) либо python на компьютере: pip install "telethon>=1.34,<2" qrcode → python tg_make_session.py.
Результат — строка; положите её в ENV Render как TG_SESSION_STRING. Строка = полный доступ к аккаунту: не публиковать, не коммитить, не пересылать в чат.
После получения строки клиент ТОЛЬКО отключается (disconnect), без выхода из аккаунта; закройте Colab — одну и ту же строку нельзя использовать в двух местах одновременно (Telegram аннулирует сессию).
Бот использует сессию только для чтения указанных каналов (BOT_SPEC §2.44.1)."""
import asyncio
import getpass


def show_qr(url):
    """QR в ячейке Colab/Jupyter (картинка) либо ASCII в терминале."""
    try:
        import qrcode
    except ImportError:
        print('Установите qrcode: pip install qrcode — или откройте ссылку на телефоне:', url)
        return
    try:
        from IPython.display import clear_output, display
        clear_output(wait=True)
        display(qrcode.make(url))
        print('Telegram на телефоне → Настройки → Устройства → Подключить устройство → наведите камеру на QR (код живёт ~30 с, потом обновится сам)')
    except ImportError:
        q = qrcode.QRCode(border=1)
        q.add_data(url)
        q.print_ascii(invert=True)


async def login_qr(client, show=show_qr, ask_password=lambda: getpass.getpass('Облачный пароль Telegram (ввод скрыт): '), max_rounds=10, session_password_error=None):
    """Вход по QR: показывает QR, ждёт подтверждения в приложении, при истечении токена обновляет QR; облачный пароль — только в терминале. → True при успехе."""
    qr = await client.qr_login()
    for _ in range(max_rounds):
        show(qr.url)
        try:
            await qr.wait()
            return True
        except asyncio.TimeoutError:
            await qr.recreate()
        except Exception as e:
            if session_password_error is not None and isinstance(e, session_password_error) or type(e).__name__ == 'SessionPasswordNeededError':
                await client.sign_in(password=ask_password())
                return True
            raise
    return False


async def main():
    from telethon import TelegramClient
    from telethon.sessions import StringSession
    api_id = int(input('api_id: ').strip())
    api_hash = getpass.getpass('api_hash (ввод скрыт): ').strip()
    client = TelegramClient(StringSession(), api_id, api_hash)
    await client.connect()
    try:
        if await client.is_user_authorized() or await login_qr(client):
            print('\nTG_SESSION_STRING (секрет, только в ENV Render):\n')
            print(client.session.save())
            print('\nГотово. Закройте этот блокнот/окно; из аккаунта НЕ выходите — иначе сессия аннулируется.')
        else:
            print('QR не был подтверждён за отведённое время — запустите заново.')
    finally:
        await client.disconnect()


if __name__ == '__main__':
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        asyncio.run(main())
    else:
        print('Цикл событий уже запущен (Colab/Jupyter): выполните в ячейке   await main()')
