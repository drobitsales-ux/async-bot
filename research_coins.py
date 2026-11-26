"""research_coins.py — набор спотовых монет с блокчейн-сетью по биржам для пункта г поправки §2.64.2 (v134). Отдельный лёгкий модуль БЕЗ numpy: сетевой запрос (aiohttp) в дочернем процессе с лимитом
адресного пространства резервирует десятки МБ на потоки/арены, поэтому выполняется отдельным заданием `h2_coins` ДО расчёта `/h2_ext` (замер v134: коннектор aiohttp +144 МБ при MALLOC_ARENA_MAX по умолчанию);
расчёт читает результат из файла `h2ext_coins.json` в каталоге данных. Только публичные GET без ключей."""
import json
import os
import time

COINS_FILE = 'h2ext_coins.json'
NET_EXCHANGES = ('gateio', 'bitget')          # публичные эндпоинты списка монет с сетями (по памяти документации, не проверены)
SIGNED_NOTE = 'список сетей требует подписанного запроса (ключ) — п. г не применяется'


def cache_path(xdir):
    return os.path.join(xdir, COINS_FILE)


async def fetch_coin_sets(venues, clients=None):
    """→ ({биржа: set | None}, {биржа: примечание}). Gate.io, Bitget — публичный запрос; остальные биржи — п. г не применяется (подпись ключом); сбой запроса — None и текст ошибки."""
    import research_venues
    coins, notes = {}, {}
    for ex in venues:
        if ex in NET_EXCHANGES:
            c = (clients or {}).get(ex) or research_venues.CLASSES[ex]()
            try:
                coins[ex] = await c.fetch_spot_coins()
                notes[ex] = f'применён ({len(coins[ex])} монет с блокчейн-сетью)'
            except Exception as e:
                coins[ex] = None
                notes[ex] = f'не применяется: запрос списка монет не удался — {type(e).__name__}: {str(e)[:100]}'
            finally:
                try:
                    await c.close()
                except Exception:
                    pass
        else:
            coins[ex] = None
            notes[ex] = f'не применяется: {SIGNED_NOTE}'
    return coins, notes


def write_cache(xdir, coins, notes, now=None):
    try:
        os.makedirs(xdir, exist_ok=True)
        with open(cache_path(xdir), 'w', encoding='utf-8') as f:
            json.dump({'t': now or time.time(), 'coins': {ex: (sorted(v) if v else None) for ex, v in coins.items()}, 'notes': notes}, f, ensure_ascii=False)
        return True
    except OSError:
        return False


def read_cache(xdir):
    """→ (coins, notes, t) | None."""
    try:
        with open(cache_path(xdir), encoding='utf-8') as f:
            z = json.load(f)
        return {ex: (set(v) if v else None) for ex, v in z['coins'].items()}, z.get('notes', {}), float(z.get('t') or 0)
    except (OSError, ValueError, KeyError):
        return None


async def refresh_job(params):
    """Задание `h2_coins`: запрос наборов монет и запись файла (`/h2_ext` без аргумента вызывает его ПЕРЕД расчётом). → короткий текст."""
    xdir = params.get('xdir')
    venues = tuple(params.get('venues') or ())
    coins, notes = await fetch_coin_sets(venues, params.get('clients'))
    ok = write_cache(xdir, coins, notes)
    return 'монеты с сетями (п. г §2.64.2): ' + '; '.join(f'{ex} — {notes[ex]}' for ex in venues) + ('' if ok else ' | ⚠️ запись файла не удалась')
