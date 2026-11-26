"""research_cprobe.py — `/contract_probe` (BOT_SPEC §2.64.1, ШАГ 0): ключи объекта контракта по РЕАЛЬНЫМ ответам API бирж (gateio, mexc, bitget, bingx, okx) на примерах — криптомонета, акция, индекс/сырьё; какие поля
отличают не-крипто; варианты списка контрактов; доступность эндпоинтов списка валют/сетей (п. г). Только публичные GET без ключей; ничего не записывает и не торгует. Формат — как получит биржа (ничего не угадывается)."""
import json
import time

ORDER = ('gateio', 'mexc', 'bitget', 'bingx', 'okx')
CRYPTO = ('BTC',)
STOCK_PICK = ('AAPLSTOCK', 'AAPL', 'AAPLX', 'TSLASTOCK', 'TSLA', 'NVDASTOCK', 'NVDA', 'NVIDIA')
INDEX_PICK = ('SP500', 'SPX500', 'GLD', 'XAU', 'USOIL', 'NAS100', 'NDX100')
MAX_VAL = 12
CUT = 1500


def _info(m):
    i = m.get('info') if isinstance(m, dict) else None
    return i if isinstance(i, dict) else {}


def pick_examples(markets):
    """→ {'крипто': (символ, market), 'акция': …, 'индекс/сырьё': …} (None, если не найден). Выбор по базе: первая из списка кандидатов; для акции — иначе любая база с суффиксом STOCK."""
    by_base = {}
    for sym, m in markets.items():
        by_base.setdefault(str(m.get('base') or '').upper(), (sym, m))
    out = {}
    out['крипто'] = next((by_base[b] for b in CRYPTO if b in by_base), None)
    st = next((by_base[b] for b in STOCK_PICK if b in by_base), None)
    if st is None:
        st = next((v for b, v in sorted(by_base.items()) if b.endswith('STOCK')), None)
    out['акция'] = st
    out['индекс/сырьё'] = next((by_base[b] for b in INDEX_PICK if b in by_base), None)
    return out


def short(v):
    s = json.dumps(v, ensure_ascii=False, default=str)
    return s if len(s) <= 160 else s[:157] + '…'


def distributions(markets, max_val=MAX_VAL):
    """Для каждого ключа объекта контракта со скалярными значениями и ≤ max_val различных — {значение: число контрактов}; ключи, у которых значений больше, не печатаются."""
    vals = {}
    for m in markets.values():
        for k, v in _info(m).items():
            if isinstance(v, (dict, list)):
                continue
            vals.setdefault(k, {}).setdefault(str(v), 0)
            vals[k][str(v)] += 1
    return {k: d for k, d in sorted(vals.items()) if 1 < len(d) <= max_val and len(d) < len(markets)}      # уникальные по контракту поля (symbol, id) — не классификаторы


def venue_section(ex, markets):
    L = [f'══ {ex}: контрактов в списке {len(markets)} ══']
    ex_ = pick_examples(markets)
    base_info = _info(ex_['крипто'][1]) if ex_.get('крипто') else {}
    for label, v in ex_.items():
        if v is None:
            L.append(f'— пример «{label}»: НЕ НАЙДЕН в списке контрактов')
            continue
        sym, m = v
        info = _info(m)
        L.append(f'— пример «{label}»: {sym} (база {m.get("base")}); ВСЕ ключи объекта контракта ({len(info)}): ' + ', '.join(info))
        txt = json.dumps(info, ensure_ascii=False, default=str)
        L.append('  объект: ' + (txt if len(txt) <= CUT else txt[:CUT] + '…'))
        if label != 'крипто' and base_info:
            diff = [k for k in sorted(set(info) | set(base_info)) if str(info.get(k)) != str(base_info.get(k)) and not isinstance(info.get(k), (dict, list))]
            only = sorted(set(info) - set(base_info))
            L.append(f'  ключи со значением, отличным от BTC ({len(diff)}): ' + ', '.join(f'{k}={short(info.get(k))}' for k in diff[:40]) + (f'; ключи, которых нет у BTC: {", ".join(only)}' if only else ''))
    dist = distributions(markets)
    L.append(f'— поля с ≤ {MAX_VAL} различных значений (значение: число контрактов):')
    for k, d in dist.items():
        L.append(f'  {k}: ' + ', '.join(f'{v}: {n}' for v, n in sorted(d.items(), key=lambda kv: -kv[1])))
    if not dist:
        L.append('  нет')
    return L


async def raw_get(session, url, params=None, timeout=15):
    """Публичный GET → (HTTP-код | None, начало тела). Без ключей и подписи."""
    import aiohttp
    try:
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=timeout)) as r:
            return r.status, (await r.text())[:300]
    except Exception as e:
        return None, f'{type(e).__name__}: {str(e)[:120]}'


COIN_ENDPOINTS = (
    ('gateio', 'https://api.gateio.ws/api/v4/spot/currencies', 'публичный'),
    ('bitget', 'https://api.bitget.com/api/v2/spot/public/coins', 'публичный'),
    ('mexc', 'https://api.mexc.com/api/v3/capital/config/getall', 'требует подписи (ключ)'),
    ('okx', 'https://www.okx.com/api/v5/asset/currencies', 'требует ключ'),
    ('bingx', 'https://open-api.bingx.com/openApi/wallets/v1/capital/config/getall', 'требует подпись (ключ)'),
)


async def coin_endpoints(session):
    L = ['══ эндпоинты списка валют/сетей для п. г (неподписанный GET; без ключей) ══']
    for ex, url, note in COIN_ENDPOINTS:
        code, body = await raw_get(session, url)
        keys = ''
        if code == 200:
            try:
                j = json.loads((await _full(session, url)) or '[]')
                data = j if isinstance(j, list) else (j.get('data') if isinstance(j, dict) else None)
                if isinstance(data, list) and data and isinstance(data[0], dict):
                    keys = f'; объектов {len(data)}; ключи объекта: ' + ', '.join(data[0])
                    ch = data[0].get('chains')
                    keys += f'; chains у первого объекта: {short(ch)}'
            except Exception as e:
                keys = f'; не удалось разобрать JSON ({type(e).__name__})'
        L.append(f'  {ex} ({note}): GET {url} → HTTP {code}; {body[:140]!r}{keys}')
    return L


async def _full(session, url):
    import aiohttp
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as r:
            return await r.text()
    except Exception:
        return None


async def variants(ex, client, session):
    """Варианты списка контрактов (по возможности): Bitget productType, Gate — другая валюта расчёта, MEXC — число; печатает число объектов или ошибку."""
    L = []
    try:
        if ex == 'bitget':
            for pt in ('USDT-FUTURES', 'USDC-FUTURES', 'COIN-FUTURES'):
                try:
                    d = client._data(await client._get('/api/v2/mix/market/contracts', {'productType': pt}), 'contracts')
                    L.append(f'  bitget productType={pt}: объектов {len(d or [])}')
                except Exception as e:
                    L.append(f'  bitget productType={pt}: {type(e).__name__}: {str(e)[:120]}')
        elif ex == 'gateio':
            for settle in ('usdt', 'btc'):
                try:
                    d = await client._get(f'/futures/{settle}/contracts')
                    L.append(f'  gateio /futures/{settle}/contracts: объектов {len(d) if isinstance(d, list) else "?"}')
                except Exception as e:
                    L.append(f'  gateio /futures/{settle}/contracts: {type(e).__name__}: {str(e)[:120]}')
    except Exception as e:
        L.append(f'  варианты {ex}: {type(e).__name__}: {str(e)[:100]}')
    return L


async def load(ex, clients=None):
    """→ (markets {symbol: {'base', 'info'…}}, client | None). BingX — ccxt (публичный), OKX — research_okx, Gate/MEXC/Bitget — research_venues."""
    if clients and ex in clients:
        c = clients[ex]
        return await c.load_markets(), c
    if ex == 'bingx':
        import ccxt.async_support as ccxt
        c = ccxt.bingx({'options': {'defaultType': 'swap'}, 'enableRateLimit': True})
        m = await c.load_markets()
        return {s: v for s, v in m.items() if v.get('swap') and v.get('linear') and str(v.get('quote')) == 'USDT'}, c
    if ex == 'okx':
        import research_okx
        c = research_okx.OkxPublic()
        return await c.load_markets(), c
    import research_venues
    c = research_venues.CLASSES[ex]()
    return await c.load_markets(), c


async def run_contract_probe(params, progress=None):
    """Отчёт шага 0 по всем биржам (§2.64.1). Ничего не изменяет."""
    import aiohttp
    L = [f'🔎 <b>/contract_probe — ключи контрактов по реальным ответам API (BOT_SPEC §2.64.1, шаг 0)</b> | код {params.get("version", "")}; {time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime())}; публичные GET без ключей']
    sess = aiohttp.ClientSession()
    try:
        for ex in params.get('order') or ORDER:
            client = None
            try:
                markets, client = await load(ex, params.get('clients'))
                L += venue_section(ex, markets)
                L += await variants(ex, client, sess)
            except Exception as e:
                L.append(f'══ {ex}: ОШИБКА {type(e).__name__}: {str(e)[:160]} ══')
            finally:
                try:
                    if client is not None:
                        await client.close()
                except Exception:
                    pass
            L.append('')
        if params.get('coins', True):
            L += await coin_endpoints(sess)
    finally:
        await sess.close()
    L.append('')
    L.append('Что прислать: этот файл целиком. По нему пункт (а) §2.64.2 дописывается отдельной поправкой (поля и значения), п. г — по строкам про эндпоинты сетей.')
    return '\n'.join(L)
