"""access_probe.py — шаг 0 (BOT_SPEC §2.50): доступность публичных API Bybit и OKX с сервера бота.
Напрямую через aiohttp (HTTP-код, заголовки Server / X-Cache / Via / Retry-After, первые 300 байт тела) и через ccxt (имя класса исключения);
вердикт «гео-блок» / «лимит частоты» / «доступно». Только публичные GET; ключей нет; не бросает исключений."""
import asyncio
import json
import re
import time

import aiohttp

PROBES = {
    'bybit': [('рынки v5', 'https://api.bybit.com/v5/market/instruments-info?category=linear&limit=1'),
              ('тикер v5', 'https://api.bybit.com/v5/market/tickers?category=linear&symbol=BTCUSDT')],
    'okx': [('тикер', 'https://www.okx.com/api/v5/market/ticker?instId=BTC-USDT-SWAP')],
    # [v129, §2.59.1] новые биржи H2EXT: прямой REST (ccxt не используется); вердикт — только по факту пробы на проде
    'gateio': [('контракт BTC_USDT', 'https://api.gateio.ws/api/v4/futures/usdt/contracts/BTC_USDT'),
               ('тикер', 'https://api.gateio.ws/api/v4/futures/usdt/tickers?contract=BTC_USDT')],
    'mexc': [('тикер', 'https://contract.mexc.com/api/v1/contract/ticker?symbol=BTC_USDT'),
             ('контракты', 'https://contract.mexc.com/api/v1/contract/detail?symbol=BTC_USDT')],
    'bitget': [('тикер', 'https://api.bitget.com/api/v2/mix/market/ticker?symbol=BTCUSDT&productType=USDT-FUTURES'),
               ('контракты', 'https://api.bitget.com/api/v2/mix/market/contracts?productType=USDT-FUTURES&symbol=BTCUSDT')],
}
CCXT_IDS = ('bybit', 'okx')                 # через ccxt пробуются только эти; новые биржи — только прямой REST
HEADERS_KEEP = ('server', 'x-cache', 'via', 'retry-after', 'x-amz-cf-id')
GEO_WORDS = re.compile(r'(?i)block access from your country|country|region|restricted location|not available in your|unavailable for legal|'
                       r'access denied|forbidden|eligibility|your location')


async def http_get(session, url: str, timeout: float = 15.0) -> dict:
    """Один GET; не бросает исключений. → {'dt','status','headers','body','err'}."""
    t0 = time.time()
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout)) as r:
            raw = await r.read()
            body = raw[:300].decode('utf-8', 'replace')
            hdr = {k.lower(): v for k, v in r.headers.items() if k.lower() in HEADERS_KEEP}
            return {'dt': time.time() - t0, 'status': r.status, 'headers': hdr, 'body': body, 'err': None}
    except Exception as e:
        return {'dt': time.time() - t0, 'status': None, 'headers': {}, 'body': '', 'err': f'{type(e).__name__}: {e}'.strip(': ')}


def _ok_body(body: str) -> bool:
    """Тело — успешный ответ API: Bybit retCode 0 / OKX code '0' (по первым 300 байтам JSON может быть обрезан — ищем маркеры)."""
    try:
        j = json.loads(body)
        if isinstance(j, list):
            return bool(j)
        return (j.get('retCode') == 0 or str(j.get('code')) in ('0', '00000') or j.get('success') is True or j.get('name') == 'BTC_USDT')
    except Exception:
        return bool(re.search(r'"retCode"\s*:\s*0\b', body) or re.search(r'"code"\s*:\s*"(0|00000)"', body) or re.search(r'"code"\s*:\s*0\b', body)
                    or re.search(r'"success"\s*:\s*true', body) or re.search(r'"name"\s*:\s*"BTC_USDT"', body) or re.search(r'^\s*\[\s*\{', body))


def classify_http(r: dict) -> str:
    """Вердикт по одному прямому ответу: 'доступно' | 'гео-блок' | 'лимит частоты' | 'не определено'."""
    st = r.get('status')
    if st is None:
        return 'не определено'
    hdr = r.get('headers') or {}
    body = r.get('body') or ''
    cf = 'cloudfront' in (hdr.get('server', '') + hdr.get('x-cache', '') + hdr.get('via', '')).lower()
    if st == 200 and _ok_body(body):
        return 'доступно'
    if st == 451:
        return 'гео-блок'
    if st == 429 or hdr.get('retry-after') or re.search(r'"ret(Code|_code)"\s*:\s*10006\b', body):
        return 'лимит частоты'
    if st == 403 and (cf or GEO_WORDS.search(body)):
        return 'гео-блок'
    if st == 403:
        return 'гео-блок'         # 403 на публичный GET без ключа — не лимит (лимит отдаёт 429/Retry-After); помечено в тексте как «403»
    return 'не определено'


def classify_exchange(http_results: list, ccxt_res: dict = None) -> str:
    """Общий вердикт по бирже: хотя бы один прямой ответ «доступно» → «доступно»; иначе самый частый однозначный вердикт; нет данных → «не определено»."""
    vs = [classify_http(r) for r in http_results or []]
    if 'доступно' in vs:
        return 'доступно'
    for v in ('гео-блок', 'лимит частоты'):
        if v in vs:
            return v
    return 'не определено'


CLOSE_DELAY = 1.0


async def close_ccxt(ex, delay: float = None, sleep=asyncio.sleep):
    """[v120] Закрывает временный экземпляр ccxt ДВАЖДЫ: после исключения из load_markets (ccxt запускает несколько запросов параллельно) недозавершённые запросы могут
    заново открыть сессию уже после первого close() — тогда в логе «<биржа> requires to release all resources». Пауза между вызовами даёт им закончиться.
    Гипотеза причины: в среде разработки без сети НЕ воспроизведена. Не бросает исключений."""
    for k in range(2):
        try:
            await ex.close()
        except Exception:
            pass
        if k == 0:
            await sleep(CLOSE_DELAY if delay is None else delay)


async def ccxt_probe(ex_id: str, timeout: float = 60.0, sleep=asyncio.sleep) -> dict:
    """load_markets через ccxt (публичный, без ключей) → {'dt','ok','exc','n'}; exc — имя класса исключения."""
    t0 = time.time()
    ex = None
    try:
        import ccxt.async_support as ccxt_async
        ex = getattr(ccxt_async, ex_id)({'enableRateLimit': True, 'timeout': 15000})
        m = await asyncio.wait_for(ex.load_markets(), timeout)
        return {'dt': time.time() - t0, 'ok': True, 'exc': None, 'n': len(m or {})}
    except Exception as e:
        return {'dt': time.time() - t0, 'ok': False, 'exc': f'{type(e).__name__}: {str(e)[:120]}', 'n': 0}
    finally:
        if ex is not None:
            await close_ccxt(ex, sleep=sleep)


async def probe_all(ex_ids=('bybit', 'okx', 'gateio', 'mexc', 'bitget'), session=None, ccxt_fn=ccxt_probe) -> dict:
    """→ {ex: {'http': [{'name','url',…результат…}], 'ccxt': {...}, 'verdict': str}}. Не бросает исключений."""
    out = {}
    own = session is None
    sess = session
    try:
        if own:
            sess = aiohttp.ClientSession()
        for ex_id in ex_ids:
            hs = []
            for name, url in PROBES.get(ex_id, []):
                r = await http_get(sess, url)
                r['name'], r['url'] = name, url
                hs.append(r)
            if ex_id not in CCXT_IDS:
                c = {'dt': 0.0, 'ok': None, 'exc': None, 'n': 0}
            else:
                try:
                    c = await ccxt_fn(ex_id)
                except Exception as e:
                    c = {'dt': 0.0, 'ok': False, 'exc': f'{type(e).__name__}: {e}', 'n': 0}
            out[ex_id] = {'http': hs, 'ccxt': c, 'verdict': classify_exchange(hs, c)}
    except Exception as e:
        out['_error'] = f'{type(e).__name__}: {e}'
    finally:
        if own and sess is not None:
            try:
                await sess.close()
            except Exception:
                pass
    return out


def format_probe(res: dict, esc=lambda x: str(x)) -> list:
    """Строки для Telegram (HTML)."""
    lines = []
    for ex_id, d in res.items():
        if ex_id == '_error':
            lines.append(f'⚠️ проба доступа: {esc(d)}')
            continue
        lines.append(f"<b>{esc(ex_id)}: {esc(d['verdict'])}</b>")
        for r in d['http']:
            if r['status'] is None:
                lines.append(f"  aiohttp {esc(r['name'])}: ОШИБКА {esc(r['err'])} ({r['dt']:.1f}с)")
                continue
            h = ', '.join(f'{k}={v}' for k, v in sorted(r['headers'].items())) or 'заголовков нет'
            lines.append(f"  aiohttp {esc(r['name'])}: HTTP {r['status']} ({r['dt']:.1f}с) [{esc(h)}] тело: <code>{esc(r['body'][:300])}</code>")
        c = d['ccxt']
        if c['ok'] is None:
            lines.append('  ccxt: не проверялся (прямой REST без ccxt, §2.59.1)')
            continue
        lines.append('  ccxt load_markets: ' + (f"ok, {c['n']} рынков ({c['dt']:.1f}с)" if c['ok'] else f"ИСКЛЮЧЕНИЕ {esc(c['exc'])} ({c['dt']:.1f}с)"))
    return lines
