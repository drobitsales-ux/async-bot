"""research_vision.py — загрузчик публичного архива data.binance.vision (USDT-M фьючерсы): свечи с taker buy volume и дневные metrics (OI).
Для BOT_SPEC §2.34/§2.35. Только чтение данных; торговлю и таблицы бота не трогает.

Причина сбоя v91 (BadZipFile, листинг 32 файла): fetcher читал тело через `resp.content.read(N)`, а StreamReader.read(n) возвращает не n байт, а то, что
уже лежит в буфере (≈64 КБ) → усечённый zip и усечённый XML-листинг. Здесь тело читается ПОЛНОСТЬЮ (`resp.read()`), ответ проверяется до разбора.

Схемы URL (проверено по структуре архива):
  свечи, месяц : {ARCH}/data/futures/um/monthly/klines/{SYM}/{tf}/{SYM}-{tf}-{YYYY}-{MM}.zip           (+ '.CHECKSUM')
  metrics, день: {ARCH}/data/futures/um/daily/metrics/{SYM}/{SYM}-metrics-{YYYY}-{MM}-{DD}.zip         (+ '.CHECKSUM')
  листинг      : {S3}?prefix={prefix}&max-keys=1000[&marker=…] — S3 ListObjects v1, продолжение по последнему ключу, пока IsTruncated=true.
Хранение: {DIR}/{SYM}/klines_{tf}.npz (ts int64 мс; o,h,l,c,v,tbv float32; months — загруженные месяцы) и {DIR}/{SYM}/oi_1h.npz (концы часов, OI на конец часа,
ts строки-источника; days — загруженные дни). Запись атомарная (tmp → os.replace): нехватка места не портит кэш."""
import asyncio
import hashlib
import io
import logging
import os
import re
import shutil
import zipfile
from datetime import datetime, timezone

import numpy as np

ARCH = 'https://data.binance.vision'
S3 = 'https://s3-ap-northeast-1.amazonaws.com/data.binance.vision'
RETRY_DELAYS = (2, 4, 8, 16, 30)          # до 5 попыток при сетевых ошибках/5xx
MAX_ATTEMPTS = len(RETRY_DELAYS)
CHECKSUM_TRIES = 3
MIN_FREE_MB = 200
HOUR_MS = 3_600_000
FETCH_TIMEOUT_S = 60
MAX_BODY = 60_000_000
_sleep = asyncio.sleep                     # подменяется в тестах


def vision_dir():
    return os.getenv('VISION_DIR') or ('/data/vision' if os.path.exists('/data') else '/tmp/vision')


def pause_s():
    try:
        return max(0.0, float(os.getenv('VISION_PAUSE_SEC', '0.2')))
    except ValueError:
        return 0.2


def concurrency():
    try:
        return max(1, int(os.getenv('VISION_CONCURRENCY', '3')))
    except ValueError:
        return 3


class VisionError(Exception):
    """kind: not_zip | http | network | missing | corrupt | parse | low_disk"""
    def __init__(self, kind, msg):
        self.kind = kind
        super().__init__(f'{kind}: {msg}')


# ── HTTP ───────────────────────────────────────────────────────────────────────────────────
def make_fetcher(session):
    """GET/HEAD через aiohttp с ПОЛНЫМ чтением тела (resp.read() — до EOF), лимит MAX_BODY. Ошибки → поле error, не исключение.
    → dict(status, headers(lower-case), body(bytes), error)."""
    import aiohttp

    async def fetch(url, params=None, method='GET'):
        try:
            async with session.request(method, url, params=params, timeout=aiohttp.ClientTimeout(total=FETCH_TIMEOUT_S)) as resp:
                cl = resp.headers.get('Content-Length')
                if method != 'HEAD' and cl and cl.isdigit() and int(cl) > MAX_BODY:
                    return {'status': resp.status, 'headers': {k.lower(): v for k, v in resp.headers.items()}, 'body': b'', 'error': f'тело {cl} Б больше лимита {MAX_BODY}'}
                body = b'' if method == 'HEAD' else await resp.read()
                return {'status': resp.status, 'headers': {k.lower(): v for k, v in resp.headers.items()}, 'body': body, 'error': ''}
        except asyncio.TimeoutError:
            return {'status': None, 'headers': {}, 'body': b'', 'error': f'таймаут {FETCH_TIMEOUT_S} с'}
        except Exception as e:
            return {'status': None, 'headers': {}, 'body': b'', 'error': f'{type(e).__name__}: {str(e)[:120]}'}
    return fetch


# ── URL-ы и листинг ────────────────────────────────────────────────────────────────────────
def kline_key(sym, tf, y, m):
    return f'data/futures/um/monthly/klines/{sym}/{tf}/{sym}-{tf}-{y}-{m:02d}.zip'


def metrics_key(sym, y, m, d):
    return f'data/futures/um/daily/metrics/{sym}/{sym}-metrics-{y}-{m:02d}-{d:02d}.zip'


def url_of(key):
    return f'{ARCH}/{key}'


def _parse_listing(xml: bytes):
    t = xml.decode('utf-8', 'ignore')
    return re.findall(r'<Key>([^<]+)</Key>', t), '<IsTruncated>true</IsTruncated>' in t


async def list_keys(fetch, prefix, max_pages=200):
    """Листинг префикса S3 со ВСЕМИ страницами (marker = последний ключ страницы), только .zip (без .CHECKSUM).
    → (keys[list], pages[int]); ошибка страницы → VisionError."""
    keys, marker, pages = [], None, 0
    while pages < max_pages:
        params = {'prefix': prefix, 'max-keys': 1000}
        if marker:
            params['marker'] = marker
        r = await fetch(S3, params, 'GET')
        pages += 1
        if r['status'] != 200:
            raise VisionError('http', f'листинг {prefix}: HTTP {r["status"]} {r["error"]}')
        page, trunc = _parse_listing(r['body'])
        if not page:
            break
        keys.extend(page)
        marker = page[-1]
        if not trunc:
            break
    return [k for k in dict.fromkeys(keys) if k.endswith('.zip')], pages


def key_date(key):
    """YYYY-MM[-DD] из имени файла архива."""
    m = re.search(r'(\d{4}-\d{2}(?:-\d{2})?)\.zip$', key)
    return m.group(1) if m else None


# ── проверка и загрузка файла ──────────────────────────────────────────────────────────────
def check_zip_response(r):
    """Ответ — действительно zip? HTTP 200, Content-Type не text/html, сигнатура PK\\x03\\x04. → '' или причина (с первыми 200 байтами)."""
    if r['status'] != 200:
        return f'HTTP {r["status"]} {r["error"]}'.strip()
    ct = (r['headers'].get('content-type') or '').lower()
    body = r['body']
    if 'text/html' in ct or not body.startswith(b'PK\x03\x04'):
        return f'не zip (HTTP 200, Content-Type={ct or "?"}, {len(body)} Б), первые 200 байт: {body[:200]!r}'
    return ''


def parse_checksum(body: bytes):
    m = re.match(r'\s*([0-9a-fA-F]{64})', body.decode('utf-8', 'ignore'))
    return m.group(1).lower() if m else None


async def download_zip(fetch, key, pause=None):
    """Скачивает .zip по ПРЯМОМУ URL, проверяет ответ и sha256 по .CHECKSUM. Сеть/5xx — до 5 попыток (паузы 2…30 с); 404 → missing; не-zip — повторы в
    тех же 5 попытках, затем ошибка; несовпадение CHECKSUM — 3 повтора, затем VisionError('corrupt') («битый»). → bytes."""
    log = logging.getLogger('vision')
    pause = pause_s() if pause is None else pause
    url = url_of(key)
    last = ''
    csum_bad = 0
    for attempt in range(1, MAX_ATTEMPTS + 1):
        r = await fetch(url, None, 'GET')
        await _sleep(pause)
        if r['status'] == 404:
            raise VisionError('missing', f'{key}: 404')
        why = check_zip_response(r)
        if why:
            last = why
            log.warning(f'[VISION] {key} попытка {attempt}/{MAX_ATTEMPTS}: {why}')
            transient = r['status'] is None or (r['status'] or 0) >= 500 or r['status'] == 200 or r['status'] == 429
            if not transient:
                raise VisionError('http', f'{key}: {why}')
            if attempt < MAX_ATTEMPTS:
                await _sleep(RETRY_DELAYS[attempt - 1])
            continue
        c = await fetch(url + '.CHECKSUM', None, 'GET')
        await _sleep(pause)
        if c['status'] == 200 and parse_checksum(c['body']):
            if hashlib.sha256(r['body']).hexdigest() != parse_checksum(c['body']):
                csum_bad += 1
                last = f'sha256 не совпал ({csum_bad}/{CHECKSUM_TRIES})'
                log.warning(f'[VISION] {key}: {last}')
                if csum_bad >= CHECKSUM_TRIES:
                    raise VisionError('corrupt', f'{key}: битый — {last}')
                continue
        elif c['status'] not in (404, 403):
            # CHECKSUM недоступен по сети — повторяем целиком; 404/403 (нет файла) — допускаем загрузку без проверки
            last = f'.CHECKSUM: HTTP {c["status"]} {c["error"]}'
            if attempt < MAX_ATTEMPTS:
                await _sleep(RETRY_DELAYS[attempt - 1])
            continue
        return r['body']
    raise VisionError('network' if 'не zip' not in last else 'not_zip', f'{key}: {last}')


def unzip_text(body: bytes) -> str:
    z = zipfile.ZipFile(io.BytesIO(body))
    return z.read(z.namelist()[0]).decode('utf-8', 'ignore')


# ── парсинг ─────────────────────────────────────────────────────────────────────────────────
def to_ms(arr):
    """Метки времени → мс: значения ≥ 1e14 считаются микросекундами (мс сейчас ≈ 1.7e12, мкс ≈ 1.7e15)."""
    a = np.asarray(arr, dtype=np.int64)
    return np.where(a >= 100_000_000_000_000, a // 1000, a)


def parse_klines_csv(text: str) -> dict:
    """Свечи Binance: колонки по порядку open_time, open, high, low, close, volume, close_time, quote_volume, count, taker_buy_volume,
    taker_buy_quote_volume, ignore; файл с заголовком или без. → dict(ts,o,h,l,c,v,tbv, header: bool, cols: int, unit: 'ms'|'us')."""
    lines = [l for l in text.splitlines() if l.strip()]
    if not lines:
        raise VisionError('parse', 'пустой файл свечей')
    header = not re.match(r'^\s*-?\d', lines[0])
    rows = lines[1:] if header else lines
    if not rows:
        raise VisionError('parse', 'нет строк данных')
    parts = [r.split(',') for r in rows]
    cols = min(len(p) for p in parts)
    if cols < 10:
        raise VisionError('parse', f'колонок {cols} < 10: нет taker_buy_volume')
    raw_ts = np.array([int(p[0]) for p in parts], dtype=np.int64)
    unit = 'us' if raw_ts[0] >= 100_000_000_000_000 else 'ms'
    f = lambda i: np.array([float(p[i]) for p in parts], dtype=np.float64)
    return {'ts': to_ms(raw_ts), 'o': f(1), 'h': f(2), 'l': f(3), 'c': f(4), 'v': f(5), 'tbv': f(9), 'header': header, 'cols': cols, 'unit': unit}


def _ts_cell(s):
    s = s.strip()
    if re.match(r'^\d+$', s):
        v = int(s)
        return v // 1000 if v >= 100_000_000_000_000 else v
    return int(datetime.strptime(s, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc).timestamp() * 1000)


def parse_metrics_csv(text: str) -> dict:
    """Дневной metrics: шаг строк определяется по данным (медиана разностей), поля — по заголовку; OI = sum_open_interest (единицы базовой монеты,
    не зависит от цены). → dict(ts мс, oi, oi_value|None, step_min, fields[list])."""
    lines = [l for l in text.splitlines() if l.strip()]
    if len(lines) < 2:
        raise VisionError('parse', 'metrics: нет строк данных')
    head = [h.strip() for h in lines[0].split(',')]
    if re.match(r'^\s*\d', lines[0]):
        raise VisionError('parse', 'metrics: нет заголовка — поле OI определить нельзя')
    low = [h.lower() for h in head]
    try:
        it, io_ = low.index('create_time'), low.index('sum_open_interest')
    except ValueError:
        raise VisionError('parse', f'metrics: нет create_time/sum_open_interest в заголовке {head}')
    iv = low.index('sum_open_interest_value') if 'sum_open_interest_value' in low else None
    ts, oi, oiv = [], [], []
    for l in lines[1:]:
        p = l.split(',')
        if len(p) <= io_:
            continue
        ts.append(_ts_cell(p[it])); oi.append(float(p[io_] or 'nan'))
        oiv.append(float(p[iv]) if iv is not None and len(p) > iv and p[iv] else float('nan'))
    ts = np.array(ts, dtype=np.int64)
    step = float(np.median(np.diff(np.sort(ts))) / 60000) if len(ts) > 1 else None
    return {'ts': ts, 'oi': np.array(oi), 'oi_value': np.array(oiv) if iv is not None else None, 'step_min': step, 'fields': head}


def hourly_oi_end(ts, oi):
    """Часовой агрегат OI: для конца часа E значение = последняя строка с create_time ∈ (E−1ч, E] (create_time ≤ E; строка ровно на E относится к часу,
    заканчивающемуся в E). → (E[int64], oi[float64], src_ts[int64]); часы без строк не создаются (в серии — пропуск)."""
    ts = np.asarray(ts, dtype=np.int64); oi = np.asarray(oi, dtype=float)
    E = -(-ts // HOUR_MS) * HOUR_MS                           # ceil до целого часа
    order = np.argsort(ts, kind='stable')
    best = {}
    for i in order:
        best[int(E[i])] = (int(ts[i]), float(oi[i]))          # возрастающий ts → остаётся последняя
    ks = sorted(best)
    return (np.array(ks, dtype=np.int64), np.array([best[k][1] for k in ks]), np.array([best[k][0] for k in ks], dtype=np.int64))


# ── хранение (атомарно, кэш не портится) ─────────────────────────────────────────────────────
def free_mb(path):
    p = path
    while not os.path.exists(p) and os.path.dirname(p) not in ('', p):
        p = os.path.dirname(p)
    return shutil.disk_usage(p).free / 1e6


def check_space(path, need_mb=MIN_FREE_MB):
    f = free_mb(path)
    if f < need_mb:
        raise VisionError('low_disk', f'свободно {f:.0f} МБ < {need_mb} МБ — загрузка остановлена, кэш не тронут')


def _atomic_savez(path, **arrays):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + '.tmp.npz'
    try:
        np.savez_compressed(tmp, **arrays)
        os.replace(tmp, path)
    except BaseException:
        try:
            os.remove(tmp)
        except OSError:
            pass
        raise


def _load_npz(path):
    if not os.path.exists(path):
        return None
    try:
        with np.load(path, allow_pickle=False) as z:
            return {k: z[k] for k in z.files}
    except Exception as e:
        logging.getLogger('vision').warning(f'[VISION] {path} не читается ({e}) — будет пересобран')
        return None


def klines_path(d, sym, tf):
    return os.path.join(d, sym, f'klines_{tf}.npz')


def oi_path(d, sym):
    return os.path.join(d, sym, 'oi_1h.npz')


def load_klines(d, sym, tf):
    """Кэш свечей → dict(ts,o,h,l,c,v,tbv float64; months) или None."""
    z = _load_npz(klines_path(d, sym, tf))
    if z is None:
        return None
    out = {k: z[k].astype(np.float64) for k in ('o', 'h', 'l', 'c', 'v', 'tbv')}
    out['ts'] = z['ts']; out['months'] = [str(x) for x in z['months']]
    return out


def load_oi(d, sym):
    z = _load_npz(oi_path(d, sym))
    if z is None:
        return None
    return {'E': z['E'], 'oi': z['oi'].astype(np.float64), 'src': z['src'], 'days': [str(x) for x in z['days']]}


def _merge_klines(old, new, month):
    if old is None:
        ts = new['ts']; cols = {k: new[k] for k in ('o', 'h', 'l', 'c', 'v', 'tbv')}; months = [month]
    else:
        ts = np.concatenate([old['ts'], new['ts']]); cols = {k: np.concatenate([old[k], new[k]]) for k in ('o', 'h', 'l', 'c', 'v', 'tbv')}
        months = sorted(set(old['months']) | {month})
    order = np.argsort(ts, kind='stable'); ts = ts[order]
    keep = np.r_[True, np.diff(ts) != 0] if len(ts) > 1 else np.ones(len(ts), bool)
    return ts[keep], {k: v[order][keep] for k, v in cols.items()}, months


def save_klines(d, sym, tf, ts, cols, months):
    _atomic_savez(klines_path(d, sym, tf), ts=ts.astype(np.int64), months=np.array(months, dtype='U7'),
                  **{k: cols[k].astype(np.float32) for k in ('o', 'h', 'l', 'c', 'v', 'tbv')})


def save_oi(d, sym, E, oi, src, days):
    _atomic_savez(oi_path(d, sym), E=E.astype(np.int64), oi=oi.astype(np.float32), src=src.astype(np.int64), days=np.array(days, dtype='U10'))


# ── загрузка символа ─────────────────────────────────────────────────────────────────────────
async def sync_klines(fetch, d, sym, tf, progress=None, keys=None):
    """Догружает недостающие месяцы свечей символа (сначала листинг, все страницы). Один месяц = один файл; сохранение после каждого.
    → dict(listed, loaded_now, failed{key: why}, first, last)."""
    sem = asyncio.Semaphore(concurrency())
    prefix = f'data/futures/um/monthly/klines/{sym}/{tf}/'
    if keys is None:
        keys, _pages = await list_keys(fetch, prefix)
    cur = load_klines(d, sym, tf)
    have = set(cur['months']) if cur else set()
    todo = [k for k in keys if key_date(k) and key_date(k) not in have]
    res = {'listed': len(keys), 'loaded_now': 0, 'failed': {}, 'first': key_date(keys[0]) if keys else None, 'last': key_date(keys[-1]) if keys else None}
    lock = asyncio.Lock()

    async def one(k):
        async with sem:
            check_space(d)
            try:
                body = await download_zip(fetch, k)
                parsed = parse_klines_csv(unzip_text(body))
            except VisionError as e:
                if e.kind == 'low_disk':
                    raise
                res['failed'][k] = str(e)[:200]; return
            except (zipfile.BadZipFile, ValueError, KeyError) as e:
                res['failed'][k] = f'parse: {type(e).__name__}: {str(e)[:100]}'; return
            async with lock:
                cur_ = load_klines(d, sym, tf)
                ts, cols, months = _merge_klines(cur_, parsed, key_date(k))
                save_klines(d, sym, tf, ts, cols, months)
                res['loaded_now'] += 1
    outs = await asyncio.gather(*[one(k) for k in todo], return_exceptions=True)
    low = [o for o in outs if isinstance(o, VisionError) and o.kind == 'low_disk']
    other = [o for o in outs if isinstance(o, BaseException) and o not in low]
    if other:
        raise other[0]
    if low:
        raise low[0]
    if progress and todo:
        await progress(f'{sym} {tf}: загружено {res["loaded_now"]} из {len(todo)} месяцев (в архиве {res["listed"]}), ошибок {len(res["failed"])}')
    return res


async def sync_oi(fetch, d, sym, progress=None, keys=None):
    """Догружает недостающие дни metrics, агрегируя в часовой OI на конец часа; сырые файлы не сохраняются."""
    sem = asyncio.Semaphore(concurrency())
    prefix = f'data/futures/um/daily/metrics/{sym}/'
    if keys is None:
        keys, _pages = await list_keys(fetch, prefix)
    cur = load_oi(d, sym)
    have = set(cur['days']) if cur else set()
    todo = [k for k in keys if key_date(k) and key_date(k) not in have]
    res = {'listed': len(keys), 'loaded_now': 0, 'failed': {}, 'first': key_date(keys[0]) if keys else None, 'last': key_date(keys[-1]) if keys else None, 'step_min': None}
    state = {'E': cur['E'] if cur else np.array([], np.int64), 'oi': cur['oi'] if cur else np.array([]), 'src': cur['src'] if cur else np.array([], np.int64),
             'days': set(have)}
    lock = asyncio.Lock()
    batch = {'n': 0}

    async def one(k):
        async with sem:
            check_space(d)
            try:
                body = await download_zip(fetch, k)
                m = parse_metrics_csv(unzip_text(body))
            except VisionError as e:
                if e.kind == 'low_disk':
                    raise
                res['failed'][k] = str(e)[:200]; return
            except (zipfile.BadZipFile, ValueError, KeyError) as e:
                res['failed'][k] = f'parse: {type(e).__name__}: {str(e)[:100]}'; return
            E, oi, src = hourly_oi_end(m['ts'], m['oi'])
            async with lock:
                res['step_min'] = m['step_min'] if res['step_min'] is None else res['step_min']
                allE = np.concatenate([state['E'], E]); allo = np.concatenate([state['oi'], oi]); alls = np.concatenate([state['src'], src])
                order = np.lexsort((alls, allE))                        # по E, внутри — по возрастанию src; берём последнюю запись для каждого E
                allE, allo, alls = allE[order], allo[order], alls[order]
                last = np.r_[allE[1:] != allE[:-1], True] if len(allE) else np.array([], bool)
                state['E'], state['oi'], state['src'] = allE[last], allo[last], alls[last]
                state['days'].add(key_date(k)); res['loaded_now'] += 1; batch['n'] += 1
                if batch['n'] % 50 == 0:
                    save_oi(d, sym, state['E'], state['oi'], state['src'], sorted(state['days']))
    outs = await asyncio.gather(*[one(k) for k in todo], return_exceptions=True)
    low = [o for o in outs if isinstance(o, VisionError) and o.kind == 'low_disk']
    other = [o for o in outs if isinstance(o, BaseException) and o not in low]
    if res['loaded_now'] or (cur is None and not low and not other):
        try:
            save_oi(d, sym, state['E'], state['oi'], state['src'], sorted(state['days']))     # уже разобранные дни сохраняются, даже если загрузка остановлена
        except OSError:
            if not low:
                raise
    if other:
        raise other[0]
    if low:
        raise low[0]
    if progress and todo:
        await progress(f'{sym} OI: загружено {res["loaded_now"]} из {len(todo)} дней (в архиве {res["listed"]}), ошибок {len(res["failed"])}')
    return res
