"""research_probe.py — /data_probe: проверка ДОСТУПНОСТИ внешних данных (Binance USDT-M, data.binance.vision, BingX) — BOT_SPEC §2.34.

ТОЛЬКО МЕТАДАННЫЕ: коды ответов, наличие полей, даты первых записей, лимиты, размеры файлов, шаг строк, объём/время загрузки, свободное место.
Ни одна строка данных из проверок не используется для расчёта сигналов в этом деплое; бэктеста H-OF1/H-OF2 в модуле НЕТ.
Запросы идут с сервера бота (не из песочницы); каждая проверка изолирована: исключения наружу не выходят — в отчёте статус/текст ошибки.
HTTP вынесен в передаваемый fetch(url, params, method) → dict(status, headers, body, error), чтобы тесты подставляли поддельные ответы."""
import asyncio
import io
import json
import math
import os
import re
import shutil
import sqlite3
import time
import zipfile
from datetime import datetime, timezone

import research_htf as H

FAPI = 'https://fapi.binance.com'
ARCH = 'https://data.binance.vision'
S3 = 'https://s3-ap-northeast-1.amazonaws.com/data.binance.vision'
BINGX = 'https://open-api.bingx.com'
SYMBOL = 'BTCUSDT'
IS_END = '2024-12-31'
IS_MIN_DAYS = 365                  # OI-история короче года до конца IS → H-OF2 «непроверяема на истории» (правило доступности, §2.34.7)
KLINE_ROW_BYTES = 100              # SQLite: строка свечи (ts,o,h,l,c,v,taker_buy_vol) + индекс — грубая оценка
OI_ROW_BYTES = 70
FETCH_TIMEOUT_S = 20


def _d(ms):
    return datetime.fromtimestamp(ms / 1000, timezone.utc).strftime('%Y-%m-%d') if ms else '—'


def _js(r):
    try:
        return json.loads(r['body'])
    except Exception:
        return None


def make_fetcher(session):
    """Реальный HTTP через aiohttp: GET/HEAD, таймаут, тело ≤ 8 МБ. Ошибки → поле error, не исключение."""
    import aiohttp

    async def fetch(url, params=None, method='GET'):
        try:
            async with session.request(method, url, params=params, timeout=aiohttp.ClientTimeout(total=FETCH_TIMEOUT_S)) as resp:
                body = b'' if method == 'HEAD' else await resp.content.read(8_000_000)
                return {'status': resp.status, 'headers': {k.lower(): v for k, v in resp.headers.items()}, 'body': body, 'error': ''}
        except asyncio.TimeoutError:
            return {'status': None, 'headers': {}, 'body': b'', 'error': f'таймаут {FETCH_TIMEOUT_S} с'}
        except Exception as e:
            return {'status': None, 'headers': {}, 'body': b'', 'error': f'{type(e).__name__}: {str(e)[:120]}'}
    return fetch


async def _safe(name, coro, res):
    """Изолированная проверка: любое исключение → res[name] = {'error': ...}."""
    try:
        res[name] = await coro
    except Exception as e:
        res[name] = {'error': f'{type(e).__name__}: {str(e)[:150]}'}


def _pause():
    return H.fetch_pause()


async def _get(fetch, url, params=None, method='GET'):
    r = await fetch(url, params, method)
    await H._sleep(_pause())
    return r


# ── 2.1 Binance REST fapi ───────────────────────────────────────────────────────────────
async def probe_fapi(fetch):
    out = {'ping': None, 'klines': {}, 'oi': {}, 'exchange_info': None}
    r = await _get(fetch, FAPI + '/fapi/v1/ping')
    out['ping'] = {'status': r['status'], 'error': r['error']}
    if r['status'] != 200:
        out['blocked'] = True            # 451/403/таймаут — дальше fapi не пробуем
        return out
    for tf in ('15m', '1h'):
        info = {}
        r1 = await _get(fetch, FAPI + '/fapi/v1/klines', {'symbol': SYMBOL, 'interval': tf, 'startTime': 0, 'limit': 1})
        js = _js(r1)
        info['status'] = r1['status']
        if isinstance(js, list) and js:
            info['first_ts'] = int(js[0][0]); info['fields'] = len(js[0])
            info['taker_buy'] = len(js[0]) >= 11                    # индекс 9 — taker buy base volume, 10 — taker buy quote volume
        else:
            info['note'] = (r1['error'] or (r1['body'][:150].decode('utf-8', 'ignore') if r1['body'] else ''))
        r2 = await _get(fetch, FAPI + '/fapi/v1/klines', {'symbol': SYMBOL, 'interval': tf, 'limit': 1500})
        js2 = _js(r2)
        info['limit_status'] = r2['status']
        info['limit_n'] = len(js2) if isinstance(js2, list) else None
        info['weight_1m'] = r2['headers'].get('x-mbx-used-weight-1m')
        out['klines'][tf] = info
    # открытый интерес: история
    for period in ('1h', '15m'):
        info = {}
        r = await _get(fetch, FAPI + '/futures/data/openInterestHist', {'symbol': SYMBOL, 'period': period, 'limit': 500})
        js = _js(r); info['status'] = r['status']
        if isinstance(js, list) and js:
            ts = [int(x['timestamp']) for x in js]
            info.update(n=len(js), first_ts=min(ts), last_ts=max(ts), step_min=(sorted(ts)[1] - sorted(ts)[0]) / 60000 if len(ts) > 1 else None,
                        fields=sorted(js[0].keys()))
        else:
            info['note'] = (r['error'] or r['body'][:150].decode('utf-8', 'ignore'))
        r = await _get(fetch, FAPI + '/futures/data/openInterestHist',
                       {'symbol': SYMBOL, 'period': period, 'startTime': int(datetime(2022, 1, 1, tzinfo=timezone.utc).timestamp() * 1000),
                        'endTime': int(datetime(2022, 1, 2, tzinfo=timezone.utc).timestamp() * 1000), 'limit': 10})
        js = _js(r)
        info['old_status'] = r['status']
        info['old_n'] = len(js) if isinstance(js, list) else None
        info['old_note'] = '' if isinstance(js, list) and js else (r['error'] or r['body'][:150].decode('utf-8', 'ignore'))
        out['oi'][period] = info
    r = await _get(fetch, FAPI + '/fapi/v1/exchangeInfo')
    js = _js(r)
    if isinstance(js, dict) and js.get('symbols'):
        out['exchange_info'] = {s['symbol']: {'onboard': s.get('onboardDate'), 'status': s.get('status'), 'type': s.get('contractType')} for s in js['symbols']}
    else:
        out['exchange_info'] = {'error': r['error'] or f'HTTP {r["status"]}'}
    return out


# ── 2.2 архив data.binance.vision ───────────────────────────────────────────────────────
def _s3_keys(xml: bytes):
    t = xml.decode('utf-8', 'ignore')
    return re.findall(r'<Key>([^<]+)</Key>', t), '<IsTruncated>true</IsTruncated>' in t


async def _s3_list(fetch, prefix, marker=None, max_keys=1000):
    p = {'prefix': prefix, 'max-keys': max_keys}
    if marker:
        p['marker'] = marker
    r = await _get(fetch, S3, p)
    if r['status'] != 200:
        return None, False, r
    keys, trunc = _s3_keys(r['body'])
    return [k for k in keys if k.endswith('.zip')], trunc, r


def _zip_first_lines(body: bytes, n=3):
    z = zipfile.ZipFile(io.BytesIO(body))
    name = z.namelist()[0]
    raw = z.read(name).decode('utf-8', 'ignore').splitlines()
    return name, raw[:n], len(raw)


async def probe_archive(fetch):
    out = {'reach': None, 'klines': {}, 'metrics': {}}
    r = await _get(fetch, ARCH + '/data/futures/um/monthly/klines/BTCUSDT/1h/BTCUSDT-1h-2024-01.zip', None, 'HEAD')
    out['reach'] = {'status': r['status'], 'error': r['error'], 'size': r['headers'].get('content-length')}
    if r['status'] is None:
        out['blocked'] = True
        return out
    for tf in ('15m', '1h'):
        info = {}
        keys, trunc, rr = await _s3_list(fetch, f'data/futures/um/monthly/klines/{SYMBOL}/{tf}/')
        if keys:
            info['n_files'] = f'≥{len(keys)}' if trunc else len(keys)
            info['first'] = keys[0].rsplit('-', 2)[-2] + '-' + keys[0].rsplit('-', 2)[-1].replace('.zip', '')
            info['last'] = keys[-1].rsplit('-', 2)[-2] + '-' + keys[-1].rsplit('-', 2)[-1].replace('.zip', '')
        else:
            info['list_note'] = 'листинг недоступен: ' + (rr['error'] or f'HTTP {rr["status"]}')
        url = ARCH + f'/data/futures/um/monthly/klines/{SYMBOL}/{tf}/{SYMBOL}-{tf}-2024-01.zip'
        g = await _get(fetch, url)
        info['sample_status'] = g['status']
        if g['status'] == 200:
            try:
                name, lines, nrows = _zip_first_lines(g['body'], 2)
                first = lines[0].split(',') if lines else []
                info['sample_size'] = len(g['body']); info['sample_rows'] = nrows
                info['header'] = lines[0] if lines and not lines[0][:1].isdigit() else ''
                data_line = next((l for l in lines if l[:1].isdigit()), '')
                info['columns'] = len(data_line.split(',')) if data_line else len(first)
                info['taker_buy'] = ('taker_buy_volume' in (lines[0] if lines else '')) or info['columns'] >= 11
            except Exception as e:
                info['parse_error'] = f'{type(e).__name__}: {str(e)[:100]}'
        else:
            info['sample_note'] = g['error']
        out['klines'][tf] = info
    # metrics (дневные файлы)
    keys, trunc, rr = await _s3_list(fetch, f'data/futures/um/daily/metrics/{SYMBOL}/')
    m = {}
    if keys:
        m['first'] = re.search(r'(\d{4}-\d{2}-\d{2})', keys[0]).group(1)
        m['page_files'] = len(keys); m['truncated'] = trunc
        tail, _t, _r = await _s3_list(fetch, f'data/futures/um/daily/metrics/{SYMBOL}/', marker=f'data/futures/um/daily/metrics/{SYMBOL}/{SYMBOL}-metrics-2026-09-')
        if tail:
            m['last'] = re.search(r'(\d{4}-\d{2}-\d{2})', tail[-1]).group(1)
        sample_date = '2024-01-02'
    else:
        m['list_note'] = 'листинг недоступен: ' + (rr['error'] or f'HTTP {rr["status"]}'); sample_date = '2024-01-02'
    g = await _get(fetch, ARCH + f'/data/futures/um/daily/metrics/{SYMBOL}/{SYMBOL}-metrics-{sample_date}.zip')
    m['sample_status'] = g['status']
    if g['status'] == 200:
        try:
            name, lines, nrows = _zip_first_lines(g['body'], 3)
            m['sample_size'] = len(g['body']); m['rows_per_day'] = nrows - 1 if lines and not lines[0][:1].isdigit() else nrows
            m['header'] = lines[0] if lines else ''
            ts = [l.split(',')[0] for l in lines[1:3]]
            if len(ts) == 2:
                t0 = datetime.strptime(ts[0], '%Y-%m-%d %H:%M:%S'); t1 = datetime.strptime(ts[1], '%Y-%m-%d %H:%M:%S')
                m['step_min'] = (t1 - t0).total_seconds() / 60
        except Exception as e:
            m['parse_error'] = f'{type(e).__name__}: {str(e)[:100]}'
    else:
        m['sample_note'] = g['error']
    out['metrics'] = m
    return out


# ── 2.4 BingX ───────────────────────────────────────────────────────────────────────────
async def probe_bingx(fetch, exchange=None):
    out = {}
    r = await _get(fetch, BINGX + '/openApi/swap/v3/quote/klines', {'symbol': 'BTC-USDT', 'interval': '15m', 'limit': 2})
    js = _js(r); out['klines_status'] = r['status']
    data = (js or {}).get('data') if isinstance(js, dict) else None
    if isinstance(data, list) and data and isinstance(data[0], dict):
        out['kline_keys'] = sorted(data[0].keys())
        out['taker_buy'] = any('taker' in k.lower() or 'buy' in k.lower() for k in data[0])
    else:
        out['note'] = r['error'] or (r['body'][:120].decode('utf-8', 'ignore') if r['body'] else '')
    r = await _get(fetch, BINGX + '/openApi/swap/v2/quote/openInterest', {'symbol': 'BTC-USDT'})
    js = _js(r); out['oi_status'] = r['status']
    d = (js or {}).get('data') if isinstance(js, dict) else None
    out['oi_current_only'] = isinstance(d, dict)               # один объект с текущим значением — не история
    if exchange is not None:
        h = getattr(exchange, 'has', {}) or {}
        out['ccxt'] = {k: h.get(k) for k in ('fetchOpenInterest', 'fetchOpenInterestHistory', 'fetchFundingRateHistory')}
    return out


# ── 2.5 оценка объёма ───────────────────────────────────────────────────────────────────
def estimate(res, n_symbols, now_ms, pause_s, disk_path):
    """Оценка объёма загрузки для n_symbols по метаданным проб: запросы/файлы, время при паузе, место в БД, свободное место."""
    est = {}
    fapi, arc = res.get('fapi') or {}, res.get('archive') or {}
    first15 = ((fapi.get('klines') or {}).get('15m') or {}).get('first_ts')
    if first15:
        bars15 = (now_ms - first15) / (15 * 60_000)
        lim = ((fapi.get('klines') or {}).get('15m') or {}).get('limit_n') or 1500
        req = math.ceil(bars15 / lim) * n_symbols
        est['rest_15m'] = {'bars_per_sym': int(bars15), 'requests': req, 'minutes': req * pause_s / 60, 'db_mb': bars15 * n_symbols * KLINE_ROW_BYTES / 1e6}
        bars1h = bars15 / 4
        req1 = math.ceil(bars1h / lim) * n_symbols
        est['rest_1h'] = {'bars_per_sym': int(bars1h), 'requests': req1, 'minutes': req1 * pause_s / 60, 'db_mb': bars1h * n_symbols * KLINE_ROW_BYTES / 1e6}
    k15 = (arc.get('klines') or {}).get('15m') or {}
    if k15.get('first'):
        months = (datetime.fromtimestamp(now_ms / 1000, timezone.utc).year - int(k15['first'][:4])) * 12 + datetime.fromtimestamp(now_ms / 1000, timezone.utc).month - int(k15['first'][5:7]) + 1
        files = months * n_symbols
        mb = (k15.get('sample_size') or 0) * files / 1e6
        est['arch_15m'] = {'files': files, 'minutes': files * pause_s / 60, 'download_mb': mb}
    mt = arc.get('metrics') or {}
    if mt.get('first'):
        days = (datetime.fromtimestamp(now_ms / 1000, timezone.utc) - datetime.strptime(mt['first'], '%Y-%m-%d').replace(tzinfo=timezone.utc)).days
        files = days * n_symbols
        rows_day = mt.get('rows_per_day') or 288
        est['arch_metrics'] = {'files': files, 'minutes': files * pause_s / 60, 'download_mb': (mt.get('sample_size') or 0) * files / 1e6,
                               'db_mb_full': files * rows_day * OI_ROW_BYTES / 1e6, 'db_mb_hourly': files * 24 * OI_ROW_BYTES / 1e6}
    try:
        du = shutil.disk_usage(disk_path)
        est['disk'] = {'path': disk_path, 'free_mb': du.free / 1e6, 'total_mb': du.total / 1e6}
    except Exception as e:
        est['disk'] = {'path': disk_path, 'error': f'{type(e).__name__}: {e}'}
    return est


# ── 2.6 вывод ───────────────────────────────────────────────────────────────────────────
def conclude(res):
    """Что проверяемо на истории. H-OF1: нужны свечи с taker buy volume (REST или архив). H-OF2: ещё и история OI; если до конца IS
    (2024-12-31) её меньше IS_MIN_DAYS дней — «непроверяема на истории» (правила гипотезы не меняются)."""
    fapi, arc = res.get('fapi') or {}, res.get('archive') or {}
    out = {}
    blocked_fapi = bool(fapi.get('blocked')) or 'error' in fapi
    blocked_arc = bool(arc.get('blocked')) or 'error' in arc
    k15 = (fapi.get('klines') or {}).get('15m') or {}
    a15 = (arc.get('klines') or {}).get('15m') or {}
    taker = bool(k15.get('taker_buy')) or bool(a15.get('taker_buy'))
    start15 = k15.get('first_ts')
    if not taker:
        out['H-OF1'] = 'НЕ проверяема: taker buy volume в свечах не подтверждён' + (' (fapi и архив недоступны с сервера)' if blocked_fapi and blocked_arc else '')
    else:
        src = []
        if k15.get('taker_buy'):
            src.append('REST fapi' + (f' с {_d(start15)}' if start15 else ''))
        if a15.get('taker_buy'):
            src.append('архив' + (f' с {a15.get("first")}' if a15.get('first') else ''))
        out['H-OF1'] = 'проверяема на истории: 15m + taker buy volume — ' + ', '.join(src)
    oi_first = None; oi_src = ''
    for per, info in ((fapi.get('oi') or {}).items()):
        if info.get('old_n'):
            oi_first = int(datetime(2022, 1, 1, tzinfo=timezone.utc).timestamp() * 1000); oi_src = f'REST openInterestHist ({per}) отдаёт данные 2022-01-01'
    mt = arc.get('metrics') or {}
    if mt.get('first'):
        d_arc = int(datetime.strptime(mt['first'], '%Y-%m-%d').replace(tzinfo=timezone.utc).timestamp() * 1000)
        if oi_first is None or d_arc < oi_first:
            oi_first = d_arc; oi_src = f'архив metrics с {mt["first"]} (шаг {mt.get("step_min")} мин)'
    if oi_first is None:
        out['H-OF2'] = 'НЕПРОВЕРЯЕМА на истории: история открытого интереса недоступна с сервера (REST отдаёт лишь недавние данные, архив недоступен/пуст)'
    else:
        is_days = (datetime.strptime(IS_END, '%Y-%m-%d').replace(tzinfo=timezone.utc).timestamp() * 1000 - oi_first) / 86_400_000
        if is_days >= IS_MIN_DAYS:
            out['H-OF2'] = f'проверяема на истории с {_d(oi_first)}: {oi_src}; IS ≈ {is_days:.0f} дн (≥ {IS_MIN_DAYS})'
        else:
            out['H-OF2'] = f'НЕПРОВЕРЯЕМА на истории: IS ≈ {max(is_days, 0):.0f} дн &lt; {IS_MIN_DAYS} ({oi_src})'
    return out


# ── отчёт ───────────────────────────────────────────────────────────────────────────────
def universe_from_db(db_path):
    """Замороженная вселенная H-XS (16 символов) из htf_xs_universe — для переноса в BOT_SPEC §2.32.1 без изменений."""
    try:
        con = sqlite3.connect(db_path)
        rows = con.execute("SELECT sym FROM htf_xs_universe ORDER BY sym").fetchall()
        con.close()
        return [r[0] for r in rows]
    except Exception:
        return []


def build_report(res, est, concl, universe, now_ms) -> str:
    L = ['📡 <b>/data_probe: доступность данных для H-OF1/H-OF2 (только метаданные, бэктеста нет)</b>',
         f'Время проверки: {datetime.fromtimestamp(now_ms / 1000, timezone.utc):%Y-%m-%d %H:%M} UTC; запросы шли с сервера бота.']
    L.append('Вселенная H-XS (16, зафиксирована; перенести в BOT_SPEC §2.32.1 без изменений): ' + (', '.join(s.split('/')[0] for s in universe) if universe else 'список не заморожен (нужен /htf_xsmom)'))
    f = res.get('fapi') or {}
    L.append('\n<b>2.1 Binance USDT-M REST (fapi)</b>')
    if 'error' in f:
        L.append(f'  ошибка проверки: {f["error"]}')
    else:
        p = f.get('ping') or {}
        L.append(f'  ping: HTTP {p.get("status")}' + (f' ({p.get("error")})' if p.get('error') else '') + (' — ДОСТУП ЗАКРЫТ с сервера (451/403 = гео-блок), остальные проверки fapi пропущены' if f.get('blocked') else ''))
        for tf, k in (f.get('klines') or {}).items():
            L.append(f'  свечи {tf} {SYMBOL}: HTTP {k.get("status")}; поля в строке: {k.get("fields", "?")}; taker buy volume: {"ДА" if k.get("taker_buy") else "нет/не подтверждено"}; '
                     f'первая свеча: {_d(k.get("first_ts"))}; limit=1500 → {k.get("limit_n")} строк (HTTP {k.get("limit_status")}), used-weight-1m={k.get("weight_1m")}'
                     + (f'; {k["note"]}' if k.get('note') else ''))
    L.append('\n<b>2.3 Открытый интерес через REST (openInterestHist)</b>')
    for per, o in ((f.get('oi') or {}).items()):
        L.append(f'  период {per}: HTTP {o.get("status")}; строк {o.get("n")}; с {_d(o.get("first_ts"))} по {_d(o.get("last_ts"))}; шаг {o.get("step_min")} мин; поля {o.get("fields")}; '
                 f'запрос за 2022-01-01: HTTP {o.get("old_status")}, строк {o.get("old_n")}' + (f' ({o["old_note"]})' if o.get('old_note') else '') + (f'; {o["note"]}' if o.get('note') else ''))
    ei = f.get('exchange_info')
    if isinstance(ei, dict) and ei and 'error' not in ei:
        L.append('  символы вселенной на Binance USDT-M (onboardDate):')
        cells = []
        for s in universe:
            b = s.split('/')[0] + 'USDT'
            e = ei.get(b)
            cells.append(f'{b}: ' + (f'{_d(e["onboard"])}' + ('' if e.get('status') == 'TRADING' else f' [{e.get("status")}]') if e else 'НЕТ'))
        L.append('   ' + '; '.join(cells))
    elif isinstance(ei, dict) and ei.get('error'):
        L.append(f'  exchangeInfo: {ei["error"]}')
    a = res.get('archive') or {}
    L.append('\n<b>2.2 Архив data.binance.vision</b>')
    if 'error' in a:
        L.append(f'  ошибка проверки: {a["error"]}')
    else:
        rc = a.get('reach') or {}
        L.append(f'  доступность (HEAD 1h-файла): HTTP {rc.get("status")}' + (f' ({rc.get("error")})' if rc.get('error') else '') + (f', размер {rc.get("size")} Б' if rc.get('size') else '') + (' — НЕДОСТУПЕН' if a.get('blocked') else ''))
        for tf, k in (a.get('klines') or {}).items():
            L.append(f'  месячные свечи {tf}: файлов {k.get("n_files", "?")}, с {k.get("first", "?")} по {k.get("last", "?")}; образец 2024-01: HTTP {k.get("sample_status")}, '
                     f'{k.get("sample_size", "?")} Б, строк {k.get("sample_rows", "?")}, колонок {k.get("columns", "?")}, заголовок: {"да" if k.get("header") else "нет"}, taker buy volume: {"ДА" if k.get("taker_buy") else "нет/не подтверждено"}'
                     + (f'; {k["list_note"]}' if k.get('list_note') else '') + (f'; {k["parse_error"]}' if k.get('parse_error') else ''))
        m = a.get('metrics') or {}
        L.append(f'  дневные metrics: с {m.get("first", "?")} по {m.get("last", "?")}; образец: HTTP {m.get("sample_status")}, {m.get("sample_size", "?")} Б, строк/день {m.get("rows_per_day", "?")}, шаг {m.get("step_min", "?")} мин')
        if m.get('header'):
            L.append(f'    поля: {m["header"]}')
        for kk in ('list_note', 'parse_error', 'sample_note'):
            if m.get(kk):
                L.append(f'    {m[kk]}')
    b = res.get('bingx') or {}
    L.append('\n<b>2.4 BingX</b>')
    if 'error' in b:
        L.append(f'  ошибка проверки: {b["error"]}')
    else:
        L.append(f'  свечи swap v3: HTTP {b.get("klines_status")}; ключи: {b.get("kline_keys", "?")}; taker buy volume: {"ДА" if b.get("taker_buy") else "НЕТ"}')
        L.append(f'  открытый интерес: HTTP {b.get("oi_status")}; ответ — {"только текущее значение (истории нет)" if b.get("oi_current_only") else "не подтверждён"}; ccxt has: {b.get("ccxt")}')
    L.append('\n<b>2.5 Оценка объёма загрузки (16 символов)</b>')
    for key, name in (('rest_15m', 'REST 15m'), ('rest_1h', 'REST 1h'), ('arch_15m', 'архив 15m (месячные)'), ('arch_metrics', 'архив metrics (дневные)')):
        e = est.get(key)
        if e:
            L.append(f'  {name}: ' + ', '.join(f'{k} {v:,.0f}' if isinstance(v, (int, float)) else f'{k} {v}' for k, v in e.items()))
    d = est.get('disk') or {}
    L.append(f'  диск ({d.get("path")}): ' + (f'свободно {d.get("free_mb", 0):,.0f} МБ из {d.get("total_mb", 0):,.0f}' if 'free_mb' in d else d.get('error', '?')) + '; оценки — по измеренным размерам образцов и грубым 100 Б/строка свечи, 70 Б/строка OI в SQLite')
    L.append('\n<b>2.6 Вывод</b>')
    for k, v in concl.items():
        L.append(f' ВЕРДИКТ ДОСТУПНОСТИ {k}: {v}')
    L.append('Ни одна строка данных из проверок не использована для сигналов; правила H-OF1/H-OF2 (BOT_SPEC §2.34) не менялись. Бэктест — отдельный деплой после этого отчёта.')
    return '\n'.join(L)


async def run_probe(db_path, exchange=None, fetch=None, now_ms=None, disk_path=None, progress=None):
    """/data_probe: все проверки изолированы; исключения наружу не выходят. → текст отчёта."""
    now_ms = now_ms or int(time.time() * 1000)
    own = None
    res = {}
    try:
        if fetch is None:
            import aiohttp
            own = aiohttp.ClientSession()
            fetch = make_fetcher(own)
        for name, coro in (('fapi', probe_fapi(fetch)), ('archive', probe_archive(fetch)), ('bingx', probe_bingx(fetch, exchange))):
            await _safe(name, coro, res)
            if progress:
                try:
                    await progress(f'/data_probe: {name} — {"ошибка" if "error" in (res.get(name) or {}) else "готово"}')
                except Exception:
                    pass
    finally:
        if own is not None:
            await own.close()
    universe = universe_from_db(db_path)
    try:
        est = estimate(res, max(len(universe), 16), now_ms, _pause(), disk_path or ('/data' if os.path.exists('/data') else '.'))
    except Exception as e:
        est = {'disk': {'path': '?', 'error': f'{type(e).__name__}: {e}'}}
    try:
        concl = conclude(res)
    except Exception as e:
        concl = {'H-OF1': f'ошибка вывода: {type(e).__name__}', 'H-OF2': f'ошибка вывода: {type(e).__name__}'}
    return build_report(res, est, concl, universe, now_ms), res, est, concl
