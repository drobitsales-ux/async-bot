"""research_probe.py — /data_probe: проверка ДОСТУПНОСТИ внешних данных (Binance USDT-M, data.binance.vision, BingX) — BOT_SPEC §2.34.

ТОЛЬКО МЕТАДАННЫЕ: коды ответов, наличие полей, даты первых записей, лимиты, размеры файлов, шаг строк, объём/время загрузки, свободное место.
Ни одна строка данных из проверок не используется для расчёта сигналов в этом деплое; бэктеста H-OF1/H-OF2 в модуле НЕТ.
Запросы идут с сервера бота (не из песочницы); каждая проверка изолирована: исключения наружу не выходят — в отчёте статус/текст ошибки.
HTTP вынесен в передаваемый fetch(url, params, method) → dict(status, headers, body, error), чтобы тесты подставляли поддельные ответы."""
import json
import math
import os
import shutil
import sqlite3
import time
from datetime import datetime, timezone

import research_htf as H
import research_vision as V

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


make_fetcher = V.make_fetcher      # [v92] полное чтение тела (resp.read()); v91 читал resp.content.read(N) — усечённые zip/листинг (BadZipFile)


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


# ── 2.2 архив data.binance.vision (v2: через загрузчик research_vision) ──────────────────────
def _pick(keys, want):
    """Ключ с датой want (YYYY-MM[-DD]) или ближайший к середине списка."""
    for k in keys:
        if V.key_date(k) == want:
            return k
    return keys[len(keys) // 2] if keys else None


async def probe_archive(fetch, universe=None):
    """v2: HEAD-доступность; ПОЛНЫЙ листинг (все страницы) свечей 15m/1h и дневных metrics по BTCUSDT и числу файлов на каждый символ вселенной;
    ОДИН месячный файл 15m, ОДИН 1h и ОДИН дневной metrics BTCUSDT реально скачиваются (проверка zip-сигнатуры и sha256) и РАЗБИРАЮТСЯ:
    фактические колонки, taker_buy_volume, единицы времени, шаг metrics, поля. Ничего не сохраняется."""
    out = {'reach': None, 'klines': {}, 'metrics': {}, 'per_symbol': {}}
    r = await _get(fetch, V.ARCH + '/' + V.kline_key(SYMBOL, '1h', 2024, 1), None, 'HEAD')
    out['reach'] = {'status': r['status'], 'error': r['error'], 'size': r['headers'].get('content-length'), 'content_type': r['headers'].get('content-type')}
    if r['status'] is None:
        out['blocked'] = True
        return out
    listed = {}
    for tf in ('15m', '1h'):
        info = {}
        try:
            keys, pages = await V.list_keys(fetch, f'data/futures/um/monthly/klines/{SYMBOL}/{tf}/')
            listed[tf] = keys
            info.update(n_files=len(keys), pages=pages, first=V.key_date(keys[0]) if keys else None, last=V.key_date(keys[-1]) if keys else None)
        except V.VisionError as e:
            info['list_error'] = str(e)[:200]
        k = _pick(listed.get(tf, []), '2024-01')
        if k:
            try:
                body = await V.download_zip(fetch, k)
                p = V.parse_klines_csv(V.unzip_text(body))
                info.update(sample=k.rsplit('/', 1)[-1], sample_size=len(body), sample_rows=len(p['ts']), columns=p['cols'], header=p['header'], unit=p['unit'],
                            taker_buy=bool(p['cols'] >= 10 and float(p['tbv'].sum()) > 0), first_ts=int(p['ts'][0]), last_ts=int(p['ts'][-1]))
            except Exception as e:
                info['sample_error'] = f'{type(e).__name__}: {str(e)[:300]}'
        out['klines'][tf] = info
    m = {}
    try:
        keys, pages = await V.list_keys(fetch, f'data/futures/um/daily/metrics/{SYMBOL}/')
        m.update(n_files=len(keys), pages=pages, first=V.key_date(keys[0]) if keys else None, last=V.key_date(keys[-1]) if keys else None)
        k = _pick(keys, '2024-01-02')
        if k:
            body = await V.download_zip(fetch, k)
            p = V.parse_metrics_csv(V.unzip_text(body))
            m.update(sample=k.rsplit('/', 1)[-1], sample_size=len(body), rows_per_day=len(p['ts']), step_min=p['step_min'], fields=p['fields'], oi_ok=bool(len(p['oi'])))
    except Exception as e:
        m['error'] = f'{type(e).__name__}: {str(e)[:300]}'
    out['metrics'] = m
    # число файлов по всем символам вселенной (листинг; без скачивания)
    for sym in (universe or []):
        b = sym.split('/')[0] + 'USDT'
        row = {}
        for tf in ('15m', '1h'):
            try:
                ks, _p = await V.list_keys(fetch, f'data/futures/um/monthly/klines/{b}/{tf}/')
                row[tf] = (len(ks), V.key_date(ks[0]) if ks else None)
            except Exception as e:
                row[tf] = (None, f'{type(e).__name__}')
        try:
            ks, _p = await V.list_keys(fetch, f'data/futures/um/daily/metrics/{b}/')
            row['metrics'] = (len(ks), V.key_date(ks[0]) if ks else None)
        except Exception as e:
            row['metrics'] = (None, f'{type(e).__name__}')
        out['per_symbol'][b] = row
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
    ps = arc.get('per_symbol') or {}
    k15 = (arc.get('klines') or {}).get('15m') or {}
    if ps:
        n15 = sum((r.get('15m') or (0,))[0] or 0 for r in ps.values()); n1h = sum((r.get('1h') or (0,))[0] or 0 for r in ps.values()); nm = sum((r.get('metrics') or (0,))[0] or 0 for r in ps.values())
        rows_day = (arc.get('metrics') or {}).get('rows_per_day') or 288
        est['arch_15m'] = {'files_15m': n15, 'files_1h': n1h, 'minutes': (n15 + n1h) * (pause_s + 0.5) / 60 / V.concurrency(), 'download_mb': (k15.get('sample_size') or 0) * (n15 + n1h) / 1e6}
        est['arch_metrics'] = {'files': nm, 'minutes': nm * (pause_s + 0.5) / 60 / V.concurrency(), 'download_mb': ((arc.get('metrics') or {}).get('sample_size') or 0) * nm / 1e6,
                               'db_mb_hourly_npz': nm * 24 * OI_ROW_BYTES / 1e6 / 4, 'raw_rows_if_stored': nm * rows_day}
    try:
        du = shutil.disk_usage(disk_path)
        est['disk'] = {'path': disk_path, 'free_mb': du.free / 1e6, 'total_mb': du.total / 1e6}
    except Exception as e:
        est['disk'] = {'path': disk_path, 'error': f'{type(e).__name__}: {e}'}
    return est


# ── 2.6 вывод ───────────────────────────────────────────────────────────────────────────
def conclude(res):
    """Вердикты доступности — по ФАКТИЧЕСКИ РАЗОБРАННЫМ файлам архива (fapi геоблокирован с сервера и в вердикте не участвует).
    H-OF1: разобран месячный файл 15m с колонками taker buy (суммарный taker_buy_volume > 0). H-OF2: ещё разобран 1h-файл и дневной metrics
    с полем OI; если до конца IS (2024-12-31) истории OI меньше IS_MIN_DAYS дней — «непроверяема на истории» (правила гипотезы не меняются).
    Строки, начинающиеся с «проверяема», — единственный признак для разрешения /of_backtest."""
    arc = res.get('archive') or {}
    k15, k1h, mt = (arc.get('klines') or {}).get('15m') or {}, (arc.get('klines') or {}).get('1h') or {}, arc.get('metrics') or {}
    out = {}
    if 'error' in arc or arc.get('blocked'):
        why = arc.get('error') or (arc.get('reach') or {}).get('error') or 'архив недоступен с сервера'
        return {'H-OF1': f'НЕ проверяема: архив недоступен ({why})', 'H-OF2': f'НЕПРОВЕРЯЕМА на истории: архив недоступен ({why})'}
    if k15.get('taker_buy') and k15.get('n_files'):
        out['H-OF1'] = (f'проверяема на истории: разобран файл {k15.get("sample")} ({k15.get("sample_rows")} строк, колонок {k15.get("columns")}, '
                        f'taker buy volume есть); 15m-файлы BTCUSDT с {k15.get("first")} по {k15.get("last")} ({k15.get("n_files")})')
    else:
        out['H-OF1'] = 'НЕ проверяема: ' + (k15.get('sample_error') or k15.get('list_error') or 'taker buy volume в разобранном файле 15m не подтверждён')
    h1_ok = bool(k1h.get('taker_buy') and k1h.get('n_files'))
    if not (mt.get('oi_ok') and mt.get('first')):
        out['H-OF2'] = 'НЕПРОВЕРЯЕМА на истории: ' + (mt.get('error') or 'дневной metrics с полем OI не разобран')
    elif not h1_ok:
        out['H-OF2'] = 'НЕПРОВЕРЯЕМА на истории: файл 1h не разобран (' + (k1h.get('sample_error') or k1h.get('list_error') or 'нет taker buy') + ')'
    else:
        first = int(datetime.strptime(mt['first'], '%Y-%m-%d').replace(tzinfo=timezone.utc).timestamp() * 1000)
        is_days = (datetime.strptime(IS_END, '%Y-%m-%d').replace(tzinfo=timezone.utc).timestamp() * 1000 - first) / 86_400_000
        base = f'metrics BTCUSDT с {mt["first"]} по {mt.get("last")} ({mt.get("n_files")} файлов), шаг {mt.get("step_min")} мин, разобран {mt.get("sample")}; IS ≈ {is_days:.0f} дн'
        out['H-OF2'] = (f'проверяема на истории с {mt["first"]}: {base} (≥ {IS_MIN_DAYS})' if is_days >= IS_MIN_DAYS
                        else f'НЕПРОВЕРЯЕМА на истории: {base} &lt; {IS_MIN_DAYS}')
    return out


def verdict_ok(text) -> bool:
    return str(text).startswith('проверяема')


def save_status(db_path, version, concl):
    """Результат последнего /data_probe — для разрешения /of_backtest (таблица of_probe_status)."""
    con = sqlite3.connect(db_path)
    con.execute("CREATE TABLE IF NOT EXISTS of_probe_status (id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT, version TEXT, h_of1 TEXT, h_of2 TEXT, ok1 INTEGER, ok2 INTEGER)")
    con.execute("INSERT INTO of_probe_status (ts, version, h_of1, h_of2, ok1, ok2) VALUES (?,?,?,?,?,?)",
                (datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'), version, concl.get('H-OF1', ''), concl.get('H-OF2', ''),
                 int(verdict_ok(concl.get('H-OF1', ''))), int(verdict_ok(concl.get('H-OF2', '')))))
    con.commit(); con.close()


def last_status(db_path):
    """→ dict(ts, version, h_of1, h_of2, ok1, ok2) последнего /data_probe или None."""
    try:
        con = sqlite3.connect(db_path)
        row = con.execute("SELECT ts, version, h_of1, h_of2, ok1, ok2 FROM of_probe_status ORDER BY id DESC LIMIT 1").fetchone()
        con.close()
    except sqlite3.Error:
        return None
    return dict(zip(('ts', 'version', 'h_of1', 'h_of2', 'ok1', 'ok2'), row)) if row else None


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
    L.append('\n<b>2.2 Архив data.binance.vision (v2: загрузчик с полным чтением тела, проверкой zip и sha256)</b>')
    if 'error' in a:
        L.append(f'  ошибка проверки: {a["error"]}')
    else:
        rc = a.get('reach') or {}
        L.append(f'  доступность (HEAD 1h-файла): HTTP {rc.get("status")}' + (f' ({rc.get("error")})' if rc.get('error') else '') + (f', размер {rc.get("size")} Б' if rc.get('size') else '') + (' — НЕДОСТУПЕН' if a.get('blocked') else ''))
        for tf, k in (a.get('klines') or {}).items():
            L.append(f'  месячные свечи {tf} BTCUSDT: файлов {k.get("n_files", "?")} (страниц листинга {k.get("pages", "?")}), с {k.get("first", "?")} по {k.get("last", "?")}'
                     + (f'; {k["list_error"]}' if k.get('list_error') else ''))
            if k.get('sample'):
                L.append(f'    разобран {k["sample"]}: {k.get("sample_size")} Б, строк {k.get("sample_rows")}, колонок {k.get("columns")}, заголовок {"да" if k.get("header") else "нет"}, '
                         f'время {k.get("unit")}, {_d(k.get("first_ts"))}…{_d(k.get("last_ts"))}, taker_buy_volume: {"ДА" if k.get("taker_buy") else "НЕТ"}')
            if k.get('sample_error'):
                L.append(f'    образец не разобран: {k["sample_error"]}')
        m = a.get('metrics') or {}
        L.append(f'  дневные metrics BTCUSDT: файлов {m.get("n_files", "?")} (страниц листинга {m.get("pages", "?")}), с {m.get("first", "?")} по {m.get("last", "?")}')
        if m.get('sample'):
            L.append(f'    разобран {m["sample"]}: {m.get("sample_size")} Б, строк/день {m.get("rows_per_day")}, шаг {m.get("step_min")} мин, поля: {m.get("fields")}')
        if m.get('error'):
            L.append(f'    ошибка: {m["error"]}')
        if a.get('per_symbol'):
            L.append('  файлов в архиве по символам (15m / 1h / metrics; первая дата):')
            for b_, row in a['per_symbol'].items():
                L.append('   ' + b_ + ': ' + ' | '.join(f'{k_} {v[0] if v[0] is not None else "ошибка"} ({v[1]})' for k_, v in row.items()))
    b = res.get('bingx') or {}
    L.append('\n<b>2.4 BingX</b>')
    if 'error' in b:
        L.append(f'  ошибка проверки: {b["error"]}')
    else:
        L.append(f'  свечи swap v3: HTTP {b.get("klines_status")}; ключи: {b.get("kline_keys", "?")}; taker buy volume: {"ДА" if b.get("taker_buy") else "НЕТ"}')
        L.append(f'  открытый интерес: HTTP {b.get("oi_status")}; ответ — {"только текущее значение (истории нет)" if b.get("oi_current_only") else "не подтверждён"}; ccxt has: {b.get("ccxt")}')
    L.append('\n<b>2.5 Оценка объёма загрузки (16 символов)</b>')
    for key, name in (('arch_15m', 'архив 15m (месячные)'), ('arch_metrics', 'архив metrics (дневные)')):
        e = est.get(key)
        if e:
            L.append(f'  {name}: ' + ', '.join(f'{k} {v:,.0f}' if isinstance(v, (int, float)) else f'{k} {v}' for k, v in e.items()))
    d = est.get('disk') or {}
    L.append(f'  диск ({d.get("path")}): ' + (f'свободно {d.get("free_mb", 0):,.0f} МБ из {d.get("total_mb", 0):,.0f}' if 'free_mb' in d else d.get('error', '?')) + '; оценки — по измеренным размерам образцов и грубым 100 Б/строка свечи, 70 Б/строка OI в SQLite')
    L.append('\n<b>2.6 Вывод</b>')
    for k, v in concl.items():
        L.append(f' ВЕРДИКТ ДОСТУПНОСТИ {k}: {v}')
    if (res.get('fapi') or {}).get('blocked'):
        L.append('fapi закрыт гео-блоком региона сервера (HTTP 451) — для H-OF1/H-OF2 это не критично: taker buy и OI берутся из архива (доступен); смена региона не требуется.')
    L.append('Ни одна строка данных из проверок не использована для сигналов; правила H-OF1/H-OF2 (BOT_SPEC §2.34) не менялись. Бэктест — /of_backtest (разрешается только при «проверяема» в этом отчёте).')
    return '\n'.join(L)


async def run_probe(db_path, exchange=None, fetch=None, now_ms=None, disk_path=None, progress=None, version=''):
    """/data_probe: все проверки изолированы; исключения наружу не выходят. → текст отчёта."""
    now_ms = now_ms or int(time.time() * 1000)
    own = None
    res = {}
    try:
        if fetch is None:
            import aiohttp
            own = aiohttp.ClientSession()
            fetch = make_fetcher(own)
        universe0 = universe_from_db(db_path)
        for name, coro in (('fapi', probe_fapi(fetch)), ('archive', probe_archive(fetch, universe0)), ('bingx', probe_bingx(fetch, exchange))):
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
    try:
        save_status(db_path, version, concl)
    except Exception:
        pass                               # статус не критичен для отчёта
    return build_report(res, est, concl, universe, now_ms), res, est, concl
