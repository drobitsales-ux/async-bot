"""research_xdata.py — данные Bybit и BingX для H2-репликации (BOT_SPEC §2.47) и межбиржевого арбитража фандинга X1 (§2.48): рынки USDT-перпетуалов (ccxt, ТОЛЬКО публичные методы), дневные
свечи и история фандинга с кэшем и повторами, правило вселенной (монеты, которых НИКОГДА не было в архиве Binance), панель в формате `research_bdata.load_panel`. Сеть — только в дочернем
процессе (research_runner); ни одного вызова создания ордеров."""
import asyncio
import collections.abc
import json
import os
import re
import time

import numpy as np

import research_bdata as B
import research_vision as V

DAY = 86_400_000
HOUR = 3_600_000
EXCHANGES = ('bybit', 'bingx', 'okx', 'gateio', 'mexc', 'bitget')
OLD_VENUES = ('bybit', 'bingx', 'okx')          # v129 (§2.59): gateio/mexc/bitget — новые биржи H2EXT (прямой REST, research_venues)
EXT_ALL = ('bingx', 'okx', 'gateio', 'mexc', 'bitget')
NEW_VENUES = ('gateio', 'mexc', 'bitget')
RETRY_DELAYS = (2, 6, 15)
LIQ_COARSE = 20_000_000.0               # грубый фильтр загрузки фандинга: максимум 30-дневного среднего оборота ≥ 20 млн USDT
FIRST30 = 30
SINCE_MS = int(np.datetime64('2019-01-01', 'ms').astype(np.int64))
MAX_FUND_PAGES = 400
PREFIX_RE = re.compile(r'^(1000000|100000|10000|1000|100|10|1M)(?=[A-Z])')


def xdata_dir():
    return os.getenv('XDATA_DIR') or ('/data/xdata' if os.path.exists('/data') else '/tmp/xdata')


def spec_line(key, path=None):
    """Последняя строка BOT_SPEC, начинающаяся с `key` → остаток строки | None. [v118] Маркеры условных поправок §2.50 (X1-MAIN-PAIR:, H2REPL-VENUES:)."""
    val = None
    try:
        with open(path or os.path.join(os.path.dirname(os.path.abspath(__file__)), 'BOT_SPEC.md'), encoding='utf-8') as f:
            for ln in f:
                if ln.startswith(key):
                    val = ln[len(key):].strip()
    except OSError:
        return None
    return val


def venues_from_spec(path=None):
    """H2-репликация: площадки вселенной по маркеру `H2REPL-VENUES: bingx,okx` (§2.51; допустимы bybit, bingx, okx; v119). Без маркера — исходный §2.47: Bybit и BingX."""
    v = (spec_line('H2REPL-VENUES:', path) or '').lower().replace(' ', '')
    got = tuple(x for x in v.split(',') if x in EXCHANGES)
    return got if got else ('bybit', 'bingx')


def excluded_venues_from_spec(path=None):
    """[v131, §2.61.4] Маркер `H2EXT-EXCLUDE: bitget <причина>` — биржи, исключённые из вселенной H2EXT/H2F правилом (ДО расчёта доходностей). → tuple (пусто, если маркера нет)."""
    out = []
    try:
        with open(path or os.path.join(os.path.dirname(os.path.abspath(__file__)), 'BOT_SPEC.md'), encoding='utf-8') as f:
            for ln in f:
                if ln.startswith('H2EXT-EXCLUDE:'):
                    w = ln[len('H2EXT-EXCLUDE:'):].strip().split()
                    if w and w[0].lower().strip(',') in EXT_ALL:
                        out.append(w[0].lower().strip(','))
    except OSError:
        pass
    return tuple(dict.fromkeys(out))


def ext_venues_from_spec(path=None):
    """H2EXT (BOT_SPEC §2.59.1): площадки по маркеру `H2EXT-VENUES: bingx,okx,<доступные новые>` → tuple | None (маркера нет — расчёт и загрузка отказывают)."""
    v = spec_line('H2EXT-VENUES:', path)
    if v is None:
        return None
    got = tuple(x for x in v.lower().replace(' ', '').split(',') if x in EXT_ALL)
    if not got:
        return None
    ex = excluded_venues_from_spec(path)
    got = tuple(x for x in got if x not in ex)
    return tuple(dict.fromkeys(('bingx', 'okx') + got))              # [v130, §2.59.10] BingX и OKX входят во вселенную всегда (из кэша §2.47); маркер перечисляет загружаемые новые биржи


def x1_pair_from_spec(path=None):
    """X1: основная пара. `X1-MAIN-PAIR: BingX/Binance` → 'binance'; иначе 'bybit' (исходный §2.48)."""
    v = (spec_line('X1-MAIN-PAIR:', path) or '').lower().replace(' ', '')
    return 'binance' if v.startswith('bingx/binance') else 'bybit'


def norm_base(base):
    """Нормализованная база: заглавные буквы без числового множителя (1000PEPE, 10000SATS, 1MBABYDOGE → PEPE, SATS, BABYDOGE)."""
    b = (base or '').upper().strip()
    return PREFIX_RE.sub('', b)


def binance_norm_set(bdir=None):
    """Нормализованные базы ВСЕХ символов индекса архива Binance (включая делистингованные и стейблкоин-подобные)."""
    idx = B.load_index(bdir or B.battery_dir())
    return {norm_base(s[:-4] if s.endswith('USDT') else s) for s in idx.get('symbols', [])}


def safe(sym):
    return re.sub(r'[^A-Za-z0-9]+', '_', sym)


def bars_path(d, ex, sym):
    return os.path.join(d, ex, safe(sym) + '.d1.npz')


def fund_path(d, ex, sym):
    return os.path.join(d, ex, safe(sym) + '.fr.npz')


def hbars_path(d, ex, sym):
    return os.path.join(d, ex, safe(sym) + '.h1.npz')


def index_path(d):
    return os.path.join(d, 'xindex.json')


JOURNAL = 'index.journal'
NEG_TTL = 7 * DAY                                         # подтверждённый «пусто / постоянная ошибка» не перезапрашивается 7 суток


def journal_path(d):
    return os.path.join(d, JOURNAL)


def _apply(idx, kind, ex, sym, rec):
    if kind in ('bars', 'fund', 'hbars'):
        idx[kind].setdefault(ex, {})[sym] = rec
    elif kind in ('neg', 'fail'):
        idx[kind][f'{ex}|{sym}'] = rec
    elif kind == 'h1classdel':
        idx['h1class'].pop(f'{ex}|{sym}', None)
    elif kind == 'migr':                                             # [v127] отметки разовых миграций индекса
        idx['migr'][f'{ex}|{sym}'] = rec
    elif kind in ('alias', 'h1class'):                               # [v125, §2.57] переименованные символы и результат классификации ошибок 1h
        idx[kind][f'{ex}|{sym}'] = rec
    elif kind == 'negdel':                                           # [v125] сброс отрицательной записи (повторный запрос символа)
        idx['neg'].pop(f'{ex}|{sym}', None)


def load_index(d):
    """Индекс = index.json (снимок) + журнал index.journal (по строке на каждый завершённый символ; v121, BOT_SPEC §2.53): завершённые символы переживают обрыв процесса."""
    try:
        with open(index_path(d), encoding='utf-8') as f:
            idx = json.load(f)
    except (OSError, ValueError):
        idx = {}
    for k in ('markets', 'bars', 'fund', 'hbars', 'fail', 'neg', 'alias', 'h1class', 'migr'):
        idx.setdefault(k, {ex: {} for ex in EXCHANGES} if k not in ('fail', 'neg', 'alias', 'h1class', 'migr') else {})
        if k not in ('fail', 'neg', 'alias', 'h1class', 'migr'):
            for ex in EXCHANGES:                                   # [v119] индекс прошлых версий без ключа новой площадки (okx)
                idx[k].setdefault(ex, {})
    idx.setdefault('t', 0)
    try:
        with open(journal_path(d), encoding='utf-8') as f:
            for ln in f:
                try:
                    kind, ex, sym, rec = json.loads(ln)
                except (ValueError, TypeError):
                    continue                                       # оборванная последняя строка — пропускается
                _apply(idx, kind, ex, sym, rec)
    except OSError:
        pass
    return idx


def put(d, idx, kind, ex, sym, rec):
    """Записывает результат по символу в память и СРАЗУ в журнал (одна строка, дозапись): обрыв процесса не теряет завершённые символы."""
    _apply(idx, kind, ex, sym, rec)
    os.makedirs(d, exist_ok=True)
    with open(journal_path(d), 'a', encoding='utf-8') as f:
        f.write(json.dumps([kind, ex, sym, rec], ensure_ascii=False) + '\n')
        f.flush()


def save_index(d, idx):
    """Снимок index.json (атомарно; имя временного файла уникально для процесса). Журнал не усекается (его компактирует compact_index в начале прогона)."""
    os.makedirs(d, exist_ok=True)
    path = index_path(d)
    tmp = f'{path}.{os.getpid()}.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(idx, f, ensure_ascii=False)
    os.replace(tmp, path)


def compact_index(d):
    """Сливает журнал в снимок и очищает журнал (вызывается в начале прогона, когда других загрузок нет — задачи исследований идут по одной)."""
    idx = load_index(d)
    save_index(d, idx)
    try:
        open(journal_path(d), 'w').close()
    except OSError:
        pass
    return idx


def migrate_v127(d, idx):
    """[v127, §2.58 п. 8] РАЗОВЫЙ сброс: отрицательные записи-ОШИБКИ (кроме «подтверждённо пусто/недоступны») по свечам, фандингу и 1h и записи классификации h1class — в «не обработано»
    (ошибки лимита запросов 100410 в v121–v126 записывались как постоянные). Отметка `migr` в журнале; повторно не выполняется. → число сброшенных записей (0 при повторе)."""
    if 'bingx|v127_reset' in idx['migr']:
        return 0
    n = 0
    for k, r in list(idx['neg'].items()):
        ex, sym, what = k.split('|')[0], '|'.join(k.split('|')[1:-1]), k.split('|')[-1]
        if r.get('why') not in ('empty', 'unavailable'):
            put(d, idx, 'negdel', ex, f'{sym}|{what}', None)
            n += 1
    for k in list(idx['h1class']):
        ex, sym = k.split('|')[0], '|'.join(k.split('|')[1:])
        idx['h1class'].pop(k, None)
        put(d, idx, 'h1classdel', ex, sym, None)
        n += 1
    put(d, idx, 'migr', 'bingx', 'v127_reset', {'t': int(time.time() * 1000), 'n': n})
    return n


def migrate_v131(d, idx):
    """[v131, §2.61.2] РАЗОВЫЙ сброс отрицательных записей-ОШИБОК Bitget по свечам и фандингу (дефект запроса v130 записан как «постоянная ошибка») в «не обработано»; «подтверждённо пусто» не трогаются."""
    if 'bitget|v131_reset' in idx['migr']:
        return 0
    n = 0
    for k, r in list(idx['neg'].items()):
        ex, sym, what = k.split('|')[0], '|'.join(k.split('|')[1:-1]), k.split('|')[-1]
        if ex == 'bitget' and r.get('why') not in ('empty', 'unavailable'):
            put(d, idx, 'negdel', ex, f'{sym}|{what}', None)
            n += 1
    put(d, idx, 'migr', 'bitget', 'v131_reset', {'t': int(time.time() * 1000), 'n': n})
    return n


def neg_rec(now_ms, e):
    """[v125] Отрицательная запись об ошибке символа: класс исключения, сообщение биржи (≤ 200 симв., публичный API — секретов нет), число успешных страниц и строк ДО ошибки
    (ошибка после успешных страниц — подозрение на «граница истории», сразу — «символ/запрос не принимается»)."""
    return {'t': now_ms, 'why': type(e).__name__, 'msg': ' '.join(str(e).split())[:200], 'pages': getattr(e, 'pages_ok', None), 'rows': getattr(e, 'rows_got', None)}


DEFECT_STOP = 10                                            # §2.61.2: у биржи в прогоне нет ни одного успеха и подряд 10 дефектов запроса — остальные символы биржи не запрашиваются


def defect_lines(idx, what, tripped=None):
    """[v131, §2.61.2] Строки отчёта: «дефект запроса: N (текст ответа)» по виду данных и биржам; остановленные биржи — отдельно."""
    groups = {}
    for k, v in idx['fail'].items():
        if k.endswith('|' + what) and str(v).startswith('ДЕФЕКТ ЗАПРОСА'):
            ex = k.split('|')[0]
            g = groups.setdefault((ex, str(v)[len('ДЕФЕКТ ЗАПРОСА: '):][:110]), {'n': 0, 'sample': k.split('|')[1]})
            g['n'] += 1
    out = [f'  дефект запроса ({what}) {ex}: {g["n"]} символов остаются «не обработано» (пример {g["sample"]}): «{msg}»' for (ex, msg), g in sorted(groups.items(), key=lambda kv: -kv[1]['n'])[:4]]
    for ex in (tripped or {}):
        out.append(f'  ⛔ {ex}: нет ни одного успеха и {DEFECT_STOP} дефектов запроса подряд — остальные символы биржи в этом прогоне не запрашивались (§2.61.2)')
    return out


def error_lines(idx, what='h1', syms=None, limit=6):
    """[v125] Сводка отрицательных записей-ошибок по виду данных (h1/bars/fund): группы по классу и началу сообщения, число, пример символа, страниц до ошибки.
    syms — ограничение [(биржа, символ)]. → список строк (пусто — ошибок нет)."""
    allow = None if syms is None else {f'{ex}|{s_}|{what}' for ex, s_ in syms}
    groups = {}
    for k, r in idx['neg'].items():
        if not k.endswith('|' + what) or r.get('why') == 'empty' or (allow is not None and k not in allow):
            continue
        g = groups.setdefault((r.get('why'), (r.get('msg') or '')[:90]), {'n': 0, 'sample': k.split('|')[1], 'p0': 0, 'pn': 0, 'rows': 0})
        g['n'] += 1
        if (r.get('pages') or 0) > 0:
            g['pn'] += 1
        else:
            g['p0'] += 1
        g['rows'] += int(r.get('rows') or 0)
    out = []
    for (why, msg), g in sorted(groups.items(), key=lambda kv: -kv[1]['n'])[:limit]:
        out.append(f'  ошибки {what}: {why} ×{g["n"]}: «{msg}» (пример: {g["sample"]}; ошибка на первой странице: {g["p0"]}, после успешных страниц: {g["pn"]}'
                   + (f', собрано строк до ошибки: {g["rows"]}' if g['pn'] else '') + ')')
    return out


def clear_neg_errors(d, idx, whats=('h1', 'bars', 'fund')):
    """[v125] Сбрасывает отрицательные записи-ОШИБКИ (не «подтверждённо пусто») — символы будут запрошены снова (иначе постоянные ошибки не перезапрашиваются 7 суток). → число сброшенных."""
    n = 0
    for k, r in list(idx['neg'].items()):
        ex, sym, what = k.split('|')[0], '|'.join(k.split('|')[1:-1]), k.split('|')[-1]
        if what in whats and r.get('why') != 'empty':
            put(d, idx, 'negdel', ex, f'{sym}|{what}', None)
            n += 1
    return n


def neg_active(idx, ex, sym, what, now_ms):
    r = idx['neg'].get(f'{ex}|{sym}|{what}')
    return bool(r and now_ms - int(r.get('t', 0)) < NEG_TTL)


TRANSIENT_CODES = ('100410',)                                  # BingX: «请稍后重试» — лимит запросов (по отчёту /xfund_1h_errors 2026-11-20, BOT_SPEC §2.58)
TRANSIENT_TEXT = re.compile(r'请稍后重试|稍后|rate.?limit|too many requests|try again later|\btimeout\b|\btimed out\b|\btime out\b', re.I)           # границы слов: «endTime out of range» — не таймаут
RATE_LIMIT_TEXT = re.compile(r'请稍后重试|稍后|rate.?limit|too many requests|try again later', re.I)
RATE_LIMIT_DELAYS = (3, 8, 20, 45)                              # §2.58 п. 7: повторы той же страницы на лимит запросов (100410)
_CODE_IN_TEXT = re.compile(r'"code"\s*:\s*"?(-?\d+)')


def is_rate_limit(e):
    """Лимит запросов: код 100410 / текст «请稍后重试» / RateLimitExceeded."""
    if 'RateLimitExceeded' in {c.__name__ for c in type(e).__mro__}:
        return True
    t = str(e)
    m = _CODE_IN_TEXT.search(t)
    return bool((m and m.group(1) in TRANSIENT_CODES) or RATE_LIMIT_TEXT.search(t))


def is_request_defect(e):
    """[v131, §2.61.2] Отказ по ПАРАМЕТРАМ запроса (HTTP 400/422, коды валидации) у бирж прямого REST: не «пусто» и не постоянная ошибка символа — символ остаётся «не обработан»."""
    try:
        import research_venues
        return research_venues.is_request_defect(e)
    except Exception:
        return False


def is_transient(e):
    """Сеть/лимиты — повторять; постоянная ошибка символа (BadSymbol, ExchangeError …) — нет (v121: раньше каждый такой символ стоил 2+6+15 с на КАЖДОМ прогоне).
    v127 (§2.58): делегирует research_htf.is_retryable (повторы на ExchangeError 100410 — с v87; в v121 я их потерял) и распознаёт код/текст лимита («请稍后重试» — «повторите позже»)."""
    try:
        import research_htf
        if research_htf.is_retryable(e):
            return True
    except Exception:
        pass
    names = {c.__name__ for c in type(e).__mro__}
    if 'VenueError' in names:                                       # v129: Gate/MEXC/Bitget (research_venues) — лимит и 5xx временные
        return bool(re.search(r'HTTP (5\d\d|429)|too many requests|rate.?limit', str(e), re.I))
    if names & {'NetworkError', 'RequestTimeout', 'DDoSProtection', 'RateLimitExceeded', 'ExchangeNotAvailable', 'OnMaintenance', 'TimeoutError',
                'ClientError', 'ConnectionError', 'OSError'}:
        return True
    t = str(e)
    if 'OkxError' in names:
        return 'HTTP' in t or '50011' in t or '50013' in t
    return is_rate_limit(e) or bool(TRANSIENT_TEXT.search(t))


def page_pause(ex_id):
    """Пауза после каждой успешной страницы свечей/фандинга BingX (ENV BINGX_PAGE_PAUSE_SEC, по умолчанию 1.0 с; §2.58 п. 7) — ДОПОЛНИТЕЛЬНО к HTF_FETCH_PAUSE_SEC."""
    if ex_id != 'bingx':
        return 0.0
    try:
        return max(0.0, float(os.getenv('BINGX_PAGE_PAUSE_SEC', '1.0')))
    except ValueError:
        return 1.0


def save_bars(d, ex, sym, ts, o, h, l, c, v):
    V._atomic_savez(bars_path(d, ex, sym), ts=np.asarray(ts, np.int64), o=np.asarray(o, np.float64), h=np.asarray(h, np.float64), l=np.asarray(l, np.float64), c=np.asarray(c, np.float64), v=np.asarray(v, np.float64))


def load_bars(d, ex, sym):
    z = V._load_npz(bars_path(d, ex, sym))
    if z is None:
        return None
    return {k: z[k] for k in ('ts', 'o', 'h', 'l', 'c', 'v')} | {'qv': z['v'] * z['c']}


def save_hbars(d, ex, sym, ts, h, l, c):
    """Часовые свечи для X1 (BOT_SPEC §2.48.6): только high/low/close (float32) — экономия памяти и диска."""
    V._atomic_savez(hbars_path(d, ex, sym), ts=np.asarray(ts, np.int64), h=np.asarray(h, np.float32), l=np.asarray(l, np.float32), c=np.asarray(c, np.float32))


def load_hbars(d, ex, sym):
    z = V._load_npz(hbars_path(d, ex, sym))
    if z is None:
        return None
    return {k: z[k] for k in ('ts', 'h', 'l', 'c')}


def save_fund(d, ex, sym, ts, rate):
    V._atomic_savez(fund_path(d, ex, sym), ts=np.asarray(ts, np.int64), rate=np.asarray(rate, np.float64))


def load_fund(d, ex, sym):
    z = V._load_npz(fund_path(d, ex, sym))
    if z is None:
        return None
    return z['ts'], z['rate']


def round_sec(ts):
    return (np.asarray(ts, np.int64) + 500) // 1000 * 1000


# ── рынки ───────────────────────────────────────────────────────────────────────────────────────────
NC_PREFIX = 'NC'                                     # §2.52: базы NCSK/NCSI/NCFX/NCCO и любые NC* — перпетуалы на акции, индексы, валюты, сырьё
CAT_KEYS = ('instCategory', 'category', 'assetType', 'productType')      # кандидаты поля «класс актива» в market['info'] (проверяются при загрузке, §2.52)
NONCRYPTO_WORDS = ('stock', 'equit', 'index', 'indic', 'forex', 'fx', 'commodit', 'metal', 'bond', 'etf')


def noncrypto_reason(base, info=None):
    """Признак не-крипто (BOT_SPEC §2.52): 'NC-префикс' | 'поле <ключ>=<значение>' | None. OKX instCategory: «1» (или пусто) — крипто, иное — не-крипто (по документации OKX по памяти, НЕ проверено —
    распределение значений печатается в отчёте загрузки); остальные поля — по словам (stock, index, forex, commodity …)."""
    b = str(base or '').upper().strip()
    if b.startswith(NC_PREFIX):
        return 'NC-префикс'
    for k in CAT_KEYS:
        v = (info or {}).get(k) if isinstance(info, dict) else None
        if v in (None, ''):
            continue
        sv = str(v).strip().lower()
        if k == 'instCategory':
            if sv not in ('1', ''):
                return f'поле {k}={v}'
        elif any(w in sv for w in NONCRYPTO_WORDS):
            return f'поле {k}={v}'
    try:                                                              # [v135, §2.65.2] поля Gate.io / Bitget / MEXC (по выводу /contract_probe)
        import research_venues
        return research_venues.meta_exclusion(info)
    except Exception:
        return None


# ── [v134, BOT_SPEC §2.64.2] механическое исключение не-крипто из вселенной H2EXT: порядок а → б → в → г ──────────────────────────────────────────────────────────────────────────
CURRENCY_CODES = frozenset('USD EUR GBP JPY CHF AUD CAD NZD CNY CNH HKD SGD KRW INR BRL MXN RUB TRY ZAR SEK NOK DKK PLN CZK HUF ILS THB IDR MYR PHP TWD AED SAR'.split())
FACTORY_BASES = frozenset('XAU XAG XPT XPD WTI BRENT OIL GOLD SILVER PLAT DAX NIKKEI SPX NAS100 US30 USOIL UKOIL'.split())      # research_ma.BINGX_EXACT и ключи поиска §2.41 + USOIL/UKOIL (§2.64.2 в)
INDEX_TICKERS = frozenset('SP500 SPX500 NDX100 NAS100 US30 US2000 GER40 UK100 JPN225 JP225 HK50 HSI KOSPI AUS200 DXY'.split())
EXT_LETTERS = ('а', 'б', 'в', 'г')


def ext_exclusion(base, norm, bases_on_ex, coins=None):
    """Пункты б, в, г (§2.64.2) → 'б' | 'в' | 'г' | None. Пункт а — прежний §2.52 (`excl`, проверяется вызывающим) + поля шага 0 (отдельной поправкой по выводу /contract_probe).
    bases_on_ex — множество баз (верхний регистр) ВСЕХ контрактов этой биржи; coins — множество кодов спотовых монет биржи с блокчейн-сетью (None/пусто — пункт г не применяется)."""
    b = str(base or '').upper().strip()
    if b.endswith('STOCK') or b.endswith('HKD') or (b + 'STOCK') in bases_on_ex:
        return 'б'
    if b in CURRENCY_CODES or b in FACTORY_BASES or b in INDEX_TICKERS:
        return 'в'
    if coins and b not in coins and str(norm or '').upper() not in coins:
        return 'г'
    return None


def apply_ext_filter(mk_ex, coins=None):
    """Проставляет `excl` ('б'/'в'/'г') символам одной биржи (словарь symbol → dict(base, norm, excl …)), если прежний фильтр §2.52 их не исключил. → число новых исключений."""
    bases = {str(m.get('base') or '').upper() for m in mk_ex.values()}
    n = 0
    for m in mk_ex.values():
        if m.get('excl'):
            continue
        r = ext_exclusion(m.get('base'), m.get('norm'), bases, coins)
        if r:
            m['excl'] = r
            n += 1
    return n


def cat_of(info):
    """Первое найденное поле «класс актива» как 'ключ=значение' (для распределения в отчёте) либо ''."""
    for k in CAT_KEYS:
        v = (info or {}).get(k) if isinstance(info, dict) else None
        if v not in (None, ''):
            return f'{k}={v}'
    return ''


def usdt_swaps(markets):
    """Линейные USDT-перпетуалы из ccxt-markets (неактивные включаются, если биржа их отдала). → {symbol: {base, norm, active, created}}."""
    out = {}
    for sym, m in (markets or {}).items():
        if not (m.get('swap') and m.get('linear') and str(m.get('quote')) == 'USDT' and str(m.get('settle') or 'USDT') == 'USDT'):
            continue
        base = str(m.get('base') or '')
        out[sym] = {'base': base, 'norm': norm_base(base), 'active': m.get('active') is not False, 'created': m.get('created'),
                    'excl': noncrypto_reason(base, m.get('info')), 'cat': cat_of(m.get('info'))}
    return out


class LazyStore(collections.abc.Mapping):
    """Отображение «символ → ряд» БЕЗ подъёма рядов в память: ключи известны из индекса, ряд читается из файла при обращении и не кэшируется (v123, BOT_SPEC §2.55)."""

    def __init__(self, keys, loader):
        self._keys, self._loader = list(keys), loader
        self._set = set(self._keys)

    def __getitem__(self, k):
        if k not in self._set:
            raise KeyError(k)
        return self._loader(k)

    def __iter__(self):
        return iter(self._keys)

    def __len__(self):
        return len(self._keys)

    def __contains__(self, k):
        return k in self._set


# ── загрузка (ccxt, публичные методы) ───────────────────────────────────────────────────────────────
class Fetcher:
    """Обёртка вызовов ccxt: повторы (2/6/15 с), пауза между успешными вызовами (HTF_FETCH_PAUSE_SEC), счётчики. sleep подменяется в тестах."""

    def __init__(self, ex, pause=None, sleep=asyncio.sleep, delays=None, retry_all=False):
        self.retry_all = retry_all                                              # [v125, §2.57 в] повторять ЛЮБУЮ ошибку (класс определён по тексту ответа как временный)
        self.ex, self.sleep, self.calls, self.errors = ex, sleep, 0, 0
        self.delays = RETRY_DELAYS if delays is None else tuple(delays)          # [v125] повторы при сетевых ошибках; для класса «в» (§2.57) — 3/8/20/45 с
        if pause is None:
            import research_htf
            pause = research_htf.fetch_pause()
        self.pause = pause

    async def call(self, name, *a, **kw):
        """Вызов с повторами: сетевые/временные ошибки — по расписанию self.delays (2/6/15 с); лимит запросов (100410) — повтор ТОЙ ЖЕ страницы по RATE_LIMIT_DELAYS 3/8/20/45 с (v127, §2.58;
        в режиме без повторов delays=() — не повторяется ничего); постоянная ошибка — сразу наверх."""
        i = 0
        while True:
            try:
                r = await getattr(self.ex, name)(*a, **kw)
                self.calls += 1
                await self.sleep(self.pause)
                return r
            except Exception as e:                      # сеть/лимиты — повтор; последняя ошибка поднимается
                self.errors += 1
                if not self.retry_all and not is_transient(e):
                    raise
                sched = RATE_LIMIT_DELAYS if (self.delays and is_rate_limit(e)) else self.delays
                if i >= len(sched):
                    raise
                if sched[i]:
                    await self.sleep(sched[i])
                i += 1


async def fetch_bars(f, sym, since_ms, now_ms, limit=1000, tf='1d'):
    """Свечи вперёд от since_ms (tf '1d' или '1h'), только ЗАКРЫТЫЕ (начало + шаг ≤ now). → (ts, o, h, l, c, v) массивы."""
    step = DAY if tf == '1d' else HOUR
    rows, since = {}, since_ms
    for _ in range(400 if tf == '1h' else 200):
        got = await f.call('fetch_ohlcv', sym, tf, since, limit)
        if not got:
            break
        new = 0
        for r in got:
            if r[0] not in rows:
                rows[int(r[0])] = r
                new += 1
        nxt = int(got[-1][0]) + step
        if new == 0 or nxt <= since or nxt > now_ms:
            break
        since = nxt
    ts = sorted(t for t in rows if t + step <= now_ms)
    a = np.array([rows[t][1:6] for t in ts], float) if ts else np.zeros((0, 5))
    return np.array(ts, np.int64), a[:, 0], a[:, 1], a[:, 2], a[:, 3], a[:, 4]


async def fetch_bars_back(f, ex_id, sym, since_ms, now_ms, tf='1d', limit=1000, max_pages=None):
    """Свечи ПОСТРАНИЧНО НАЗАД от «сейчас» (параметр конца интервала endTime/until), только ЗАКРЫТЫЕ и не раньше since_ms. → (ts, o, h, l, c, v, complete).
    complete = история дошла до начала (пустая страница либо нет прогресса) либо до since_ms; False — упёрлись в лимит страниц.
    v121 (BOT_SPEC §2.53): прямой проход от since=2019-01-01 с limit=1000 даёт пустое окно для символов, листингованных позже окна, а пустой ответ принимался за «данных нет»;
    проход назад от настоящего от даты листинга не зависит. ПРИЧИНА НА ПРОДЕ — гипотеза (BingX отдаёт [] на окно [startTime, startTime+limit·шаг] без данных), не проверена."""
    step = DAY if tf == '1d' else HOUR
    rows, end, complete = {}, None, False
    key = 'until' if ex_id == 'bybit' else 'endTime'
    pages_ok = 0
    for _ in range(max_pages or (400 if tf == '1h' else 200)):
        params = {} if end is None else {key: end}
        try:
            got = await f.call('fetch_ohlcv', sym, tf, None, limit, params)
        except Exception as e:                                  # [v125] сколько страниц и строк успели получить до ошибки — в отрицательную запись
            try:
                e.pages_ok, e.rows_got = pages_ok, len(rows)
            except Exception:
                pass
            raise
        pages_ok += 1
        if page_pause(ex_id):
            await f.sleep(page_pause(ex_id))                    # [v127] пауза между страницами (BINGX_PAGE_PAUSE_SEC)
        if not got:
            complete = True
            break
        new, mn = 0, None
        for r in got:
            t = int(r[0])
            mn = t if mn is None else min(mn, t)
            if t not in rows:
                rows[t] = r
                new += 1
        if new == 0:
            complete = True
            break
        if mn <= since_ms:
            complete = True
            break
        end = mn - 1
    ts = sorted(t for t in rows if t + step <= now_ms and t >= since_ms)
    a = np.array([rows[t][1:6] for t in ts], float) if ts else np.zeros((0, 5))
    return np.array(ts, np.int64), a[:, 0], a[:, 1], a[:, 2], a[:, 3], a[:, 4], complete


async def fetch_funding(f, ex_id, sym, stop_ms, limit=None):
    """Совместимая обёртка: (ts, rate) без признака полноты (см. fetch_funding_ex)."""
    ts, r, _c = await fetch_funding_ex(f, ex_id, sym, stop_ms, limit)
    return ts, r


async def fetch_funding_ex(f, ex_id, sym, stop_ms, limit=None):
    """История фандинга постранично НАЗАД (параметры конца интервала `until`/`endTime`), до stop_ms (первый бар символа) либо пока страница не даёт более ранних записей.
    → (ts, rate) по возрастанию, время округлено до секунды."""
    lim = limit or {'bybit': 200, 'okx': 100}.get(ex_id, 1000)
    ev, end, earliest, complete = {}, None, None, False
    for _ in range(MAX_FUND_PAGES):
        params = {} if end is None else ({'until': end} if ex_id == 'bybit' else {'endTime': end})   # BingX в ccxt 4.2.14: 'until' кладётся в startTime — конец передаём сырым endTime (как research_htf)
        got = await f.call('fetch_funding_rate_history', sym, None, lim, params)
        batch = [(int(x['timestamp']), float(x['fundingRate'])) for x in (got or []) if x.get('timestamp') is not None and x.get('fundingRate') is not None]
        if not batch:
            complete = True                                   # API исчерпан — глубже истории нет
            break
        mn = min(t for t, _r in batch)
        if page_pause(ex_id):
            await f.sleep(page_pause(ex_id))                    # [v127] пауза между страницами (BINGX_PAGE_PAUSE_SEC)
        for t, r in batch:
            ev[t] = r
        if earliest is not None and mn >= earliest:
            complete = True
            break
        earliest = mn
        if mn <= stop_ms:
            complete = True                                   # дошли до первого бара символа
            break
        end = mn - 1
    ts = sorted(ev)
    return (round_sec(np.array(ts, np.int64)) if ts else np.zeros(0, np.int64)), np.array([ev[t] for t in ts], float), complete


def noncrypto_lines(ex, swaps):
    """Строки отчёта загрузки (§2.52): сколько исключено и по какому признаку; NC-базы списком; распределение значений найденных полей «класс актива» по бирже."""
    from collections import Counter
    out = []
    ex_by = Counter((v['excl'].split('=')[0] if v.get('excl') else None) for v in swaps.values())
    ex_by.pop(None, None)
    if ex_by:
        ncs = sorted(v['base'] for v in swaps.values() if v.get('excl') == 'NC-префикс')
        out.append(f'    исключено не-крипто (§2.52): {sum(ex_by.values())} — ' + ', '.join(f'{k}: {n}' for k, n in sorted(ex_by.items()))
                   + (f'; NC-базы ({len(ncs)}): ' + ', '.join(ncs[:40]) + (' …' if len(ncs) > 40 else '') if ncs else ''))
    cats = Counter(v.get('cat') for v in swaps.values() if v.get('cat'))
    out.append(f'    поля «класс актива» в markets ({ex}): ' + (', '.join(f'{k}: {n}' for k, n in sorted(cats.items())[:12]) if cats else 'не найдено (проверялись ' + ', '.join(CAT_KEYS) + ') — признак только по NC-префиксу'))
    return out


def coarse_liquid(qv, window=30):
    """Максимум 30-дневного среднего оборота за всю историю ≥ LIQ_COARSE (грубый фильтр загрузки фандинга)."""
    if len(qv) < 1:
        return False
    a, _c = B.pit_adv(np.asarray(qv, np.float32).reshape(-1, 1), window)
    full = np.concatenate([a[:, 0], [float(np.mean(qv[-window:]))]])
    return bool(np.nanmax(full) >= LIQ_COARSE)


def first30_turnover(bars):
    n = min(FIRST30, len(bars['ts']))
    return float(np.mean(bars['qv'][:n])) if n else 0.0


# ── правило вселенной H2-репликации ─────────────────────────────────────────────────────────────────
def h2_universe(markets, bars, binance_norm):
    """Вселенная §2.47.2 (правило, без ручного отбора). markets = {ex: usdt_swaps(...)}, bars = {ex: {sym: bars}}. Кандидат — нормализованная база, которой НЕТ в архиве Binance; монета на обеих биржах —
    биржа с большим оборотом за первые 30 баров (при равенстве — Bybit). Исключаются стейблкоин-подобные (σ < 0.5%) и существовавшие до начала истории биржи (первый бар в самый ранний день
    среди загруженных символов биржи). → (список dict(ex, sym, norm, first_day), справка dict счётчиков и исключений)."""
    earliest = {ex: min((int(b['ts'][0]) for b in bars.get(ex, {}).values() if len(b['ts'])), default=None) for ex in EXCHANGES}
    cand, nonc, excluded_syms = {}, {}, []
    for ex in EXCHANGES:
        for sym, m in markets.get(ex, {}).items():
            b = bars.get(ex, {}).get(sym)
            if b is None or not len(b['ts']) or m['norm'] in binance_norm:
                continue
            why = m.get('excl') or noncrypto_reason(m.get('base') or m['norm'])      # [v120, §2.52] не-крипто — до расчёта вселенной (кэш прошлых версий — по базе)
            if why:
                nonc.setdefault(why.split('=')[0], []).append(f'{ex}:{m.get("base") or m["norm"]}')
                excluded_syms.append((ex, sym, why))                 # [v134, §2.64.3] для файла-обзора: биржа, символ, пункт
                continue
            cand.setdefault(m['norm'], []).append((ex, sym, first30_turnover(b), int(b['ts'][0])))      # [v132, §2.62.1] в памяти — только оборот и первый бар (ряды целиком не удерживаются; кандидатов ≈ 1800)
    out, ref = [], {'stable': [], 'pre_existing': [], 'both': 0, 'not_in_binance_bases': len(cand), 'noncrypto': nonc, 'excluded_syms': excluded_syms}
    for norm in sorted(cand):
        opts = cand[norm]
        if len(opts) > 1:
            ref['both'] += 1
        ex, sym, _turn, first_ts = max(opts, key=lambda o: (o[2], o[0] == 'bybit'))
        if B.stable_like(bars[ex][sym]['c']):                          # ряд выбранного символа перечитывается (результат тот же, что при хранении ряда)
            ref['stable'].append(f'{ex}:{sym}')
            excluded_syms.append((ex, sym, 'стейблкоин'))
            continue
        if earliest[ex] is not None and first_ts <= earliest[ex]:
            ref['pre_existing'].append(f'{ex}:{sym}')
            excluded_syms.append((ex, sym, 'до начала истории'))
            continue
        out.append({'ex': ex, 'sym': sym, 'norm': norm, 'first_day': first_ts // DAY})
    return out, ref


def universe_line(universe):
    """Строка для BOT_SPEC: `H2REPL-UNIVERSE: N=<число> sha=<12 hex> <ex:sym,…>` (отсортировано)."""
    import hashlib
    names = sorted(f'{u["ex"]}:{u["sym"]}' for u in universe)
    h = hashlib.sha256(','.join(names).encode()).hexdigest()[:12]
    return f'H2REPL-UNIVERSE: N={len(names)} sha={h} ' + ','.join(names)


# ── панель в формате research_bdata.load_panel ──────────────────────────────────────────────────────
def panel_from_series(series, funding=None, exclude_stable=False):
    """series = {имя: dict(ts(мс), o, h, l, c, qv)}, funding = {имя: (ts, rate)} → панель «день × символ» (O,H,L,C,QV float32; F/G/GC; first/last; syms; day0; nd) — тот же формат и тот же
    алгоритм, что у `research_bdata.load_panel` (тест сравнивает прогоны `run_cell` побитово на одних и тех же данных)."""
    funding = funding or {}
    names = []
    excl = []
    for s in sorted(series):
        x = series[s]
        if not len(x['ts']):
            continue
        if exclude_stable and B.stable_like(x['c']):
            excl.append(s)
            continue
        names.append(s)
    if not names:
        return None
    day0 = min(int(series[s]['ts'][0]) // DAY for s in names)
    day1 = max(int(series[s]['ts'][-1]) // DAY for s in names)
    nd, ns = day1 - day0 + 1, len(names)
    P = {k: np.full((nd, ns), np.nan, np.float32) for k in ('O', 'H', 'L', 'C', 'QV')}
    F, G, GC = (np.zeros((nd, ns), np.float32) for _ in range(3))
    first, last, hf = [], [], []
    for j, s in enumerate(names):
        x = series[s]
        di = (np.asarray(x['ts']) // DAY - day0).astype(np.int64)
        for k, kk in zip(('O', 'H', 'L', 'C', 'QV'), ('o', 'h', 'l', 'c', 'qv')):
            P[k][di, j] = np.asarray(x[kk])
        first.append(int(di[0]))
        last.append(int(di[-1]))
        f = funding.get(s)
        if f is not None and len(f[0]):
            F[:, j], G[:, j], GC[:, j] = B.daily_funding(day0, nd, np.asarray(f[0], np.int64), np.asarray(f[1], float))
        hf.append(f is not None and len(f[0]) > 0)
    P.update({'F': F, 'G': G, 'GC': GC, 'syms': names, 'day0': day0, 'nd': nd, 'first': np.array(first, np.int64), 'last': np.array(last, np.int64), 'excluded_stable': excl, 'has_fund': np.array(hf)})
    return P


# ── задача загрузки ─────────────────────────────────────────────────────────────────────────────────
def htf_funding_rows(db_path, sym):
    """Кэш фандинга v86–v88 (таблица htf_funding в БД бота): [(ts мс, ставка)] по символу BingX либо [] (нет БД/таблицы/символа)."""
    if not db_path:
        return []
    try:
        import sqlite3
        con = sqlite3.connect(db_path)
        try:
            return [(int(t), float(r)) for t, r in con.execute('SELECT ts, rate FROM htf_funding WHERE symbol=? ORDER BY ts', (sym,))]
        finally:
            con.close()
    except Exception:
        return []


def _stats_line(what, ex_syms, idx, kind, extra=''):
    """«загружено X из Y, осталось Z» (X — символы с данными в индексе, N — подтверждённо пустые/постоянные ошибки, Z — ещё не обработаны)."""
    y = len(ex_syms)
    x = sum(1 for ex, s_ in ex_syms if s_ in idx[kind].get(ex, {}))
    n = sum(1 for ex, s_ in ex_syms if s_ not in idx[kind].get(ex, {}) and f'{ex}|{s_}|{what}' in idx['neg'])
    return x, y, n, y - x - n


async def load_hourly(d, idx, fx, ex, syms, now_ms, deadline, progress=None, sleep=None):
    """1h-свечи заданных символов (ПО ОДНОМУ СИМВОЛУ — сразу в журнал); проход назад от настоящего. → (загружено, подтверждённо пусто, ошибка биржи, осталось, остановлено_бюджетом) — v124: пусто и ошибка раздельно (§2.53.2)."""
    done = empty = err = left = 0
    stopped = False
    for sym in sorted(syms):
        if time.time() > deadline:
            stopped = True
            left = sum(1 for s_ in syms if s_ not in idx['hbars'].get(ex, {}) and not neg_active(idx, ex, s_, 'h1', now_ms))
            break
        rec = idx['hbars'].get(ex, {}).get(sym)
        if rec and 'complete' in rec and rec.get('last', 0) + 3 * HOUR >= now_ms - DAY:
            done += 1
            continue
        if neg_active(idx, ex, sym, 'h1', now_ms):
            if idx['neg'][f'{ex}|{sym}|h1'].get('why') == 'empty':
                empty += 1
            else:
                err += 1
            continue
        cur = load_hbars(d, ex, sym) if rec else None
        since = (int(cur['ts'][-1]) + HOUR) if cur is not None and len(cur['ts']) else SINCE_MS
        api_sym = (idx['alias'].get(f'{ex}|{sym}') or {}).get('new', sym)          # [v125, §2.57 б] переименованный символ: запрос по новому имени, ряд хранится под старым ключом
        try:
            hts, _o, hh, hl, hc, _v, comp = await fetch_bars_back(fx[ex], ex, api_sym, since, now_ms, tf='1h')
        except Exception as e:
            if is_transient(e):
                put(d, idx, 'fail', ex, f'{sym}|h1', type(e).__name__)
            else:
                put(d, idx, 'neg', ex, f'{sym}|h1', neg_rec(now_ms, e))
                err += 1
            continue
        if cur is not None and len(cur['ts']):
            hts = np.concatenate([cur['ts'], hts]); hh = np.concatenate([cur['h'], hh]); hl = np.concatenate([cur['l'], hl]); hc = np.concatenate([cur['c'], hc])
        if len(hts):
            save_hbars(d, ex, sym, hts, hh, hl, hc)
            put(d, idx, 'hbars', ex, sym, {'first': int(hts[0]), 'last': int(hts[-1]), 'n': int(len(hts)), 't': now_ms, 'complete': bool(comp)})
            done += 1
        else:
            put(d, idx, 'neg', ex, f'{sym}|h1', {'t': now_ms, 'why': 'empty'})
            empty += 1
        if progress and (done + empty + err) % 5 == 0:
            await progress(f'1h-свечи: обработано {done + empty + err}/{len(syms)} ({ex}:{sym})')
    return done, empty, err, left, stopped


async def run_xload(params, progress=None, exchanges=None, now_ms=None, sleep=asyncio.sleep):
    """`/h2_replicate load` и `/xfund_test load`: рынки → дневные свечи → история фандинга (все символы вселенной; X1 с архивом Binance — все общие базы, без предфильтра ликвидности) →
    1h-свечи общих баз Bybit∩BingX. v121 (BOT_SPEC §2.53): каждый символ сразу пишется в журнал индекса, повторный запуск продолжает с оставшихся (порядок — сортировка),
    постоянные ошибки/пустые ответы кэшируются (не перезапрашиваются 7 суток), свечи — проходом назад от настоящего. exchanges — {id: ccxt-клиент} (в тестах фейки). → текст отчёта."""
    d = params.get('xdir') or xdata_dir()
    now_ms = int(now_ms or time.time() * 1000)
    deadline = time.time() + float(params.get('max_minutes', 150)) * 60
    own = exchanges is None
    if own:
        import ccxt.async_support as ccxt
        import research_okx
        exchanges = {'okx': research_okx.OkxPublic(), 'bybit': ccxt.bybit({'options': {'defaultType': 'swap'}, 'enableRateLimit': True}), 'bingx': ccxt.bingx({'options': {'defaultType': 'swap'}, 'enableRateLimit': True})}
        if params.get('ext'):                                       # [v129, §2.59.2] H2EXT: BingX + OKX + новые биржи (прямой REST без ccxt); Bybit не используется
            import research_venues
            exchanges = {ex: c for ex, c in exchanges.items() if ex in (params.get('venues') or ())}
            for ex_, cls in research_venues.CLASSES.items():
                if ex_ in (params.get('venues') or ()):
                    exchanges[ex_] = cls()
    idx = compact_index(d)
    n_mig = migrate_v127(d, idx)
    n_mig131 = migrate_v131(d, idx)                                    # [v131, §2.61.2] разовый сброс ошибок Bitget (дефект запроса v130)
    #                                                                  # [v127, §2.58 п. 8] разовый сброс записей-ошибок (лимит запросов 100410 записывался как постоянная ошибка)
    idx['fail'] = {}                                                # ошибки текущего прогона (транзиентные); постоянные — в idx['neg']
    L = [f'📥 <b>загрузка данных бирж</b> — Bybit/BingX (публичные методы ccxt), OKX (прямой REST без ccxt) → {d}']
    if n_mig131:
        L.append(f'↺ v131 (§2.61.2): сброшено {n_mig131} записей-ошибок Bitget (дефект запроса v130, HTTP 400 / 40017) в «не обработано»')
    if n_mig:
        L.append(f'↺ v127 (§2.58): сброшено {n_mig} записей-ошибок загрузки и классификации в «не обработано» (код 100410 «请稍后重试» — лимит запросов — ранее записывался как постоянная ошибка)')
    stopped = ''
    left_total = 0
    try:
        fx = {ex: Fetcher(c, sleep=sleep) for ex, c in exchanges.items()}
        mk = {}
        unavailable = {}
        venues = tuple(params.get('venues') or ('bybit', 'bingx'))
        for ex, c in exchanges.items():
            if ex not in venues:                                    # [v118] площадка исключена пре-регистрацией (H2REPL-VENUES, §2.50)
                mk[ex] = {}
                L.append(f'  {ex}: пропущена по пре-регистрации (H2REPL-VENUES)')
                continue
            try:
                m = await fx[ex].call('load_markets')
            except Exception as e:                                  # [v118] одна недоступная биржа не останавливает загрузку остальных (§2.50)
                unavailable[ex] = type(e).__name__
                mk[ex] = {}
                L.append(f'  ⛔ {ex}: недоступна (вердикт пробы /net_probe — гео-блок или лимит частоты): load_markets {type(e).__name__}; '
                         f'загрузка остальных продолжается')
                continue
            mk[ex] = usdt_swaps(m)
            idx['markets'][ex] = {s: {'base': v['base'], 'norm': v['norm'], 'active': v['active'], 'created': v['created'], 'excl': v.get('excl'), 'cat': v.get('cat', '')} for s, v in mk[ex].items()}
            L.append(f'  {ex}: линейных USDT-перпетуалов {len(mk[ex])} (неактивных {sum(1 for v in mk[ex].values() if not v["active"])})')
            L += noncrypto_lines(ex, mk[ex])
            mk[ex] = {s: v for s, v in mk[ex].items() if not v.get('excl')}          # [v120, §2.52] данные по исключённым символам не запрашиваются
        if exchanges and not any(mk.get(ex) for ex in exchanges) and (unavailable or len(venues) < len(exchanges)):
            save_index(d, idx)
            return '\n'.join(L + ['⛔ все биржи недоступны — загружать нечего (Binance — из архива /battery_load, не отсюда)'])
        idx['unavailable'] = dict(unavailable)
        save_index(d, idx)
        bnorm = binance_norm_set(params.get('bdir'))
        norms = {ex: {} for ex in EXCHANGES}
        for ex in EXCHANGES:
            for s, v in mk.get(ex, {}).items():
                norms[ex].setdefault(v['norm'], []).append(s)
        common = set(norms['bybit']) & set(norms['bingx'])
        want = {ex: [] for ex in EXCHANGES}
        for ex in EXCHANGES:
            for s, v in mk.get(ex, {}).items():
                if v['norm'] not in bnorm or (ex in OLD_VENUES and v['norm'] in common):
                    want[ex].append(s)
        x1_main = bool(params.get('x1_main'))                        # [v118] X1 (BingX, Binance): ВСЕ BingX-символы с базой из архива Binance (§2.50, §2.53)
        if x1_main:
            for s_, v in mk.get('bingx', {}).items():
                if v['norm'] in bnorm and s_ not in want['bingx']:
                    want['bingx'].append(s_)
        syms_all = [(ex, s_) for ex in EXCHANGES for s_ in sorted(set(want[ex]))]
        L.append(f'  вселенная H2 (базы вне архива Binance): {sum(1 for n in set().union(*[set(v) for v in norms.values()]) if n not in bnorm)} баз; общих баз Bybit∩BingX: {len(common)}; '
                 f'символов к загрузке свечей: ' + ', '.join(f'{ex} {len(set(want[ex]))}' for ex in EXCHANGES))
        guard = params.get('space_guard')                            # [v130, §2.59.11 п. 3] отказ до скачивания, если не хватит места при идущем замере
        if guard is not None:
            refuse = guard(syms_all, idx)
            if refuse:
                save_index(d, idx)
                return '\n'.join(L + [refuse])
        # 1. свечи — по одному символу, сразу в журнал; порядок детерминирован (сортировка)
        n_try = 0
        ok_n, dstreak, tripped = {}, {}, {}                         # [v131, §2.61.2] успехи, подряд дефектов запроса и «остановленные» биржи (на прогон)
        for ex, sym in syms_all:
            if time.time() > deadline:
                stopped = '⏱ бюджет времени исчерпан — повторите команду (продолжит с места остановки)'
                break
            rec = idx['bars'][ex].get(sym)
            if rec and 'complete' in rec and rec.get('last', 0) + 2 * DAY >= now_ms - DAY:      # записи прошлых версий (без признака полноты) загружаются заново
                continue
            if neg_active(idx, ex, sym, 'bars', now_ms):
                continue
            if ex in tripped:
                continue
            cur = load_bars(d, ex, sym) if rec else None                  # без записи в индексе файл не открываем (индекс — единственный источник «что готово»)
            since = (int(cur['ts'][-1]) + DAY) if cur is not None and len(cur['ts']) else SINCE_MS
            try:
                ts, o, h, l_, c, v, comp = await fetch_bars_back(fx[ex], ex, sym, since, now_ms)
            except Exception as e:
                if is_transient(e):
                    put(d, idx, 'fail', ex, f'{sym}|bars', type(e).__name__)
                elif is_request_defect(e):                                  # [v131, §2.61.2] отказ по параметрам запроса ≠ «пусто»: символ остаётся «не обработан» (в отрицательный кэш не пишется)
                    put(d, idx, 'fail', ex, f'{sym}|bars', 'ДЕФЕКТ ЗАПРОСА: ' + ' '.join(str(e).split())[:140])
                    dstreak[ex] = dstreak.get(ex, 0) + 1
                    if not ok_n.get(ex) and dstreak[ex] >= DEFECT_STOP:
                        tripped[ex] = str(e)
                else:
                    put(d, idx, 'neg', ex, f'{sym}|bars', neg_rec(now_ms, e))
                continue
            ok_n[ex] = ok_n.get(ex, 0) + 1
            dstreak[ex] = 0
            if cur is not None and len(cur['ts']):
                ts = np.concatenate([cur['ts'], ts]); o = np.concatenate([cur['o'], o]); h = np.concatenate([cur['h'], h]); l_ = np.concatenate([cur['l'], l_]); c = np.concatenate([cur['c'], c]); v = np.concatenate([cur['v'], v])
                comp = bool(comp and (rec or {}).get('complete', True))
            if len(ts):
                save_bars(d, ex, sym, ts, o, h, l_, c, v)
                put(d, idx, 'bars', ex, sym, {'first': int(ts[0]), 'last': int(ts[-1]), 'n': int(len(ts)), 't': now_ms, 'complete': bool(comp)})
            else:
                put(d, idx, 'neg', ex, f'{sym}|bars', {'t': now_ms, 'why': 'empty'})
            n_try += 1
            if progress and n_try % 25 == 0:
                x_, y_, n_, z_ = _stats_line('bars', syms_all, idx, 'bars')
                await progress(f'свечи: загружено {x_} из {y_}, осталось {z_} ({ex}:{sym})')
        save_index(d, idx)
        x_, y_, n_, z_ = _stats_line('bars', syms_all, idx, 'bars')
        L.append(f'  свечи: загружено {x_} из {y_}, осталось {z_}; подтверждённо пусто/постоянная ошибка: {n_}')
        L += error_lines(idx, 'bars', syms_all, 4)
        L += defect_lines(idx, 'bars', tripped)
        left_total += z_
        # 2. фандинг — все символы со свечами (без предфильтра ликвидности: ликвидность — point-in-time при расчёте)
        if not stopped:
            todo = [(ex, sym) for ex, sym in syms_all if sym in idx['bars'][ex]]
            n_f = 0
            for ex, sym in todo:
                if time.time() > deadline:
                    stopped = '⏱ бюджет времени исчерпан — повторите команду (продолжит с места остановки)'
                    break
                rec = idx['fund'][ex].get(sym)
                if rec and 'complete' in rec and rec.get('t', 0) >= now_ms - 2 * DAY:
                    continue
                if neg_active(idx, ex, sym, 'fund', now_ms):
                    continue
                if ex in tripped:
                    continue
                b0 = int(idx['bars'][ex][sym]['first'])
                try:
                    ts, r, comp = await fetch_funding_ex(fx[ex], ex, sym, b0)
                except Exception as e:
                    if is_transient(e):
                        put(d, idx, 'fail', ex, f'{sym}|fund', type(e).__name__)
                    elif is_request_defect(e):                              # [v131, §2.61.2]
                        put(d, idx, 'fail', ex, f'{sym}|fund', 'ДЕФЕКТ ЗАПРОСА: ' + ' '.join(str(e).split())[:140])
                        dstreak[ex] = dstreak.get(ex, 0) + 1
                        if not ok_n.get(ex) and dstreak[ex] >= DEFECT_STOP:
                            tripped[ex] = str(e)
                    else:
                        put(d, idx, 'neg', ex, f'{sym}|fund', neg_rec(now_ms, e))
                    continue
                ok_n[ex] = ok_n.get(ex, 0) + 1
                dstreak[ex] = 0
                cur = load_fund(d, ex, sym) if rec else None
                parts_t, parts_r = [ts], [r]
                if cur is not None and len(cur[0]):
                    parts_t.append(cur[0]); parts_r.append(cur[1])
                if ex == 'bingx':                                   # кэш v86–v88 (htf_funding) глубже — объединяем по времени события
                    hr = htf_funding_rows(params.get('htf_db'), sym)
                    if hr:
                        parts_t.append(round_sec(np.array([t for t, _ in hr], np.int64))); parts_r.append(np.array([x for _, x in hr], float))
                allts, allr = np.concatenate(parts_t), np.concatenate(parts_r)
                u, ui = np.unique(allts, return_index=True)
                ts, r = u, allr[ui]
                if len(ts):
                    save_fund(d, ex, sym, ts, r)
                    put(d, idx, 'fund', ex, sym, {'first': int(ts[0]), 'last': int(ts[-1]), 'n': int(len(ts)), 't': now_ms, 'complete': bool(comp)})
                else:
                    put(d, idx, 'neg', ex, f'{sym}|fund', {'t': now_ms, 'why': 'empty'})
                n_f += 1
                if progress and n_f % 10 == 0:
                    x_, y_, _n, z_ = _stats_line('fund', todo, idx, 'fund')
                    await progress(f'фандинг: загружено {x_} из {y_}, осталось {z_} ({ex}:{sym})')
            save_index(d, idx)
            x_, y_, n_, z_ = _stats_line('fund', todo, idx, 'fund')
            L.append(f'  фандинг: загружено {x_} из {y_}, осталось {z_}; подтверждённо пусто/постоянная ошибка: {n_} (без предфильтра ликвидности)')
            L += error_lines(idx, 'fund', todo, 4)
            L += defect_lines(idx, 'fund', tripped)
            left_total += z_
            # 3. часовые свечи общих баз Bybit∩BingX (X1 с архивом Binance — по символам сделок, отдельным шагом /xfund_test load)
            if params.get('hourly', True) and not stopped and common and not params.get('ext'):
                for ex in EXCHANGES:
                    hs = [s_ for s_ in want[ex] if mk[ex][s_]['norm'] in common and s_ in idx['bars'][ex]]
                    if not hs or stopped:
                        continue
                    dn, em, er, lf, stp = await load_hourly(d, idx, fx, ex, hs, now_ms, deadline, progress)
                    if stp:
                        stopped = '⏱ бюджет времени исчерпан — повторите команду (продолжит с места остановки)'
                    L.append(f'  1h-свечи ({ex}, общие базы Bybit∩BingX): загружено {dn} из {len(hs)}, осталось {lf}; подтверждённо пусто: {em}; ошибка биржи: {er}')
                    left_total += lf
        for c_ in exchanges.values():                                   # [v132] принятая форма запроса биржи прямого REST (Bitget) — в отчёт загрузки
            fi_ = getattr(c_, 'form_info', None)
            if callable(fi_) and fi_():
                L.append('  ' + fi_())
        idx['t'] = now_ms
        save_index(d, idx)
    finally:
        if own:
            import access_probe
            for c in exchanges.values():                            # [v120] двойное закрытие с паузой (см. access_probe.close_ccxt)
                await access_probe.close_ccxt(c)
    L += coverage_lines(d, idx, now_ms)
    if idx['fail']:
        L.append(f'⚠ транзиентные ошибки сети в этом прогоне: {len(idx["fail"])} (первые: ' + ', '.join(f'{k}={v}' for k, v in list(idx["fail"].items())[:5]) + ') — повторите команду, они будут запрошены снова')
    if stopped:
        L.append(stopped)
    elif left_total == 0 and not idx['fail']:
        L.append('✅ загрузка завершена')
    else:
        L.append(f'⏳ загрузка не завершена: осталось {left_total + len(idx["fail"])} — повторите команду')
    return '\n'.join(L)


COVERAGE_MIN = 0.9


def hourly_status(idx, syms):
    """[v124, §2.53.2] Состояние 1h-истории по символам сделок [(биржа, символ)]: загружена / подтверждённо пуста (ответ биржи без ошибки: отрицательная запись с причиной `empty` —
    дневной запасной базис §2.48.6 п. 2) / ошибка загрузки (постоянная ошибка биржи) / не обработана (сеть, продолжение). → dict(loaded, empty, error, pending: списки символов, total)."""
    out = {'loaded': [], 'empty': [], 'error': [], 'pending': [], 'unavailable': [], 'partial': [], 'alias': [], 'total': len(syms)}
    for ex, s_ in syms:
        if s_ in idx['hbars'].get(ex, {}):
            out['loaded'].append(s_)
            hb_, br_ = idx['hbars'][ex][s_], (idx['bars'].get(ex, {}).get(s_) or {})
            if hb_.get('limit') == 'depth' or (hb_.get('first') and br_.get('first') and int(hb_['first']) > int(br_['first']) + 3 * DAY):
                out['partial'].append(s_)                           # [v125 §2.57 а1, v127 §2.58 п. 4] 1h-история короче дневной (глубина BingX ≈ 22 500 баров) — остальной период по дневному запасному базису
            if f'{ex}|{s_}' in idx['alias']:
                out['alias'].append(s_)
            continue
        r = idx['neg'].get(f'{ex}|{s_}|h1')
        if r is None:
            out['pending'].append(s_)
        elif r.get('why') == 'empty':
            out['empty'].append(s_)
        elif r.get('why') == 'unavailable':                         # [v125, §2.57 а2] подтверждённо недоступны (символ закрыт / данных нет) — как «пусто»: дневной запасной базис
            out['unavailable'].append(s_)
        else:
            out['error'].append(s_)
    return out


def hourly_line(st):
    """Строка отчёта: «1h доступны для X из Y символов сделок; …» (BOT_SPEC §2.53.2)."""
    return (f'1h доступны для {len(st["loaded"])} из {st["total"]} символов сделок; символы с подтверждённо пустой 1h-историей ({len(st["empty"])}) используют дневной запасной базис (§2.48 п. 3.2 = §2.48.6 п. 2); '
            f'ошибка загрузки 1h: {len(st["error"])}, не обработано: {len(st["pending"])}'
            + (f'; подтверждённо недоступны (§2.57 а2, дневной запасной базис): {len(st["unavailable"])}' if st.get('unavailable') else '')
            + (f'; частичной глубины (§2.57 а1 / §2.58 п. 4, остальной период — дневной базис): {len(st["partial"])}' if st.get('partial') else '')
            + (f'; через соответствие переименованию (§2.57 б): {len(st["alias"])}' if st.get('alias') else ''))


def coverage_report(d, kind, bdir=None, venues=None, hourly_syms=None, minimum=None):
    """Контроль полноты данных ПЕРЕД расчётом (BOT_SPEC §2.53 п. 3). kind: 'x1' (BingX-символы, общие с архивом Binance) | 'h2' (вселенная H2 площадок venues, базы вне архива Binance).
    Ожидаемое — активные не-исключённые рынки из markets; считаются доли: свечи, фандинг, глубина (свечи и фандинг доведены до начала истории — признак complete; для OKX глубина
    фандинга ограничена API, §2.51 — не требуется) и (X1) 1h по символам сделок. Любая доля < minimum (0.9) → ok=False. → dict(ok, lines, parts)."""
    minimum = COVERAGE_MIN if minimum is None else minimum
    idx = load_index(d)
    bnorm = binance_norm_set(bdir)
    exs = ('bingx',) if kind == 'x1' else tuple(venues or ('bingx', 'okx'))
    exp = []
    for ex in exs:
        for sym, m in idx['markets'].get(ex, {}).items():
            if m.get('excl') or noncrypto_reason(m.get('base') or m.get('norm')) or not m.get('active', True):
                continue
            if (m['norm'] in bnorm) == (kind == 'x1'):
                exp.append((ex, sym))
    parts, lines = {}, []
    n = len(exp)
    if n == 0:
        return {'ok': False, 'parts': {}, 'lines': ['ожидаемая вселенная пуста — выполните load']}
    nb = sum(1 for ex, s_ in exp if s_ in idx['bars'].get(ex, {}))
    nf = sum(1 for ex, s_ in exp if s_ in idx['fund'].get(ex, {}) or (ex in NEW_VENUES and idx['neg'].get(f'{ex}|{s_}|fund', {}).get('why') == 'empty'))     # [v129, §2.59.2] новые биржи: подтверждённо пустая история фандинга = «фандинг 0 со счётчиком»
    deep = [(ex, s_) for ex, s_ in exp if ex in ('bybit', 'bingx')]
    nd = sum(1 for ex, s_ in deep if idx['bars'].get(ex, {}).get(s_, {}).get('complete') and idx['fund'].get(ex, {}).get(s_, {}).get('complete'))
    parts['свечи'] = (nb, n)
    parts['фандинг'] = (nf, n)
    if deep:
        parts['глубина (свечи и фандинг до начала истории)'] = (nd, len(deep))
    hst = None
    if hourly_syms is not None:                                     # [v124, §2.53.2] 1h: гейт только по ОШИБКАМ загрузки — «загружена» и «подтверждённо пуста» (дневной запасной базис) проходят
        hst = hourly_status(idx, hourly_syms)
        parts['1h по символам сделок (загружены или подтверждённо пусты/недоступны; ошибка/не обработано — гейт)'] = (len(hst['loaded']) + len(hst['empty']) + len(hst['unavailable']), len(hourly_syms))
    ok = True
    for name, (a_, b_) in parts.items():
        share = a_ / b_ if b_ else 0.0
        bad = share < minimum
        ok = ok and not bad
        lines.append(f'{name}: {a_} из {b_} ({share * 100:.0f}%)' + (f' < {minimum * 100:.0f}%' if bad else ''))
    if hst is not None:
        lines.append(hourly_line(hst))
        if hst['error']:                                            # [v125] какие именно ошибки биржи (класс, сообщение, страниц до ошибки)
            lines += [x.strip() for x in error_lines(idx, 'h1', [(ex, s_) for ex, s_ in hourly_syms if s_ in hst['error']])]
    return {'ok': ok, 'parts': parts, 'lines': lines, 'expected': n, 'hourly': hst}


def coverage_lines(d, idx, now_ms):
    """Глубина и неактивные/закрытые инструменты (ответ на вопрос §2.47.2 о делистингах)."""
    out = []
    for ex in EXCHANGES:
        bs = idx['bars'].get(ex, {})
        fs = idx['fund'].get(ex, {})
        if bs:
            firsts = [v['first'] for v in bs.values()]
            first_s = time.strftime("%Y-%m-%d", time.gmtime(min(firsts) / 1000))
            if ex in ('bingx', 'okx', 'bybit'):                       # [v131, §2.61.3] кэш §2.47 — конец данных по бирже, а не делистинг
                end_ms = max(v['last'] for v in bs.values())
                early = [s for s, v in bs.items() if v['last'] + 3 * DAY < end_ms]
                out.append(f'  {ex}: свечей по {len(bs)} символам, самый ранний бар {first_s}; <b>конец кэша {time.strftime("%Y-%m-%d", time.gmtime(end_ms / 1000))}</b> '
                           f'(не делистинг, §2.61.3); прекративших торги раньше конца кэша > 3 суток: {len(early)}')
            else:
                stale = [s for s, v in bs.items() if v['last'] + 3 * DAY < now_ms]
                n990 = sum(1 for v in bs.values() if int(v.get('n') or 0) >= 990)
                if ex in NEW_VENUES:                                    # [v132] признак усечения глубины лимитом «последние 1000 баров»: сколько символов с ≥ 990 барами
                    out.append(f'    {ex}: символов с ≥ 990 барами истории: {n990} из {len(bs)} (много таких — подозрение на усечение глубины лимитом 1000 баров; для листингов H2 глубина не нужна)')
                out.append(f'  {ex}: свечей по {len(bs)} символам, самый ранний бар {first_s}; не обновлявшихся > 3 суток (закрытые/делистингованные): {len(stale)}')
        if fs:
            fl = [v['first'] for v in fs.values() if v.get('first')]
            out.append(f'    фандинг по {len(fs)} символам; самое раннее событие {time.strftime("%Y-%m-%d", time.gmtime(min(fl) / 1000)) if fl else "н/д"}')
        if ex in ('okx',) + NEW_VENUES and fs:                      # [v119/v129] измеренная глубина истории фандинга (§2.51, §2.59.2): сколько суток отдаёт API
            dep = sorted((v['last'] - v['first']) / DAY for v in fs.values() if v.get('first') and v.get('last'))
            if dep:
                out.append(f'    {ex.upper()}: глубина истории фандинга (последняя − первая отметка) по {len(dep)} символам: мин {dep[0]:.0f}, медиана {dep[len(dep) // 2]:.0f}, макс {dep[-1]:.0f} сут. '
                           f'(окно позиции H2 вне этой глубины → фандинг = 0 со счётчиком, §2.51)')
            cov30 = sum(1 for s_, v in fs.items() if v.get('first') and s_ in bs and int(v['first']) <= int(bs[s_]['first']) + 3 * DAY)
            if ex in NEW_VENUES:                                    # [v129, §2.59.2] доля символов, у которых история фандинга начинается у листинга (иначе начало окна H2 без фандинга)
                nempty = sum(1 for s_ in bs if idx['neg'].get(f'{ex}|{s_}|fund', {}).get('why') == 'empty')
                out.append(f'    {ex.upper()}: история фандинга начинается у листинга (≤ 3 сут. от первого бара) у {cov30} из {len(bs)} символов; подтверждённо без истории фандинга: {nempty}')
        mk = idx['markets'].get(ex, {})
        inactive = [s for s, v in mk.items() if not v.get('active')]
        out.append(f'    неактивных рынков в markets: {len(inactive)}' + ('' if inactive else ' — закрытых инструментов API не отдаёт: ОШИБКА ВЫЖИВШЕГО (делистинги отсутствуют; смещение ПРОТИВ шорт-стратегии)'))
    return out
