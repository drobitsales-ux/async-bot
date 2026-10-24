"""research_ma.py — /data_probe_ma: проверка ИСТОЧНИКОВ данных для «фабрики» на новых классах активов (BOT_SPEC §2.41, этап 1). ТОЛЬКО метаданные и образцы:
доступность, глубина истории, формат, пропуски, выходные, часовой пояс; ни одна строка не используется для расчётов, полные данные не загружаются.
Источники: Stooq (дневные CSV), Dukascopy (часовые свечи через публичный datafeed, .bi5 = LZMA), BingX (перпетуалы на металлы/нефть/индексы: листинг, комиссии из markets,
спред по тикеру). Сеть Stooq/Dukascopy — в дочернем процессе (research_runner), BingX через общий exchange — в процессе бота ДО запуска (prepare_ma_bingx).
Форматы Dukascopy записаны по моим сведениям и НЕ проверены из среды разработки (сеть закрыта) — проба проверяет их на лету (монотонность, согласованность OHLC) и
сообщает фактическое."""
import asyncio
import lzma
import time
from datetime import datetime, timezone

import numpy as np

import research_ma_lib as L
import research_vision as V

DAY_MS = 86_400_000
STOOQ_URL = 'https://stooq.com/q/d/l/?s={sym}&i=d'
DUKA_BASE = 'https://datafeed.dukascopy.com/datafeed'
UA = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36'}

# инструменты (кандидаты §2.41.2) → класс
STOOQ_CLASS = {'xauusd': 'METALS', 'xagusd': 'METALS', 'xptusd': 'METALS',
               'eurusd': 'FX', 'gbpusd': 'FX', 'usdjpy': 'FX', 'audusd': 'FX', 'usdchf': 'FX',
               '^spx': 'INDICES', '^ndx': 'INDICES', '^dax': 'INDICES', '^nkx': 'INDICES'}
# нефть и облигации — только проверка и запись символов, в классы НЕ входят (§2.41.2); символы Stooq — кандидаты, проба сообщает, какие отвечают
STOOQ_EXTRA = ('cl.f', 'cb.f', 'ng.f', 'zn.f', 'zb.f', 'zf.f', 'zt.f', 'gc.f')
DUKA_SYMS = ('XAUUSD', 'XAGUSD', 'EURUSD', 'GBPUSD', 'USDJPY')
DUKA_CLASS = {'XAUUSD': 'METALS', 'XAGUSD': 'METALS', 'EURUSD': 'FX', 'GBPUSD': 'FX', 'USDJPY': 'FX'}
DUKA_PRICE_RANGE = {'XAUUSD': (300.0, 8000.0), 'XAGUSD': (5.0, 120.0), 'EURUSD': (0.5, 2.0), 'GBPUSD': (0.8, 2.3), 'USDJPY': (50.0, 220.0)}
DUKA_YEARS = (2005, 2008, 2010, 2012, 2014, 2016, 2018, 2019, 2020, 2022, 2024)
STOOQ_MAX_FIRST = '2019-01-01'            # история должна начинаться не позже (TRAIN с 2020 + разогрев индикаторов)
DUKA_MAX_FIRST_YEAR = 2019
BINGX_NC_PREFIX = 'NC'                    # перпетуалы BingX на не-крипто активы (NCCOGOLD2USD и т. п.)
BINGX_EXACT = ('XAU', 'XAG', 'XPT', 'XPD', 'WTI', 'BRENT', 'OIL', 'SPX', 'NAS100', 'US30', 'DAX', 'NIKKEI')
BINGX_MAX_SPREAD = 12                     # сколько кандидатов опрашивать тикером (5 замеров каждый)


class ParseError(Exception):
    pass


def _ms(date_str):
    return int(datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc).timestamp() * 1000)


def _date(ms):
    return datetime.fromtimestamp(ms / 1000, timezone.utc).strftime('%Y-%m-%d')


# ── Stooq: парсер дневного CSV ───────────────────────────────────────────────────────────────────────
def parse_stooq_csv(text: str) -> dict:
    """CSV Stooq (Date,Open,High,Low,Close[,Volume]). Ответ не CSV (apikey/«нет данных»/HTML) → ParseError с причиной.
    → dict(cols, ts[мс UTC, 00:00 даты], o,h,l,c,v, n, first, last, weekend_rows, missing_weekdays, duplicates, unsorted)."""
    t = (text or '').strip().lstrip('﻿')
    if not t:
        raise ParseError('пустой ответ')
    lines = [x for x in t.splitlines() if x.strip()]
    head = lines[0].strip()
    cols = [c.strip().lower() for c in head.split(',')]
    if 'date' not in cols or 'close' not in cols:
        tl = t.lower()
        if 'apikey' in tl or 'api key' in tl:
            raise ParseError(f'требуется apikey: {t[:160]!r}')
        if 'no data' in tl or 'brak danych' in tl:
            raise ParseError('нет данных по символу')
        raise ParseError(f'ответ не CSV с колонками Date/Close (первые 160 символов: {t[:160]!r})')
    ix = {c: i for i, c in enumerate(cols)}
    rows = []
    for ln in lines[1:]:
        p = ln.split(',')
        try:
            ts = _ms(p[ix['date']].strip())
            f = lambda k: float(p[ix[k]]) if k in ix and len(p) > ix[k] and p[ix[k]].strip() not in ('', 'null') else float('nan')
            rows.append((ts, f('open'), f('high'), f('low'), f('close'), f('volume')))
        except (ValueError, IndexError):
            continue
    if not rows:
        raise ParseError('нет строк данных после заголовка')
    unsorted = any(rows[i][0] > rows[i + 1][0] for i in range(len(rows) - 1))
    rows.sort(key=lambda r: r[0])
    arr = np.array(rows, dtype=np.float64)
    ts = arr[:, 0].astype(np.int64)
    dup = int(np.sum(np.diff(ts) == 0)) if len(ts) > 1 else 0
    wd = ((ts // DAY_MS) + 3) % 7                      # 0=пн … 6=вс (1970-01-01 — четверг)
    weekend_rows = int(np.sum(wd >= 5))
    # пропущенные будние дни между first и last (праздники входят)
    days = np.arange(ts[0] // DAY_MS, ts[-1] // DAY_MS + 1)
    biz = int(np.sum(((days + 3) % 7) < 5))
    uts = np.unique(ts)
    have_biz = int(np.sum(((uts // DAY_MS) + 3) % 7 < 5))         # уникальные будние даты (дубли покрытие не завышают)
    return {'cols': [c for c in head.split(',')], 'ts': ts, 'o': arr[:, 1], 'h': arr[:, 2], 'l': arr[:, 3], 'c': arr[:, 4], 'v': arr[:, 5], 'n': len(ts),
            'first': _date(int(ts[0])), 'last': _date(int(ts[-1])), 'weekend_rows': weekend_rows, 'missing_weekdays': max(0, biz - have_biz),
            'weekday_coverage': have_biz / biz if biz else 0.0, 'duplicates': dup, 'unsorted': unsorted, 'has_volume': not bool(np.all(np.isnan(arr[:, 5])))}


def stooq_verdict(p: dict, now_ms: int):
    """→ (пригоден: bool, причина). Критерии: история с ≤ 2019-01-01, последняя дата не старше 10 дней, покрытие будних дней ≥ 90%, нет дублей."""
    why = []
    if p['first'] > STOOQ_MAX_FIRST:
        why.append(f'история начинается {p["first"]} (> {STOOQ_MAX_FIRST})')
    if now_ms - _ms(p['last']) > 10 * DAY_MS:
        why.append(f'последняя дата {p["last"]} старше 10 дней')
    if p['weekday_coverage'] < 0.90:
        why.append(f'покрытие будних дней {p["weekday_coverage"] * 100:.0f}% (< 90%)')
    if p['duplicates']:
        why.append(f'дубли дат: {p["duplicates"]}')
    return (not why), ('; '.join(why) if why else 'история достаточна, свежие данные, пропусков мало')


# ── Dukascopy: часовые свечи (.bi5 = LZMA) ───────────────────────────────────────────────────────────
def duka_hour_url(sym: str, year: int, month0: int) -> str:
    """Месячный файл часовых BID-свечей; месяц в пути 0-базовый (00 = январь)."""
    return f'{DUKA_BASE}/{sym}/{year}/{month0:02d}/BID_candles_hour_1.bi5'


def duka_tick_url(sym: str, year: int, month0: int, day: int, hour: int) -> str:
    return f'{DUKA_BASE}/{sym}/{year}/{month0:02d}/{day:02d}/{hour:02d}h_ticks.bi5'


def decode_bi5(body: bytes) -> bytes:
    """LZMA (формат .lzma «alone») → сырые байты; ошибка → ParseError."""
    try:
        return lzma.decompress(body)
    except lzma.LZMAError as e:
        raise ParseError(f'не LZMA: {e}; первые байты {body[:8].hex()}')


def parse_duka_candles(raw: bytes, period_start_ms: int, order: str = 'ocLH') -> dict:
    """Распакованный файл свечей: записи по 24 байта big-endian `>IIIIIf` = (секунды, 4 цены int, объём float). order — порядок цен: 'ocLH' (open, close, low, high —
    по моим сведениям о формате) или 'ohlc'. Время — смещение секунд от начала периода файла (если значение > 1e9 — абсолютные секунды). Цены — СЫРЫЕ int (масштаб —
    pick_scale). → dict(ts мс, o,h,l,c сырые float, v, n, rel_time: bool)."""
    if len(raw) % 24:
        raise ParseError(f'размер {len(raw)} не кратен 24 байтам записи')
    n = len(raw) // 24
    if n == 0:
        raise ParseError('пустой файл свечей')
    rec = np.frombuffer(raw, dtype=np.dtype([('t', '>u4'), ('a', '>u4'), ('b', '>u4'), ('c', '>u4'), ('d', '>u4'), ('v', '>f4')]))
    t = rec['t'].astype(np.int64)
    rel = bool(t.max() < 1_000_000_000)
    ts = (period_start_ms + t * 1000) if rel else t * 1000
    a, b, c, d = (rec[k].astype(np.float64) for k in ('a', 'b', 'c', 'd'))
    if order == 'ocLH':
        o, cl, lo, hi = a, b, c, d
    elif order == 'ohlc':
        o, hi, lo, cl = a, b, c, d
    else:
        raise ParseError(f'неизвестный порядок полей {order}')
    return {'ts': ts, 'o': o, 'h': hi, 'l': lo, 'c': cl, 'v': rec['v'].astype(np.float64), 'n': n, 'rel_time': rel}


def ohlc_consistency(p: dict) -> float:
    """Доля баров с low ≤ min(open, close) ≤ max(open, close) ≤ high."""
    ok = (p['l'] <= np.minimum(p['o'], p['c'])) & (np.maximum(p['o'], p['c']) <= p['h'])
    return float(np.mean(ok))


def pick_scale(raw_close_median: float, lo: float, hi: float):
    """Масштаб цены (делитель сырых int): первый из 1e5, 1e3, 1e2, 1, при котором медиана цены попадает в правдоподобный диапазон инструмента; None — ни один."""
    for sc in (1e5, 1e3, 1e2, 1.0):
        v = raw_close_median / sc
        if lo <= v <= hi:
            return sc
    return None


def duka_analyze(p: dict, sym: str, period_start_ms: int):
    """Проверки образца месячного файла часовых свечей: монотонность, шаг 1 час, пропуски в будни, бары выходных, час первого бара после выходных (UTC-подтверждение).
    → dict метрик."""
    ts = p['ts']
    d = np.diff(ts)
    steps = {int(x): int(c) for x, c in zip(*np.unique(d, return_counts=True))} if len(d) else {}
    hrs = 3_600_000
    mono = bool(np.all(d > 0)) if len(d) else True
    wd = ((ts // DAY_MS) + 3) % 7
    hour = (ts // hrs) % 24
    # первый бар после разрыва ≥ 24 ч — открытие недели
    gaps = np.where(d >= 24 * hrs)[0]
    reopen_hours = sorted({int(hour[i + 1]) for i in gaps})
    reopen_wd = sorted({int(wd[i + 1]) for i in gaps})
    biz_hours = int(np.sum(wd < 5)) if len(ts) else 0
    return {'n': int(p['n']), 'monotonic': mono, 'hourly_share': (steps.get(hrs, 0) / len(d)) if len(d) else 1.0, 'steps': dict(sorted(steps.items())[:5]),
            'weekend_bars': int(np.sum(wd >= 5)), 'reopen_hours_utc': reopen_hours, 'reopen_weekdays': reopen_wd,
            'first': datetime.fromtimestamp(int(ts[0]) / 1000, timezone.utc).strftime('%Y-%m-%d %H:%M'),
            'last': datetime.fromtimestamp(int(ts[-1]) / 1000, timezone.utc).strftime('%Y-%m-%d %H:%M'), 'biz_bars': biz_hours,
            'in_period': bool(ts[0] >= period_start_ms and ts[-1] < period_start_ms + 32 * DAY_MS)}


def _month_start_ms(year, month0):
    return int(datetime(year, month0 + 1, 1, tzinfo=timezone.utc).timestamp() * 1000)


async def probe_stooq(fetch, now_ms, pause=0.4, progress=None):
    out = {}
    for sym in list(STOOQ_CLASS) + list(STOOQ_EXTRA):
        r = await fetch(STOOQ_URL.format(sym=sym.replace('^', '%5E')), None, 'GET')
        await asyncio.sleep(pause)
        rec = {'class': STOOQ_CLASS.get(sym, 'EXTRA (нефть/облигации — не в классах)'), 'status': r['status'], 'bytes': len(r['body']), 'error': r['error']}
        if r['status'] == 200:
            try:
                p = parse_stooq_csv(r['body'].decode('utf-8', 'replace'))
                ok, why = stooq_verdict(p, now_ms)
                rec.update(parsed=True, ok=ok, why=why, cols=p['cols'], first=p['first'], last=p['last'], n=p['n'], weekend_rows=p['weekend_rows'],
                           missing_weekdays=p['missing_weekdays'], coverage=p['weekday_coverage'], has_volume=p['has_volume'], unsorted=p['unsorted'],
                           csv_bytes=len(r['body']))
            except ParseError as e:
                rec.update(parsed=False, ok=False, why=f'не разобран: {e}')
        else:
            rec.update(parsed=False, ok=False, why=f'HTTP {r["status"]} {r["error"]}'.strip())
        out[sym] = rec
        if progress:
            try:
                await progress(f'Stooq {sym}: {"пригоден" if rec["ok"] else "НЕпригоден"} ({rec["why"][:90]})')
            except Exception:
                pass
    return out


async def _duka_get(fetch, url, pause):
    r = await fetch(url, None, 'GET')
    await asyncio.sleep(pause)
    return r


async def probe_dukascopy(fetch, now_ms, pause=0.3, progress=None):
    out = {}
    now = datetime.fromtimestamp(now_ms / 1000, timezone.utc)
    ly, lm = (now.year, now.month - 2) if now.month >= 2 else (now.year - 1, 11)      # последний ЗАВЕРШЁННЫЙ месяц (month0: янв = 0)
    for sym in DUKA_SYMS:
        rec = {'class': DUKA_CLASS[sym], 'depth': {}, 'sample': None, 'tick': None, 'ok': False, 'why': ''}
        for y in DUKA_YEARS:
            r = await _duka_get(fetch, duka_hour_url(sym, y, 0), pause)
            rec['depth'][y] = (r['status'], len(r['body']))
        have = [y for y, (st, n) in rec['depth'].items() if st == 200 and n > 0]
        rec['earliest_year'] = min(have) if have else None
        r = await _duka_get(fetch, duka_hour_url(sym, ly, lm), pause)
        rec['sample'] = {'url': duka_hour_url(sym, ly, lm), 'status': r['status'], 'bytes': len(r['body']), 'error': r['error']}
        if r['status'] == 200 and r['body']:
            try:
                raw = decode_bi5(r['body'])
                ps = _month_start_ms(ly, lm)
                best = None
                for order in ('ocLH', 'ohlc'):
                    p = parse_duka_candles(raw, ps, order)
                    cons = ohlc_consistency(p)
                    if best is None or cons > best[1]:
                        best = (order, cons, p)
                order, cons, p = best
                an = duka_analyze(p, sym, ps)
                lo, hi = DUKA_PRICE_RANGE[sym]
                sc = pick_scale(float(np.median(p['c'])), lo, hi)
                rec['sample'].update(raw_bytes=len(raw), order=order, consistency=cons, scale=sc, analysis=an, rel_time=p['rel_time'])
                bad = []
                if cons < 0.99:
                    bad.append(f'согласованность OHLC {cons * 100:.0f}% (формат записи не распознан)')
                if not an['monotonic']:
                    bad.append('время не монотонно')
                if sc is None:
                    bad.append('масштаб цены не определён')
                if an['hourly_share'] < 0.8:
                    bad.append(f'часовой шаг у {an["hourly_share"] * 100:.0f}% баров')
                if rec['earliest_year'] is None or rec['earliest_year'] > DUKA_MAX_FIRST_YEAR:
                    bad.append(f'глубина: самый ранний проверенный январь {rec["earliest_year"]} (> {DUKA_MAX_FIRST_YEAR})')
                rec['ok'], rec['why'] = (not bad), ('; '.join(bad) if bad else 'формат распознан, часовой шаг, история достаточна')
            except ParseError as e:
                rec['why'] = f'не разобран: {e}'
        else:
            rec['why'] = f'HTTP {r["status"]} {r["error"]}'.strip()
        # запасной формат — тики за один рабочий час (проверка доступности)
        tr = await _duka_get(fetch, duka_tick_url(sym, ly, lm, 10, 12), pause)
        rec['tick'] = {'status': tr['status'], 'bytes': len(tr['body'])}
        out[sym] = rec
        if progress:
            try:
                await progress(f'Dukascopy {sym}: {"пригоден" if rec["ok"] else "НЕпригоден"} ({rec["why"][:90]})')
            except Exception:
                pass
    return out


# ── BingX: перпетуалы на не-крипто активы (в процессе бота, через общий exchange) ────────────────────
def bingx_candidates(markets: dict):
    """Свопы BingX на металлы/нефть/индексы/FX: base начинается с 'NC' либо точное совпадение из BINGX_EXACT."""
    out = []
    for sym, m in (markets or {}).items():
        if not (m.get('swap') or m.get('type') == 'swap'):
            continue
        base = str(m.get('base') or '').upper()
        mid = str(m.get('id') or '').upper()
        if base.startswith(BINGX_NC_PREFIX) or base in BINGX_EXACT or mid.startswith(BINGX_NC_PREFIX):
            out.append(sym)
    return sorted(out)


def _priority(sym: str) -> int:
    s = sym.upper()
    for i, k in enumerate(('GOLD', 'XAU', 'SILVER', 'XAG', 'PLAT', 'XPT', 'OIL', 'WTI', 'BRENT', 'SPX', 'SP500', 'NAS', 'NDX', 'DAX', 'EUR', 'GBP', 'JPY')):
        if k in s:
            return i
    return 99


def market_row(sym: str, m: dict) -> dict:
    info = m.get('info') or {}
    created = m.get('created')
    if created in (None, '', 0):
        for k in ('launchTime', 'onlineTime', 'listTime', 'launch_time', 'listingTime'):
            if info.get(k) not in (None, '', '0', 0):
                created = info.get(k)
                break
    try:
        created_s = _date(int(created)) if created not in (None, '') and int(created) > 10 ** 11 else (str(created) if created else None)
    except (TypeError, ValueError):
        created_s = str(created) if created else None
    return {'symbol': sym, 'id': m.get('id'), 'created': created_s, 'taker': m.get('taker'), 'maker': m.get('maker'), 'contract_size': m.get('contractSize'),
            'active': m.get('active'), 'info_keys': sorted(info.keys())[:14]}


async def prepare_ma_bingx(exchange, progress=None, samples=5, pause=1.0):
    """Сетевая часть (процесс бота, общий exchange): markets → кандидаты → по 5 замеров bid/ask тикером (спред, б.п.). → payload для дочернего процесса."""
    mk = exchange.markets or await exchange.load_markets()
    cand = bingx_candidates(mk)
    rows = [market_row(s, mk[s]) for s in cand]
    if progress:
        await progress(f'BingX: кандидатов на не-крипто перпетуалы {len(cand)}; замеры спреда: {min(len(cand), BINGX_MAX_SPREAD)} × {samples}')
    for r in sorted(rows, key=lambda x: (_priority(x['symbol']), x['symbol']))[:BINGX_MAX_SPREAD]:
        sp = []
        for _ in range(samples):
            try:
                t = await exchange.fetch_ticker(r['symbol'])
                bid, ask = float(t.get('bid') or 0), float(t.get('ask') or 0)
                if bid > 0 and ask > 0:
                    sp.append((ask - bid) / ((ask + bid) / 2) * 1e4)
            except Exception as e:
                r['ticker_error'] = f'{type(e).__name__}: {str(e)[:80]}'
            await asyncio.sleep(pause)
        r['spread_bps'] = sp
    return {'rows': rows, 'n_markets': len(mk)}


# ── отчёт ────────────────────────────────────────────────────────────────────────────────────────────
def proposal(stooq: dict, duka: dict):
    """Предложение точных списков для поправки к §2.41.2: инструменты Stooq (пригодные) по классам; ячейки (класс, tf, n) и N по формуле пре-регистрации."""
    cls = {'METALS': [], 'FX': [], 'INDICES': []}
    for sym, rec in stooq.items():
        c = STOOQ_CLASS.get(sym)
        if c and rec.get('ok'):
            cls[c].append(sym)
    cells = [('CRYPTO', tf, 16) for tf in L.TIMEFRAMES]
    for c in ('METALS', 'FX', 'INDICES'):
        if cls[c]:
            cells.append((c, '1D', len(cls[c])))
    for c in ('METALS', 'FX'):
        ds = [s for s, r in duka.items() if DUKA_CLASS[s] == c and r.get('ok')]
        if ds:
            for tf in ('4h', '1h'):
                cells.append((c, tf, len(ds)))
    return {'classes': cls, 'cells': cells, 'N': L.count_tests(cells)}


def _line_stooq(sym, r):
    if not r.get('parsed'):
        return f'  {sym} [{r["class"]}]: НЕПРИГОДЕН — {r["why"]}'
    return (f'  {sym} [{r["class"]}]: {"ПРИГОДЕН" if r["ok"] else "НЕПРИГОДЕН"} — {r["why"]} | {r["first"]} … {r["last"]}, строк {r["n"]}, колонки {",".join(r["cols"])}, '
            f'объём {"есть" if r["has_volume"] else "нет"}, строк в выходные {r["weekend_rows"]}, пропущено будних дней {r["missing_weekdays"]} (покрытие {r["coverage"] * 100:.0f}%), '
            f'CSV {r["csv_bytes"] / 1024:.0f} КБ')


def build_report(stooq, duka, bingx, now_ms, disk_mb, version=''):
    L_ = ['📡 <b>/data_probe_ma: проверка источников для «фабрики» (BOT_SPEC §2.41, этап 1; только метаданные, расчётов нет)</b>',
          f'Время проверки: {datetime.fromtimestamp(now_ms / 1000, timezone.utc):%Y-%m-%d %H:%M} UTC; запросы шли с сервера бота (Stooq/Dukascopy — из дочернего процесса). {version}']
    L_.append('\n<b>Stooq, дневные CSV</b> (время — календарная дата без часового пояса: время закрытия дня Stooq не документирует; единицы — цены в единицах котировки):')
    for sym, r in stooq.items():
        L_.append(_line_stooq(sym, r))
    ok_s = [s for s, r in stooq.items() if r.get('ok') and s in STOOQ_CLASS]
    ex_ok = [s for s, r in stooq.items() if s in STOOQ_EXTRA and r.get('ok')]
    L_.append(f'  Итого по классам: пригодны {len(ok_s)} из {len(STOOQ_CLASS)}. Нефть/облигации (в классы НЕ входят, §2.41.2): отвечают символы — {", ".join(ex_ok) if ex_ok else "ни один"}.')
    L_.append('\n<b>Dukascopy, часовые BID-свечи</b> (datafeed.dukascopy.com, месячные файлы `BID_candles_hour_1.bi5`, LZMA; формат — по моим сведениям, проверен на лету):')
    for sym, r in duka.items():
        smp = r['sample'] or {}
        L_.append(f'  {sym} [{r["class"]}]: {"ПРИГОДЕН" if r["ok"] else "НЕПРИГОДЕН"} — {r["why"]}')
        L_.append('    глубина (январь года → HTTP, байт): ' + ', '.join(f'{y}:{st}/{n}' for y, (st, n) in r['depth'].items()) + f'; самый ранний с данными: {r["earliest_year"]}')
        if smp.get('analysis'):
            a = smp['analysis']
            L_.append(f'    образец {smp["url"].split("/datafeed/")[1]}: HTTP {smp["status"]}, {smp["bytes"]} Б → {smp["raw_bytes"]} Б, баров {a["n"]} ({a["first"]} … {a["last"]} UTC), порядок полей {smp["order"]} '
                      f'(согласованность OHLC {smp["consistency"] * 100:.0f}%), масштаб цены ÷{smp["scale"]:g}, время — {"смещение от начала месяца" if smp["rel_time"] else "абсолютные секунды"}, '
                      f'шаг 1 ч у {a["hourly_share"] * 100:.0f}% баров, баров в выходные {a["weekend_bars"]}, открытие недели в {a["reopen_hours_utc"]} ч UTC (дни {a["reopen_weekdays"]}; '
                      f'воскресенье 21–22 ч UTC подтверждает UTC), шаги {a["steps"]}')
        elif smp:
            L_.append(f'    образец: HTTP {smp.get("status")} {smp.get("error", "")} ({smp.get("bytes")} Б)')
        L_.append(f'    запасной формат (тики за час): HTTP {r["tick"]["status"]}, {r["tick"]["bytes"]} Б')
    L_.append('\n<b>BingX: перпетуалы на металлы/нефть/индексы/FX</b> (комиссии — из markets; спред — медиана 5 замеров bid/ask):')
    rows = (bingx or {}).get('rows') or []
    if not rows:
        L_.append(f'  кандидатов (base начинается с NC или точное имя) не найдено среди {((bingx or {}).get("n_markets") or 0)} рынков — сценарий (а) недоступен.')
    for r in sorted(rows, key=lambda x: (_priority(x['symbol']), x['symbol']))[:60]:
        sp = r.get('spread_bps')
        sps = (f'спред {float(np.median(sp)):.1f} б.п. (замеров {len(sp)}, разброс {min(sp):.1f}…{max(sp):.1f})' if sp else ('спред не измерен' + (f' ({r["ticker_error"]})' if r.get('ticker_error') else '')))
        L_.append(f'  {r["symbol"]} (id {r["id"]}): листинг {r["created"] or "н/д"}, taker {r["taker"]}, maker {r["maker"]}, размер контракта {r["contract_size"]}, активен {r["active"]}, {sps}')
    if rows and all(r['created'] is None for r in rows):
        L_.append(f'  ⚠️ дата листинга в markets отсутствует — поля info: {rows[0]["info_keys"]}')
    prop = proposal(stooq, duka)
    L_.append('\n<b>Оценка объёма и диска (хранение — сжатые .npz, как в v92)</b>')
    st_kb = sum(r.get('csv_bytes', 0) for r in stooq.values()) / 1024
    n_inst = len(ok_s)
    duka_ok = [s for s, r in duka.items() if r.get('ok')]
    avg_month = (sum((r['sample'] or {}).get('bytes', 0) for r in duka.values() if r.get('ok')) / max(1, len(duka_ok)))
    months = (datetime.fromtimestamp(now_ms / 1000, timezone.utc).year - 2018) * 12 + 10
    L_.append(f'  Stooq: {n_inst + len(ex_ok)} запросов, CSV ≈ {st_kb / 1024:.1f} МБ суммарно, в .npz ≈ {st_kb * 0.35 / 1024:.1f} МБ.')
    L_.append(f'  Dukascopy: {len(duka_ok)} инструментов × ≈ {months} месячных файлов с 2018 = ≈ {len(duka_ok) * months} запросов (пауза {V.pause_s():g} с ⇒ ≈ {len(duka_ok) * months * (V.pause_s() + 0.3) / 60:.0f} мин), '
              f'сжатые файлы ≈ {len(duka_ok) * months * avg_month / 1e6:.1f} МБ, в .npz (float32 OHLCV) ≈ {len(duka_ok) * months * 720 * 20 / 1e6 * 0.5:.1f} МБ.')
    L_.append(f'  Свободно на диске: {disk_mb:.0f} МБ (порог остановки загрузки {V.MIN_FREE_MB} МБ).')
    L_.append('\n<b>ВЫВОД (предложение для поправки к §2.41.2 — решает владелец; добавлять инструменты после поправки нельзя):</b>')
    for c, syms in prop['classes'].items():
        L_.append(f'  {c} (1D, Stooq): {", ".join(syms) if syms else "нет пригодных"} — {len(syms)} инструментов')
    for c in ('METALS', 'FX'):
        ds = [s for s, r in duka.items() if DUKA_CLASS[s] == c and r.get('ok')]
        L_.append(f'  {c} 4h/1h (Dukascopy): ' + (', '.join(ds) if ds else 'нет пригодных — только 1D'))
    L_.append(f'  Ячейки (класс, таймфрейм, число инструментов): {prop["cells"]}')
    lo, hi = L.n_bounds()
    L_.append(f'  <b>Число тестов N по формуле §2.41.5 для этих ячеек = {prop["N"]}</b> (границы пре-регистрации {lo}…{hi}).')
    L_.append('  Сценарий (а) (перпетуалы BingX) доступен только для инструментов из списка выше, найденных в разделе BingX; остальные оцениваются по (б) с пометкой.')
    L_.append('<b>ВЕРДИКТ источников: ' + ('Stooq — пригоден для ' + str(n_inst) + ' из ' + str(len(STOOQ_CLASS)) + ' инструментов' if ok_s else 'Stooq — непригоден') + '; Dukascopy — ' +
              ('пригоден для ' + ', '.join(duka_ok) if duka_ok else 'непригоден') + '; BingX не-крипто перпетуалов: ' + str(len(rows)) + '.</b>')
    return '\n'.join(L_)


async def run_probe_ma(payload, params, progress=None):
    """Дочерний процесс: Stooq + Dukascopy (aiohttp) + отчёт; payload = результат prepare_ma_bingx (или None). → текст отчёта."""
    import aiohttp
    now_ms = int(time.time() * 1000)
    async with aiohttp.ClientSession(headers=UA) as ses:
        fetch = V.make_fetcher(ses)
        stooq = await probe_stooq(fetch, now_ms, progress=progress)
        duka = await probe_dukascopy(fetch, now_ms, progress=progress)
    try:
        disk = V.free_mb(params.get('vision_dir') or V.vision_dir())
    except Exception:
        disk = 0.0
    return build_report(stooq, duka, payload, now_ms, disk, params.get('version', ''))
