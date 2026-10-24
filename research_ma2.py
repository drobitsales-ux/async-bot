"""research_ma2.py — /data_probe_ma, ПРОБА v2 (BOT_SPEC §2.41.14): качество данных Dukascopy, расширение списка инструментов, спреды из истории и по стакану BingX, диагностика Stooq.
ТОЛЬКО метаданные и образцы (форматы, выходные, часовой пояс, спреды, правила агрегации дневок); ни одной доходности стратегий не считается. Результат — основа для проекта
поправки к §2.41 (после подтверждения владельцем). Форматы Dukascopy и имена инструментов — по моим сведениям, проверяются на лету; имена, не найденные в datafeed, помечаются
«не найден» (перебираются варианты написания). Сеть Stooq/Dukascopy — в дочернем процессе (research_runner); BingX (стакан) — через общий exchange в процессе бота ДО запуска."""
import asyncio
import socket
import time
from collections import Counter
from datetime import datetime, timedelta, timezone

import numpy as np

import research_ma as M
import research_ma_lib as L
import research_vision as V

DAY_MS = 86_400_000
HOUR_MS = 3_600_000
WD = ('Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб', 'Вс')

# цель → (класс, варианты имён папки в datafeed, правдоподобный диапазон цены для выбора масштаба)
TARGETS = {
    'XAUUSD': ('METALS', ['XAUUSD'], (300.0, 8000.0)),
    'XAGUSD': ('METALS', ['XAGUSD'], (5.0, 120.0)),
    'XPTUSD': ('METALS', ['XPTUSD'], (400.0, 4000.0)),
    'BRENT': ('ENERGY', ['BRENTCMDUSD'], (10.0, 200.0)),
    'WTI': ('ENERGY', ['LIGHTCMDUSD', 'WTICMDUSD'], (10.0, 200.0)),
    'EURUSD': ('FX', ['EURUSD'], (0.5, 2.0)),
    'GBPUSD': ('FX', ['GBPUSD'], (0.8, 2.3)),
    'USDJPY': ('FX', ['USDJPY'], (50.0, 220.0)),
    'AUDUSD': ('FX', ['AUDUSD'], (0.4, 1.2)),
    'USDCHF': ('FX', ['USDCHF'], (0.5, 1.5)),
    'USDCAD': ('FX', ['USDCAD'], (0.9, 1.7)),
    'NZDUSD': ('FX', ['NZDUSD'], (0.4, 1.0)),
    'SPX': ('INDICES', ['USA500IDXUSD'], (800.0, 12000.0)),
    'NDX': ('INDICES', ['USATECHIDXUSD'], (2000.0, 40000.0)),
    'DAX': ('INDICES', ['DEUIDXEUR'], (4000.0, 30000.0)),
    'NIKKEI': ('INDICES', ['JPNIDXJPY'], (8000.0, 70000.0)),
    'FTSE': ('INDICES', ['GBRIDXGBP'], (3000.0, 12000.0)),
    'BUND': ('BONDS', ['BUNDTREUR', 'DEUBUNDTREUR'], (80.0, 200.0)),
    'TNOTE': ('BONDS', ['USTBONDTRUSD', 'USTNOTETRUSD'], (80.0, 200.0)),
}
CLASS_SAMPLE = {'METALS': 'XAUUSD', 'FX': 'EURUSD', 'INDICES': 'SPX'}        # образец года для проверки агрегации дневок (1.5)
TZ_SYMS = ('EURUSD', 'XAUUSD')
DEPTH_YEARS = (2008, 2012, 2016, 2018, 2019, 2020, 2022, 2024)
DEPTH_MAX_FIRST_YEAR = 2019
AGG_YEAR = 2024
SPREAD_SAMPLES = ((2021, 5), (2023, 1))           # (год, month0) — плюс последний завершённый месяц
ACTIVE_HOURS = range(7, 21)                       # 07:00–20:59 UTC в будни — «активные» часы
STUB_BARS = 4                                     # дневной бар из < 4 живых часовых баров — «обрубок»
RUN_MIN = 12                                      # серия из ≥ 12 подряд идущих плоских часовых баров — закрытие рынка (правило исключения)
DAYS_RANGE = (250, 262)                           # ожидаемое число дневных баров в год (~252–260)
BINGX_BOOK = {'золото': 'NCCOGOLD2USD', 'серебро': 'NCCOXAG2USD', 'платина': 'NCCOXPT2USD', 'палладий': 'NCCOPALLADIUM2USD', 'Brent': 'NCCO1OILBRENT2USD',
              'WTI': 'NCCO1OILWTI2USD', 'газ': 'NCCO7241NATGAS2USD', 'медь': 'NCCO724COPPER2USD', 'S&P 500': 'NCSISP5002USD', 'Nasdaq 100': 'NCSINASDAQ1002USD'}


# ── время: летнее время США (для границы дня FX «закрытие Нью-Йорка») ─────────────────────────────────
def _nth_sunday(year, month, n):
    d = datetime(year, month, 1, tzinfo=timezone.utc)
    first = d + timedelta(days=(6 - d.weekday()) % 7)
    return first + timedelta(days=7 * (n - 1))


def us_dst(ts_ms) -> bool:
    """Нью-Йорк на летнем времени (EDT)? С 2-го воскресенья марта 07:00 UTC по 1-е воскресенье ноября 06:00 UTC (правило США с 2007 г.)."""
    t = datetime.fromtimestamp(ts_ms / 1000, timezone.utc)
    start = _nth_sunday(t.year, 3, 2).replace(hour=7)
    end = _nth_sunday(t.year, 11, 1).replace(hour=6)
    return start <= t < end


def ny_offset_ms(ts_ms) -> int:
    return (-4 if us_dst(ts_ms) else -5) * HOUR_MS


# ── 1.1 выходные: плоские бары и окна закрытия ───────────────────────────────────────────────────────
def flat_mask(p: dict):
    """Бары, где O=H=L=C."""
    return (p['o'] == p['h']) & (p['h'] == p['l']) & (p['l'] == p['c'])


def _cells(ts):
    d = ts // DAY_MS
    wd = (d + 3) % 7                                # 0 = пн
    hr = (ts // HOUR_MS) % 24
    return (wd * 24 + hr).astype(np.int64)


def weekend_stats(p: dict, min_share=0.9, min_n=3):
    """Структура закрытия рынка по данным месяца(ев): доля плоских баров (всего/выходные Сб+Вс/будни), объём и нулевой объём в окнах закрытия и вне их, окна закрытия (ячейки
    «день недели × час UTC», где ≥ 90% баров плоские), число живых (не плоских) баров внутри окон закрытия. → dict."""
    ts = p['ts']
    flat = flat_mask(p)
    cell = _cells(ts)
    n = np.bincount(cell, minlength=168)
    f = np.bincount(cell, weights=flat.astype(float), minlength=168)
    closed_cell = (n >= min_n) & (f / np.maximum(n, 1) >= min_share)
    in_closed = closed_cell[cell]
    wd = cell // 24
    weekend = wd >= 5
    v = p['v']
    zero = v == 0
    ranges = []
    if closed_cell.all():
        ranges = [(0, 168)]
    elif closed_cell.any():
        off = int(np.argmax(~closed_cell))                 # обход недели по кругу, начиная с первой незакрытой ячейки
        run, start = 0, None
        for k in range(168):
            c = (off + k) % 168
            if closed_cell[c]:
                if run == 0:
                    start = c
                run += 1
            elif run:
                ranges.append((start, run)); run = 0
        if run:
            ranges.append((start, run))
    def share(mask, base):
        return float(np.sum(mask & base) / max(1, np.sum(base)))
    return {'n': int(len(ts)), 'flat_share': float(flat.mean()), 'flat_weekend': share(flat, weekend), 'flat_weekday': share(flat, ~weekend),
            'zero_vol_closed': share(zero, in_closed), 'zero_vol_open': share(zero, ~in_closed),
            'mean_vol_closed': float(v[in_closed].mean()) if in_closed.any() else 0.0, 'mean_vol_open': float(v[~in_closed].mean()) if (~in_closed).any() else 0.0,
            'closed_bars': int(in_closed.sum()), 'live_in_closed': int(np.sum(in_closed & ~flat)), 'flat_outside_closed': int(np.sum(flat & ~in_closed)),
            'ranges': [(int(s), int(r)) for s, r in ranges], 'closed_mask': in_closed, 'flat': flat, 'run_excluded': int(closure_mask(p).sum())}


def fmt_range(start_cell: int, length: int) -> str:
    a_wd, a_h = divmod(start_cell, 24)
    e = (start_cell + length) % 168
    e_wd, e_h = divmod(e, 24)
    return f'{WD[a_wd]} {a_h:02d}:00 → {WD[e_wd]} {e_h:02d}:00 UTC ({length} ч)'


def closure_mask(p: dict, min_run: int = RUN_MIN):
    """Бары, входящие в серию из ≥ min_run ПОДРЯД идущих (шаг ровно 1 час) плоских баров (O=H=L=C) — закрытие рынка. Устойчиво к переходу на летнее время: окно закрытия сдвигается на час
    между зимой и летом, и «ячейки день×час» (weekend_stats) на длинной выборке дают ложные полуоткрытые часы (проверено тестом на годовом образце); серии — нет. Одиночные плоские
    бары и короткие серии (ночной перерыв, реальные нулевые часы) НЕ исключаются."""
    ts = p['ts']
    flat = flat_mask(p)
    n = len(ts)
    out = np.zeros(n, bool)
    i = 0
    while i < n:
        if not flat[i]:
            i += 1
            continue
        j = i
        while j + 1 < n and flat[j + 1] and ts[j + 1] - ts[j] == HOUR_MS:
            j += 1
        if j - i + 1 >= min_run:
            out[i:j + 1] = True
        i = j + 1
    return out


def exclusion_mask(p: dict, ws: dict = None):
    """ПРАВИЛО ИСКЛЮЧЕНИЯ (предложение пробы v2): исключается бар, у которого O=H=L=C И который входит в серию ≥ 12 подряд плоских часовых баров (закрытие рынка). Отличие от формулировки
    «O=H=L=C и время в окне закрытия»: окно по ячейкам «день×час» плывёт с летним временем (см. closure_mask); окна в отчёте остаются справочными. Параметр ws оставлен для совместимости."""
    return closure_mask(p)


# ── 1.2 часовой пояс: первый «живой» бар недели и последний бар пятницы ──────────────────────────────
def week_edges(p: dict):
    """Живой бар = не плоский. Разрывы между живыми барами ≥ 24 ч — выходные: бар после разрыва = открытие недели, бар перед = закрытие (пятница). → (открытия, закрытия): списки (день
    недели, час UTC) (начало бара)."""
    ts = p['ts']
    live_ts = ts[~flat_mask(p)]
    if len(live_ts) < 2:
        return [], []
    d = np.diff(live_ts)
    gaps = np.where(d >= 24 * HOUR_MS)[0]
    cell = lambda t: (int(((t // DAY_MS) + 3) % 7), int((t // HOUR_MS) % 24))
    opens = [cell(int(live_ts[i + 1])) for i in gaps]
    closes = [cell(int(live_ts[i])) for i in gaps]
    return opens, closes


def _mode(lst):
    return Counter(lst).most_common(1)[0][0] if lst else None


def tz_verdict(jan, jul):
    """Вердикт по часовому поясу. jan/jul = (opens, closes) из week_edges. Ожидание для UTC-меток и расписания FX по Нью-Йорку (закрытие пт 17:00 NY): зима — открытие вс 22 ч, последний бар
    пт 21 ч; лето — вс 21 ч и пт 20 ч (все значения — начало часового бара, UTC). → dict(utc_confirmed True/False/None, text)."""
    jo, jc, uo, uc = _mode(jan[0]), _mode(jan[1]), _mode(jul[0]), _mode(jul[1])
    obs = f'зима: открытие {fmt_cell(jo)}, последний бар недели {fmt_cell(jc)}; лето: открытие {fmt_cell(uo)}, последний бар {fmt_cell(uc)}'
    if None in (jo, jc, uo, uc):
        return {'utc_confirmed': None, 'text': f'недостаточно живых баров/разрывов для вывода ({obs})', 'obs': obs}
    if jo == (6, 22) and jc == (4, 21) and uo == (6, 21) and uc == (4, 20):
        return {'utc_confirmed': True, 'text': f'UTC подтверждён: сезонный сдвиг на 1 ч, часы совпали с ожиданием ({obs})', 'obs': obs}
    shift_o = (uo[0] * 24 + uo[1]) - (jo[0] * 24 + jo[1])
    shift_c = (uc[0] * 24 + uc[1]) - (jc[0] * 24 + jc[1])
    if shift_o == -1 and shift_c == -1:
        return {'utc_confirmed': None, 'text': f'сезонный сдвиг на 1 ч есть (метки привязаны к UTC, расписание — к США), но абсолютные часы отличаются от ожидаемых ({obs})', 'obs': obs}
    if shift_o == 0 and shift_c == 0:
        return {'utc_confirmed': False, 'text': f'сезонного сдвига нет — метки, вероятно, НЕ UTC (фиксированный местный пояс) либо расписание не привязано к США ({obs})', 'obs': obs}
    return {'utc_confirmed': None, 'text': f'картина неоднозначна, нужна ручная проверка ({obs})', 'obs': obs}


def fmt_cell(c):
    return f'{WD[c[0]]} {c[1]:02d}:00' if c else 'н/д'


# ── 1.3 спред из истории BID/ASK ──────────────────────────────────────────────────────────────────────
def spread_bps_series(bid: dict, ask: dict):
    """Выравнивание BID и ASK по времени; спред = (ASK_close − BID_close) / mid × 1e4 (б.п.) по барам, где есть оба. → (ts, spread_bps, flat_bid_or_ask)."""
    ts, i_b, i_a = np.intersect1d(bid['ts'], ask['ts'], return_indices=True)
    bc, ac = bid['c'][i_b], ask['c'][i_a]
    mid = (bc + ac) / 2.0
    spr = (ac - bc) / mid * 1e4
    flat = flat_mask(bid)[i_b] | flat_mask(ask)[i_a]
    return ts, spr, flat


def spread_stats(ts, spr, flat):
    """Медиана и 90-й перцентиль, б.п.: активные часы (будни 07–20 UTC) и ночные (будни 21–06 UTC); плоские бары и выходные исключены. → dict с n."""
    cell = _cells(ts)
    wd, hr = cell // 24, cell % 24
    ok = (~flat) & (wd < 5) & np.isfinite(spr)
    act = ok & np.isin(hr, list(ACTIVE_HOURS))
    ngt = ok & ~np.isin(hr, list(ACTIVE_HOURS))
    out = {}
    for name, m in (('active', act), ('night', ngt)):
        x = spr[m]
        out[name] = {'n': int(len(x)), 'median': float(np.median(x)) if len(x) else None, 'p90': float(np.percentile(x, 90)) if len(x) else None}
    return out


def pooled_spread(parts):
    """Объединение выборок месяцев: parts = список (ts, spr, flat) → spread_stats по склеенному ряду."""
    if not parts:
        return None
    ts = np.concatenate([p[0] for p in parts]); spr = np.concatenate([p[1] for p in parts]); fl = np.concatenate([p[2] for p in parts])
    return spread_stats(ts, spr, fl)


# ── 1.5 дневки из часовых ─────────────────────────────────────────────────────────────────────────────
def day_labels(ts, rule: str):
    """Метка дня (индекс суток) бара. 'utc00' — граница суток 00:00 UTC; 'ny17' — граница 17:00 по Нью-Йорку (22:00 UTC зимой / 21:00 UTC летом): день d = от 17:00 NY дня d−1 до 17:00 NY дня d."""
    ts = np.asarray(ts, dtype=np.int64)
    if rule == 'utc00':
        return ts // DAY_MS
    if rule == 'ny17':
        off = np.array([ny_offset_ms(int(t)) for t in ts], dtype=np.int64)
        return (ts + off + 7 * HOUR_MS) // DAY_MS
    raise ValueError(f'неизвестное правило границы дня: {rule}')


def aggregate_daily(p: dict, rule: str, exclude=None):
    """Дневные бары из часовых по правилу границы дня; исключённые бары (exclude — булев массив) не участвуют. → dict(day, o, h, l, c, v, nbars)."""
    keep = np.ones(len(p['ts']), bool) if exclude is None else ~np.asarray(exclude)
    ts = p['ts'][keep]
    lab = day_labels(ts, rule)
    o, h, l, c, v = (p[k][keep] for k in ('o', 'h', 'l', 'c', 'v'))
    days = np.unique(lab)
    out = {k: [] for k in ('day', 'o', 'h', 'l', 'c', 'v', 'nbars')}
    for d in days:
        m = lab == d
        out['day'].append(int(d)); out['o'].append(float(o[m][0])); out['h'].append(float(h[m].max())); out['l'].append(float(l[m].min()))
        out['c'].append(float(c[m][-1])); out['v'].append(float(v[m].sum())); out['nbars'].append(int(m.sum()))
    return {k: np.array(x) for k, x in out.items()}


def daily_stats(days: dict):
    """Число дневных баров и «обрубков» (< STUB_BARS живых часов). Первый и последний день образца неполные (граница выборки) и в подсчёт не входят."""
    nb = days['nbars']
    if len(nb) > 2:
        nb = nb[1:-1]
    return {'days': int(len(nb)), 'stubs': int(np.sum(nb < STUB_BARS)), 'median_bars': float(np.median(nb)) if len(nb) else 0.0}


def choose_rule(stats_by_rule: dict, default: str):
    """Правило границы дня по данным: подходит, если дней в году в DAYS_RANGE и обрубков нет; из подходящих — default (предложение владельца), иначе первое подходящее; нет подходящих → None."""
    ok = [r for r, s in stats_by_rule.items() if DAYS_RANGE[0] <= s['days'] <= DAYS_RANGE[1] and s['stubs'] == 0]
    if not ok:
        return None, 'ни одно правило не даёт 250–262 дневных баров без обрубков'
    return (default if default in ok else ok[0]), ('подходит правило по умолчанию' if default in ok else f'правило по умолчанию не подходит, подходит {ok[0]}')


# ── BingX: стакан ─────────────────────────────────────────────────────────────────────────────────────
def book_stats(samples):
    """Спред (б.п.) и глубина лучшего уровня (price × amount, по ccxt-единицам) по замерам стакана. samples = [{'bids': [[price, amount], ...], 'asks': [...]}]."""
    sp, bid_d, ask_d = [], [], []
    for s in samples:
        try:
            b, a = s['bids'][0], s['asks'][0]
            mid = (b[0] + a[0]) / 2
            sp.append((a[0] - b[0]) / mid * 1e4)
            bid_d.append(b[0] * b[1]); ask_d.append(a[0] * a[1])
        except (IndexError, KeyError, TypeError, ZeroDivisionError):
            continue
    return {'n': len(sp), 'spread_median': float(np.median(sp)) if sp else None, 'spread_min': min(sp) if sp else None, 'spread_max': max(sp) if sp else None,
            'bid_depth_usdt': float(np.median(bid_d)) if bid_d else None, 'ask_depth_usdt': float(np.median(ask_d)) if ask_d else None}


async def prepare_ma_bingx_book(exchange, progress=None, samples=5, pause=1.0):
    """Сетевая часть (процесс бота, общий exchange): ТОЛЬКО активные контракты из списка владельца; 5 замеров fetch_order_book(limit=5). Приостановленные (109415) не трогаются.
    → payload {'books': {имя: {...}}, 'n_active', 'n_paused'}."""
    mk = exchange.markets or await exchange.load_markets()
    nc = [m for m in mk.values() if (m.get('swap') or m.get('type') == 'swap') and str(m.get('id', '')).upper().startswith(M.BINGX_NC_PREFIX)]
    books = {}
    by_id = {str(m.get('id', '')).upper(): m for m in nc}
    for name, base in BINGX_BOOK.items():
        m = by_id.get(f'{base}-USDT')
        if not m:
            books[name] = {'symbol': None, 'error': f'контракт {base} не найден в markets'}
            continue
        if not m.get('active'):
            books[name] = {'symbol': m['symbol'], 'error': 'контракт не активен (приостановлен) — не опрашивался'}
            continue
        smp, err = [], None
        for _ in range(samples):
            try:
                ob = await exchange.fetch_order_book(m['symbol'], limit=5)
                smp.append({'bids': [[float(x[0]), float(x[1])] for x in ob.get('bids', [])[:5]], 'asks': [[float(x[0]), float(x[1])] for x in ob.get('asks', [])[:5]]})
            except Exception as e:
                err = f'{type(e).__name__}: {str(e)[:100]}'
            await asyncio.sleep(pause)
        books[name] = {'symbol': m['symbol'], 'contract_size': m.get('contractSize'), 'samples': smp, 'error': err}
        if progress:
            try:
                await progress(f'BingX стакан {name}: замеров {len(smp)}')
            except Exception:
                pass
    return {'books': books, 'n_active': sum(1 for m in nc if m.get('active')), 'n_paused': sum(1 for m in nc if not m.get('active'))}


# ── Stooq: одна диагностическая попытка ───────────────────────────────────────────────────────────────
async def stooq_diagnose(session, url='https://stooq.com/q/d/l/?s=eurusd&i=d', timeout=15, dns=True):
    """Одна попытка с таймаутом 15 с без следования редиректам: код ответа, заголовки (выборочно), редирект, признаки капчи/apikey, первые байты; при сбое — класс исключения и этап
    (DNS-разрешение проверяется отдельно). → dict."""
    import aiohttp
    out = {'url': url, 'timeout_s': timeout}
    t0 = time.time()
    if dns:
        try:
            loop = asyncio.get_running_loop()
            infos = await asyncio.wait_for(loop.getaddrinfo('stooq.com', 443, type=socket.SOCK_STREAM), 5)
            out['dns'] = sorted({i[4][0] for i in infos})[:4]
        except Exception as e:
            out['dns_error'] = f'{type(e).__name__}: {e}'
    try:
        async with session.get(url, allow_redirects=False, timeout=aiohttp.ClientTimeout(total=timeout)) as r:
            body = await r.read()
            hdr = {k.lower(): v for k, v in r.headers.items()}
            low = body[:2000].decode('utf-8', 'replace').lower()
            out.update(status=r.status, headers={k: hdr[k] for k in ('location', 'content-type', 'server', 'content-length', 'set-cookie', 'cf-ray') if k in hdr},
                       redirect=hdr.get('location'), bytes=len(body), head=body[:200].decode('utf-8', 'replace'),
                       captcha=('captcha' in low or 'cloudflare' in low), apikey=('apikey' in low or 'api key' in low))
    except Exception as e:
        out['error'] = f'{type(e).__name__}: {str(e)[:120]}'
    out['elapsed_s'] = round(time.time() - t0, 1)
    return out


# ── Dukascopy: загрузка месяцев и сбор данных пробы ───────────────────────────────────────────────────
def duka_url(folder, side, year, month0):
    return f'{M.DUKA_BASE}/{folder}/{year}/{month0:02d}/{side}_candles_hour_1.bi5'


class Fetcher:
    """Кэш месячных файлов (BID/ASK) на время пробы: один и тот же месяц не качается дважды."""

    def __init__(self, fetch, pause):
        self.fetch, self.pause, self.cache, self.requests = fetch, pause, {}, 0

    async def month(self, folder, side, year, month0, order='ocLH'):
        key = (folder, side, year, month0)
        if key in self.cache:
            return self.cache[key]
        r = await self.fetch(duka_url(folder, side, year, month0), None, 'GET')
        self.requests += 1
        await asyncio.sleep(self.pause)
        res = {'status': r['status'], 'bytes': len(r['body']), 'error': r['error'], 'p': None, 'why': ''}
        if r['status'] == 200 and r['body']:
            try:
                raw = M.decode_bi5(r['body'])
                ps = M._month_start_ms(year, month0)
                best = None
                for o in ('ocLH', 'ohlc'):
                    p = M.parse_duka_candles(raw, ps, o)
                    cons = M.ohlc_consistency(p)
                    if best is None or cons > best[1]:
                        best = (o, cons, p)
                res['order'], res['consistency'], res['p'] = best
            except M.ParseError as e:
                res['why'] = str(e)
        else:
            res['why'] = f'HTTP {r["status"]} {r["error"]}'.strip()
        self.cache[key] = res
        return res


def last_complete_month(now_ms):
    now = datetime.fromtimestamp(now_ms / 1000, timezone.utc)
    return (now.year, now.month - 2) if now.month >= 2 else (now.year - 1, 11)


def scale_prices(p: dict, scale: float):
    q = dict(p)
    for k in ('o', 'h', 'l', 'c'):
        q[k] = p[k] / scale
    return q


async def discover(fx: Fetcher, key, now_ms):
    """Находит папку datafeed по вариантам имён: первый вариант, отдающий разбираемый месячный файл. → dict(key, class, folder, tried, sample, scale)."""
    cls, variants, (lo, hi) = TARGETS[key]
    ly, lm = last_complete_month(now_ms)
    tried = []
    for folder in variants:
        m = await fx.month(folder, 'BID', ly, lm)
        tried.append((folder, m['status'], m['bytes']))
        if m['p'] is not None:
            sc = M.pick_scale(float(np.median(m['p']['c'])), lo, hi)
            return {'key': key, 'class': cls, 'folder': folder, 'tried': tried, 'order': m['order'], 'consistency': m['consistency'], 'scale': sc, 'ok_format': m['consistency'] >= 0.99 and sc is not None}
    return {'key': key, 'class': cls, 'folder': None, 'tried': tried, 'ok_format': False}


async def probe_instrument(fx: Fetcher, info, now_ms, progress=None):
    """Всё по одному найденному инструменту: глубина, выходные, спред из истории BID/ASK."""
    key, folder = info['key'], info['folder']
    cls, _v, (lo, hi) = TARGETS[key]
    out = dict(info, depth={}, weekend=None, spread={}, spread_pooled=None, earliest_year=None)
    ly, lm = last_complete_month(now_ms)
    for y in DEPTH_YEARS:
        m = await fx.month(folder, 'BID', y, 0)
        out['depth'][y] = (m['status'], m['bytes'])
    have = [y for y, (st, n) in out['depth'].items() if st == 200 and n > 0]
    out['earliest_year'] = min(have) if have else None
    sc = info['scale']
    m = await fx.month(folder, 'BID', ly, lm)
    if m['p'] is not None and sc:
        out['weekend'] = weekend_stats(scale_prices(m['p'], sc))
        out['weekend'].pop('closed_mask', None); out['weekend'].pop('flat', None)
    parts = []
    for (y, mo) in list(SPREAD_SAMPLES) + [(ly, lm)]:
        b = await fx.month(folder, 'BID', y, mo)
        a = await fx.month(folder, 'ASK', y, mo)
        if b['p'] is None or a['p'] is None:
            out['spread'][(y, mo)] = {'error': f'BID: {b["why"] or "ok"}; ASK: {a["why"] or "ok"}'}
            continue
        ts, spr, fl = spread_bps_series(b['p'], a['p'])
        out['spread'][(y, mo)] = spread_stats(ts, spr, fl)
        parts.append((ts, spr, fl))
    out['spread_pooled'] = pooled_spread(parts)
    return out


async def probe_tz(fx: Fetcher, infos, year=2024):
    out = {}
    for key in TZ_SYMS:
        info = infos.get(key)
        if not info or not info.get('folder') or not info.get('scale'):
            out[key] = {'error': 'инструмент не найден/без масштаба'}
            continue
        edges = {}
        for name, mo in (('jan', 0), ('jul', 6)):
            m = await fx.month(info['folder'], 'BID', year, mo)
            if m['p'] is None:
                edges[name] = ([], [])
                continue
            edges[name] = week_edges(scale_prices(m['p'], info['scale']))
        out[key] = tz_verdict(edges['jan'], edges['jul'])
    return out


async def probe_daily_rules(fx: Fetcher, infos, year=AGG_YEAR):
    """Агрегация дневок на образце года (по одному инструменту на класс): дней в году и «обрубки» для обоих правил границы дня; правило по классу."""
    out = {}
    default = {'METALS': 'utc00', 'INDICES': 'utc00', 'FX': 'ny17'}
    for cls, key in CLASS_SAMPLE.items():
        info = infos.get(key)
        if not info or not info.get('folder') or not info.get('scale'):
            out[cls] = {'error': f'образец {key} не найден'}
            continue
        parts = []
        for mo in range(12):
            m = await fx.month(info['folder'], 'BID', year, mo)
            if m['p'] is not None:
                parts.append(scale_prices(m['p'], info['scale']))
        if len(parts) < 12:
            out[cls] = {'error': f'за {year} получено {len(parts)} из 12 месяцев ({key})'}
            continue
        p = {k: np.concatenate([x[k] for x in parts]) for k in ('ts', 'o', 'h', 'l', 'c', 'v')}
        excl = closure_mask(p)
        stats = {r: daily_stats(aggregate_daily(p, r, excl)) for r in ('utc00', 'ny17')}
        rule, why = choose_rule(stats, default[cls])
        out[cls] = {'sample': key, 'year': year, 'stats': stats, 'rule': rule, 'why': why, 'default': default[cls], 'excluded_bars': int(excl.sum()), 'bars': int(len(p['ts']))}
    return out


def instrument_ok(r: dict):
    """Пригодность инструмента Dukascopy для фабрики: формат/масштаб распознаны, история с ≤ 2019, спред посчитан."""
    why = []
    if not r.get('folder'):
        return False, 'не найден в datafeed'
    if not r.get('ok_format'):
        why.append('формат/масштаб не распознаны')
    if r.get('earliest_year') is None or r['earliest_year'] > DEPTH_MAX_FIRST_YEAR:
        why.append(f'история с {r.get("earliest_year")} (> {DEPTH_MAX_FIRST_YEAR})')
    sp = r.get('spread_pooled')
    if not sp or sp['active']['median'] is None:
        why.append('спред из истории не посчитан')
    return (not why), ('; '.join(why) if why else 'формат распознан, история достаточна, спред посчитан')


def proposal2(instruments: dict, daily: dict):
    """Предложение списков (по классам, только пригодные) и ячеек (класс, таймфрейм, n) для проекта поправки: 1D — все классы; 4h/1h — CRYPTO, METALS, FX; INDICES — 1D и 4h.
    ENERGY/BONDS в базовые классы не входят (решение владельца). → dict."""
    cls = {}
    for key, r in instruments.items():
        ok, _ = instrument_ok(r)
        if ok:
            cls.setdefault(r['class'], []).append(key)
    cells = [('CRYPTO', tf, 16) for tf in L.TIMEFRAMES]
    for c in ('METALS', 'FX'):
        n = len(cls.get(c, []))
        if n:
            cells += [(c, tf, n) for tf in ('1D', '4h', '1h')]
    n = len(cls.get('INDICES', []))
    if n:
        cells += [('INDICES', '1D', n), ('INDICES', '4h', n)]
    extra = {c: len(cls.get(c, [])) for c in ('ENERGY', 'BONDS')}
    return {'classes': cls, 'cells': cells, 'N': L.count_tests(cells), 'extra': extra}


# ── отчёт ─────────────────────────────────────────────────────────────────────────────────────────────
def _f(x, nd=1):
    return 'н/д' if x is None else f'{x:.{nd}f}'


def build_report2(stooq, instruments, tz, daily, book, now_ms, disk_mb, requests, version=''):
    R = ['📡 <b>/data_probe_ma, ПРОБА v2 (BOT_SPEC §2.41.14): качество данных Dukascopy, спреды, правила; только метаданные и образцы — доходностей стратегий не считалось</b>',
         f'Время проверки: {datetime.fromtimestamp(now_ms / 1000, timezone.utc):%Y-%m-%d %H:%M} UTC; запросов к Dukascopy: {requests}. {version}']
    # 3. Stooq
    R.append('\n<b>Stooq — одна диагностическая попытка (таймаут 15 с)</b>')
    if 'error' in stooq:
        R.append(f'  сбой за {stooq["elapsed_s"]} с: {stooq["error"]}; DNS: {stooq.get("dns") or stooq.get("dns_error")}')
        R.append('  ВЫВОД: Stooq недоступен с сервера — не использовать (решение владельца).')
    else:
        R.append(f'  HTTP {stooq["status"]} за {stooq["elapsed_s"]} с, байт {stooq["bytes"]}, редирект {stooq.get("redirect")}, заголовки {stooq.get("headers")}, капча/Cloudflare {stooq["captcha"]}, apikey {stooq["apikey"]}')
        R.append(f'  начало ответа: {stooq["head"][:160]!r}')
        R.append('  ВЫВОД: Stooq ' + ('отвечает, но не отдаёт CSV — не использовать' if (stooq['captcha'] or stooq['apikey'] or stooq['status'] != 200) else 'отвечает; формат CSV не проверялся в v2 (решение владельца)'))
    # 1.4 / найденные инструменты
    R.append('\n<b>Dukascopy: найденные инструменты, глубина, пригодность</b> (имена папок datafeed — по моим сведениям; перебор вариантов)')
    for key, r in instruments.items():
        ok, why = instrument_ok(r)
        if not r.get('folder'):
            R.append(f'  {key} [{r["class"]}]: НЕ НАЙДЕН — варианты {r["tried"]}')
            continue
        dep = ', '.join(f'{y}:{st}' for y, (st, n) in r['depth'].items())
        R.append(f'  {key} [{r["class"]}] папка {r["folder"]}: {"ПРИГОДЕН" if ok else "НЕПРИГОДЕН"} — {why}; порядок полей {r["order"]} (OHLC {r["consistency"] * 100:.0f}%), масштаб ÷{r["scale"]:g}; '
                 f'глубина (январь→HTTP): {dep}; самый ранний {r["earliest_year"]}')
    # 1.1 выходные
    R.append('\n<b>1.1 Выходные и закрытие рынка (последний завершённый месяц, по данным)</b> — плоский бар: O=H=L=C; окно закрытия — ячейки «день недели × час UTC», где ≥ 90% баров плоские')
    for key, r in instruments.items():
        w = r.get('weekend')
        if not w:
            continue
        rng = '; '.join(fmt_range(s, n) for s, n in w['ranges']) or 'окон закрытия нет'
        R.append(f'  {key}: баров {w["n"]}, плоских {w["flat_share"] * 100:.0f}% (выходные Сб+Вс {w["flat_weekend"] * 100:.0f}%, будни {w["flat_weekday"] * 100:.0f}%); окна закрытия: {rng}; '
                 f'живых баров внутри окон {w["live_in_closed"]} из {w["closed_bars"]}; плоских вне окон {w["flat_outside_closed"]}; нулевой объём в окнах {w["zero_vol_closed"] * 100:.0f}% / вне {w["zero_vol_open"] * 100:.0f}%, '
                 f'средний объём в окнах {w["mean_vol_closed"]:.2f} / вне {w["mean_vol_open"]:.2f}')
    R.append('  ПРЕДЛОЖЕНИЕ правила исключения: исключать бар, у которого O=H=L=C И который входит в серию ≥ 12 подряд идущих плоских часовых баров (закрытие рынка). Формулировка владельца «O=H=L=C и время в окне закрытия» '
             'уточнена: окно «день недели × час» плывёт с летним временем США (зимой и летом закрытие сдвинуто на час), и на длинной выборке в окно не попадают часы, плоские только в одном из сезонов — '
             'проверено на годовом образце; серии устойчивы. Одиночные и короткие (< 12 ч) плоские бары не исключаются. Исключено баров серией в образце: ' + ', '.join(f'{k} {r["weekend"]["run_excluded"]}' for k, r in instruments.items() if r.get('weekend')) + '.')
    # 1.2 часовой пояс
    R.append('\n<b>1.2 Часовой пояс (январь и июль 2024; живой бар = не плоский; время — начало часового бара, UTC)</b>')
    for key, v in tz.items():
        R.append(f'  {key}: ' + (v['text'] if 'text' in v else v['error']))
    # 1.3 спред
    R.append('\n<b>1.3 Спред из истории: (ASK_close − BID_close)/mid, б.п.; активные часы (будни 07–20 UTC) / ночные (будни 21–06 UTC); плоские бары и выходные исключены; выборка — 3 месяца разных лет</b>')
    for key, r in instruments.items():
        sp = r.get('spread_pooled')
        if not r.get('folder'):
            continue
        if not sp:
            R.append(f'  {key}: не посчитан — {r["spread"]}')
            continue
        a, n = sp['active'], sp['night']
        R.append(f'  {key}: активные {_f(a["median"], 2)} (p90 {_f(a["p90"], 2)}, n={a["n"]}) | ночные {_f(n["median"], 2)} (p90 {_f(n["p90"], 2)}, n={n["n"]}); по месяцам: ' +
                 '; '.join(f'{y}-{mo + 1:02d} акт. {_f((s.get("active") or {}).get("median"), 2)} / ноч. {_f((s.get("night") or {}).get("median"), 2)}' if 'error' not in s else f'{y}-{mo + 1:02d} {s["error"][:60]}'
                           for (y, mo), s in r['spread'].items()))
    # 1.5 дневки
    R.append('\n<b>1.5 Дневки из часовых: число дневных баров за 2024 год и «обрубки» (< 4 живых часов) для двух правил границы дня; выходные бары исключены по правилу 1.1</b>')
    for cls, d in daily.items():
        if 'error' in d:
            R.append(f'  {cls}: {d["error"]}')
            continue
        s = d['stats']
        R.append(f'  {cls} (образец {d["sample"]} {d["year"]}): utc00 — дней {s["utc00"]["days"]}, обрубков {s["utc00"]["stubs"]}; ny17 (17:00 Нью-Йорка = 22:00/21:00 UTC) — дней {s["ny17"]["days"]}, '
                 f'обрубков {s["ny17"]["stubs"]}; исключено баров {d["excluded_bars"]} из {d["bars"]}; по умолчанию {d["default"]} → ПРЕДЛАГАЕТСЯ {d["rule"] or "нет подходящего"} ({d["why"]})')
    # 2. BingX
    R.append('\n<b>2. BingX — спред по стакану (fetch_order_book limit 5, 5 замеров; только АКТИВНЫЕ контракты; приостановленные не опрашивались)</b>')
    R.append(f'  не-крипто контрактов NC*: активных {book.get("n_active")}, приостановленных {book.get("n_paused")}')
    for name, b in (book.get('books') or {}).items():
        if not b.get('samples'):
            R.append(f'  {name}: {b.get("error")}')
            continue
        st = book_stats(b['samples'])
        R.append(f'  {name} ({b["symbol"]}, размер контракта {b.get("contract_size")}): спред медиана {_f(st["spread_median"], 2)} б.п. (мин {_f(st["spread_min"], 2)}, макс {_f(st["spread_max"], 2)}, замеров {st["n"]}), '
                 f'глубина лучшего уровня bid {_f(st["bid_depth_usdt"], 0)} / ask {_f(st["ask_depth_usdt"], 0)} USDT (price × amount; единицы amount в ccxt для этих контрактов не проверены)'
                 + (f'; ошибка: {b["error"]}' if b.get('error') else ''))
    # вывод
    prop = proposal2(instruments, daily)
    R.append('\n<b>Предварительный вывод (основа проекта поправки к §2.41 — решает владелец)</b>')
    for c in ('METALS', 'FX', 'INDICES', 'ENERGY', 'BONDS'):
        ok_l = prop['classes'].get(c, [])
        R.append(f'  {c}: ' + (', '.join(ok_l) if ok_l else 'нет пригодных') + (' (вне базовых классов §2.41 — решение владельца)' if c in ('ENERGY', 'BONDS') else ''))
    R.append(f'  Ячейки (класс, таймфрейм, число инструментов): {prop["cells"]}')
    R.append(f'  <b>N по формуле §2.41.5 для базовых классов = {prop["N"]}</b> (X-MOM — только при ≥ 6 инструментах в ячейке). Свободно на диске: {disk_mb:.0f} МБ.')
    R.append('<b>ВЕРДИКТ пробы v2: ' + '; '.join(f'{c}: {len(prop["classes"].get(c, []))}' for c in ('METALS', 'FX', 'INDICES', 'ENERGY', 'BONDS')) +
             ' пригодных инструментов; N (база) = ' + str(prop['N']) + '. Доходностей стратегий не считалось.</b>')
    return '\n'.join(R)


async def run_probe_ma2(payload, params, progress=None):
    """Дочерний процесс: Stooq-диагностика + Dukascopy (aiohttp) + отчёт. payload = результат prepare_ma_bingx_book. → текст отчёта."""
    import aiohttp
    now_ms = int(time.time() * 1000)
    async with aiohttp.ClientSession(headers=M.UA) as ses:
        stooq = await stooq_diagnose(ses)
        if progress:
            await progress('Stooq: ' + (stooq.get('error') or f'HTTP {stooq.get("status")}'))
        fx = Fetcher(V.make_fetcher(ses), 0.25)
        infos = {}
        for key in TARGETS:
            infos[key] = await discover(fx, key, now_ms)
            if progress:
                await progress(f'Dukascopy {key}: ' + (f'папка {infos[key]["folder"]}' if infos[key]['folder'] else 'не найден'))
        instruments = {}
        for key, info in infos.items():
            if info['folder']:
                instruments[key] = await probe_instrument(fx, info, now_ms)
            else:
                instruments[key] = info
        tz = await probe_tz(fx, infos)
        daily = await probe_daily_rules(fx, infos)
    try:
        disk = V.free_mb(params.get('vision_dir') or V.vision_dir())
    except Exception:
        disk = 0.0
    return build_report2(stooq, instruments, tz, daily, payload or {}, now_ms, disk, fx.requests, params.get('version', ''))
