"""research_fdata.py — данные «фабрики» (BOT_SPEC §2.41, поправка §2.41.17): загрузка часовых BID-свечей Dukascopy (18 не-крипто инструментов), хранение по месяцам в .npz,
сборка рядов 1h/4h/1D (граница дня 17:00 Нью-Йорка для не-крипто, 00:00 UTC для CRYPTO), исключение баров с нулевым объёмом, доступ к LOCKBOX только через сессию финала.
Сеть — в дочернем процессе (research_runner). Расчётов стратегий здесь нет."""
import asyncio
import json
import os
import time
from datetime import datetime, timedelta, timezone

import numpy as np

import research_ma as M
import research_ma_lib as L
import research_vision as V

HOUR_MS = 3_600_000
DAY_MS = 86_400_000
RETRY_DELAYS = (3, 8, 20, 45)            # повторы при 503/сети (как v101/v102)
PAUSE_S = 1.0
MAX_CONSEC_FAIL = 3                      # после 3 подряд неполученных месяцев инструмент пропускается до следующего запуска (загрузка частями)
MIN_FREE_MB = 200
WARM_START = '2019-01-01'                # с этой даты бары участвуют в расчётах (разогрев индикаторов перед TRAIN)
MIN_BARS_1D = 4                          # дневной бар из < 4 живых часовых — «обрубок», отбрасывается
MIN_BARS_4H = 2

# ключ → (класс, папка datafeed, правдоподобный диапазон цены для выбора масштаба, первый месяц загрузки (год, месяц 1..12))
INSTRUMENTS = {
    'XAUUSD': ('COMMODITIES', 'XAUUSD', (300.0, 8000.0), (2012, 1)),
    'XAGUSD': ('COMMODITIES', 'XAGUSD', (5.0, 120.0), (2012, 1)),
    'BRENT': ('COMMODITIES', 'BRENTCMDUSD', (10.0, 200.0), (2012, 1)),
    'WTI': ('COMMODITIES', 'LIGHTCMDUSD', (1.0, 200.0), (2012, 1)),
    'EURUSD': ('FX', 'EURUSD', (0.5, 2.0), (2012, 1)),
    'GBPUSD': ('FX', 'GBPUSD', (0.8, 2.3), (2012, 1)),
    'USDJPY': ('FX', 'USDJPY', (50.0, 220.0), (2012, 1)),
    'AUDUSD': ('FX', 'AUDUSD', (0.4, 1.2), (2012, 1)),
    'USDCHF': ('FX', 'USDCHF', (0.5, 1.5), (2012, 1)),
    'USDCAD': ('FX', 'USDCAD', (0.9, 1.7), (2012, 1)),
    'NZDUSD': ('FX', 'NZDUSD', (0.4, 1.0), (2012, 1)),
    'SPX': ('INDICES', 'USA500IDXUSD', (800.0, 12000.0), (2012, 1)),
    'NDX': ('INDICES', 'USATECHIDXUSD', (2000.0, 40000.0), (2012, 1)),
    'DAX': ('INDICES', 'DEUIDXEUR', (4000.0, 30000.0), (2012, 1)),
    'NIKKEI': ('INDICES', 'JPNIDXJPY', (8000.0, 70000.0), (2012, 1)),
    'FTSE': ('INDICES', 'GBRIDXGBP', (3000.0, 12000.0), (2012, 1)),
    'BUND': ('BONDS', 'BUNDTREUR', (80.0, 220.0), (2018, 1)),
    'TNOTE': ('BONDS', 'USTBONDTRUSD', (80.0, 220.0), (2018, 1)),
}
CLASS_MEMBERS = {c: [k for k, v in INSTRUMENTS.items() if v[0] == c] for c in ('COMMODITIES', 'FX', 'INDICES', 'BONDS')}
CLASS_MEMBERS['MULTI'] = list(INSTRUMENTS)
# ячейки (класс, таймфрейм) — порядок фиксирован (номер ячейки входит в seed калибровки)
CELLS = [('CRYPTO', '1D'), ('CRYPTO', '4h'), ('CRYPTO', '1h'), ('COMMODITIES', '1D'), ('COMMODITIES', '4h'), ('FX', '1D'), ('FX', '4h'), ('FX', '1h'),
         ('INDICES', '1D'), ('INDICES', '4h'), ('BONDS', '1D'), ('MULTI', '1D')]


def factory_dir():
    return os.getenv('FACTORY_DIR') or ('/data/factory' if os.path.exists('/data') else '/tmp/factory')


def _ms(date_str):
    return int(datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc).timestamp() * 1000)


def warm_start_ms():
    return _ms(WARM_START)


# ── хранение ─────────────────────────────────────────────────────────────────────────────────────────
def inst_dir(d, key):
    return os.path.join(d, 'duka', key)


def month_path(d, key, y, m1):
    return os.path.join(inst_dir(d, key), f'{y}-{m1:02d}.npz')


def index_path(d, key):
    return os.path.join(inst_dir(d, key), 'index.json')


def load_index(d, key):
    try:
        with open(index_path(d, key), encoding='utf-8') as f:
            x = json.load(f)
    except (OSError, ValueError):
        x = {}
    x.setdefault('scale', None)
    x.setdefault('absent', [])
    x.setdefault('bad', [])
    return x


def save_index(d, key, idx):
    os.makedirs(inst_dir(d, key), exist_ok=True)
    tmp = index_path(d, key) + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(idx, f, ensure_ascii=False)
    os.replace(tmp, index_path(d, key))


def month_tags(first, last):
    """first = (год, месяц 1..12), last = (год, месяц 1..12) включительно → ['2012-01', ...]."""
    out = []
    y, m = first
    while (y, m) <= last:
        out.append(f'{y}-{m:02d}')
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


def last_complete(now_ms):
    """Последний полный месяц (год, месяц 1..12) относительно now."""
    t = datetime.fromtimestamp(now_ms / 1000, timezone.utc)
    return (t.year, t.month - 1) if t.month > 1 else (t.year - 1, 12)


def stored_tags(d, key):
    p = inst_dir(d, key)
    try:
        return sorted(f[:7] for f in os.listdir(p) if f.endswith('.npz') and not f.endswith('.tmp.npz'))
    except OSError:
        return []


def coverage(d, key, now_ms):
    """Полнота загрузки: → dict(first, last, stored, absent, missing[], complete). missing — месяцы между первым имеющимся и последним полным, которых нет ни файлом, ни пометкой 404."""
    cls, folder, rng, first = INSTRUMENTS[key]
    idx = load_index(d, key)
    have = set(stored_tags(d, key))
    absent = set(idx['absent'])
    bad = set(idx['bad'])
    ly, lm = last_complete(now_ms)
    want = month_tags(first, (ly, lm))
    missing = [t for t in want if t not in have and t not in absent and t not in bad]
    # месяцы до первого имеющегося — допустимо отсутствуют только если 404 (в absent), иначе — пропуск загрузки
    return {'key': key, 'first_wanted': want[0], 'last_wanted': want[-1], 'stored': len(have), 'absent': len(absent), 'bad': len(bad), 'missing': missing,
            'first_have': min(have) if have else None, 'last_have': max(have) if have else None, 'complete': (not missing and bool(have))}


# ── загрузка Dukascopy ───────────────────────────────────────────────────────────────────────────────
def is_transient(status):
    return status is None or status == 429 or status >= 500


async def fetch_with_retry(fetch, url, delays=RETRY_DELAYS, sleep=None):
    sleep = sleep or asyncio.sleep
    r = await fetch(url, None, 'GET')
    tries = 0
    while is_transient(r['status']) and tries < len(delays):
        await sleep(delays[tries])
        tries += 1
        r = await fetch(url, None, 'GET')
    r['retries'] = tries
    return r


def decode_month(body: bytes, y: int, m1: int):
    """Тело месячного файла → dict сырых цен (ts,o,h,l,c,v) с автоопределением порядка полей; ParseError — не разобран."""
    raw = M.decode_bi5(body)
    ps = M._month_start_ms(y, m1 - 1)
    best = None
    for o in ('ocLH', 'ohlc'):
        p = M.parse_duka_candles(raw, ps, o)
        cons = M.ohlc_consistency(p)
        if best is None or cons > best[1]:
            best = (o, cons, p)
    return best


async def load_instrument(fetch, key, d, now_ms, progress=None, pause=PAUSE_S, delays=RETRY_DELAYS, sleep=None, deadline=None, max_consec_fail=MAX_CONSEC_FAIL):
    """Докачивает недостающие месяцы инструмента; существующие файлы и пометки 404 пропускаются (продолжение с места остановки). → dict статистики."""
    sleep = sleep or asyncio.sleep
    cls, folder, (lo, hi), first = INSTRUMENTS[key]
    idx = load_index(d, key)
    ly, lm = last_complete(now_ms)
    have = set(stored_tags(d, key))
    st = {'key': key, 'loaded': 0, 'skipped': 0, 'absent_new': 0, 'failed': [], 'bad': [], 'stopped': '', 'requests': 0}
    consec = 0
    for tag in month_tags(first, (ly, lm)):
        y, m1 = int(tag[:4]), int(tag[5:])
        if tag in have or tag in idx['absent'] or tag in idx['bad']:
            st['skipped'] += 1
            continue
        if deadline is not None and time.time() > deadline:
            st['stopped'] = 'время'
            break
        V.check_space(d, MIN_FREE_MB)
        r = await fetch_with_retry(fetch, M.duka_hour_url(folder, y, m1 - 1), delays, sleep)
        st['requests'] += 1 + r['retries']
        await sleep(pause)
        if r['status'] == 404:
            idx['absent'].append(tag); st['absent_new'] += 1; consec = 0
            save_index(d, key, idx)
            continue
        if r['status'] == 200 and not r['body']:
            idx['absent'].append(tag); st['absent_new'] += 1; consec = 0
            save_index(d, key, idx)
            continue
        if r['status'] != 200:
            st['failed'].append(tag); consec += 1
            if consec >= max_consec_fail:
                st['stopped'] = f'{consec} подряд неполученных месяцев (HTTP {r["status"]})'
                break
            continue
        consec = 0
        try:
            order, cons, p = decode_month(r['body'], y, m1)
        except M.ParseError as e:
            idx['bad'].append(tag); st['bad'].append(f'{tag}: {e}')
            save_index(d, key, idx)
            continue
        if cons < 0.99:
            idx['bad'].append(tag); st['bad'].append(f'{tag}: согласованность OHLC {cons:.2f}')
            save_index(d, key, idx)
            continue
        if idx['scale'] is None:
            sc = M.pick_scale(float(np.median(p['c'])), lo, hi)
            if sc is None:
                idx['bad'].append(tag); st['bad'].append(f'{tag}: масштаб цены не определён (медиана сырых {float(np.median(p["c"])):.0f})')
                save_index(d, key, idx)
                continue
            idx['scale'] = sc
            save_index(d, key, idx)
        sc = idx['scale']
        V._atomic_savez(month_path(d, key, y, m1), ts=p['ts'].astype(np.int64), o=p['o'] / sc, h=p['h'] / sc, l=p['l'] / sc, c=p['c'] / sc, v=p['v'])
        st['loaded'] += 1
        if progress and st['loaded'] % 24 == 0:
            await progress(f'{key}: загружено {st["loaded"]} мес. (до {tag})')
    return st


# ── чтение и сборка рядов ────────────────────────────────────────────────────────────────────────────
def _read_month(path):
    z = V._load_npz(path)
    return z


def load_raw(d, key, mode='pre', session=None):
    """Сырые часовые бары инструмента с WARM_START. mode='pre' — строго до LOCKBOX (месячные файлы с 2025-01 НЕ открываются, бары ts ≥ LOCKBOX отбрасываются);
    mode='full' — полный ряд, часть LOCKBOX берётся ТОЛЬКО через session (open_lockbox_session(final=True)). → dict ts,o,h,l,c,v (float64) или None."""
    lb = L.lockbox_start_ms()
    w0 = warm_start_ms()
    if mode == 'full' and session is None:
        raise L.LockboxError('mode=full требует сессию финала')
    parts = []
    for tag in stored_tags(d, key):
        y, m1 = int(tag[:4]), int(tag[5:])
        mstart = int(datetime(y, m1, 1, tzinfo=timezone.utc).timestamp() * 1000)
        if mstart >= lb and mode == 'pre':
            continue
        if (y, m1) < (int(WARM_START[:4]), int(WARM_START[5:7])):
            continue
        z = _read_month(month_path(d, key, y, m1))
        if z is not None:
            parts.append(z)
    if not parts:
        return None
    out = {k: np.concatenate([z[k] for z in parts]).astype(np.float64) for k in ('o', 'h', 'l', 'c', 'v')}
    ts = np.concatenate([z['ts'] for z in parts]).astype(np.int64)
    order = np.argsort(ts, kind='stable')
    ts = ts[order]
    out = {k: v[order] for k, v in out.items()}
    keep = np.r_[True, np.diff(ts) != 0] if len(ts) > 1 else np.ones(len(ts), bool)
    ts = ts[keep]
    out = {k: v[keep] for k, v in out.items()}
    m = ts >= w0
    ts = ts[m]
    out = {k: v[m] for k, v in out.items()}
    if mode == 'pre':
        m = ts < lb
        ts = ts[m]
        out = {k: v[m] for k, v in out.items()}
    else:
        pre = ts < lb
        t2, *rest = session.slice(ts, out['o'], out['h'], out['l'], out['c'], out['v'])
        ts = np.concatenate([ts[pre], t2])
        out = {k: np.concatenate([out[k][pre], r]) for k, r in zip(('o', 'h', 'l', 'c', 'v'), rest)}
    out['ts'] = ts
    return out


def exclude_zero_volume(p: dict) -> dict:
    """Правило поправки §2.41.17: бары Dukascopy с нулевым объёмом исключаются."""
    m = p['v'] > 0
    return {k: v[m] for k, v in p.items()}


# граница дня ny17: 17:00 America/New_York; летнее время США — 2-е воскресенье марта 07:00 UTC … 1-е воскресенье ноября 06:00 UTC
def _nth_sunday(year, month, n):
    d = datetime(year, month, 1, tzinfo=timezone.utc)
    first = d + timedelta(days=(6 - d.weekday()) % 7)
    return first + timedelta(days=7 * (n - 1))


def us_dst_vec(ts):
    """Векторно: True, если Нью-Йорк на летнем времени (EDT) в момент ts (мс UTC)."""
    ts = np.asarray(ts, dtype=np.int64)
    out = np.zeros(len(ts), bool)
    if not len(ts):
        return out
    y0 = datetime.fromtimestamp(ts[0] / 1000, timezone.utc).year
    y1 = datetime.fromtimestamp(ts[-1] / 1000, timezone.utc).year
    for y in range(y0, y1 + 1):
        a = int(_nth_sunday(y, 3, 2).replace(hour=7).timestamp() * 1000)
        b = int(_nth_sunday(y, 11, 1).replace(hour=6).timestamp() * 1000)
        out |= (ts >= a) & (ts < b)
    return out


def ny17_labels(ts):
    """→ (метка торгового дня, метка 4h-бара внутри дня): граница в 17:00 Нью-Йорка; 4h — 6 баров в сутках от границы."""
    ts = np.asarray(ts, dtype=np.int64)
    off = np.where(us_dst_vec(ts), -4, -5).astype(np.int64) * HOUR_MS
    shifted = ts + off + 7 * HOUR_MS
    day = shifted // DAY_MS
    q = (shifted % DAY_MS) // (4 * HOUR_MS)
    return day, day * 6 + q


def _group(label, ts, p, min_count, drop_last=True):
    """Агрегация подряд идущих баров с одинаковой меткой. → dict ts,o,h,l,c,v,day-метки не заполняются."""
    n = len(label)
    if n == 0:
        return None
    starts = np.r_[0, np.flatnonzero(np.diff(label)) + 1]
    counts = np.diff(np.r_[starts, n])
    out = {'ts': ts[starts], 'o': p['o'][starts], 'h': np.maximum.reduceat(p['h'], starts), 'l': np.minimum.reduceat(p['l'], starts),
           'c': p['c'][np.r_[starts[1:] - 1, n - 1]], 'v': np.add.reduceat(p['v'], starts), 'label': label[starts], 'cnt': counts}
    keep = counts >= min_count
    if drop_last:
        keep[-1] = False                         # последняя группа ряда может быть неполной
    return {k: v[keep] for k, v in out.items()}


def build_noncrypto(raw: dict, tf: str):
    """Сырые часовые бары → ряд tf ('1h'|'4h'|'1D') по правилам §2.41.17: бары с нулевым объёмом исключены, граница дня ny17, обрубки и последняя неполная группа отброшены.
    → dict ts,o,h,l,c,day (день для суммирования доходностей: 1D — метка торгового дня, 4h/1h — день UTC начала бара)."""
    p = exclude_zero_volume(raw)
    if not len(p['ts']):
        return None
    if tf == '1h':
        g = {k: p[k][:-1] for k in ('ts', 'o', 'h', 'l', 'c')}
        g['day'] = g['ts'] // DAY_MS
        return g
    day, lab4 = ny17_labels(p['ts'])
    if tf == '1D':
        g = _group(day, p['ts'], p, MIN_BARS_1D)
        if g is None:
            return None
        g['day'] = g['label']
    elif tf == '4h':
        g = _group(lab4, p['ts'], p, MIN_BARS_4H)
        if g is None:
            return None
        g['day'] = g['ts'] // DAY_MS
    else:
        raise ValueError(tf)
    return {k: g[k] for k in ('ts', 'o', 'h', 'l', 'c', 'day')}


def build_crypto(k15: dict, tf: str):
    """15m (кэш Binance) → ряд tf с границей 00:00 UTC; группа с < половины баров и последняя неполная — отбрасываются. → dict ts,o,h,l,c,day."""
    per = {'1h': HOUR_MS, '4h': 4 * HOUR_MS, '1D': DAY_MS}[tf]
    need = max(1, (per // (15 * 60_000)) // 2)
    ts = k15['ts']
    if not len(ts):
        return None
    g = _group(ts // per, ts, k15, need)
    if g is None:
        return None
    g['ts'] = g['label'] * per
    g['day'] = g['ts'] // DAY_MS
    return {k: g[k] for k in ('ts', 'o', 'h', 'l', 'c', 'day')}


def load_crypto_15m(vdir, sym, mode='pre', session=None):
    """15m-кэш Binance (v92/v95) с WARM_START; mode как у load_raw (до LOCKBOX / полный через сессию)."""
    k = V.load_klines(vdir, sym, '15m')
    if k is None:
        return None
    lb = L.lockbox_start_ms()
    if mode == 'full' and session is None:
        raise L.LockboxError('mode=full требует сессию финала')
    m = k['ts'] >= warm_start_ms()
    out = {c: k[c][m] for c in ('o', 'h', 'l', 'c', 'v')}
    ts = k['ts'][m]
    del k
    if mode == 'pre':
        m = ts < lb
        out = {c: v[m] for c, v in out.items()}
        out['ts'] = ts[m]
    else:
        pre = ts < lb
        t2, *rest = session.slice(ts, out['o'], out['h'], out['l'], out['c'], out['v'])
        out = {c: np.concatenate([out[c][pre], r]) for c, r in zip(('o', 'h', 'l', 'c', 'v'), rest)}
        out['ts'] = np.concatenate([ts[pre], t2])
    return out


def funding_per_bar(bar_ts, f_ts, f_rate, step_ms):
    """Сумма ставок фандинга, начисленных внутри каждого бара [ts, ts+step): векторно по cumsum. Нет данных → нули."""
    if f_ts is None or not len(f_ts):
        return np.zeros(len(bar_ts))
    cs = np.r_[0.0, np.cumsum(f_rate)]
    a = np.searchsorted(f_ts, bar_ts, side='right')            # события с временем > ts (начисление ровно в ts относится к предыдущему бару)
    b = np.searchsorted(f_ts, bar_ts + step_ms, side='right')
    return cs[b] - cs[a]


def step_ms_of(tf):
    return {'1h': HOUR_MS, '4h': 4 * HOUR_MS, '1D': DAY_MS}[tf]
