"""research_bdata.py — данные финальной серии «батарея» (BOT_SPEC §2.43): все перпетуалы Binance USDT-M (дневные свечи и fundingRate, месячные файлы data.binance.vision),
панели «день × символ», ликвидность «на дату» (point-in-time, без заглядывания вперёд), часовые ряды 16 символов H-XS (агрегация кэша 15m) с фандингом.
Сеть — только в дочернем процессе (research_runner); расчётов стратегий здесь нет. LOCKBOX читается только через сессию финала (research_ma_lib.open_lockbox_session)."""
import asyncio
import json
import os
import re
import time
import zipfile

import numpy as np

import research_fdata as FD
import research_ma_lib as L
import research_vision as V

DAY_MS = 86_400_000
HOUR_MS = 3_600_000
KL_PREFIX = 'data/futures/um/monthly/klines/'
FR_PREFIX = 'data/futures/um/monthly/fundingRate/'
GAP_ATTEMPTS = 3                          # месяц, не полученный в 3 запусках, — «постоянный разрыв»
DELIST_MONTHS = 2                         # символ делистингован, если последний месяц ≥ 2 мес. раньше последнего месяца архива
SYMBOL_RE = re.compile(r'^[A-Z0-9]{2,20}USDT$')
LIQ_MIN = 20_000_000.0                    # D3: средний дневной оборот (USDT) за 30 дней до сигнала
ADV_WINDOW = 30
TOP_N = 30                                # проскальзывание 0.0003 для топ-30 по обороту на дату, иначе 0.0005
STABLE_SIGMA = 0.005                      # стейблкоин-подобные: σ дневных лог-доходностей < 0.5% — исключаются (поправка §2.43 D1)
ATR_N = 20


def battery_dir():
    return os.getenv('BATTERY_DIR') or ('/data/battery' if os.path.exists('/data') else '/tmp/battery')


def sym_dir(d, sym):
    return os.path.join(d, 'sym', sym)


def d1_path(d, sym):
    return os.path.join(sym_dir(d, sym), 'd1.npz')


def fr_path(d, sym):
    return os.path.join(sym_dir(d, sym), 'fund.npz')


def index_path(d):
    return os.path.join(d, 'index.json')


def load_index(d):
    try:
        with open(index_path(d), encoding='utf-8') as f:
            idx = json.load(f)
    except (OSError, ValueError):
        idx = {}
    idx.setdefault('symbols', [])
    idx.setdefault('keys', {})          # sym → {'d1': [месяцы], 'fr': [месяцы]} по листингу
    idx.setdefault('attempts', {})      # 'SYM|d1|YYYY-MM' → число неудачных попыток
    idx.setdefault('done', {})          # sym → True, если все месяцы загружены либо исчерпаны попытки
    idx.setdefault('listed_t', 0)
    return idx


def save_index(d, idx):
    os.makedirs(d, exist_ok=True)
    tmp = index_path(d) + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(idx, f, ensure_ascii=False)
    os.replace(tmp, index_path(d))


def valid_symbol(name):
    return bool(SYMBOL_RE.match(name)) and '_' not in name


# ── парсинг ─────────────────────────────────────────────────────────────────────────────────────────
def parse_d1(text):
    """Дневные свечи Binance (open_time, open, high, low, close, volume, close_time, quote_volume, …; с заголовком или без). → dict ts(мс),o,h,l,c,qv."""
    lines = [ln for ln in text.splitlines() if ln.strip()]
    if not lines:
        raise V.VisionError('parse', 'пустой файл свечей')
    header = not re.match(r'^\s*-?\d', lines[0])
    rows = lines[1:] if header else lines
    if not rows:
        raise V.VisionError('parse', 'нет строк данных')
    parts = [r.split(',') for r in rows]
    if min(len(p) for p in parts) < 8:
        raise V.VisionError('parse', 'колонок < 8: нет quote_volume')
    ts = V.to_ms(np.array([int(p[0]) for p in parts], dtype=np.int64))
    f = lambda i: np.array([float(p[i]) for p in parts], dtype=np.float64)
    return {'ts': ts, 'o': f(1), 'h': f(2), 'l': f(3), 'c': f(4), 'qv': f(7)}


def parse_funding(text):
    """fundingRate: calc_time, funding_interval_hours, last_funding_rate (заголовок есть не всегда). Время округляется до секунды (калькулятор Binance добавляет миллисекунды).
    → dict ts(мс),rate."""
    lines = [ln for ln in text.splitlines() if ln.strip()]
    if not lines:
        raise V.VisionError('parse', 'пустой файл фандинга')
    header = not re.match(r'^\s*-?\d', lines[0])
    t_i, r_i = 0, -1
    rows = lines[1:] if header else lines
    if header:
        names = [x.strip().lower() for x in lines[0].split(',')]
        t_i = names.index('calc_time') if 'calc_time' in names else 0
        r_i = names.index('last_funding_rate') if 'last_funding_rate' in names else len(names) - 1
    if not rows:
        raise V.VisionError('parse', 'нет строк фандинга')
    ts = V.to_ms(np.array([int(r.split(',')[t_i]) for r in rows], dtype=np.int64))
    rate = np.array([float(r.split(',')[r_i]) for r in rows], dtype=np.float64)
    ts = (ts + 500) // 1000 * 1000
    return {'ts': ts, 'rate': rate}


# ── листинг ─────────────────────────────────────────────────────────────────────────────────────────
async def list_symbols(fetch, max_pages=50):
    """Все символы архива USDT-M (префиксы каталога klines, S3 delimiter='/'), только …USDT без даты поставки. → отсортированный список. Ошибка страницы → VisionError."""
    syms, marker, pages = [], None, 0
    while pages < max_pages:
        params = {'prefix': KL_PREFIX, 'delimiter': '/', 'max-keys': 1000}
        if marker:
            params['marker'] = marker
        r = await fetch(V.S3, params, 'GET')
        pages += 1
        if r['status'] != 200:
            raise V.VisionError('http', f'листинг символов: HTTP {r["status"]} {r["error"]}')
        t = r['body'].decode('utf-8', 'ignore')
        pref = re.findall(r'<CommonPrefixes>\s*<Prefix>([^<]+)</Prefix>\s*</CommonPrefixes>', t)
        for p in pref:
            name = p[len(KL_PREFIX):].strip('/')
            if valid_symbol(name):
                syms.append(name)
        m = re.search(r'<NextMarker>([^<]+)</NextMarker>', t)
        if '<IsTruncated>true</IsTruncated>' in t and (m or pref):
            marker = m.group(1) if m else pref[-1]
        else:
            break
    return sorted(set(syms))


def months_of(keys):
    return sorted({V.key_date(k) for k in keys if V.key_date(k)})


# ── хранение ────────────────────────────────────────────────────────────────────────────────────────
def load_d1(d, sym):
    z = V._load_npz(d1_path(d, sym))
    if z is None:
        return None
    return {'ts': z['ts'], 'o': z['o'], 'h': z['h'], 'l': z['l'], 'c': z['c'], 'qv': z['qv'], 'months': [str(x) for x in z['months']]}


def load_fund(d, sym):
    z = V._load_npz(fr_path(d, sym))
    if z is None:
        return None
    return {'ts': z['ts'], 'rate': z['rate'], 'months': [str(x) for x in z['months']]}


def _merge(old, new, month, cols):
    if old is None:
        ts, c_, months = new['ts'], {k: new[k] for k in cols}, [month]
    else:
        ts = np.concatenate([old['ts'], new['ts']])
        c_ = {k: np.concatenate([old[k], new[k]]) for k in cols}
        months = sorted(set(old['months']) | {month})
    order = np.argsort(ts, kind='stable')
    ts = ts[order]
    keep = np.r_[True, np.diff(ts) != 0] if len(ts) > 1 else np.ones(len(ts), bool)
    return ts[keep], {k: v[order][keep] for k, v in c_.items()}, months


def save_d1(d, sym, ts, cols, months):
    V._atomic_savez(d1_path(d, sym), ts=ts.astype(np.int64), months=np.array(months, dtype='U7'), **{k: cols[k].astype(np.float64) for k in ('o', 'h', 'l', 'c', 'qv')})


def save_fund(d, sym, ts, cols, months):
    V._atomic_savez(fr_path(d, sym), ts=ts.astype(np.int64), months=np.array(months, dtype='U7'), rate=cols['rate'].astype(np.float64))


KINDS = {'d1': (KL_PREFIX, parse_d1, ('o', 'h', 'l', 'c', 'qv'), load_d1, save_d1, lambda s: f'{s}/1d/'),
         'fr': (FR_PREFIX, parse_funding, ('rate',), load_fund, save_fund, lambda s: f'{s}/')}


async def sync_kind(fetch, d, idx, sym, kind, deadline=None, refresh=False):
    """Догружает недостающие месяцы одного вида (d1/fr) символа. Листинг ключей кэшируется в индексе (refresh — перелистать). → dict(listed, loaded, failed, stopped)."""
    prefix, parser, cols, loader, saver, sub = KINDS[kind]
    full_prefix = prefix + sub(sym)
    rec = idx['keys'].setdefault(sym, {})
    if refresh or kind not in rec:
        keys, _p = await V.list_keys(fetch, full_prefix)
        rec[kind] = months_of(keys)
    listed = rec[kind]
    cur = loader(d, sym)
    have = set(cur['months']) if cur else set()
    todo = [m for m in listed if m not in have and idx['attempts'].get(f'{sym}|{kind}|{m}', 0) < GAP_ATTEMPTS]
    res = {'listed': len(listed), 'loaded': 0, 'failed': 0, 'stopped': False}
    sem = asyncio.Semaphore(V.concurrency())
    got = []

    async def one(m):
        if deadline is not None and time.time() > deadline:
            res['stopped'] = True
            return
        async with sem:
            V.check_space(d)
            y, mo = m.split('-')
            key = f'{full_prefix}{sym}-{"1d" if kind == "d1" else "fundingRate"}-{m}.zip'
            try:
                body = await V.download_zip(fetch, key)
                parsed = parser(V.unzip_text(body))
            except V.VisionError as e:
                if e.kind == 'low_disk':
                    raise
                if e.kind != 'missing':
                    idx['attempts'][f'{sym}|{kind}|{m}'] = idx['attempts'].get(f'{sym}|{kind}|{m}', 0) + 1
                else:
                    idx['attempts'][f'{sym}|{kind}|{m}'] = GAP_ATTEMPTS         # 404 в листинге — файла нет, повторять нечего
                res['failed'] += 1
                return
            except (zipfile.BadZipFile, ValueError, KeyError, IndexError):
                idx['attempts'][f'{sym}|{kind}|{m}'] = idx['attempts'].get(f'{sym}|{kind}|{m}', 0) + 1
                res['failed'] += 1
                return
            got.append((m, parsed))
    outs = await asyncio.gather(*[one(m) for m in todo], return_exceptions=True)
    for o in outs:
        if isinstance(o, BaseException):
            raise o
    for m, parsed in sorted(got, key=lambda x: x[0]):
        cur = loader(d, sym)
        ts, cc, months = _merge(cur, parsed, m, cols)
        saver(d, sym, ts, cc, months)
        res['loaded'] += 1
    return res


def symbol_done(d, idx, sym):
    rec = idx['keys'].get(sym, {})
    for kind in ('d1', 'fr'):
        if kind not in rec:
            return False
        cur = (load_d1 if kind == 'd1' else load_fund)(d, sym)
        have = set(cur['months']) if cur else set()
        for m in rec[kind]:
            if m not in have and idx['attempts'].get(f'{sym}|{kind}|{m}', 0) < GAP_ATTEMPTS:
                return False
    return True


async def sync_all(fetch, d, progress=None, deadline=None, refresh=False, only=None):
    """Продолжение с места остановки: символы по алфавиту; завершённые пропускаются. → dict(symbols, done_now, stopped, n_done)."""
    idx = load_index(d)
    if not idx['symbols'] or refresh:
        idx['symbols'] = await list_symbols(fetch)
        idx['listed_t'] = time.time()
        save_index(d, idx)
    syms = [s for s in idx['symbols'] if not only or s in only]
    out = {'symbols': len(syms), 'processed': 0, 'stopped': False}
    for i, sym in enumerate(syms):
        if deadline is not None and time.time() > deadline:
            out['stopped'] = True
            break
        if idx['done'].get(sym) and not refresh:
            continue
        for kind in ('d1', 'fr'):
            r = await sync_kind(fetch, d, idx, sym, kind, deadline, refresh)
            if r['stopped']:
                out['stopped'] = True
        if not out['stopped'] and symbol_done(d, idx, sym):
            idx['done'][sym] = True
        save_index(d, idx)
        out['processed'] += 1
        if progress and (out['processed'] % 5 == 0):
            await progress(f'{sym}: символов обработано {out["processed"]}, всего в архиве {len(syms)} (завершено {sum(1 for s in syms if idx["done"].get(s))})')
    out['n_done'] = sum(1 for s in syms if idx['done'].get(s))
    return out


# ── покрытие и отчёт загрузки ───────────────────────────────────────────────────────────────────────
def month_index(m):
    y, mo = m.split('-')
    return int(y) * 12 + int(mo) - 1


def coverage(d, idx=None):
    """Состояние загрузки по символам. → dict(n_symbols, n_done, n_partial, gaps, first_month, last_month, delisted[list], listed_sample, no_files[list])."""
    idx = idx or load_index(d)
    syms = idx['symbols']
    last_global = 0
    last_of = {}
    first_of = {}
    gaps, partial, nofile, notstarted = 0, [], [], []
    for s in syms:
        rec = idx['keys'].get(s, {})
        ms = rec.get('d1', [])
        if 'd1' not in rec:
            notstarted.append(s)
        if ms:
            last_of[s] = month_index(ms[-1])
            first_of[s] = ms[0]
            last_global = max(last_global, last_of[s])
        elif 'd1' in rec:
            nofile.append(s)
        for kind in ('d1', 'fr'):
            cur = (load_d1 if kind == 'd1' else load_fund)(d, s)
            have = set(cur['months']) if cur else set()
            for m in rec.get(kind, []):
                if m not in have:
                    if idx['attempts'].get(f'{s}|{kind}|{m}', 0) >= GAP_ATTEMPTS:
                        gaps += 1
                    elif s not in partial:
                        partial.append(s)
    delisted = sorted(s for s, lm in last_of.items() if last_global - lm >= DELIST_MONTHS)
    return {'n_symbols': len(syms), 'n_done': sum(1 for s in syms if idx['done'].get(s)), 'partial': partial, 'gaps': gaps, 'delisted': delisted,
            'first_month': min(first_of.values()) if first_of else None, 'last_month': (f'{last_global // 12}-{last_global % 12 + 1:02d}' if last_global else None), 'no_files': nofile, 'not_started': notstarted}


# ── панели ──────────────────────────────────────────────────────────────────────────────────────────
def daily_funding(day0, nd, f_ts, f_rate):
    """Суммы ставок по суткам: F — по правилу (ts, ts+сутки] (расчёт ровно в 00:00 относится к ПРЕДЫДУЩИМ суткам; для начисления P&L), G — по [ts, ts+сутки) (расчёт в 00:00
    относится к своим суткам; для сигналов «строго до 00:00»), GC — число расчётов (G). → (F, G, GC) float32 длины nd."""
    F = np.zeros(nd, np.float32)
    G = np.zeros(nd, np.float32)
    GC = np.zeros(nd, np.float32)
    if f_ts is None or not len(f_ts):
        return F, G, GC
    dr = (f_ts - 1) // DAY_MS - day0
    dl = f_ts // DAY_MS - day0
    m = (dr >= 0) & (dr < nd)
    np.add.at(F, dr[m], f_rate[m])
    m = (dl >= 0) & (dl < nd)
    np.add.at(G, dl[m], f_rate[m])
    np.add.at(GC, dl[m], 1.0)
    return F, G, GC


def _clip_period(ts, mode, session, *arrs):
    lb = L.lockbox_start_ms()
    if mode == 'pre':
        m = ts < lb
        return (ts[m],) + tuple(a[m] for a in arrs)
    if session is None:
        raise L.LockboxError('mode=full требует сессию финала')
    pre = ts < lb
    t2, *rest = session.slice(ts, *arrs)
    return (np.concatenate([ts[pre], t2]),) + tuple(np.concatenate([a[pre], r]) for a, r in zip(arrs, rest))


def stable_like(c):
    """Стейблкоин-подобный ряд: σ дневных лог-доходностей < STABLE_SIGMA (поправка §2.43 D1)."""
    c = c[np.isfinite(c) & (c > 0)]
    if len(c) < 30:
        return False
    return float(np.diff(np.log(c)).std()) < STABLE_SIGMA


def _month_start_day(m):
    return int(np.datetime64(m + '-01', 'D').astype(np.int64))


def load_panel(d, mode='pre', session=None, syms=None, exclude_stable=True, with_funding=True):
    """Панель «день × символ»: O,H,L,C,QV (float32, NaN — нет бара), F/G/GC (фандинг), first/last (индексы первого/последнего бара), имена, day0. До LOCKBOX (mode='pre')
    либо полная через сессию финала. Символы читаются по одному и сразу записываются в панель (память: не держим все ряды одновременно). → dict или None, если данных нет."""
    idx = load_index(d)
    syms = list(syms or idx['symbols'])
    firsts = [idx['keys'][s]['d1'][0] for s in syms if idx['keys'].get(s, {}).get('d1')]
    lasts = [idx['keys'][s]['d1'][-1] for s in syms if idx['keys'].get(s, {}).get('d1')]
    if not firsts:
        return None
    day0 = _month_start_day(min(firsts))
    y, m = max(lasts).split('-')
    ny, nm = (int(y) + 1, 1) if int(m) == 12 else (int(y), int(m) + 1)
    day_end = _month_start_day(f'{ny}-{nm:02d}')                                   # первый день после последнего месяца архива
    if mode == 'pre':
        day_end = min(day_end, L.lockbox_start_ms() // DAY_MS)
    nd_max = day_end - day0
    if nd_max <= 0:
        return None
    n_max = len(syms)
    P = {k: np.full((nd_max, n_max), np.nan, np.float32) for k in ('O', 'H', 'L', 'C', 'QV')}
    F, G, GC = (np.zeros((nd_max, n_max), np.float32) for _ in range(3))
    names, excl, first, last, has_fund = [], [], [], [], []
    for s in syms:
        z = load_d1(d, s)
        if z is None or not len(z['ts']):
            continue
        ts, o, h, l, c, qv = _clip_period(z['ts'], mode, session, z['o'], z['h'], z['l'], z['c'], z['qv'])
        del z
        if not len(ts):
            continue
        if exclude_stable and stable_like(c):
            excl.append(s)
            continue
        j = len(names)
        names.append(s)
        di = (ts // DAY_MS - day0).astype(np.int64)
        ok = (di >= 0) & (di < nd_max)
        di = di[ok]
        for k, a_ in zip(('O', 'H', 'L', 'C', 'QV'), (o, h, l, c, qv)):
            P[k][di, j] = a_[ok]
        first.append(int(di[0]))
        last.append(int(di[-1]))
        hf = False
        if with_funding:
            fz = load_fund(d, s)
            if fz is not None and len(fz['ts']):
                fts, fr = _clip_period(fz['ts'], mode, session, fz['rate'])
                F[:, j], G[:, j], GC[:, j] = daily_funding(day0, nd_max, fts, fr)
                hf = True
        has_fund.append(hf)
    if not names:
        return None
    ns = len(names)
    nd = max(last) + 1
    out = {}
    for k in list(P):                                                               # [v114] обрезка по одной матрице с освобождением исходной (пик памяти: +1 матрица, а не ×2)
        out[k] = P.pop(k)[:nd, :ns].copy()
    del P
    out['F'] = F[:nd, :ns].copy()
    del F
    out['G'] = G[:nd, :ns].copy()
    del G
    out['GC'] = GC[:nd, :ns].copy()
    del GC
    out.update({'syms': names, 'day0': day0, 'nd': nd, 'first': np.array(first, np.int64),
                'last': np.array(last, np.int64), 'excluded_stable': excl, 'has_fund': np.array(has_fund)})
    return out


def pit_adv(QV, window=ADV_WINDOW):
    """Средний дневной оборот за `window` суток, ПРЕДШЕСТВУЮЩИХ дню d (дни d−window…d−1; день d и позже не используются), по доступным дням. → (adv NaN при 0 дней, число дней).
    Point-in-time: изменение QV в днях ≥ d не меняет adv[d]."""
    nd, ns = QV.shape
    x = np.nan_to_num(QV.astype(np.float64))
    ok = np.isfinite(QV).astype(np.float64)
    cs = np.vstack([np.zeros((1, ns)), np.cumsum(x, axis=0)])
    cn = np.vstack([np.zeros((1, ns)), np.cumsum(ok, axis=0)])
    d = np.arange(nd)
    lo = np.maximum(0, d - window)
    sums = cs[d] - cs[lo]
    cnt = cn[d] - cn[lo]
    with np.errstate(invalid='ignore', divide='ignore'):
        adv = np.where(cnt > 0, sums / np.maximum(cnt, 1), np.nan)
    return adv, cnt


def liquid_mask(adv):
    return np.isfinite(adv) & (adv >= LIQ_MIN)


def top_n_mask(adv, n=TOP_N):
    a = np.where(np.isfinite(adv), adv, -np.inf)
    rank = np.argsort(np.argsort(-a, axis=1, kind='stable'), axis=1, kind='stable') + 1
    return np.isfinite(adv) & (rank <= n)


def atr_panel(H, L_, C, n=ATR_N):
    """ATR_n (простое среднее TR за n суток, строго ДО дня d: дни d−n…d−1; нужно все n значений, иначе NaN). TR = max(H−L, |H−Cпред|, |L−Cпред|)."""
    nd, ns = C.shape
    Cp = np.vstack([np.full((1, ns), np.nan, C.dtype), C[:-1]])
    tr = np.fmax(np.fmax(H - L_, np.abs(H - Cp)), np.abs(L_ - Cp))
    tr = np.where(np.isfinite(H) & np.isfinite(L_) & np.isfinite(Cp), tr, np.nan)
    x = np.nan_to_num(tr.astype(np.float64))
    ok = np.isfinite(tr).astype(np.float64)
    cs = np.vstack([np.zeros((1, ns)), np.cumsum(x, axis=0)])
    cn = np.vstack([np.zeros((1, ns)), np.cumsum(ok, axis=0)])
    d = np.arange(nd)
    lo = np.maximum(0, d - n)
    sums, cnt = cs[d] - cs[lo], cn[d] - cn[lo]
    with np.errstate(invalid='ignore', divide='ignore'):
        return np.where(cnt >= n, sums / n, np.nan)


def slip_of(top30, mult=1.0):
    """Проскальзывание на сторону: 0.0003 для топ-30 по обороту на дату, иначе 0.0005 (×mult — стресс)."""
    return np.where(top30, 0.0003, 0.0005) * mult


# ── часовые ряды H-XS ───────────────────────────────────────────────────────────────────────────────
def hourly_series(vdir, sym, mode='pre', session=None):
    """Часовой ряд символа из кэша 15m (агрегация build_crypto): dict ts,o,h,l,c; None — нет кэша."""
    k15 = FD.load_crypto_15m(vdir, sym, mode, session)
    if k15 is None:
        return None
    g = FD.build_crypto(k15, '1h')
    if g is None:
        return None
    return {k: g[k] for k in ('ts', 'o', 'h', 'l', 'c')}


def hourly_grid(series_by_sym, names):
    """Сетка часов: общие метки t0…t1 (шаг 1 ч), массивы O,H,L,C (nh × ns, NaN — нет бара). → dict(ts0, nh, O,H,L,C, names)."""
    t0 = min(int(series_by_sym[s]['ts'][0]) for s in names) // HOUR_MS
    t1 = max(int(series_by_sym[s]['ts'][-1]) for s in names) // HOUR_MS
    nh = t1 - t0 + 1
    out = {k: np.full((nh, len(names)), np.nan, np.float64) for k in ('O', 'H', 'L', 'C')}
    for j, s in enumerate(names):
        hi = (series_by_sym[s]['ts'] // HOUR_MS - t0).astype(np.int64)
        for k, kk in zip(('O', 'H', 'L', 'C'), ('o', 'h', 'l', 'c')):
            out[k][hi, j] = series_by_sym[s][kk]
    out.update({'t0': t0, 'nh': nh, 'names': list(names)})
    return out


def hourly_funding(grid, fund_by_sym):
    """Фандинг по часовым барам [ts, ts+1ч] (расчёт ровно в ts+1ч относится к бару) и события (для знака): → (FB nh × ns float64, events {sym: (ts, rate)})."""
    nh, ns = grid['nh'], len(grid['names'])
    FB = np.zeros((nh, ns))
    bar_ts = (grid['t0'] + np.arange(nh)) * HOUR_MS
    for j, s in enumerate(grid['names']):
        f = fund_by_sym.get(s)
        if f is not None and len(f[0]):
            FB[:, j] = FD.funding_per_bar(bar_ts, f[0], f[1], HOUR_MS)
    return FB
