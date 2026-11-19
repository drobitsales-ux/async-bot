"""research_xdata.py — данные Bybit и BingX для H2-репликации (BOT_SPEC §2.47) и межбиржевого арбитража фандинга X1 (§2.48): рынки USDT-перпетуалов (ccxt, ТОЛЬКО публичные методы), дневные
свечи и история фандинга с кэшем и повторами, правило вселенной (монеты, которых НИКОГДА не было в архиве Binance), панель в формате `research_bdata.load_panel`. Сеть — только в дочернем
процессе (research_runner); ни одного вызова создания ордеров."""
import asyncio
import json
import os
import re
import time

import numpy as np

import research_bdata as B
import research_vision as V

DAY = 86_400_000
EXCHANGES = ('bybit', 'bingx')
RETRY_DELAYS = (2, 6, 15)
LIQ_COARSE = 20_000_000.0               # грубый фильтр загрузки фандинга: максимум 30-дневного среднего оборота ≥ 20 млн USDT
FIRST30 = 30
SINCE_MS = int(np.datetime64('2019-01-01', 'ms').astype(np.int64))
MAX_FUND_PAGES = 400
PREFIX_RE = re.compile(r'^(1000000|100000|10000|1000|100|10|1M)(?=[A-Z])')


def xdata_dir():
    return os.getenv('XDATA_DIR') or ('/data/xdata' if os.path.exists('/data') else '/tmp/xdata')


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


def index_path(d):
    return os.path.join(d, 'xindex.json')


def load_index(d):
    try:
        with open(index_path(d), encoding='utf-8') as f:
            idx = json.load(f)
    except (OSError, ValueError):
        idx = {}
    for k in ('markets', 'bars', 'fund', 'fail'):
        idx.setdefault(k, {ex: {} for ex in EXCHANGES} if k != 'fail' else {})
    idx.setdefault('t', 0)
    return idx


def save_index(d, idx):
    os.makedirs(d, exist_ok=True)
    tmp = index_path(d) + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(idx, f, ensure_ascii=False)
    os.replace(tmp, index_path(d))


def save_bars(d, ex, sym, ts, o, h, l, c, v):
    V._atomic_savez(bars_path(d, ex, sym), ts=np.asarray(ts, np.int64), o=np.asarray(o, np.float64), h=np.asarray(h, np.float64), l=np.asarray(l, np.float64), c=np.asarray(c, np.float64), v=np.asarray(v, np.float64))


def load_bars(d, ex, sym):
    z = V._load_npz(bars_path(d, ex, sym))
    if z is None:
        return None
    return {k: z[k] for k in ('ts', 'o', 'h', 'l', 'c', 'v')} | {'qv': z['v'] * z['c']}


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
def usdt_swaps(markets):
    """Линейные USDT-перпетуалы из ccxt-markets (неактивные включаются, если биржа их отдала). → {symbol: {base, norm, active, created}}."""
    out = {}
    for sym, m in (markets or {}).items():
        if not (m.get('swap') and m.get('linear') and str(m.get('quote')) == 'USDT' and str(m.get('settle') or 'USDT') == 'USDT'):
            continue
        base = str(m.get('base') or '')
        out[sym] = {'base': base, 'norm': norm_base(base), 'active': m.get('active') is not False, 'created': m.get('created')}
    return out


# ── загрузка (ccxt, публичные методы) ───────────────────────────────────────────────────────────────
class Fetcher:
    """Обёртка вызовов ccxt: повторы (2/6/15 с), пауза между успешными вызовами (HTF_FETCH_PAUSE_SEC), счётчики. sleep подменяется в тестах."""

    def __init__(self, ex, pause=None, sleep=asyncio.sleep):
        self.ex, self.sleep, self.calls, self.errors = ex, sleep, 0, 0
        if pause is None:
            import research_htf
            pause = research_htf.fetch_pause()
        self.pause = pause

    async def call(self, name, *a, **kw):
        last = None
        for delay in (0,) + RETRY_DELAYS:
            if delay:
                await self.sleep(delay)
            try:
                r = await getattr(self.ex, name)(*a, **kw)
                self.calls += 1
                await self.sleep(self.pause)
                return r
            except Exception as e:                      # сеть/лимиты — повтор; последняя ошибка поднимается
                last = e
                self.errors += 1
        raise last


async def fetch_bars(f, sym, since_ms, now_ms, limit=1000):
    """Дневные свечи вперёд от since_ms, только ЗАКРЫТЫЕ (бар с началом + сутки ≤ now). → (ts, o, h, l, c, v) массивы."""
    rows, since = {}, since_ms
    for _ in range(200):
        got = await f.call('fetch_ohlcv', sym, '1d', since, limit)
        if not got:
            break
        new = 0
        for r in got:
            if r[0] not in rows:
                rows[int(r[0])] = r
                new += 1
        nxt = int(got[-1][0]) + DAY
        if new == 0 or nxt <= since or nxt > now_ms:
            break
        since = nxt
    ts = sorted(t for t in rows if t + DAY <= now_ms)
    a = np.array([rows[t][1:6] for t in ts], float) if ts else np.zeros((0, 5))
    return np.array(ts, np.int64), a[:, 0], a[:, 1], a[:, 2], a[:, 3], a[:, 4]


async def fetch_funding(f, ex_id, sym, stop_ms, limit=None):
    """История фандинга постранично НАЗАД (параметры конца интервала `until`/`endTime`), до stop_ms (первый бар символа) либо пока страница не даёт более ранних записей.
    → (ts, rate) по возрастанию, время округлено до секунды."""
    lim = limit or (200 if ex_id == 'bybit' else 1000)
    ev, end, earliest = {}, None, None
    for _ in range(MAX_FUND_PAGES):
        params = {} if end is None else ({'until': end} if ex_id == 'bybit' else {'endTime': end})   # BingX в ccxt 4.2.14: 'until' кладётся в startTime — конец передаём сырым endTime (как research_htf)
        got = await f.call('fetch_funding_rate_history', sym, None, lim, params)
        batch = [(int(x['timestamp']), float(x['fundingRate'])) for x in (got or []) if x.get('timestamp') is not None and x.get('fundingRate') is not None]
        if not batch:
            break
        mn = min(t for t, _r in batch)
        for t, r in batch:
            ev[t] = r
        if earliest is not None and mn >= earliest:
            break
        earliest = mn
        if mn <= stop_ms:
            break
        end = mn - 1
    ts = sorted(ev)
    return round_sec(np.array(ts, np.int64)) if ts else np.zeros(0, np.int64), np.array([ev[t] for t in ts], float)


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
    cand = {}
    for ex in EXCHANGES:
        for sym, m in markets.get(ex, {}).items():
            b = bars.get(ex, {}).get(sym)
            if b is None or not len(b['ts']) or m['norm'] in binance_norm:
                continue
            cand.setdefault(m['norm'], []).append((ex, sym, b))
    out, ref = [], {'stable': [], 'pre_existing': [], 'both': 0, 'not_in_binance_bases': len(cand)}
    for norm in sorted(cand):
        opts = cand[norm]
        if len(opts) > 1:
            ref['both'] += 1
        ex, sym, b = max(opts, key=lambda o: (first30_turnover(o[2]), o[0] == 'bybit'))
        if B.stable_like(b['c']):
            ref['stable'].append(f'{ex}:{sym}')
            continue
        if earliest[ex] is not None and int(b['ts'][0]) <= earliest[ex]:
            ref['pre_existing'].append(f'{ex}:{sym}')
            continue
        out.append({'ex': ex, 'sym': sym, 'norm': norm, 'first_day': int(b['ts'][0]) // DAY})
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
async def run_xload(params, progress=None, exchanges=None, now_ms=None, sleep=asyncio.sleep):
    """`/h2_replicate load` и `/xfund_test load`: рынки → дневные свечи (вселенная H2 + общие символы X1) → фандинг для грубо ликвидных; продолжение с места остановки, бюджет времени max_minutes.
    exchanges — {id: ccxt-клиент} (в тестах фейки). → текст отчёта."""
    d = params.get('xdir') or xdata_dir()
    now_ms = int(now_ms or time.time() * 1000)
    deadline = time.time() + float(params.get('max_minutes', 150)) * 60
    own = exchanges is None
    if own:
        import ccxt.async_support as ccxt
        exchanges = {'bybit': ccxt.bybit({'options': {'defaultType': 'swap'}, 'enableRateLimit': True}), 'bingx': ccxt.bingx({'options': {'defaultType': 'swap'}, 'enableRateLimit': True})}
    idx = load_index(d)
    L = [f'📥 <b>/xdata load</b> — Bybit/BingX (публичные методы ccxt) → {d}']
    stopped = ''
    try:
        fx = {ex: Fetcher(c, sleep=sleep) for ex, c in exchanges.items()}
        mk = {}
        for ex, c in exchanges.items():
            try:
                m = await fx[ex].call('load_markets')
            except Exception as e:
                return '\n'.join(L + [f'⛔ {ex}: load_markets не удался: {type(e).__name__}'])
            mk[ex] = usdt_swaps(m)
            idx['markets'][ex] = {s: {'base': v['base'], 'norm': v['norm'], 'active': v['active'], 'created': v['created']} for s, v in mk[ex].items()}
            L.append(f'  {ex}: линейных USDT-перпетуалов {len(mk[ex])} (неактивных {sum(1 for v in mk[ex].values() if not v["active"])})')
        bnorm = binance_norm_set(params.get('bdir'))
        norms = {ex: {} for ex in EXCHANGES}
        for ex in EXCHANGES:
            for s, v in mk.get(ex, {}).items():
                norms[ex].setdefault(v['norm'], []).append(s)
        common = set(norms['bybit']) & set(norms['bingx'])
        want = {ex: [] for ex in EXCHANGES}
        for ex in EXCHANGES:
            for s, v in mk.get(ex, {}).items():
                if v['norm'] not in bnorm or v['norm'] in common:
                    want[ex].append(s)
        L.append(f'  вселенная H2 (базы вне архива Binance): {sum(1 for n in set(norms["bybit"]) | set(norms["bingx"]) if n not in bnorm)} баз; общих баз Bybit∩BingX: {len(common)}; символов к загрузке свечей: ' + ', '.join(f'{ex} {len(want[ex])}' for ex in EXCHANGES))
        # 1. свечи
        n_new = 0
        for ex in EXCHANGES:
            for sym in want[ex]:
                if time.time() > deadline:
                    stopped = '⏱ бюджет времени исчерпан — повторите команду (продолжит с места остановки)'
                    break
                rec = idx['bars'][ex].get(sym)
                if rec and rec.get('last', 0) + 2 * DAY >= now_ms - DAY:
                    continue
                cur = load_bars(d, ex, sym)
                since = (int(cur['ts'][-1]) + DAY) if cur is not None and len(cur['ts']) else SINCE_MS
                try:
                    ts, o, h, l_, c, v = await fetch_bars(fx[ex], sym, since, now_ms)
                except Exception as e:
                    idx['fail'][f'{ex}:{sym}:bars'] = type(e).__name__
                    continue
                if cur is not None and len(cur['ts']):
                    ts = np.concatenate([cur['ts'], ts]); o = np.concatenate([cur['o'], o]); h = np.concatenate([cur['h'], h]); l_ = np.concatenate([cur['l'], l_]); c = np.concatenate([cur['c'], c]); v = np.concatenate([cur['v'], v])
                if len(ts):
                    save_bars(d, ex, sym, ts, o, h, l_, c, v)
                    idx['bars'][ex][sym] = {'first': int(ts[0]), 'last': int(ts[-1]), 'n': int(len(ts)), 't': now_ms}
                n_new += 1
                if progress and n_new % 25 == 0:
                    await progress(f'свечи: загружено символов {n_new} ({ex}:{sym})')
                    save_index(d, idx)
            if stopped:
                break
        save_index(d, idx)
        # 2. фандинг для грубо ликвидных (X1: общие символы — на обеих биржах; H2: вселенная)
        if not stopped:
            todo = []
            for ex in EXCHANGES:
                for sym in want[ex]:
                    b = load_bars(d, ex, sym)
                    if b is None or not coarse_liquid(b['qv']):
                        continue
                    nrm = mk[ex][sym]['norm']
                    if nrm in common and nrm in bnorm:
                        other = [s for s in norms['bingx' if ex == 'bybit' else 'bybit'].get(nrm, [])]
                        ob = [load_bars(d, 'bingx' if ex == 'bybit' else 'bybit', s) for s in other]
                        if not any(x is not None and coarse_liquid(x['qv']) for x in ob):
                            continue
                    todo.append((ex, sym, b))
            n_f = 0
            for ex, sym, b in todo:
                if time.time() > deadline:
                    stopped = '⏱ бюджет времени исчерпан — повторите команду (продолжит с места остановки)'
                    break
                rec = idx['fund'][ex].get(sym)
                if rec and rec.get('t', 0) >= now_ms - 2 * DAY:
                    continue
                try:
                    ts, r = await fetch_funding(fx[ex], ex, sym, int(b['ts'][0]))
                except Exception as e:
                    idx['fail'][f'{ex}:{sym}:fund'] = type(e).__name__
                    continue
                cur = load_fund(d, ex, sym)
                if cur is not None and len(cur[0]):
                    allts = np.concatenate([cur[0], ts]); allr = np.concatenate([cur[1], r])
                    u, ui = np.unique(allts, return_index=True)
                    ts, r = u, allr[ui]
                if len(ts):
                    save_fund(d, ex, sym, ts, r)
                idx['fund'][ex][sym] = {'first': int(ts[0]) if len(ts) else None, 'last': int(ts[-1]) if len(ts) else None, 'n': int(len(ts)), 't': now_ms}
                n_f += 1
                if progress and n_f % 10 == 0:
                    await progress(f'фандинг: загружено символов {n_f}/{len(todo)} ({ex}:{sym})')
                    save_index(d, idx)
            save_index(d, idx)
            L.append(f'  фандинг запрошен для {len(todo)} грубо ликвидных символов (загружено в этом прогоне {n_f})')
        idx['t'] = now_ms
        save_index(d, idx)
    finally:
        if own:
            for c in exchanges.values():
                try:
                    await c.close()
                except Exception:
                    pass
    L += coverage_lines(d, idx, now_ms)
    if stopped:
        L.append(stopped)
    elif idx['fail']:
        L.append(f'⚠ ошибки загрузки: {len(idx["fail"])} (первые: ' + ', '.join(f'{k}={v}' for k, v in list(idx['fail'].items())[:5]) + ') — повторите команду')
    else:
        L.append('✅ загрузка завершена')
    return '\n'.join(L)


def coverage_lines(d, idx, now_ms):
    """Глубина и неактивные/закрытые инструменты (ответ на вопрос §2.47.2 о делистингах)."""
    out = []
    for ex in EXCHANGES:
        bs = idx['bars'].get(ex, {})
        fs = idx['fund'].get(ex, {})
        if bs:
            firsts = [v['first'] for v in bs.values()]
            stale = [s for s, v in bs.items() if v['last'] + 3 * DAY < now_ms]
            out.append(f'  {ex}: свечей по {len(bs)} символам, самый ранний бар {time.strftime("%Y-%m-%d", time.gmtime(min(firsts) / 1000))}; не обновлявшихся > 3 суток (закрытые/делистингованные): {len(stale)}')
        if fs:
            fl = [v['first'] for v in fs.values() if v.get('first')]
            out.append(f'    фандинг по {len(fs)} символам; самое раннее событие {time.strftime("%Y-%m-%d", time.gmtime(min(fl) / 1000)) if fl else "н/д"}')
        mk = idx['markets'].get(ex, {})
        inactive = [s for s, v in mk.items() if not v.get('active')]
        out.append(f'    неактивных рынков в markets: {len(inactive)}' + ('' if inactive else ' — закрытых инструментов API не отдаёт: ОШИБКА ВЫЖИВШЕГО (делистинги отсутствуют; смещение ПРОТИВ шорт-стратегии)'))
    return out
