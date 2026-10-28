"""audit_sim.py — симуляция сигналов внешнего источника и отчёт /audit (BOT_SPEC §2.44.2–2.44.3). Расчёты — в research_runner. Модель A «как в канале» (плечо, изолированная маржа,
ликвидация) и модель B «разумный риск» (1% капитала до заявленного стопа, без ликвидации); решение по критериям §2.44.2 п. 5 — по модели B и только по форвард-сигналам (source = live)."""
import json
import math
import os
import time
from datetime import datetime, timezone

import numpy as np

import audit_db as AD
import research_fengine as E

MIN = 60_000
DAY = 86_400_000
ENTRY_WINDOW_MS = 24 * 3_600_000
HOLD_MS = 7 * DAY
SLIP = 0.0005
RISK_CAP = 0.01
MIN_N = 100
MIN_T = 2.5
MIN_PF = 1.3


# ── ядро: вход и выход по 1m-свечам ─────────────────────────────────────────────────────────────────
def find_entry(c, pub_ms, side, lo, hi):
    """Первая свеча (со следующей минуты после публикации, в окне 24 ч), задевшая уровень входа: шорт — нижняя граница зоны (high ≥ уровня), лонг — верхняя (low ≤ уровня).
    → (индекс, уровень) или (None, уровень)."""
    level = lo if side == 'short' else hi
    ts = c['ts']
    i0 = int(np.searchsorted(ts, (pub_ms // MIN + 1) * MIN, side='left'))
    i1 = int(np.searchsorted(ts, pub_ms + ENTRY_WINDOW_MS, side='left'))
    if i1 <= i0:
        return None, level
    seg = c['h'][i0:i1] >= level if side == 'short' else c['l'][i0:i1] <= level
    hit = np.flatnonzero(seg)
    return (i0 + int(hit[0]) if len(hit) else None), level


def walk(side, c, i_fill, level, targets, stop, liq_px, now_ms):
    """Путь позиции после входа. Приоритет в одной свече: стоп/ликвидация раньше цели; цели — со следующей свечи после входа; после первой цели стоп оставшейся части = цена входа
    (действует со следующей свечи); таймаут 7 суток от входа — остаток по close свечи таймаута. → dict(fills=[(kind, price, portion, ts)], closed, liq, hit=[номера целей])."""
    n = len(targets)
    portion = 1.0 / n
    short = side == 'short'
    ts, h, l, cl = c['ts'], c['h'], c['l'], c['c']
    t_end = ts[i_fill] + HOLD_MS
    i_end = int(np.searchsorted(ts, t_end, side='left'))
    last = min(len(ts), i_end)
    remaining = 1.0
    cur_stop = stop
    be = False
    nxt = 0
    fills, hit = [], []
    liq_closer = liq_px is not None and ((liq_px < stop) if short else (liq_px > stop))
    for i in range(i_fill, last):
        hi_, lo_ = float(h[i]), float(l[i])
        if not be and liq_closer and ((hi_ >= liq_px) if short else (lo_ <= liq_px)):
            return {'fills': [('liq', liq_px, remaining, int(ts[i]))], 'closed': True, 'liq': True, 'hit': hit}
        if (hi_ >= cur_stop) if short else (lo_ <= cur_stop):
            fills.append(('be' if be else 'stop', cur_stop, remaining, int(ts[i])))
            return {'fills': fills, 'closed': True, 'liq': False, 'hit': hit}
        if i > i_fill:
            while nxt < n and ((lo_ <= targets[nxt]) if short else (hi_ >= targets[nxt])):
                fills.append(('tp', targets[nxt], portion, int(ts[i])))
                hit.append(nxt + 1)
                remaining -= portion
                nxt += 1
                if not be:
                    be, cur_stop = True, level                      # после первой цели стоп остатка = цена входа (с следующей свечи)
            if remaining <= 1e-9:
                return {'fills': fills, 'closed': True, 'liq': False, 'hit': hit}
    if i_end <= len(ts) - 1 or now_ms >= t_end:
        j = last - 1                                              # close последней свечи внутри срока хранения
        fills.append(('timeout', float(cl[j]), remaining, int(ts[j])))
        return {'fills': fills, 'closed': True, 'liq': False, 'hit': hit}
    return {'fills': fills, 'closed': False, 'liq': False, 'hit': hit}


def unit_pnl(side, level, fills, fees, funding=None, fill_ts=None):
    """P&L в долях начального номинала: издержки вход (taker) + выходы (maker на цели, taker на остальные), проскальзывание на каждой стороне, фандинг по начислениям внутри удержания
    (лонг платит положительную ставку; доля открытой части на момент начисления)."""
    d = 1.0 if side == 'long' else -1.0
    e_eff = level * (1.0 + d * SLIP)
    pnl = -fees['taker']
    for kind, px, portion, _t in fills:
        x_eff = px * (1.0 - d * SLIP)
        pnl += portion * (d * (x_eff / e_eff - 1.0)) - portion * (fees['maker'] if kind == 'tp' else fees['taker'])
    if funding is not None and fills and fill_ts is not None:
        t_last = max(f[3] for f in fills)
        for ft, fr in zip(*funding):
            if fill_ts < ft <= t_last:
                rem = 1.0 - sum(p for _k, _x, p, tt in fills if tt < ft)
                pnl -= d * float(fr) * rem
    return pnl


def liq_price(side, level, lev):
    dist = 1.0 / lev - 0.005
    return level * (1.0 + dist) if side == 'short' else level * (1.0 - dist)


def simulate(sig, c, fees, now_ms, funding=None):
    """Один сигнал. sig: dict(side, leverage, entry_lo, entry_hi, targets, stop, pub_ms). c: 1m-свечи или None.
    → dict(status='no_data'|'pending'|'not_filled'|'filled', A, B, hit, fill_ts, exit_ts, closed)."""
    if c is None or len(c['ts']) == 0:
        return {'status': 'no_data'}
    side = sig['side']
    i_fill, level = find_entry(c, sig['pub_ms'], side, sig['entry_lo'], sig['entry_hi'])
    if i_fill is None:
        return {'status': 'pending' if now_ms < sig['pub_ms'] + ENTRY_WINDOW_MS else 'not_filled'}
    fill_ts = int(c['ts'][i_fill])
    out = {'status': 'filled', 'fill_ts': fill_ts, 'level': level}
    wb = walk(side, c, i_fill, level, sig['targets'], sig['stop'], None, now_ms)
    sd = abs(sig['stop'] - level) / level
    pb = unit_pnl(side, level, wb['fills'], fees, funding, fill_ts)
    out['B'] = {'R': pb / sd if sd > 0 else float('nan'), 'closed': wb['closed'], 'kinds': [f[0] for f in wb['fills']]}
    out['hit'] = wb['hit']
    out['closed'] = wb['closed']
    out['exit_ts'] = max((f[3] for f in wb['fills']), default=fill_ts)
    lev = sig.get('leverage')
    if lev:
        wa = walk(side, c, i_fill, level, sig['targets'], sig['stop'], liq_price(side, level, lev), now_ms)
        if wa['liq']:
            ret = -1.0
        else:
            ret = max(-1.0, lev * unit_pnl(side, level, wa['fills'], fees, funding, fill_ts))
        out['A'] = {'ret': ret, 'liq': wa['liq'], 'closed': wa['closed'], 'liq_ts': (wa['fills'][0][3] if wa['liq'] else None)}
        out['hit_A'] = wa['hit']
    else:
        out['A'] = None
    return out


# ── статистика ──────────────────────────────────────────────────────────────────────────────────────
def r_stats(rs):
    rs = np.asarray(rs, float)
    n = len(rs)
    if not n:
        return {'n': 0, 'wr': None, 'mean': None, 'sd': None, 't': None, 'pf': None, 'streak': 0}
    wins, losses = rs[rs > 0].sum(), -rs[rs < 0].sum()
    sd = float(rs.std(ddof=1)) if n > 1 else float('nan')
    streak = cur = 0
    for x in rs:
        cur = cur + 1 if x < 0 else 0
        streak = max(streak, cur)
    return {'n': n, 'wr': float((rs > 0).mean()), 'mean': float(rs.mean()), 'sd': sd, 't': (float(rs.mean() / (sd / math.sqrt(n))) if n > 1 and sd > 0 else float('nan')),
            'pf': (float(wins / losses) if losses > 0 else (float('inf') if wins > 0 else None)), 'streak': int(streak)}


def daily_t(items):
    """t по дням: ряд календарных суток UTC от первого до последнего закрытия; значение суток = сумма R закрытых в них позиций (нет закрытий = 0); Ньюи–Уэст лаг 5."""
    if not items:
        return float('nan'), 0
    days = [int(ts // DAY) for ts, _r in items]
    d0, d1 = min(days), max(days)
    x = np.zeros(d1 - d0 + 1)
    for (ts, r), d in zip(items, days):
        x[d - d0] += r
    return E.nw_t(x, 5)[1], len(x)


def max_dd(items, risk=RISK_CAP):
    """Максимальная просадка при риске 1% капитала на сделку: накопленная сумма risk·R в порядке закрытия (доля капитала)."""
    if not items:
        return 0.0
    cs = np.cumsum([risk * r for _ts, r in sorted(items)])
    peak = np.maximum.accumulate(np.r_[0.0, cs])[1:]
    return float(np.max(peak - cs))


def month_key(ts_ms):
    t = datetime.fromtimestamp(ts_ms / 1000, timezone.utc)
    return t.year * 12 + t.month - 1


def btc_months_from_daily(ts, c, now_ms):
    """Месячные доходности BTC по дневным close: close последнего дня месяца / close последнего дня предыдущего − 1; неполный месяц (нет дня после его конца) исключён."""
    ts = np.asarray(ts)
    c = np.asarray(c, float)
    mk = np.array([month_key(int(t)) for t in ts])
    out = {}
    for m in sorted(set(mk.tolist())):
        nxt = m + 1
        start_next = int(datetime(nxt // 12, nxt % 12 + 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
        if int(ts[-1]) + DAY < start_next:
            continue
        idx, idx_prev = np.flatnonzero(mk == m), np.flatnonzero(mk == m - 1)
        if len(idx) and len(idx_prev):
            out[m] = float(c[idx[-1]] / c[idx_prev[-1]] - 1.0)
    return out


def month_groups(rows, months_ret):
    """mean R на исполненный сигнал по месяцам роста / падения BTC (месяц публикации)."""
    res = {}
    for name, cond in (('up', lambda x: x > 0), ('down', lambda x: x < 0)):
        ms = {m for m, x in months_ret.items() if cond(x)}
        v = [r['B']['R'] for r in rows if month_key(r['pub_ms']) in ms]
        res[name] = {'n_months': len(ms), 'n': len(v), 'mean': (float(np.mean(v)) if v else None)}
    return res


def verdict(rows, months_ret):
    """Критерии §2.44.2 п. 5 по форвард-сигналам: n исполненных ≥ 100; модель B: mean R > 0 при t ≥ 2.5 (по дням), PF ≥ 1.3, mean ≥ 0 в месяцы роста и падения BTC."""
    closed = [r for r in rows if r['status'] == 'filled' and r['B']['closed'] and math.isfinite(r['B']['R'])]
    st = r_stats([r['B']['R'] for r in closed])
    td, nd = daily_t([(r['exit_ts'], r['B']['R']) for r in closed])
    grp = month_groups(closed, months_ret)
    crit = {'n': st['n'] >= MIN_N, 'mean_t': bool(st['n'] and st['mean'] > 0 and math.isfinite(td) and td >= MIN_T), 'pf': bool(st['pf'] is not None and st['pf'] >= MIN_PF),
            'up': (None if grp['up']['mean'] is None else grp['up']['mean'] >= 0), 'down': (None if grp['down']['mean'] is None else grp['down']['mean'] >= 0)}
    ok = all(v for v in crit.values() if v is not None)
    return {'stats': st, 't_day': td, 'n_days': nd, 'groups': grp, 'crit': crit, 'pass': bool(ok)}


# ── цены ────────────────────────────────────────────────────────────────────────────────────────────
def scale_ok(c, ref, factor=3.0):
    if c is None or not len(c['c']):
        return False
    med = float(np.median(c['c']))
    return ref / factor <= med <= ref * factor


class PriceSource:
    """1m-свечи: BingX (ccxt), затем Binance (архив) с пометкой. Кэш — по суткам (npz), неполные текущие сутки не кэшируются. Сеть — только в дочернем процессе runner."""

    def __init__(self, cache_dir, session=None):
        self.cache = cache_dir
        self.session = session
        self.ex = None
        self.markets = {}
        os.makedirs(cache_dir, exist_ok=True)

    async def open(self):
        import ccxt.async_support as ccxt
        self.ex = ccxt.bingx({'options': {'defaultType': 'swap'}, 'enableRateLimit': True})
        try:
            self.markets = await self.ex.load_markets()
        except Exception:
            self.markets = {}

    async def close(self):
        if self.ex is not None:
            await self.ex.close()

    def _cpath(self, src, sym, day):
        return os.path.join(self.cache, f'{src}_{sym.replace("/", "_").replace(":", "_")}_{day}.npz')

    async def _bingx_day(self, sym, day, now_ms):
        p = self._cpath('bingx', sym, day)
        if os.path.exists(p):
            with np.load(p) as z:
                return {k: z[k] for k in z.files}
        t0, t1 = day * DAY, (day + 1) * DAY
        rows, since = [], t0
        while since < min(t1, now_ms):
            got = await self.ex.fetch_ohlcv(sym, '1m', since, 1000)
            if not got:
                break
            rows.extend(r for r in got if t0 <= r[0] < t1)
            nxt = got[-1][0] + MIN
            if nxt <= since:
                break
            since = nxt
        if not rows:
            return None
        a = np.array(sorted({int(r[0]): r for r in rows}.values()), float)
        d = {'ts': a[:, 0].astype(np.int64), 'o': a[:, 1], 'h': a[:, 2], 'l': a[:, 3], 'c': a[:, 4]}
        if t1 <= now_ms:
            np.savez_compressed(p, **d)
        return d

    async def _binance_day(self, sym, day, now_ms):
        import research_vision as V
        p = self._cpath('binance', sym, day)
        if os.path.exists(p):
            with np.load(p) as z:
                return {k: z[k] for k in z.files}
        if (day + 1) * DAY > now_ms:
            return None
        ds = datetime.fromtimestamp(day * DAY / 1000, timezone.utc).strftime('%Y-%m-%d')
        key = f'data/futures/um/daily/klines/{sym}/1m/{sym}-1m-{ds}.zip'
        try:
            body = await V.download_zip(V.make_fetcher(self.session), key)
            q = V.parse_klines_csv(V.unzip_text(body))
        except Exception:
            return None
        d = {'ts': q['ts'], 'o': q['o'], 'h': q['h'], 'l': q['l'], 'c': q['c']}
        np.savez_compressed(p, **d)
        return d

    @staticmethod
    def _join(parts):
        parts = [x for x in parts if x is not None]
        if not parts:
            return None
        ts = np.concatenate([x['ts'] for x in parts])
        o = np.argsort(ts, kind='stable')
        return {k: np.concatenate([x[k] for x in parts])[o] for k in ('ts', 'o', 'h', 'l', 'c')}

    async def candles(self, base, start_ms, end_ms, ref_price, now_ms):
        """→ (свечи | None, источник/примечание)."""
        d0, d1 = start_ms // DAY, min(end_ms, now_ms) // DAY
        cands = [f'{base}/USDT:USDT', f'1000{base}/USDT:USDT']
        for sym in cands:
            if sym in self.markets:
                try:
                    c = self._join([await self._bingx_day(sym, d, now_ms) for d in range(d0, d1 + 1)])
                except Exception:
                    c = None
                if c is not None and scale_ok(c, ref_price):
                    return c, 'BingX ' + sym
        for sym in (f'{base}USDT', f'1000{base}USDT'):
            c = self._join([await self._binance_day(sym, d, now_ms) for d in range(d0, d1 + 1)])
            if c is not None and scale_ok(c, ref_price):
                return c, 'Binance архив (не BingX) ' + sym
        return None, 'нет данных'

    async def funding(self, base, start_ms, end_ms):
        for sym in (f'{base}/USDT:USDT', f'1000{base}/USDT:USDT'):
            if sym in self.markets:
                try:
                    got = await self.ex.fetch_funding_rate_history(sym, since=start_ms, limit=1000, params={'endTime': end_ms})
                    rows = sorted((int(r['timestamp']), float(r['fundingRate'])) for r in got if r.get('timestamp') and r.get('fundingRate') is not None)
                    rows = [r for r in rows if start_ms <= r[0] <= end_ms]
                    return (np.array([r[0] for r in rows]), np.array([r[1] for r in rows]))
                except Exception:
                    return None
        return None

    async def btc_daily(self, since_ms, now_ms):
        """Дневные свечи BTC за весь период (постранично по 1000 дней)."""
        try:
            ts, cl, since = [], [], since_ms
            while since < now_ms:
                got = await self.ex.fetch_ohlcv('BTC/USDT:USDT', '1d', since, 1000)
                if not got:
                    break
                ts += [r[0] for r in got]
                cl += [r[4] for r in got]
                if got[-1][0] + DAY <= since:
                    break
                since = got[-1][0] + DAY
                if len(got) < 2:
                    break
            return (ts, cl) if ts else None
        except Exception:
            return None


# ── прогон и отчёт ──────────────────────────────────────────────────────────────────────────────────
def load_signals(con):
    out = []
    for r in con.execute('SELECT channel, msg_id, source, pub_ts, symbol, side, leverage, entry_lo, entry_hi, targets, stop FROM audit_signals ORDER BY pub_ts'):
        out.append({'channel': r[0], 'msg_id': r[1], 'source': r[2], 'pub_ts': r[3], 'symbol': r[4], 'side': r[5], 'leverage': r[6], 'entry_lo': r[7], 'entry_hi': r[8], 'targets': json.loads(r[9]), 'stop': r[10]})
    return out


def load_reports(con):
    """Отчёты о целях: все сохранённые сообщения-не-сигналы разбираются ЗАНОВО текущим парсером (таблицы неизменяемы, поэтому переклассификация — при чтении). → (reports, reply_map {(канал, id): reply_to})."""
    import audit_parse as AP
    out, reply_map = [], {}
    for r in con.execute("SELECT channel, msg_id, source, pub_ts, symbol, reply_to, text FROM audit_other WHERE kind IN ('report','other')"):
        if r[5]:
            reply_map[(r[0], r[1])] = r[5]
        p = AP.parse(r[6] or '')
        if p['kind'] == 'report':
            out.append({'channel': r[0], 'msg_id': r[1], 'source': r[2], 'pub_ts': r[3], 'symbol': p.get('symbol') or r[4], 'reply_to': r[5], 'side': p.get('side'),
                        'target_no': p.get('target_no'), 'claimed_pct': p.get('claimed_pct')})
    return out, reply_map


LINK_MAX_AGE_S = 14 * 86400
REPLY_HOPS = 5
MATCH_REL = 0.10


def link_reports(reports, sigs, reply_map=None):
    """Привязка отчёта к сигналу (BOT_SPEC §2.44.8 п. 2): (а) по reply_to (по цепочке ответов до сигнала, ≤ 5 звеньев); (б) иначе — последний сигнал того же символа (и направления, если названо)
    в том же канале не старше 14 суток. → список dict(report, signal|None, how='reply'|'symbol'|None, why)."""
    reply_map = reply_map or {}
    by_key = {(s['channel'], s['msg_id']): s for s in sigs}
    links = []
    for rp in reports:
        sig, how = None, None
        cur = rp.get('reply_to')
        for _ in range(REPLY_HOPS):
            if not cur:
                break
            if (rp['channel'], cur) in by_key:
                sig, how = by_key[(rp['channel'], cur)], 'reply'
                break
            cur = reply_map.get((rp['channel'], cur))
        if sig is None and rp.get('symbol'):
            cand = [s for s in sigs if s['channel'] == rp['channel'] and s['symbol'] == rp['symbol'] and 0 <= (rp['pub_ts'] or 0) - s['pub_ts'] <= LINK_MAX_AGE_S
                    and (not rp.get('side') or s['side'] == rp['side'])]
            if cand:
                sig, how = max(cand, key=lambda x: x['pub_ts']), 'symbol'
        links.append({'report': rp, 'signal': sig, 'how': how, 'why': '' if sig else ('нет reply_to и символа' if not rp.get('symbol') and not rp.get('reply_to') else 'сигнал не найден (14 суток / символ)')})
    return links


def report_check(links, results):
    """Сверка по привязкам: results — {(канал, id сигнала): итог simulate}. Для каждой привязки с номером цели: заявленный %, рассчитанный по модели A % до издержек, достигнута ли цель до ликвидации/стопа,
    ликвидирован ли сигнал к моменту отчёта. → (rows, итоги)."""
    rows = []
    for l in links:
        rp, s = l['report'], l['signal']
        if s is None:
            continue
        k = rp.get('target_no')
        res = results.get((s['channel'], s['msg_id']))
        row = {'symbol': s['symbol'], 'k': k, 'claimed': rp.get('claimed_pct'), 'how': l['how'], 'computed': None, 'reached': None, 'match': None, 'after_liq': None, 'lev': s['leverage']}
        if k and 1 <= k <= len(s['targets']):
            lvl = s['entry_lo'] if s['side'] == 'short' else s['entry_hi']
            d = 1.0 if s['side'] == 'long' else -1.0
            row['computed'] = d * (s['targets'][k - 1] / lvl - 1.0) * 100.0 * (s['leverage'] or 1)
        if res is not None and res.get('status') == 'filled':
            hit = res.get('hit_A') if res.get('A') else res.get('hit')
            row['reached'] = bool(k) and k in (hit or [])
            if res.get('A') and res['A'].get('liq'):
                row['after_liq'] = res['A']['liq_ts'] is not None and res['A']['liq_ts'] <= (rp['pub_ts'] or 0) * 1000
            else:
                row['after_liq'] = False
        elif res is not None:
            row['reached'] = False
            row['after_liq'] = False
        if row['computed'] is not None and rp.get('claimed_pct') is not None:
            row['match'] = abs(rp['claimed_pct'] - row['computed']) <= MATCH_REL * abs(row['computed'])
        rows.append(row)
    n = len(rows)
    with_k = [r for r in rows if r['k'] and r['computed'] is not None]
    ev = [r for r in with_k if r['reached'] is not None]
    summ = {'n_reports': len(links), 'n_linked': n, 'n_reply': sum(1 for r in rows if r['how'] == 'reply'), 'n_symbol': sum(1 for r in rows if r['how'] == 'symbol'),
            'n_no_k': n - len(with_k), 'n_eval': len(ev),
            'reached': sum(1 for r in ev if r['reached']), 'match': sum(1 for r in with_k if r['match']), 'n_match': len(with_k),
            'after_liq': sum(1 for r in ev if r['after_liq']), 'abs_med': (float(np.median([abs(r['claimed'] - r['computed']) for r in with_k])) if with_k else None)}
    return rows, summ


def expectancy(closed):
    """Расшифровка ожидания модели B (BOT_SPEC §2.44.8 п. 3): средний R выигрыша и проигрыша, доля безубытков после первой цели, вклад полных стопов; строка-пояснение."""
    R = [r['B']['R'] for r in closed if math.isfinite(r['B']['R'])]
    kinds = [r['B'].get('kinds') or [] for r in closed if math.isfinite(r['B']['R'])]
    n = len(R)
    if not n:
        return None
    arr = np.asarray(R)
    wins, losses = arr[arr > 0], arr[arr < 0]
    w_avg = float(wins.mean()) if len(wins) else 0.0
    l_avg = float(-losses.mean()) if len(losses) else 0.0
    be = sum(1 for k in kinds if 'be' in k and 'tp' in k)
    stops = [x for x, k in zip(R, kinds) if 'stop' in k and 'tp' not in k]
    timeouts = [x for x, k in zip(R, kinds) if 'timeout' in k]
    out = {'n': n, 'wr': float((arr > 0).mean()), 'w_avg': w_avg, 'l_avg': l_avg, 'ratio': (l_avg / w_avg if w_avg > 0 else None), 'be_share': be / n, 'n_be': be,
           'n_stop': len(stops), 'sum_stop': float(sum(stops)), 'mean_stop': (float(np.mean(stops)) if stops else None), 'n_timeout': len(timeouts), 'sum_all': float(arr.sum()),
           'sum_rest': float(arr.sum() - sum(stops)), 'mean': float(arr.mean())}
    if out['ratio'] is not None and l_avg > 0:
        out['text'] = (f'WR {out["wr"] * 100:.0f}%, но средний проигрыш в {out["ratio"]:.1f} раза ' + ('больше' if out['ratio'] >= 1 else 'меньше') + f' среднего выигрыша '
                       f'(выигрыш {w_avg:+.3f} R, проигрыш −{l_avg:.3f} R) → ожидание {out["mean"]:+.3f} R на сделку')
    else:
        out['text'] = f'WR {out["wr"] * 100:.0f}%, ожидание {out["mean"]:+.3f} R на сделку'
    return out


def fm(x, nd=2):
    return 'н/д' if x is None or (isinstance(x, float) and not math.isfinite(x)) else f'{x:.{nd}f}'


async def run_simulation(sigs, source, fees, now_ms, progress=None):
    rows = []
    for i, s in enumerate(sigs):
        pub_ms = s['pub_ts'] * 1000
        ref = (s['entry_lo'] + s['entry_hi']) / 2
        c, note = await source.candles(s['symbol'], pub_ms, pub_ms + ENTRY_WINDOW_MS + HOLD_MS, ref, now_ms)
        fund = await source.funding(s['symbol'], pub_ms, pub_ms + ENTRY_WINDOW_MS + HOLD_MS) if c is not None and note.startswith('BingX') else None
        sim = simulate(dict(s, pub_ms=pub_ms), c, fees, now_ms, fund)
        sim.update({'key': (s['channel'], s['msg_id']), 'pub_ms': pub_ms, 'source': s['source'], 'symbol': s['symbol'], 'side': s['side'], 'lev': s['leverage'], 'note': note, 'fund_missing': fund is None})
        rows.append(sim)
        if progress and (i + 1) % 20 == 0:
            await progress(f'/audit: просчитано сигналов {i + 1}/{len(sigs)}')
    return rows


def section(title, rows, months_ret):
    """Блок отчёта по набору строк (одна группа источника)."""
    L = []
    n_all = len(rows)
    cnt = {k: sum(1 for r in rows if r['status'] == k) for k in ('filled', 'not_filled', 'pending', 'no_data')}
    filled = [r for r in rows if r['status'] == 'filled']
    closed = [r for r in filled if r['B']['closed']]
    L.append(f'<b>{title}</b>: сигналов {n_all}; исполнено {cnt["filled"]} (закрыто {len(closed)}, открыто {len(filled) - len(closed)}); не исполнено {cnt["not_filled"]}; ожидают входа {cnt["pending"]}; нет данных цен {cnt["no_data"]}')
    A = [r['A'] for r in closed if r.get('A')]
    if A:
        liq = sum(1 for a in A if a['liq'])
        rets = [a['ret'] for a in A]
        L.append(f'  модель A (как в канале): с плечом {len(A)}; ликвидаций {liq} ({liq / len(A) * 100:.0f}%); итог на маржу: сумма {sum(rets) * 100:+.0f}%, среднее {np.mean(rets) * 100:+.1f}%, WR {np.mean([x > 0 for x in rets]) * 100:.0f}%')
    else:
        L.append('  модель A: нет закрытых сигналов с плечом')
    st = r_stats([r['B']['R'] for r in closed if math.isfinite(r['B']['R'])])
    td, nd = daily_t([(r['exit_ts'], r['B']['R']) for r in closed if math.isfinite(r['B']['R'])])
    if st['n']:
        L.append(f'  модель B (1% риска): n {st["n"]}, WR {st["wr"] * 100:.0f}%, mean R net {fm(st["mean"], 3)}, PF net {fm(st["pf"])}, t по сигналам {fm(st["t"])}, t по дням {fm(td)} (дней {nd}), '
                 f'макс. серия убытков {st["streak"]}, макс. просадка при 1% риска {max_dd([(r["exit_ts"], r["B"]["R"]) for r in closed]) * 100:.1f}%')
        for name, cond in (('лонг', lambda r: r['side'] == 'long'), ('шорт', lambda r: r['side'] == 'short')):
            sub = [r['B']['R'] for r in closed if cond(r) and math.isfinite(r['B']['R'])]
            if sub:
                s2 = r_stats(sub)
                L.append(f'    {name}: n {s2["n"]}, WR {s2["wr"] * 100:.0f}%, mean R {fm(s2["mean"], 3)}, PF {fm(s2["pf"])}')
        by = {}
        for r in closed:
            by.setdefault(r['symbol'], []).append(r['B']['R'])
        top = sorted(by.items(), key=lambda kv: -len(kv[1]))[:8]
        L.append('    по символам (топ-8 по числу): ' + '; '.join(f'{k} n={len(v)} mean R {np.mean(v):+.2f}' for k, v in top))
        ex = expectancy(closed)
        if ex:
            L.append(f'    расшифровка ожидания: {ex["text"]}')
            L.append(f'      средний выигрыш {ex["w_avg"]:+.3f} R, средний проигрыш −{ex["l_avg"]:.3f} R; закрыто в безубыток после первой цели: {ex["n_be"]} ({ex["be_share"] * 100:.0f}%); '
                     f'полные стопы (без единой цели): {ex["n_stop"]} шт., ΣR {ex["sum_stop"]:+.2f} (среднее {fm(ex["mean_stop"], 2)}), остальные сделки ΣR {ex["sum_rest"]:+.2f}, итого ΣR {ex["sum_all"]:+.2f}; по таймауту: {ex["n_timeout"]}')
        g = month_groups(closed, months_ret)
        for nm, lab in (('up', 'месяцы роста BTC'), ('down', 'месяцы падения BTC')):
            sub = [r['B']['R'] for r in closed if month_key(r['pub_ms']) in {m for m, x in months_ret.items() if (x > 0) == (nm == 'up') and x != 0} and math.isfinite(r['B']['R'])]
            pf = r_stats(sub)['pf'] if sub else None
            L.append(f'    {lab}: месяцев {g[nm]["n_months"]}, сигналов {g[nm]["n"]}, mean R {fm(g[nm]["mean"], 3) if g[nm]["mean"] is not None else "н/п"}, PF {fm(pf)}')
    else:
        L.append('  модель B: нет закрытых исполненных сигналов')
    miss = sum(1 for r in filled if r.get('fund_missing'))
    alt = sum(1 for r in rows if r.get('note', '').startswith('Binance'))
    if filled:
        L.append(f'  фандинг: нет данных у {miss} из {len(filled)} исполненных (учтён только там, где получен); цены с Binance (не BingX): {alt}')
    return L


async def run_audit(params, progress=None, source=None):
    path = params.get('adb') or AD.db_path()
    if not os.path.exists(path):
        return '⛔ /audit: база аудита пуста (нет ни импорта, ни форварда)'
    con = AD.connect(path)
    fees = {'taker': float(params.get('fee_taker', 0.0005)), 'maker': float(params.get('fee_maker', 0.0005))}
    now_ms = int(params.get('now_ms') or time.time() * 1000)
    sigs = load_signals(con)
    cnts = AD.counts(con)
    reports, reply_map = load_reports(con)
    head = [f'📋 <b>/audit — аудит внешнего источника сигналов (BOT_SPEC §2.44)</b> | код {params.get("version", "")}',
            f'комиссии: FEE_TAKER {fees["taker"] * 100:.4f}%, FEE_MAKER {fees["maker"] * 100:.4f}%, проскальзывание {SLIP * 100:.2f}% на сторону']
    unp = sum(sum(v.values()) for k, v in cnts['other'].items() if k == 'unparsed')
    rep = sum(sum(v.values()) for k, v in cnts['other'].items() if k == 'report')
    head.append(f'Сообщений-сигналов: {sum(cnts["signals"].values())} (live {cnts["signals"].get("live", 0)}, догонка {cnts["signals"].get("catchup", 0)}, импорт {cnts["signals"].get("import", 0)}); '
                f'нераспознано {unp}; отчётов о целях (по текущему парсеру) {len(reports)} (при записи {rep}); событий правок {cnts["events"].get("edit", 0)}, удалений {cnts["events"].get("delete", 0)}')
    if not sigs:
        return '\n'.join(head + ['Сигналов нет — нечего считать.'])
    own = source is None
    if own:
        import aiohttp
        ses = aiohttp.ClientSession()
        source = PriceSource(params.get('cache_dir') or os.path.join(os.path.dirname(path), 'audit_cache'), ses)
        await source.open()
    try:
        rows = await run_simulation(sigs, source, fees, now_ms, progress)
        first_ms = min(r['pub_ms'] for r in rows)
        btc = await source.btc_daily(first_ms - 40 * DAY, now_ms)
        months_ret = btc_months_from_daily(btc[0], btc[1], now_ms) if btc and len(btc[0]) > 5 else {}
    finally:
        if own:
            await source.close()
            await ses.close()
    L = list(head)
    live = [r for r in rows if r['source'] == 'live']
    L += ['', *section('ФОРВАРД (live) — основа решения', live, months_ret)]
    ret = [r for r in rows if r['source'] != 'live']
    L += ['', *section('РЕТРОСПЕКТИВА (импорт и догонка) — только справочно, удалённые сообщения неизвестны', ret, months_ret)]
    v = verdict(live, months_ret)
    c = v['crit']
    mark = lambda x: 'н/п' if x is None else ('да' if x else 'НЕТ')
    L += ['', '<b>Критерии §2.44.2 п. 5 (только форвард, модель B)</b>',
          f'  n исполненных закрытых ≥ {MIN_N}: {v["stats"]["n"]} [{mark(c["n"])}]',
          f'  mean R net > 0 при t(по дням) ≥ {MIN_T}: mean {fm(v["stats"]["mean"], 3)}, t {fm(v["t_day"])} [{mark(c["mean_t"])}]',
          f'  PF net ≥ {MIN_PF}: {fm(v["stats"]["pf"])} [{mark(c["pf"])}]',
          f'  mean ≥ 0 в месяцы роста BTC: {fm(v["groups"]["up"]["mean"], 3) if v["groups"]["up"]["mean"] is not None else "н/п"} [{mark(c["up"])}]; в месяцы падения: '
          f'{fm(v["groups"]["down"]["mean"], 3) if v["groups"]["down"]["mean"] is not None else "н/п"} [{mark(c["down"])}]']
    if v['stats']['n'] < MIN_N:
        L.append(f'  <b>Вердикт: рано — набрано {v["stats"]["n"]} из {MIN_N}</b> (решение невозможно до n ≥ {MIN_N}).')
    else:
        L.append('  <b>Вердикт: ' + ('ПРОЙДЕНО → отдельный пре-коммит автоисполнения (не в этом деплое)' if v['pass'] else 'НЕ ПРОЙДЕНО → канал отклонён') + '</b>')
    links = link_reports(reports, sigs, reply_map)
    results = {r['key']: r for r in rows if r.get('key')}
    rrows, sm = report_check(links, results)
    if links:
        pct = lambda a, b: f'{a} из {b} ({a / b * 100:.0f}%)' if b else 'н/д'
        L += ['', f'<b>Сверка отчётов о целях с расчётом</b> (§2.44.8 п. 2): отчётов {sm["n_reports"]}; привязано {sm["n_linked"]} (по reply_to {sm["n_reply"]}, по символу ≤ 14 суток {sm["n_symbol"]}); '
                  f'без привязки {sm["n_reports"] - sm["n_linked"]}; привязано, но без номера цели/расчёта {sm["n_no_k"]}',
              f'  (1) цель реально достигнута по ценам до ликвидации/стопа: {pct(sm["reached"], sm["n_eval"])}',
              f'  (2) заявленный % совпадает с расчётным модели A до издержек в пределах ±10% от расчётного: {pct(sm["match"], sm["n_match"])}; медиана |заявлено − рассчитано| {fm(sm["abs_med"], 1)} п. п.',
              f'  (3) отчёт опубликован по сигналу, уже ликвидированному в модели A: {pct(sm["after_liq"], sm["n_eval"])}']
        shown = [x for x in rrows if x['computed'] is not None][:12]
        for x in shown:
            L.append(f'    {x["symbol"]} цель {x["k"]}: заявлено {x["claimed"]:+.0f}%, рассчитано {x["computed"]:+.0f}%' + (' (без плеча)' if not x['lev'] else '') +
                     f'; достигнута: {"да" if x["reached"] else ("нет" if x["reached"] is not None else "н/д")}' + ('; сигнал уже ликвидирован' if x['after_liq'] else ''))
    return '\n'.join(L)
