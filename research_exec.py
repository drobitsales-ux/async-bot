"""research_exec.py — проверка исполнения БЕЗ ОРДЕРОВ (BOT_SPEC §2.47.4 шаг 1.3 для H2 на BingX и §2.48.3 шаг 2.5 для X1): раз в час ≤ 7 суток публичные запросы — стакан, ставки фандинга, рынки;
оценка издержек входа и сравнение с допущениями моделей. Только публичные методы чтения; методов создания/отмены ордеров здесь нет (тест на исходниках)."""
import asyncio
import json
import os
import sqlite3
import time

import numpy as np

import research_xdata as X

DAY_S = 86_400
INTERVAL_S = 3600
MAX_DAYS = 7
SIZES_H2 = (20, 100, 500)                 # номинал позиции H2 = 1% капитала при капитале 2000 / 10 000 / 50 000 USDT
SIZES_X1 = (100, 500, 1000)               # USDT на ногу (§2.48.3)
MODEL_SLIP = 0.0005                       # допущение моделей: проскальзывание на исполнение
MODEL_FEE_DEFAULT = 0.0005
H2_MAX_AGE_DAYS = 7
X1_CAND_LIMIT = 10
X1_SCAN = 40
H2_COST_OK_BPS = 5.0                      # уточнение Claude: «допущения подтверждены», если медиана односторонних издержек (отклонение от mid по стакану на 100 USDT) ≤ 5 б.п. и шорт доступен
H2_AVAIL_OK = 0.90


def db_path():
    return os.getenv('EXEC_DB') or ('/data/exec_check.db' if os.path.exists('/data') else '/tmp/exec_check.db')


def connect(path=None):
    con = sqlite3.connect(path or db_path())
    con.execute('CREATE TABLE IF NOT EXISTS samples (id INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT, ts REAL, symbol TEXT, payload TEXT)')
    con.execute('CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT)')
    return con


def meta_get(con, k, default=None):
    r = con.execute('SELECT v FROM meta WHERE k=?', (k,)).fetchone()
    return json.loads(r[0]) if r else default


def meta_set(con, k, v):
    con.execute('INSERT OR REPLACE INTO meta (k, v) VALUES (?, ?)', (k, json.dumps(v)))
    con.commit()


# ── стакан ──────────────────────────────────────────────────────────────────────────────────────────
def book_stats(ob, sizes, side):
    """Издержки исполнения по стакану: spread (б.п.), глубина ±0.5% (USDT) и проскальзывание относительно mid для каждого размера (USDT номинала): side 'sell' (шорт — по бидам) / 'buy' (по аскам).
    → dict(mid, spread_bps, depth, slip{size: б.п. | None если стакана не хватает}). Только арифметика над стаканом."""
    bids, asks = ob.get('bids') or [], ob.get('asks') or []
    if not bids or not asks:
        return None
    bb, ba = float(bids[0][0]), float(asks[0][0])
    mid = (bb + ba) / 2.0
    depth = sum(float(p) * float(q) for p, q in bids if float(p) >= mid * 0.995) + sum(float(p) * float(q) for p, q in asks if float(p) <= mid * 1.005)
    levels = bids if side == 'sell' else asks
    slip = {}
    for sz in sizes:
        need, filled, qty = float(sz), 0.0, 0.0
        for p, q in levels:
            p, q = float(p), float(q)
            take = min(need - filled, p * q)
            qty += take / p
            filled += take
            if filled >= need - 1e-9:
                break
        if filled < need - 1e-9:
            slip[sz] = None                                                      # стакана (в запрошенной глубине) не хватает
            continue
        avg = need / qty
        slip[sz] = ((mid - avg) / mid if side == 'sell' else (avg - mid) / mid) * 1e4          # отклонение средней цены от mid (включает полуспред)
    return {'mid': mid, 'spread_bps': (ba - bb) / mid * 1e4, 'depth': depth, 'slip': slip}


class ExecSampler:
    """Почасовой сборщик. Клиенты ccxt (публичные) передаются снаружи (в тестах — фейки). Все вызовы — только чтение."""

    def __init__(self, kind, bingx, bybit=None, db=None, fee_bingx=MODEL_FEE_DEFAULT, sleep=asyncio.sleep, now=time.time, pause=1.0):
        self.kind, self.bingx, self.bybit, self.fee_bingx = kind, bingx, bybit, fee_bingx
        self.con = connect(db)
        self.sleep, self.now, self.pause = sleep, now, pause
        self.stop = False

    async def _call(self, ex, name, *a, **kw):
        last = None
        for delay in (0, 2, 6):
            if delay:
                await self.sleep(delay)
            try:
                r = await getattr(ex, name)(*a, **kw)
                await self.sleep(self.pause)
                return r
            except Exception as e:
                last = e
        raise last

    def _put(self, symbol, payload):
        self.con.execute('INSERT INTO samples (kind, ts, symbol, payload) VALUES (?,?,?,?)', (self.kind, self.now(), symbol, json.dumps(payload)))
        self.con.commit()

    # H2: новые листинги BingX
    async def sample_h2(self):
        mk = await self._call(self.bingx, 'load_markets')
        swaps = X.usdt_swaps(mk)
        known = meta_get(self.con, 'h2_known')
        now = self.now()
        first_seen = meta_get(self.con, 'h2_first_seen', {})
        if known is None:                                               # первый срез — база: новыми считаются только появившиеся ПОСЛЕ него (и рынки с датой created за последнюю неделю)
            meta_set(self.con, 'h2_known', sorted(swaps))
            known = sorted(swaps)
        new = []
        for sym, m in swaps.items():
            created = m.get('created')
            age = (now - created / 1000) / DAY_S if created else None
            if sym not in known and sym not in first_seen:
                first_seen[sym] = now
            seen_age = (now - first_seen[sym]) / DAY_S if sym in first_seen else None
            a = age if age is not None else seen_age
            if a is not None and a <= H2_MAX_AGE_DAYS:
                new.append((sym, a, m))
        meta_set(self.con, 'h2_first_seen', first_seen)
        n = 0
        for sym, age, m in new:
            try:
                ob = await self._call(self.bingx, 'fetch_order_book', sym, 50)
                st = book_stats(ob, SIZES_H2, 'sell')
                fr = None
                try:
                    r = await self._call(self.bingx, 'fetch_funding_rate', sym)
                    fr = float(r.get('fundingRate')) if r and r.get('fundingRate') is not None else None
                except Exception:
                    pass
            except Exception as e:
                self._put(sym, {'age_days': age, 'active': m['active'], 'error': type(e).__name__})
                continue
            self._put(sym, {'age_days': age, 'active': m['active'], 'book': st, 'funding': fr})
            n += 1
        return n

    # X1: текущие кандидаты
    async def sample_x1(self, scan_symbols):
        """scan_symbols — [(метка, символ_bingx, символ_bybit)] (из кэша /xfund_test load). Текущие ставки обеих бирж → d (за 8 ч), лучшие кандидаты → стаканы и издержки входа."""
        rows = []
        for lab, sa, sb in scan_symbols[:X1_SCAN]:
            try:
                ra = await self._call(self.bingx, 'fetch_funding_rate', sa)
                rb = await self._call(self.bybit, 'fetch_funding_rate', sb)
            except Exception:
                continue
            fa, fb = ra.get('fundingRate'), rb.get('fundingRate')
            if fa is None or fb is None:
                continue

            def to8(r, x):
                iv = r.get('interval')
                h = {'1h': 1.0, '2h': 2.0, '4h': 4.0, '8h': 8.0}.get(iv, 8.0) if isinstance(iv, str) else 8.0
                return float(x) * 8.0 / h, (h if isinstance(iv, str) else None)
            a8, ia = to8(ra, fa)
            b8, ib = to8(rb, fb)
            rows.append((abs(a8 - b8), lab, sa, sb, a8, b8, ia, ib))
        rows.sort(reverse=True)
        n = 0
        for dv, lab, sa, sb, a8, b8, ia, ib in rows[:X1_CAND_LIMIT]:
            short_a = a8 >= b8
            try:
                oa = await self._call(self.bingx, 'fetch_order_book', sa, 50)
                ob = await self._call(self.bybit, 'fetch_order_book', sb, 50)
            except Exception as e:
                self._put(lab, {'d8': dv, 'error': type(e).__name__})
                continue
            sta = book_stats(oa, SIZES_X1, 'sell' if short_a else 'buy')
            stb = book_stats(ob, SIZES_X1, 'buy' if short_a else 'sell')
            self._put(lab, {'d8': dv, 'rate8': {'bingx': a8, 'bybit': b8}, 'interval_h': {'bingx': ia, 'bybit': ib}, 'short': 'bingx' if short_a else 'bybit', 'bingx': sta, 'bybit': stb})
            n += 1
        return n

    async def run(self, scan_symbols=None, max_days=MAX_DAYS, interval_s=INTERVAL_S):
        con = self.con
        t0 = meta_get(con, f'{self.kind}_started') or self.now()
        meta_set(con, f'{self.kind}_started', t0)
        meta_set(con, f'{self.kind}_active', True)
        while not self.stop and self.now() - t0 < max_days * DAY_S:
            try:
                if self.kind == 'h2':
                    await self.sample_h2()
                else:
                    await self.sample_x1(scan_symbols or [])
            except Exception:
                pass
            await self.sleep(interval_s)
        meta_set(con, f'{self.kind}_active', False)


# ── отчёт ───────────────────────────────────────────────────────────────────────────────────────────
def report(kind, db=None, fee_bingx=MODEL_FEE_DEFAULT):
    con = connect(db)
    rows = [(r[0], r[1], json.loads(r[2])) for r in con.execute('SELECT ts, symbol, payload FROM samples WHERE kind=? ORDER BY id', (kind,))]
    t0 = meta_get(con, f'{kind}_started')
    act = meta_get(con, f'{kind}_active')
    head = [f'🧪 <b>/exec_check {kind} — проверка исполнения без ордеров</b> (BOT_SPEC §2.{"47.4" if kind == "h2" else "48.3"}); сэмплов {len(rows)}; ' + (f'идёт с {time.strftime("%Y-%m-%d %H:%M", time.gmtime(t0))} UTC, ' if t0 else '') + ('активна' if act else 'остановлена')]
    if not rows:
        return '\n'.join(head + ['Данных пока нет.'])
    if kind == 'h2':
        ok = [(s, p) for _t, s, p in rows if p.get('book')]
        syms = {s for _t, s, p in rows}
        spreads = [p['book']['spread_bps'] for _s, p in ok]
        cost100 = [p['book']['slip'].get(100) for _s, p in ok if p['book']['slip'].get(100) is not None]
        miss = sum(1 for _s, p in ok for v in p['book']['slip'].values() if v is None)
        old = [p for _t, _s, p in rows if (p.get('age_days') or 0) >= 3]
        avail = (sum(1 for p in old if p.get('active')) / len(old)) if old else None
        fr = [p['funding'] for _t, _s, p in rows if p.get('funding') is not None]
        L = head + [f'Листингов в наблюдении: {len(syms)}; сэмплов со стаканом {len(ok)}',
                    f'Спред: медиана {np.median(spreads):.1f} б.п. (p90 {np.percentile(spreads, 90):.1f})' if spreads else 'Спред: н/д',
                    f'Издержки одной стороны (отклонение средней цены исполнения от mid по стакану на 100 USDT, включает полуспред): медиана {np.median(cost100):.1f} б.п. против допущения модели {MODEL_SLIP * 1e4:.0f} б.п. + комиссия {fee_bingx * 1e4:.0f} б.п.' if cost100 else 'Издержки: н/д',
                    'Проскальзывание по размерам (медиана, б.п.): ' + '; '.join(f'{sz} USDT: {np.median([p["book"]["slip"][sz] for _s, p in ok if p["book"]["slip"].get(sz) is not None]):.1f}' for sz in SIZES_H2 if any(p['book']['slip'].get(sz) is not None for _s, p in ok)) if ok else '',
                    f'Стакана не хватило на размер: {miss} раз(а); шорт на день ≥ 3 после листинга доступен (рынок активен): ' + (f'{avail * 100:.0f}% сэмплов (n={len(old)})' if avail is not None else 'н/д (листингов старше 3 суток в наблюдении ещё нет)'),
                    'Текущий фандинг новых листингов: ' + (f'медиана {np.median(fr) * 1e4:+.2f} б.п. за период (n={len(fr)})' if fr else 'н/д')]
        good = bool(cost100) and np.median(cost100) <= H2_COST_OK_BPS and (avail is None or avail >= H2_AVAIL_OK)
        L.append('<b>Итог (уточнение Claude §2.47.4: медиана односторонних издержек ≤ 5 б.п. и шорт доступен ≥ 90%): ' + ('допущения подтверждены' if good else 'допущения НЕ подтверждены (см. цифры выше)') + '</b>'
                 + ('' if avail is not None else ' — оценка предварительная: листингов старше 3 суток пока нет'))
        return '\n'.join(L)
    # x1
    c = [(s, p) for _t, s, p in rows if p.get('bingx') and p.get('bybit')]
    L = head + [f'Кандидатов (сэмплов со стаканами): {len(c)}; разных символов {len({s for s, _p in c})}']
    if not c:
        return '\n'.join(L + ['Стаканов пока нет.'])
    L.append(f'Текущая разница ставок d (за 8 ч): медиана {np.median([p["d8"] for _s, p in c]) * 100:.4f}%, максимум {max(p["d8"] for _s, p in c) * 100:.4f}%')
    for sz in SIZES_X1:
        costs = []
        for _s, p in c:
            sa, sb = p['bingx']['slip'].get(sz), p['bybit']['slip'].get(sz)
            if sa is None or sb is None:
                continue
            costs.append(sa + sb + (fee_bingx + MODEL_FEE_DEFAULT) * 1e4)             # вход, 2 исполнения: отклонения от mid обеих ног (включают полуспред) + комиссии, б.п. номинала
        n_all = len(c)
        if costs:
            model = (fee_bingx + MODEL_SLIP + MODEL_FEE_DEFAULT + MODEL_SLIP) * 1e4
            L.append(f'  вход {sz} USDT на ногу: расчётные издержки входа (2 исполнения) медиана {np.median(costs):.1f} б.п. номинала (p90 {np.percentile(costs, 90):.1f}) против модели {model:.1f}; стакана хватило в {len(costs)} из {n_all}')
        else:
            L.append(f'  вход {sz} USDT на ногу: стакана не хватает ни в одном сэмпле')
    over = [p for _s, p in c if (p['bingx']['slip'].get(500) or 0) + (p['bybit']['slip'].get(500) or 0) > 4 * MODEL_SLIP * 1e4]
    L.append(f'<b>Итог:</b> сэмплов, где проскальзывание на 500 USDT превышает допущение модели вдвое и более: {len(over)} из {len(c)} — ' + ('допущения подтверждены' if len(over) <= 0.1 * len(c) else 'допущения НЕ подтверждены (стакан тоньше модели)'))
    return '\n'.join(L)
