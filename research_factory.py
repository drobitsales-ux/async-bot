"""research_factory.py — этап 2 «фабрики» простых систематических стратегий (BOT_SPEC §2.41 + поправка §2.41.17): загрузка Dukascopy (/factory_load), заморозка пар P-PAIR
(/factory_pairs), калибровка на случайных стратегиях и шаги A/B (/factory), шаг C на LOCKBOX (/factory_final, один раз). Расчёты — в дочернем процессе (research_runner).
Состояние (результаты ячеек, флаги, спреды, пары) — SQLite {FACTORY_DIR}/factory.db (таблица kv); прерванный /factory продолжается с первой несчитанной ячейки.
Ничего не торгует."""
import asyncio
import json
import math
import os
import sqlite3
import time
from datetime import datetime, timezone

import numpy as np

import research_fdata as FD
import research_fengine as E
import research_ma as M
import research_ma_lib as L
import research_vision as V

REV = 'v103'
SEED = 20261025
CALIB_N = L.CALIB_N
CALIB_MAX = int(L.CALIB_MAX_FRAC * L.CALIB_N)              # > 10 из 200 — СТОП
PAIR_N = 10
PAIR_LS = (20, 60)
PAIR_TFS = ('1h', '4h', '1D')
LAG_HS = (1, 3)
LAG_TFS = ('1h', '4h')
MAX_FINALISTS = L.FINAL_MAX
DURS_KEEP = 4000                                            # образец длительностей сделок для калибровки (на ячейку)
BTC = 'BTCUSDT'


def db_path(d=None):
    return os.path.join(d or FD.factory_dir(), 'factory.db')


def _con(d=None):
    p = db_path(d)
    os.makedirs(os.path.dirname(p), exist_ok=True)
    con = sqlite3.connect(p)
    con.execute('CREATE TABLE IF NOT EXISTS kv (k TEXT PRIMARY KEY, v TEXT)')
    return con


def kv_get(k, d=None, default=None):
    con = _con(d)
    try:
        r = con.execute('SELECT v FROM kv WHERE k=?', (k,)).fetchone()
    finally:
        con.close()
    return json.loads(r[0]) if r else default


def kv_set(k, v, d=None):
    con = _con(d)
    try:
        con.execute('INSERT OR REPLACE INTO kv (k, v) VALUES (?, ?)', (k, json.dumps(v, ensure_ascii=False)))
        con.commit()
    finally:
        con.close()


def kv_set_if_absent(k, v, d=None) -> bool:
    """Атомарно: записывает значение, только если ключа нет. True — записано (флаг «взят»)."""
    con = _con(d)
    try:
        cur = con.execute('INSERT OR IGNORE INTO kv (k, v) VALUES (?, ?)', (k, json.dumps(v, ensure_ascii=False)))
        con.commit()
        return cur.rowcount == 1
    finally:
        con.close()


# ── тесты и N ────────────────────────────────────────────────────────────────────────────────────────
def cell_classes(n_by_class):
    return [(i, c, tf) for i, (c, tf) in enumerate(FD.CELLS)]


def enumerate_tests(n_by_class):
    """Все тесты этапа 2: [(индекс ячейки, класс, ТФ, id конфигурации, семейство)]. n_by_class — число доступных инструментов по классам (X-MOM при ≥ 6)."""
    out = []
    for i, (cls, tf) in enumerate(FD.CELLS):
        for cfg in L.configs_for(n_by_class[cls]):
            out.append((i, cls, tf, cfg.id, cfg.family))
        if cls == 'CRYPTO':
            if tf in PAIR_TFS:
                for lb in PAIR_LS:
                    out.append((i, cls, tf, f'P-PAIR-L{lb}', 'P-PAIR'))
            if tf in LAG_TFS:
                for h in LAG_HS:
                    out.append((i, cls, tf, f'L-LAG-H{h}', 'L-LAG'))
    return out


def count_n(n_by_class):
    return len(enumerate_tests(n_by_class))


def tid(cls, tf, cid):
    return f'{cls}|{tf}|{cid}'


# ── ряды ячеек ───────────────────────────────────────────────────────────────────────────────────────
def crypto_series(vdir, syms, tf, mode='pre', session=None):
    import research_edge
    out = []
    step = FD.step_ms_of(tf)
    for sym in syms:
        k15 = FD.load_crypto_15m(vdir, sym, mode, session)
        if k15 is None:
            continue
        bars = FD.build_crypto(k15, tf)
        del k15
        if bars is None or len(bars['ts']) < 300:
            continue
        f = research_edge.load_funding(vdir, sym)
        fund = FD.funding_per_bar(bars['ts'], f['ts'], f['rate'], step) if f is not None and len(f['ts']) else None
        out.append(E.make_series(sym, bars, FD_trade_from(), fund))
    return out


def FD_trade_from():
    return L.period_bounds_ms('TRAIN')[0]


def noncrypto_series(d, keys, tf, mode='pre', session=None):
    out = []
    for key in keys:
        raw = FD.load_raw(d, key, mode, session)
        if raw is None:
            continue
        bars = FD.build_noncrypto(raw, tf)
        del raw
        if bars is None or len(bars['ts']) < 100:
            continue
        out.append(E.make_series(key, bars, FD_trade_from()))
    return out


def cell_series(cls, tf, ctx, mode='pre', session=None):
    if cls == 'CRYPTO':
        return crypto_series(ctx['vdir'], ctx['syms'], tf, mode, session)
    keys = [k for k in FD.CLASS_MEMBERS[cls] if k in ctx['avail']]
    return noncrypto_series(ctx['fdir'], keys, tf, mode, session)


def cost_of(S, cls, spreads, mult=1.0):
    if cls == 'CRYPTO':
        return E.cost_crypto(mult)
    return E.cost_noncrypto(S['key'], spreads, mult)


# ── прогон одной конфигурации по ячейке ──────────────────────────────────────────────────────────────
def run_config(spec, series, tf, cls, spreads, pairs, mult=1.0):
    """spec = ('lib', L.Config) | ('pair', Lb) | ('lag', H). → dict(days, r, trades=[(ключ, e, last)…], entries, bars) — портфель ячейки (простая сумма вкладов)."""
    kind = spec[0]
    parts, trades = [], []
    by_key = {S['key']: S for S in series}
    t0 = FD_trade_from()
    if kind == 'lib':
        cfg = spec[1]
        if cfg.cross_sectional:
            sigs = E.xmom_signals(series, tf, dict(cfg.params)['lookback'], dict(cfg.params)['rebalance'])
        elif len(spec) > 2 and spec[2] == 'oco':                                                  # диагностика B-VC (не входит в N)
            sigs = {S['key']: E.sig_b_vc(S, dict(cfg.params)['nr'], dict(cfg.params)['hold'], oco=True) for S in series}
        else:
            sigs = {S['key']: E.library_signals(cfg, S) for S in series}
        for S in series:
            g = sigs[S['key']]
            r = E.simulate(S, g['sig'], cost_of(S, cls, spreads, mult), exit_l=g.get('exit_l'), exit_s=g.get('exit_s'), reverse=g.get('reverse', False),
                           hold=g.get('hold'), entry_px=g.get('entry_px'))
            parts.append(r)
            trades += [(S['key'], tr[0], tr[1], S['ts'][tr[0]]) for tr in r['trades']]
    elif kind == 'pair':
        for a, b in pairs:
            if a in by_key and b in by_key:
                r = E.simulate_pair(by_key[a], by_key[b], spec[1], cost_of(by_key[a], cls, spreads, mult), cost_of(by_key[b], cls, spreads, mult), t0)
                parts.append(r)
                ts = by_key[a]['ts']
                ia = np.searchsorted(by_key[b]['ts'], ts)
                ok = by_key[b]['ts'][np.minimum(ia, by_key[b]['n'] - 1)] == ts
                tsa = ts[np.flatnonzero(ok)]
                trades += [(f'{a}/{b}', tr[0], tr[1], tsa[tr[0]]) for tr in r['trades']]
    elif kind == 'lag':
        btc = by_key.get(BTC)
        if btc is not None:
            for S in series:
                if S['key'] == BTC:
                    continue
                g = E.sig_l_lag(S, btc, spec[1])
                r = E.simulate(S, g['sig'], cost_of(S, cls, spreads, mult), hold=g['hold'], reverse=False)
                parts.append(r)
                trades += [(S['key'], tr[0], tr[1], S['ts'][tr[0]]) for tr in r['trades']]
    for S in series:
        S['_c'].clear()                                    # кэш индикаторов — только на время одной конфигурации (память: лимит RESEARCH_MEM_MB)
    days, r = E.portfolio_daily(parts)
    return {'days': days, 'r': r, 'trades': trades}


def period_stats(days, r):
    """TRAIN / VALID / TRAIN+VALID: среднее, t Ньюи–Уэста, число дней."""
    out = {}
    for name in ('TRAIN', 'VALID'):
        d, x = E.period_slice(days, r, name)
        m, t, n = E.nw_t(x)
        out[name] = {'mean': m, 't': (None if not math.isfinite(t) else t), 'n': n}
    lb = L.lockbox_start_ms() // E.DAY_MS
    tr_lo = E.day_bounds('TRAIN')[0]
    m = (days >= tr_lo) & (days < lb)
    mm, tt, nn = E.nw_t(r[m])
    out['TV'] = {'mean': mm, 't': (None if not math.isfinite(tt) else tt), 'n': nn}
    return out


def trade_stats(trades, series, tf):
    """Статистика сделок TRAIN для калибровки: число входов, длительности (в барах)."""
    lo = L.period_bounds_ms('TRAIN')[0]
    hi = L.period_bounds_ms('VALID')[0]
    ent = [(e, last) for _k, e, last, ts_e in trades if lo <= ts_e < hi]
    return len(ent), [last - e + 1 for e, last in ent]


def eligible_bars(series):
    lo = L.period_bounds_ms('TRAIN')[0]
    hi = L.period_bounds_ms('VALID')[0]
    return int(sum(np.sum((S['ts'] >= lo) & (S['ts'] < hi)) for S in series))


# ── калибровка (§2.41.9) ─────────────────────────────────────────────────────────────────────────────
def calibrate_cell(cell_idx, cls, tf, series, spreads, p_entry, durs, n_rand=CALIB_N, progress=None):
    """200 случайных стратегий на ячейку: вход с вероятностью p_entry на бар, направление 50/50, длительность — из эмпирического распределения реальных конфигураций, стоп 3·ATR20,
    тот же портфельный расчёт и издержки; только TRAIN. → {'n', 'n_pass', 'frac', 'fail'}."""
    end = L.period_bounds_ms('VALID')[0]
    cut = [E.cut_series(S, int(np.searchsorted(S['ts'], end, side='left'))) for S in series]
    durs = np.asarray(durs if len(durs) else [1], dtype=np.int64)
    ts_list, p_list = [], []
    for k in range(n_rand):
        rng = np.random.default_rng([SEED, cell_idx, k])
        parts = []
        for S in cut:
            n = S['n']
            sig = np.zeros(n, np.int8)
            hold = np.zeros(n, np.int32)
            m = rng.random(n) < p_entry
            m[:S['trade_from']] = False
            idx = np.flatnonzero(m)
            if len(idx):
                sig[idx] = rng.integers(0, 2, len(idx)) * 2 - 1
                hold[idx] = rng.choice(durs, size=len(idx))
            parts.append(E.simulate(S, sig, cost_of(S, cls, spreads), hold=hold, reverse=False))
        days, r = E.portfolio_daily(parts)
        d, x = E.period_slice(days, r, 'TRAIN')
        _m, t, _n = E.nw_t(x)
        t_ = t if math.isfinite(t) else 0.0
        ts_list.append(t_)
        p_list.append(E.p_one_sided(t))
    rej = E.bh_reject(p_list)
    passed = int(np.sum(rej & (np.asarray(ts_list) > 0)))
    return {'n': n_rand, 'n_pass': passed, 'frac': passed / n_rand, 'fail': passed > int(L.CALIB_MAX_FRAC * n_rand)}


# ── выбор пар P-PAIR ─────────────────────────────────────────────────────────────────────────────────
def select_pairs(vdir, syms, top=PAIR_N, min_days=365):
    """10 пар с максимальной корреляцией дневных лог-доходностей на TRAIN (1D, ≥ min_days общих дней, ничья — по имени, ориентация A < B). → список [A, B, corr, n]."""
    lo, hi = E.day_bounds('TRAIN')
    rets = {}
    for sym in sorted(syms):
        k15 = FD.load_crypto_15m(vdir, sym, 'pre')
        if k15 is None:
            continue
        bars = FD.build_crypto(k15, '1D')
        del k15
        if bars is None:
            continue
        day, c = bars['day'], bars['c']
        m = (day >= lo) & (day < hi)
        day, c = day[m], c[m]
        if len(c) < min_days + 1:
            continue
        # лог-доходность дня d = ln(c[d]/c[d−1]) только при соседних днях (без пропусков)
        ok = np.diff(day) == 1
        rets[sym] = (day[1:][ok], np.diff(np.log(c))[ok])
    names = sorted(rets)
    cand = []
    for i in range(len(names)):
        for j in range(i + 1, len(names)):
            da, ra = rets[names[i]]
            db, rb = rets[names[j]]
            common, ia, ib = np.intersect1d(da, db, return_indices=True)
            if len(common) < min_days:
                continue
            x, y = ra[ia], rb[ib]
            if x.std() == 0 or y.std() == 0:
                continue
            cand.append((float(np.corrcoef(x, y)[0, 1]), names[i], names[j], int(len(common))))
    cand.sort(key=lambda t: (-round(t[0], 12), t[1], t[2]))
    return [[a, b, round(c, 6), n] for c, a, b, n in cand[:top]]


# ── спред XAGUSD и загрузка ──────────────────────────────────────────────────────────────────────────
async def measure_spread(fetch, key, now_ms, progress=None):
    """Медиана спреда активных часов (б.п.) тем же способом, что в пробе v2: BID/ASK часовых свечей за 2021-06, 2023-02 и последний полный месяц. → dict | None."""
    import research_ma2 as M2
    cls, folder, (lo, hi), _first = FD.INSTRUMENTS[key]
    fx = M2.Fetcher(fetch, FD.PAUSE_S)
    ly, lm1 = FD.last_complete(now_ms)
    parts, info = [], {}
    for (y, m0) in ((2021, 5), (2023, 1), (ly, lm1 - 1)):
        b = await fx.month(folder, 'BID', y, m0)
        a = await fx.month(folder, 'ASK', y, m0)
        if b['p'] is None or a['p'] is None:
            info[f'{y}-{m0 + 1:02d}'] = f'BID: {b["why"] or "ok"}; ASK: {a["why"] or "ok"}'
            continue
        sc = M.pick_scale(float(np.median(b['p']['c'])), lo, hi)
        if not sc:
            info[f'{y}-{m0 + 1:02d}'] = 'масштаб не определён'
            continue
        ts, spr, fl = M2.spread_bps_series(M2.scale_prices(b['p'], sc), M2.scale_prices(a['p'], sc))
        parts.append((ts, spr, fl))
        info[f'{y}-{m0 + 1:02d}'] = M2.spread_stats(ts, spr, fl)['active']['median']
    sp = M2.pooled_spread(parts) if parts else None
    if not sp or sp['active']['median'] is None:
        return {'ok': False, 'info': info}
    return {'ok': True, 'median': float(sp['active']['median']), 'p90': float(sp['active']['p90']), 'n': int(sp['active']['n']), 'info': info}


async def run_load(params, progress=None):
    """/factory_load: докачка Dukascopy по 18 инструментам (с места остановки), спред XAGUSD. → текст отчёта."""
    import aiohttp
    d = params.get('fdir') or FD.factory_dir()
    now_ms = params.get('now_ms') or int(time.time() * 1000)
    deadline = time.time() + float(params.get('max_minutes', 120)) * 60
    keys = list(FD.INSTRUMENTS)
    os.makedirs(d, exist_ok=True)
    lines = [f'📥 <b>/factory_load</b> — загрузка Dukascopy (часовые BID) в {d}; свободно {V.free_mb(d):.0f} МБ']
    stopped_all = ''
    async with aiohttp.ClientSession(headers=M.UA) as ses:
        fetch = V.make_fetcher(ses)
        for key in keys:
            try:
                st = await FD.load_instrument(fetch, key, d, now_ms, progress=progress, deadline=deadline)
            except V.VisionError as e:
                stopped_all = f'⛔ ЗАГРУЗКА ОСТАНОВЛЕНА: {e}'
                break
            cov = FD.coverage(d, key, now_ms)
            if progress:
                await progress(f'{key}: файлов {cov["stored"]}, пометок 404 {cov["absent"]}, не получено {len(cov["missing"])}')
            lines.append(f'  {key}: файлов {cov["stored"]} ({cov["first_have"]} … {cov["last_have"]}), до начала истории (404) {cov["absent"]}, нечитаемых {cov["bad"]}, '
                         f'НЕ ПОЛУЧЕНО {len(cov["missing"])}' + (f' [{", ".join(cov["missing"][:6])}{"…" if len(cov["missing"]) > 6 else ""}]' if cov['missing'] else '')
                         + f'; запросов {st["requests"]}' + (f'; остановлен: {st["stopped"]}' if st['stopped'] else '') + (f'; плохие: {st["bad"][:2]}' if st['bad'] else ''))
            if st['stopped'] == 'время':
                stopped_all = '⏱ бюджет времени прогона исчерпан — повторите /factory_load (продолжит с места остановки)'
                break
        if not stopped_all and kv_get('spread:XAGUSD', d) is None and FD.stored_tags(d, 'XAGUSD'):
            sp = await measure_spread(fetch, 'XAGUSD', now_ms, progress)
            if sp['ok']:
                kv_set('spread:XAGUSD', sp, d)
                lines.append(f'  спред XAGUSD (медиана активных часов, 3 месяца): {sp["median"]:.2f} б.п. (p90 {sp["p90"]:.2f}, n={sp["n"]}) — записан в БД, перенести в BOT_SPEC §2.41.17')
            else:
                lines.append(f'  спред XAGUSD НЕ измерен: {sp["info"]} — повторите /factory_load')
        elif kv_get('spread:XAGUSD', d):
            sp = kv_get('spread:XAGUSD', d)
            lines.append(f'  спред XAGUSD: {sp["median"]:.2f} б.п. (из БД)')
    if stopped_all:
        lines.append(stopped_all)
    ok, probs, avail, excl = availability(d, now_ms)
    lines.append('\n' + ('✅ Данные полны: можно запускать /factory_pairs, затем /factory.' if ok else '⚠️ Данные неполны: ' + '; '.join(probs) + ' — повторите /factory_load.'))
    if excl:
        lines.append('Исключены как недоступные (404 на всю историю): ' + ', '.join(f'{k} ({v})' for k, v in excl.items()))
    return '\n'.join(lines)


def availability(d, now_ms):
    """→ (готово, проблемы[], доступные инструменты, исключённые {ключ: причина}). Инструмент без единого файла и со всеми месяцами в 404 исключается (допустимо §2.41.17);
    пропуски месяцев между имеющимися — проблема (хронические 503)."""
    probs, avail, excl = [], [], {}
    for key in FD.INSTRUMENTS:
        cov = FD.coverage(d, key, now_ms)
        if cov['stored'] == 0:
            if cov['absent'] >= len(FD.month_tags(FD.INSTRUMENTS[key][3], FD.last_complete(now_ms))) and cov['absent'] > 0:
                excl[key] = '404 на всю историю'
            else:
                probs.append(f'{key}: данных нет (не загружено)')
            continue
        if cov['missing']:
            probs.append(f'{key}: не получено {len(cov["missing"])} мес.')
            continue
        avail.append(key)
    return (not probs), probs, avail, excl


# ── /factory_pairs ───────────────────────────────────────────────────────────────────────────────────
def run_pairs(params):
    d = params.get('fdir') or FD.factory_dir()
    pairs = select_pairs(params['vdir'], params['syms'])
    kv_set('pairs', pairs, d)
    kv_set('pairs_ack', False, d)
    lines = ['🔗 <b>/factory_pairs</b> — 10 пар P-PAIR с максимальной корреляцией дневных лог-доходностей на TRAIN (2020–2023, 1D, ≥ 365 общих дней; BOT_SPEC §2.41.17)']
    for i, (a, b, c, n) in enumerate(pairs, 1):
        lines.append(f'  {i}. {a}/{b}: corr {c:.4f} (n={n})')
    if len(pairs) < PAIR_N:
        lines.append(f'⚠️ найдено пар {len(pairs)} из {PAIR_N} — проверьте кэш Binance (/data/vision).')
    lines.append('Список заморожен в БД. ДО /factory: пришлите его мне — запишу в BOT_SPEC; затем `/factory_pairs ok` (подтверждение, что список записан). Без подтверждения /factory не запустится.')
    return '\n'.join(lines)


# ── /factory: калибровка, шаги A и B ─────────────────────────────────────────────────────────────────
def build_ctx(params):
    d = params.get('fdir') or FD.factory_dir()
    now_ms = params.get('now_ms') or int(time.time() * 1000)
    return {'fdir': d, 'vdir': params['vdir'], 'syms': list(params['syms']), 'now_ms': now_ms}


def preflight(ctx):
    """Условия запуска /factory. → (проблемы[], spreads dict, pairs, исключённые)."""
    d = ctx['fdir']
    probs = []
    ok, p, avail, excl = availability(d, ctx['now_ms'])
    probs += p
    spreads = dict(E.SPREAD_BPS)
    if 'XAGUSD' in avail:
        sp = kv_get('spread:XAGUSD', d)
        if not sp:
            probs.append('спред XAGUSD не измерен (/factory_load)')
        else:
            spreads['XAGUSD'] = sp['median']
    pairs = kv_get('pairs', d)
    if not pairs or len(pairs) != PAIR_N:
        probs.append('пары P-PAIR не заморожены (/factory_pairs)')
    elif not kv_get('pairs_ack', d):
        probs.append('список пар не подтверждён (/factory_pairs ok после записи в BOT_SPEC)')
    for s in ctx['syms']:
        if V.load_klines(ctx['vdir'], s, '15m') is None:
            probs.append(f'{s}: нет кэша 15m (/data/vision)')
    ctx['avail'] = avail
    return probs, spreads, [(a, b) for a, b, _c, _n in (pairs or [])], excl


def n_by_class(ctx):
    n = {'CRYPTO': len(ctx['syms'])}
    for c in ('COMMODITIES', 'FX', 'INDICES', 'BONDS'):
        n[c] = len([k for k in FD.CLASS_MEMBERS[c] if k in ctx['avail']])
    n['MULTI'] = len([k for k in FD.CLASS_MEMBERS['MULTI'] if k in ctx['avail']])
    return n


def specs_for_cell(cls, tf, n_cls):
    """Список (id, семейство, spec) тестов ячейки."""
    out = []
    for cfg in L.configs_for(n_cls):
        out.append((cfg.id, cfg.family, ('lib', cfg)))
    if cls == 'CRYPTO':
        if tf in PAIR_TFS:
            out += [(f'P-PAIR-L{lb}', 'P-PAIR', ('pair', lb)) for lb in PAIR_LS]
        if tf in LAG_TFS:
            out += [(f'L-LAG-H{h}', 'L-LAG', ('lag', h)) for h in LAG_HS]
    return out


async def run_factory(params, progress=None):
    """/factory. → текст отчёта. Порядок: по каждой ячейке — реальные конфигурации (результаты не показываются) → калибровка ячейки; провал любой калибровки = СТОП без реальных
    результатов; затем BH по всем N, шаги A и B, кандидаты на финал."""
    ctx = build_ctx(params)
    d = ctx['fdir']
    probs, spreads, pairs, excl = preflight(ctx)
    if probs:
        return '⛔ /factory отказ: ' + '; '.join(probs)
    ncls = n_by_class(ctx)
    tests = enumerate_tests(ncls)
    N = len(tests)

    async def say(m):
        if progress:
            await progress(m)
    await say(f'/factory: N={N} тестов, seed {SEED}, ячеек {len(FD.CELLS)}; инструментов {ncls}; rev {REV}')
    results = {}                                           # tid → статистика
    calib = {}
    for ci, (cls, tf) in enumerate(FD.CELLS):
        key = f'cell:{REV}:{ci}'
        saved = kv_get(key, d)
        if saved is None:
            series = cell_series(cls, tf, ctx)
            await say(f'ячейка {ci + 1}/{len(FD.CELLS)} {cls} {tf}: инструментов {len(series)}, баров {sum(S["n"] for S in series)}')
            if not series:
                return f'⛔ /factory: ячейка {cls} {tf}: нет данных'
            ncell = len(series)
            real, ent_freqs, durs_all = {}, [], []
            elig = eligible_bars(series)
            for cid, fam, spec in specs_for_cell(cls, tf, ncell):
                out = run_config(spec, series, tf, cls, spreads, pairs)
                real[tid(cls, tf, cid)] = period_stats(out['days'], out['r'])
                ne, durs = trade_stats(out['trades'], series, tf)
                if fam in ('T-MA', 'T-DC', 'T-MOM', 'R-RSI2', 'R-BB', 'B-VC', 'X-MOM'):          # статистика сделок — только библиотека (§2.41.9)
                    ent_freqs.append(ne / max(1, elig))
                    durs_all += durs
                await asyncio.sleep(0)
            for cfg in L.configs_for(ncell):                                                       # диагностика B-VC без отбора по исходу бара (не входит в N, BH и кандидаты)
                if cfg.family == 'B-VC':
                    o2 = run_config(('lib', cfg, 'oco'), series, tf, cls, spreads, pairs)
                    real['diag:' + tid(cls, tf, 'B-VC-OCO')] = period_stats(o2['days'], o2['r'])
            p_entry = float(np.mean(ent_freqs)) if ent_freqs else 0.0
            rng = np.random.default_rng([SEED, ci, 999999])
            if len(durs_all) > DURS_KEEP:
                durs_all = list(rng.choice(durs_all, DURS_KEEP, replace=False))
            await say(f'  реальные конфигурации посчитаны (результаты скрыты до калибровки); частота входов {p_entry:.5f}, сделок в образце {len(durs_all)}; калибровка 200…')
            cal = calibrate_cell(ci, cls, tf, series, spreads, p_entry, durs_all, n_rand=CALIB_N, progress=progress)
            cal.update({'p_entry': p_entry, 'durs_n': len(durs_all)})
            saved = {'real': real, 'calib': cal}
            kv_set(key, saved, d)
            series = None
            await say(f'  калибровка {cls} {tf}: прошло шаг A {cal["n_pass"]} из {cal["n"]} ({cal["frac"] * 100:.1f}%) — ' + ('ПРОВАЛ' if cal['fail'] else 'норма'))
        results.update(saved['real'])
        calib[ci] = saved['calib']
        if saved['calib']['fail']:
            break
    return build_report(ctx, tests, N, results, calib, ncls, excl, spreads, pairs)


def build_report(ctx, tests, N, results, calib, ncls, excl, spreads, pairs):
    d = ctx['fdir']
    R = [f'📈 <b>/factory — этап 2 «фабрики» (BOT_SPEC §2.41 + поправка §2.41.17)</b> | код {REV} | N = {N} | seed калибровки {SEED}',
         f'Инструменты по классам: {ncls}. Исключены: {excl or "нет"}. Время: {datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC.', '',
         '<b>КАЛИБРОВКА (200 случайных стратегий на ячейку, шаг A на TRAIN; порог ≤ 5% = ≤ 10 из 200)</b>']
    failed = []
    for ci, (cls, tf) in enumerate(FD.CELLS):
        c = calib.get(ci)
        if c is None:
            R.append(f'  {cls} {tf}: не считалась')
            continue
        R.append(f'  {cls} {tf}: прошло {c["n_pass"]} из {c["n"]} ({c["frac"] * 100:.1f}%), частота входов {c["p_entry"]:.5f}, сделок в образце {c["durs_n"]} — {"ПРОВАЛ" if c["fail"] else "норма"}')
        if c['fail']:
            failed.append(f'{cls} {tf}')
    if failed:
        R.append('')
        R.append('<b>ВЕРДИКТ: ПРОЦЕДУРА ОТБОРА НЕНАДЁЖНА</b> — в ячейках ' + ', '.join(failed) + ' случайные стратегии проходят шаг A чаще 5%. Этап 2 дальше не идёт (BOT_SPEC §2.41.9); '
                 'результаты реальных конфигураций не показываются.')
        return '\n'.join(R)
    ids = [tid(cls, tf, cid) for _ci, cls, tf, cid, _fam in tests]
    miss = [i for i in ids if i not in results]
    if miss:
        R.append(f'⚠️ нет результатов для {len(miss)} тестов')
    t_tr = [results[i]['TRAIN']['t'] if i in results else None for i in ids]
    pv = [E.p_one_sided(t) for t in t_tr]
    rej = E.bh_reject(pv)
    passA = [bool(rej[k] and (t_tr[k] or 0) > 0) for k in range(len(ids))]
    passB = []
    for k, i in enumerate(ids):
        v = results[i]['VALID'] if i in results else None
        passB.append(bool(passA[k] and v and v['mean'] > 0 and v['t'] is not None and v['t'] >= L.VALID_MIN_T))
    R.append('')
    R.append(f'<b>ШАГ A (TRAIN 2020–2023):</b> прошло {sum(passA)} из {N} (t Ньюи–Уэста лаг {L.NW_LAG}, p = 1 − Φ(t), Бенджамини–Хохберг q = {L.FDR_Q}, t > 0).')
    R.append(f'<b>ШАГ B (VALID 2024):</b> прошло {sum(passB)} из {sum(passA)} (mean > 0 и t ≥ {L.VALID_MIN_T}).')
    cands = sorted([k for k in range(len(ids)) if passB[k]], key=lambda k: -(results[ids[k]]['TV']['t'] or -99))[:MAX_FINALISTS]
    R.append('')
    R.append('<b>КАНДИДАТЫ НА ФИНАЛ</b> (не более 3, по t на TRAIN+VALID): ' + ('нет — финал не запускается' if not cands else ''))
    for k in cands:
        r = results[ids[k]]
        dg = results.get('diag:' + ids[k].replace('B-VC-NR7', 'B-VC-OCO')) if '|B-VC' in ids[k] else None
        warn = ''
        if '|B-VC' in ids[k]:
            warn = ('  ⚠️ B-VC: правило «оба уровня — сделка не открывается» использует исход бара t+1 (оптимистичное смещение; на случайном блуждании даёт t > 5). '
                    + ('Диагностика без отбора по исходу (OCO): TRAIN t %s, VALID t %s, TRAIN+VALID t %s.' % tuple('н/д' if x is None else '%.2f' % x for x in (dg['TRAIN']['t'], dg['VALID']['t'], dg['TV']['t'])) if dg else ''))
        R.append(f'  {ids[k]}: t(TRAIN+VALID) {r["TV"]["t"]:.2f}, mean {r["TV"]["mean"] * 1e4:.2f} б.п./день; TRAIN t {r["TRAIN"]["t"]:.2f}, VALID t {r["VALID"]["t"]:.2f}' + warn)
    kv_set('candidates', [ids[k] for k in cands], d)
    kv_set('factory_report_rev', REV, d)
    if cands:
        R.append('Шаг C — ТОЛЬКО вручную: подтвердите список и запустите /factory_final (один прогон, повтор запрещён).')
    R.append('')
    R.append('<b>ТАБЛИЦА ВСЕХ N ТЕСТОВ</b> (mean — б.п./день; t — Ньюи–Уэста; p — односторонний; A/B — прошёл)')
    R.append('ячейка | конфигурация | mean TRAIN | t TRAIN | p | mean VALID | t VALID | A | B')
    for k, (ci, cls, tf, cid, fam) in enumerate(tests):
        r = results.get(ids[k])
        if not r:
            R.append(f'{cls} {tf} | {cid} | нет данных')
            continue
        f = lambda x, nd=2: ('н/д' if x is None else f'{x:.{nd}f}')
        R.append(f'{cls} {tf} | {cid} | {r["TRAIN"]["mean"] * 1e4:.2f} | {f(r["TRAIN"]["t"])} | {pv[k]:.4f} | {r["VALID"]["mean"] * 1e4:.2f} | {f(r["VALID"]["t"])} | '
                 f'{"да" if passA[k] else "нет"} | {"да" if passB[k] else "нет"}')
    dgs = [(k, v) for k, v in results.items() if k.startswith('diag:')]
    if dgs:
        R.append('')
        R.append('<b>ДИАГНОСТИКА B-VC без отбора по исходу бара (OCO; НЕ входит в N, BH и список кандидатов)</b>')
        for k, v in dgs:
            R.append(f'  {k[5:]}: mean TRAIN {v["TRAIN"]["mean"] * 1e4:.2f} б.п./день, t {("н/д" if v["TRAIN"]["t"] is None else "%.2f" % v["TRAIN"]["t"])}; '
                     f'VALID mean {v["VALID"]["mean"] * 1e4:.2f}, t {("н/д" if v["VALID"]["t"] is None else "%.2f" % v["VALID"]["t"])}')
    R.append('')
    R.append('Оговорки: спреды/издержки — BOT_SPEC §2.41.17 (сценарий (б), перенос FIN_RATE_YR); данные до 2019-01-01 в расчётах не участвуют; B-VC имеет встроенное оптимистичное смещение (см. выше); '
             'WTI в 2020-04 может иметь аномальные цены — сделки с px ≤ 0 пропускаются; фандинг CRYPTO — по кэшу Binance (нет данных — без фандинга).')
    return '\n'.join(R)


# ── /factory_final (шаг C) ───────────────────────────────────────────────────────────────────────────
def lockbox_metrics(days, r, periods, days_s, r_s):
    """Метрики LOCKBOX по базовому (days, r) и стресс-прогону (days_s, r_s: издержки ×2). Стресс без совпадающих дней — не пройдено (stress_mean = nan)."""
    lo = E.day_bounds('LOCKBOX')[0]
    x = r[days >= lo]
    xs = r_s[days_s >= lo]
    mean, t, n = E.nw_t(x)
    ms = float(xs.mean()) if len(xs) and len(xs) == len(x) else float('nan')
    sh = E.sharpe(x, periods)
    dd = E.max_drawdown(x)
    ok = (n > 0 and math.isfinite(sh) and sh >= L.FINAL_MIN_SHARPE and mean > 0 and math.isfinite(t) and t >= L.FINAL_MIN_T and dd <= L.FINAL_MAX_DD and math.isfinite(ms) and ms > 0)
    return {'n': n, 'mean': mean, 't': (None if not math.isfinite(t) else t), 'sharpe': (None if not math.isfinite(sh) else sh), 'dd': dd, 'stress_mean': ms, 'pass': bool(ok)}


def parse_tid(t):
    cls, tf, cid = t.split('|')
    return cls, tf, cid


def spec_of(cid, ncls_cell):
    for cfg in L.configs_for(ncls_cell):
        if cfg.id == cid:
            return ('lib', cfg)
    if cid.startswith('P-PAIR-L'):
        return ('pair', int(cid[8:]))
    if cid.startswith('L-LAG-H'):
        return ('lag', int(cid[7:]))
    raise ValueError(cid)


async def run_final(params, progress=None):
    """/factory_final: шаг C для финалистов (≤ 3) на LOCKBOX. Флаг final_started ставится ДО чтения LOCKBOX (повтор запрещён навсегда, даже при сбое). → текст отчёта."""
    ctx = build_ctx(params)
    d = ctx['fdir']
    cands = kv_get('candidates', d) or []
    ids = params.get('ids') or cands
    bad = [i for i in ids if i not in cands]
    if bad or not ids or len(ids) > MAX_FINALISTS:
        return f'⛔ /factory_final отказ: список финалистов {ids} не входит в утверждённых кандидатов {cands} или пуст/больше {MAX_FINALISTS}'
    if kv_get('final_started', d) or kv_get('final_done', d):
        return '⛔ /factory_final отказ: финал уже запускался (флаг в БД) — шаг C выполняется один раз, повтор запрещён (BOT_SPEC §2.41.8)'
    probs, spreads, pairs, excl = preflight(ctx)
    if probs:
        return '⛔ /factory_final отказ: ' + '; '.join(probs) + ' (флаг финала НЕ взят)'
    if not kv_set_if_absent('final_started', {'t': time.time(), 'ids': ids, 'rev': REV}, d):
        return '⛔ /factory_final отказ: флаг финала уже взят параллельным запуском'
    session = L.open_lockbox_session(final=True)
    cells = {}
    rows = []
    for t in ids:
        cls, tf, cid = parse_tid(t)
        ci = FD.CELLS.index((cls, tf))
        if ci not in cells:
            cells = {ci: cell_series(cls, tf, ctx, mode='full', session=session)}          # по одной ячейке в памяти
        series = cells[ci]
        spec = spec_of(cid, len(series) if cls != 'CRYPTO' else len(ctx['syms']))
        base = run_config(spec, series, tf, cls, spreads, pairs, 1.0)
        stress = run_config(spec, series, tf, cls, spreads, pairs, 2.0)
        periods = 365 if cls == 'CRYPTO' else 252
        met = lockbox_metrics(base['days'], base['r'], periods, stress['days'], stress['r'])
        rows.append((t, met))
        if progress:
            await progress(f'финал {t}: готово')
        await asyncio.sleep(0)
    kv_set('final_result', {'rows': rows, 'rev': REV}, d)
    kv_set('final_done', True, d)
    R = [f'🔒 <b>/factory_final — шаг C на LOCKBOX (2025+), единственный прогон</b> | код {REV}', 'Критерии: Шарп ≥ 0.5, mean > 0 при t ≥ 2.0, макс. просадка ≤ 15%, при издержках ×2 mean > 0.', '']
    for t, m in rows:
        t_s = 'н/д' if m['t'] is None else '%.2f' % m['t']
        sh_s = 'н/д' if m['sharpe'] is None else '%.2f' % m['sharpe']
        st_s = 'н/д' if not math.isfinite(m['stress_mean']) else '%.2f' % (m['stress_mean'] * 1e4)
        verdict = 'ПРОЙДЕНО' if m['pass'] else 'НЕ ПРОЙДЕНО'
        R.append('%s: дней %d, mean %.2f б.п./день, t %s, Шарп %s, просадка %.1f%%, mean при издержках ×2 %s б.п./день — %s' % (t, m['n'], m['mean'] * 1e4, t_s, sh_s, m['dd'] * 100, st_s, verdict))
    R.append('')
    R.append('Решение (BOT_SPEC §2.41.10): прошедший финалист → shadow-форвард (отдельный пре-коммит); ни один → поиск простых систематических стратегий завершён для всех классов.')
    return '\n'.join(R)
