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

import research_fbias as RB
import research_fdata as FD
import research_fengine as E
import research_ma as M
import research_ma_lib as L
import research_vision as V

REV = 'v103'                                                # ревизия кэша результатов /factory (не менялась: результаты первого прогона сохраняются)
NC_REV = 'v105'                                             # ревизия кэша /factory_nc и /factory_decomp (поправка §2.41.23)
SWAP_NC = 0.025                                             # перенос не-крипто в /factory_nc (BOT_SPEC §2.41.23), годовых от номинала
NC_LAGS_1D = (5, 20)                                        # лаги Ньюи–Уэста для 1D-ячеек в /factory_nc: отбор по меньшему из двух t
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
BIAS_REPS = RB.REPS                                         # 50 повторов на конфигурацию (тесты уменьшают)
SPREAD_XAG_FALLBACK = 10.99                                 # б.п., BOT_SPEC §2.41.21 п. 3.4 (значение владельца; используется, если /factory_load не измерил)
SPEC_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'BOT_SPEC.md')


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


def enumerate_tests(n_by_class, excluded=()):
    """Все тесты этапа 2: [(индекс ячейки, класс, ТФ, id конфигурации, семейство)]. n_by_class — число доступных инструментов по классам (X-MOM при ≥ 6);
    excluded — семейства, исключённые проверкой смещения (§2.41.20)."""
    out = []
    for i, (cls, tf) in enumerate(FD.CELLS):
        for cid, fam, _spec in specs_for_cell(cls, tf, n_by_class[cls], excluded):
            out.append((i, cls, tf, cid, fam))
    return out


def count_n(n_by_class, excluded=()):
    return len(enumerate_tests(n_by_class, excluded))


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
        S = E.make_series(key, bars, FD_trade_from())
        out += E.split_series(S, FD.gap_breaks(d, key, S['ts']), FD_trade_from())          # постоянные разрывы → независимые сегменты (§2.41.21)
    return out


def cell_series(cls, tf, ctx, mode='pre', session=None):
    if cls == 'CRYPTO':
        return crypto_series(ctx['vdir'], ctx['syms'], tf, mode, session)
    keys = [k for k in FD.CLASS_MEMBERS[cls] if k in ctx['avail']]
    return noncrypto_series(ctx['fdir'], keys, tf, mode, session)


def cost_of(S, cls, spreads, mult=1.0, swap_yr=None):
    if cls == 'CRYPTO':
        return E.cost_crypto(mult)
    return E.cost_noncrypto(S['key'], spreads, mult, swap_yr)


# ── прогон одной конфигурации по ячейке ──────────────────────────────────────────────────────────────
def run_config(spec, series, tf, cls, spreads, pairs, mult=1.0, swap_yr=None, detail=False):
    """spec = ('lib', L.Config) | ('pair', Lb) | ('lag', H). → dict(days, r, trades=[(ключ, e, last, время)…]) — портфель ячейки (простая сумма вкладов).
    swap_yr — годовой перенос не-крипто (None → ENV FIN_RATE_YR); detail=True добавляет 'multi' (по дням: ret, c_side, c_swap, c_fund, expo) для /factory_decomp."""
    kind = spec[0]
    parts, trades = [], []
    by_key = {S['key']: S for S in series}
    t0 = FD_trade_from()
    if kind == 'lib':
        cfg = spec[1]
        if cfg.cross_sectional:
            sigs = E.xmom_signals(series, tf, dict(cfg.params)['lookback'], dict(cfg.params)['rebalance'])
        else:
            sigs = {S['key']: E.library_signals(cfg, S) for S in series}
        for S in series:
            g = sigs[S['key']]
            r = E.simulate(S, g['sig'], cost_of(S, cls, spreads, mult, swap_yr), exit_l=g.get('exit_l'), exit_s=g.get('exit_s'), reverse=g.get('reverse', False),
                           hold=g.get('hold'), entry_px=g.get('entry_px'), detail=detail)
            parts.append(r)
            trades += [(S['key'], tr[0], tr[1], S['ts'][tr[0]]) for tr in r['trades']]
    elif kind == 'pair':
        for a, b in pairs:
            if a in by_key and b in by_key:
                r = E.simulate_pair(by_key[a], by_key[b], spec[1], cost_of(by_key[a], cls, spreads, mult, swap_yr), cost_of(by_key[b], cls, spreads, mult, swap_yr), t0, detail=detail)
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
                r = E.simulate(S, g['sig'], cost_of(S, cls, spreads, mult, swap_yr), hold=g['hold'], reverse=False, detail=detail)
                parts.append(r)
                trades += [(S['key'], tr[0], tr[1], S['ts'][tr[0]]) for tr in r['trades']]
    for S in series:
        S['_c'].clear()                                    # кэш индикаторов — только на время одной конфигурации (память: лимит RESEARCH_MEM_MB)
    if detail:
        days, multi = E.portfolio_daily_multi(parts, ('ret', 'c_side', 'c_swap', 'c_fund', 'expo'))
        return {'days': days, 'r': multi['ret'], 'trades': trades, 'multi': multi}
    days, r = E.portfolio_daily(parts)
    return {'days': days, 'r': r, 'trades': trades}


def t_select(x, lags=(5,)):
    """t Ньюи–Уэста по нескольким лагам; выбираемая статистика — МЕНЬШЕЕ из t (BOT_SPEC §2.41.23 п. 3 для 1D-ячеек). → (mean, t_sel | nan, {лаг: t}, n)."""
    ts, m, n = {}, 0.0, len(x)
    for lag in lags:
        m, t, n = E.nw_t(x, lag)
        ts[lag] = t
    vals = list(ts.values())
    t_sel = min(vals) if vals and all(math.isfinite(v) for v in vals) else float('nan')
    return m, t_sel, ts, n


def period_stats(days, r, lags=(5,)):
    """TRAIN / VALID / TRAIN+VALID: среднее, выбираемое t Ньюи–Уэста (меньшее по лагам), число дней; при нескольких лагах — ещё 'tl' = {лаг: t}."""
    out = {}
    lb = L.lockbox_start_ms() // E.DAY_MS
    tr_lo = E.day_bounds('TRAIN')[0]
    parts = {name: E.period_slice(days, r, name)[1] for name in ('TRAIN', 'VALID')}
    parts['TV'] = r[(days >= tr_lo) & (days < lb)]
    for name, x in parts.items():
        m, t, ts, n = t_select(x, lags)
        rec = {'mean': m, 't': (None if not math.isfinite(t) else t), 'n': n}
        if len(lags) > 1:
            rec['tl'] = {str(k): (None if not math.isfinite(v) else v) for k, v in ts.items()}
        out[name] = rec
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
def calibrate_cell(cell_idx, cls, tf, series, spreads, p_entry, durs, n_rand=CALIB_N, progress=None, swap_yr=None, lags=(5,)):
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
            parts.append(E.simulate(S, sig, cost_of(S, cls, spreads, 1.0, swap_yr), hold=hold, reverse=False))
        days, r = E.portfolio_daily(parts)
        d, x = E.period_slice(days, r, 'TRAIN')
        _m, t, _ts, _n = t_select(x, lags)
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
            att = FD.load_index(d, key)['attempts']
            lines.append(f'  {key}: файлов {cov["stored"]} из {cov["wanted"]} месяцев 2019+ ({cov["first_have"]} … {cov["last_have"]}), 404 {cov["absent"]}, нечитаемых {cov["bad"]}, '
                         f'НЕ ПОЛУЧЕНО {len(cov["missing"])}' + (f' [{", ".join(f"{t} (попыток {att.get(t, 0)}/{FD.GAP_ATTEMPTS})" for t in cov["missing"][:6])}{"…" if len(cov["missing"]) > 6 else ""}]' if cov['missing'] else '')
                         + (f'; ПОСТОЯННЫЕ РАЗРЫВЫ {", ".join(cov["gaps"])}' if cov['gaps'] else '')
                         + f'; запросов {st["requests"]}' + (f'; остановлен: {st["stopped"]}' if st['stopped'] else '') + (f'; плохие: {st["bad"][:2]}' if st['bad'] else ''))
            if st['stopped'] == 'время':
                stopped_all = '⏱ бюджет времени прогона исчерпан — повторите /factory_load (продолжит с места остановки)'
                break
        if not stopped_all and kv_get('spread:XAGUSD', d) is None and FD.stored_tags(d, 'XAGUSD'):
            sp = await measure_spread(fetch, 'XAGUSD', now_ms, progress)
            if sp['ok']:
                kv_set('spread:XAGUSD', sp, d)
                lines.append(f'  спред XAGUSD (медиана активных часов, 3 месяца): {sp["median"]:.2f} б.п. (p90 {sp["p90"]:.2f}, n={sp["n"]}) — записан в БД и будет использован вместо 10.99 б.п. из §2.41.21 (перенести в BOT_SPEC)')
            else:
                lines.append(f'  спред XAGUSD НЕ измерен: {sp["info"]} — будет использовано 10.99 б.п. (BOT_SPEC §2.41.21 п. 3.4)')
        elif kv_get('spread:XAGUSD', d):
            sp = kv_get('spread:XAGUSD', d)
            lines.append(f'  спред XAGUSD: {sp["median"]:.2f} б.п. (из БД)')
    if stopped_all:
        lines.append(stopped_all)
    ok, probs, avail, excl = availability(d, now_ms)
    lines.append('\n' + ('✅ Данные полны: можно запускать /factory_pairs, затем /factory.' if ok else '⚠️ Данные неполны: ' + '; '.join(probs) + ' — повторите /factory_load.'))
    if excl:
        lines.append('Исключены из расчётов: ' + ', '.join(f'{k} ({v})' for k, v in excl.items()))
    gp = gaps_of(d, now_ms, avail)
    if gp:
        lines.append('Постоянные разрывы (допустимо ≤ 2 на инструмент; сегментация — BOT_SPEC §2.41.21): ' + '; '.join(f'{k}: {", ".join(v)}' for k, v in gp.items()))
    return '\n'.join(lines)


def availability(d, now_ms):
    """→ (готово, проблемы[], доступные инструменты, исключённые {ключ: причина}). Учитываются только месяцы с 2019-01 (§2.41.21). Инструмент без файлов и со всеми месяцами в 404
    исключается (§2.41.17); с > 2 постоянными разрывами — исключается (§2.41.21); непостоянные пропуски — проблема (повторить /factory_load)."""
    probs, avail, excl = [], [], {}
    for key in FD.INSTRUMENTS:
        cov = FD.coverage(d, key, now_ms)
        if cov['stored'] == 0:
            if cov['absent'] >= cov['wanted'] and cov['absent'] > 0:
                excl[key] = '404 на всю историю (2019+)'
            else:
                probs.append(f'{key}: данных нет (не загружено)')
            continue
        if cov['missing']:
            probs.append(f'{key}: не получено {len(cov["missing"])} мес.')
            continue
        if len(cov['gaps']) > FD.GAP_MAX_PER_INSTRUMENT:
            excl[key] = f'{len(cov["gaps"])} постоянных разрывов (> {FD.GAP_MAX_PER_INSTRUMENT}): {", ".join(cov["gaps"])}'
            continue
        avail.append(key)
    return (not probs), probs, avail, excl


def gaps_of(d, now_ms, keys):
    """Постоянные разрывы (≥ 2019-01) по доступным инструментам: {ключ: [месяцы]}; пустые не включаются."""
    out = {}
    for k in keys:
        g = FD.coverage(d, k, now_ms)['gaps']
        if g:
            out[k] = g
    return out


def read_spec_pairs(path=None):
    """Список пар из строки `PAIRS-FROZEN:` файла BOT_SPEC (формат `A/B, C/D, …`) → ['A/B', …] | None (строки нет или список не записан)."""
    try:
        with open(path or SPEC_PATH, encoding='utf-8') as f:
            for ln in f:
                if ln.startswith('PAIRS-FROZEN:'):
                    items = [x.strip() for x in ln.split(':', 1)[1].split(',') if x.strip()]
                    return items if items and all('/' in x and ' ' not in x for x in items) else None
    except OSError:
        return None
    return None


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
    return {'fdir': d, 'vdir': params['vdir'], 'syms': list(params['syms']), 'now_ms': now_ms, 'spec_path': params.get('spec_path'), 'version': params.get('version')}


def preflight(ctx, need_pairs=True, need_crypto=True):
    """Условия запуска /factory. → (проблемы[], spreads dict, pairs, исключённые). need_pairs/need_crypto=False — для /factory_nc (крипто и пары не используются)."""
    d = ctx['fdir']
    probs = []
    ok, p, avail, excl = availability(d, ctx['now_ms'])
    probs += p
    spreads = dict(E.SPREAD_BPS)
    ctx['xag_src'] = None
    if 'XAGUSD' in avail:
        sp = kv_get('spread:XAGUSD', d)
        if sp:
            spreads['XAGUSD'] = sp['median']
            ctx['xag_src'] = f'измерено /factory_load: {sp["median"]:.2f} б.п.'
        else:
            spreads['XAGUSD'] = SPREAD_XAG_FALLBACK
            ctx['xag_src'] = f'из поправки BOT_SPEC §2.41.21 (значение владельца, не измерено): {SPREAD_XAG_FALLBACK:.2f} б.п.'
    pairs = kv_get('pairs', d)
    if not need_pairs:
        pass
    elif not pairs or len(pairs) != PAIR_N:
        probs.append('пары P-PAIR не заморожены (/factory_pairs)')
    else:
        if not kv_get('pairs_ack', d):
            probs.append('список пар не подтверждён (/factory_pairs ok после записи в BOT_SPEC)')
        spec_pairs = read_spec_pairs(ctx.get('spec_path'))
        db_pairs = [f'{a}/{b}' for a, b, _c, _n in pairs]
        if spec_pairs is None:
            probs.append('список пар не записан в BOT_SPEC (строка PAIRS-FROZEN:, §2.41.21 п. 3.5)')
        elif spec_pairs != db_pairs:
            probs.append(f'список пар в BOT_SPEC не совпадает с замороженным в БД (BOT_SPEC: {", ".join(spec_pairs)}; БД: {", ".join(db_pairs)})')
    for s in (ctx['syms'] if need_crypto else []):
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


def specs_for_cell(cls, tf, n_cls, excluded=()):
    """Список (id, семейство, spec) тестов ячейки без исключённых семейств."""
    out = []
    for cfg in L.configs_for(n_cls):
        out.append((cfg.id, cfg.family, ('lib', cfg)))
    if cls == 'CRYPTO':
        if tf in PAIR_TFS:
            out += [(f'P-PAIR-L{lb}', 'P-PAIR', ('pair', lb)) for lb in PAIR_LS]
        if tf in LAG_TFS:
            out += [(f'L-LAG-H{h}', 'L-LAG', ('lag', h)) for h in LAG_HS]
    return [x for x in out if x[1] not in excluded]


async def run_factory(params, progress=None):
    """/factory. → текст отчёта. Порядок: 0) проверка смещения правил на синтетике (§2.41.20; смещённые семейства исключаются из N); затем по каждой ячейке — реальные конфигурации
    (результаты не показываются) → калибровка ячейки; провал любой калибровки = СТОП без реальных результатов; затем BH по оставшимся N, шаги A и B, кандидаты на финал."""
    ctx = build_ctx(params)
    d = ctx['fdir']
    probs, spreads, pairs, excl = preflight(ctx)
    if probs:
        return '⛔ /factory отказ: ' + '; '.join(probs)
    ncls = n_by_class(ctx)
    full_tests = enumerate_tests(ncls)
    family_of = {cid: fam for _ci, _c, _t, cid, fam in full_tests}
    cell_names = [f'{c} {tf}' for c, tf in FD.CELLS]

    async def say(m):
        if progress:
            await progress(m)
    await say(f'/factory: тестов до проверки смещения {len(full_tests)}, seed {SEED}, ячеек {len(FD.CELLS)}; инструментов {ncls}; rev {REV}')
    # ── 0. проверка смещения правил (синтетика, без издержек) ──
    per_cell = {}
    for ci, (cls, tf) in enumerate(FD.CELLS):
        bkey = f'bias:{REV}:{ci}'
        saved = kv_get(bkey, d)
        if saved is None:
            series = cell_series(cls, tf, ctx)
            if not series:
                return f'⛔ /factory: ячейка {cls} {tf}: нет данных'
            specs = specs_for_cell(cls, tf, len(series))
            await say(f'проверка смещения {ci + 1}/{len(FD.CELLS)} {cls} {tf}: {len(specs)} конфигураций × {BIAS_REPS} повторов (синтетика)')
            saved = RB.bias_cell(ci, series, specs, lambda spec, syn, _tf=tf, _cls=cls: run_config(spec, syn, _tf, _cls, spreads, pairs, 0.0),
                                 lambda days, r: period_stats(days, r)['TRAIN']['t'], FD_trade_from(), SEED, reps=BIAS_REPS)
            kv_set(bkey, saved, d)
            series = None
        per_cell[ci] = saved
        await asyncio.sleep(0)
    vd = RB.verdict(per_cell, family_of)
    excluded = tuple(vd['excluded'])
    tests = enumerate_tests(ncls, excluded)
    N = len(tests)
    await say('проверка смещения: исключены ' + (', '.join(excluded) if excluded else 'нет') + f'; N = {N}')
    bias_lines = RB.report_section(vd, cell_names, BIAS_REPS, SEED)
    results = {}                                           # tid → статистика
    calib = {}
    tag = ','.join(sorted(excluded)) or '-'
    for ci, (cls, tf) in enumerate(FD.CELLS):
        key = f'cell:{REV}:{ci}:{tag}'
        saved = kv_get(key, d)
        if saved is None:
            series = cell_series(cls, tf, ctx)
            await say(f'ячейка {ci + 1}/{len(FD.CELLS)} {cls} {tf}: инструментов (сегментов) {len(series)}, баров {sum(S["n"] for S in series)}')
            if not series:
                return f'⛔ /factory: ячейка {cls} {tf}: нет данных'
            ncell = len(series)
            real, ent_freqs, durs_all = {}, [], []
            elig = eligible_bars(series)
            for cid, fam, spec in specs_for_cell(cls, tf, ncell, excluded):
                out = run_config(spec, series, tf, cls, spreads, pairs)
                real[tid(cls, tf, cid)] = period_stats(out['days'], out['r'])
                ne, durs = trade_stats(out['trades'], series, tf)
                if fam in ('T-MA', 'T-DC', 'T-MOM', 'R-RSI2', 'R-BB', 'B-VC', 'X-MOM'):          # статистика сделок — только библиотека (§2.41.9)
                    ent_freqs.append(ne / max(1, elig))
                    durs_all += durs
                await asyncio.sleep(0)
            p_entry = float(np.mean(ent_freqs)) if ent_freqs else 0.0
            rng = np.random.default_rng([SEED, ci, 999999])
            if len(durs_all) > DURS_KEEP:
                durs_all = list(rng.choice(durs_all, DURS_KEEP, replace=False))
            await say(f'  реальные конфигурации посчитаны (результаты скрыты до калибровки); частота входов {p_entry:.5f}, сделок в образце {len(durs_all)}; калибровка {CALIB_N}…')
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
    info = [f'Спред XAGUSD: {ctx["xag_src"]}.' if ctx.get('xag_src') else 'XAGUSD не участвует.']
    gp = gaps_of(d, ctx['now_ms'], ctx['avail'])
    info.append('Постоянные разрывы данных (сегментация §2.41.21): ' + ('; '.join(f'{k}: {", ".join(v)}' for k, v in gp.items()) if gp else 'нет') + '.')
    return build_report(ctx, tests, N, results, calib, ncls, excl, spreads, pairs, bias_lines, excluded, info)


def build_report(ctx, tests, N, results, calib, ncls, excl, spreads, pairs, bias_lines=(), excluded=(), info=()):
    d = ctx['fdir']
    R = [f'📈 <b>/factory — этап 2 «фабрики» (BOT_SPEC §2.41 + поправки §2.41.17, §2.41.19–21)</b> | код {ctx.get("version") or REV} | N = {N} | seed калибровки {SEED}',
         f'Инструменты по классам: {ncls}. Исключены инструменты: {excl or "нет"}. Исключены семейства (смещение): {", ".join(excluded) or "нет"}. Время: {datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC.']
    R += list(info)
    R.append('')
    R += list(bias_lines)
    R += ['', f'<b>КАЛИБРОВКА ({CALIB_N} случайных стратегий на ячейку, шаг A на TRAIN; порог ≤ 5% = ≤ {int(L.CALIB_MAX_FRAC * CALIB_N)} из {CALIB_N})</b>']
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
        R.append(f'  {ids[k]}: t(TRAIN+VALID) {r["TV"]["t"]:.2f}, mean {r["TV"]["mean"] * 1e4:.2f} б.п./день; TRAIN t {r["TRAIN"]["t"]:.2f}, VALID t {r["VALID"]["t"]:.2f}')
    kv_set('candidates', [ids[k] for k in cands], d)
    kv_set('excluded_families', list(excluded), d)
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
    R.append('')
    R.append('Оговорки: спреды/издержки — BOT_SPEC §2.41.17 (сценарий (б), перенос FIN_RATE_YR); данные до 2019-01-01 в расчётах не участвуют; B-VC — правило §2.41.19 (оба уровня = −1R); '
             'WTI в 2020-04 может иметь аномальные цены — сделки с px ≤ 0 пропускаются; фандинг CRYPTO — по кэшу Binance (нет данных — без фандинга).')
    return '\n'.join(R)


# ── /factory_decomp (шаг 0 поправки §2.41.23): декомпозиция доходности, только чтение ────────────────
def decomp_stats(r, mask_name):
    """По результату run_config(detail=True) и периоду: б.п./день капитала — net, до издержек (net + спред/комиссии + перенос), спред/комиссии, перенос, вклад фандинга, плечо (среднее и p95 по дням)."""
    lo, hi = E.day_bounds(mask_name)
    m = (r['days'] >= lo) & (r['days'] < hi)
    mu = r['multi']
    n = int(m.sum())
    if not n:
        return {'n': 0}
    net, side, swap, fund, ex = (mu['ret'][m].mean(), mu['c_side'][m].mean(), mu['c_swap'][m].mean(), mu['c_fund'][m].mean(), mu['expo'][m])
    k = 1e4
    return {'n': n, 'net': net * k, 'gross': (net + side + swap) * k, 'side': side * k, 'swap': swap * k, 'fund': -fund * k, 'lev': float(ex.mean()), 'lev95': float(np.percentile(ex, 95))}


async def run_decomp(params, progress=None):
    """/factory_decomp: для каждой ячейки и конфигурации (кроме исключённых семейств) на TRAIN и VALID — net, до издержек, вклад спреда/комиссий, переноса, фандинга и плечо по факту.
    ТОЛЬКО ЧТЕНИЕ: модель издержек не меняется (перенос по текущей модели), LOCKBOX и флаг финала не используются, решений по таблице не принимается."""
    ctx = build_ctx(params)
    d = ctx['fdir']
    probs, spreads, pairs, excl = preflight(ctx)
    if probs:
        return '⛔ /factory_decomp отказ: ' + '; '.join(probs)
    excluded = tuple(kv_get('excluded_families', d) or ())
    tag = ','.join(sorted(excluded)) or '-'

    async def say(m):
        if progress:
            await progress(m)
    rows = []
    for ci, (cls, tf) in enumerate(FD.CELLS):
        key = f'decomp:{NC_REV}:{ci}:{tag}'
        saved = kv_get(key, d)
        if saved is None:
            series = cell_series(cls, tf, ctx)
            if not series:
                return f'⛔ /factory_decomp: ячейка {cls} {tf}: нет данных'
            await say(f'декомпозиция {ci + 1}/{len(FD.CELLS)} {cls} {tf}: инструментов (сегментов) {len(series)}')
            saved = {}
            for cid, fam, spec in specs_for_cell(cls, tf, len(series), excluded):
                r = run_config(spec, series, tf, cls, spreads, pairs, detail=True)
                saved[cid] = {'TRAIN': decomp_stats(r, 'TRAIN'), 'VALID': decomp_stats(r, 'VALID')}
                await asyncio.sleep(0)
            kv_set(key, saved, d)
            series = None
        for cid, v in saved.items():
            rows.append((ci, cls, tf, cid, v))
    return build_decomp_report(ctx, rows, excluded)


def build_decomp_report(ctx, rows, excluded):
    f = lambda x: 'н/д' if x is None else f'{x:.2f}'
    R = [f'📊 <b>/factory_decomp — декомпозиция доходности (шаг 0, BOT_SPEC §2.41.23 п. 0)</b> | код {ctx.get("version") or NC_REV} | только чтение; решений по таблице не принимается',
         f'Исключённые семейства (смещение): {", ".join(excluded) or "нет"}. Модель издержек — как в /factory (перенос не-крипто FIN_RATE_YR = {E.fin_rate_yr():g}; спреды §2.41.17; CRYPTO: комиссия + проскальзывание, фандинг Binance). '
         'LOCKBOX и флаг финала не использовались.',
         'Единицы: б.п./день капитала (среднее по дням периода; дни без позиции входят нулями). net — после всех издержек; «до издержек» = net + спред/комиссии + перенос (фандинг остаётся внутри); '
         'фандинг — вклад в net (− = платили); плечо — суммарный номинал открытых позиций / капитал, среднее по дням (и 95-й перцентиль по дням).', '']
    R.append('ячейка | конфигурация | период | net | до издержек | спред/комиссии | перенос | фандинг | плечо ср | плечо p95')
    for ci, cls, tf, cid, v in rows:
        for name in ('TRAIN', 'VALID'):
            x = v[name]
            if not x.get('n'):
                R.append(f'{cls} {tf} | {cid} | {name} | нет данных')
                continue
            R.append(f'{cls} {tf} | {cid} | {name} | {f(x["net"])} | {f(x["gross"])} | {f(x["side"])} | {f(x["swap"])} | {f(x["fund"])} | {x["lev"]:.2f} | {x["lev95"]:.2f}')
    R.append('')
    R.append('<b>Сводка по ячейкам</b> (среднее по конфигурациям ячейки; описательно): ячейка | период | net | до издержек | спред/комиссии | перенос | фандинг | плечо ср')
    by = {}
    for ci, cls, tf, cid, v in rows:
        by.setdefault((ci, cls, tf), []).append(v)
    for (ci, cls, tf), vs in by.items():
        for name in ('TRAIN', 'VALID'):
            xs = [v[name] for v in vs if v[name].get('n')]
            if not xs:
                continue
            avg = lambda k: float(np.mean([x[k] for x in xs]))
            R.append(f'{cls} {tf} | {name} | {avg("net"):.2f} | {avg("gross"):.2f} | {avg("side"):.2f} | {avg("swap"):.2f} | {avg("fund"):.2f} | {avg("lev"):.2f}')
    return '\n'.join(R)


# ── /factory_nc (поправка §2.41.23): перезапуск ТОЛЬКО не-крипто ячеек ──────────────────────────────
def nc_cells():
    """Индексы ячеек не-крипто (CRYPTO не перезапускается)."""
    return [ci for ci, (cls, _tf) in enumerate(FD.CELLS) if cls != 'CRYPTO']


def nc_lags(tf):
    return NC_LAGS_1D if tf == '1D' else (5,)


def union_bh_note(tests, results, d, tag):
    """Справочно (без права отбора): шаг A при BH по объединению p-значений перезапуска и первого прогона (одни и те же тесты, BOT_SPEC §2.41.23 (в)). → строка | None."""
    p_new, p_old, ids = [], [], []
    for ci, cls, tf, cid, fam in tests:
        saved = kv_get(f'cell:{REV}:{ci}:{tag}', d)
        i = tid(cls, tf, cid)
        if not saved or i not in saved['real'] or i not in results:
            continue
        t_old, t_new = saved['real'][i]['TRAIN']['t'], results[i]['TRAIN']['t']
        p_new.append(E.p_one_sided(t_new))
        p_old.append(E.p_one_sided(t_old))
        ids.append(i)
    if not ids:
        return None
    rej = E.bh_reject(list(p_new) + list(p_old))
    k = len(ids)
    n_new = int(sum(1 for j in range(k) if rej[j] and results[ids[j]]['TRAIN']['t'] and results[ids[j]]['TRAIN']['t'] > 0))
    return f'Справочно: шаг A при BH по объединению двух прогонов ({2 * k} p-значений): прошло {n_new} из {k} тестов перезапуска (без права отбора).'


async def run_nc(params, progress=None):
    """/factory_nc: калибровка + шаги A/B для не-крипто ячеек по поправке §2.41.23: перенос 0.025 (диагностика — 0), 1D: t Ньюи–Уэста лаг 5 и 20, отбор по меньшему. CRYPTO и LOCKBOX не
    затрагиваются. Кандидаты (≤ 3) и сценарий издержек сохраняются в БД для /factory_final."""
    ctx = build_ctx(params)
    d = ctx['fdir']
    probs, spreads, _pairs, excl = preflight(ctx, need_pairs=False, need_crypto=False)
    if probs:
        return '⛔ /factory_nc отказ: ' + '; '.join(probs)
    ex_saved = kv_get('excluded_families', d)
    if ex_saved is None:
        return '⛔ /factory_nc отказ: нет результата проверки смещения — сначала /factory (фаза 0, §2.41.20)'
    excluded = tuple(ex_saved)
    tag = ','.join(sorted(excluded)) or '-'
    ncls = n_by_class(ctx)
    cells = nc_cells()
    tests = [t for t in enumerate_tests(ncls, excluded) if t[0] in cells]
    N = len(tests)

    async def say(m):
        if progress:
            await progress(m)
    await say(f'/factory_nc: N_nc = {N} тестов в {len(cells)} не-крипто ячейках, перенос {SWAP_NC}, исключены семейства: {", ".join(excluded) or "нет"}; rev {NC_REV}')
    results, diag, calib = {}, {}, {}
    for ci in cells:
        cls, tf = FD.CELLS[ci]
        key = f'nc:{NC_REV}:{ci}:{tag}'
        saved = kv_get(key, d)
        lags = nc_lags(tf)
        if saved is None:
            series = cell_series(cls, tf, ctx)
            if not series:
                return f'⛔ /factory_nc: ячейка {cls} {tf}: нет данных'
            await say(f'ячейка {cls} {tf}: инструментов (сегментов) {len(series)}, баров {sum(S["n"] for S in series)}')
            real, dg, ent_freqs, durs_all = {}, {}, [], []
            elig = eligible_bars(series)
            for cid, fam, spec in specs_for_cell(cls, tf, len(series), excluded):
                out = run_config(spec, series, tf, cls, spreads, [], swap_yr=SWAP_NC)
                real[tid(cls, tf, cid)] = period_stats(out['days'], out['r'], lags)
                ne, durs = trade_stats(out['trades'], series, tf)
                ent_freqs.append(ne / max(1, elig))
                durs_all += durs
                o0 = run_config(spec, series, tf, cls, spreads, [], swap_yr=0.0)                 # диагностика (перенос 0): без права отбора
                dg[tid(cls, tf, cid)] = period_stats(o0['days'], o0['r'], lags)
                await asyncio.sleep(0)
            p_entry = float(np.mean(ent_freqs)) if ent_freqs else 0.0
            rng = np.random.default_rng([SEED, ci, 999999])
            if len(durs_all) > DURS_KEEP:
                durs_all = list(rng.choice(durs_all, DURS_KEEP, replace=False))
            await say(f'  реальные конфигурации посчитаны (результаты скрыты до калибровки); калибровка {CALIB_N}…')
            cal = calibrate_cell(ci, cls, tf, series, spreads, p_entry, durs_all, n_rand=CALIB_N, progress=progress, swap_yr=SWAP_NC, lags=lags)
            cal.update({'p_entry': p_entry, 'durs_n': len(durs_all)})
            saved = {'real': real, 'diag': dg, 'calib': cal}
            kv_set(key, saved, d)
            series = None
            await say(f'  калибровка {cls} {tf}: прошло {cal["n_pass"]} из {cal["n"]} ({cal["frac"] * 100:.1f}%) — ' + ('ПРОВАЛ' if cal['fail'] else 'норма'))
        results.update(saved['real'])
        diag.update(saved['diag'])
        calib[ci] = saved['calib']
        if saved['calib']['fail']:
            break
    return build_nc_report(ctx, tests, N, results, diag, calib, excluded, tag, excl)


def build_nc_report(ctx, tests, N, results, diag, calib, excluded, tag, excl):
    d = ctx['fdir']
    R = [f'📈 <b>/factory_nc — перезапуск не-крипто ячеек (BOT_SPEC §2.41.23)</b> | код {ctx.get("version") or NC_REV} | N_nc = {N} | seed {SEED}',
         f'Перенос {SWAP_NC} годовых от номинала в обе стороны (было 0.06, §2.41.17); спреды §2.41.17 (XAGUSD: {ctx.get("xag_src")}); 1D: t Ньюи–Уэста лаг 5 и 20, отбор по меньшему; 4h/1h — лаг 5. '
         f'Исключены семейства: {", ".join(excluded) or "нет"}; исключены инструменты: {excl or "нет"}.',
         '⚠️ Поправка принята ПОСЛЕ просмотра результатов этих ячеек на TRAIN/VALID (пост-фактум изменение модели издержек): единственная нетронутая выборка — LOCKBOX (не открывался). '
         'Крипто-ячейки не перезапускались; LOCKBOX для CRYPTO считается скомпрометированным.', '',
         f'<b>КАЛИБРОВКА ({CALIB_N} случайных стратегий на ячейку, шаг A на TRAIN; порог ≤ 5% = ≤ {int(L.CALIB_MAX_FRAC * CALIB_N)} из {CALIB_N})</b>']
    failed = []
    for ci in nc_cells():
        cls, tf = FD.CELLS[ci]
        c = calib.get(ci)
        if c is None:
            R.append(f'  {cls} {tf}: не считалась')
            continue
        R.append(f'  {cls} {tf}: прошло {c["n_pass"]} из {c["n"]} ({c["frac"] * 100:.1f}%), частота входов {c["p_entry"]:.5f} — {"ПРОВАЛ" if c["fail"] else "норма"}')
        if c['fail']:
            failed.append(f'{cls} {tf}')
    if failed:
        R += ['', '<b>ВЕРДИКТ: ПРОЦЕДУРА ОТБОРА НЕНАДЁЖНА</b> — в ячейках ' + ', '.join(failed) + ' случайные стратегии проходят шаг A чаще 5%. Дальше не идём (§2.41.9); результаты реальных конфигураций не показываются.']
        return '\n'.join(R)
    ids = [tid(cls, tf, cid) for _ci, cls, tf, cid, _fam in tests]
    t_tr = [results[i]['TRAIN']['t'] if i in results else None for i in ids]
    pv = [E.p_one_sided(t) for t in t_tr]
    rej = E.bh_reject(pv)
    passA = [bool(rej[k] and (t_tr[k] or 0) > 0) for k in range(len(ids))]
    passB = []
    for k, i in enumerate(ids):
        v = results[i]['VALID'] if i in results else None
        passB.append(bool(passA[k] and v and v['mean'] > 0 and v['t'] is not None and v['t'] >= L.VALID_MIN_T))
    R += ['', f'<b>ШАГ A (TRAIN 2020–2023):</b> прошло {sum(passA)} из {N} (выбираемое t, p = 1 − Φ(t), Бенджамини–Хохберг q = {L.FDR_Q} по N_nc = {N}, t > 0).',
          f'<b>ШАГ B (VALID 2024):</b> прошло {sum(passB)} из {sum(passA)} (mean > 0 и t ≥ {L.VALID_MIN_T}).']
    note = union_bh_note(tests, results, d, tag)
    if note:
        R.append(note)
    cands = sorted([k for k in range(len(ids)) if passB[k]], key=lambda k: -(results[ids[k]]['TV']['t'] or -99))[:MAX_FINALISTS]
    R += ['', '<b>КАНДИДАТЫ НА ФИНАЛ</b> (не более 3, по выбираемому t на TRAIN+VALID): ' + ('нет — финал не запускается' if not cands else '')]
    for k in cands:
        r = results[ids[k]]
        R.append(f'  {ids[k]}: t(TRAIN+VALID) {r["TV"]["t"]:.2f}, mean {r["TV"]["mean"] * 1e4:.2f} б.п./день; TRAIN t {r["TRAIN"]["t"]:.2f}, VALID t {r["VALID"]["t"]:.2f}')
    kv_set('candidates', [ids[k] for k in cands], d)
    kv_set('candidates_scenario', {'swap_yr': SWAP_NC, 'source': 'nc', 'rev': NC_REV}, d)
    kv_set('nc_report_rev', NC_REV, d)
    if cands:
        R.append('Шаг C — ТОЛЬКО вручную: подтвердите список и запустите /factory_final confirm (один прогон, общий флаг финала, повтор запрещён).')
    R += ['', '<b>ТАБЛИЦА ВСЕХ N_nc ТЕСТОВ</b> (mean — б.п./день; t — выбираемое (1D: min лагов 5/20); «перенос 0» — диагностика БЕЗ права отбора)',
          'ячейка | конфигурация | mean TRAIN | t TRAIN (t5/t20) | p | mean VALID | t VALID | A | B | перенос 0: mean TRAIN / t TRAIN']
    fm = lambda x, nd=2: ('н/д' if x is None else f'{x:.{nd}f}')
    for k, (ci, cls, tf, cid, fam) in enumerate(tests):
        r, g = results.get(ids[k]), diag.get(ids[k])
        if not r:
            R.append(f'{cls} {tf} | {cid} | нет данных')
            continue
        tl = r['TRAIN'].get('tl')
        tl_s = f'{fm(r["TRAIN"]["t"])} ({fm(tl["5"])}/{fm(tl["20"])})' if tl else fm(r['TRAIN']['t'])
        dg_s = f'{g["TRAIN"]["mean"] * 1e4:.2f} / {fm(g["TRAIN"]["t"])}' if g else 'н/д'
        R.append(f'{cls} {tf} | {cid} | {r["TRAIN"]["mean"] * 1e4:.2f} | {tl_s} | {pv[k]:.4f} | {r["VALID"]["mean"] * 1e4:.2f} | {fm(r["VALID"]["t"])} | '
                 f'{"да" if passA[k] else "нет"} | {"да" if passB[k] else "нет"} | {dg_s}')
    return '\n'.join(R)


# ── /factory_final (шаг C) ───────────────────────────────────────────────────────────────────────────
def lockbox_metrics(days, r, periods, days_s, r_s):
    """Метрики LOCKBOX по базовому (days, r) и стресс-прогону (days_s, r_s: издержки ×2). Стресс без совпадающих дней — не пройдено (stress_mean = nan)."""
    lo = E.day_bounds('LOCKBOX')[0]
    x = r[days >= lo]
    xs = r_s[days_s >= lo]
    mean, t, n = E.nw_t(x)
    t20 = E.nw_t(x, 20)[1]                                                     # справочно (критерий §2.41.7 — лаг 5)
    ms = float(xs.mean()) if len(xs) and len(xs) == len(x) else float('nan')
    sh = E.sharpe(x, periods)
    dd = E.max_drawdown(x)
    ok = (n > 0 and math.isfinite(sh) and sh >= L.FINAL_MIN_SHARPE and mean > 0 and math.isfinite(t) and t >= L.FINAL_MIN_T and dd <= L.FINAL_MAX_DD and math.isfinite(ms) and ms > 0)
    return {'n': n, 'mean': mean, 't': (None if not math.isfinite(t) else t), 'sharpe': (None if not math.isfinite(sh) else sh), 'dd': dd, 'stress_mean': ms, 'pass': bool(ok), 't20': (None if not math.isfinite(t20) else t20)}


def fed_rate_avg(days):
    """Средняя ставка ФРС за LOCKBOX-период по таблице ENV FED_RATE_BY_YEAR (формат `2025:0.043,2026:0.040`; источник записывает владелец, BOT_SPEC §2.41.23 п. 5): среднее по дням ряда
    LOCKBOX. → (ставка | None, комментарий). Нет таблицы или нет года — None (проверка не выполняется; ставок в коде нет)."""
    raw = os.getenv('FED_RATE_BY_YEAR', '').strip()
    if not raw:
        return None, 'не выполнена: ставка ФРС не задана (ENV FED_RATE_BY_YEAR)'
    try:
        table = {int(k): float(v) for k, v in (x.split(':') for x in raw.split(',') if x.strip())}
    except ValueError:
        return None, 'не выполнена: FED_RATE_BY_YEAR не разобрана'
    lo = E.day_bounds('LOCKBOX')[0]
    years = [int(datetime.fromtimestamp(int(dd) * E.DAY_MS / 1000, timezone.utc).year) for dd in days[days >= lo]]
    if not years:
        return None, 'не выполнена: нет дней LOCKBOX'
    miss = sorted({y for y in years if y not in table})
    if miss:
        return None, f'не выполнена: в FED_RATE_BY_YEAR нет годов {miss}'
    return float(np.mean([table[y] for y in years])), f'средняя ставка за период {np.mean([table[y] for y in years]) * 100:.2f}% годовых (ENV)'


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
    crypto = any(parse_tid(i)[0] == 'CRYPTO' for i in ids)                       # крипто-кандидатам нужны пары и кэш Binance; не-крипто (/factory_nc) — нет
    probs, spreads, pairs, excl = preflight(ctx, need_pairs=crypto, need_crypto=crypto)
    if probs:
        return '⛔ /factory_final отказ: ' + '; '.join(probs) + ' (флаг финала НЕ взят)'
    sc = (kv_get('candidates_scenario', d) or {}).get('swap_yr')                  # сценарий переноса кандидатов (None — модель §2.41.17: ENV FIN_RATE_YR)
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
        base = run_config(spec, series, tf, cls, spreads, pairs, 1.0, swap_yr=sc)
        stress = run_config(spec, series, tf, cls, spreads, pairs, 2.0, swap_yr=sc)                 # ×2 включает перенос (0.025 → 5% годовых)
        periods = 365 if cls == 'CRYPTO' else 252
        met = lockbox_metrics(base['days'], base['r'], periods, stress['days'], stress['r'])
        met['fed'] = None
        if cls != 'CRYPTO' and sc is not None:                                                       # дополнительная проверка (справочно): перенос = sc + средняя ставка ФРС за период
            fed, fed_note = fed_rate_avg(base['days'])
            met['fed_note'] = fed_note
            if fed is not None:
                fr = run_config(spec, series, tf, cls, spreads, pairs, 1.0, swap_yr=sc + fed)
                lo = E.day_bounds('LOCKBOX')[0]
                xf = fr['r'][fr['days'] >= lo]
                met['fed'] = {'swap_yr': sc + fed, 'mean': float(xf.mean()) if len(xf) else None}
        rows.append((t, met))
        if progress:
            await progress(f'финал {t}: готово')
        await asyncio.sleep(0)
    lb_gaps = {}
    for t in ids:
        cls = parse_tid(t)[0]
        for k in ([] if cls == 'CRYPTO' else [k for k in FD.CLASS_MEMBERS[cls] if k in ctx['avail']]):
            g = FD.gaps_in_period(d, k, L.lockbox_start_ms())
            if g:
                lb_gaps[k] = g
    kv_set('final_result', {'rows': rows, 'rev': REV, 'lockbox_gaps': lb_gaps}, d)
    kv_set('final_done', True, d)
    R = [f'🔒 <b>/factory_final — шаг C на LOCKBOX (2025+), единственный прогон</b> | код {ctx.get("version") or REV}', 'Критерии: Шарп ≥ 0.5, mean > 0 при t ≥ 2.0 (лаг 5), макс. просадка ≤ 15%, при издержках ×2 (включая перенос ×2) mean > 0.',
         f'Сценарий переноса кандидатов: {("%.3f годовых (поправка §2.41.23)" % sc) if sc is not None else "FIN_RATE_YR (§2.41.17)"}.', '']
    for t, m in rows:
        t_s = 'н/д' if m['t'] is None else '%.2f' % m['t']
        sh_s = 'н/д' if m['sharpe'] is None else '%.2f' % m['sharpe']
        st_s = 'н/д' if not math.isfinite(m['stress_mean']) else '%.2f' % (m['stress_mean'] * 1e4)
        verdict = 'ПРОЙДЕНО' if m['pass'] else 'НЕ ПРОЙДЕНО'
        R.append('%s: дней %d, mean %.2f б.п./день, t %s (лаг 20: %s), Шарп %s, просадка %.1f%%, mean при издержках ×2 %s б.п./день — %s' %
                 (t, m['n'], m['mean'] * 1e4, t_s, 'н/д' if m.get('t20') is None else '%.2f' % m['t20'], sh_s, m['dd'] * 100, st_s, verdict))
        if m.get('fed_note'):
            R.append('    доп. проверка с переносом 0.025 + ФРС (справочно, не критерий): ' + (m['fed_note'] if not m.get('fed') else
                     '%s; перенос %.2f%% годовых → mean %s б.п./день' % (m['fed_note'], m['fed']['swap_yr'] * 100, 'н/д' if m['fed']['mean'] is None else '%.2f' % (m['fed']['mean'] * 1e4))))
    R.append('Постоянные разрывы данных в LOCKBOX-периоде (§2.41.21): ' + ('; '.join(f'{k}: {", ".join(v)}' for k, v in lb_gaps.items()) if lb_gaps else 'нет') + '.')
    R.append('')
    R.append('Решение (BOT_SPEC §2.41.10): прошедший финалист → shadow-форвард (отдельный пре-коммит); ни один → поиск простых систематических стратегий завершён для всех классов.')
    return '\n'.join(R)
