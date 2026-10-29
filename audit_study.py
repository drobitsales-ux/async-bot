"""audit_study.py — поиск по данным внешнего источника сигналов (BOT_SPEC §2.45, пре-регистрация записана ДО кода): «его вход + наш выход» (12 вариантов), портрет метода автора, признаки полных стопов,
финал на HOLDOUT. DISCOVERY = первые 60% сигналов по времени публикации, HOLDOUT = остальные; сигналы и цены HOLDOUT читаются только функцией, требующей флага `signal_final_started` (как LOCKBOX).
Расчёты — в дочернем процессе research_runner. Ядро входа/выхода/издержек — audit_sim (§2.44), здесь не дублируется."""
import json
import math
import os
import time

import numpy as np

import audit_db as AD
import audit_sim as AS

MIN, H, DAY = AS.MIN, 3_600_000, AS.DAY
SPLIT_FRAC = 0.6
BH_Q = 0.05
FINAL_MIN_N, FINAL_MIN_T, FINAL_MIN_PF = 150, 2.5, 1.3
MAX_CANDS, MAX_FILTERS, MAX_FILTER_FEATS = 3, 3, 2
Q_LO, Q_HI = 0.2, 0.8
NEAR_ATR = 1.0                              # порог «у экстремума» в портрете (§2.45.6 п. 5)
TURN_DAYS = 30
STOPS = (('P2', 'pct', 0.02), ('P3', 'pct', 0.03), ('P5', 'pct', 0.05), ('P8', 'pct', 0.08), ('A1.5', 'atr', 1.5), ('A3', 'atr', 3.0))
EXITS = ('T', 'R2')                          # T — его цели долями + безубыток после первой; R2 — фикс. TP = 2R от нашего стопа
VARIANTS = tuple(f'{s[0]}|{e}' for s in STOPS for e in EXITS)
K_SPLIT, K_CANDS, K_FLAG, K_DONE, K_RESULT = 'sigstudy_split', 'sigstudy_cands', 'signal_final_started', 'signal_final_done', 'signal_final_result'
RAW_FEATURES = ('ret4h', 'ret24h', 'ret7d', 'hi24', 'lo24', 'hi7', 'lo7', 'rsi', 'atr_pct', 'vol_ratio', 'btc_ret24h', 'btc_slope', 'hour', 'dow', 'stop_pct', 'tp1_pct', 'n_sig_1h', 'same_dir_share',
                'funding', 'turn_rank')
FEATURES = ('ret4h_s', 'ret24h_s', 'ret7d_s', 'near24', 'far24', 'near7d', 'far7d', 'rsi_s', 'atr_pct', 'vol_ratio', 'btc_ret24h_s', 'btc_slope_s', 'hour', 'dow', 'stop_pct', 'tp1_pct',
            'n_sig_1h', 'same_dir_share', 'funding_s', 'turn_rank')
LABELS = {'ret4h_s': 'доходность 4h (в сторону сделки)', 'ret24h_s': 'доходность 24h (в сторону сделки)', 'ret7d_s': 'доходность 7d (в сторону сделки)',
          'near24': 'до ближнего экстремума 24h, ATR', 'far24': 'до дальнего экстремума 24h, ATR', 'near7d': 'до ближнего экстремума 7d, ATR', 'far7d': 'до дальнего экстремума 7d, ATR',
          'rsi_s': 'RSI14 1h (лонг: RSI, шорт: 100−RSI)', 'atr_pct': 'ATR% 1h', 'vol_ratio': 'объём часа / медиана 24h', 'btc_ret24h_s': 'BTC 24h (в сторону сделки)',
          'btc_slope_s': 'наклон EMA50 4h BTC (в сторону сделки)', 'hour': 'час UTC', 'dow': 'день недели UTC', 'stop_pct': 'его стоп, % от входа', 'tp1_pct': 'его цель 1, % от входа',
          'n_sig_1h': 'сигналов канала за прошлый час', 'same_dir_share': 'доля в том же направлении', 'funding_s': 'фандинг (положительный = плата за сторону сделки)', 'turn_rank': 'ранг оборота 30 сут'}
RAW_LABELS = {'ret4h': 'доходность 4h', 'ret24h': 'доходность 24h', 'ret7d': 'доходность 7d', 'hi24': 'до макс. 24h, ATR', 'lo24': 'до мин. 24h, ATR', 'hi7': 'до макс. 7d, ATR', 'lo7': 'до мин. 7d, ATR',
              'rsi': 'RSI14 1h', 'atr_pct': 'ATR% 1h', 'vol_ratio': 'объём часа / медиана 24h', 'btc_ret24h': 'BTC 24h', 'btc_slope': 'наклон EMA50 4h BTC', 'hour': 'час UTC', 'dow': 'день недели',
              'stop_pct': 'его стоп, %', 'tp1_pct': 'его цель 1, %', 'n_sig_1h': 'сигналов за прошлый час', 'same_dir_share': 'доля в том же направлении', 'funding': 'фандинг', 'turn_rank': 'ранг оборота'}
NAN = float('nan')


# ── статистика ──────────────────────────────────────────────────────────────────────────────────────
def p_from_t(t):
    """Двусторонний p по нормальному приближению; нет t → 1."""
    return 1.0 if t is None or not math.isfinite(t) else math.erfc(abs(t) / math.sqrt(2.0))


def bh(pvals, q=BH_Q):
    """Бенджамини–Хохберг: список флагов «отвергнута» в исходном порядке."""
    m = len(pvals)
    order = sorted(range(m), key=lambda i: pvals[i])
    k = 0
    for rank, i in enumerate(order, 1):
        if pvals[i] <= q * rank / m:
            k = rank
    out = [False] * m
    for rank, i in enumerate(order, 1):
        if rank <= k:
            out[i] = True
    return out


def rank_avg(a):
    a = np.asarray(a, float)
    order = np.argsort(a, kind='mergesort')
    sa = a[order]
    ranks = np.empty(len(a))
    i = 0
    while i < len(a):
        j = i
        while j + 1 < len(a) and sa[j + 1] == sa[i]:
            j += 1
        ranks[order[i:j + 1]] = (i + j) / 2.0 + 1.0
        i = j + 1
    return ranks


def mannwhitney_p(x, y):
    """Манн–Уитни, двусторонний, нормальное приближение с поправкой на связки и непрерывность. → (U1, p); мало данных → (nan, 1)."""
    x = np.asarray(x, float)
    y = np.asarray(y, float)
    x, y = x[np.isfinite(x)], y[np.isfinite(y)]
    n1, n2 = len(x), len(y)
    if n1 < 3 or n2 < 3:
        return NAN, 1.0
    allv = np.concatenate([x, y])
    r = rank_avg(allv)
    u1 = float(r[:n1].sum() - n1 * (n1 + 1) / 2.0)
    n = n1 + n2
    _, cnt = np.unique(allv, return_counts=True)
    tie = float(((cnt ** 3) - cnt).sum()) / (n * (n - 1.0))
    var = n1 * n2 / 12.0 * ((n + 1.0) - tie)
    if var <= 0:
        return u1, 1.0
    z = max(0.0, abs(u1 - n1 * n2 / 2.0) - 0.5) / math.sqrt(var)
    return u1, float(math.erfc(z / math.sqrt(2.0)))


def stats_of(items):
    """items: [(exit_ts_ms, R)] → n, WR, средние выигрыш/проигрыш в R, mean R, PF, t по сигналам и по дням (Ньюи–Уэст лаг 5, §2.44), p по t дням."""
    items = [(t, r) for t, r in items if math.isfinite(r)]
    rs = [r for _t, r in items]
    st = AS.r_stats(rs)
    arr = np.asarray(rs, float)
    td, nd = AS.daily_t(items)
    return {'n': st['n'], 'wr': st['wr'], 'w_avg': (float(arr[arr > 0].mean()) if st['n'] and (arr > 0).any() else 0.0), 'l_avg': (float(-arr[arr < 0].mean()) if st['n'] and (arr < 0).any() else 0.0),
            'mean': st['mean'], 'pf': st['pf'], 't': st['t'], 't_day': td, 'n_days': nd, 'p': p_from_t(td)}


# ── границы DISCOVERY / HOLDOUT и флаг финала ───────────────────────────────────────────────────────
def meta_set_if_absent(con, k, v):
    cur = con.execute('INSERT OR IGNORE INTO audit_meta (k, v) VALUES (?, ?)', (k, v if isinstance(v, str) else json.dumps(v, ensure_ascii=False)))
    con.commit()
    return cur.rowcount == 1


def freeze_split(con):
    """Граница по времени публикации по ВСЕМ сигналам независимо от исхода; замораживается при первом вызове. → dict(t_split, t_end, n_total, n_disc) | None (мало сигналов)."""
    v = AD.meta_get(con, K_SPLIT)
    if v:
        return json.loads(v)
    ts = [r[0] for r in con.execute('SELECT pub_ts FROM audit_signals ORDER BY pub_ts, id')]
    if len(ts) < 20:
        return None
    k = int(math.floor(SPLIT_FRAC * len(ts)))
    sp = {'t_split': int(ts[k]), 't_end': int(ts[-1]), 'n_total': len(ts), 'n_disc': sum(1 for t in ts if t < ts[k]), 'frozen_at': int(time.time())}
    meta_set_if_absent(con, K_SPLIT, sp)
    return json.loads(AD.meta_get(con, K_SPLIT))


_COLS = 'channel, msg_id, source, pub_ts, symbol, side, leverage, entry_lo, entry_hi, targets, stop'


def _sig(r):
    return {'channel': r[0], 'msg_id': r[1], 'source': r[2], 'pub_ts': r[3], 'symbol': r[4], 'side': r[5], 'leverage': r[6], 'entry_lo': r[7], 'entry_hi': r[8], 'targets': json.loads(r[9]), 'stop': r[10]}


def load_discovery(con, split):
    """Сигналы DISCOVERY (pub_ts < t_split). Строки HOLDOUT этим запросом не выбираются."""
    return [_sig(r) for r in con.execute(f'SELECT {_COLS} FROM audit_signals WHERE pub_ts < ? ORDER BY pub_ts, id', (split['t_split'],))]


def final_flag(con):
    return AD.meta_get(con, K_FLAG) or AD.meta_get(con, K_DONE)


def load_holdout(con, split):
    """Сигналы HOLDOUT — ТОЛЬКО после взятия флага финала (как LOCKBOX)."""
    if not AD.meta_get(con, K_FLAG):
        raise PermissionError('HOLDOUT закрыт: флаг signal_final_started не взят')
    return [_sig(r) for r in con.execute(f'SELECT {_COLS} FROM audit_signals WHERE pub_ts >= ? AND pub_ts <= ? ORDER BY pub_ts, id', (split['t_split'], split['t_end']))]


def context_light(con, split, upto_holdout):
    """(канал, pub_ts, сторона) для счётчиков «за прошлый час»: DISCOVERY; с флагом — и HOLDOUT до t_end (только прошлое относительно каждого сигнала используется в признаке)."""
    if upto_holdout:
        if not AD.meta_get(con, K_FLAG):
            raise PermissionError('HOLDOUT закрыт: флаг signal_final_started не взят')
        q, a = 'SELECT channel, pub_ts, side FROM audit_signals WHERE pub_ts <= ? ORDER BY pub_ts', (split['t_end'],)
    else:
        q, a = 'SELECT channel, pub_ts, side FROM audit_signals WHERE pub_ts < ? ORDER BY pub_ts', (split['t_split'],)
    return [(r[0], r[1], r[2]) for r in con.execute(q, a)]


# ── признаки на момент сигнала (только данные ДО публикации) ────────────────────────────────────────
def wilder(x, n):
    """Сглаживание Уайлдера: первое значение (индекс n−1) — SMA первых n, дальше RMA. → массив длины len(x) (до n−1 — nan)."""
    x = np.asarray(x, float)
    out = np.full(len(x), NAN)
    if len(x) < n:
        return out
    out[n - 1] = x[:n].mean()
    for i in range(n, len(x)):
        out[i] = (out[i - 1] * (n - 1) + x[i]) / n
    return out


def atr14(h, l, c):
    pc = c[:-1]
    tr = np.maximum(h[1:] - l[1:], np.maximum(np.abs(h[1:] - pc), np.abs(l[1:] - pc)))
    return wilder(tr, 14)


def rsi14(c):
    d = np.diff(c)
    up, dn = wilder(np.maximum(d, 0.0), 14), wilder(np.maximum(-d, 0.0), 14)
    if not len(up) or not math.isfinite(up[-1]) or not math.isfinite(dn[-1]):
        return NAN
    return 100.0 if dn[-1] == 0 else float(100.0 - 100.0 / (1.0 + up[-1] / dn[-1]))


def _close_at(b, ts):
    i = int(np.searchsorted(b['ts'], ts))
    return float(b['c'][i]) if i < len(b['ts']) and int(b['ts'][i]) == ts else NAN


def _window(b, lo_ts, hi_ts, need):
    i0, i1 = int(np.searchsorted(b['ts'], lo_ts, side='left')), int(np.searchsorted(b['ts'], hi_ts, side='right'))
    return (i0, i1) if i1 - i0 >= need else None


def hourly_features(b, pub_ms):
    """Признаки по часовым барам b (ts,o,h,l,c,v): используются ТОЛЬКО бары с open ≤ floor(pub,1h) − 1h (закрыты до публикации). → dict (ключи RAW_FEATURES части 'рынок' + atr)."""
    t_h = pub_ms // H * H
    last = t_h - H
    n = int(np.searchsorted(b['ts'], last, side='right'))
    out = {k: NAN for k in ('ret4h', 'ret24h', 'ret7d', 'hi24', 'lo24', 'hi7', 'lo7', 'rsi', 'atr_pct', 'vol_ratio', 'atr', 'close')}
    if n < 20 or int(b['ts'][n - 1]) != last:
        return out
    lo = max(0, n - 240)
    h, l, c, v, ts = (b[k][lo:n] for k in ('h', 'l', 'c', 'v', 'ts'))
    cl = float(c[-1])
    a = atr14(h, l, c)
    atr = float(a[-1]) if len(a) and math.isfinite(a[-1]) else NAN
    out.update({'close': cl, 'atr': atr, 'rsi': rsi14(c)})
    for key, k in (('ret4h', 4), ('ret24h', 24), ('ret7d', 168)):
        p = _close_at(b, last - k * H)
        out[key] = cl / p - 1.0 if math.isfinite(p) and p > 0 else NAN
    if math.isfinite(atr) and atr > 0:
        out['atr_pct'] = atr / cl
        for key_h, key_l, k in (('hi24', 'lo24', 24), ('hi7', 'lo7', 168)):
            w = _window(b, last - (k - 1) * H, last, int(0.9 * k))
            if w:
                out[key_h] = (float(b['h'][w[0]:w[1]].max()) - cl) / atr
                out[key_l] = (cl - float(b['l'][w[0]:w[1]].min())) / atr
    w = _window(b, last - 23 * H, last, 22)
    if w:
        med = float(np.median(b['v'][w[0]:w[1]]))
        out['vol_ratio'] = float(v[-1]) / med if med > 0 else NAN
    return out


def btc_features(b, pub_ms):
    """BTC: доходность 24h и наклон EMA50(4h) за 6 закрытых 4h-баров (%/100). Только закрытые бары до публикации."""
    out = {'btc_ret24h': NAN, 'btc_slope': NAN}
    if b is None:
        return out
    t_h = pub_ms // H * H
    last = t_h - H
    n = int(np.searchsorted(b['ts'], last, side='right'))
    if n < 30:
        return out
    c1, c0 = _close_at(b, last), _close_at(b, last - 24 * H)
    if math.isfinite(c1) and math.isfinite(c0) and c0 > 0:
        out['btc_ret24h'] = c1 / c0 - 1.0
    ts, cl = b['ts'][:n], b['c'][:n]
    grp = ts // (4 * H)
    closes = []
    for g in np.unique(grp[-6 * 130:]):
        idx = np.flatnonzero(grp == g)
        if len(idx) == 4 and int(ts[idx[0]]) % (4 * H) == 0 and (g + 1) * 4 * H <= t_h:
            closes.append(float(cl[idx[-1]]))
    if len(closes) < 60:
        return out
    ema, al = closes[0], 2.0 / 51.0
    hist = []
    for x in closes:
        ema = ema + al * (x - ema)
        hist.append(ema)
    if hist[-7] > 0:
        out['btc_slope'] = hist[-1] / hist[-7] - 1.0
    return out


def make_turn(b):
    """Оборот в USDT по часам (Σ объём·close) как накопленная сумма на сетке часов: (t0, cum), cum[i] = оборот баров с индексом < i."""
    t0 = int(b['ts'][0]) // H * H
    nb = int((int(b['ts'][-1]) - t0) // H) + 1
    q = np.zeros(nb)
    q[((b['ts'] - t0) // H).astype(int)] = b['v'] * b['c']
    return t0, np.concatenate([[0.0], np.cumsum(q)]), int(b['ts'][0])


def turnover30(turn, pub_ms):
    """Оборот за 30 суток закрытых баров [floor(pub,1h) − 30 сут, floor(pub,1h)); история короче 28 суток → nan."""
    if turn is None:
        return NAN
    t0, cum, first = turn
    t_h = pub_ms // H * H
    a = t_h - TURN_DAYS * 24 * H
    if first > a + 2 * DAY or t_h > t0 + (len(cum) - 1) * H + H:
        return NAN
    i0 = min(max(int((a - t0) // H), 0), len(cum) - 1)
    i1 = min(max(int((t_h - t0) // H), 0), len(cum) - 1)
    return float(cum[i1] - cum[i0])


def turn_rank(sym, pub_ms, turns, universe):
    """1 + число символов универсума U (кроме самого) с БОЛЬШИМ оборотом на тот же момент (§2.45.6 п. 6)."""
    mine = turnover30(turns.get(sym), pub_ms)
    if not math.isfinite(mine):
        return NAN
    n = 0
    for u in universe:
        if u != sym:
            t = turnover30(turns.get(u), pub_ms)
            if math.isfinite(t) and t > mine:
                n += 1
    return float(n + 1)


def last_funding(fund, pub_ms):
    if fund is None or not len(fund[0]):
        return NAN
    i = int(np.searchsorted(fund[0], pub_ms, side='right')) - 1
    return float(fund[1][i]) if i >= 0 else NAN


def channel_counts(ctx, channel, side, pub_s):
    """Сигналы того же канала в прошлом часе [pub − 1h, pub) (сам сигнал не считается, §2.45.6 п. 1): (число, доля в том же направлении | nan)."""
    n = same = 0
    for ch, t, sd in ctx:
        if ch == channel and pub_s - 3600 <= t < pub_s:
            n += 1
            same += 1 if sd == side else 0
    return n, (same / n if n else NAN)


def signal_features(sig, hf, btcf, ctx, turns, universe, fund):
    pub_ms = sig['pub_ts'] * 1000
    level = sig['entry_lo'] if sig['side'] == 'short' else sig['entry_hi']
    n1, share = channel_counts(ctx, sig['channel'], sig['side'], sig['pub_ts'])
    f = {k: hf.get(k, NAN) for k in ('ret4h', 'ret24h', 'ret7d', 'hi24', 'lo24', 'hi7', 'lo7', 'rsi', 'atr_pct', 'vol_ratio')}
    f.update(btcf)
    f.update({'hour': float(sig['pub_ts'] // 3600 % 24), 'dow': float((sig['pub_ts'] // 86400 + 3) % 7),            # 0 = понедельник (1970-01-01 — четверг)
              'stop_pct': abs(sig['stop'] - level) / level * 100.0, 'tp1_pct': abs(sig['targets'][0] - level) / level * 100.0, 'n_sig_1h': float(n1), 'same_dir_share': share,
              'funding': last_funding(fund, pub_ms), 'turn_rank': turn_rank(sig['symbol'], pub_ms, turns, universe)})
    return f


def signed_features(f, side):
    """Признаки «в сторону сделки» (§2.45.6 п. 2) — вход части 3."""
    s = 1.0 if side == 'long' else -1.0
    long_ = side == 'long'
    g = lambda k: f.get(k, NAN)
    return {'ret4h_s': s * g('ret4h'), 'ret24h_s': s * g('ret24h'), 'ret7d_s': s * g('ret7d'),
            'near24': g('lo24') if long_ else g('hi24'), 'far24': g('hi24') if long_ else g('lo24'), 'near7d': g('lo7') if long_ else g('hi7'), 'far7d': g('hi7') if long_ else g('lo7'),
            'rsi_s': g('rsi') if long_ else 100.0 - g('rsi'), 'atr_pct': g('atr_pct'), 'vol_ratio': g('vol_ratio'), 'btc_ret24h_s': s * g('btc_ret24h'), 'btc_slope_s': s * g('btc_slope'),
            'hour': g('hour'), 'dow': g('dow'), 'stop_pct': g('stop_pct'), 'tp1_pct': g('tp1_pct'), 'n_sig_1h': g('n_sig_1h'), 'same_dir_share': g('same_dir_share'),
            'funding_s': s * g('funding'), 'turn_rank': g('turn_rank')}


# ── симуляция: его вход + наш выход ─────────────────────────────────────────────────────────────────
def _rec(R, w, fill_ts):
    return {'R': R, 'closed': w['closed'], 'kinds': [x[0] for x in w['fills']], 'exit_ts': max((x[3] for x in w['fills']), default=fill_ts)}


def _fslice(funding, fill_ts):
    if funding is None or not len(funding[0]):
        return None
    i0, i1 = np.searchsorted(funding[0], fill_ts, side='right'), np.searchsorted(funding[0], fill_ts + AS.HOLD_MS, side='right')
    return (funding[0][i0:i1], funding[1][i0:i1])


def sim_signal(sig, c, fees, now_ms, atr, funding=None):
    """Один сигнал: модель B (его стоп/цели), 12 вариантов «наш стоп × выход», обратная торговля (справочно), MAE до первой цели. Вход и цены — как в audit_sim."""
    if c is None or len(c['ts']) == 0:
        return {'status': 'no_data'}
    side, pub_ms = sig['side'], sig['pub_ts'] * 1000
    i_fill, level = AS.find_entry(c, pub_ms, side, sig['entry_lo'], sig['entry_hi'])
    if i_fill is None:
        return {'status': 'pending' if now_ms < pub_ms + AS.ENTRY_WINDOW_MS else 'not_filled'}
    fill_ts = int(c['ts'][i_fill])
    fund = _fslice(funding, fill_ts)
    d = 1.0 if side == 'long' else -1.0
    tg = list(sig['targets'])
    out = {'status': 'filled', 'fill_ts': fill_ts, 'level': level, 'V': {}}
    wb = AS.walk(side, c, i_fill, level, tg, sig['stop'], None, now_ms)
    sd = abs(sig['stop'] - level) / level
    out['B'] = _rec(AS.unit_pnl(side, level, wb['fills'], fees, fund, fill_ts) / sd if sd > 0 else NAN, wb, fill_ts)
    for name, kind, par in STOPS:
        dist = level * par if kind == 'pct' else (par * atr if atr is not None and math.isfinite(atr) and atr > 0 else None)
        if dist is None or dist >= level * 0.95 or dist <= 0:
            continue
        stop_px, sdv = level - d * dist, dist / level
        for ex in EXITS:
            targets = tg if ex == 'T' else [level + d * 2.0 * dist]
            w = AS.walk(side, c, i_fill, level, targets, stop_px, None, now_ms)
            out['V'][f'{name}|{ex}'] = _rec(AS.unit_pnl(side, level, w['fills'], fees, fund, fill_ts) / sdv, w, fill_ts)
    rside = 'short' if side == 'long' else 'long'                                    # обратная торговля: справочно, в отбор не входит
    rstop, rtgt = tg[0], sig['stop']
    ok = (rstop < level < rtgt) if rside == 'long' else (rtgt < level < rstop)
    if ok:
        w = AS.walk(rside, c, i_fill, level, [rtgt], rstop, None, now_ms)
        out['REV'] = _rec(AS.unit_pnl(rside, level, w['fills'], fees, fund, fill_ts) / (abs(rstop - level) / level), w, fill_ts)
    ts, hh, ll = c['ts'], c['h'], c['l']
    last = min(len(ts), int(np.searchsorted(ts, ts[i_fill] + AS.HOLD_MS, side='left')))
    hit = np.flatnonzero((ll[i_fill + 1:last] <= tg[0]) if side == 'short' else (hh[i_fill + 1:last] >= tg[0]))
    j = i_fill + 1 + int(hit[0]) if len(hit) else last - 1
    adv = (float(hh[i_fill:j + 1].max()) - level) if side == 'short' else (level - float(ll[i_fill:j + 1].min()))
    out['MAE'] = {'pct': max(0.0, adv / level * 100.0), 'reached': bool(len(hit))}
    return out


# ── источник цен: к audit_sim.PriceSource добавлены часовые бары и история фандинга ─────────────────
class StudySource(AS.PriceSource):
    BLOCK = 1000                                              # баров 1h в блоке кэша (≈ 41.7 суток)

    async def _bingx_hblock(self, sym, k, now_ms):
        p = self._cpath('bingx1h', sym, k)
        if os.path.exists(p):
            with np.load(p) as z:
                return {kk: z[kk] for kk in z.files}
        t0, t1 = k * self.BLOCK * H, (k + 1) * self.BLOCK * H
        rows, since = [], t0
        while since < min(t1, now_ms):
            got = await self.ex.fetch_ohlcv(sym, '1h', since, self.BLOCK)
            if not got:
                break
            rows.extend(r for r in got if t0 <= r[0] < t1)
            nxt = got[-1][0] + H
            if nxt <= since:
                break
            since = nxt
        if not rows:
            return None
        a = np.array(sorted({int(r[0]): r for r in rows}.values()), float)
        d = {'ts': a[:, 0].astype(np.int64), 'o': a[:, 1], 'h': a[:, 2], 'l': a[:, 3], 'c': a[:, 4], 'v': a[:, 5]}
        if t1 <= now_ms - 2 * H:
            np.savez_compressed(p, **d)
        return d

    async def hourly(self, base, start_ms, end_ms, ref_price, now_ms):
        """Часовые бары BingX [start, end] (ts,o,h,l,c,v) или None; ref_price — проверка масштаба (контракты 1000…), None — без проверки."""
        k0, k1 = start_ms // (self.BLOCK * H), min(end_ms, now_ms) // (self.BLOCK * H)
        for sym in (f'{base}/USDT:USDT', f'1000{base}/USDT:USDT'):
            if sym not in self.markets:
                continue
            try:
                parts = [await self._bingx_hblock(sym, k, now_ms) for k in range(k0, k1 + 1)]
            except Exception:
                continue
            parts = [x for x in parts if x is not None]
            if not parts:
                continue
            ts = np.concatenate([x['ts'] for x in parts])
            o = np.argsort(ts, kind='stable')
            c = {k: np.concatenate([x[k] for x in parts])[o] for k in ('ts', 'o', 'h', 'l', 'c', 'v')}
            if ref_price is None or AS.scale_ok(c, ref_price):
                return c
        return None

    async def funding_all(self, base, start_ms, end_ms):
        """История ставок фандинга за период постранично → (ts, rate) | None."""
        for sym in (f'{base}/USDT:USDT', f'1000{base}/USDT:USDT'):
            if sym not in self.markets:
                continue
            try:
                rows, since = {}, start_ms
                for _ in range(8):
                    got = await self.ex.fetch_funding_rate_history(sym, since=since, limit=1000, params={'endTime': end_ms})
                    new = {int(r['timestamp']): float(r['fundingRate']) for r in got if r.get('timestamp') and r.get('fundingRate') is not None}
                    if not new or max(new) < since:
                        break
                    rows.update(new)
                    if max(new) >= end_ms:
                        break
                    since = max(new) + 1
                ks = sorted(k for k in rows if start_ms <= k <= end_ms)
                return (np.array(ks, dtype=np.int64), np.array([rows[k] for k in ks]))
            except Exception:
                return None
        return None


# ── сбор строк: признаки + симуляция для списка сигналов ────────────────────────────────────────────
async def compute_rows(sigs, ctx, uref, source, fees, now_ms, progress=None):
    """sigs — сигналы набора; ctx — (канал, pub_ts, сторона) для счётчиков; uref — {символ: опорная цена} универсума U (для ранга оборота). Свечи 1m не хранятся (память), сохраняются результаты."""
    if not sigs:
        return []
    lo = min(s['pub_ts'] for s in sigs) * 1000 - (TURN_DAYS + 2) * DAY
    hi = max(s['pub_ts'] for s in sigs) * 1000
    by_sym = {}
    for s in sigs:
        by_sym.setdefault(s['symbol'], []).append(s)
    refs = dict(uref)
    for s in sigs:
        refs.setdefault(s['symbol'], (s['entry_lo'] + s['entry_hi']) / 2)
    syms = sorted(set(refs) | set(by_sym))
    universe = sorted(uref)
    turns, funds, hfeat = {}, {}, {}
    btc = await source.hourly('BTC', lo - 20 * DAY, hi, None, now_ms)
    for i, sym in enumerate(syms):
        b = await source.hourly(sym, lo, hi, refs.get(sym), now_ms)
        if b is not None and len(b['ts']):
            turns[sym] = make_turn(b)
            for s in by_sym.get(sym, []):
                hfeat[(s['channel'], s['msg_id'])] = hourly_features(b, s['pub_ts'] * 1000)
        if sym in by_sym:
            ps = [s['pub_ts'] for s in by_sym[sym]]
            funds[sym] = await source.funding_all(sym, min(ps) * 1000 - 2 * DAY, max(ps) * 1000 + AS.HOLD_MS + AS.ENTRY_WINDOW_MS)
        del b
        if progress and (i + 1) % 25 == 0:
            await progress(f'/signal: часовые данные символов {i + 1}/{len(syms)}')
    rows = []
    for i, s in enumerate(sigs):
        key = (s['channel'], s['msg_id'])
        pub_ms = s['pub_ts'] * 1000
        hf = hfeat.get(key) or {}
        f = signal_features(s, hf, btc_features(btc, pub_ms), ctx, turns, universe, funds.get(s['symbol']))
        c, note = await source.candles(s['symbol'], pub_ms, pub_ms + AS.ENTRY_WINDOW_MS + AS.HOLD_MS, refs[s['symbol']], now_ms)
        sim = sim_signal(s, c, fees, now_ms, hf.get('atr'), funds.get(s['symbol']))
        del c
        sim.update({'key': key, 'pub_ms': pub_ms, 'symbol': s['symbol'], 'side': s['side'], 'note': note, 'f': f, 'fs': signed_features(f, s['side']), 'atr_ok': math.isfinite(hf.get('atr', NAN))})
        rows.append(sim)
        if progress and (i + 1) % 25 == 0:
            await progress(f'/signal: просчитано сигналов {i + 1}/{len(sigs)}')
    return rows


# ── выборка по варианту / фильтру / кандидату ───────────────────────────────────────────────────────
def rec_of(row, variant):
    """Результат сигнала по варианту: 'B' — его выход, 'REV' — обратная, иначе ключ части 1; None — нет."""
    if row.get('status') != 'filled':
        return None
    r = row['B'] if variant == 'B' else (row.get('REV') if variant == 'REV' else row['V'].get(variant))
    return r if r and r['closed'] and math.isfinite(r['R']) else None


def items_of(rows, variant):
    out = []
    for r in rows:
        x = rec_of(r, variant)
        if x:
            out.append((x['exit_ts'], x['R']))
    return out


def passes_filter(fs, flt):
    """Фильтр: список условий {name, tail, thr}; сигнал отбрасывается, если ЛЮБОЙ признак в запретном хвосте; нет значения → сигнал остаётся (§2.45.6 п. 3)."""
    for cnd in flt or []:
        v = fs.get(cnd['name'], NAN)
        if not math.isfinite(v):
            continue
        if (cnd['tail'] == 'gt' and v > cnd['thr']) or (cnd['tail'] == 'lt' and v < cnd['thr']):
            return False
    return True


def filtered(rows, flt):
    return [r for r in rows if passes_filter(r.get('fs', {}), flt)]


def is_full_stop(kinds):
    return 'stop' in kinds and 'tp' not in kinds


# ── часть 1 ─────────────────────────────────────────────────────────────────────────────────────────
def part1(rows):
    """12 вариантов + справочная обратная торговля. → dict(variants={key: stats+bh}, rev=stats, best, passed=[ключи с BH и t>0])."""
    res = {k: stats_of(items_of(rows, k)) for k in VARIANTS}
    flags = bh([res[k]['p'] for k in VARIANTS])
    for k, fl in zip(VARIANTS, flags):
        res[k]['bh'] = bool(fl)
    finite = [k for k in VARIANTS if res[k]['n'] and math.isfinite(res[k]['t_day'])]
    best = max(finite, key=lambda k: res[k]['t_day']) if finite else None
    passed = [k for k in VARIANTS if res[k]['bh'] and math.isfinite(res[k]['t_day']) and res[k]['t_day'] > 0]
    return {'variants': res, 'rev': stats_of(items_of(rows, 'REV')), 'best': best, 'passed': passed}


def mae_percentiles(rows):
    out = {}
    for name, cond in (('все', lambda m: True), ('достигли цели 1', lambda m: m['reached']), ('не достигли', lambda m: not m['reached'])):
        v = [r['MAE']['pct'] for r in rows if r.get('status') == 'filled' and r.get('MAE') and cond(r['MAE'])]
        out[name] = {'n': len(v), 'q': ([float(np.percentile(v, p)) for p in (25, 50, 75, 90)] if v else None)}
    return out


# ── часть 2 ─────────────────────────────────────────────────────────────────────────────────────────
def portrait_class(side, f):
    r24 = f.get('ret24h', NAN)
    if not math.isfinite(r24):
        return 'нет данных'
    near = f.get('hi24' if side == 'short' else 'lo24', NAN)
    if side == 'short':
        if r24 > 0 and math.isfinite(near) and near <= NEAR_ATR:
            return 'против движения у экстремума'
        return 'по тренду' if r24 < 0 else 'иное'
    if r24 < 0 and math.isfinite(near) and near <= NEAR_ATR:
        return 'против движения у экстремума'
    return 'по тренду' if r24 > 0 else 'иное'


def part2(rows):
    out = {}
    filled = [r for r in rows if r.get('status') == 'filled' or r.get('f')]
    for side in ('long', 'short'):
        sub = [r for r in filled if r['side'] == side]
        med = {}
        for k in RAW_FEATURES:
            v = np.array([r['f'].get(k, NAN) for r in sub], float)
            v = v[np.isfinite(v)]
            med[k] = {'n': len(v), 'q': ([float(np.percentile(v, p)) for p in (25, 50, 75)] if len(v) else None)}
        cls = {}
        for r in sub:
            c = portrait_class(side, r['f'])
            cls[c] = cls.get(c, 0) + 1
        up = [r['f']['ret24h'] > 0 for r in sub if math.isfinite(r['f'].get('ret24h', NAN))]
        out[side] = {'n': len(sub), 'med': med, 'cls': cls, 'up24': (float(np.mean(up)) if up else None)}
    return out


# ── часть 3 ─────────────────────────────────────────────────────────────────────────────────────────
def part3_features(rows):
    """Полные стопы без единой цели против остальных (модель B, закрытые исполненные): по каждому признаку разность медиан, p (Манн–Уитни), BH q = 0.05 по числу признаков."""
    base = [r for r in rows if rec_of(r, 'B')]
    stops = [r for r in base if is_full_stop(r['B']['kinds'])]
    rest = [r for r in base if not is_full_stop(r['B']['kinds'])]
    res = []
    for k in FEATURES:
        x = np.array([r['fs'].get(k, NAN) for r in stops], float)
        y = np.array([r['fs'].get(k, NAN) for r in rest], float)
        x, y = x[np.isfinite(x)], y[np.isfinite(y)]
        _u, p = mannwhitney_p(x, y)
        res.append({'name': k, 'n_stop': len(x), 'n_rest': len(y), 'med_stop': (float(np.median(x)) if len(x) else NAN), 'med_rest': (float(np.median(y)) if len(y) else NAN), 'p': p})
    flags = bh([r['p'] for r in res])
    for r, fl in zip(res, flags):
        r['bh'] = bool(fl)
        r['diff'] = r['med_stop'] - r['med_rest'] if math.isfinite(r['med_stop']) and math.isfinite(r['med_rest']) else NAN
    return {'features': res, 'n_stop': len(stops), 'n_rest': len(rest)}


def build_filters(p3, rows):
    """Не более 3 фильтров из ≤ 2 признаков, прошедших BH; порог — квантиль 20/80 по ВСЕМ закрытым исполненным DISCOVERY; хвост — по знаку разности медиан (§2.45.6 п. 3)."""
    base = [r for r in rows if rec_of(r, 'B')]
    passed = sorted([f for f in p3['features'] if f['bh'] and math.isfinite(f['diff'])], key=lambda f: f['p'])[:MAX_FILTER_FEATS]
    conds = []
    for f in passed:
        v = np.array([r['fs'].get(f['name'], NAN) for r in base], float)
        v = v[np.isfinite(v)]
        if not len(v):
            continue
        gt = f['diff'] > 0
        conds.append({'name': f['name'], 'tail': 'gt' if gt else 'lt', 'thr': float(np.quantile(v, Q_HI if gt else Q_LO))})
    flts = []
    if conds:
        flts.append({'id': 'F1', 'conds': [conds[0]]})
    if len(conds) > 1:
        flts.append({'id': 'F2', 'conds': [conds[1]]})
        flts.append({'id': 'F3', 'conds': [conds[0], conds[1]]})
    return flts[:MAX_FILTERS]


def part33(rows, flts, best):
    out = []
    base_total = len([r for r in rows if rec_of(r, 'B')])
    for fl in flts:
        sub = filtered(rows, fl['conds'])
        kept = len([r for r in sub if rec_of(r, 'B')])
        out.append({'id': fl['id'], 'share_removed': (1 - kept / base_total) if base_total else NAN, 'B': stats_of(items_of(sub, 'B')), 'best': (stats_of(items_of(sub, best)) if best else None)})
    return out


def pick_candidates(p1, flts, p33):
    """Пул (§2.45.6 п. 4): варианты части 1 (BH и t > 0) и пары фильтр × {B, лучший вариант} с t > 0; ≤ 3 с наибольшим t по дням; обратная торговля не входит."""
    pool = []
    for k in p1['passed']:
        pool.append({'id': f'V:{k}', 'filter': None, 'variant': k, 't_day': p1['variants'][k]['t_day']})
    fl_by_id = {f['id']: f for f in flts}
    for r in p33:
        for var, st in (('B', r['B']), (p1['best'], r['best'])):
            if var and st and st['n'] and math.isfinite(st['t_day']) and st['t_day'] > 0:
                pool.append({'id': f'{r["id"]}+{var}', 'filter': fl_by_id[r['id']]['conds'], 'variant': var, 't_day': st['t_day']})
    seen, uniq = set(), []
    for c in sorted(pool, key=lambda c: -c['t_day']):
        if c['id'] not in seen:
            seen.add(c['id'])
            uniq.append(c)
    return uniq[:MAX_CANDS]


# ── финал ───────────────────────────────────────────────────────────────────────────────────────────
def final_eval(rows, cand, months_ret):
    """Критерии §2.45.5 по закрытым исполненным позициям кандидата. Нет данных по месяцам роста/падения BTC = критерий не выполнен (строго)."""
    sub = filtered(rows, cand['filter']) if cand.get('filter') else rows
    its = [(rec_of(r, cand['variant'])['exit_ts'], rec_of(r, cand['variant'])['R'], r['pub_ms']) for r in sub if rec_of(r, cand['variant'])]
    st = stats_of([(t, x) for t, x, _p in its])
    grp = AS.month_groups([{'pub_ms': p, 'B': {'R': x}} for _t, x, p in its], months_ret)
    pos = lambda v: v is not None and v >= 0
    crit = {'n': st['n'] >= FINAL_MIN_N, 'mean_t': bool(st['n'] and st['mean'] > 0 and math.isfinite(st['t_day']) and st['t_day'] >= FINAL_MIN_T),
            'pf': bool(st['pf'] is not None and st['pf'] >= FINAL_MIN_PF), 'up': pos(grp['up']['mean']), 'down': pos(grp['down']['mean'])}
    return {'stats': st, 'groups': grp, 'crit': crit, 'pass': all(crit.values())}


# ── отчёты ──────────────────────────────────────────────────────────────────────────────────────────
fm = AS.fm


def fmt_stats_line(name, st, extra=''):
    if not st or not st['n']:
        return f'  {name}: n 0'
    return (f'  {name}: n {st["n"]}, WR {st["wr"] * 100:.0f}%, выигрыш {st["w_avg"]:+.2f} R / проигрыш −{st["l_avg"]:.2f} R, mean R net {fm(st["mean"], 3)}, PF {fm(st["pf"])}, '
            f't по дням {fm(st["t_day"])} (дней {st["n_days"]}), p {st["p"]:.3f}{extra}')


def describe_filter(conds):
    return ' И '.join(f'{LABELS[c["name"]]} {"выше" if c["tail"] == "gt" else "ниже"} {c["thr"]:.4g} → сигнал пропускается' for c in conds)


def fees_note(fees):
    return f'комиссии: FEE_TAKER {fees["taker"] * 100:.4f}%, FEE_MAKER {fees["maker"] * 100:.4f}%, проскальзывание {AS.SLIP * 100:.2f}% на сторону'


def study_report(version, fees, split, sigs, rows, p1, p2, p3, flts, p33, cands, mae, cands_saved):
    cnt = {k: sum(1 for r in rows if r['status'] == k) for k in ('filled', 'not_filled', 'pending', 'no_data')}
    closed = len([r for r in rows if rec_of(r, 'B')])
    L = [f'🔬 <b>/signal_study — поиск по данным канала, ТОЛЬКО DISCOVERY (BOT_SPEC §2.45)</b> | код {version}', fees_note(fees),
         f'Граница заморожена: DISCOVERY — сигналы до {time.strftime("%Y-%m-%d %H:%M", time.gmtime(split["t_split"]))} UTC ({split["n_disc"]} из {split["n_total"]}, 60%); HOLDOUT не читался.',
         f'DISCOVERY: сигналов {len(sigs)}; исполнено {cnt["filled"]} (закрыто по модели B {closed}); не исполнено {cnt["not_filled"]}; ожидают {cnt["pending"]}; нет данных цен {cnt["no_data"]}; '
         f'без ATR (символ не на BingX) {sum(1 for r in rows if r.get("status") == "filled" and not r.get("atr_ok"))}', '',
         '<b>ЧАСТЬ 1 — его вход + наш стоп</b> (R = P&amp;L / расстояние до НАШЕГО стопа; T — его цели долями + безубыток, R2 — фикс. TP 2R; BH q 0.05 по 12 вариантам)']
    for k in VARIANTS:
        st = p1['variants'][k]
        L.append(fmt_stats_line(k, st, ' [BH]' if st['bh'] else '') if st['n'] else f'  {k}: n 0')
    L.append(f'Лучший по t: {p1["best"] or "нет"}; прошли BH с t&gt;0: {", ".join(p1["passed"]) or "нет"}')
    L.append(fmt_stats_line('справочно: обратная торговля (вне отбора)', p1['rev']))
    L.append('Диагностика MAE до первой цели (% от входа; перцентили 25/50/75/90; не для решений):')
    for k, v in mae.items():
        L.append(f'  {k}: n {v["n"]}' + (', ' + ' / '.join(f'{x:.2f}' for x in v['q']) if v['q'] else ''))
    L += ['', '<b>ЧАСТЬ 2 — портрет метода автора</b> (сырые значения, медиана [25–75%])']
    for side, nm in (('short', 'ШОРТЫ'), ('long', 'ЛОНГИ')):
        d = p2[side]
        tot = sum(d['cls'].values()) or 1
        L.append(f'{nm}: n {d["n"]}; доходность 24h &gt; 0 в {fm(d["up24"] * 100 if d["up24"] is not None else None, 0)}% случаев; ' + '; '.join(f'{c} {v / tot * 100:.0f}%' for c, v in sorted(d['cls'].items(), key=lambda kv: -kv[1])))
        for k in RAW_FEATURES:
            m = d['med'][k]
            if m['q']:
                L.append(f'    {RAW_LABELS[k]}: {m["q"][1]:.4g} [{m["q"][0]:.4g} – {m["q"][2]:.4g}] (n {m["n"]})')
    L += ['', f'<b>ЧАСТЬ 3 — чем отличаются полные стопы</b> (без единой цели: {p3["n_stop"]}; остальные закрытые: {p3["n_rest"]}; Манн–Уитни, BH q 0.05 по {len(FEATURES)} признакам)']
    for f in sorted(p3['features'], key=lambda f: f['p']):
        L.append(f'  {LABELS[f["name"]]}: медиана стопов {fm(f["med_stop"], 4)} против {fm(f["med_rest"], 4)}, p {f["p"]:.4f}' + (' [BH]' if f['bh'] else ''))
    if flts:
        L.append('Фильтры-кандидаты (порог — квантиль 20/80 DISCOVERY):')
        for fl in flts:
            L.append(f'  {fl["id"]}: {describe_filter(fl["conds"])}')
        L.append('3.3 на DISCOVERY:')
        for r in p33:
            L.append(f'  {r["id"]}: отфильтровано {r["share_removed"] * 100:.0f}%')
            L.append('  ' + fmt_stats_line('модель B (его выход)', r['B']))
            if r['best']:
                L.append('  ' + fmt_stats_line(f'лучший вариант части 1 ({p1["best"]})', r['best']))
    else:
        L.append('Ни один признак не прошёл BH → фильтров-кандидатов нет (3.2–3.3 пусты).')
    L += ['', f'<b>Кандидаты на финал</b> (≤ 3 по t по дням; список {"заморожен" if cands_saved else "НЕ изменён: флаг финала уже взят"}):']
    if cands:
        for c in cands:
            L.append(f'  {c["id"]}: вариант {c["variant"]}' + (f', фильтр: {describe_filter(c["filter"])}' if c['filter'] else ', без фильтра') + f', t по дням {fm(c["t_day"])}')
        L.append('Финал — только вручную: /signal_final confirm (один прогон на HOLDOUT, повтор запрещён навсегда).')
    else:
        L.append('  кандидатов нет → по §2.45.5 финал не запускается: ни один не прошёл, канал и производные от него стратегии закрыты окончательно (HOLDOUT не читался).')
    L.append('Оговорки (§2.45.7): множественные сравнения (12 вариантов, 20 признаков), ретроспектива смещена; «лучший вариант» выбран по t без поправки; результат DISCOVERY — только гипотеза.')
    return '\n'.join(L)


def final_report(version, fees, split, cands, evs, ref, nrows):
    mark = lambda v: 'н/п' if v is None else ('да' if v else 'НЕТ')
    L = [f'🔒 <b>/signal_final — финал на HOLDOUT, единственный прогон (BOT_SPEC §2.45.5)</b> | код {version}', fees_note(fees),
         f'HOLDOUT: сигналов {nrows}, с {time.strftime("%Y-%m-%d", time.gmtime(split["t_split"]))} UTC. Критерии (все): mean R net &gt; 0 при t по дням ≥ {FINAL_MIN_T}; PF net ≥ {FINAL_MIN_PF}; mean ≥ 0 в месяцы роста и падения BTC; n ≥ {FINAL_MIN_N}.', '']
    for c, ev in zip(cands, evs):
        st, g, cr = ev['stats'], ev['groups'], ev['crit']
        L += [f'<b>{c["id"]}</b>: вариант {c["variant"]}' + (f', фильтр: {describe_filter(c["filter"])}' if c['filter'] else ', без фильтра'),
              f'  n {st["n"]} [{mark(cr["n"])}], mean R net {fm(st["mean"], 3)}, t по дням {fm(st["t_day"])} [{mark(cr["mean_t"])}], PF {fm(st["pf"])} [{mark(cr["pf"])}]',
              f'  месяцы роста BTC: {g["up"]["n_months"]} мес., mean {fm(g["up"]["mean"], 3) if g["up"]["mean"] is not None else "н/п"} [{mark(cr["up"])}]; падения: {g["down"]["n_months"]} мес., '
              f'mean {fm(g["down"]["mean"], 3) if g["down"]["mean"] is not None else "н/п"} [{mark(cr["down"])}]', f'  <b>{"ПРОЙДЕНО" if ev["pass"] else "НЕ ПРОЙДЕНО"}</b>', '']
    L.append(fmt_stats_line('справочно: модель B «как есть» на HOLDOUT', ref))
    if any(e['pass'] for e in evs):
        L.append('Решение (§2.45.5): прошёл → отдельный пре-коммит shadow на живых сигналах канала (без денег, n ≥ 100); торговли нет. Оговорки §2.45.7 остаются.')
    else:
        L.append('Решение (§2.45.5): НЕ ПРОЙДЕН НИ ОДИН → канал и производные от него стратегии закрыты окончательно. «Не пройдено» = «эффект не доказан на этих данных».')
    return '\n'.join(L)


# ── прогоны ─────────────────────────────────────────────────────────────────────────────────────────
def _fees(params):
    return {'taker': float(params.get('fee_taker', 0.0005)), 'maker': float(params.get('fee_maker', 0.0005))}


async def _open_source(params, path, source):
    if source is not None:
        return source, None
    import aiohttp
    ses = aiohttp.ClientSession()
    src = StudySource(params.get('cache_dir') or os.path.join(os.path.dirname(path), 'audit_cache'), ses)
    await src.open()
    return src, ses


def _uref(disc):
    d = {}
    for s in disc:
        d.setdefault(s['symbol'], (s['entry_lo'] + s['entry_hi']) / 2)
    return d


async def run_study(params, progress=None, source=None):
    """/signal_study: части 1–3 на DISCOVERY (HOLDOUT не читается), кандидаты замораживаются в audit_meta (пока флаг финала не взят)."""
    path = params.get('adb') or AD.db_path()
    if not os.path.exists(path):
        return '⛔ /signal_study: база аудита пуста'
    con = AD.connect(path)
    split = freeze_split(con)
    if split is None:
        return '⛔ /signal_study: сигналов меньше 20 — делить нечего'
    fees, version = _fees(params), params.get('version', '')
    now_ms = int(params.get('now_ms') or time.time() * 1000)
    disc = load_discovery(con, split)
    ctx = context_light(con, split, False)
    src, ses = await _open_source(params, path, source)
    try:
        rows = await compute_rows(disc, ctx, _uref(disc), src, fees, now_ms, progress)
    finally:
        if ses is not None:
            await src.close()
            await ses.close()
    p1 = part1(rows)
    p2 = part2(rows)
    p3 = part3_features(rows)
    flts = build_filters(p3, rows)
    p33 = part33(rows, flts, p1['best'])
    cands = pick_candidates(p1, flts, p33)
    saved = not final_flag(con)
    if saved:
        con.execute('INSERT OR REPLACE INTO audit_meta (k, v) VALUES (?, ?)', (K_CANDS, json.dumps({'cands': cands, 'version': version, 'ts': int(time.time())}, ensure_ascii=False)))
        con.commit()
    return study_report(version, fees, split, disc, rows, p1, p2, p3, flts, p33, cands, mae_percentiles(rows), saved)


def load_cands(con):
    v = AD.meta_get(con, K_CANDS)
    return json.loads(v)['cands'] if v else None


async def run_final(params, progress=None, source=None):
    """/signal_final: без confirm — кандидаты и критерии (HOLDOUT не читается); с confirm — единственный прогон (флаг берётся ДО чтения HOLDOUT, повтор запрещён навсегда)."""
    path = params.get('adb') or AD.db_path()
    if not os.path.exists(path):
        return '⛔ /signal_final: база аудита пуста'
    con = AD.connect(path)
    split = AD.meta_get(con, K_SPLIT)
    cands = load_cands(con)
    version, fees = params.get('version', ''), _fees(params)
    flag = final_flag(con)
    if not split or cands is None:
        return '⛔ /signal_final отказ: сначала /signal_study (границы и кандидаты не заморожены; флаг НЕ взят)'
    split = json.loads(split)
    head = ['🔒 <b>/signal_final</b> (BOT_SPEC §2.45.5): один прогон ≤ 3 кандидатов на HOLDOUT, повтор запрещён навсегда (флаг в БД)',
            f'Критерии (все): mean R net &gt; 0 при t по дням ≥ {FINAL_MIN_T}; PF net ≥ {FINAL_MIN_PF}; mean ≥ 0 в месяцы роста и падения BTC; n ≥ {FINAL_MIN_N}',
            'Кандидаты (заморожены при /signal_study): ' + ('; '.join(f'{c["id"]} (вариант {c["variant"]}' + (f', фильтр: {describe_filter(c["filter"])}' if c['filter'] else '') + ')' for c in cands) or 'нет'),
            f'Флаг финала: {"УЖЕ ВЗЯТ — запуск невозможен" if flag else "свободен"}.']
    if not cands:
        return '\n'.join(head + ['Кандидатов нет → по §2.45.5 ни один не прошёл: канал и производные от него стратегии закрыты окончательно (HOLDOUT не читался, флаг не нужен).'])
    if not params.get('confirm'):
        return '\n'.join(head + ['Запуск: /signal_final confirm'])
    if flag:
        return '⛔ /signal_final отказ: флаг финала уже взят — финал выполняется один раз, повтор запрещён (§2.45.5)'
    if not meta_set_if_absent(con, K_FLAG, {'t': time.time(), 'version': version}):
        return '⛔ /signal_final отказ: флаг финала уже взят параллельным запуском'
    now_ms = int(params.get('now_ms') or time.time() * 1000)
    hold = load_holdout(con, split)                             # флаг уже взят → доступ открыт
    disc = load_discovery(con, split)
    ctx = context_light(con, split, True)
    src, ses = await _open_source(params, path, source)
    try:
        rows = await compute_rows(hold, ctx, _uref(disc), src, fees, now_ms, progress)
        first = min((r['pub_ms'] for r in rows), default=now_ms)
        btc = await src.btc_daily(first - 40 * DAY, now_ms) if rows else None
    finally:
        if ses is not None:
            await src.close()
            await ses.close()
    months = AS.btc_months_from_daily(btc[0], btc[1], now_ms) if btc and len(btc[0]) > 5 else {}
    evs = [final_eval(rows, c, months) for c in cands]
    ref = stats_of(items_of(rows, 'B'))
    con.execute('INSERT OR REPLACE INTO audit_meta (k, v) VALUES (?, ?)', (K_RESULT, json.dumps({'evs': evs, 'ref': ref, 'version': version, 'ts': int(time.time())}, ensure_ascii=False, default=float)))
    con.execute('INSERT OR REPLACE INTO audit_meta (k, v) VALUES (?, ?)', (K_DONE, '1'))
    con.commit()
    return final_report(version, fees, split, cands, evs, ref, len(hold))
