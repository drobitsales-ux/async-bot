"""Фикстуры «батареи» (v108): кэш data.binance.vision в формате research_bdata (D1 + фандинг по символам), 15m-кэш 16 символов H-XS — синтетика, НЕ реальные данные."""
import numpy as np

import factory_fixtures as F
import research_bdata as B

DAY = B.DAY_MS
H16 = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'BNBUSDT', 'XRPUSDT', 'ADAUSDT', 'DOGEUSDT', 'AVAXUSDT', 'LINKUSDT', 'DOTUSDT', 'LTCUSDT', 'TRXUSDT', 'ATOMUSDT', 'NEARUSDT', 'UNIUSDT', 'ETCUSDT']


def month_list(first, last):
    out = []
    y, m = first
    while (y, m) <= last:
        out.append(f'{y}-{m:02d}')
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


def write_battery(d, n_syms=40, first_day='2019-12-01', last_day='2025-03-31', seed=7, vol=0.04, delist_n=3, qv=60e6, listings=True, fund_persist=0.9, stable=0, list_hi=None):
    """Синтетический кэш: n_syms символов; первые 16 — H16 (с начала); остальные листятся в разные даты (если listings), delist_n из них обрывается рано. Фандинг: AR(1) на 8-часовых расчётах.
    qv — средний дневной оборот (USDT). → список символов."""
    rng = np.random.default_rng(seed)
    d0 = int(np.datetime64(first_day, 'D').astype(np.int64))
    d1 = int(np.datetime64(last_day, 'D').astype(np.int64))
    nd = d1 - d0 + 1
    names = list(H16) + [f'COIN{i:02d}USDT' for i in range(n_syms - len(H16))]
    names = names[:n_syms]
    idx = B.load_index(d)
    idx['symbols'] = []
    for i, s in enumerate(names):
        if i < len(H16) or not listings:
            f = 0
        else:
            f = int(rng.integers(30, list_hi or (nd - 400)))
        l = nd - 1
        if i >= len(H16) and i - len(H16) < delist_n:
            l = min(nd - 1, f + int(rng.integers(150, 500)))
        sig = vol if i >= len(H16) else vol * 0.6
        n = l - f + 1
        r = rng.normal(-0.5 * sig * sig, sig, n)
        c = 100.0 * np.exp(np.cumsum(r))
        o = np.r_[100.0, c[:-1]]
        hi = np.maximum(o, c) * (1 + np.abs(rng.normal(0, sig * 0.3, n)))
        lo = np.minimum(o, c) * (1 - np.abs(rng.normal(0, sig * 0.3, n)))
        q = qv * rng.uniform(0.5, 1.5, n)
        if i >= len(H16) and stable and i - len(H16) < stable:
            c = 1.0 + rng.normal(0, 0.0005, n)
            o, hi, lo = np.r_[1.0, c[:-1]], np.maximum(c, 1.0) + 0.0003, np.minimum(c, 1.0) - 0.0003
        ts = (d0 + f + np.arange(n)) * DAY
        B.save_d1(d, s, ts, {'o': o, 'h': hi, 'l': lo, 'c': c, 'qv': q}, month_list(*_ym(ts[0], ts[-1])))
        # фандинг: 3 расчёта в сутки, AR(1)
        nf = n * 3
        fr = np.zeros(nf)
        e = rng.normal(0, 0.0002, nf)
        for k in range(1, nf):
            fr[k] = fund_persist * fr[k - 1] + e[k]
        fts = (d0 + f) * DAY + np.arange(nf) * 8 * B.HOUR_MS
        B.save_fund(d, s, fts, {'rate': fr}, month_list(*_ym(fts[0], fts[-1])))
        idx['symbols'].append(s)
        idx['keys'][s] = {'d1': month_list(*_ym(ts[0], ts[-1])), 'fr': month_list(*_ym(fts[0], fts[-1]))}
        idx['done'][s] = True
    B.save_index(d, idx)
    return names


def _ym(t0, t1):
    a = np.datetime64(int(t0), 'ms').astype('datetime64[M]').astype(object)
    b = np.datetime64(int(t1), 'ms').astype('datetime64[M]').astype(object)
    return (a.year, a.month), (b.year, b.month)


def write_h16_15m(vdir, first=(2019, 12), last=(2025, 3)):
    for i, s in enumerate(H16):
        F.write_crypto(vdir, s, first, last, seed=100 + i, vol=0.002)
