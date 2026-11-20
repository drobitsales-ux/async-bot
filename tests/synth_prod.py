"""synth_prod.py — синтетика ПРОД-размера для замеров памяти (BOT_SPEC §2.55): BingX 1295 рынков / 552 символа свечей / 523 символа фандинга, Binance — архив D1 + фандинг
по общим базам и 1h по 9 символам, OKX 89 символов (фандинг ≤ 92 суток), вселенная H2 (базы вне архива). scale < 1 уменьшает числа (быстрые тесты). Данные — детерминированные
случайные блуждания; НЕ реальные. Файлы пишутся теми же функциями, что и боевой загрузчик (формат и dtype совпадают с продом)."""
import numpy as np

import research_bdata as B
import research_xdata as X

DAY = X.DAY
HOUR = X.HOUR
END_DAY = int(np.datetime64('2025-06-30', 'D').astype(np.int64))


def _walk(rng, n, vol, p0=100.0):
    r = rng.normal(-0.5 * vol * vol, vol, n)
    c = p0 * np.exp(np.cumsum(r))
    o = np.r_[p0, c[:-1]]
    h = np.maximum(o, c) * (1 + np.abs(rng.normal(0, vol * 0.3, n)))
    lo = np.minimum(o, c) * (1 - np.abs(rng.normal(0, vol * 0.3, n)))
    return o, h, lo, c


def _months(t0, t1):
    a, b = np.datetime64(int(t0), 'ms').astype('datetime64[M]'), np.datetime64(int(t1), 'ms').astype('datetime64[M]')
    return [str(m) for m in np.arange(a, b + np.timedelta64(1, 'M'))]


def build(xdir, bdir, scale=1.0, seed=20261120, h1_bars=52000, days=(300, 1800), fund_gap=0.0, h1_n=None):
    """→ dict со списками символов и счётчиками. Рынки: BingX 1295·scale (из них символов со свечами 552·scale, с фандингом 523·scale; общих с архивом Binance ≈ 75% символов со свечами),
    OKX 89·scale (365 суток свечей, фандинг 92 суток)."""
    rng = np.random.default_rng(seed)
    n_mk, n_bars, n_fund, n_okx = (max(2, int(round(v * scale))) for v in (1295, 552, 523, 89))
    n_common = int(n_bars * 0.75)
    idx = X.load_index(xdir)
    bidx = B.load_index(bdir)
    bidx['symbols'], out = [], {'bingx_bars': [], 'bingx_fund': [], 'common': [], 'okx': [], 'h1': []}
    for i in range(n_mk):
        base = f'C{i:04d}' if i < n_common else f'N{i:04d}'
        sym = f'{base}/USDT:USDT'
        idx['markets']['bingx'][sym] = {'base': base, 'norm': base, 'active': i < n_bars or i % 3 == 0, 'created': None, 'excl': None, 'cat': ''}
    for i in range(n_bars):
        base = f'C{i:04d}' if i < n_common else f'N{i:04d}'
        sym = f'{base}/USDT:USDT'
        nd = int(rng.integers(days[0], days[1] + 1))
        d0 = END_DAY - nd + 1
        o, h, lo, c = _walk(rng, nd, 0.05)
        qv = 5e7 * rng.uniform(0.5, 1.5, nd)
        ts = ((d0 + np.arange(nd)) * DAY).astype(np.int64)
        X.save_bars(xdir, 'bingx', sym, ts, o, h, lo, c, qv / c)
        idx['bars']['bingx'][sym] = {'first': int(ts[0]), 'last': int(ts[-1]), 'n': nd, 't': END_DAY * DAY, 'complete': True}
        out['bingx_bars'].append(sym)
        if i < n_fund:
            k = nd * 3
            fts = (d0 * DAY + (np.arange(k) + 1) * 8 * HOUR).astype(np.int64)
            fr = 0.0001 + fund_gap + rng.normal(0, 0.0002, k)
            X.save_fund(xdir, 'bingx', sym, fts, fr)
            idx['fund']['bingx'][sym] = {'first': int(fts[0]), 'last': int(fts[-1]), 'n': k, 't': END_DAY * DAY, 'complete': True}
            out['bingx_fund'].append(sym)
        if i < n_common:
            nb = min(2500, nd + int(rng.integers(0, 600)))
            bd0 = END_DAY - nb + 1
            # Binance: тот же актив (скоррелированные цены): логарифмическая добавка к ряду BingX, предыстория — отдельное блуждание
            ob, hb, lb, cb = _walk(rng, nb, 0.05)
            cb[-nd:] = c * np.exp(rng.normal(0, 0.002, nd))
            ob[-nd:] = o
            hb[-nd:] = h
            lb[-nd:] = lo
            qb = 8e7 * rng.uniform(0.5, 1.5, nb)
            bts = ((bd0 + np.arange(nb)) * DAY).astype(np.int64)
            bs = f'{base}USDT'
            B.save_d1(bdir, bs, bts, {'o': ob, 'h': hb, 'l': lb, 'c': cb, 'qv': qb}, _months(bts[0], bts[-1]))
            kf = nb * 3
            bf = (bd0 * DAY + np.arange(kf) * 8 * HOUR + 3).astype(np.int64)
            B.save_fund(bdir, bs, bf, {'rate': 0.0001 + rng.normal(0, 0.0002, kf)}, _months(bf[0], bf[-1]))
            bidx['symbols'].append(bs)
            bidx['keys'][bs] = {'d1': _months(bts[0], bts[-1]), 'fr': _months(bf[0], bf[-1])}
            bidx['done'][bs] = True
            out['common'].append(sym)
            if len(out['h1']) < (h1_n if h1_n is not None else max(1, int(round(9 * scale)))):
                nh = min(h1_bars, nb * 24)
                hts = ((END_DAY * 24 - nh + 1 + np.arange(nh)) * HOUR).astype(np.int64)
                hc = np.repeat(cb[-nb:][-(nh // 24 + 1):], 24)[:nh]
                B.save_h1(bdir, bs, hts, {'h': hc * 1.0005, 'l': hc * 0.9995, 'c': hc}, _months(hts[0], hts[-1]))
                hts2 = ((END_DAY * 24 - min(nh, nd * 24) + 1 + np.arange(min(nh, nd * 24))) * HOUR).astype(np.int64)
                hcb = np.repeat(c[-(len(hts2) // 24 + 1):], 24)[:len(hts2)]
                X.save_hbars(xdir, 'bingx', sym, hts2, hcb * 1.0005, hcb * 0.9995, hcb)
                idx['hbars']['bingx'][sym] = {'first': int(hts2[0]), 'last': int(hts2[-1]), 'n': int(len(hts2)), 't': END_DAY * DAY, 'complete': True}
                out['h1'].append(sym)
    for i in range(n_okx):
        base = f'O{i:04d}'
        sym = f'{base}/USDT:USDT'
        nd = 365
        d0 = END_DAY - nd + 1
        o, h, lo, c = _walk(rng, nd, 0.06)
        ts = ((d0 + np.arange(nd)) * DAY).astype(np.int64)
        X.save_bars(xdir, 'okx', sym, ts, o, h, lo, c, 6e7 / c)
        idx['markets']['okx'][sym] = {'base': base, 'norm': base, 'active': True, 'created': int(ts[0]), 'excl': None, 'cat': 'instCategory=1'}
        idx['bars']['okx'][sym] = {'first': int(ts[0]), 'last': int(ts[-1]), 'n': nd, 't': END_DAY * DAY, 'complete': True}
        k = 92 * 3
        fts = ((END_DAY + 1) * DAY - (np.arange(k)[::-1] + 1) * 8 * HOUR).astype(np.int64)
        X.save_fund(xdir, 'okx', sym, fts, 0.0001 + rng.normal(0, 0.0002, k))
        idx['fund']['okx'][sym] = {'first': int(fts[0]), 'last': int(fts[-1]), 'n': k, 't': END_DAY * DAY, 'complete': False}
        out['okx'].append(sym)
    idx['t'] = END_DAY * DAY
    X.save_index(xdir, idx)
    B.save_index(bdir, bidx)
    out['counts'] = {'bingx_markets': n_mk, 'bingx_bars': n_bars, 'bingx_fund': n_fund, 'common': n_common, 'okx': n_okx, 'h1': len(out['h1'])}
    return out
