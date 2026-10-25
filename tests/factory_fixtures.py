"""Общие фикстуры «фабрики» (v103): синтетические ряды, часовые файлы Dukascopy по месяцам, 15m-кэш Binance — для tests/test_v103_*.py."""
import calendar
from datetime import datetime, timezone

import numpy as np

import research_fdata as FD
import research_vision as V

HOUR = 3_600_000


def ms(y, m, d=1, h=0):
    return int(datetime(y, m, d, h, tzinfo=timezone.utc).timestamp() * 1000)


def gbm(n, rng, vol, p0=100.0, drift=0.0):
    r = rng.normal(drift, vol, n)
    c = p0 * np.exp(np.cumsum(r))
    o = np.r_[p0, c[:-1]]
    hi = np.maximum(o, c) * (1 + np.abs(rng.normal(0, vol * 0.3, n)))
    lo = np.minimum(o, c) * (1 - np.abs(rng.normal(0, vol * 0.3, n)))
    return o, hi, lo, c


def month_hours(y, m1):
    n = calendar.monthrange(y, m1)[1] * 24
    return ms(y, m1) + np.arange(n) * HOUR


def is_closed_fx(ts):
    """Закрытие рынка: пятница ≥ 21:00 UTC … воскресенье < 21:00 UTC (приблизительно, без перехода на летнее время — для фикстур)."""
    d = ts // 86_400_000
    wd = (d + 3) % 7
    h = (ts // HOUR) % 24
    return ((wd == 4) & (h >= 21)) | (wd == 5) | ((wd == 6) & (h < 21))


def write_instrument(d, key, first=(2019, 1), last=(2025, 2), seed=1, vol=0.002, p0=None, closed_filler=True):
    """Месячные .npz как у load_instrument: бары по расписанию рынка, закрытые часы — плоские бары с нулевым объёмом; GBM непрерывный между месяцами."""
    rng = np.random.default_rng(seed)
    tags = FD.month_tags(first, last)
    p = float(p0 or FD.INSTRUMENTS[key][2][0] * 2) if key in FD.INSTRUMENTS else 100.0
    for tag in tags:
        y, m1 = int(tag[:4]), int(tag[5:])
        ts = month_hours(y, m1)
        n = len(ts)
        o, h, l, c = gbm(n, rng, vol, p)
        p = float(c[-1])
        closed = is_closed_fx(ts)
        v = np.where(closed, 0.0, rng.uniform(1, 10, n))
        h = np.where(closed, c, h)
        l = np.where(closed, c, l)
        o = np.where(closed, c, o)
        if not closed_filler:
            keep = ~closed
            ts, o, h, l, c, v = ts[keep], o[keep], h[keep], l[keep], c[keep], v[keep]
        V._atomic_savez(FD.month_path(d, key, y, m1), ts=ts.astype(np.int64), o=o, h=h, l=l, c=c, v=v)
    idx = FD.load_index(d, key)
    idx['scale'] = 1.0
    FD.save_index(d, key, idx)


def write_crypto(vdir, sym, first=(2019, 1), last=(2025, 2), seed=1, vol=0.0012, p0=100.0):
    """15m-кэш Binance (формат research_vision.save_klines) — синтетический GBM."""
    rng = np.random.default_rng(seed)
    tss, cols = [], {k: [] for k in ('o', 'h', 'l', 'c', 'v', 'tbv')}
    p = p0
    months = FD.month_tags(first, last)
    for tag in months:
        y, m1 = int(tag[:4]), int(tag[5:])
        n = calendar.monthrange(y, m1)[1] * 96
        ts = ms(y, m1) + np.arange(n) * 900_000
        o, h, l, c = gbm(n, rng, vol, p)
        p = float(c[-1])
        tss.append(ts)
        for k, a in zip(('o', 'h', 'l', 'c'), (o, h, l, c)):
            cols[k].append(a)
        cols['v'].append(rng.uniform(1, 10, n))
        cols['tbv'].append(rng.uniform(0, 5, n))
    V.save_klines(vdir, sym, '15m', np.concatenate(tss), {k: np.concatenate(v) for k, v in cols.items()}, months)
