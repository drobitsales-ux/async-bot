"""memprobe.py — замер памяти этапов загрузки/расчёта на синтетике прод-размера (BOT_SPEC §2.55): tracemalloc (топ мест аллокации и пик) + RSS/адресное пространство (VmHWM/VmPeak).
Каждый этап — в ОТДЕЛЬНОМ процессе с окружением дочернего процесса бота (потоки BLAS = 1); при limit_mb — RLIMIT_AS как у research_runner. Запуск: python tests/memprobe.py <этап> <xdir> <bdir>."""
import asyncio
import json
import os
import re
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
STAGES = ('xfund_load', 'xfund_prelim', 'xfund_test', 'h2_load', 'h2_show')


def _vm():
    s = open('/proc/self/status').read()
    g = lambda k: int(re.search(k + r':\s+(\d+) kB', s).group(1)) / 1024.0
    return {'rss_mb': g('VmRSS'), 'hwm_mb': g('VmHWM'), 'vmsize_mb': g('VmSize'), 'vmpeak_mb': g('VmPeak')}


def _top(snap, n=15):
    out = []
    for st in snap.statistics('lineno')[:n]:
        fr = st.traceback[0]
        out.append({'where': f'{os.path.basename(fr.filename)}:{fr.lineno}', 'kb': round(st.size / 1024.0), 'n': st.count})
    return out


def _fake_ccxt(xdir, ex_id, on_first):
    import numpy as np
    import research_xdata as X
    idx = X.load_index(xdir)
    mk = {s: {'swap': True, 'linear': True, 'quote': 'USDT', 'settle': 'USDT', 'base': v['base'], 'active': v['active'], 'info': {'symbol': s}} for s, v in idx['markets'][ex_id].items()}
    state = {'first': False}

    class Ex:
        async def load_markets(self):
            return mk

        async def fetch_ohlcv(self, sym, tf, since=None, limit=None, params=None):
            if not state['first']:
                state['first'] = True
                on_first()
            end = (params or {}).get('endTime')
            n = 400
            now = int(np.datetime64('2025-06-30', 'D').astype(np.int64)) * 86_400_000
            ts = [now - (n - 1 - i) * 86_400_000 for i in range(n)]
            rows = [[t, 1.0, 1.1, 0.9, 1.0, 100.0] for t in ts if end is None or t <= end]
            return rows[-limit:]

        async def fetch_funding_rate_history(self, sym, since=None, limit=None, params=None):
            return []

        async def close(self):
            return None
    return Ex()


def run_stage(stage, xdir, bdir, trace=True, limit_mb=None):
    """Выполняет этап в текущем процессе и возвращает {'peak_mb' (tracemalloc), 'top', 'vm', 'at_first' (для загрузки), 'secs', 'text'}."""
    import tracemalloc
    sys.path.insert(0, ROOT)
    sys.path.insert(0, HERE)
    if limit_mb:
        import resource
        resource.setrlimit(resource.RLIMIT_AS, (int(limit_mb * 1048576),) * 2)
    import research_xdata as X                                     # как в research_runner: каждая задача импортирует только свои модули
    if stage in ('xfund_load', 'h2_load'):
        import importlib
        for m_ in ('ccxt.async_support', 'research_okx', 'access_probe'):             # боевая загрузка создаёт собственные клиенты ccxt и импортирует эти модули
            importlib.import_module(m_)
    if stage in ('xfund_prelim', 'xfund_test'):
        import research_xfund as XF
    if stage == 'h2_show':
        import research_h2repl as H2R
        import research_factory as RF
    at_first = {}

    def on_first():
        at_first.update({'vm': _vm(), 'traced_mb': round(tracemalloc.get_traced_memory()[0] / 1048576, 1) if trace else None,
                         'top': _top(tracemalloc.take_snapshot(), 8) if trace else None})
    checkpoints = []

    def wrap(mod, name):
        orig = getattr(mod, name)

        def w(*a, **k):
            r = orig(*a, **k)
            if trace:
                checkpoints.append({'after': name, 'traced_mb': round(tracemalloc.get_traced_memory()[0] / 1048576, 1), 'peak_mb': round(tracemalloc.get_traced_memory()[1] / 1048576, 1),
                                    'rss_mb': round(_vm()['rss_mb'], 1), 'top': _top(tracemalloc.take_snapshot(), 15) if name == 'build_inputs' else None})
            else:
                checkpoints.append({'after': name, 'rss_mb': round(_vm()['rss_mb'], 1), 'hwm_mb': round(_vm()['hwm_mb'], 1)})
            return r
        setattr(mod, name, w)
    if stage in ('xfund_prelim', 'xfund_test'):
        for nm in ('load_venues', 'binance_venue', 'build_inputs', 'simulate_x1'):
            wrap(XF, nm)
    if trace:
        tracemalloc.start()
    t0 = time.time()
    text = ''
    os.environ['HTF_FETCH_PAUSE_SEC'] = '0'

    async def nosleep(_s):
        return None
    if stage in ('xfund_load', 'h2_load'):
        idx = X.load_index(xdir)
        if stage == 'xfund_load':                                   # «осталось 9»: 9 символов без записи в индексе
            for s in sorted(idx['bars']['bingx'])[:9]:
                X.put(xdir, idx, 'bars', 'bingx', s, {'first': 1, 'last': 1, 'n': 1, 't': 1})        # устаревшая запись (без complete) → будет загружена заново
        ex = {'bingx': _fake_ccxt(xdir, 'bingx', on_first)}
        venues = ('bingx',)
        if stage == 'h2_load':
            ex['okx'] = _fake_ccxt(xdir, 'okx', on_first)
            venues = ('bingx', 'okx')
        text = asyncio.run(X.run_xload({'xdir': xdir, 'bdir': bdir, 'venues': venues, 'x1_main': stage == 'xfund_load', 'hourly': False}, None, exchanges=ex,
                                       now_ms=(int(__import__('numpy').datetime64('2025-06-30', 'D').astype('int64')) + 1) * 86_400_000, sleep=nosleep))
    elif stage == 'xfund_prelim':
        traded, npairs = XF.prelim_traded(xdir, bdir, 0.0005)
        text = f'пар {npairs}, символов сделок {len(traded)}'
    elif stage == 'xfund_test':
        text = asyncio.run(XF.run_xfund({'xdir': xdir, 'bdir': bdir, 'fdir': os.path.join(xdir, 'fdir'), 'fee_taker': 0.0005, 'version': 'probe', 'x1_pair': 'binance', 'coverage_min': 0.0}))
    elif stage == 'h2_show':
        fdir = os.path.join(xdir, 'fdir')
        RF.kv_set('h2_funding_check', {'comb': 1.0, 'allow': True, 't': 0}, fdir)
        H2R.K_REPS = 3
        text = asyncio.run(H2R.run_h2repl({'xdir': xdir, 'bdir': bdir, 'fdir': fdir, 'fee_taker': 0.0005, 'fee_maker': 0.0002, 'version': 'probe', 'venues': ('bingx', 'okx'),
                                           'mode': 'show', 'coverage_min': 0.0}))
    out = {'stage': stage, 'secs': round(time.time() - t0, 1), 'vm': _vm(), 'at_first': at_first, 'checkpoints': checkpoints, 'text_tail': text[-300:]}
    if trace:
        cur, peak = tracemalloc.get_traced_memory()
        out.update({'peak_mb': round(peak / 1048576, 1), 'top': _top(tracemalloc.take_snapshot())})
    return out


def run_subprocess(stage, xdir, bdir, trace=True, limit_mb=None):
    sys.path.insert(0, ROOT)
    import research_runner as RR
    env = RR.child_env(limit_mb or 300)
    args = [sys.executable, os.path.join(HERE, 'memprobe.py'), stage, xdir, bdir, '1' if trace else '0'] + ([str(limit_mb)] if limit_mb else [])
    r = subprocess.run(args, capture_output=True, text=True, env=env, cwd=ROOT, timeout=1800)
    if r.returncode != 0:
        return {'stage': stage, 'error': f'код {r.returncode}: {r.stderr[-400:]}'}
    return json.loads(r.stdout.strip().splitlines()[-1])


if __name__ == '__main__':
    st, xd, bd = sys.argv[1:4]
    tr = (sys.argv[4] == '1') if len(sys.argv) > 4 else True
    lm = float(sys.argv[5]) if len(sys.argv) > 5 else None
    print(json.dumps(run_stage(st, xd, bd, tr, lm), ensure_ascii=False))
