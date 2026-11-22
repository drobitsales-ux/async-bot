"""research_leadlag.py — замер расхождений цен OKX ↔ BingX (BOT_SPEC §2.60), БЕЗ ТОРГОВЛИ.
Сборщик: раз в `step` с (по умолчанию 2) — пакетные тикеры OKX (прямой REST, все USDT-SWAP одним запросом) и BingX (`exchange_pub.fetch_tickers()`, один запрос, публичный канал без ключей),
7 суток; хранит на символ mid и половины спредов обеих бирж (12 байт на символ-тик), пишет пачками. ТОЛЬКО публичные GET — методов создания ордеров и приватных методов нет (тест проверяет исходники).
Отчёт `/leadlag_report`: распределение gap, события |gap| > C, длительность, «кто догоняет», исполнимость, критерий B3 (§2.60.3)."""
import asyncio
import json
import math
import os
import time

import numpy as np

import research_xdata as X

STEP_S = 2.0
STEP_MAX_S = 10.0
DAYS = 7
MIN_TURNOVER = 5_000_000.0
FLUSH_TICKS = 150
WARM_TICKS = 5
MAX_ABS_GAP_WARM = 0.05                  # медианный |gap| в первых тиках > 5% → масштаб/контракт не совпадает, символ исключается
BYTES_PER_SYM = 12
C_SET = (0.0015, 0.0025, 0.0040)
MIN_DUR_S = 4.0
EXEC_GAP_MIN = 0.0010
EXEC_PER_DAY_MIN = 20.0
EXEC_SYMS_MIN = 10
CONV_SHARE_MIN = 0.70
BREAK_STEPS = 3
DISK_CHECK_S = 600.0                      # §2.59.9 п. 5: проверка свободного места раз в 10 минут
DT = np.dtype([('mb', '<f4'), ('mo', '<f4'), ('sb', '<u2'), ('so', '<u2')])
SPREAD_UNIT = 1e-6                       # half-spread: доля mid в единицах 1e-6 (0.01 б.п.), uint16 → до 6.55%


def leadlag_dir():
    return os.getenv('LEADLAG_DIR') or ('/data/leadlag' if os.path.exists('/data') else '/tmp/leadlag')


def max_mb():
    try:
        return float(os.getenv('LEADLAG_MAX_MB', '500'))
    except ValueError:
        return 500.0


def estimate_bytes(n_syms, step=STEP_S, days=DAYS):
    """Оценка объёма заранее: символы × 12 байт × число тиков за период (+ 16 байт на тик — времена ответов бирж)."""
    ticks = days * 86400.0 / step
    return int(n_syms * BYTES_PER_SYM * ticks + 16 * ticks)


def max_syms_for(cap_mb, step=STEP_S, days=DAYS):
    ticks = days * 86400.0 / step
    return max(0, int((cap_mb * 1e6 - 16 * ticks) // (BYTES_PER_SYM * ticks)))


def spath(d, base):
    return os.path.join(d, f'sym_{base}.bin')


def state_path(d):
    return os.path.join(d, 'state.json')


def ts_path(d):
    return os.path.join(d, 'ts.bin')


def load_state(d):
    try:
        with open(state_path(d), encoding='utf-8') as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}


def save_state(d, st):
    os.makedirs(d, exist_ok=True)
    tmp = state_path(d) + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(st, f, ensure_ascii=False)
    os.replace(tmp, state_path(d))


def min_free_mb():
    """Минимум свободного места на томе сбора (§2.59.9 п. 5): ENV LEADLAG_MIN_FREE_MB, по умолчанию 200 МБ."""
    try:
        return float(os.getenv('LEADLAG_MIN_FREE_MB', '200'))
    except ValueError:
        return 200.0


def free_mb(d):
    """Свободное место на томе каталога d (МБ); каталога ещё нет — берётся ближайший существующий родитель. При ошибке — inf (проверка не блокирует)."""
    import shutil
    p = d
    while p and not os.path.exists(p) and os.path.dirname(p) != p:
        p = os.path.dirname(p)
    try:
        return shutil.disk_usage(p or '/').free / 1e6
    except OSError:
        return float('inf')


def dir_mb(d):
    tot = 0
    try:
        for fn in os.listdir(d):
            tot += os.path.getsize(os.path.join(d, fn))
    except OSError:
        pass
    return tot / 1e6


# ── источники (публичные GET) ──────────────────────────────────────────────────────────────────────
def _f(x):
    try:
        v = float(x)
        return v if math.isfinite(v) else None
    except (TypeError, ValueError):
        return None


async def okx_tickers(session, timeout=10.0):
    """OKX: GET /api/v5/market/tickers?instType=SWAP → {база: (bid, ask, оборот_24ч_USDT)}. Оборот линейного свопа = volCcy24h (в базовой монете) · last (формат по памяти, не проверен)."""
    import aiohttp
    async with session.get('https://www.okx.com/api/v5/market/tickers', params={'instType': 'SWAP'}, timeout=aiohttp.ClientTimeout(total=timeout)) as r:
        if r.status != 200:
            raise RuntimeError(f'okx HTTP {r.status}' + (' too many requests' if r.status == 429 else ''))
        j = await r.json(content_type=None)
    if str(j.get('code')) != '0':
        raise RuntimeError(f'okx {j.get("code")} {str(j.get("msg", ""))[:80]}')
    out = {}
    for d in j.get('data') or []:
        inst = str(d.get('instId') or '')
        if not inst.endswith('-USDT-SWAP'):
            continue
        bid, ask, last, vc = _f(d.get('bidPx')), _f(d.get('askPx')), _f(d.get('last')), _f(d.get('volCcy24h'))
        if bid and ask and bid > 0 and ask > 0:
            out[inst.split('-')[0]] = (bid, ask, (vc * last) if (vc is not None and last) else None)
    return out


async def bingx_tickers(ex):
    """BingX: `fetch_tickers()` публичного экземпляра (один запрос, все перпетуалы) → {база: (bid, ask, quoteVolume)}."""
    tk = await ex.fetch_tickers()
    out = {}
    for sym, t in (tk or {}).items():
        if not str(sym).endswith('/USDT:USDT'):
            continue
        bid, ask = _f(t.get('bid')), _f(t.get('ask'))
        if bid and ask and bid > 0 and ask > 0:
            out[str(sym).split('/')[0]] = (bid, ask, _f(t.get('quoteVolume')))
    return out


def is_rate_limited(e):
    t = f'{type(e).__name__} {e}'.lower()
    return any(x in t for x in ('429', 'too many requests', 'ratelimit', 'rate limit', '100410', '请稍后重试'))


# ── выбор символов ────────────────────────────────────────────────────────────────────────────────
def select_symbols(snap_b, snap_o, step=STEP_S, cap_mb=None, min_turnover=MIN_TURNOVER):
    """Общие базы (точное совпадение; не-крипто исключаются §2.52; оборот ≥ min_turnover на обеих биржах) → (список баз, справка). Объём — по оценке §2.60.1: при превышении LEADLAG_MAX_MB
    число символов урезается по убыванию минимального оборота."""
    common = sorted(set(snap_b) & set(snap_o))
    ref = {'common': len(common), 'noncrypto': 0, 'low_turnover': 0, 'trimmed': 0}
    cand = []
    for b in common:
        if X.noncrypto_reason(b):
            ref['noncrypto'] += 1
            continue
        qb, qo = snap_b[b][2], snap_o[b][2]
        if qb is None or qo is None or qb < min_turnover or qo < min_turnover:
            ref['low_turnover'] += 1
            continue
        cand.append((min(qb, qo), b))
    cand.sort(key=lambda x: (-x[0], x[1]))
    cap = max_mb() if cap_mb is None else cap_mb
    lim = max_syms_for(cap, step)
    if len(cand) > lim:
        ref['trimmed'] = len(cand) - lim
        cand = cand[:lim]
    return sorted(b for _q, b in cand), ref


def row_of(snap_b, snap_o, base):
    bb, ba, _ = snap_b[base]
    ob, oa, _ = snap_o[base]
    mb, mo = (bb + ba) / 2.0, (ob + oa) / 2.0
    sb = min(65535, int(round(((ba - bb) / 2.0 / mb) / SPREAD_UNIT)))
    so = min(65535, int(round(((oa - ob) / 2.0 / mo) / SPREAD_UNIT)))
    return mb, mo, sb, so


# ── сборщик ───────────────────────────────────────────────────────────────────────────────────────
class Collector:
    """get_b() и get_o() — async-функции без аргументов, возвращающие {база: (bid, ask, оборот)}. Для тестов подменяются; sleep/now тоже."""

    def __init__(self, d, get_b, get_o, step=STEP_S, days=DAYS, sleep=asyncio.sleep, now=time.time, notify=None):
        self.d, self.get_b, self.get_o = d, get_b, get_o
        self.step, self.days, self.sleep, self.now, self.notify = step, days, sleep, now, notify
        self.stop = False
        self.buf_ts, self.buf = [], []
        self.syms = []
        self.ticks = 0
        self.durs = []
        self.errors = {'rate': 0, 'other': 0}

    async def _say(self, m):
        if self.notify:
            try:
                await self.notify(m)
            except Exception:
                pass

    def repair(self):
        """После сбоя в середине записи: ts.bin и файлы символов приводятся к одному числу тиков (хвост обрезается), иначе строки разъедутся."""
        n = None
        sizes = []
        if os.path.exists(ts_path(self.d)):
            sizes.append(os.path.getsize(ts_path(self.d)) // 16)
        for b in self.syms:
            p = spath(self.d, b)
            sizes.append(os.path.getsize(p) // DT.itemsize if os.path.exists(p) else 0)
        n = min(sizes) if sizes else 0
        if os.path.exists(ts_path(self.d)):
            with open(ts_path(self.d), 'r+b') as f:
                f.truncate(n * 16)
        for b in self.syms:
            p = spath(self.d, b)
            if os.path.exists(p):
                with open(p, 'r+b') as f:
                    f.truncate(n * DT.itemsize)
        self.ticks = n
        return n

    def flush(self):
        if not self.buf_ts:
            return
        os.makedirs(self.d, exist_ok=True)
        arr = np.array(self.buf, dtype=DT)                                 # (тики, символы)
        for j, b in enumerate(self.syms):
            with open(spath(self.d, b), 'ab') as f:
                arr[:, j].tofile(f)
        with open(ts_path(self.d), 'ab') as f:                              # ts.bin пишется ПОСЛЕДНИМ: при обрыве repair() обрежет лишнее в файлах символов
            np.array(self.buf_ts, dtype='<i8').tofile(f)
        self.ticks += len(self.buf_ts)
        self.buf_ts, self.buf = [], []

    async def start(self, resume=False):
        """Новый сбор: выбор символов по первому снимку, оценка объёма, запись состояния. resume=True — продолжить по состоянию на диске."""
        st = load_state(self.d)
        if resume:
            self.syms = list(st.get('symbols') or [])
            self.step = float(st.get('step', self.step))
            self.repair()
            return st
        sb, so = await asyncio.gather(self.get_b(), self.get_o())
        syms, ref = select_symbols(sb, so, self.step)
        est = estimate_bytes(len(syms), self.step, self.days)
        now = self.now()
        st = {'running': True, 'started': now, 'until': now + self.days * 86400, 'step': self.step, 'symbols': syms, 'ref': ref, 'est_bytes': est, 'warm_done': False, 'excluded_scale': []}
        self.syms = syms
        save_state(self.d, st)
        return st

    async def run(self, resume=False):
        st = await self.start(resume)
        until = float(st['until'])
        warm_left = 0 if st.get('warm_done') else WARM_TICKS
        warm_rows = []
        next_disk = self.now() + DISK_CHECK_S
        while not self.stop and self.now() < until:
            t0 = self.now()
            rb, ro = await asyncio.gather(self.get_b(), self.get_o(), return_exceptions=True)
            tb = int(self.now() * 1000)
            bad = [r for r in (rb, ro) if isinstance(r, BaseException)]
            if bad:
                rate = any(is_rate_limited(e) for e in bad)
                self.errors['rate' if rate else 'other'] += 1
                if rate:
                    self.step = min(STEP_MAX_S, self.step * 1.5)             # §2.60.1: при лимите — пауза и шаг ×1.5 (до 10 с)
                    st['step'] = self.step
                    st['step_raised'] = st.get('step_raised', 0) + 1
                    save_state(self.d, st)
                await self.sleep(self.step)
                continue
            row, ok = [], 0
            for b in self.syms:
                try:
                    row.append(row_of(rb, ro, b))
                    ok += 1
                except KeyError:
                    row.append((float('nan'), float('nan'), 0, 0))
            self.buf.append(row)
            self.buf_ts.append((tb, int(self.now() * 1000)))
            if warm_left:
                warm_rows.append(row)
                warm_left -= 1
                if warm_left == 0:                                           # §2.60.1: символы с медианным |gap| в первых тиках > 5% исключаются
                    keep = self._warm_filter(warm_rows)
                    st['excluded_scale'] = [self.syms[j] for j in range(len(self.syms)) if j not in keep]
                    self._drop_cols(keep)
                    st['symbols'] = list(self.syms)
                    st['warm_done'] = True
                    save_state(self.d, st)
                    await self._say(f'📈 /leadlag: после прогрева символов {len(self.syms)} (исключено по масштабу: {len(st["excluded_scale"])})')
            if self.now() >= next_disk:                                      # §2.59.9 п. 5: свободно < 200 МБ — запись прекращается, сообщение в Telegram
                next_disk = self.now() + DISK_CHECK_S
                fr = free_mb(self.d)
                if fr < min_free_mb():
                    self.flush()
                    st['running'] = False
                    st['stopped'] = f'мало места на диске: свободно {fr:.0f} МБ < {min_free_mb():.0f} МБ'
                    st['ticks'] = self.ticks
                    save_state(self.d, st)
                    await self._say(f'⛔ /leadlag: свободно {fr:.0f} МБ < {min_free_mb():.0f} МБ — запись прекращена (данные до этого момента сохранены; /leadlag_report)')
                    break
            if len(self.buf_ts) >= FLUSH_TICKS and not warm_left:
                self.flush()
                st['ticks'] = self.ticks
                st['last_ts'] = tb
                if dir_mb(self.d) > max_mb():
                    st['running'] = False
                    st['stopped'] = 'лимит LEADLAG_MAX_MB'
                    save_state(self.d, st)
                    await self._say(f'⛔ /leadlag: достигнут лимит {max_mb():.0f} МБ — запись прекращена')
                    break
                save_state(self.d, st)
            self.durs.append(self.now() - t0)
            await self.sleep(max(0.0, self.step - (self.now() - t0)))
        self.flush()
        st = load_state(self.d) or st
        st['ticks'] = self.ticks
        if self.stop:
            st['running'] = False
            st['stopped'] = 'остановлено командой'
        elif self.now() >= until:
            st['running'] = False
            st['stopped'] = 'срок 7 суток истёк'
        if self.durs:
            st['median_cycle_s'] = float(np.median(self.durs))
        save_state(self.d, st)
        return st

    def _warm_filter(self, rows):
        a = np.array(rows, dtype=DT)
        keep = set()
        for j in range(a.shape[1]):
            g = np.abs(a['mb'][:, j].astype(float) / a['mo'][:, j].astype(float) - 1.0)
            g = g[np.isfinite(g)]
            if len(g) and float(np.median(g)) <= MAX_ABS_GAP_WARM:
                keep.add(j)
        return keep

    def _drop_cols(self, keep):
        idx = sorted(keep)
        self.syms = [self.syms[j] for j in idx]
        self.buf = [[r[j] for j in idx] for r in self.buf]


def status_text(d):
    st = load_state(d)
    if not st:
        return 'Сбор не запускался (/leadlag_start).'
    t = time.time()
    left = max(0.0, float(st.get('until', 0)) - t) / 86400
    return (f'{"▶️ идёт" if st.get("running") else "⏹ не идёт"} сбор lead-lag OKX↔BingX: символов {len(st.get("symbols") or [])}, тиков {st.get("ticks", 0)}, осталось {left:.1f} сут., '
            f'шаг {st.get("step")} с, занято сборщиком {dir_mb(d):.0f} МБ (лимит {max_mb():.0f}), свободно на томе {free_mb(d):.0f} МБ (минимум {min_free_mb():.0f})' + (f', причина остановки: {st["stopped"]}' if st.get('stopped') else '')
            + (f', повышений шага из-за лимита: {st["step_raised"]}' if st.get('step_raised') else '') + (f', медианная длительность цикла {st["median_cycle_s"]:.2f} с' if st.get('median_cycle_s') else ''))


# ── отчёт ──────────────────────────────────────────────────────────────────────────────────────────
def load_ts(d):
    p = ts_path(d)
    if not os.path.exists(p):
        return np.zeros((0, 2), np.int64)
    a = np.fromfile(p, dtype='<i8')
    return a[:len(a) // 2 * 2].reshape(-1, 2)


def load_sym(d, base, n):
    p = spath(d, base)
    a = np.fromfile(p, dtype=DT) if os.path.exists(p) else np.zeros(0, DT)
    return a[:n]


def find_events(gap, t, c, step):
    """События |gap| > c: подряд идущие тики над порогом (разрыв времени > 3·step разрывает). → список (first, last, end) индексов; end — первый тик под порогом сразу после last
    (события, не закончившиеся до конца данных или оборванные разрывом, не включаются)."""
    n = len(gap)
    ok = np.isfinite(gap)
    a = ok & (np.abs(np.where(ok, gap, 0.0)) > c)
    idx = np.flatnonzero(a)
    if not len(idx):
        return []
    dt = np.diff(t)
    brk_after = np.concatenate([dt > BREAK_STEPS * step, [True]])           # между i и i+1 разрыв
    runs = []
    s = idx[0]
    prev = idx[0]
    for i in idx[1:]:
        if i == prev + 1 and not brk_after[prev]:
            prev = i
            continue
        runs.append((s, prev))
        s, prev = i, i
    runs.append((s, prev))
    out = []
    for f, l in runs:
        if f > 0 and brk_after[f - 1] and a[f - 1]:                           # событие началось до разрыва данных — начало неизвестно
            continue
        e = l + 1
        if e >= n or brk_after[l] or not ok[e] or a[e]:
            continue
        out.append((int(f), int(l), int(e)))
    return out


def analyse_symbol(arr, ts, step, c, fee, min_dur=MIN_DUR_S):
    """Одна пара (символ, C) → dict: события, исполнимые события, доля «BingX сходится», длительности, реализованный ход одной ноги (справочно)."""
    t = (ts[:, 0] + ts[:, 1]) / 2000.0
    mb, mo = arr['mb'].astype(np.float64), arr['mo'].astype(np.float64)
    gap = mb / mo - 1.0
    hsb = arr['sb'].astype(np.float64) * SPREAD_UNIT
    ev = find_events(gap, t, c, step)
    res = {'events': len(ev), 'exec': 0, 'b_conv': 0, 'dur_exec': [], 'dur_all': [], 'real': []}
    for f, l, e in ev:
        dur = float(t[l] - t[f])
        res['dur_all'].append(dur)
        if dur < min_dur:
            continue
        j = f + int(np.searchsorted(t[f:l + 1] - t[f], min_dur, side='left'))      # первый тик с возрастом события ≥ 4 с
        if j > l:
            continue
        sig = 1.0 if gap[f] > 0 else -1.0
        eg = abs(gap[j]) - hsb[j] - hsb[l] - 2.0 * fee                           # §2.60.2: вход j, выход — последний тик над порогом l
        if not (eg > EXEC_GAP_MIN):
            continue
        res['exec'] += 1
        res['dur_exec'].append(dur)
        db, do = mb[e] / mb[f] - 1.0, mo[e] / mo[f] - 1.0
        if (-sig * db) > (sig * do):
            res['b_conv'] += 1
        res['real'].append(-sig * (mb[e] / mb[j] - 1.0) - hsb[j] - hsb[e] - 2.0 * fee)
    return res


def report(d, fee, params=None):
    """Отчёт B2/B3 по собранным данным (§2.60.2–§2.60.3). Символы обрабатываются по одному (память)."""
    st = load_state(d)
    ts = load_ts(d)
    if not st or not len(ts):
        return '⛔ /leadlag_report: данных нет (сначала /leadlag_start)'
    step = float(st.get('step', STEP_S))
    syms = list(st.get('symbols') or [])
    n = len(ts)
    for b in syms:
        n = min(n, os.path.getsize(spath(d, b)) // DT.itemsize if os.path.exists(spath(d, b)) else 0)
    ts = ts[:n]
    t = (ts[:, 0] + ts[:, 1]) / 2000.0
    dtt = np.diff(t)
    covered_days = float(dtt[dtt <= BREAK_STEPS * step].sum()) / 86400.0 if n > 1 else 0.0
    total_days = float(t[-1] - t[0]) / 86400.0 if n > 1 else 0.0
    L = ['📈 <b>/leadlag_report — расхождения цен OKX ↔ BingX (BOT_SPEC §2.60, без торговли)</b>',
         f'Тиков {n}, символов {len(syms)}, период {total_days:.2f} сут. (с данными {covered_days:.2f} сут.), шаг {step} с (медиана цикла {st.get("median_cycle_s", float("nan")):.2f} с); '
         f'медианный интервал между тиками {float(np.median(dtt)) if len(dtt) else float("nan"):.2f} с, разрывов (> {BREAK_STEPS}·шаг) {int((dtt > BREAK_STEPS * step).sum()) if len(dtt) else 0}.',
         f'Отбор при старте: общих баз {st.get("ref", {}).get("common")}, не-крипто {st.get("ref", {}).get("noncrypto")}, оборот < 5 млн $ {st.get("ref", {}).get("low_turnover")}, урезано по объёму '
         f'{st.get("ref", {}).get("trimmed")}; исключено по масштабу (|gap| > 5% в прогреве) {len(st.get("excluded_scale") or [])}. Комиссия taker BingX {fee * 100:.3f}% (×2: вход и выход).']
    if covered_days <= 0:
        return '\n'.join(L + ['Данных для расчёта недостаточно.'])
    stats, per = {}, {}
    for b in syms:
        arr = load_sym(d, b, n)
        gap = arr['mb'].astype(np.float64) / arr['mo'].astype(np.float64) - 1.0
        g = gap[np.isfinite(gap)]
        if not len(g):
            continue
        ag = np.abs(g)
        stats[b] = {'med': float(np.median(g)), 'p5': float(np.percentile(g, 5)), 'p95': float(np.percentile(g, 95)), 'abs99': float(np.percentile(ag, 99)),
                    'hsb': float(np.median(arr['sb'].astype(np.float64)) * SPREAD_UNIT), 'hso': float(np.median(arr['so'].astype(np.float64)) * SPREAD_UNIT)}
        per[b] = {c: analyse_symbol(arr, ts, step, c, fee) for c in C_SET}
        del arr, gap, g, ag
    L.append('')
    L.append('<b>Распределение gap = mid_BingX / mid_OKX − 1</b> (медианы по символам): ' + f'медиана gap {np.median([v["med"] for v in stats.values()]) * 1e4:+.2f} б.п.; '
             f'|gap| p99 — медиана по символам {np.median([v["abs99"] for v in stats.values()]) * 1e4:.1f} б.п.; половина спреда BingX {np.median([v["hsb"] for v in stats.values()]) * 1e4:.2f} б.п., OKX '
             f'{np.median([v["hso"] for v in stats.values()]) * 1e4:.2f} б.п.')
    verdicts = []
    for c in C_SET:
        ex_day = {b: per[b][c]['exec'] / covered_days for b in per}
        good = [b for b in per if ex_day[b] >= EXEC_PER_DAY_MIN]
        n_ev = sum(per[b][c]['events'] for b in per)
        n_ex = sum(per[b][c]['exec'] for b in per)
        pooled = sum(per[b][c]['b_conv'] for b in good)
        pooled_n = sum(per[b][c]['exec'] for b in good)
        share = (pooled / pooled_n) if pooled_n else float('nan')
        dur_all = [x for b in per for x in per[b][c]['dur_all']]
        dur_ex = [x for b in per for x in per[b][c]['dur_exec']]
        real = [x for b in per for x in per[b][c]['real']]
        conv_all = sum(per[b][c]['b_conv'] for b in per)
        ok = len(good) >= EXEC_SYMS_MIN and pooled_n > 0 and share >= CONV_SHARE_MIN
        verdicts.append(ok)
        L.append('')
        L.append(f'<b>C = {c * 100:.2f}%</b>: событий {n_ev} ({n_ev / covered_days:.1f} в сутки по всем символам), медианная длительность {np.median(dur_all) if dur_all else float("nan"):.1f} с; '
                 f'исполнимых (exec_gap > {EXEC_GAP_MIN * 100:.2f}%, ≥ {MIN_DUR_S:.0f} с) {n_ex} ({n_ex / covered_days:.2f} в сутки всего), медианная длительность {np.median(dur_ex) if dur_ex else float("nan"):.1f} с, '
                 f'BingX сходится к OKX: {conv_all}/{n_ex}' + (f' ({conv_all / n_ex * 100:.0f}%)' if n_ex else '') + (f'; справочно реализованный ход одной ноги BingX (вход — тик возраста ≥ 4 с, выход — первый тик под порогом, '
                 f'за вычетом издержек): медиана {np.median(real) * 1e4:+.1f} б.п., доля > 0 {np.mean(np.array(real) > 0) * 100:.0f}%' if real else ''))
        top = sorted(per, key=lambda b: -ex_day[b])[:8]
        L.append('  символы по исполнимым событиям/сутки: ' + '; '.join(f'{b} {ex_day[b]:.1f}' for b in top))
        L.append(f'  B3: символов с ≥ {EXEC_PER_DAY_MIN:.0f} исполнимых событий в сутки: {len(good)} (нужно ≥ {EXEC_SYMS_MIN}); доля «BingX сходится» по ним {share * 100 if math.isfinite(share) else float("nan"):.0f}% (нужно ≥ {CONV_SHARE_MIN * 100:.0f}%) → '
                 + ('<b>критерий выполнен</b>' if ok else 'не выполнен'))
    L.append('')
    L.append('<b>ИТОГ B3 (§2.60.3):</b> ' + ('критерий выполнен хотя бы при одном C → «стоит проектировать стратегию» (только решение о проектировании; любая стратегия — отдельная пре-регистрация и shadow; 3 проверки без поправки на множественность — оговорка)'
                                        if any(verdicts) else 'критерий не выполнен ни при одном C → направление закрыто'))
    return '\n'.join(L)
