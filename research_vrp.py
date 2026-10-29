"""research_vrp.py — премия за волатильность (VRP): /vrp_probe, /vrp_measure, /vrp_sim (BOT_SPEC §2.46, пре-регистрация записана ДО кода). Индекс подразумеваемой волатильности Deribit DVOL (BTC, ETH),
VRP_t = DVOL_t − RV(t, t+30) по кэшу Binance D1, приближённая симуляция недельного «железного кондора». Торговых модулей опционов НЕТ; решения о торговле по результатам не принимаются.
HTTP вынесен в передаваемый fetch(url, params, method) → dict(status, headers, body, error) (как research_probe), чтобы тесты подставляли поддельные ответы."""
import json
import math
import os
import time
from datetime import datetime, timezone

import numpy as np

import research_bdata as B

DERIBIT = 'https://www.deribit.com/api/v2/public/get_volatility_index_data'
CURRENCIES = ('BTC', 'ETH')
RESOLUTIONS = ('1D', '86400')                          # второй вариант пробуется при HTTP 400 (формат не проверен, §2.46.1 п. 1)
BLOCKED = (403, 451)
DAY = 86_400_000
START_MS = int(datetime(2018, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
HORIZON = 30
GATE_T, GATE_SHARE = 2.0, 0.60
MAX_PAGES = 60
WEEK_DAYS = 7
SPREAD, FEE = 0.03, 0.0003
RISK_WEEK = 0.02
LAGS = (1, 7, 15, 30, 60)


def vrp_dir():
    return os.getenv('VRP_DIR') or ('/data/vrp' if os.path.exists('/data') else '/tmp/vrp')


def _d(ms):
    return datetime.fromtimestamp(ms / 1000, timezone.utc).strftime('%Y-%m-%d')


# ── Deribit ─────────────────────────────────────────────────────────────────────────────────────────
def parse_dvol(js):
    """Ответ Deribit → ({ts_ms: close}, continuation | None, ключи ответа для диагностики). Строки — [ts, open, high, low, close] или словари с полем close."""
    res = js.get('result') if isinstance(js, dict) else None
    if not isinstance(res, dict):
        return {}, None, sorted(js.keys()) if isinstance(js, dict) else []
    out = {}
    for row in res.get('data') or []:
        try:
            if isinstance(row, dict):
                out[int(row['timestamp'])] = float(row['close'])
            else:
                out[int(row[0])] = float(row[4])
        except (KeyError, IndexError, TypeError, ValueError):
            continue
    cont = res.get('continuation')
    return out, (int(cont) if isinstance(cont, (int, float)) and not isinstance(cont, bool) else None), sorted(res.keys())


async def fetch_dvol(fetch, cur, start_ms=START_MS, end_ms=None, max_pages=MAX_PAGES, pause=None):
    """История DVOL постранично назад от end_ms. → dict(rows {ts: close}, status (код первого ответа), resolution, pages, error, keys, blocked)."""
    end_ms = end_ms or int(time.time() * 1000)
    out = {'rows': {}, 'status': None, 'resolution': None, 'pages': 0, 'error': '', 'keys': [], 'blocked': False, 'body': ''}
    for res in RESOLUTIONS:
        out.update({'rows': {}, 'pages': 0, 'error': ''})
        cur_end = end_ms
        while out['pages'] < max_pages:
            r = await fetch(DERIBIT, {'currency': cur, 'start_timestamp': start_ms, 'end_timestamp': cur_end, 'resolution': res}, 'GET')
            if pause:
                await pause()
            out['pages'] += 1
            if out['pages'] == 1:
                out['status'] = r.get('status')
            if r.get('status') in BLOCKED:
                out.update({'blocked': True, 'error': f'HTTP {r["status"]}', 'body': (r.get('body') or b'')[:200].decode('utf-8', 'replace')})
                return out
            if r.get('error') or r.get('status') != 200:
                out['error'] = r.get('error') or f'HTTP {r.get("status")}'
                out['body'] = (r.get('body') or b'')[:200].decode('utf-8', 'replace')
                break
            try:
                js = json.loads(r['body'])
            except Exception:
                out['error'] = 'ответ не JSON'
                break
            rows, cont, keys = parse_dvol(js)
            out['keys'] = keys
            new = {k: v for k, v in rows.items() if start_ms <= k < cur_end + DAY}
            fresh = [k for k in new if k not in out['rows']]
            out['rows'].update(new)
            if not fresh:
                break
            nxt = cont if cont is not None and cont < cur_end else (min(new) - 1 if min(new) > start_ms else None)
            if nxt is None or nxt >= cur_end:
                break
            cur_end = nxt
        if out['rows']:
            out['resolution'] = res
            return out
        if out['status'] != 400:
            return out
    return out


def daily_from_rows(rows):
    """{ts: close} → {номер суток UTC: close}: свечи DVOL с началом в сутках t (если шаг данных меньше суток — close последней свечи суток)."""
    out = {}
    for ts in sorted(rows):
        out[int(ts // DAY)] = rows[ts]
    return out


def save_dvol(cur, rows, d=None):
    d = d or vrp_dir()
    os.makedirs(d, exist_ok=True)
    with open(os.path.join(d, f'dvol_{cur}.json'), 'w', encoding='utf-8') as f:
        json.dump({str(k): v for k, v in rows.items()}, f)


def load_dvol(cur, d=None):
    try:
        with open(os.path.join(d or vrp_dir(), f'dvol_{cur}.json'), encoding='utf-8') as f:
            return {int(k): float(v) for k, v in json.load(f).items()}
    except (OSError, ValueError):
        return {}


def step_info(rows):
    ks = sorted(rows)
    if len(ks) < 2:
        return None, 0
    dif = np.diff(ks)
    return float(np.median(dif)), int((dif > np.median(dif) * 1.5).sum())


def probe_report(res, now_ms, version=''):
    L = [f'🔌 <b>/vrp_probe — доступность DVOL (Deribit, публичный API без ключа)</b> | код {version}', f'метод: GET {DERIBIT} (currency, start_timestamp, end_timestamp, resolution)']
    blocked = False
    for cur in CURRENCIES:
        r = res[cur]
        if r['blocked']:
            blocked = True
            L.append(f'{cur}: HTTP {r["status"]} — ДОСТУП ЗАКРЫТ (451/403), дальше не идём. Ответ: {r["body"][:120]}')
            continue
        if not r['rows']:
            L.append(f'{cur}: данных нет; первый ответ HTTP {r["status"]}; ошибка: {r["error"] or "пустой result.data"}' + (f'; тело: {r["body"][:150]}' if r['body'] else '') + (f'; ключи result: {r["keys"]}' if r['keys'] else ''))
            continue
        ks = sorted(r['rows'])
        step, gaps = step_info(r['rows'])
        L.append(f'{cur}: HTTP {r["status"]}; resolution={r["resolution"]}; страниц {r["pages"]}; точек {len(ks)}; первая {_d(ks[0])}, последняя {_d(ks[-1])}; шаг (медиана) {step / 3_600_000:.2f} ч; '
                 f'разрывов шага {gaps}; последнее значение {r["rows"][ks[-1]]:.2f}; ключи result: {r["keys"]}')
    ok = all(res[c]['rows'] for c in CURRENCIES)
    if blocked:
        L.append('Вывод: публичный API Deribit с сервера бота НЕДОСТУПЕН (451/403) — измерение VRP невозможно, дальше не идём (§2.46 п. 1).')
    elif ok:
        L.append('Вывод: DVOL доступен для BTC и ETH — можно /vrp_measure.')
    else:
        L.append('Вывод: данные получены не по всем валютам — смотрите причины выше; формат запроса/ответа не проверялся ранее (§2.46.1 п. 1), возможна правка разбора.')
    return '\n'.join(L), ok


async def run_probe(params, progress=None, fetch=None):
    """/vrp_probe: коды ответов, глубина и шаг DVOL по BTC и ETH; результат сохраняется (данные — в кэш)."""
    own = fetch is None
    ses = None
    if own:
        import aiohttp
        import research_vision as V
        ses = aiohttp.ClientSession()
        fetch = V.make_fetcher(ses)
    import asyncio

    async def pause():
        if own:
            await asyncio.sleep(1.0)                           # пауза между запросами к публичному API; в тестах (поддельный fetch) без паузы
    now_ms = int(params.get('now_ms') or time.time() * 1000)
    try:
        res = {cur: await fetch_dvol(fetch, cur, START_MS, now_ms, pause=pause) for cur in CURRENCIES}
    finally:
        if ses is not None:
            await ses.close()
    d = params.get('vrp_dir') or vrp_dir()
    for cur in CURRENCIES:
        if res[cur]['rows']:
            save_dvol(cur, res[cur]['rows'], d)
    text, ok = probe_report(res, now_ms, params.get('version', ''))
    os.makedirs(d, exist_ok=True)
    with open(os.path.join(d, 'probe.json'), 'w', encoding='utf-8') as f:
        json.dump({'t': now_ms, 'ok': ok, 'blocked': any(res[c]['blocked'] for c in CURRENCIES)}, f)
    return text


# ── VRP ─────────────────────────────────────────────────────────────────────────────────────────────
def rv_forward(closes, t, h=HORIZON):
    """RV(t, t+h) в пунктах: ddof=1 по лог-доходностям ln(C_k/C_{k−1}), k = t+1…t+h, ×√365×100; closes — {сутки: close}; все h+1 close подряд обязательны, иначе None."""
    cs = []
    for k in range(t, t + h + 1):
        c = closes.get(k)
        if c is None or not (c > 0):
            return None
        cs.append(c)
    lr = np.diff(np.log(np.asarray(cs)))
    return float(lr.std(ddof=1) * math.sqrt(365.0) * 100.0)


def vrp_series(dvol, closes, h=HORIZON):
    """Сутки с вычислимым VRP: {days, dvol, rv, vrp} (массивы, по возрастанию). VRP_t = DVOL_t − RV(t, t+h)."""
    days, dv, rv = [], [], []
    for t in sorted(dvol):
        r = rv_forward(closes, t, h)
        if r is not None:
            days.append(t)
            dv.append(dvol[t])
            rv.append(r)
    dv, rv = np.asarray(dv, float), np.asarray(rv, float)
    return {'days': np.asarray(days, np.int64), 'dvol': dv, 'rv': rv, 'vrp': dv - rv}


def year_stats(ser):
    out = {}
    if not len(ser['days']):
        return out
    yrs = np.array([datetime.fromtimestamp(int(d) * DAY / 1000, timezone.utc).year for d in ser['days']])
    for y in sorted(set(yrs.tolist())):
        v = ser['vrp'][yrs == y]
        out[int(y)] = {'n': int(len(v)), 'mean': float(v.mean()), 'median': float(np.median(v)), 'share_pos': float((v > 0).mean())}
    return out


def nonoverlap_t(ser, offset=0, step=HORIZON):
    """Неперекрывающиеся точки: t₀ = первые сутки с вычислимым VRP (+ offset), далее каждые `step` суток, только вычислимые. → dict(n, mean, sd, t)."""
    if not len(ser['days']):
        return {'n': 0, 'mean': None, 'sd': None, 't': float('nan')}
    pos = {int(d): i for i, d in enumerate(ser['days'])}
    d = int(ser['days'][0]) + offset
    last = int(ser['days'][-1])
    vals = []
    while d <= last:
        if d in pos:
            vals.append(float(ser['vrp'][pos[d]]))
        d += step
    n = len(vals)
    if n < 3:
        return {'n': n, 'mean': (float(np.mean(vals)) if vals else None), 'sd': None, 't': float('nan')}
    a = np.asarray(vals)
    sd = float(a.std(ddof=1))
    return {'n': n, 'mean': float(a.mean()), 'sd': sd, 't': (float(a.mean() / (sd / math.sqrt(n))) if sd > 0 else float('nan'))}


def sensitivity(ser):
    ts = [nonoverlap_t(ser, o)['t'] for o in range(HORIZON)]
    ts = [t for t in ts if math.isfinite(t)]
    return (float(min(ts)), float(np.median(ts)), float(max(ts))) if ts else (None, None, None)


def acf(ser, lags=LAGS):
    """Автокорреляция VRP по суткам (пары, где обе вычислимы)."""
    pos = {int(d): float(v) for d, v in zip(ser['days'], ser['vrp'])}
    out = {}
    for lag in lags:
        a, b = [], []
        for d, v in pos.items():
            if d + lag in pos:
                a.append(v)
                b.append(pos[d + lag])
        out[lag] = float(np.corrcoef(a, b)[0, 1]) if len(a) > 5 and np.std(a) > 0 and np.std(b) > 0 else None
    return out


def worst_n(ser, n=10):
    idx = np.argsort(ser['vrp'])[:n]
    return [(int(ser['days'][i]), float(ser['vrp'][i]), float(ser['dvol'][i]), float(ser['rv'][i])) for i in idx]


def top_drops(ser, closes, n=10, gap=HORIZON):
    """10 наименьших НЕПЕРЕСЕКАЮЩИХСЯ (даты не ближе gap суток) 30-дневных доходностей BTC вперёд ln(C_{t+30}/C_t) среди суток с вычислимым VRP (в периоде DVOL). → [(сутки, ret, DVOL, RV, VRP)]."""
    cand = []
    for i, t in enumerate(ser['days']):
        c0, c1 = closes.get(int(t)), closes.get(int(t) + HORIZON)
        if c0 and c1 and c0 > 0 and c1 > 0:
            cand.append((math.log(c1 / c0), i))
    cand.sort()
    out, taken = [], []
    for r, i in cand:
        t = int(ser['days'][i])
        if all(abs(t - u) >= gap for u in taken):
            out.append((t, float(r), float(ser['dvol'][i]), float(ser['rv'][i]), float(ser['vrp'][i])))
            taken.append(t)
        if len(out) >= n:
            break
    return out


def measure(dvol, closes):
    """Все статистики §2.46 п. 2 для одной валюты."""
    ser = vrp_series(dvol, closes)
    no = nonoverlap_t(ser)
    share = float((ser['vrp'] > 0).mean()) if len(ser['vrp']) else float('nan')
    gate = bool(no['mean'] is not None and no['mean'] > 0 and math.isfinite(no['t']) and no['t'] >= GATE_T and math.isfinite(share) and share >= GATE_SHARE)
    return {'ser': ser, 'n_days': int(len(ser['days'])), 'years': year_stats(ser), 'share_pos': share, 'no': no, 'sens': sensitivity(ser), 'acf': acf(ser), 'worst': worst_n(ser),
            'drops': top_drops(ser, closes), 'gate': gate, 'mean_all': (float(ser['vrp'].mean()) if len(ser['vrp']) else None), 'median_all': (float(np.median(ser['vrp'])) if len(ser['vrp']) else None)}


def load_closes(bdir, sym):
    z = B.load_d1(bdir, sym)
    if z is None or not len(z['ts']):
        return {}
    return {int(t // DAY): float(c) for t, c in zip(z['ts'], z['c'])}


def fm(x, nd=2):
    return 'н/д' if x is None or (isinstance(x, float) and not math.isfinite(x)) else f'{x:.{nd}f}'


def measure_report(m_by_cur, notes, version=''):
    L = [f'📐 <b>/vrp_measure — премия за волатильность VRP_t = DVOL_t − RV(t, t+30)</b> (BOT_SPEC §2.46) | код {version}',
         'DVOL — пункты годовой волатильности (%); RV — ddof=1 по дневным лог-доходностям t+1…t+30 (Binance D1), ×√365×100. Окна перекрываются: t считается по неперекрывающимся точкам (каждые 30 суток от первых вычислимых).']
    L += notes
    for cur, m in m_by_cur.items():
        no = m['no']
        L += ['', f'<b>{cur}</b>: суток с вычислимым VRP {m["n_days"]}; среднее {fm(m["mean_all"])}, медиана {fm(m["median_all"])} п.; доля дней VRP &gt; 0: {fm(m["share_pos"] * 100 if math.isfinite(m["share_pos"]) else None, 1)}%',
              f'  неперекрывающиеся точки: n {no["n"]}, среднее {fm(no["mean"])} п., sd {fm(no["sd"])}, t = {fm(no["t"])} (сдвиг начала 0..29: min/медиана/max t = ' +
              ' / '.join(fm(x) for x in m['sens']) + ')',
              '  по годам (среднее / медиана / доля &gt; 0): ' + '; '.join(f'{y}: {v["mean"]:.1f} / {v["median"]:.1f} / {v["share_pos"] * 100:.0f}% (n {v["n"]})' for y, v in m['years'].items()),
              '  автокорреляция VRP по суткам: ' + ', '.join(f'лаг {k}: {fm(v)}' for k, v in m['acf'].items()),
              '  худшие 10 значений VRP: ' + '; '.join(f'{_d(d * DAY)}: {v:.1f} (DVOL {a:.1f}, RV {b:.1f})' for d, v, a, b in m['worst'])]
        if cur == 'BTC':
            L.append('  перед крупнейшими падениями BTC (10 непересекающихся 30-дневных окон в периоде DVOL): ' +
                     '; '.join(f'{_d(d * DAY)}: ret30 {r * 100:+.0f}%, DVOL {a:.1f}, RV {b:.1f}, VRP {v:+.1f}' for d, r, a, b, v in m['drops']))
    b = m_by_cur.get('BTC')
    if b is not None:
        no = b['no']
        L += ['', f'<b>Гейт для /vrp_sim (BTC)</b>: средний VRP по неперекрывающимся точкам &gt; 0: {fm(no["mean"])} [{"да" if (no["mean"] or 0) > 0 else "НЕТ"}]; t ≥ {GATE_T:g}: {fm(no["t"])} '
              f'[{"да" if math.isfinite(no["t"]) and no["t"] >= GATE_T else "НЕТ"}]; доля VRP &gt; 0 ≥ {GATE_SHARE * 100:.0f}%: {fm(b["share_pos"] * 100 if math.isfinite(b["share_pos"]) else None, 1)}% '
              f'[{"да" if math.isfinite(b["share_pos"]) and b["share_pos"] >= GATE_SHARE else "НЕТ"}] → <b>{"ГЕЙТ ПРОЙДЕН: можно /vrp_sim" if b["gate"] else "ГЕЙТ НЕ ПРОЙДЕН: /vrp_sim не запускается"}</b>',
              'Оговорка: положительная средняя премия не означает прибыльную торговлю; левый хвост (обвалы) определяет результат; «не пройден» = «премия не доказана на этих данных».']
    return '\n'.join(L)


def prepare(params):
    """Данные для измерения: (dvol {cur: {сутки: close}}, closes {cur: {…}}, заметки, ошибка | None). DVOL — из кэша пробы."""
    d = params.get('vrp_dir') or vrp_dir()
    bdir = params.get('bdir') or B.battery_dir()
    try:
        with open(os.path.join(d, 'probe.json'), encoding='utf-8') as f:
            pr = json.load(f)
    except (OSError, ValueError):
        pr = None
    if pr is None:
        return None, None, [], '⛔ сначала /vrp_probe (результат пробы не найден)'
    if pr.get('blocked'):
        return None, None, [], '⛔ /vrp_probe показал HTTP 451/403 — доступ к Deribit закрыт, дальше не идём (§2.46 п. 1)'
    dvol, closes, notes = {}, {}, []
    for cur in CURRENCIES:
        rows = load_dvol(cur, d)
        if not rows:
            return None, None, [], f'⛔ нет данных DVOL {cur} в кэше — повторите /vrp_probe'
        dvol[cur] = daily_from_rows(rows)
        closes[cur] = load_closes(bdir, f'{cur}USDT')
        if not closes[cur]:
            return None, None, [], f'⛔ нет дневных свечей {cur}USDT в кэше /battery_load ({bdir})'
        notes.append(f'{cur}: DVOL {len(dvol[cur])} суток ({_d(min(dvol[cur]) * DAY)} … {_d(max(dvol[cur]) * DAY)}); Binance D1 {len(closes[cur])} суток ({_d(min(closes[cur]) * DAY)} … {_d(max(closes[cur]) * DAY)})')
    return dvol, closes, notes, None


async def run_measure(params, progress=None):
    dvol, closes, notes, err = prepare(params)
    if err:
        return err
    m = {cur: measure(dvol[cur], closes[cur]) for cur in CURRENCIES}
    return measure_report(m, notes, params.get('version', ''))


# ── кондор ──────────────────────────────────────────────────────────────────────────────────────────
def _cdf(x):
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def bs_call(s, k, iv, t):
    if t <= 0 or iv <= 0:
        return max(s - k, 0.0)
    v = iv * math.sqrt(t)
    d1 = (math.log(s / k) + 0.5 * v * v) / v
    return s * _cdf(d1) - k * _cdf(d1 - v)


def bs_put(s, k, iv, t):
    return bs_call(s, k, iv, t) - s + k                    # паритет при r = q = 0


def condor_week(s0, st, dvol_pct, spread=SPREAD, fee=FEE, risk=RISK_WEEK):
    """Недельный железный кондор (§2.46.1 п. 7): короткие страйки S·e^{±σ_w}, защита S·e^{±2σ_w}, σ_w = DVOL/100·√(7/365); Блэк–Шоулз r = q = 0, IV = DVOL. P&L на единицу (USD на 1 BTC).
    → dict(pnl_gross, costs, pnl_net, maxloss, ret) | None (нет разумного максимального убытка)."""
    iv = dvol_pct / 100.0
    t = WEEK_DAYS / 365.0
    sw = iv * math.sqrt(t)
    k1c, k2c, k1p, k2p = s0 * math.exp(sw), s0 * math.exp(2 * sw), s0 * math.exp(-sw), s0 * math.exp(-2 * sw)
    c1, c2, p1, p2 = bs_call(s0, k1c, iv, t), bs_call(s0, k2c, iv, t), bs_put(s0, k1p, iv, t), bs_put(s0, k2p, iv, t)
    credit = c1 + p1 - c2 - p2
    costs = spread * (c1 + p1 + c2 + p2) + 4 * fee * s0
    payoff = -max(st - k1c, 0.0) + max(st - k2c, 0.0) - max(k1p - st, 0.0) + max(k2p - st, 0.0)
    gross = credit + payoff
    net = gross - costs
    maxloss = max(k2c - k1c, k1p - k2p) - (credit - costs)
    if not (maxloss > 0):
        return None
    return {'pnl_gross': gross, 'costs': costs, 'pnl_net': net, 'maxloss': maxloss, 'ret': risk * net / maxloss, 'ret_gross': risk * gross / maxloss}


def sim_condor(closes, dvol):
    """Каждые 7 суток от первых суток с DVOL: вход по close суток t, экспирация по close суток t+7. → список (сутки, ret, dict недели)."""
    out = []
    if not dvol:
        return out
    t = min(d for d in dvol if closes.get(d))
    last = max(closes)
    while t + WEEK_DAYS <= last:
        if t in dvol and closes.get(t) and closes.get(t + WEEK_DAYS):
            w = condor_week(closes[t], closes[t + WEEK_DAYS], dvol[t])
            if w is not None:
                out.append((t, w['ret'], w))
        t += WEEK_DAYS
    return out


def condor_stats(weeks):
    r = np.array([w[1] for w in weeks], float)
    n = len(r)
    if n < 2:
        return None
    eq = np.cumprod(1.0 + r)
    peak = np.maximum.accumulate(np.r_[1.0, eq])[1:]
    years = n * WEEK_DAYS / 365.0
    sd = float(r.std(ddof=1))
    return {'n': n, 'cagr': float(eq[-1] ** (1.0 / years) - 1.0) if eq[-1] > 0 else -1.0, 'mean_x52': float(r.mean() * 52), 'sharpe': (float(r.mean() / sd * math.sqrt(52)) if sd > 0 else float('nan')),
            'max_dd': float(np.max(1.0 - eq / peak)), 'worst': float(r.min()), 'worst_day': int(weeks[int(r.argmin())][0]), 'share_loss': float((r < 0).mean()),
            'gross_mean': float(np.mean([w[2]['ret_gross'] for w in weeks])), 'net_mean': float(r.mean())}


def sim_report(st, gate_lines, version=''):
    pc = lambda x: f'{x * 100:.1f}%'
    L = [f'🧪 <b>/vrp_sim — ПРИБЛИЖЁННАЯ симуляция недельного железного кондора на BTC</b> (BOT_SPEC §2.46) | код {version}',
         '⚠ <b>приближение: без перекоса по страйкам и временной структуры; решения о торговле по нему не принимаются.</b> '
         'Определения: вход — close суток t каждые 7 суток, экспирация — close t+7 (дневная сетка); короткие страйки ±1σ_w, защита ±2σ_w, σ_w = DVOL/100·√(7/365); Блэк–Шоулз, r = 0, IV = DVOL (плоская); '
         'издержки: 3% от премии каждой из 4 ног + 0.03% от номинала на ногу (один раз, при входе); риск на неделю — максимальный убыток кондора = 2% текущего капитала (сложный процент).']
    L += gate_lines
    if st is None:
        L.append('Недель слишком мало для статистики.')
        return '\n'.join(L)
    L += [f'Недель: {st["n"]}; доходность в год (CAGR): {pc(st["cagr"])} (среднее за неделю ×52: {pc(st["mean_x52"])}); Шарп (недельный, ×√52): {fm(st["sharpe"])}; макс. просадка: {pc(st["max_dd"])}; '
          f'худшая неделя: {pc(st["worst"])} ({_d(st["worst_day"] * DAY)}); доля убыточных недель: {pc(st["share_loss"])}',
          f'Справочно: среднее за неделю до издержек {st["gross_mean"] * 100:.3f}% капитала, после издержек {st["net_mean"] * 100:.3f}%.',
          'Оговорки: приближение (см. выше); реальные спреды, маржа, перекос и ранний вызов не учтены; левый хвост — разовые обвалы; решений о торговле нет, торговых модулей опционов в деплое нет.']
    return '\n'.join(L)


async def run_sim(params, progress=None):
    """/vrp_sim: ТОЛЬКО при пройденном гейте BTC (§2.46 п. 3), иначе отказ с причинами."""
    dvol, closes, notes, err = prepare(params)
    if err:
        return err
    m = measure(dvol['BTC'], closes['BTC'])
    no = m['no']
    gate_lines = [f'Гейт BTC: среднее VRP (неперекрывающиеся) {fm(no["mean"])} п., t {fm(no["t"])}, доля VRP &gt; 0 {fm(m["share_pos"] * 100 if math.isfinite(m["share_pos"]) else None, 1)}%']
    if not m['gate']:
        return ('⛔ /vrp_sim отказ: гейт §2.46 п. 3 не пройден (нужно: среднее VRP &gt; 0 по неперекрывающимся точкам с t ≥ 2.0 И доля VRP &gt; 0 ≥ 60%). ' + gate_lines[0] +
                '. Симуляция не запускается; решения о торговле не принимаются.')
    return sim_report(condor_stats(sim_condor(closes['BTC'], dvol['BTC'])), gate_lines, params.get('version', ''))
