"""research_h1err.py — диагностика и классификация ошибок загрузки 1h BingX (BOT_SPEC §2.57): `/xfund_1h_errors` (диагностика, только публичное чтение) и `/xfund_1h_errors apply` (исходы по классам
а1/а2/б/в/г). Правило записано в BOT_SPEC ДО этого кода; шаблоны текста НЕ проверены на реальных ответах BingX."""
import asyncio
import re
import time
from datetime import datetime, timezone

import numpy as np

import research_xdata as X

DAY = X.DAY
HOUR = X.HOUR
RETRY_V = (3, 8, 20, 45)                        # §2.57 в): паузы повторов
NODATA_RE = re.compile(r'no data|not exist|does not exist|invalid symbol|symbol .{0,20}(closed|offline|delist)|out of (range|history)|time range|startTime|endTime|too early|beyond', re.I)
TRANSIENT_RE = re.compile(r'rate.?limit|too many|timeout|timed out|busy|temporar|try again|unavailable|internal (server )?error|5\d\d', re.I)
CODE_RE = re.compile(r'"code"\s*:\s*"?(-?\d+)')
MSG_RE = re.compile(r'"msg"\s*:\s*"([^"]*)"')


def code_text(e):
    """(код ответа BingX | '', текст) из исключения ccxt; без ключей (публичный API)."""
    raw = ' '.join(str(e).split())
    m, t = CODE_RE.search(raw), MSG_RE.search(raw)
    return (m.group(1) if m else ''), ((t.group(1) if t else raw)[:200])


def iso(ms):
    return datetime.fromtimestamp(ms / 1000, timezone.utc).strftime('%Y-%m-%d %H:%M') if ms else '—'


async def probe_pages(f, ex_id, sym, now_ms, max_pages=60, limit=1000):
    """Воспроизводит запросы загрузчика (1h, проход назад от настоящего). → dict(rows {ts: строка}, pages_ok, end_fail — endTime неудавшегося запроса (None — первая страница), exc, complete)."""
    key = 'until' if ex_id == 'bybit' else 'endTime'
    rows, end, pages = {}, None, 0
    for _ in range(max_pages):
        params = {} if end is None else {key: end}
        try:
            got = await f.call('fetch_ohlcv', sym, '1h', None, limit, params)
        except Exception as e:
            return {'rows': rows, 'pages_ok': pages, 'end_fail': end, 'exc': e, 'complete': False}
        pages += 1
        if not got:
            return {'rows': rows, 'pages_ok': pages, 'end_fail': None, 'exc': None, 'complete': True}
        new, mn = 0, None
        for r in got:
            t = int(r[0])
            mn = t if mn is None else min(mn, t)
            if t not in rows:
                rows[t] = r
                new += 1
        if new == 0 or mn <= X.SINCE_MS:
            return {'rows': rows, 'pages_ok': pages, 'end_fail': None, 'exc': None, 'complete': True}
        end = mn - 1
    return {'rows': rows, 'pages_ok': pages, 'end_fail': end, 'exc': None, 'complete': False}


async def probe_7d(f, sym, now_ms):
    """Пробный запрос 1h за последние 7 суток → (число свечей | None, исключение | None)."""
    try:
        got = await f.call('fetch_ohlcv', sym, '1h', now_ms - 7 * DAY, 200, {})
        return len(got or []), None
    except Exception as e:
        return None, e


def classify(p, p7, market, daily, candidates):
    """Правило §2.57 (порядок: а, б, в, г). p — результат probe_pages; p7 = (n | None, exc | None); market — 'активен' | 'неактивен' | 'нет'; daily = (first_ts, last_ts) дневной истории;
    candidates — активные рынки с той же базой и другим именем [(символ, created | None)]. → (класс, причина)."""
    exc = p['exc']
    n7, e7 = p7
    code, text = code_text(exc) if exc is not None else ('', '')
    if exc is None:
        return 'в', 'ошибка не воспроизвелась при повторном проходе (временная)'
    transient = X.is_transient(exc) or bool(TRANSIENT_RE.search(text))
    nodata_text = bool(NODATA_RE.search(text))
    # а) недоступны: интервал неудавшегося запроса попадает в диапазон дневных свечей
    in_daily = daily is not None and (p['end_fail'] is None or daily[0] <= p['end_fail'] <= daily[1] + DAY)
    if not transient and in_daily:
        if n7 and p['pages_ok'] >= 1:
            return 'а1', f'1h за 7 суток есть ({n7}), ошибка на странице {p["pages_ok"] + 1} (endTime {iso(p["end_fail"])}) — ограничение глубины истории'
        if market != 'активен' and not candidates and not n7 and (nodata_text or e7 is None):
            return 'а2', f'символ {market}, 1h за 7 суток нет, ответ биржи — «нет данных/символа»'
    # б) переименован
    if market != 'активен' and candidates and not transient:
        if len(candidates) == 1 or any(c is None or daily is None or c >= daily[1] - 30 * DAY for _s, c in candidates):
            return 'б', f'символа {market}; кандидат по базе: {candidates[0][0]}'
    # в) временная
    if transient:
        return 'в', 'сетевая/временная ошибка (класс или текст ответа)'
    return 'г', 'не подошло ни одно правило (структурные признаки и текст ответа не согласуются либо ответ нестандартный)'


def candidates_for(norm, sym, mk):
    return [(s_, v.get('created')) for s_, v in mk.items() if v.get('norm') == norm and s_ != sym and v.get('active') and not v.get('excl')]


async def run_h1_errors(params, progress=None, exchanges=None, now_ms=None, sleep=asyncio.sleep):
    """`/xfund_1h_errors [apply]` → текст отчёта. Без apply данные (hbars/neg) не меняются; пишутся только записи классификации `h1class`."""
    d = params.get('xdir') or X.xdata_dir()
    apply_ = bool(params.get('apply'))
    now_ms = int(now_ms or time.time() * 1000)
    own = exchanges is None
    if own:
        import ccxt.async_support as ccxt
        exchanges = {'bingx': ccxt.bingx({'options': {'defaultType': 'swap'}, 'enableRateLimit': True})}
    idx = X.compact_index(d)
    errs = sorted((k.split('|')[0], k.split('|')[1]) for k, r in idx['neg'].items() if k.endswith('|h1') and r.get('why') not in ('empty', 'unavailable'))
    errs = [(ex, s_) for ex, s_ in errs if ex == 'bingx']
    if not errs:
        return '✅ /xfund_1h_errors: записей-ошибок загрузки 1h BingX нет'
    L = [f'🔎 <b>/xfund_1h_errors{" apply" if apply_ else ""}</b> — ошибок загрузки 1h BingX: {len(errs)} (BOT_SPEC §2.57; запросы только публичного чтения)']
    try:
        fx = X.Fetcher(exchanges['bingx'], sleep=sleep, delays=())                       # диагностика: БЕЗ повторов — видна исходная ошибка
        try:
            live = X.usdt_swaps(await fx.call('load_markets'))
            L.append(f'рынки BingX (живая загрузка): {len(live)}')
        except Exception as e:
            live = {s_: dict(v) for s_, v in idx['markets'].get('bingx', {}).items()}
            L.append(f'⚠ load_markets не удался ({type(e).__name__}); статус символов — по индексу прошлой загрузки')
        results = {}
        for n, (ex, sym) in enumerate(errs, 1):
            p = await probe_pages(fx, ex, sym, now_ms)
            p7 = await probe_7d(fx, sym, now_ms)
            norm = (live.get(sym) or idx['markets'].get('bingx', {}).get(sym) or {}).get('norm') or X.norm_base(sym.split('/')[0])
            mrow = live.get(sym)
            market = 'нет' if mrow is None else ('активен' if mrow.get('active') else 'неактивен')
            br = idx['bars'].get(ex, {}).get(sym) or {}
            daily = (br['first'], br['last']) if br.get('first') else None
            created, src = (mrow or {}).get('created'), 'markets.created'
            if not created:
                created, src = (daily[0] if daily else None), 'первый дневной бар'
            cands = candidates_for(norm, sym, live)
            cls, why = classify(p, p7, market, daily, cands)
            code, text = code_text(p['exc']) if p['exc'] is not None else ('', '')
            results[sym] = {'cls': cls, 'why': why, 'p': p, 'p7': p7, 'market': market, 'created': created, 'src': src, 'cands': cands, 'code': code, 'text': text, 'daily': daily}
            X.put(d, idx, 'h1class', ex, sym, {'cls': cls, 'code': code, 'text': text, 'cc': type(p['exc']).__name__ if p['exc'] is not None else '', 'end': p['end_fail'], 'pages': p['pages_ok'],
                                                'rows': len(p['rows']), 'n7': p7[0], 'e7': (type(p7[1]).__name__ if p7[1] is not None else ''), 'market': market, 'created': created,
                                                'new': cands[0][0] if cands else '', 't': now_ms})
            if progress and n % 8 == 0:
                await progress(f'диагностика 1h: {n}/{len(errs)}')
        # отчёт: по символам
        L.append('')
        L.append('<b>По символам</b> (запрос: проход назад от настоящего, 1h, limit 1000; startTime не задаётся):')
        for sym, r in results.items():
            p = r['p']
            ival = f'startTime=—, endTime={iso(p["end_fail"]) if p["end_fail"] else "не задан (первая страница)"}'
            L.append(f'• {sym}: {ival}; ccxt: {type(p["exc"]).__name__ if p["exc"] is not None else "ошибка не воспроизвелась"}; код {r["code"] or "—"}; «{r["text"] or "—"}»; '
                     f'листинг {iso(r["created"])} ({r["src"]}); markets: {r["market"]}; 7 суток: ' + (f'{r["p7"][0]} свечей' if r['p7'][0] is not None else f'ошибка {type(r["p7"][1]).__name__}')
                     + f'; страниц до ошибки {p["pages_ok"]}, строк {len(p["rows"])}; <b>класс {r["cls"]}</b> ({r["why"]})')
        # сводка по коду
        by = {}
        for r in results.values():
            by.setdefault((r['code'] or '—', r['text'][:70]), []).append(r['cls'])
        L.append('')
        L.append('<b>Сводка по коду ошибки</b>')
        for (code, text), cl in sorted(by.items(), key=lambda kv: -len(kv[1])):
            L.append(f'  код {code} «{text}» ×{len(cl)} (классы: ' + ', '.join(f'{k}×{cl.count(k)}' for k in sorted(set(cl))) + ')')
        cnt = {}
        for r in results.values():
            cnt[r['cls']] = cnt.get(r['cls'], 0) + 1
        L.append('  итого по классам: ' + ', '.join(f'{k}: {v}' for k, v in sorted(cnt.items())))
        if not apply_:
            L.append('')
            L.append('Данные не менялись. Применить исходы по правилу §2.57: /xfund_1h_errors apply; затем /xfund_test load и /xfund_test.')
        else:
            L += await apply_outcomes(d, idx, exchanges['bingx'], results, now_ms, sleep)
    finally:
        if own:
            import access_probe
            for c in exchanges.values():
                await access_probe.close_ccxt(c)
    return '\n'.join(L)


def _save_rows(d, idx, ex, sym, rows, now_ms, complete, limit=None):
    ts = sorted(rows)
    arr = np.array([rows[t][1:6] for t in ts], float)
    X.save_hbars(d, ex, sym, np.array(ts, np.int64), arr[:, 1], arr[:, 2], arr[:, 3])
    rec = {'first': int(ts[0]), 'last': int(ts[-1]), 'n': len(ts), 't': now_ms, 'complete': bool(complete)}
    if limit:
        rec['limit'] = limit
    X.put(d, idx, 'hbars', ex, sym, rec)


async def apply_outcomes(d, idx, bingx, results, now_ms, sleep):
    """Исходы по классам §2.57: а1 — загрузить доступную часть; а2 — «подтверждённо недоступны»; б — загрузка под новым именем, ключ — старый символ, запись alias; в — повторы 3/8/20/45 с;
    г — без изменений (остаётся ошибкой в гейте). → строки отчёта."""
    ex = 'bingx'
    done = {'а1': 0, 'а2': 0, 'б': [], 'в_ok': 0, 'в_fail': 0, 'г': 0}
    fx_retry = X.Fetcher(bingx, sleep=sleep, delays=RETRY_V, retry_all=True)               # класс в: повтор любой ошибки с паузами 3/8/20/45 с
    fx_plain = X.Fetcher(bingx, sleep=sleep, delays=())                                    # класс б: без повторов (правило их не задаёт)
    for sym, r in results.items():
        cls, key = r['cls'], f'{sym}|h1'
        if cls == 'а1' and r['p']['rows']:
            _save_rows(d, idx, ex, sym, r['p']['rows'], now_ms, False, 'depth')
            X.put(d, idx, 'negdel', ex, key, None)
            done['а1'] += 1
        elif cls == 'а2':
            X.put(d, idx, 'neg', ex, key, {'t': now_ms, 'why': 'unavailable', 'msg': f'{r["code"]} {r["text"]}'.strip()[:200], 'pages': r['p']['pages_ok'], 'rows': 0})
            done['а2'] += 1
        elif cls == 'б':
            new = r['cands'][0][0]
            try:
                out = await X.fetch_bars_back(fx_plain, ex, new, X.SINCE_MS, now_ms, tf='1h')
                ts = out[0]
                if len(ts):
                    rows = {int(t): [int(t), 0.0, float(h), float(l_), float(c), 0.0] for t, h, l_, c in zip(out[0], out[2], out[3], out[4])}
                    _save_rows(d, idx, ex, sym, rows, now_ms, out[6])
                    X.put(d, idx, 'alias', ex, sym, {'new': new, 't': now_ms})
                    X.put(d, idx, 'negdel', ex, key, None)
                    done['б'].append((sym, new))
                else:
                    done['г'] += 1
            except Exception:
                done['г'] += 1
        elif cls == 'в':
            try:
                out = await X.fetch_bars_back(fx_retry, ex, sym, X.SINCE_MS, now_ms, tf='1h')
                if len(out[0]):
                    rows = {int(t): [int(t), 0.0, float(h), float(l_), float(c), 0.0] for t, h, l_, c in zip(out[0], out[2], out[3], out[4])}
                    _save_rows(d, idx, ex, sym, rows, now_ms, out[6])
                    X.put(d, idx, 'negdel', ex, key, None)
                    done['в_ok'] += 1
                else:
                    X.put(d, idx, 'neg', ex, key, {'t': now_ms, 'why': 'empty'})                  # после повторов биржа отвечает пустым списком — подтверждённо пусто
                    done['в_ok'] += 1
            except Exception as e:
                X.put(d, idx, 'neg', ex, key, X.neg_rec(now_ms, e))                       # после повторов — снова в гейт (как г)
                done['в_fail'] += 1
        else:
            done['г'] += 1
    X.save_index(d, idx)
    L = ['', '<b>Применено (§2.57)</b>',
         f'  а1 (ограничение глубины): загружена доступная часть 1h у {done["а1"]} символов, остальной период — дневной запасной базис (счётчик сделок по дневному базису — в отчёте /xfund_test)',
         f'  а2 (подтверждённо недоступны): {done["а2"]} — расчёт не блокируют, дневной запасной базис',
         f'  б (переименованы): {len(done["б"])}' + (' — соответствие: ' + ', '.join(f'{a} → {b}' for a, b in done['б']) if done['б'] else ''),
         f'  в (временные): загружено после повторов 3/8/20/45 с — {done["в_ok"]}, снова ошибка (в гейт) — {done["в_fail"]}',
         f'  г (прочее): {done["г"]} — остаются ошибкой загрузки 1h в гейте §2.53.2',
         'Дальше: /xfund_test load (отчёт покрытия), затем /xfund_test.']
    return L
