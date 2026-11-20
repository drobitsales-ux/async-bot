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
TRANSIENT_RE = re.compile(r'rate.?limit|too many|timeout|timed out|busy|temporar|try again|unavailable|internal (server )?error|5\d\d|请稍后重试|稍后', re.I)       # v127 (§2.58): + китайское «повторите позже»
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
    """Воспроизводит запросы загрузчика (1h, проход назад от настоящего). → dict(ts, h, l, c — КОМПАКТНЫЕ массивы (int64/float32, ≈ 20 байт на бар), n, pages_ok, end_fail — endTime
    неудавшегося запроса (None — первая страница), exc, complete). v126: ряды хранятся массивами, а не списками Python (v125 держал по ~250 байт на бар для всех символов — MemoryError на 8-м)."""
    key = 'until' if ex_id == 'bybit' else 'endTime'
    parts, end, pages, prev_mn = [], None, 0, None

    def out(exc=None, complete=False, end_fail=None):
        if parts:
            ts = np.concatenate([q[0] for q in parts]); h = np.concatenate([q[1] for q in parts]); l_ = np.concatenate([q[2] for q in parts]); c = np.concatenate([q[3] for q in parts])
            o = np.argsort(ts, kind='stable')
            ts, h, l_, c = ts[o], h[o], l_[o], c[o]
        else:
            ts, h, l_, c = np.zeros(0, np.int64), np.zeros(0, np.float32), np.zeros(0, np.float32), np.zeros(0, np.float32)
        return {'ts': ts, 'h': h, 'l': l_, 'c': c, 'n': int(len(ts)), 'pages_ok': pages, 'end_fail': end_fail, 'exc': exc, 'complete': complete}
    for _ in range(max_pages):
        params = {} if end is None else {key: end}
        try:
            got = await f.call('fetch_ohlcv', sym, '1h', None, limit, params)
        except Exception as e:
            return out(e, False, end)
        pages += 1
        if not got:
            return out(None, True)
        ts = np.array([int(r[0]) for r in got], np.int64)
        mn = int(ts.min())
        keep = ts < prev_mn if prev_mn is not None else np.ones(len(ts), bool)           # окна идут назад: новыми считаются бары строго старше предыдущей страницы
        if not keep.any():
            return out(None, True)
        parts.append((ts[keep], np.array([float(r[2]) for r in got], np.float32)[keep], np.array([float(r[3]) for r in got], np.float32)[keep], np.array([float(r[4]) for r in got], np.float32)[keep]))
        del got
        if mn <= X.SINCE_MS:
            return out(None, True)
        prev_mn, end = mn, mn - 1
    return out(None, False, end)


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
    transient = X.is_transient(exc) or code in X.TRANSIENT_CODES or bool(TRANSIENT_RE.search(text))
    nodata_text = bool(NODATA_RE.search(text))
    if transient:                                                  # [v127, §2.58 п. 2] временная ошибка проверяется ПЕРЕД а1/а2: лимит запросов никогда не становится «недоступно»/«глубина»
        return 'в', f'временная ошибка (лимит запросов/сеть): код {code or "—"}'
    # а) недоступны: интервал неудавшегося запроса попадает в диапазон дневных свечей
    in_daily = daily is not None and (p['end_fail'] is None or daily[0] <= p['end_fail'] <= daily[1] + DAY)
    if not transient and in_daily:
        if n7 and p['pages_ok'] >= 1 and nodata_text:                # [v127, §2.58 п. 6] а1 — только при явном ответе биржи «нет данных / вне диапазона», не по ошибке без такого текста
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
    """`/xfund_1h_errors [apply]` → текст отчёта. Без apply данные (hbars/neg) не меняются; пишутся только записи классификации `h1class`.
    v126: символы обрабатываются ПО ОДНОМУ — ряды пробного прохода (компактные массивы) освобождаются сразу после классификации и применения исхода (в v125 ряды всех символов держались в памяти: MemoryError)."""
    d = params.get('xdir') or X.xdata_dir()
    apply_ = bool(params.get('apply'))
    now_ms = int(now_ms or time.time() * 1000)
    own = exchanges is None
    if own:
        import ccxt.async_support as ccxt
        exchanges = {'bingx': ccxt.bingx({'options': {'defaultType': 'swap'}, 'enableRateLimit': True})}
    idx = X.compact_index(d)
    X.migrate_v127(d, idx)                                        # [v127, §2.58 п. 8] разовый сброс записей-ошибок и классификации в «не обработано»
    errs = sorted((k.split('|')[0], k.split('|')[1]) for k, r in idx['neg'].items() if k.endswith('|h1') and r.get('why') not in ('empty', 'unavailable'))
    errs = [(ex, s_) for ex, s_ in errs if ex == 'bingx']
    if not errs:
        return '✅ /xfund_1h_errors: записей-ошибок загрузки 1h BingX нет'
    L = [f'🔎 <b>/xfund_1h_errors{" apply" if apply_ else ""}</b> — ошибок загрузки 1h BingX: {len(errs)} (BOT_SPEC §2.57; запросы только публичного чтения)']
    done = {'а1': 0, 'а2': 0, 'б': [], 'в_ok': 0, 'в_fail': 0, 'г': 0}
    try:
        fx = X.Fetcher(exchanges['bingx'], sleep=sleep, delays=())                       # диагностика: БЕЗ повторов — видна исходная ошибка
        fx_retry = X.Fetcher(exchanges['bingx'], sleep=sleep, delays=RETRY_V, retry_all=True)       # класс в: повтор любой ошибки с паузами 3/8/20/45 с
        fx_plain = X.Fetcher(exchanges['bingx'], sleep=sleep, delays=())                 # класс б: без повторов (правило их не задаёт)
        try:
            live = X.usdt_swaps(await fx.call('load_markets'))
            L.append(f'рынки BingX (живая загрузка): {len(live)}')
        except Exception as e:
            live = {s_: dict(v) for s_, v in idx['markets'].get('bingx', {}).items()}
            L.append(f'⚠ load_markets не удался ({type(e).__name__}); статус символов — по индексу прошлой загрузки')
        info = {}
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
            info[sym] = {'cls': cls, 'why': why, 'market': market, 'created': created, 'src': src, 'code': code, 'text': text, 'end_fail': p['end_fail'], 'pages_ok': p['pages_ok'], 'n': p['n'],
                         'cc': type(p['exc']).__name__ if p['exc'] is not None else '', 'n7': p7[0], 'e7': type(p7[1]).__name__ if p7[1] is not None else ''}
            X.put(d, idx, 'h1class', ex, sym, {'cls': cls, 'code': code, 'text': text, 'cc': info[sym]['cc'], 'end': p['end_fail'], 'pages': p['pages_ok'], 'rows': p['n'], 'n7': p7[0],
                                                'e7': info[sym]['e7'], 'market': market, 'created': created, 'new': cands[0][0] if cands else '', 't': now_ms})
            if apply_:
                await apply_one(d, idx, ex, sym, cls, p, code, text, cands, now_ms, fx_retry, fx_plain, done)
            del p, p7                                                                    # ряды символа освобождаются до следующего
            if progress and n % 8 == 0:
                await progress(f'диагностика 1h: {n}/{len(errs)}')
        # отчёт: по символам
        L.append('')
        L.append('<b>По символам</b> (запрос: проход назад от настоящего, 1h, limit 1000; startTime не задаётся):')
        for sym, r in info.items():
            ival = f'startTime=—, endTime={iso(r["end_fail"]) if r["end_fail"] else "не задан (первая страница)"}'
            L.append(f'• {sym}: {ival}; ccxt: {r["cc"] or "ошибка не воспроизвелась"}; код {r["code"] or "—"}; «{r["text"] or "—"}»; '
                     f'листинг {iso(r["created"])} ({r["src"]}); markets: {r["market"]}; 7 суток: ' + (f'{r["n7"]} свечей' if r['n7'] is not None else f'ошибка {r["e7"]}')
                     + f'; страниц до ошибки {r["pages_ok"]}, строк {r["n"]}; <b>класс {r["cls"]}</b> ({r["why"]})')
        # сводка по коду
        by = {}
        for r in info.values():
            by.setdefault((r['code'] or '—', r['text'][:70]), []).append(r['cls'])
        L.append('')
        L.append('<b>Сводка по коду ошибки</b>')
        for (code, text), cl in sorted(by.items(), key=lambda kv: -len(kv[1])):
            L.append(f'  код {code} «{text}» ×{len(cl)} (классы: ' + ', '.join(f'{k}×{cl.count(k)}' for k in sorted(set(cl))) + ')')
        cnt = {}
        for r in info.values():
            cnt[r['cls']] = cnt.get(r['cls'], 0) + 1
        L.append('  итого по классам: ' + ', '.join(f'{k}: {v}' for k, v in sorted(cnt.items())))
        if not apply_:
            L.append('')
            L.append('Данные не менялись. Применить исходы по правилу §2.57: /xfund_1h_errors apply; затем /xfund_test load и /xfund_test.')
        else:
            X.save_index(d, idx)
            L += summary_lines(done)
    finally:
        if own:
            import access_probe
            for c in exchanges.values():
                await access_probe.close_ccxt(c)
    return '\n'.join(L)


def _save_arrays(d, idx, ex, sym, ts, h, l_, c, now_ms, complete, limit=None):
    """Сохраняет 1h-ряд символа (ts int64; high/low/close → float32 как у загрузчика) и запись индекса."""
    X.save_hbars(d, ex, sym, np.asarray(ts, np.int64), h, l_, c)
    rec = {'first': int(ts[0]), 'last': int(ts[-1]), 'n': int(len(ts)), 't': now_ms, 'complete': bool(complete)}
    if limit:
        rec['limit'] = limit
    X.put(d, idx, 'hbars', ex, sym, rec)


async def apply_one(d, idx, ex, sym, cls, p, code, text, cands, now_ms, fx_retry, fx_plain, done):
    """Исход одного символа по классу §2.57: а1 — загрузить доступную часть (успешные страницы пробы); а2 — «подтверждённо недоступны»; б — загрузка под новым именем (ключ — старый символ, запись alias);
    в — повторы 3/8/20/45 с с повтором любой ошибки (после — снова гейт); г — без изменений. Счётчики — в done."""
    key = f'{sym}|h1'
    if cls == 'а1' and p['n']:
        _save_arrays(d, idx, ex, sym, p['ts'], p['h'], p['l'], p['c'], now_ms, False, 'depth')
        X.put(d, idx, 'negdel', ex, key, None)
        done['а1'] += 1
    elif cls == 'а2':
        X.put(d, idx, 'neg', ex, key, {'t': now_ms, 'why': 'unavailable', 'msg': f'{code} {text}'.strip()[:200], 'pages': p['pages_ok'], 'rows': 0})
        done['а2'] += 1
    elif cls == 'б':
        new = cands[0][0]
        try:
            out = await X.fetch_bars_back(fx_plain, ex, new, X.SINCE_MS, now_ms, tf='1h')
            if len(out[0]):
                _save_arrays(d, idx, ex, sym, out[0], out[2], out[3], out[4], now_ms, out[6])
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
                _save_arrays(d, idx, ex, sym, out[0], out[2], out[3], out[4], now_ms, out[6])
                X.put(d, idx, 'negdel', ex, key, None)
            else:
                X.put(d, idx, 'neg', ex, key, {'t': now_ms, 'why': 'empty'})                  # после повторов биржа отвечает пустым списком — подтверждённо пусто
            done['в_ok'] += 1
        except Exception as e:
            X.put(d, idx, 'neg', ex, key, X.neg_rec(now_ms, e))                               # после повторов — снова в гейт (как г)
            done['в_fail'] += 1
    else:
        done['г'] += 1


def summary_lines(done):
    return ['', '<b>Применено (§2.57)</b>',
            f'  а1 (ограничение глубины): загружена доступная часть 1h у {done["а1"]} символов, остальной период — дневной запасной базис (счётчик сделок по дневному базису — в отчёте /xfund_test)',
            f'  а2 (подтверждённо недоступны): {done["а2"]} — расчёт не блокируют, дневной запасной базис',
            f'  б (переименованы): {len(done["б"])}' + (' — соответствие: ' + ', '.join(f'{a} → {b}' for a, b in done['б']) if done['б'] else ''),
            f'  в (временные): загружено после повторов 3/8/20/45 с — {done["в_ok"]}, снова ошибка (в гейт) — {done["в_fail"]}',
            f'  г (прочее): {done["г"]} — остаются ошибкой загрузки 1h в гейте §2.53.2',
            'Дальше: /xfund_test load (отчёт покрытия), затем /xfund_test.']
