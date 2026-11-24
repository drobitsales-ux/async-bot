"""disk_usage.py — `/disk_usage` и `/disk_cleanup` (BOT_SPEC §2.59.11, v130). Ничего не удаляет само: удаление — только `/disk_cleanup <имя> confirm`, только для пометки
«архив закрытого исследования», результаты которого записаны в BOT_SPEC; без confirm команда показывает, что будет удалено. Сетевых вызовов нет (подсчёт контрактов — отдельная функция в задаче)."""
import os
import shutil

KB = 1024.0
MB = 1e6
PER_SYM_KB_LOW, PER_SYM_KB_HIGH = 30.0, 140.0         # §2.59.11 п. 1: оценка снизу/сверху на символ (свечи + фандинг, сжатые npz)
INDEX_KB_PER_MARKET = 0.3


def du(path):
    """Размер файла или каталога (байты); не бросает исключений."""
    try:
        if os.path.isfile(path) or os.path.islink(path):
            return os.path.getsize(path)
        tot = 0
        for r, _ds, fs in os.walk(path):
            for f in fs:
                try:
                    tot += os.path.getsize(os.path.join(r, f))
                except OSError:
                    pass
        return tot
    except OSError:
        return 0


def _belongs(name, base):
    return name == base or name.startswith(base + '-') or name.startswith(base + '.')


def layout(root, paths=None):
    """Элементы `root` с классификацией. paths — {ключ: фактический путь} для каталогов, настраиваемых ENV (vision, factory, edge, battery, xdata, leadlag). →
    список dict(key, name, path, size, cls, note, cleanable). cls: 'активно' | 'архив закрытого исследования' | 'не классифицировано'."""
    paths = dict(paths or {})
    rp = lambda p: os.path.realpath(p)
    known = {
        'vision': (paths.get('vision') or os.path.join(root, 'vision'), 'архив закрытого исследования',
                   'кэш 15m/1h Binance: H-XS §2.32, R0/R1 §2.37.9, фабрика §2.41, финал 4h §2.42, батарея §2.43.10 (результаты записаны)', True),
        'edge': (paths.get('edge') or os.path.join(root, 'edge.db'), 'архив закрытого исследования', 'сделки R0/R1 `/edge_audit`: итог §2.37.9 (результаты записаны)', True),
        'battery': (paths.get('battery') or os.path.join(root, 'battery'), 'активно', 'D1+фандинг Binance: список баз архива и BTC для H2EXT/H2F (§2.47, §2.59); не удалять', False),
        'xdata': (paths.get('xdata') or os.path.join(root, 'xdata'), 'активно', 'кэш BingX/OKX/новых бирж для H2EXT/H2F (§2.59); внутри `*.h1.npz` — 1h-свечи X1 (§2.48.8, закрыто)', False),
        'leadlag': (paths.get('leadlag') or os.path.join(root, 'leadlag'), 'активно', 'замер расхождений цен OKX↔BingX (§2.60)', False),
        'logs': (os.path.join(root, 'logs'), 'активно', 'дайджесты и отчёты (§2.25, §11)', False),
        'research_jobs': (paths.get('jobs') or os.path.join(root, 'research_jobs'), 'активно', 'рабочая папка заданий исследований (§2.39); не удалять при выполняющейся задаче', False),
        'trades.db': (os.path.join(root, 'trades.db'), 'активно', 'сделки, shadow, состояние breaker-ов (§2.36)', False),
        'bot.db': (os.path.join(root, 'bot.db'), 'активно', 'позиции бота', False),
        'exec_check.db': (os.path.join(root, 'exec_check.db'), 'активно', 'замеры /exec_check (§2.47.4), инструмент', False),
        'audit.db': (os.path.join(root, 'audit.db'), 'активно', 'аудит канала: неизменяемые сигналы и запечатанный HOLDOUT (§2.44, §2.45.9); не удалять', False),
        'tg': (os.path.join(root, 'tg'), 'активно', 'сессия Telethon (служебное; содержимое не показывается)', False),
    }
    fac = paths.get('factory') or os.path.join(root, 'factory')
    items, used = [], set()
    for key, (p, cls, note, clean) in known.items():
        if os.path.exists(p) or any(_belongs(n, os.path.basename(p)) for n in _ls(os.path.dirname(p))):
            sz = du(p) + sum(du(os.path.join(os.path.dirname(p), n)) for n in _ls(os.path.dirname(p)) if n != os.path.basename(p) and _belongs(n, os.path.basename(p)))
            items.append({'key': key, 'name': os.path.basename(p), 'path': p, 'size': sz, 'cls': cls, 'note': note, 'cleanable': clean})
            used.add(rp(p))
            for n in _ls(os.path.dirname(p)):
                if _belongs(n, os.path.basename(p)):
                    used.add(rp(os.path.join(os.path.dirname(p), n)))
    if os.path.isdir(fac):                                           # factory/: factory.db (флаги финалов, результаты) — активно; остальное — данные закрытой «фабрики»
        db = sum(du(os.path.join(fac, n)) for n in _ls(fac) if n.startswith('factory.db'))
        rest = du(fac) - db
        items.append({'key': 'factory.db', 'name': os.path.join(os.path.basename(fac), 'factory.db*'), 'path': os.path.join(fac, 'factory.db'), 'size': db, 'cls': 'активно',
                      'note': 'флаги финалов LOCKBOX и результаты (§2.42, §2.43.17, §2.47–§2.59); не удалять', 'cleanable': False})
        items.append({'key': 'factory_data', 'name': os.path.basename(fac) + ' (данные «фабрики»)', 'path': fac, 'size': rest, 'cls': 'архив закрытого исследования',
                      'note': 'Dukascopy и прочие данные «фабрики»: §2.41, §2.42 (результаты записаны); `factory.db` сохраняется', 'cleanable': True})
        used.add(rp(fac))
    for n in sorted(_ls(root)):
        p = os.path.join(root, n)
        if rp(p) in used:
            continue
        items.append({'key': n, 'name': n, 'path': p, 'size': du(p), 'cls': 'не классифицировано', 'note': 'нет в списке известных (BOT_SPEC §2.59.11); не удаляется командой', 'cleanable': False})
    return items


def _ls(d):
    try:
        return os.listdir(d)
    except OSError:
        return []


def h1_bytes(xdata):
    """Размер 1h-свечей X1 внутри xdata (`*.h1.npz`) — справочно (архив закрытого X1, §2.48.8); отдельной командой не удаляется."""
    tot = 0
    for r, _ds, fs in os.walk(xdata):
        for f in fs:
            if f.endswith('.h1.npz'):
                try:
                    tot += os.path.getsize(os.path.join(r, f))
                except OSError:
                    pass
    return tot


def need_h2ext_mb(n_symbols, n_markets):
    """(низ, верх) МБ для загрузки H2EXT: символы × 30–140 КБ + индекс 0.3 КБ на контракт (§2.59.11 п. 1)."""
    ix = n_markets * INDEX_KB_PER_MARKET * KB / MB
    return n_symbols * PER_SYM_KB_LOW * KB / MB + ix, n_symbols * PER_SYM_KB_HIGH * KB / MB + ix


def leadlag_remaining_mb(st, now):
    """Сколько ещё займёт замер до конца срока: оставшиеся тики × (12·S + 16) байт (текущий шаг). → МБ (0, если замер не идёт)."""
    if not st or not st.get('running'):
        return 0.0
    left = max(0.0, float(st.get('until', 0)) - now)
    step = float(st.get('step') or 2.0)
    s = len(st.get('symbols') or [])
    return (left / step) * (12 * s + 16) / MB


def space_check(free_mb, n_to_load, st, now, min_free, margin=50.0, per_sym_kb=PER_SYM_KB_HIGH):
    """§2.59.11 п. 3: пока идёт замер, загрузка H2EXT отказывает, если после неё останется меньше min_free + margin МБ.
    Расчёт: свободно − символы × per_sym_kb − рост замера до конца срока. → (ok, dict(load, growth, after, need_after, short))."""
    load = n_to_load * per_sym_kb * KB / MB
    growth = leadlag_remaining_mb(st, now)
    after = free_mb - load - growth
    need = min_free + margin
    return (after >= need), {'load': load, 'growth': growth, 'after': after, 'need_after': need, 'short': max(0.0, need - after)}


def format_report(root, items, free_mb, total_mb, ll_state, now, min_free, contracts=None, venues=()):
    L = [f'💽 <b>/disk_usage</b> — {root}: всего {total_mb:.0f} МБ, свободно {free_mb:.0f} МБ']
    for x in sorted(items, key=lambda z: -z['size']):
        L.append(f'  {x["size"] / MB:8.1f} МБ  {x["name"]} — <b>{x["cls"]}</b>' + (f' (можно: /disk_cleanup {x["key"]} confirm)' if x['cleanable'] else '') + f'\n      {x["note"]}')
    h1 = [x for x in items if x['key'] == 'xdata']
    if h1:
        L.append(f'  справочно: 1h-свечи X1 внутри xdata (`*.h1.npz`) — {h1_bytes(h1[0]["path"]) / MB:.1f} МБ (архив закрытого X1, §2.48.8; отдельной командой не удаляются)')
    growth = leadlag_remaining_mb(ll_state, now)
    L.append('')
    if contracts:
        tot_s = sum(c['want'] for c in contracts.values() if c.get('want') is not None)
        tot_m = sum(c['markets'] for c in contracts.values() if c.get('markets') is not None)
        lo, hi = need_h2ext_mb(tot_s, tot_m)
        L.append('Контракты новых бирж (по спискам, публичные запросы): ' + '; '.join(
            f'{ex}: всего {c["markets"]}, к загрузке {c["want"]}' if c.get('markets') is not None else f'{ex}: нет данных ({c.get("err", "?")})' for ex, c in contracts.items()))
        need_txt = f'{lo:.0f}–{hi:.0f} МБ ({tot_s} символов × 30–140 КБ; оценка Claude §2.59.11)'
        hi_v = hi
    else:
        need_txt, hi_v = 'нет данных (число контрактов не получено)', None
    L.append(f'<b>Итог:</b> свободно {free_mb:.0f} МБ | нужно для /h2_ext load: {need_txt} | нужно до конца lead-lag: {growth:.0f} МБ')
    if hi_v is not None:
        after = free_mb - hi_v - growth
        L.append(f'После загрузки (по верхней оценке) и до конца замера останется ≈ {after:.0f} МБ при минимуме {min_free:.0f} + 50 = {min_free + 50:.0f} МБ: '
                 + ('хватает' if after >= min_free + 50 else f'<b>не хватает {min_free + 50 - after:.0f} МБ</b> — освободите архивы (пометка «архив закрытого исследования») командой /disk_cleanup'))
    return '\n'.join(L)


# ── очистка ───────────────────────────────────────────────────────────────────────────────────────
def cleanup_plan(root, items, key):
    """→ (ok, текст, список путей для удаления, размер байт). Допустимы только элементы с пометкой «архив закрытого исследования» и cleanable."""
    it = next((x for x in items if x['key'] == key), None)
    if it is None:
        return False, f'⛔ /disk_cleanup: нет элемента «{key}» (список: /disk_usage)', [], 0
    if it['cls'] != 'архив закрытого исследования' or not it['cleanable']:
        return False, f'⛔ /disk_cleanup {key}: элемент «{it["cls"]}» — удаление запрещено (только «архив закрытого исследования», §2.59.11 п. 4)', [], 0
    p = it['path']
    real, rroot = os.path.realpath(p), os.path.realpath(root)
    if not (real == rroot or real.startswith(rroot + os.sep)) or real == rroot:
        return False, f'⛔ /disk_cleanup {key}: путь вне каталога данных', [], 0
    if key == 'factory_data':
        paths = [os.path.join(p, n) for n in _ls(p) if not n.startswith('factory.db')]
    elif os.path.isdir(p):
        paths = [p]
    else:
        paths = [os.path.join(os.path.dirname(p), n) for n in _ls(os.path.dirname(p)) if _belongs(n, os.path.basename(p))]
    return True, '', paths, it['size']


def cleanup_run(paths):
    """Удаляет перечисленные пути (файлы и каталоги). → число удалённых байт (по размеру до удаления)."""
    freed = 0
    for p in paths:
        freed += du(p)
        try:
            if os.path.isdir(p) and not os.path.islink(p):
                shutil.rmtree(p)
            elif os.path.exists(p):
                os.remove(p)
        except OSError:
            pass
    return freed
