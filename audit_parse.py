"""audit_parse.py — парсер текстовых сигналов внешнего канала (BOT_SPEC §2.44.1): украинский/русский/английский, эмодзи.
Результат: dict(kind='signal'|'report'|'unparsed'|'other', ...). Ничего не «додумывается»: нет обязательного поля или геометрия неправдоподобна → 'unparsed' с причиной.
Формат реальных сообщений на момент написания НЕ известен (скриншоты не переданы) — парсер гибкий по меткам; уточняется по реальным текстам до набора форварда."""
import re

NUM = r'\d{1,3}(?: \d{3})+(?:[.,]\d+)?(?!\d)|\d+(?:[.,]\d+)?'
NUM_RE = re.compile('(?:' + NUM + ')')
NUMG = '(?:' + NUM + ')'
RANGE_SEP = r'\s*(?:[-–—~]|\bдо\b|\bto\b|…|\.{2,3})\s*'
LABEL_ENTRY = r'(?:зон[аыуе]\s+вход[а-я]*|точк[аиуе]\s+вход[а-я]*|вход[а-я]*|вх[іи]д[а-я]*|entry\s*zone|buy\s*zone|entry|enter|вход|т\.?\s?в\.?\s?х\.?|твх)'
LABEL_TARGET = r'(?:цел[ьиейях]+|ц[іi]л[ьіи]+|targets?|tp|тейк[а-я\-]*|take[\s\-]*profits?|тп)'
LABEL_STOP = r'(?:стоп[\s\-]*лосс[а-я]*|стоп[а-я]*|stop[\s\-]*loss|stop|sl)'
LABEL_LEV = r'(?:кредитн[а-я]+\s+плеч[а-я]+|плеч[а-я]+|leverage|lev)'
LONG_RE = re.compile(r'\blong\b|лонг|\bbuy\b|покупк|купівл|купить|купити', re.I)
SHORT_RE = re.compile(r'\bshort\b|шорт|\bsell\b|продаж|продать|продати', re.I)
SKIP_SYMS = {'USDT', 'USD', 'PERP', 'LONG', 'SHORT', 'SIGNAL', 'BUY', 'SELL', 'TP', 'SL', 'ENTRY', 'STOP', 'TARGET', 'TARGETS', 'CROSS', 'ISOLATED', 'X'}
TAG_SKIP = {'signal', 'long', 'short', 'сигнал', 'лонг', 'шорт', 'crypto', 'futures', 'binance', 'bingx', 'bybit', 'trade', 'free', 'vip'}
EMOJI_RE = re.compile('[\U0001F000-\U0001FAFF☀-➿️‍⬀-⯿←-⇿■-◿]+')


def to_float(s):
    """'0,2345' → 0.2345; '1 234.5' → 1234.5; '1,234,567' → 1234567; одиночная запятая — десятичная."""
    s = s.replace(' ', '').replace(' ', '').replace(' ', '')
    if ',' in s and '.' in s:
        s = s.replace(',', '')
    elif re.fullmatch(r'\d{1,3}(?:,\d{3}){2,}', s):
        s = s.replace(',', '')
    else:
        s = s.replace(',', '.')
    return float(s)


def _clean(text):
    t = (text or '').replace(' ', ' ').replace('`', '').replace('*', '').replace('__', '').replace('~~', '')
    t = EMOJI_RE.sub(' ', t)
    t = re.sub(r'[ \t]+', ' ', t)
    return t.strip()


def _strip_pct(s):
    return re.sub(r'[\(\[]?\s*[+\-−]?\s*' + NUMG + r'\s*%\s*[\)\]]?', ' ', s)


def find_symbol(t):
    m = re.search(r'(?<![A-Za-z0-9])#?\$?([A-Z0-9]{2,15})\s*[/\-_]\s*USDT\b', t)
    if m and m.group(1) not in SKIP_SYMS:
        return m.group(1)
    m = re.search(r'(?<![A-Za-z0-9])#?\$?([A-Z0-9]{2,15}?)USDT\b', t)
    if m and m.group(1) not in SKIP_SYMS:
        return m.group(1)
    m = re.search(r'(?:coin|pair|монета|пара|актив|токен|символ|тикер)\s*[:\-]\s*#?\$?([A-Za-z0-9]{2,15})', t, re.I)
    if m and m.group(1).upper() not in SKIP_SYMS:
        return m.group(1).upper()
    for m in re.finditer(r'[#$]([A-Za-zА-Яа-я0-9]{2,15})', t):
        w = m.group(1)
        if w.lower() not in TAG_SKIP and re.fullmatch(r'[A-Za-z0-9]+', w):
            return w.upper()
    m = re.search(r'(?:long|short|лонг|шорт|buy|sell)\s*[:\-]?\s*#?\$?([A-Z0-9]{2,15})\b', t, re.I)
    if m and m.group(1).upper() not in SKIP_SYMS and not m.group(1).isdigit() and m.group(1).upper() == m.group(1):
        return m.group(1)
    m = re.search(r'(?<![A-Za-z0-9])#?\$?([A-Z0-9]{2,15})\s*(?:long|short|лонг|шорт)\b', t, re.I)
    if m and m.group(1).upper() not in SKIP_SYMS and m.group(1).upper() == m.group(1) and not m.group(1).isdigit():
        return m.group(1)
    return None


def find_side(t):
    lg, sh = bool(LONG_RE.search(t)), bool(SHORT_RE.search(t))
    if lg and sh:
        return None, 'оба направления в тексте'
    if lg:
        return 'long', ''
    if sh:
        return 'short', ''
    return None, 'направление не найдено'


def find_leverage(t):
    m = re.search(LABEL_LEV + r'[^\d\n]{0,25}?(\d{1,3})\s*(?:[xх×])?', t, re.I)
    if not m:
        m = re.search(r'(?<![\w.])[xх×]\s?(\d{1,3})(?![\d.,])', t, re.I) or re.search(r'(?<![\w.,])(\d{1,3})\s?[xх×](?![\w])', t, re.I)
    if not m:
        m = re.search(r'(?:cross|isolated|кросс|изолир\w*)\s*[\(\[]?\s*(\d{1,3})', t, re.I)
    if m:
        v = int(m.group(1))
        if 1 <= v <= 150:
            return v
    return None


def _range_or_point(s):
    """Первое число или диапазон из строки. → (lo, hi) или None."""
    s = _strip_pct(s)
    m = re.search('(' + NUMG + ')' + RANGE_SEP + '(' + NUMG + ')', s)
    if m:
        a, b = to_float(m.group(1)), to_float(m.group(2))
        return (min(a, b), max(a, b))
    m = NUM_RE.search(s)
    if m:
        v = to_float(m.group(0))
        return (v, v)
    return None


def find_entry(t):
    for line in t.split('\n'):
        m = re.search(LABEL_ENTRY + r'\b[^\d\n]{0,30}?(?=\d)', line, re.I)
        if m:
            r = _range_or_point(line[m.end():])
            if r:
                return r
    m = re.search(LABEL_ENTRY + r'\b[^\d]{0,30}?(?=\d)', t, re.I)
    if m:
        return _range_or_point(t[m.end():m.end() + 80].split('\n')[0])
    return None


def find_stop(t):
    for line in t.split('\n'):
        m = re.search(r'(?<![а-яa-z])' + LABEL_STOP + r'(?![а-яa-z])[^\d\n%]{0,25}?(?=\d)', line, re.I)
        if m:
            tail = line[m.end():]
            mm = NUM_RE.search(tail)
            if mm and not re.match(r'\s*%', tail[mm.end():]):
                return to_float(mm.group(0))
    return None


def _nums_from(s):
    s = _strip_pct(s)
    out = []
    for m in NUM_RE.finditer(s):
        out.append(to_float(m.group(0)))
    return out


def find_targets(t):
    """Цели: строки с меткой (tp1/цель 2/Цели:/Тейки:) + нумерованные строки под заголовком без чисел."""
    lines = t.split('\n')
    targets = []
    i = 0
    while i < len(lines):
        line = lines[i]
        m = re.search(r'(?<![а-яa-z])' + LABEL_TARGET + r'(?![а-яa-z])', line, re.I)
        if m:
            tail = line[m.end():]
            idx_m = re.match(r'\s*#?(\d)\s*(?:[:)\-–.]|\s)\s*(?=\d)', tail)           # 'tp1: 0.5' / 'цель 2 - 0.5'
            if idx_m and len(NUM_RE.findall(_strip_pct(tail))) >= 2:
                tail = tail[idx_m.end():]
            elif re.match(r'\s*#?\d(?![\d.,])\s*[:)\-–.]\s*\d', tail):
                tail = re.sub(r'^\s*#?\d\s*[:)\-–.]\s*', '', tail)
            nums = _nums_from(tail)
            if nums:
                targets.extend(nums)
            else:
                j = i + 1
                while j < len(lines):
                    mm = re.match(r'\s*(?:#?\d\s*[)\.\-:–]\s*)?(' + NUMG + r')(?!\d)', lines[j])
                    if not mm or re.search(LABEL_STOP, lines[j], re.I):
                        break
                    targets.append(to_float(mm.group(1)))
                    j += 1
                i = j - 1
        i += 1
    return targets[:5] if len(targets) <= 5 else targets[:5]


def _geometry_ok(side, lo, hi, targets, stop):
    entry_worst = lo if side == 'short' else hi
    if side == 'short':
        if not (stop > hi):
            return 'шорт: стоп не выше входа'
        if not all(x < lo for x in targets):
            return 'шорт: цели не ниже входа'
        if any(b >= a for a, b in zip(targets, targets[1:])):
            return 'цели не монотонны'
    else:
        if not (stop < lo):
            return 'лонг: стоп не ниже входа'
        if not all(x > hi for x in targets):
            return 'лонг: цели не выше входа'
        if any(b <= a for a, b in zip(targets, targets[1:])):
            return 'цели не монотонны'
    del entry_worst
    return ''


REPORT_PCT = re.compile(r'[+]?\s*(' + NUMG + r')\s*%')


def parse(text):
    """Разбор одного сообщения. kind: 'signal' (все поля и правдоподобная геометрия), 'report' (отчёт о цели: символ + %), 'unparsed' (похоже на сигнал, но не разобрано; reason),
    'other' (болтовня). Поля signal: symbol, side, leverage|None, entry_lo, entry_hi, targets, stop."""
    t = _clean(text)
    if not t:
        return {'kind': 'other', 'reason': 'пусто'}
    sym = find_symbol(t)
    side, side_why = find_side(t)
    entry = find_entry(t)
    stop = find_stop(t)
    targets = find_targets(t)
    has_stop_label = bool(re.search(r'(?<![а-яa-z])' + LABEL_STOP + r'(?![а-яa-z])', t, re.I))
    signalish = entry is not None or (side is not None and (targets or has_stop_label)) or (sym and targets and has_stop_label)
    if not signalish:
        rep = _parse_report(t, sym)
        return rep if rep else {'kind': 'other', 'reason': 'не сигнал'}
    miss = []
    if not sym:
        miss.append('символ не найден')
    if side is None:
        miss.append(side_why)
    if entry is None:
        miss.append('вход не найден')
    if stop is None:
        miss.append('стоп не найден (или только в процентах)')
    if not targets:
        miss.append('цели не найдены')
    if miss:
        return {'kind': 'unparsed', 'reason': '; '.join(miss), 'symbol': sym, 'side': side}
    why = _geometry_ok(side, entry[0], entry[1], targets, stop)
    if why:
        return {'kind': 'unparsed', 'reason': 'геометрия: ' + why, 'symbol': sym, 'side': side}
    return {'kind': 'signal', 'symbol': sym, 'side': side, 'leverage': find_leverage(t), 'entry_lo': entry[0], 'entry_hi': entry[1], 'targets': targets, 'stop': stop}


def _parse_report(t, sym):
    """Отчёт о цели: «ENA цель 2 достигнута +85%»: символ, номер цели (если есть), заявленный процент."""
    if not re.search(r'достигнут|досягнут|reached|hit|выполнен|виконан|профит|profit|прибыл|прибут|✅|цел|ціл|target|tp', t, re.I):
        return None
    m = REPORT_PCT.search(t)
    if not sym:
        b = re.match(r'\W*([A-Z0-9]{2,10})\b', t)
        sym = b.group(1) if b and b.group(1) not in SKIP_SYMS and not b.group(1).isdigit() else None
    if not m or not sym:
        return None
    tn = re.search(r'(?:цел[ьиейя]+|ціл[ьіи]+|target|tp)\s*#?(\d)', t, re.I) or re.search(r'(\d)\s*(?:-?[яйе]|st|nd|rd|th)?\s*(?:цел|ціл|target|tp)', t, re.I)
    return {'kind': 'report', 'symbol': sym, 'target_no': int(tn.group(1)) if tn else None, 'claimed_pct': to_float(m.group(1))}
