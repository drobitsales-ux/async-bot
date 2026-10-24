"""research_ma_lib.py — пре-регистрация «фабрики» простых систематических стратегий на новых классах активов (BOT_SPEC §2.41): библиотека конфигураций, подсчёт
числа тестов N, периоды TRAIN/VALID/LOCKBOX и ЗАЩИТА LOCKBOX. Этап 1 (v99): расчётов здесь нет — только определения, которые зафиксированы в BOT_SPEC ДО данных.
Параметры библиотеки — ровно из §2.41.4, других нет."""
import os
from collections import namedtuple
from datetime import datetime, timezone

# ── константы пре-регистрации (§2.41.4–2.41.9) ─────────────────────────────────────────────────────
RISK = 0.005                    # риск на инструмент (доля эквити на стоп)
STOP_ATR = 3.0                  # защитный стоп: 3·ATR20
ATR_N = 20
REBALANCE_BARS = 5              # T-MOM и X-MOM
XMOM_MIN_INSTRUMENTS = 6
NW_LAG = 5                      # Ньюи–Уэст (Бартлетт)
FDR_Q = 0.05                    # Бенджамини–Хохберг
VALID_MIN_T = 1.5
FINAL_MAX = 3                   # финалистов на LOCKBOX
FINAL_MIN_SHARPE = 0.5
FINAL_MIN_T = 2.0
FINAL_MAX_DD = 0.15
CALIB_N = 200                   # случайных стратегий на ячейку (класс, таймфрейм)
CALIB_MAX_FRAC = 0.05
SPREAD_BPS_DEFAULT = {'METALS': 3.0, 'FX': 1.0, 'INDICES': 2.0}      # сценарий (б): ENV SPREAD_BPS_<КЛАСС>
CLASSES = ('CRYPTO', 'METALS', 'FX', 'INDICES')
TIMEFRAMES = ('1D', '4h', '1h')

PERIODS = {'TRAIN': ('2020-01-01', '2023-12-31'), 'VALID': ('2024-01-01', '2024-12-31'), 'LOCKBOX': ('2025-01-01', None)}


def spread_bps(cls: str) -> float:
    """Сценарий (б), б.п.: ENV SPREAD_BPS_METALS / SPREAD_BPS_FX / SPREAD_BPS_INDICES (по умолчанию 3 / 1 / 2)."""
    try:
        return float(os.getenv(f'SPREAD_BPS_{cls}', str(SPREAD_BPS_DEFAULT[cls])))
    except (ValueError, KeyError):
        return SPREAD_BPS_DEFAULT.get(cls, 0.0)


# ── библиотека конфигураций ──────────────────────────────────────────────────────────────────────────
Config = namedtuple('Config', 'id family params cross_sectional')


def library():
    """Все конфигурации фабрики (§2.41.4): list[Config]. Порядок и состав фиксированы."""
    out = []
    for fast, slow in ((10, 50), (20, 100), (50, 200)):
        out.append(Config(f'T-MA-{fast}-{slow}', 'T-MA', (('fast', fast), ('slow', slow)), False))
    for n in (20, 55, 100):
        out.append(Config(f'T-DC-{n}', 'T-DC', (('n', n), ('exit_n', n // 2)), False))
    for lb in (21, 63, 126, 252):
        out.append(Config(f'T-MOM-{lb}', 'T-MOM', (('lookback', lb), ('rebalance', REBALANCE_BARS)), False))
    for filt in (False, True):
        out.append(Config(f'R-RSI2-{"SMA200" if filt else "plain"}', 'R-RSI2', (('lo', 10), ('hi', 90), ('exit', 50), ('sma200_filter', filt)), False))
    for z in (2.0, 2.5):
        out.append(Config(f'R-BB-{z}', 'R-BB', (('z', z), ('sma', 20)), False))
    out.append(Config('B-VC-NR7', 'B-VC', (('nr', 7), ('hold', 5)), False))
    for lb in (21, 63):
        out.append(Config(f'X-MOM-{lb}', 'X-MOM', (('lookback', lb), ('rebalance', REBALANCE_BARS)), True))
    return out


def configs_for(n_instruments: int):
    """Конфигурации, допустимые для класса с n инструментами (X-MOM — только при n ≥ 6)."""
    return [c for c in library() if (not c.cross_sectional) or n_instruments >= XMOM_MIN_INSTRUMENTS]


def count_tests(cells):
    """N = число тестов. cells: iterable of (класс, таймфрейм, число инструментов). Единица теста = конфигурация × класс × таймфрейм."""
    return sum(len(configs_for(n)) for _cls, _tf, n in cells)


def cells_from_spec(spec):
    """spec = {класс: {'tfs': [...], 'n': {tf: число инструментов} или int}} → список ячеек (класс, tf, n)."""
    out = []
    for cls, d in spec.items():
        for tf in d['tfs']:
            n = d['n'][tf] if isinstance(d['n'], dict) else d['n']
            out.append((cls, tf, n))
    return out


# границы N по кандидатам §2.41.5 (CRYPTO 16; METALS 3; FX 5; INDICES 4)
SPEC_MIN = {'CRYPTO': {'tfs': ['1D', '4h', '1h'], 'n': 16}, 'METALS': {'tfs': ['1D'], 'n': 3}, 'FX': {'tfs': ['1D'], 'n': 5}, 'INDICES': {'tfs': ['1D'], 'n': 4}}
SPEC_MAX = {'CRYPTO': {'tfs': ['1D', '4h', '1h'], 'n': 16}, 'METALS': {'tfs': ['1D', '4h', '1h'], 'n': 3}, 'FX': {'tfs': ['1D', '4h', '1h'], 'n': 5},
            'INDICES': {'tfs': ['1D'], 'n': 4}}


def n_bounds():
    return count_tests(cells_from_spec(SPEC_MIN)), count_tests(cells_from_spec(SPEC_MAX))


# ── периоды и защита LOCKBOX ─────────────────────────────────────────────────────────────────────────
class LockboxError(Exception):
    """Попытка прочитать LOCKBOX без флага финала (или повторно)."""


def _ms(date_str):
    return int(datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc).timestamp() * 1000)


def period_bounds_ms(name):
    """(начало включительно, конец исключительно или None) в мс UTC."""
    lo, hi = PERIODS[name]
    return _ms(lo), (_ms(hi) + 86_400_000 if hi else None)


_lockbox_state = {'read': False}


def slice_period(ts, name):
    """Булева маска баров периода TRAIN/VALID по времени ts (мс). LOCKBOX обычным срезом НЕ читается — только read_lockbox(final=True)."""
    import numpy as np
    if name == 'LOCKBOX':
        raise LockboxError('LOCKBOX нельзя читать обычным срезом: используйте read_lockbox(..., final=True) на финальном шаге C')
    lo, hi = period_bounds_ms(name)
    ts = np.asarray(ts)
    return (ts >= lo) & (ts < hi)


def read_lockbox(ts, *arrays, final=False):
    """Единственный путь к данным LOCKBOX: требует final=True (шаг C) и работает ОДИН раз за процесс. → (ts, *arrays) периода LOCKBOX."""
    import numpy as np
    if final is not True:
        raise LockboxError('LOCKBOX не читается до финального шага: нужен флаг final=True')
    if _lockbox_state['read']:
        raise LockboxError('LOCKBOX уже прочитан: шаг C выполняется один раз, повтор запрещён')
    _lockbox_state['read'] = True
    lo, _ = period_bounds_ms('LOCKBOX')
    ts = np.asarray(ts)
    m = ts >= lo
    return (ts[m],) + tuple(np.asarray(a)[m] for a in arrays)
