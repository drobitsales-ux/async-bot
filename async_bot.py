"""
╔══════════════════════════════════════════════════════════════════════╗
║  UNIFIED SMC + RSI MEAN REVERSION BOT  v10.0  PROP FIRM EDITION     ║
║  BingX Perpetual Futures | Render-Ready | $10k → $100k Path         ║
║                                                                      ║
║  СТРАТЕГИИ:                                                          ║
║  [SMC]  Smart Money Concepts — CHoCH + FVG + Killzones              ║
║  [RSI]  Mean Reversion — RSI Extreme + Pattern + SMA/VWAP Magnet    ║
║  [SA]   Smart Analysis — AI-driven VWAP Mean Reversion on BTC       ║
║                                                                      ║
║  ИСПРАВЛЕНИЯ vs RSI bot v8.30:                                       ║
║  [R-FIX-1]  BASE_RISK 2% → 0.75% | MAX_SL 4.5% → 2.5%             ║
║  [R-FIX-2]  MAX_POSITIONS 3 → 3 shared (SMC+RSI budget)            ║
║  [R-FIX-3]  SL = min(l) - ATR*3.0 → ATR*1.5 (tight + safe)        ║
║  [R-FIX-4]  RSI thresholds 28/72 → 25/75 (cleaner signals)         ║
║  [R-FIX-5]  LLM decision 'reject' теперь реально блокирует вход     ║
║  [R-FIX-6]  BTC Flat больше не блокирует лонги (лишний фильтр)     ║
║  [R-FIX-7]  vol_ratio потолок 8x → 12x (не режем лучшие импульсы)  ║
║  [R-FIX-8]  EXCLUDED_KEYWORDS: фиксирован фильтр (parts, не full)  ║
║  [R-FIX-9]  telebot + requests → aiohttp (единый async стек)        ║
║  [R-FIX-10] News: точное совпадение → окно ±15 мин                  ║
║  [R-FIX-11] Daily circuit breaker: стоп при DD > 2.5% за день      ║
║  [R-FIX-12] Отдельные таблицы позиций (smc_pos / rsi_pos)          ║
║                                                                      ║
║  ПАТЧ v10.1 (по анализу логов деплоя):                              ║
║  [P-1] tg(): логирует WARNING если TOKEN/CHAT_ID не заданы         ║
║  [P-2] oracle_volume: advisory (не блокирует), только логирует     ║
║  [P-3] MIN_VOL_USDT на BingX-тикере, не внешних биржах            ║
║  [P-4] scan_smc: диагностический лог env-переменных при старте     ║
║  [P-5] RSI_LONG_MAX/SHORT_MIN расширены до 27/73 для большего охвата║
╚══════════════════════════════════════════════════════════════════════╝
"""

import asyncio
import json
import os
import logging
import sqlite3
import time
import aiohttp
import numpy as np
import ccxt.async_support as ccxt_async
from datetime import datetime, timezone, timedelta
from threading import Thread
from http.server import BaseHTTPRequestHandler, HTTPServer

# ═══════════════════════════════════════════════════════
#  КОНФИГУРАЦИЯ
# ═══════════════════════════════════════════════════════
DB_PATH       = '/data/bot.db' if os.path.exists('/data') else 'bot.db'
TOKEN         = os.getenv('TELEGRAM_TOKEN')
# ── Telegram Chat ID ────────────────────────────────────
# Читаем из нескольких возможных названий переменной.
# На Render добавьте ОДНУ из переменных (любое название):
#   GROUP_CHAT_ID = -1003407154454
#   CHAT_ID       = -1003407154454
#   TG_CHAT_ID    = -1003407154454
# Значение: результат команды /chatid в вашей группе через @userinfobot
def _parse_chat_id() -> int:
    """Читает Chat ID из любого из трёх возможных имён переменной окружения."""
    for var_name in ('GROUP_CHAT_ID', 'CHAT_ID', 'TG_CHAT_ID'):
        raw = os.getenv(var_name, '').strip().strip('"').strip("'")
        if raw:
            try:
                val = int(raw)
                # print используется т.к. logging ещё не сконфигурирован
                print(f"[INIT] Chat ID прочитан из {var_name}: {val}", flush=True)
                return val
            except ValueError:
                print(f"[INIT] ❌ {var_name}='{raw}' не число — пропуск", flush=True)
    print("[INIT] ⚠️  GROUP_CHAT_ID/CHAT_ID/TG_CHAT_ID не заданы — Telegram выключен", flush=True)
    return -1

CHAT_ID = _parse_chat_id()
BINGX_KEY     = os.getenv('BINGX_API_KEY')
BINGX_SECRET  = os.getenv('BINGX_SECRET')
GEMINI_KEY    = os.getenv('GEMINI_API_KEY')
OPENROUTER_KEY = os.getenv('OPENROUTER_API_KEY', '')  # https://openrouter.ai (бесплатно)

# ── Риск-параметры (оба алгоритма) ─────────────────────
RISK_PER_TRADE   = 0.01     # [USER] 1% на сделку (тест; для проп → 0.0075)
RISK_WEEKEND     = 0.01     # [USER] 1% в выходные (тест; для проп → 0.00375)
MAX_TOTAL_POS    = 3        # [R-FIX-2] суммарно SMC+RSI
MAX_PER_DIR      = 2        # макс 2 лонга или 2 шорта
# ── [MOMENTUM] режим тренд-следования (shadow по умолчанию) ──
MOMENTUM_ENABLED = os.getenv('MOMENTUM_ENABLED', 'true').lower() == 'true'   # детект+лог
MOMENTUM_LIVE    = os.getenv('MOMENTUM_LIVE', 'false').lower() == 'true'      # реальная торговля
MOM_ADX_MIN      = 25      # тренд должен быть сильным
MOM_TRAIL_ATR    = float(os.getenv('MOM_TRAIL_ATR', '1.5'))  # чандельер-трейл множитель ATR (env-настройка; 3.0 не фиксировал прибыль → 1.5)
MOM_MIN_QUOTE    = 5000.0   # мин. оборот свечи USDT
# [STEP-C] RSI-фильтр входа: не ловить истощённые движения
# (не шортить уже перепроданное / не лонговать уже перекупленное)
MOM_RSI_SHORT_MAX = float(os.getenv('MOM_RSI_SHORT_MAX', '35'))  # шорт только если RSI > этого
MOM_RSI_LONG_MIN  = float(os.getenv('MOM_RSI_LONG_MIN', '65'))
# [PULLBACK] вход по откату к EMA20 внутри тренда (НЕ пробой экстремума)
PB_ENABLED   = os.getenv('PB_ENABLED', 'true').lower() == 'true'   # shadow-детект pullback
PB_LIVE      = os.getenv('PB_LIVE', 'false').lower() == 'true'      # [PB] gated live (микро-размер)
PB_RISK_MULT = float(os.getenv('PB_RISK_MULT', '0.25'))            # [PB] 25% риска до накопления n>=30
PB_NEAR_PCT  = float(os.getenv('PB_NEAR_PCT', '0.012'))  # близость к EMA20 (1.2%)
PB_RSI_LO    = float(os.getenv('PB_RSI_LO', '40'))       # RSI reset зона: низ
PB_RSI_HI    = float(os.getenv('PB_RSI_HI', '60'))       # RSI reset зона: верх   # лонг только если RSI < этого
# [SHADOW] кулдаун: не пересэмплировать тот же символ+стратегию+направление
SHADOW_COOLDOWN_BARS = int(os.getenv('SHADOW_COOLDOWN_BARS', '6'))
# [v18] час отправки 'Итоги дня' (UTC). 19 UTC = 22:00 Киев. Настраивается.
REPORT_HOUR_UTC = int(os.getenv('REPORT_HOUR_UTC', '19'))
# [v19] SINGLE-ASSET алгоритм: mean reversion BTC от дневного VWAP (shadow)
SA_ENABLED   = os.getenv('SA_ENABLED', 'true').lower() == 'true'
SA_SYMBOL    = os.getenv('SA_SYMBOL', 'BTC/USDT:USDT')   # один актив
SA_ATR_DIST  = float(os.getenv('SA_ATR_DIST', '1.5'))    # отклонение от VWAP в ATR для входа
SA_RSI_LO    = float(os.getenv('SA_RSI_LO', '30'))       # лонг: RSI ниже этого (перепродан)
SA_RSI_HI    = float(os.getenv('SA_RSI_HI', '70'))       # шорт: RSI выше этого (перекуплен)
SA_WINDOW    = os.getenv('SA_WINDOW', '')                # окно UTC 'HH-HH' или '' = весь день
SA_LIVE      = os.getenv('SA_LIVE', 'false').lower() == 'true'  # [v23] реальная торговля SA
SA_HIST_OFFSET = 37  # сделок SA до перевода в live (включены в общий счётчик)
SA_PARTIAL_PCT = float(os.getenv('SA_PARTIAL_PCT', '0.8'))  # [SA-EXIT] фикс 50% при +0.8% (MFE гаснет ~1%)
SA_TRAIL_ATR   = float(os.getenv('SA_TRAIL_ATR',  '0.8'))   # [SA-EXIT] чувствительный трейл хвоста (MR-ход короткий)
LEVERAGE         = 5
MIN_VOL_USDT     = 500_000    # [TEST] снижен для проверки (было 3_000_000)
MAX_SL_PCT       = 2.5        # [R-FIX-1] жёсткий лимит SL %
MIN_SL_PCT       = 1.0        # мин. SL чтобы не убивало спредом
MAX_TRADE_MIN_SMC = 180       # SMC: 12 свечей (3ч) — даём отработать структуре
MAX_TRADE_MIN_RSI = 240       # RSI MR: 16 свечей (4ч) — возврат к среднему медленнее
MAX_TRADE_MIN_SA  = 150       # [v36] SA: возврат к дневному VWAP; нет за 2.5ч → сетап мёртв
FEE_RATE         = 0.0005
DAILY_DD_LIMIT   = float(os.getenv('DAILY_DD_LIMIT', '0.025'))    # [R-FIX-11] стоп торговли при -2.5% за день
SCAN_LIMIT       = 80       # [EXPAND] 60→80: больше монет, +33% шансов на сетап
SCAN_SEM         = 60       # [EXPAND] 50→60: больше параллелизма для 80 символов
MIN_LOT_USDT     = 1.0      # Минимальный размер позиции в USDT (ниже → force close)

# ── Webhook для копи-трейдинга (Bybit Worker и другие) ──────────
# Когда BingX открывает сделку → POST на воркеры со структурой сигнала
# Добавьте в Render Environment: WORKER_URLS=https://bybit-worker.onrender.com/signal
# Несколько воркеров через запятую: url1,url2,url3
_worker_urls: list = [u.strip() for u in
                      os.getenv('WORKER_URLS', '').split(',') if u.strip()]
WORKER_SECRET = os.getenv('WORKER_SECRET', 'change-me-secret')  # общий секрет

# ── SMC-параметры ───────────────────────────────────────
SMC_TF          = '15m'
SMC_PIVOT_ORDER = 5

# ── RSI-параметры ───────────────────────────────────────
RSI_TF          = '15m'
RSI_LONG_MAX    = 30    # [FIX] 27→30: на 15m RSI<27 почти недостижимо в логах
RSI_SHORT_MIN   = 70    # [FIX] 73→70: симметрично
RSI_PERIOD      = 14

# ── Исключения (части имён символов) ───────────────────
# [R-FIX-8] Проверка через `any(kw in sym for kw in EXCLUDED_PARTS)`
EXCLUDED_PARTS = [
    'BTC', 'ETH', 'SOL', 'BNB', 'XRP',
    'NCS', 'NCFX', 'NCCO', 'NCSI', 'NIKKEI', 'NASDAQ', 'SP500',
    'GOLD', 'SILVER', 'XAU', 'PAXG', 'EUR',
    'LUNC', 'USTC', 'USDC', 'FART', 'PEPE', 'SHIB', 'DOGE',
    'WIF', 'BONK', 'FLOKI', 'BOME', 'MEME', 'TURBO',
    'SATS', 'RATS', 'ORDI', '1000',
]

# ── Состояние ────────────────────────────────────────────
smc_positions   = []   # [R-FIX-12] отдельные списки
rsi_positions   = []
sa_positions    = []   # [v24] SA отдельный список позиций
pb_positions    = []   # [PB] отдельный список live-позиций pullback (gated)
_sa_last_entry  = 0.0  # [v24] cooldown: timestamp последнего SA входа (не через notified)
daily_stats     = {
    'pnl_pct': 0.0, 'trades': 0, 'wins': 0,
    'start_balance': 0.0, 'stat_date': None,
    'smc_trades': 0, 'rsi_trades': 0,
}
news_events     = []       # [float timestamp, ...]
notified        = {}       # {sym: timestamp}  cooldown 4h
markets_cache   = None
markets_ts      = 0.0
news_ts         = 0.0
http            = None     # единая aiohttp.ClientSession
circuit_open    = False    # [R-FIX-11] дневной DD-стоп
_tg_offset         = 0     # Telegram getUpdates offset
_sync_counter      = 0     # счётчик до авто-синхронизации
_daily_report_sent = False  # флаг отчёта сегодня (19:00 UTC)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s'
)

# ── Биржи ────────────────────────────────────────────────
exchange = ccxt_async.bingx({
    'apiKey': BINGX_KEY, 'secret': BINGX_SECRET,
    'options': {'defaultType': 'swap', 'marginMode': 'isolated'},
    'enableRateLimit': True,
})
binance = ccxt_async.binance({
    'enableRateLimit': True,
    'options': {'defaultType': 'future'},
})
bybit = ccxt_async.bybit({
    'enableRateLimit': True,
    'options': {'defaultType': 'linear'},
})
mexc = ccxt_async.mexc({
    'enableRateLimit': True,
    'options': {'defaultType': 'swap'},
})

# ═══════════════════════════════════════════════════════
#  БАЗА ДАННЫХ
# ═══════════════════════════════════════════════════════
def get_db():
    return sqlite3.connect(DB_PATH, check_same_thread=False)

def init_db():
    conn = get_db()
    c = conn.cursor()
    # [R-FIX-12] раздельные таблицы стратегий
    c.execute("CREATE TABLE IF NOT EXISTS smc_pos  (id INTEGER PRIMARY KEY, data TEXT)")
    c.execute("CREATE TABLE IF NOT EXISTS rsi_pos  (id INTEGER PRIMARY KEY, data TEXT)")
    c.execute("CREATE TABLE IF NOT EXISTS sa_pos   (id INTEGER PRIMARY KEY, data TEXT)")  # [v29]
    c.execute("CREATE TABLE IF NOT EXISTS pb_pos   (id INTEGER PRIMARY KEY, data TEXT)")  # [PB]
    c.execute("""CREATE TABLE IF NOT EXISTS stats
                 (id INTEGER PRIMARY KEY, pnl_pct REAL, trades INTEGER,
                  wins INTEGER, start_balance REAL, stat_date TEXT,
                  smc_trades INTEGER, rsi_trades INTEGER)""")
    conn.commit()
    conn.close()

def _save(table: str, data: list):
    try:
        conn = get_db()
        # [SELF-HEAL] гарантируем существование таблицы перед записью —
        # защита от "no such table" при частичном патче / персистентном диске /
        # порядке запуска. IF NOT EXISTS не трогает уже существующие таблицы.
        conn.execute(f"CREATE TABLE IF NOT EXISTS {table} (id INTEGER PRIMARY KEY, data TEXT)")
        conn.execute(f"INSERT OR REPLACE INTO {table} (id, data) VALUES (1, ?)",
                     (json.dumps(data),))
        conn.commit()
        conn.close()
    except Exception as e:
        logging.error(f"DB save {table}: {e}")

def _load(table: str) -> list:
    try:
        conn = get_db()
        row = conn.execute(f"SELECT data FROM {table} WHERE id=1").fetchone()
        conn.close()
        return json.loads(row[0]) if row else []
    except:
        return []

def save_all():
    _save('smc_pos', smc_positions)
    _save('rsi_pos', rsi_positions)
    _save('sa_pos',  sa_positions)   # [v24]
    _save('pb_pos',  pb_positions)   # [PB]
    try:
        conn = get_db()
        conn.execute(
            "INSERT OR REPLACE INTO stats VALUES (1,?,?,?,?,?,?,?)",
            (daily_stats['pnl_pct'], daily_stats['trades'], daily_stats['wins'],
             daily_stats['start_balance'], str(daily_stats['stat_date']),
             daily_stats['smc_trades'], daily_stats['rsi_trades'])
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logging.error(f"DB save stats: {e}")

def load_all():
    global smc_positions, rsi_positions, sa_positions, pb_positions, daily_stats
    smc_positions = _load('smc_pos')
    rsi_positions = _load('rsi_pos')
    sa_positions  = _load('sa_pos')   # [v24]
    pb_positions  = _load('pb_pos')   # [PB]
    try:
        conn = get_db()
        row = conn.execute("SELECT * FROM stats WHERE id=1").fetchone()
        conn.close()
        if row:
            (_, daily_stats['pnl_pct'], daily_stats['trades'], daily_stats['wins'],
             daily_stats['start_balance'], date_str,
             daily_stats['smc_trades'], daily_stats['rsi_trades']) = row
            try:
                daily_stats['stat_date'] = datetime.strptime(date_str, "%Y-%m-%d").date()
            except:
                pass
    except:
        pass

# ═══════════════════════════════════════════════════════
#  TELEGRAM
# ═══════════════════════════════════════════════════════
async def tg(text: str):
    # [P-1] Логируем причину молчания вместо тихого return
    if not TOKEN:
        logging.warning("⚠️ [TG] TELEGRAM_TOKEN не задан — сообщение не отправлено")
        return
    if CHAT_ID == -1:
        logging.warning(
            "⚠️ [TG] GROUP_CHAT_ID = -1 (не задан).\n"
            "    Установите в Render → Environment → GROUP_CHAT_ID\n"
            "    Для супергруппы формат: -1003407154454 (префикс -100 + ID)\n    Название переменной: GROUP_CHAT_ID, CHAT_ID или TG_CHAT_ID\n"
            "    Для получения ID: перешлите сообщение боту @userinfobot"
        )
        return
    if not http:
        return
    try:
        async with http.post(
            f"https://api.telegram.org/bot{TOKEN}/sendMessage",
            json={"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML"},
            timeout=aiohttp.ClientTimeout(total=5)
        ) as resp:
            if resp.status != 200:
                body = await resp.text()
                logging.warning(f"⚠️ [TG] API вернул {resp.status}: {body[:200]}")
    except Exception as e:
        logging.warning(f"⚠️ [TG] Ошибка отправки: {e}")

# ═══════════════════════════════════════════════════════
#  НОВОСТНОЙ ФИЛЬТР  [R-FIX-10]
# ═══════════════════════════════════════════════════════
async def refresh_news():
    """Загружает высоко-импактные USD-события. Обновляется каждые 6 часов."""
    global news_events, news_ts
    url = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
    headers = {'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json'}
    try:
        async with http.get(url, headers=headers,
                            timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status == 200:
                data = await resp.json()
                news_events = []
                for item in data:
                    if item.get('country') == 'USD' and item.get('impact') == 'High':
                        try:
                            news_events.append(
                                datetime.fromisoformat(item['date']).timestamp()
                            )
                        except:
                            pass
                news_ts = time.time()
                logging.info(f"📰 [NEWS] {len(news_events)} USD High-Impact events loaded")
    except Exception as e:
        logging.warning(f"News fetch failed: {e}")

def is_news_now() -> bool:
    """[R-FIX-10] ±15 минут вокруг события."""
    now = time.time()
    return any(abs(ev - now) < 900 for ev in news_events)

# ═══════════════════════════════════════════════════════
#  CIRCUIT BREAKER  [R-FIX-11]
# ═══════════════════════════════════════════════════════
def check_circuit_breaker() -> bool:
    """Возвращает True если торговля разрешена."""
    global circuit_open
    if daily_stats['pnl_pct'] <= -DAILY_DD_LIMIT:
        if not circuit_open:
            circuit_open = True
            logging.warning(f"🔴 [CIRCUIT BREAKER] Дневной DD {daily_stats['pnl_pct']*100:.2f}% → торговля остановлена")
            asyncio.create_task(
                tg(f"🔴 <b>CIRCUIT BREAKER</b>\nДневной убыток {daily_stats['pnl_pct']*100:.2f}% "
                   f"превысил лимит {DAILY_DD_LIMIT*100:.1f}%\nТорговля остановлена до следующего дня.")
            )
        return False
    circuit_open = False
    return True

# ═══════════════════════════════════════════════════════
#  ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ═══════════════════════════════════════════════════════
def is_session() -> bool:
    """
    Расширенная сессия: 06:30 – 17:00 UTC (Киев 09:30 – 20:00).
    Покрывает: Pre-London + London + London/NY overlap + NY.
    Убран перерыв 10:30-13:00 — именно там происходит Pre-NY buildup
    и профессионалы делают лучшие CHoCH/FVG сетапы.
    Крипта 24/7 — форекс-перерывы не актуальны.
    """
    t = datetime.now(timezone.utc).time()
    session_start = datetime.strptime("06:30", "%H:%M").time()
    session_end   = datetime.strptime("17:00", "%H:%M").time()
    return session_start <= t <= session_end

def is_weekend() -> bool:
    return datetime.now(timezone.utc).weekday() >= 5

def current_risk() -> float:
    return RISK_WEEKEND if is_weekend() else RISK_PER_TRADE

def all_positions() -> list:
    return smc_positions + rsi_positions + sa_positions + pb_positions  # [v24] SA + [PB]

def calc_vwap(h, l, c, v) -> float:
    denom = np.sum(v)
    return float(np.sum(v * (h + l + c) / 3) / denom) if denom > 0 else float(c[-1])

def calc_rsi(prices, w: int = 14) -> float:
    if len(prices) < w + 1:
        return 50.0
    d = np.diff(prices[-(w + 1):])
    g, lo = np.maximum(d, 0), np.abs(np.minimum(d, 0))
    ag, al = np.mean(g), np.mean(lo)
    if al == 0:
        return 100.0
    return float(100 - 100 / (1 + ag / al))

def calc_atr(h, l, c, w: int = 14) -> float:
    tr = np.maximum(h[1:] - l[1:],
         np.maximum(np.abs(h[1:] - c[:-1]),
                    np.abs(l[1:] - c[:-1])))
    return float(np.mean(tr[-w:]))

def calc_adx(h, l, c, w: int = 14) -> float:
    """
    Average Directional Index — мера силы тренда (0-100).
    ADX > 25: сильный тренд (хорошо для SMC)
    ADX < 20: флэт (хорошо для RSI MR)
    """
    if len(h) < w + 2:
        return 0.0
    h, l, c = np.array(h), np.array(l), np.array(c)
    # True Range
    tr = np.maximum(h[1:] - l[1:],
         np.maximum(np.abs(h[1:] - c[:-1]),
                    np.abs(l[1:] - c[:-1])))
    # Directional Movement
    up_move   = h[1:] - h[:-1]
    down_move = l[:-1] - l[1:]
    plus_dm  = np.where((up_move > down_move) & (up_move > 0), up_move, 0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0)
    # Smoothed
    atr      = np.mean(tr[-w:]) if len(tr) >= w else np.mean(tr)
    plus_di  = 100 * np.mean(plus_dm[-w:]) / atr if atr > 0 else 0
    minus_di = 100 * np.mean(minus_dm[-w:]) / atr if atr > 0 else 0
    # DX
    di_sum = plus_di + minus_di
    dx = 100 * abs(plus_di - minus_di) / di_sum if di_sum > 0 else 0
    return float(dx)

def calc_ema(data, w: int) -> float:
    if len(data) < w:
        return float(data[-1])
    alpha = 2 / (w + 1)
    ema = float(data[0])
    for p in data[1:]:
        ema = p * alpha + ema * (1 - alpha)
    return ema

def get_pivots(highs, lows, order: int = 5):
    """Swing High / Low (NumPy, без scipy)."""
    h_idx, l_idx = [], []
    n = len(highs)
    for i in range(order, n - order):
        win_h = highs[i - order: i + order + 1]
        if highs[i] == np.max(win_h) and np.sum(win_h == highs[i]) == 1:
            h_idx.append(i)
        win_l = lows[i - order: i + order + 1]
        if lows[i] == np.min(win_l) and np.sum(win_l == lows[i]) == 1:
            l_idx.append(i)
    return h_idx, l_idx

def find_fvg(h, l, mode: str, lookback: int = 15):  # баров поиска FVG
    """Fair Value Gap: разрыв между свечой i-1 и i+1 через среднюю i."""
    for i in range(1, min(lookback, len(h) - 1)):
        idx = len(h) - 1 - i
        if idx < 1 or idx + 1 >= len(h):
            continue
        if mode == 'Long' and h[idx - 1] < l[idx + 1]:
            return {'top': float(l[idx + 1]), 'bottom': float(h[idx - 1])}
        if mode == 'Short' and l[idx - 1] > h[idx + 1]:
            return {'top': float(l[idx - 1]), 'bottom': float(h[idx + 1])}
    return None

# ═══════════════════════════════════════════════════════
#  ОРАКУЛЫ
# ═══════════════════════════════════════════════════════
# Кэш объёмов внутри цикла — не делаем внешние запросы повторно
_vol_cache: dict = {}  # {sym: (timestamp, bool)}
VOL_CACHE_TTL = 300    # 5 минут — объём не меняется быстро

async def oracle_volume(sym: str, bingx_vol: float = 0) -> bool:
    """
    Volume oracle v2: использует tickers_cache (уже загружен!) вместо
    3 отдельных API запросов. Экономит 2-3 сек на каждую проверку.
    Fallback на внешние биржи только если тикеры недоступны.
    """
    # Проверяем кэш внутри цикла
    now_t = time.time()
    if sym in _vol_cache:
        ts, result = _vol_cache[sym]
        if now_t - ts < VOL_CACHE_TTL:
            return result

    # Fast-path: BingX-объём из tickers_cache достаточен
    if bingx_vol >= MIN_VOL_USDT:
        _vol_cache[sym] = (now_t, True)
        return True

    # Средний объём: BingX < MIN_VOL но >= 200k — принимаем (BingX-эксклюзив)
    if bingx_vol >= 200_000:
        # Проверяем через кэш тикеров (уже загружен, без доп. запросов)
        cached = _tickers_cache.get(sym, {})
        cached_vol = float((cached or {}).get('quoteVolume', 0) or 0)
        if cached_vol > 0:
            result = cached_vol >= MIN_VOL_USDT * 0.5  # 50% порог для BingX
            _vol_cache[sym] = (now_t, result)
            return result
        _vol_cache[sym] = (now_t, True)  # если нет кэша — пропускаем
        return True

    # Низкий объём: быстрая проверка через внешние биржи (только 1 раз за 5 мин)
    try:
        base = sym.split('/')[0]
        async def _vol(exch, t):
            try:
                r = await asyncio.wait_for(exch.fetch_ticker(t), timeout=2.0)
                return float(r.get('quoteVolume', 0) or 0)
            except Exception:
                return 0.0
        vol_b, vol_y, vol_m = await asyncio.gather(
            _vol(binance, f'{base}/USDT'),
            _vol(bybit,   f'{base}/USDT'),
            _vol(mexc,    f'{base}/USDT'),
        )
        result = vol_b > MIN_VOL_USDT or vol_y > MIN_VOL_USDT or vol_m > MIN_VOL_USDT
        if any(v > 0 for v in (vol_b, vol_y, vol_m)):
            logging.debug(f'[VOL] {base} Bin:{vol_b:.0f} Bbt:{vol_y:.0f} MEXC:{vol_m:.0f}')
        _vol_cache[sym] = (now_t, result)
        return result
    except Exception:
        return bingx_vol >= 100_000


# ══════════════════════════════════════════════════════════════
#  AI ORACLE  —  Groq (primary) → Gemini (fallback) → Score (local)
#
#  Groq:   бесплатно, 14 400 req/day, стабильно, регистрация на groq.com
#  Gemini: резерв при ошибке Groq, 1500 req/day
#  Score:  локальный скоринг — всегда работает без API
#
#  Добавьте в Render Environment:
#    GROQ_API_KEY = gsk_xxxxxxxxxxxxxxxxxxxx
#    (GEMINI_API_KEY и OPENROUTER_API_KEY можно оставить как резервы)
# ══════════════════════════════════════════════════════════════

# ── Groq ─────────────────────────────────────────────────────
_groq_req_times: list = []
_groq_quota_ok  = True
_groq_quota_reset: float = 0.0
GROQ_RPM        = 25    # запас от лимита 30/min
GROQ_MODELS     = [
    'llama-3.1-8b-instant',     # очень быстро (~100ms)
    'llama-3.3-70b-versatile',  # лучшее качество
    'gemma2-9b-it',             # Google, запасная
]

async def oracle_groq(sym: str, strategy: str, mode: str,
                      price: float, extra: dict = None) -> dict:
    """
    Groq — основной AI Oracle.
    Бесплатно, 14 400 req/day, стабильные модели без :free суффикса.
    Регистрация: https://console.groq.com/keys
    """
    global _groq_quota_ok, _groq_quota_reset

    groq_key = os.getenv('GROQ_API_KEY', '')
    if not groq_key or not http:
        return {'ok': True, 'conf': 0, 'comment': 'groq_not_set'}

    # Quota check
    if not _groq_quota_ok:
        if time.time() < _groq_quota_reset:
            return {'ok': True, 'conf': 0, 'comment': 'groq_quota_wait'}
        _groq_quota_ok = True

    # Rate limit
    now_t = time.time()
    _groq_req_times[:] = [t for t in _groq_req_times if now_t - t < 60]
    if len(_groq_req_times) >= GROQ_RPM:
        wait = 61 - (now_t - _groq_req_times[0])
        await asyncio.sleep(max(0, wait))

    context = {'symbol': sym, 'strategy': strategy,
               'direction': mode, 'price': round(price, 6)}
    if extra:
        ctx_keys = ('rsi', 'vol_ratio', 'sma_dist', 'vwap_dist', 'btc_trend')
        context.update({k: v for k, v in extra.items() if k in ctx_keys})

    # Добавляем контекст BTC + числовые альт-метрики (факты из биржи, не выдумка AI)
    btc_ctx_str = context.get('btc_trend', 'Flat')
    _spread = context.get('eth_btc_spread', 0.0)
    _ascore = context.get('alt_score', 50)
    alt_ctx_str = (
        f"altseason ACTIVE (ETH/BTC spread {_spread:+.1f}%, alt_score {_ascore}/100)"
        if context.get('altseason')
        else f"no altseason (ETH/BTC spread {_spread:+.1f}%, alt_score {_ascore}/100)"
    )

    prompt = (
        'You are a crypto futures risk manager specializing in SMC and RSI mean reversion. '
        'Evaluate this trade and respond ONLY with valid JSON: '
        '{"decision":"approve"|"reject","confidence":0-100,"comment":"brief reason"}. '
        'HARD RULES — proven losses when violated: '
        # [v29] Разделяем правила по стратегиям — LLM путал SMC Short с SA Long
        'RULE 1 (SMC ONLY — Short): RSI < 32 → REJECT. TIA(28)SL, XMR(30)SL at absolute floor. '
        'IMPORTANT: RSI 32-45 after CHoCH is NORMAL — the bearish candle pulls RSI down naturally. Do NOT reject RSI 32-45 for SMC. '
        'RULE 2 (SMC ONLY — Long): RSI > 68 → REJECT. Overbought late entry. '
        'RULE 3 (SA STRATEGY — Mean Reversion / Counter-trend): '
        'If direction is SHORT: High RSI (> 65) means the asset is OVERBOUGHT. This is EXCELLENT for a Short. You MUST APPROVE. NEVER reject a Short because RSI is high. '
        'If direction is LONG: Low RSI (< 35) means the asset is OVERSOLD. This is EXCELLENT for a Long. You MUST APPROVE. NEVER reject a Long because RSI is low. '
        'SMC trend rules DO NOT APPLY to SA. SA explicitly trades against the local trend. '
        'RULE 4 (RSI MR): volume > 3.5x AND sma_dist < 3.5% → REJECT (trend impulse, not MR). '
        'RULE 5: volume > 3.5x AND sma_dist > 3.5% → consider APPROVE (blow-off top exhaustion). '
        'High volume = momentum impulse, NOT mean reversion opportunity. '
        'NEAR Vol=10.8x MFE=0.60%, IMX Vol=3.5x MFE=0.53%, DASH Vol=2.9x MFE=0.07%. '
        'RULE 6 (RSI MR): If RSI > 88 → REJECT even if volume normal. '
        'Extreme RSI values with any volume = strong momentum, not reversion. '
        'SOFT RULES (use judgement): '
        'BTC:Flat does NOT invalidate altcoin setups. '
        'During altseason, long setups on altcoins are more valid. '
        'RSI 40-60 neutral zone for SMC = valid structural trade. '
        f'Market context: BTC={btc_ctx_str}, {alt_ctx_str}. '
        f'Setup: {json.dumps(context)}'
    )

    url = 'https://api.groq.com/openai/v1/chat/completions'
    headers = {
        'Authorization': f'Bearer {groq_key}',
        'Content-Type': 'application/json',
    }

    _groq_req_times.append(time.time())

    for model in GROQ_MODELS:
        try:
            payload = {
                'model': model,
                'messages': [{'role': 'user', 'content': prompt}],
                'temperature': 0.1,
                'max_tokens': 120,
            }
            async with http.post(url, headers=headers, json=payload,
                                 timeout=aiohttp.ClientTimeout(total=8)) as resp:
                data = await resp.json()

                if resp.status == 429:
                    err_msg = str(data.get('error', {}).get('message', ''))
                    if 'day' in err_msg.lower() or 'daily' in err_msg.lower():
                        _groq_quota_ok = False
                        _groq_quota_reset = time.time() + 86400
                        logging.warning(f'🤖 [GROQ] Daily quota — резерв на 24ч')
                    else:
                        logging.warning(f'🤖 [GROQ] {model} 429 rate limit — следующая модель')
                    continue

                if resp.status != 200 or 'choices' not in data:
                    err = str(data.get('error', ''))[:80]
                    logging.warning(f'🤖 [GROQ] {model} {resp.status}: {err}')
                    continue

                raw = data['choices'][0]['message']['content'].strip()
                clean = raw
                if '```' in clean:
                    clean = clean.split('```')[1].lstrip('json').strip()

                try:
                    parsed = json.loads(clean)
                except json.JSONDecodeError:
                    ok_fb = 'approve' in raw.lower()
                    logging.info(f'🤖 [GROQ/{model[:12]}] {sym} text→{ok_fb}')
                    return {'ok': ok_fb, 'conf': 50, 'comment': 'text_fallback'}

                ok   = str(parsed.get('decision', '')).lower() == 'approve'
                conf = int(parsed.get('confidence', 50))
                comment = parsed.get('comment', '')
                if conf < 55:
                    ok = False
                verdict = 'APPROVE' if ok else 'REJECT'
                logging.info(
                    f'🤖 [GROQ/{model[:12]}] {sym} {strategy} {mode} '
                    f'→ {verdict} ({conf}/100) | {comment}'
                )
                return {'ok': ok, 'conf': conf, 'comment': comment}

        except Exception as e:
            logging.warning(f'🤖 [GROQ] {model} exception: {e}')
            continue

    logging.warning(f'🤖 [GROQ] все модели недоступны для {sym}')
    return {'ok': True, 'conf': 0, 'comment': 'groq_all_failed'}


# ── Локальный скоринг (резерв без API) ───────────────────────
def score_setup_local(strategy: str, mode: str, extra: dict) -> dict:
    """
    Алгоритмический скоринг сетапа без LLM.
    Работает всегда, не зависит от внешних API.
    """
    score = 50  # базовый балл
    reasons = []

    rsi = float(extra.get('rsi', 50))
    vol_ratio = float(extra.get('vol_ratio', 1.0))
    btc_trend = str(extra.get('btc_trend', 'Flat'))
    sma_dist = float(extra.get('sma_dist', 0))
    vwap_dist = float(extra.get('vwap_dist', 0))

    # [v26] SA — mean-reversion: логика ПРОТИВОПОЛОЖНА трендовой.
    # Вход ПРОТИВ тренда — это и есть суть стратегии, штрафовать нельзя.
    if strategy == 'SA':
        # RSI экстремум = основной сигнал
        if mode == 'Long' and rsi <= 30:
            score += 20; reasons.append('RSI deeply oversold')
        elif mode == 'Long' and rsi <= 35:
            score += 12; reasons.append('RSI oversold')
        elif mode == 'Short' and rsi >= 70:
            score += 20; reasons.append('RSI deeply overbought')
        elif mode == 'Short' and rsi >= 65:
            score += 12; reasons.append('RSI overbought')
        # Volume climax уже отфильтрован в single_asset_signal (>=2.0)
        # Здесь дополнительно поощряем экстремальный объём
        if vol_ratio >= 3.0:
            score += 10; reasons.append('volume climax')
        elif vol_ratio >= 2.0:
            score += 5; reasons.append('volume spike')
        # VWAP отклонение = мера растянутости (чем дальше — тем лучше для MR)
        if mode == 'Long' and vwap_dist <= -2.0:
            score += 10; reasons.append('deep below VWAP')
        elif mode == 'Short' and vwap_dist >= 2.0:
            score += 10; reasons.append('deep above VWAP')
        score = max(0, min(100, score))
        ok = score >= 55
        comment = ', '.join(reasons) if reasons else 'neutral'
        logging.info(
            f'📊 [LOCAL] {strategy} {mode} → {"APPROVE" if ok else "REJECT"} '
            f'({score}/100) | {comment}'
        )
        return {'ok': ok, 'conf': score, 'comment': f'local:{comment}'}

    # RSI alignment
    if mode == 'Long' and rsi < 35:
        score += 15; reasons.append('RSI oversold')
    elif mode == 'Short' and rsi > 65:
        score += 15; reasons.append('RSI overbought')
    elif mode == 'Long' and rsi > 70:
        score -= 20; reasons.append('RSI too high for long')
    elif mode == 'Short' and rsi < 30:
        score -= 20; reasons.append('RSI too low for short')

    # Volume
    if vol_ratio >= 3.0:
        score += 10; reasons.append('strong volume')
    elif vol_ratio >= 2.0:
        score += 5
    elif vol_ratio < 1.5:
        score -= 10; reasons.append('weak volume')

    # BTC trend alignment
    if (mode == 'Long' and btc_trend == 'Long') or        (mode == 'Short' and btc_trend == 'Short'):
        score += 10; reasons.append('BTC aligned')
    elif (mode == 'Long' and btc_trend == 'Short') or          (mode == 'Short' and btc_trend == 'Long'):
        score -= 15; reasons.append('BTC opposed')

    # VWAP alignment for RSI
    if strategy == 'RSI':
        if mode == 'Long' and vwap_dist < -1.5:
            score += 8; reasons.append('below VWAP pullback')
        elif mode == 'Short' and vwap_dist > 1.5:
            score += 8; reasons.append('above VWAP extension')

    score = max(0, min(100, score))
    ok = score >= 55
    comment = ', '.join(reasons) if reasons else 'neutral'
    logging.info(
        f'📊 [LOCAL] {strategy} {mode} → {"APPROVE" if ok else "REJECT"} '
        f'({score}/100) | {comment}'
    )
    return {'ok': ok, 'conf': score, 'comment': f'local:{comment}'}


# ── Единый каскадный AI Oracle ───────────────────────────────
async def oracle_ai(sym: str, strategy: str, mode: str,
                    price: float, extra: dict = None) -> dict:
    """
    Каскад: Groq → Gemini → Local Score
    Приоритет 1: Groq (стабильно, бесплатно, 14400/day)
    Приоритет 2: Gemini (если Groq недоступен)
    Приоритет 3: Локальный скоринг (всегда работает)
    """
    ctx = extra or {}

    # 1. Groq
    groq_key = os.getenv('GROQ_API_KEY', '')
    if groq_key and _groq_quota_ok:
        result = await oracle_groq(sym, strategy, mode, price, ctx)
        if result['comment'] not in ('groq_not_set', 'groq_quota_wait', 'groq_all_failed'):
            return result

    # 2. Gemini
    if GEMINI_KEY and _gemini_quota_ok:
        result = await oracle_gemini(sym, strategy, mode, price, ctx)
        if result['comment'] not in ('API not set', 'quota_wait'):
            return result

    # 3. Локальный скоринг
    logging.info(f'📊 [{strategy}] {sym} — AI недоступен, используем локальный скоринг')
    return score_setup_local(strategy, mode, ctx)



# Кэш решений Gemini: {sym_strategy_mode: (timestamp, result)}
_gemini_cache: dict = {}
GEMINI_CACHE_TTL = 3600  # 1 час — не спрашиваем повторно

# Rate limiter Gemini: free plan = 15 req/min
_gemini_req_times: list = []   # timestamps последних запросов
GEMINI_RPM_LIMIT  = 12         # оставляем запас (из 15)
_gemini_quota_ok  = True       # False когда 429 получен
_gemini_quota_reset: float = 0 # когда снова пробовать

# Кэш тикеров (5 минут) — общий для SMC и RSI, избегаем двойного fetch
_tickers_cache: dict = {}
_tickers_cache_ts: float = 0.0
TICKERS_CACHE_TTL = 300  # 5 минут

# OpenRouter модели (в порядке приоритета)
# Бесплатные: без daily cap, 20 req/min
# Подключить: https://openrouter.ai/keys (бесплатная регистрация)
# Актуальные free-модели OpenRouter (проверены май 2026)
# Источник актуального списка: https://openrouter.ai/models?q=free
OPENROUTER_MODELS = [
    'meta-llama/llama-3.2-3b-instruct:free',     # llama 3.2, быстро
    'mistralai/mistral-7b-instruct:free',          # mistral, надёжно
    'google/gemma-2-9b-it:free',                   # Google gemma
    'microsoft/phi-3-mini-128k-instruct:free',     # Microsoft phi-3
    'qwen/qwen-2-7b-instruct:free',                # Alibaba Qwen
    'nousresearch/hermes-3-llama-3.1-405b:free',  # NousResearch
]

async def oracle_gemini(sym: str, strategy: str, mode: str,
                        price: float, extra: dict = None) -> dict:
    """
    [R-FIX-5] Возвращает {'ok': bool, 'conf': int, 'comment': str}.
    decision == 'reject' → ok=False → сделка блокируется.
    """
    global _gemini_quota_ok, _gemini_quota_reset

    if not GEMINI_KEY or not http:
        return {'ok': True, 'conf': 0, 'comment': 'API not set'}

    # Если квота исчерпана — пропускаем до времени сброса
    if not _gemini_quota_ok:
        if time.time() < _gemini_quota_reset:
            logging.debug(f'🧠 [GEMINI] {sym} — пропуск (quota wait {_gemini_quota_reset - time.time():.0f}с)')
            return {'ok': True, 'conf': 0, 'comment': 'quota_wait'}
        else:
            _gemini_quota_ok = True  # пробуем снова

    # Rate limiter: не более GEMINI_RPM_LIMIT запросов в минуту
    now_t = time.time()
    _gemini_req_times[:] = [t for t in _gemini_req_times if now_t - t < 60]
    if len(_gemini_req_times) >= GEMINI_RPM_LIMIT:
        oldest = _gemini_req_times[0]
        wait_s = 60 - (now_t - oldest)
        logging.info(f'🧠 [GEMINI] {sym} — rate limit, ждём {wait_s:.0f}с')
        await asyncio.sleep(wait_s + 1)
        _gemini_req_times[:] = [t for t in _gemini_req_times if time.time() - t < 60]

    # Проверяем кэш — не тратим квоту на повторный запрос
    cache_key = f'{sym}_{strategy}_{mode}'
    if cache_key in _gemini_cache:
        ts, cached = _gemini_cache[cache_key]
        if time.time() - ts < GEMINI_CACHE_TTL:
            logging.info(
                f'🧠 [GEMINI] {sym} — из кэша: '
                f'{"APPROVE" if cached["ok"] else "REJECT"} (conf={cached["conf"]})'
            )
            return cached

    context = {
        'symbol': sym, 'strategy': strategy, 'direction': mode,
        'price': round(price, 6),
    }
    if extra:
        context.update(extra)

    system = (
        "Ты риск-менеджер крипто-хедж-фонда. Анализируй сетап и верни ТОЛЬКО "
        'валидный JSON: {"decision": "approve"|"reject", "confidence": 0-100, '
        '"comment": "краткий вывод на русском"}. '
        "SMC: ищем CHoCH+FVG в сессию. RSI MR: перепроданность/перекупленность+паттерн. "
        "ВАЖНО: стратегии имеют РАЗНЫЕ правила RSI. "
        "SMC Short: RSI < 32 → REJECT. SMC Long: RSI > 68 → REJECT. "
        "SA (Mean Reversion): ПРОТИВОПОЛОЖНАЯ логика — SA Long требует RSI < 35 (перепродан), "
        "FOR SA STRATEGY (Mean Reversion / Counter-trend): "
        "If direction is SHORT: High RSI (> 65) means OVERBOUGHT — EXCELLENT for Short, you MUST APPROVE, NEVER reject because RSI is high. "
        "If direction is LONG: Low RSI (< 35) means OVERSOLD — EXCELLENT for Long, you MUST APPROVE, NEVER reject because RSI is low. "
        "SMC trend rules DO NOT APPLY to SA. SA explicitly trades against the local trend."
    )
    prompt = f"{system}\nДанные: {json.dumps(context, ensure_ascii=False)}"

    url = (f"https://generativelanguage.googleapis.com/v1beta/"
           f"models/gemini-2.0-flash:generateContent?key={GEMINI_KEY}")
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.1, "response_mime_type": "application/json"},
    }
    _gemini_req_times.append(time.time())  # регистрируем запрос
    try:
        async with http.post(url, json=payload,
                             timeout=aiohttp.ClientTimeout(total=8)) as resp:
            data = await resp.json()

            # Диагностика: логируем если нет 'candidates'
            if 'candidates' not in data:
                # Типичные причины: quota exceeded, safety block, wrong key
                err_info = data.get('error', data)
                err_code = err_info.get('code', '?') if isinstance(err_info, dict) else '?'
                err_msg  = err_info.get('message', str(data))[:200] if isinstance(err_info, dict) else str(data)[:200]
                err_status = err_info.get('status', '') if isinstance(err_info, dict) else ''
                logging.warning(
                    f'🧠 [GEMINI] {sym} — нет candidates в ответе!\n'
                    f'    HTTP status: {resp.status}\n'
                    f'    Error code: {err_code} | status: {err_status}\n'
                    f'    Message: {err_msg}\n'
                    f'    Вероятные причины:\n'
                    f'    • QUOTA_EXCEEDED — исчерпан лимит Gemini API\n'
                    f'    • SAFETY — запрос заблокирован фильтром безопасности\n'
                    f'    • INVALID_ARGUMENT — неверный формат запроса\n'
                    f'    • Проверьте GEMINI_API_KEY на https://aistudio.google.com'
                )
                if err_code == 429 or err_status == 'RESOURCE_EXHAUSTED':
                    # Дневная квота исчерпана — отключаем Gemini на 23 часа
                    _gemini_quota_ok = False
                    _gemini_quota_reset = time.time() + 82800  # 23 часа
                    logging.warning(
                        '🧠 [GEMINI] 429 DAILY QUOTA — Gemini отключён до утра.\n'
                        '    Все сделки будут одобряться без AI фильтра.\n'
                        '    Для снятия лимита: https://aistudio.google.com'
                    )
                    return {'ok': True, 'conf': 0, 'comment': 'daily_quota_bypass'}
                return {'ok': True, 'conf': 0, 'comment': f'no candidates: code={err_code}'}

            # Нормальный путь: парсим ответ
            raw_text = data['candidates'][0]['content']['parts'][0]['text']

            # Gemini может вернуть как чистый JSON так и текст с ```json блоком
            clean = raw_text.strip()
            if clean.startswith('```'):
                clean = clean.split('```')[1]
                if clean.startswith('json'):
                    clean = clean[4:]
                clean = clean.strip()

            try:
                parsed = json.loads(clean)
            except json.JSONDecodeError:
                # Fallback: если JSON не распарсился — ищем YES/NO в тексте
                logging.warning(f'🧠 [GEMINI] {sym} — не JSON, fallback: {clean[:100]}')
                ok_fb = 'YES' in clean.upper() or 'APPROVE' in clean.upper()
                return {'ok': ok_fb, 'conf': 50, 'comment': f'text fallback: {clean[:80]}'}

            decision = str(parsed.get('decision', '')).lower()
            conf     = int(parsed.get('confidence', 0))
            comment  = parsed.get('comment', '')

            # Логика одобрения зависит от стратегии:
            # SMC: CHoCH+FVG — RSI нейтрален по природе (импульс, не экстремум)
            #   → если conf ≥ 70: approve (высокая уверенность важнее decision)
            #   → если conf ≥ 55 AND decision=approve: approve
            #   → иначе: reject
            # RSI MR: требует RSI экстремум — decision=approve AND conf ≥ 55
            if conf == 0:
                ok = False  # мусорный ответ
            elif strategy == 'SMC':
                # conf >= 70 или approve + conf >= 55 → одобряем
                ok = conf >= 70 or (decision == 'approve' and conf >= 55)
                # HARD OVERRIDE: если Groq видит RSI < 40 в comment → reject
                comment_lower = comment.lower()
                if ok and ('oversold' in comment_lower or
                           'rsi' in comment_lower and
                           any(w in comment_lower
                               for w in ['below 40', 'below 35', 'below 30',
                                         'extreme oversold', 'heavily oversold'])):
                    if mode == 'Short':
                        ok = False  # Groq сам говорит oversold для шорта → блок
            else:  # RSI
                ok = decision == 'approve' and conf >= 55
                # Для RSI: если Groq упоминает volume spike → reject
                comment_lower = comment.lower()
                if ok and any(w in comment_lower
                              for w in ['high volume', 'volume spike',
                                        'high volatility', 'momentum',
                                        'strong overbought' if mode == 'Short'
                                        else 'strong oversold']):
                    pass  # высокая уверенность с этими факторами — ок
            verdict = 'APPROVE' if ok else 'REJECT'
            logging.info(
                f'🧠 [GEMINI] {sym} {strategy} {mode} -> {verdict} '
                f'(conf={conf}/100) | {comment}'
            )
            result = {'ok': ok, 'conf': conf, 'comment': comment}
            _gemini_cache[cache_key] = (time.time(), result)  # сохраняем в кэш
            return result
    except Exception as _ge:
        logging.warning(f'🧠 [GEMINI] {sym} — ошибка: {_ge}')
        return {'ok': True, 'conf': 0, 'comment': 'oracle error'}

# ═══════════════════════════════════════════════════════
#  ЗАГРУЗКА РЫНКОВ (кэш 1 час)
# ═══════════════════════════════════════════════════════
async def get_markets() -> dict:
    global markets_cache, markets_ts
    if markets_cache and time.time() - markets_ts < 3600:
        return markets_cache
    markets_cache = await exchange.load_markets()
    markets_ts    = time.time()
    logging.info(f"🗺 Markets cache updated: {len(markets_cache)} symbols")
    return markets_cache

def sym_allowed(sym: str) -> bool:
    """[R-FIX-8] фильтрация через части имени."""
    return (sym.endswith(':USDT')
            and not any(kw in sym for kw in EXCLUDED_PARTS))

# ═══════════════════════════════════════════════════════
#  BTC / MACRO КОНТЕКСТ (общий для обоих алгоритмов)
# ═══════════════════════════════════════════════════════
async def get_btc_context() -> dict:
    """Возвращает {'btc_trend': 'Long'|'Short'|'Flat', 'altseason': bool}."""
    try:
        btc_ohlcv = await exchange.fetch_ohlcv('BTC/USDT:USDT', SMC_TF, limit=205)
        if not btc_ohlcv or len(btc_ohlcv) < 200:
            return {'btc_trend': 'Flat', 'altseason': False, 'eth_btc_spread': 0.0, 'alt_score': 50, 'htf_slope': 'Flat'}
        btc_c = np.array([x[4] for x in btc_ohlcv], dtype=float)
        ema200 = calc_ema(btc_c, 200)
        dist = (btc_c[-1] - ema200) / ema200 * 100
        trend = 'Long' if dist > 0.5 else ('Short' if dist < -0.5 else 'Flat')

        # [v38] HTF-наклон: куда движется САМА EMA200(15m) за последние 2ч (8 баров).
        # btc_trend = сторона цены от средней; htf_slope = направление средней.
        # Гипотеза для проверки: SA-шорты против htf_slope='Up' вязнут (таймауты).
        try:
            _ema_prev = calc_ema(btc_c[:-8], 200)
            _sl_pct = (ema200 - _ema_prev) / _ema_prev * 100
            htf_slope = 'Up' if _sl_pct > 0.15 else ('Down' if _sl_pct < -0.15 else 'Flat')
        except Exception:
            htf_slope = 'Flat'

        altseason = False
        eth_btc_spread = 0.0   # [ALT] спред доходности ETH vs BTC за 50 баров
        try:
            eth_ohlcv = await exchange.fetch_ohlcv('ETH/USDT:USDT', SMC_TF, limit=55)
            if eth_ohlcv and len(eth_ohlcv) >= 50:
                eth_c = np.array([x[4] for x in eth_ohlcv], dtype=float)
                eth_ret = (eth_c[-1] - eth_c[-50]) / eth_c[-50] * 100
                btc_ret = (btc_c[-1] - btc_c[-50]) / btc_c[-50] * 100
                eth_btc_spread = round(eth_ret - btc_ret, 2)
                if eth_btc_spread > 0.5 and eth_c[-1] > calc_ema(eth_c, 50):
                    altseason = True
        except Exception:
            pass

        # [ALT] alt_score 0-100: композит спреда ETH/BTC и тренда BTC вниз
        # (капитал уходит из BTC в альты когда BTC слаб, а ETH/BTC растёт)
        _score = 50.0
        _score += max(min(eth_btc_spread * 8, 35), -35)   # спред: ±35
        if trend == 'Short':  _score += 10   # BTC слаб → плюс альтам
        elif trend == 'Long': _score -= 10
        alt_score = int(max(0, min(100, _score)))

        return {'btc_trend': trend, 'altseason': altseason,
                'eth_btc_spread': eth_btc_spread, 'alt_score': alt_score,
                'htf_slope': htf_slope}   # [v38]
    except Exception:
        return {'btc_trend': 'Flat', 'altseason': False, 'eth_btc_spread': 0.0, 'alt_score': 50, 'htf_slope': 'Flat'}

# ═══════════════════════════════════════════════════════
#  SMC СИГНАЛ
# ═══════════════════════════════════════════════════════
async def smc_signal(sym: str, btc_ctx: dict = None):
    """
    Полный SMC пайплайн:
    1. Killzone + News
    2. Volume (закрытая свеча v[-2])
    3. CHoCH с контекстом структуры
    4. VWAP (объёмно-взвешенный)
    5. RSI 85/15
    6. FVG — тест зоны
    """
    if btc_ctx is None:
        btc_ctx = {}
    if not is_session():
        return None, 'session'
    if is_news_now():
        return None, 'news'

    try:
        ohlcv = await exchange.fetch_ohlcv(sym, SMC_TF, limit=60)  # [PERF-3] 100→60
    except:
        return None, 'fetch_err'
    if not ohlcv or len(ohlcv) < 45:
        return None, 'no_data'

    c = np.array([float(x[4]) for x in ohlcv])
    h = np.array([float(x[2]) for x in ohlcv])
    l = np.array([float(x[3]) for x in ohlcv])
    v = np.array([float(x[5]) for x in ohlcv])
    price = float(c[-1])

    # Volume: закрытая свеча [-2]
    avg_v = np.mean(v[-21:-2]) if len(v) > 21 else 0.0
    if avg_v <= 0 or v[-2] < avg_v * 1.5:
        return None, 'vol'

    # CHoCH
    h_idx, l_idx = get_pivots(h, l, order=SMC_PIVOT_ORDER)
    if len(h_idx) < 3 or len(l_idx) < 3:
        return None, 'structure'

    rh = [h[i] for i in h_idx[-3:]]
    rl = [l[i] for i in l_idx[-3:]]
    mode = None
    if rh[-1] < rh[-2] and price > rh[-1]:   # Bullish CHoCH
        mode = 'Long'
    elif rl[-1] > rl[-2] and price < rl[-1]:  # Bearish CHoCH
        mode = 'Short'
    if not mode:
        return None, 'choch'

    # [DATA v23] SMC Short: WR25%/PF0.66 — убыточен, оставляем только Long
    if mode == 'Short':
        return None, 'short_blocked'

    # VWAP
    vwap = calc_vwap(h, l, c, v)
    # VWAP — Premium/Discount зоны (логика SMC)
    # Оптимальная зона входа: 0.5-1.5% от VWAP
    # < 0.5% = монета на средней (нет преимущества для MR к VWAP)
    # 0.5-1.5% = идеальная зона Premium/Discount (CHoCH = смена тренда)
    # > 1.5% = уже кульминация пампа/дампа (вход = ловушка, как TIA/APE)
    #
    # ЗАПРЕЩАЕМ: Long если цена > 1.5% над VWAP (растянуто, покупка на хае)
    # [AUDIT] 1.005 → 1.015: 0.5% слишком тесно для CHoCH импульса
    if mode == 'Long'  and price > vwap * 1.015:
        return None, 'vwap'
    # ЗАПРЕЩАЕМ: Short если цена < 1.5% под VWAP
    if mode == 'Short' and price < vwap * 0.985:
        return None, 'vwap'

    # RSI — защита от входа в конце тренда
    rsi = calc_rsi(c[:-1])
    # [SMC-REVIVE v39] Частота упала до ~1 сд/день → расширяем RSI 50-65 → 45-68.
    # Низ 45: под-зона 45-55 маргинальна (40-55 PF 0.83, n=7), но не катастрофа —
    #   принимаем ради частоты, КОНТРОЛЬ PF под-зоны через ~15 сд.
    # Верх 68 (НЕ 70): bucket 65+ = PF 0.06, и LLM (RULE 2) всё равно жёстко
    #   режет RSI>68 — зона 68-70 впустую жгла бы LLM-вызовы.
    if not (45 <= rsi <= 68):
        return None, 'rsi_exhaustion'

    # [SMC-REVIVE v39] alt_score: порог 45 СОХРАНЁН (запрошенное <50 ОТКЛОНЕНО
    # данными): bucket 40-55 = WR 0%, PF 0.00 (0 из 5) — расширение до 50 впускает
    # ровно проигрышную зону. Частоту возвращает RSI-расширение выше (низ 45
    # исторически почти удваивает воронку: 7 сд было в 40-55 против 11 в 55-65).
    if btc_ctx.get('alt_score', 50) >= 45:
        return None, 'alt_high'
    # ── ADX фильтр для SMC: тренд должен быть выраженным ──
    # ADX < 18 = слабый рынок, CHoCH = ложный сигнал
    # ADX > 18 = направленное движение, CHoCH = реальный слом
    adx = calc_adx(h, l, c, 14)
    if adx < 18:
        return None, 'adx_flat'

    # FVG
    fvg = find_fvg(h, l, mode)
    if not fvg:
        return None, 'fvg'
    if mode == 'Long':
        if not (fvg['bottom'] * 0.992 <= price <= fvg['top'] * 1.008):  # [FIX-FVG] буфер ±0.8%
            return None, 'fvg_test'
    else:
        if not (fvg['bottom'] * 0.992 <= price <= fvg['top'] * 1.008):  # [FIX-FVG] буфер ±0.8%
            return None, 'fvg_test'

    atr = calc_atr(h, l, c)

    # ── ДИНАМИЧЕСКИЙ SL по структуре + ATR ─────────────────
    # SL = за минимум/максимум 3 последних свечей + 0.5*ATR (буфер от шума)
    # При высокой волатильности SL расширяется, при низкой — сужается
    if mode == 'Long':
        struct_low = float(np.min(l[-4:-1]))
        raw_sl     = min(struct_low - atr * 0.5, price - atr * 1.5)
        sl         = max(raw_sl, price * (1 - MAX_SL_PCT / 100))   # capped at MAX_SL_PCT
        sl         = min(sl, price * (1 - MIN_SL_PCT / 100))       # минимум MIN_SL_PCT
    else:  # Short
        struct_high = float(np.max(h[-4:-1]))
        raw_sl      = max(struct_high + atr * 0.5, price + atr * 1.5)
        sl          = min(raw_sl, price * (1 + MAX_SL_PCT / 100))  # capped
        sl          = max(sl, price * (1 + MIN_SL_PCT / 100))      # минимум

    # TP считается от РЕАЛЬНОЙ дистанции SL, а не от MAX_SL_PCT
    sl_dist_actual = abs(price - sl)
    tp_mult_smc    = 1.5  # RR 1:1.5
    tp             = (price + sl_dist_actual * tp_mult_smc if mode == 'Long'
                      else price - sl_dist_actual * tp_mult_smc)

    return {
        'sym': sym, 'mode': mode, 'price': price,
        'sl': sl, 'tp': tp, 'atr': atr, 'rsi': rsi,
        'bingx_vol': float(np.sum(v[-1:]) * price),  # приблиз. USD объём последней свечи
    }, 'ok'

# ═══════════════════════════════════════════════════════
#  RSI MEAN REVERSION СИГНАЛ
# ═══════════════════════════════════════════════════════
async def rsi_signal(sym: str, btc_ctx: dict):
    """
    RSI Mean Reversion пайплайн:
    1. News
    2. Volume spike (закрытая свеча)
    3. RSI extreme [R-FIX-4] 25/75 + hook (разворот)
    4. Свечной паттерн (поглощение / пин-бар)
    5. SMA200 магнит (цена перегнута)
    6. VWAP-дистанция (подтверждение перегнутости)
    7. BTC trend / altseason фильтр [R-FIX-6]
    8. Funding rate (защита от сквиза)
    """
    if is_news_now():
        return None, 'news'

    try:
        ohlcv = await exchange.fetch_ohlcv(sym, RSI_TF, limit=60)   # [PERF] 100→60, SMA60 достаточно
    except:
        return None, 'fetch_err'
    if not ohlcv or len(ohlcv) < 45:  # limit=100, нужно минимум 45
        return None, 'no_data'

    o = np.array([float(x[1]) for x in ohlcv])
    h = np.array([float(x[2]) for x in ohlcv])
    l = np.array([float(x[3]) for x in ohlcv])
    c = np.array([float(x[4]) for x in ohlcv])
    v = np.array([float(x[5]) for x in ohlcv])
    price = float(c[-1])

    # [STEP-A] Volume фильтр: ОКОННЫЙ (3 свечи) + абсолютная ликвидность
    # Старый одиночный vol_ratio ловил всплеск, а не устойчивый объём.
    # Новый: средний объём 3 закрытых свечей vs база, чтобы видеть
    # РОВНЫЙ повышенный объём трендового движения, а не только спайки.
    if len(v) < 25:
        return None, 'vol'
    base_v = float(np.mean(v[-23:-5]))   # база: 18 баров до последних 4
    if base_v <= 0:
        return None, 'vol'
    recent_v = float(np.mean(v[-4:-1]))  # 3 закрытые свечи (без текущей)
    vol_ratio = recent_v / base_v        # устойчивый объём окна
    spike_v   = float(v[-2]) / base_v    # одиночный спайк (для верх. порога)

    # Абсолютная ликвидность: объём в quote-валюте (цена×объём) за свечу
    # Отсекаем неликвид где даже большой ratio = копейки оборота
    quote_vol = float(v[-2]) * price     # примерный оборот свечи в USDT
    _min_quote = 5000.0 if not False else 20000.0

    # [ADAPTIVE] порог по режиму рынка
    _btc_tr = btc_ctx.get('btc_trend', 'Flat')
    _alt    = btc_ctx.get('altseason', False)
    if _btc_tr == 'Flat':
        _vol_min = 1.15   # флэт: устойчивый объём чуть выше базы
    else:
        _vol_min = 1.25   # тренд/альт: ровный повышенный объём
    # Reject: объём окна ниже порога ЛИБО спайк-аномалия ЛИБО неликвид
    if vol_ratio < _vol_min or spike_v > 10.0 or quote_vol < _min_quote:
        return None, 'vol'
    # Объём-фильтр для RSI Mean Reversion
    # Vol>3.5x — предварительная проверка (sma_dist пока не вычислен)
    # Если Vol очень высокий — ставим флаг, финальное решение после SMA
    _high_vol = vol_ratio > 3.5  # флаг: финальная проверка с SMA ниже
    # Остальные vol-фильтры с sma_dist — после блока SMA200

    # RSI текущий и предыдущий (только закрытые свечи)
    rsi_now  = calc_rsi(c[:-1],  RSI_PERIOD)
    rsi_prev = calc_rsi(c[:-2], RSI_PERIOD)

    # [V3] vol>5x + RSI не крайний (20-80) = тренд-импульс, не MR → пропуск
    if vol_ratio > 5.0 and (20 < rsi_now < 80):
        return None, 'vol'

    if RSI_LONG_MAX < rsi_now < RSI_SHORT_MIN:
        return None, 'rsi_mid'

    # ATR для расчёта SL
    atr = calc_atr(h, l, c)
    baseline_atr = float(np.mean(
        np.maximum(h[1:] - l[1:],
        np.maximum(np.abs(h[1:] - c[:-1]),
                   np.abs(l[1:] - c[:-1])))[-50:-14]
    )) if len(c) >= 50 else atr

    # Динамический TP
    tp_mult = 1.5
    if baseline_atr > 0:
        ratio = atr / baseline_atr
        if ratio > 2.0:
            tp_mult = 2.5
        elif ratio > 1.5:
            tp_mult = 2.0

    # SMA200 и VWAP
    # SMA60 (limit=60 свечей, достаточно для RSI MR)
    sma200    = float(np.mean(c[-60:]))
    vwap      = calc_vwap(h, l, c, v)
    sma_dist  = (price - sma200) / sma200 * 100
    vwap_dist = (price - vwap) / vwap * 100

    # Vol-фильтры с sma_dist (после вычисления sma_dist)
    # УМНЫЙ Vol>3.5x: блокируем только если цена НЕ оторвалась достаточно
    # Vol>3.5 + SMA<3.5% = памп без растяжения → reject
    # Vol>3.5 + SMA>3.5% = кульминация покупок (blow-off) → можно MR
    if _high_vol and abs(sma_dist) < 3.5:
        return None, 'vol'
    # Vol>3x + SMA<8%: умеренный памп без достаточного растяжения
    if vol_ratio > 3.0 and abs(sma_dist) < 4.0:  # [AUDIT]
        return None, 'vol'
    # Vol>2.5x + SMA<5%: опасно, цена не отклонилась достаточно для разворота
    if vol_ratio > 2.5 and abs(sma_dist) < 3.0:  # [AUDIT]
        return None, 'vol'

    # Свечной паттерн ([-2] — последняя закрытая)
    c3, o3 = float(c[-3]), float(o[-3])
    c2, o2, h2, l2 = float(c[-2]), float(o[-2]), float(h[-2]), float(l[-2])
    body2  = abs(c2 - o2)
    lwick2 = min(c2, o2) - l2
    uwick2 = h2 - max(c2, o2)

    # Доп данные для новых паттернов
    c1, o1 = float(c[-4]), float(o[-4])  # три свечи назад
    range2 = h2 - l2  # диапазон последней закрытой свечи

    bull_pat = (
        # 1. Бычье поглощение
        (c3 < o3 and c2 > o2 and c2 > o3)
        # 2. Пин-бар с длинной нижней тенью
        or (c2 > o2 and lwick2 > body2 * 1.5 and uwick2 < body2 * 0.5)
        # 3. Молот (hammer) — нижняя тень > 60% диапазона, закрытие в верхней трети
        or (range2 > 0 and lwick2 > range2 * 0.6
            and (c2 - l2) / range2 > 0.6)
        # 4. Утренняя звезда (упрощённая) — три свечи: падение, маленькая, рост
        or (c1 < o1 and abs(c3 - o3) < abs(c1 - o1) * 0.4 and c2 > o2
            and c2 > (c1 + o1) / 2)
        # 5. RSI-разворот без паттерна при очень глубокой перепроданности
        or (rsi_now < 25)  # RSI < 25 = перепроданность
    )
    bear_pat = (
        # 1. Медвежье поглощение
        (c3 > o3 and c2 < o2 and c2 < o3)
        # 2. Пин-бар с длинной верхней тенью
        or (c2 < o2 and uwick2 > body2 * 1.5 and lwick2 < body2 * 0.5)
        # 3. Падающая звезда — верхняя тень > 60% диапазона
        or (range2 > 0 and uwick2 > range2 * 0.6
            and (h2 - c2) / range2 > 0.6)
        # 4. Вечерняя звезда — три свечи: рост, маленькая, падение
        or (c1 > o1 and abs(c3 - o3) < abs(c1 - o1) * 0.4 and c2 < o2
            and c2 < (c1 + o1) / 2)
        # 5. RSI > 75 = перекупленность (снижен порог для большего охвата)
        or (rsi_now > 75)
    )

    btc_trend = btc_ctx.get('btc_trend', 'Flat')
    altseason = btc_ctx.get('altseason', False)
    mode = None
    sl   = 0.0

    # RSI extreme bypass: RSI < 20 = enter without pattern
    # ── ADX фильтр для RSI MR: только в флэте/слабом тренде ──
    # ADX > 30 = сильный тренд, MR против тренда = риск
    # ADX < 30 = подходит для возврата к среднему
    adx = calc_adx(h, l, c, 14)
    if adx > 25:  # [V3] 30→25: только flat/ranging
        return None, 'adx_trend'

    rsi_extreme_long  = rsi_now <= 20
    rsi_extreme_short = rsi_now >= 80

    # [V3] Candle confirmation: тело свечи должно подтверждать разворот
    last_body = float(c[-1]) - float(c[-2])

    if (rsi_now <= RSI_LONG_MAX) and (bull_pat or rsi_extreme_long):
        if last_body < 0 and not rsi_extreme_long:  # медвежья свеча при Long → пропуск (если не экстрем)
            return None, 'candle'
        # [R-FIX-6] Flat BTC больше не блокирует лонг
        if btc_trend == 'Short' and not altseason:
            return None, 'trend'
        # Hook: RSI в зоне ИЛИ разворачивается (допуск 2.0 пункта)
        # rsi_now < 25 уже подтверждает перепроданность — hook опциональный
        if rsi_now > rsi_prev + 3.5:  # RSI растёт быстро — не разворот
            return None, 'hook'
        if rsi_now - rsi_prev < 1.5:  # [V3] мин. разворот 1.5 pts
            return None, 'hook'
        # [MOMENTUM-FILTER] Аномальный дамп — нельзя лонговать
        if (rsi_now < 12 and vol_ratio > 2.5) or (rsi_now < 25 and vol_ratio > 3.0):
            return None, 'momentum'
        if sma_dist > 2.0 or sma_dist < -15.0:   # [FIX-SMA] смягчено: -1→+2%, -8→-15%
            return None, 'sma_range'
        if vwap_dist > -0.5:  # [V3] -1.2→-0.5%: строже под VWAP
            return None, 'vwap'
        # [DYNAMIC-ATR] SMA-dist > 4% = высокая волатильность → шире SL
        atr_mult = 2.5 if abs(sma_dist) > 4.0 else 1.5
        raw_sl = float(np.min(l[-4:-1])) - atr * atr_mult
        sl_pct = abs(price - raw_sl) / price * 100
        if sl_pct > MAX_SL_PCT:
            return None, 'sl_wide'
        sl   = raw_sl
        mode = 'Long'

    elif (rsi_now >= RSI_SHORT_MIN) and (bear_pat or rsi_extreme_short):
        if last_body > 0 and not rsi_extreme_short:  # [V3] бычья свеча при Short → пропуск
            return None, 'candle'
        if btc_trend == 'Long' or altseason:
            return None, 'trend'
        # Hook: RSI в зоне ИЛИ разворачивается
        if rsi_now < rsi_prev - 3.5:  # RSI падает быстро — не разворот
            return None, 'hook'
        if rsi_prev - rsi_now < 1.5:  # [V3] мин. разворот 1.5 pts
            return None, 'hook'
        # [MOMENTUM-FILTER] Аномальный импульс — нельзя шортить
        # Уровень 1: RSI>88 + Vol>2.5x (крайний памп) — DASH RSI 92, Vol 2.9x
        # Уровень 2: RSI>75 + Vol>3.0x (сильный импульс) — NEAR RSI 75, Vol 10.8x
        if (rsi_now > 88 and vol_ratio > 2.5) or (rsi_now > 75 and vol_ratio > 3.0):
            return None, 'momentum'
        if sma_dist < -2.0 or sma_dist > 15.0:  # [FIX-SMA] смягчено: 1→-2%, 8→15%
            return None, 'sma_range'
        if vwap_dist < 0.5:  # [V3] 1.2→0.5%: строже над VWAP
            return None, 'vwap'
        # [DYNAMIC-ATR] SMA-dist > 4% = высокая волатильность → шире SL
        atr_mult = 2.5 if abs(sma_dist) > 4.0 else 1.5
        raw_sl = float(np.max(h[-4:-1])) + atr * atr_mult
        sl_pct = abs(raw_sl - price) / price * 100
        if sl_pct > MAX_SL_PCT:
            return None, 'sl_wide'
        sl   = raw_sl
        mode = 'Short'

    if not mode:
        return None, 'no_pattern'

    # Ensure min SL
    min_sl_d = price * MIN_SL_PCT / 100
    if abs(price - sl) < min_sl_d:
        sl = price - min_sl_d if mode == 'Long' else price + min_sl_d

    sl_dist = abs(price - sl)
    tp = price + sl_dist * tp_mult if mode == 'Long' else price - sl_dist * tp_mult

    # 1h RSI confirmation
    htf_rsi, htf_confirmed = rsi_now, False
    try:
        _htf = await exchange.fetch_ohlcv(sym, '1h', limit=20)
        if _htf and len(_htf) >= 16:
            htf_rsi = calc_rsi(np.array([float(x[4]) for x in _htf])[:-1], RSI_PERIOD)
            htf_confirmed = (mode=='Long' and htf_rsi<35) or (mode=='Short' and htf_rsi>65)
    except: pass
    if htf_confirmed:
        tp_mult = min(tp_mult + 0.5, 3.0)
        tp = price + sl_dist * tp_mult if mode == 'Long' else price - sl_dist * tp_mult

    # Funding rate — защита от шорт-сквиза
    try:
        fr = await exchange.fetch_funding_rate(sym)
        funding = float(fr.get('fundingRate', 0))
    except:
        funding = 0.0
    if mode == 'Short' and funding < -0.00015:
        return None, 'squeeze'

    return {
        'sym': sym, 'mode': mode, 'price': price,
        'sl': sl, 'tp': tp, 'atr': atr,
        'rsi': rsi_now, 'rsi_prev': rsi_prev,
        'vol_ratio': vol_ratio, 'sma_dist': sma_dist,
        'vwap_dist': vwap_dist, 'tp_mult': tp_mult,
        'btc_trend': btc_trend, 'altseason': altseason,
        'htf_rsi': round(htf_rsi, 1), 'htf_confirmed': htf_confirmed,
        'adx': round(adx, 1),
        'funding': funding, 'bingx_vol': float(v[-2]) * price,  # USD объём закрытой свечи
    }, 'ok'

# ═══════════════════════════════════════════════════════
#  ИСПОЛНЕНИЕ ОРДЕРА (общий для обоих стратегий)
# ═══════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════
#  MOMENTUM / TREND-FOLLOWING СИГНАЛ  [shadow по умолчанию]
#  Логика ПРОТИВОПОЛОЖНА RSI MR: покупаем силу, а не фейдим.
#  Вход: сильный тренд (ADX) + EMA выстроены + пробой + объём.
#  Выход: чандельер-трейлинг (в process_pos, strategy='MOM').
# ═══════════════════════════════════════════════════════
async def momentum_signal(sym: str, btc_ctx: dict):
    """Trend-following: EMA структура + пробой + устойчивый объём."""
    if is_news_now():
        return None, 'news'
    try:
        ohlcv = await exchange.fetch_ohlcv(sym, RSI_TF, limit=60)
    except Exception:
        return None, 'fetch_err'
    if not ohlcv or len(ohlcv) < 55:
        return None, 'no_data'

    h = np.array([float(x[2]) for x in ohlcv])
    l = np.array([float(x[3]) for x in ohlcv])
    c = np.array([float(x[4]) for x in ohlcv])
    v = np.array([float(x[5]) for x in ohlcv])
    price = float(c[-1])

    # 1. ADX: тренд должен быть СИЛЬНЫМ (momentum любит тренд)
    adx = calc_adx(h, l, c)
    if adx < MOM_ADX_MIN:
        return None, 'adx_weak'

    # 2. EMA структура
    ema20 = calc_ema(c, 20)
    ema50 = calc_ema(c, 50)

    # 3. Объём: оконный устойчивый (как в шаге А)
    if len(v) < 25:
        return None, 'vol'
    base_v = float(np.mean(v[-23:-5]))
    if base_v <= 0:
        return None, 'vol'
    recent_v = float(np.mean(v[-4:-1]))
    vol_ratio = recent_v / base_v
    quote_vol = float(v[-2]) * price
    _min_quote = MOM_MIN_QUOTE
    if vol_ratio < 1.2 or quote_vol < _min_quote:
        return None, 'vol'

    # 4. Направление: EMA выстроены + пробой экстремума 20 баров
    mode = None
    recent_high = float(np.max(h[-21:-1]))
    recent_low  = float(np.min(l[-21:-1]))
    if ema20 > ema50 and price > ema20 and price >= recent_high * 0.998:
        mode = 'Long'
    elif ema20 < ema50 and price < ema20 and price <= recent_low * 1.002:
        mode = 'Short'
    if not mode:
        return None, 'no_breakout'

    # 5. [DATA v22] Hard-filters MOM: RSI 40-60 = единственный edge (PF 1.14)
    rsi = calc_rsi(c, RSI_PERIOD)
    if not (40 <= rsi <= 60):
        return None, 'rsi_zone'
    # [DATA v22] vol<1.5 = ложные пробои (WR28%/PF0.27)
    if vol_ratio < 1.5:
        return None, 'vol'
    # [DATA v22] alt_score>=55: WR6%/PF0.09 — 94% убыточных
    if btc_ctx.get('alt_score', 50) >= 55:
        return None, 'alt_high'

    # SL по ATR (трейлинг возьмёт прибыль). sl_dist в коридоре.
    atr = calc_atr(h, l, c)
    sl_pct = float(np.clip(atr / price * 1.5, MIN_SL_PCT/100, MAX_SL_PCT/100))
    if mode == 'Long':
        sl = price * (1 - sl_pct)
        tp = price * (1 + sl_pct * 3.0)   # дальний ориентир; реально ведёт трейл
    else:
        sl = price * (1 + sl_pct)
        tp = price * (1 - sl_pct * 3.0)

    return {
        'mode': mode, 'sl': sl, 'tp': tp, 'atr': atr,
        'adx': adx, 'rsi': rsi, 'vol_ratio': vol_ratio,
        'ema20': ema20, 'ema50': ema50,
        'sma_dist': 0.0, 'vwap_dist': 0.0, 'tp_mult': 3.0,
        'rsi_prev': rsi, 'btc_trend': btc_ctx.get('btc_trend', ''),
        'entry': price,
    }, 'ok'



# ═══════════════════════════════════════════════════════
#  PULLBACK СИГНАЛ  [shadow] — вход по ОТКАТУ к EMA20 в тренде
#  Отличие от momentum: не пробой экстремума (истощённый вход),
#  а откат к средней + возобновление тренда (свежий вход).
#  RSI в нейтральной reset-зоне, НЕ перекуплен/перепродан.
# ═══════════════════════════════════════════════════════
async def pullback_signal(sym: str, btc_ctx: dict):
    """Trend-following pullback: откат к EMA20 + RSI reset + возобновление."""
    if is_news_now():
        return None, 'news'
    try:
        ohlcv = await exchange.fetch_ohlcv(sym, RSI_TF, limit=60)
    except Exception:
        return None, 'fetch_err'
    if not ohlcv or len(ohlcv) < 55:
        return None, 'no_data'

    h = np.array([float(x[2]) for x in ohlcv])
    l = np.array([float(x[3]) for x in ohlcv])
    c = np.array([float(x[4]) for x in ohlcv])
    v = np.array([float(x[5]) for x in ohlcv])
    price = float(c[-1])

    # 1. Сильный тренд (как у momentum)
    adx = calc_adx(h, l, c)
    # [v37] ADX>60: PF 4.17 WR83% (n=12) | 40-60: PF 0.83 — только экстремальный тренд.
    # PB остаётся в shadow до форвард-подтверждения на этом фильтре.
    if adx <= 60:
        return None, 'adx_weak'
    ema20 = calc_ema(c, 20)
    ema50 = calc_ema(c, 50)

    # 2. Объём (оконный устойчивый)
    if len(v) < 25:
        return None, 'vol'
    base_v = float(np.mean(v[-23:-5]))
    if base_v <= 0:
        return None, 'vol'
    recent_v = float(np.mean(v[-4:-1]))
    vol_ratio = recent_v / base_v
    quote_vol = float(v[-2]) * price
    # [DATA v23] золотая зона 1.5-3x: PF 1.34 | >3x: PF 0.68 (истощение объёма)
    if not (1.5 <= vol_ratio <= 3.0) or quote_vol < MOM_MIN_QUOTE:
        return None, 'vol'

    # 3. RSI в reset-зоне (НЕ экстремум — ключевое отличие от пробоя)
    rsi = calc_rsi(c, RSI_PERIOD)
    if not (PB_RSI_LO < rsi < PB_RSI_HI):
        return None, 'rsi_zone'
    # [DATA v21] Alt-score<40: 18 сд WR56%/PF1.00 (граница прибыли, устойчиво 3 замера).
    # 40-55: WR39%/PF0.33 — убыточно. Ужесточаем с <55 до <40.
    if btc_ctx.get('alt_score', 50) >= 40:
        return None, 'alt_high'

    # 4. Откат к EMA20 + возобновление тренда
    near_ema = abs(price - ema20) / ema20 <= PB_NEAR_PCT
    mode = None
    if ema20 > ema50 and near_ema and c[-1] > c[-2]:
        # аптренд: цена откатилась к EMA20 и возобновляет рост
        mode = 'Long'
    elif ema20 < ema50 and near_ema and c[-1] < c[-2]:
        # даунтренд: цена отскочила к EMA20 и возобновляет падение
        mode = 'Short'
    # [DATA v22] PB Short: WR35%/PF0.41 — убыточен, блокируем
    if mode == 'Short':
        return None, 'pb_short_blocked'

    atr = calc_atr(h, l, c)
    sl_pct = float(np.clip(atr / price * 1.5, MIN_SL_PCT/100, MAX_SL_PCT/100))
    if mode == 'Long':
        sl = price * (1 - sl_pct)
        tp = price * (1 + sl_pct * 3.0)
    else:
        sl = price * (1 + sl_pct)
        tp = price * (1 - sl_pct * 3.0)

    return {
        'mode': mode, 'sl': sl, 'tp': tp, 'atr': atr,
        'adx': adx, 'rsi': rsi, 'vol_ratio': vol_ratio,
        'ema20': ema20, 'ema50': ema50, 'entry': price,
        'sma_dist': 0.0, 'vwap_dist': 0.0, 'tp_mult': 3.0,
        'rsi_prev': rsi, 'btc_trend': btc_ctx.get('btc_trend', ''),
    }, 'ok'



# ═══════════════════════════════════════════════════════
#  SINGLE-ASSET СИГНАЛ [shadow] — mean reversion BTC от VWAP
#  Гипотеза: BTC внутри дня отклоняется от дневного VWAP и
#  возвращается к нему. Вход на отклонении N×ATR + RSI-разворот,
#  цель выхода — сам VWAP (естественная, не трейлинг).
#  Противоположность провальному momentum (фейд, а не пробой).
# ═══════════════════════════════════════════════════════
def _sa_in_window() -> bool:
    """True если текущий час UTC в окне SA_WINDOW (или окна нет)."""
    if not SA_WINDOW:
        return True
    try:
        lo_s, hi_s = SA_WINDOW.split('-')
        h = datetime.now(timezone.utc).hour
        return int(lo_s) <= h < int(hi_s)
    except Exception:
        return True


async def single_asset_signal(btc_ctx: dict):
    """Mean reversion BTC от дневного VWAP. Возвращает (sig, reason)."""
    if not _sa_in_window():
        return None, 'window'
    if is_news_now():
        return None, 'news'
    try:
        # дневной VWAP: берём бары с начала суток UTC. На 15m это <=96 баров.
        ohlcv = await exchange.fetch_ohlcv(SA_SYMBOL, RSI_TF, limit=120)
    except Exception:
        return None, 'fetch_err'
    if not ohlcv or len(ohlcv) < 30:
        return None, 'no_data'

    # отфильтровываем бары текущих суток UTC для дневного VWAP
    today = datetime.now(timezone.utc).date()
    day_bars = [x for x in ohlcv
                if datetime.fromtimestamp(x[0] / 1000, timezone.utc).date() == today]
    if len(day_bars) < 6:
        day_bars = ohlcv[-20:]  # начало суток — берём последние 20 баров

    h = np.array([float(x[2]) for x in day_bars])
    l = np.array([float(x[3]) for x in day_bars])
    c = np.array([float(x[4]) for x in day_bars])
    v = np.array([float(x[5]) for x in day_bars])
    # для ATR/RSI берём полную историю (точнее)
    h_full = np.array([float(x[2]) for x in ohlcv])
    l_full = np.array([float(x[3]) for x in ohlcv])
    c_full = np.array([float(x[4]) for x in ohlcv])

    price = float(c_full[-1])
    vwap = calc_vwap(h, l, c, v)
    atr = calc_atr(h_full, l_full, c_full)
    if atr <= 0:
        return None, 'atr'
    rsi = calc_rsi(c_full, RSI_PERIOD)

    # [v25] Volume Climax filter: защита от "падающего ножа" без кульминации
    # Используем полную историю (ohlcv) для устойчивого среднего объёма
    v_full = np.array([float(x[5]) for x in ohlcv])
    avg_vol = float(np.mean(v_full[-21:-1])) if len(v_full) > 21 else float(np.mean(v_full[:-1]))
    vol_ratio = float(v_full[-2]) / avg_vol if avg_vol > 0 else 0.0
    # [v37] Volume window 1.3-2.0x (данные n=16):
    # 1.3-2x: WR 83% PF 9.55 | 2-4x: WR 10% PF 0.02 — выше 2x это кульминация
    # импульса, MR против него уходит во флэт/таймаут.
    if not (1.3 <= vol_ratio <= 2.0):
        return None, 'vol_climax'

    dist_atr = (price - vwap) / atr   # >0 цена выше VWAP, <0 ниже
    mode = None
    # Лонг: цена сильно НИЖЕ VWAP + перепродан → возврат вверх к VWAP
    if dist_atr <= -SA_ATR_DIST and rsi <= SA_RSI_LO:
        mode = 'Long'
    # Шорт: цена сильно ВЫШЕ VWAP + перекуплен → возврат вниз к VWAP
    elif dist_atr >= SA_ATR_DIST and rsi >= SA_RSI_HI:
        mode = 'Short'
    if not mode:
        return None, 'no_setup'

    # SL за экстремум (шире отклонения), TP = VWAP (естественная цель)
    sl_pct = float(np.clip(atr / price * 2.0, MIN_SL_PCT/100, MAX_SL_PCT/100))
    if mode == 'Long':
        sl = price * (1 - sl_pct)
        tp = vwap
    else:
        sl = price * (1 + sl_pct)
        tp = vwap

    return {
        'mode': mode, 'sl': sl, 'tp': tp, 'atr': atr,
        'price': price,          # [v29 FIX] execute() требует sig['price'] — отсутствовал → KeyError
        'adx': 0.0, 'rsi': rsi, 'vol_ratio': round(vol_ratio, 2),
        'ema20': vwap, 'ema50': vwap, 'entry': price,
        'sma_dist': round(dist_atr, 2), 'vwap_dist': round(dist_atr, 2),
        'tp_mult': 0.0, 'rsi_prev': rsi,
        'btc_trend': btc_ctx.get('btc_trend', ''),
        'htf_trend': btc_ctx.get('htf_slope', 'Flat'),  # [v38] наклон EMA200 → лог
    }, 'ok'


async def publish_to_workers(sym: str, mode: str, price: float,
                              sl: float, tp: float, strategy: str,
                              risk_usdt: float, atr: float = 0.0):
    """
    Отправляет сигнал всем зарегистрированным Worker-сервисам.
    BingX-бот продолжает работать независимо от результата.
    """
    if not _worker_urls or not http:
        return

    payload = {
        'secret':    WORKER_SECRET,
        'symbol':    sym,
        'direction': mode,
        'entry':     round(price, 8),
        'sl':        round(sl, 8),
        'tp':        round(tp, 8),
        'strategy':  strategy,
        'atr':       round(atr, 8),   # [v36] реальный ATR для трейлинга воркера
        'timestamp': time.time(),
    }

    for url in _worker_urls:
        try:
            async with http.post(
                url, json=payload,
                timeout=aiohttp.ClientTimeout(total=5)
            ) as resp:
                status = resp.status
                if status == 200:
                    logging.info(f'📡 [WORKER] {sym} {mode} → {url[:40]} OK')
                else:
                    body = (await resp.text())[:100]
                    logging.warning(f'📡 [WORKER] {url[:40]} → {status}: {body}')
        except Exception as e:
            logging.warning(f'📡 [WORKER] {url[:40]} недоступен: {e}')


async def execute(sym: str, sig: dict, strategy: str,
                  pos_list: list, extra_tg: str = "", risk_mult: float = 1.0):
    """
    Общий исполнитель. strategy = 'SMC' | 'RSI'.
    sig может содержать 'bingx_vol' для oracle_volume.
    """
    global daily_stats

    mode  = sig['mode']
    price = sig['price']
    sl    = sig['sl']
    tp    = sig['tp']
    atr   = sig['atr']

    # Общие лимиты позиций
    all_pos = all_positions()
    if len(all_pos) >= MAX_TOTAL_POS:
        return
    if sum(1 for p in all_pos if p['direction'] == mode) >= MAX_PER_DIR:
        return

    # Запрет хеджа: если BCH Short открыт — BCH Long не открываем (и наоборот)
    if any(p['symbol'] == sym and p['direction'] != mode for p in all_pos):
        opp = 'Long' if mode == 'Short' else 'Short'
        logging.info(f'[{strategy}] {sym}: уже открыт {opp} — {mode} пропущен')
        return
    # [DUP-GUARD] Запрет дубля в ТОМ ЖЕ направлении (важно для MOM:
    # momentum держит позицию дольше кулдауна notified → возможен 2й вход)
    if any(p['symbol'] == sym and p['direction'] == mode for p in all_pos):
        logging.info(f'[{strategy}] {sym}: уже открыт {mode} — дубль пропущен')
        return

    # [P-2] Передаём BingX-объём чтобы не блокировать BingX-эксклюзивы
    bingx_vol = sig.get('bingx_vol', 0)
    has_vol = await oracle_volume(sym, bingx_vol)
    if not has_vol:
        logging.info(f"[{strategy}] {sym}: volume oracle reject (bingx:{bingx_vol:.0f})")
        return

    extra_ctx = {k: v for k, v in sig.items()
                 if k in ('rsi', 'vol_ratio', 'sma_dist', 'vwap_dist', 'btc_trend')}
    groq_key = os.getenv('GROQ_API_KEY', '')
    provider = ('Groq' if groq_key and _groq_quota_ok else
                'Gemini' if (GEMINI_KEY and _gemini_quota_ok) else 'LocalScore')
    logging.info(
        f'🔔 [{strategy}] {sym} {mode} @ {price:.6f} → AI Oracle [{provider}]'
    )
    ai = await oracle_ai(sym, strategy, mode, price, extra_ctx)
    if not ai['ok']:
        thr = 70 if strategy == 'SMC' else 55
        if strategy == 'SMC' and ai['conf'] >= 30:
            logging.info(f"[{strategy}] {sym}: AI advisory (conf={ai['conf']}) — CHoCH приоритет")
        else:
            logging.info(f"[{strategy}] {sym}: AI reject (conf={ai['conf']}/{thr})")
            return

    # Баланс
    try:
        bal       = await exchange.fetch_balance()
        free_usdt = float(bal.get('USDT', {}).get('free', 0))
    except Exception as e:
        logging.error(f"Balance error: {e}")
        return
    if free_usdt < 50:
        return

    risk_usdt = free_usdt * current_risk() * risk_mult  # [PB] risk_mult<1 для gated-live
    sl_dist   = abs(price - sl)
    if sl_dist <= 0:
        return

    # ── [v39] Объём: округление к БЛИЖАЙШЕМУ шагу лота биржи ──────────────
    # Было: qty = round(qty, 4) — хардкод 4 знака, игнорировал реальный шаг лота.
    # Стало: ccxt.ROUND по precision рынка. Важно: ccxt.amount_to_precision по
    # умолчанию TRUNCATE (обрезал бы 0.0018 → 0.001 при шаге 0.001 = −44% позиции).
    qty_ideal = risk_usdt / sl_dist
    try:
        if not exchange.markets or sym not in exchange.markets:
            await exchange.load_markets()
        _mkt     = exchange.market(sym)
        _min_qty = float((_mkt.get('limits', {}).get('amount', {}) or {}).get('min') or 0)
        qty = float(exchange.decimal_to_precision(
            qty_ideal, ccxt_async.ROUND, _mkt['precision']['amount'],
            exchange.precisionMode, exchange.paddingMode))
    except Exception as _me:
        logging.warning(f'[{strategy}] {sym}: market info недоступен ({_me}) — fallback round(,4)')
        _min_qty = 0.0
        qty = round(qty_ideal, 4)

    if qty <= 0 or (_min_qty and qty < _min_qty):
        logging.warning(f'[{strategy}] {sym}: qty={qty} < min_lot={_min_qty} — пропуск')
        return

    # [v39] ФАКТИЧЕСКИЙ риск от округлённого qty. Округление ВВЕРХ повышает риск,
    # поэтому для пропа обязателен потолок: грубый шаг лота на малом депо может
    # раздуть риск в разы → отказ от сделки вместо превышения дневного DD.
    risk_real = qty * sl_dist
    _risk_dev = risk_real / risk_usdt if risk_usdt > 0 else 1.0
    if _risk_dev > 1.30:
        logging.warning(
            f'[{strategy}] {sym}: шаг лота грубый — реальный риск ${risk_real:.2f} = '
            f'{_risk_dev:.2f}× целевого ${risk_usdt:.2f} (qty {qty_ideal:.6f}→{qty}) — '
            f'пропуск (проп-защита DD)'
        )
        return
    if _risk_dev > 1.05 or _risk_dev < 0.95:
        logging.info(
            f'[{strategy}] {sym}: qty {qty_ideal:.6f}→{qty} (шаг лота) | '
            f'риск ${risk_usdt:.2f}→${risk_real:.2f}'
        )
    # Всё ниже (TG, лог, воркер) использует РЕАЛЬНЫЙ риск округлённой позиции
    risk_usdt = risk_real

    # ── MIN/MAX notional guard ─────────────────────────────
    notional_est = qty * price
    if notional_est < 20:   # BingX мин. контракт ~$20
        logging.info(f'[{strategy}] {sym}: notional ${notional_est:.1f}<$20 — пропуск')
        return
    # [v31] Лимит учитывает плечо: проверяем маржу (notional / LEVERAGE), а не notional.
    # SA: до 20% депозита маржой | SMC/RSI/MOM/PB: до 15% депозита маржой.
    _margin_pct  = 0.30 if strategy == 'SA' else 0.15  # [v33] SA 20%→30%
    max_notional = free_usdt * LEVERAGE * _margin_pct
    if notional_est > max_notional:
        logging.warning(
            f'[{strategy}] {sym}: notional ${notional_est:.1f} > max ${max_notional:.1f} '
            f'({int(_margin_pct*100)}% маржи × {LEVERAGE}x плечо от ${free_usdt:.0f}) — пропуск'
        )
        return

    pos_side   = 'LONG'  if mode == 'Long'  else 'SHORT'
    order_side = 'buy'   if mode == 'Long'  else 'sell'
    sl_side    = 'sell'  if mode == 'Long'  else 'buy'

    try:
        await exchange.set_margin_mode('isolated', sym)
        # BingX hedge mode: leverage нужно установить для каждой стороны
        await exchange.set_leverage(LEVERAGE, sym,
            params={'side': pos_side})
        await exchange.create_order(
            sym, 'market', order_side, qty,
            params={'positionSide': pos_side}
        )
        sl_ord = await exchange.create_order(
            sym, 'STOP_MARKET', sl_side, qty,
            params={'positionSide': pos_side,
                    'stopPrice': round(sl, 8),
                    'reduceOnly': True}
        )
    except Exception as e:
        logging.error(f"[{strategy}] Order error {sym}: {e}")
        return

    rec = {
        'symbol':      sym,
        'direction':   mode,
        'strategy':    strategy,
        'entry_price': price,
        'initial_qty': qty,
        'current_qty': qty,
        'sl_price':    sl,
        'current_sl':  sl,
        'tp1':         tp,
        'sl_order_id': sl_ord['id'],
        'be_moved':    False,
        'tp50_hit':    False,
        'tp100_hit':   False,
        'atr':         atr,
        'adx':         round(float(sig.get('adx', 0)), 1),  # [FIX] sig.get вместо bare adx
        'sl_dist_pct': abs(price - sl) / price * 100,  # для динамического BE/TP50
        'open_time':   datetime.now(timezone.utc).isoformat(),
        'mfe_price':   price,
        'mae_price':   price,
        'mfe_time_min': 0,         # [v38] минута пика MFE (калибровка smart-timeout)
        'realized_pnl_usdt': 0.0,  # [v36] USDT от частичных закрытий (TP50)
        # Контекст рынка для аналитики
        'rsi_val':     round(float(sig.get('rsi', 0)), 1),
        'vol_ratio':   round(float(sig.get('vol_ratio', 0)), 2),
        'sma_dist':    round(float(sig.get('sma_dist', 0)), 2),
        'vwap_dist':   round(float(sig.get('vwap_dist', 0)), 2),
        'btc_trend':   str(sig.get('btc_trend', '')),
        'htf_trend':   str(sig.get('htf_trend', '')),  # [v38] Up/Down/Flat (пока только SA)
        # [v16] признаки для /stats_analyze
        'adx_val':     round(float(sig.get('adx', 0)), 1),
        'alt_score':   int(sig.get('alt_score', 0)),
        'entry_hour':  datetime.now(timezone.utc).hour,
        'open_time':   datetime.now(timezone.utc).isoformat(),
        'ai_conf':     int(ai.get('conf', 0)),
        'ai_comment':  str(ai.get('comment', '')).replace(',', ';')[:120],
        'tp_mult':     float(sig.get('tp_mult', 1.5)),
    }
    pos_list.append(rec)
    daily_stats[f"{strategy.lower()}_trades"] = daily_stats.get(f"{strategy.lower()}_trades", 0) + 1
    save_all()

    wknd = ' (Weekend ½)' if is_weekend() else ''
    rr = abs(tp - price) / abs(price - sl)
    msg = (
        f"{'🟢' if mode=='Long' else '🔴'} <b>[{strategy}] {sym}</b> — {mode}{wknd}\n"
        f"Цена: <code>{price:.6f}</code>\n"
        f"SL: <code>{sl:.6f}</code>  TP: <code>{tp:.6f}</code>\n"
        # [v39] Qty/Notional/Риск — от РЕАЛЬНОГО округлённого объёма (= биржа)
        f"Qty: <code>{qty}</code>  Notional: <b>${qty * price:.2f}</b>\n"
        f"RR: <b>1:{rr:.2f}</b>  Риск: <b>${risk_usdt:.2f}</b>\n"
        + (f"🤖 AI: bypass\n" if 'bypass' in ai['comment']
           else f"🧠 AI({provider}): {ai['conf']}/100 | {ai['comment']}\n")
        + extra_tg
    )
    await tg(msg)
    logging.info(f"✅ [{strategy}] {sym} {mode} @ {price:.6f} | SL:{sl:.6f} | "
                 f"Qty:{qty} | Notional:${qty*price:.2f} | Risk:${risk_usdt:.2f}")
    # Публикуем сигнал воркерам (копи-трейдинг на Bybit и др.)
    # [PB] экспериментальный PB НЕ копируем на воркер до валидации (n>=30)
    if strategy != 'PB':
        asyncio.create_task(publish_to_workers(sym, mode, price, sl, tp, strategy, risk_usdt,
                                               atr=float(sig.get('atr', 0.0))))

# ═══════════════════════════════════════════════════════
#  МОНИТОРИНГ ПОЗИЦИЙ (общий)
# ═══════════════════════════════════════════════════════
async def monitor_all():
    """
    Управление всеми открытыми позициями:
    - pnl >= 1.5%: Breakeven
    - pnl >= 2.5%: TP50%, SL → вход
    - pnl >= TP100: TP25% + трейлинг ATR
    - Нет позиции на бирже: закрыть и записать результат
    """
    global daily_stats, smc_positions, rsi_positions

    all_pos = all_positions()
    if not all_pos:
        return

    syms = list({p['symbol'] for p in all_pos})
    try:
        pos_raw = await exchange.fetch_positions(syms)
        tickers = await exchange.fetch_tickers(syms)
    except Exception as e:
        logging.error(f"Monitor fetch error: {e}")
        return

    async def process_pos(pos: dict, pos_list: list):
        sym      = pos['symbol']
        is_long  = pos['direction'] == 'Long'
        entry    = float(pos['entry_price'])
        strategy = pos.get('strategy', '?')

        ticker_d = tickers.get(sym, {})
        curr_p   = float(ticker_d.get('last', entry))

        live = next(
            (r for r in pos_raw
             if r.get('symbol') == sym
             and abs(float(r.get('contracts', 0))) > 0),
            None
        )
        pos_side = 'LONG' if is_long else 'SHORT'
        sl_side  = 'sell' if is_long else 'buy'

        # Обновить MFE/MAE ([v38] + минута пика MFE — калибровка smart-timeout)
        _prev_mfe = float(pos.get('mfe_price', entry))
        _mfe_improved = (curr_p > _prev_mfe) if is_long else (curr_p < _prev_mfe)
        if _mfe_improved:
            pos['mfe_price'] = curr_p
            try:
                pos['mfe_time_min'] = int((datetime.now(timezone.utc)
                    - datetime.fromisoformat(pos['open_time'])).total_seconds() / 60)
            except Exception:
                pass
        if is_long:
            pos['mae_price'] = min(float(pos.get('mae_price', entry)), curr_p)
        else:
            pos['mae_price'] = max(float(pos.get('mae_price', entry)), curr_p)

        if live:
            real_qty = abs(float(live.get('contracts', 0)))
            pos['current_qty'] = real_qty

            # ── [MOMENTUM] Чандельер-трейлинг (держим пока тренд жив) ──
            if pos.get('strategy') == 'MOM':
                atr_v = float(pos.get('atr', entry * 0.01))
                mfe_p = float(pos['mfe_price'])
                # [BREATHING STOP] трейл активируется только после +1% профита
                _mfe_pct = ((mfe_p - entry)/entry if is_long
                            else (entry - mfe_p)/entry) * 100
                if is_long:
                    if _mfe_pct >= 1.0:
                        new_trail = mfe_p - atr_v * MOM_TRAIL_ATR
                        if new_trail > pos.get('current_sl', 0):
                            pos['current_sl'] = new_trail
                    hit = curr_p <= pos['current_sl']
                else:
                    if _mfe_pct >= 1.0:
                        new_trail = mfe_p + atr_v * MOM_TRAIL_ATR
                        if new_trail < pos.get('current_sl', float('inf')):
                            pos['current_sl'] = new_trail
                    hit = curr_p >= pos['current_sl']
                if hit:
                    try:
                        await exchange.create_order(
                            sym, 'market', sl_side, real_qty,
                            params={'positionSide': pos_side, 'reduceOnly': True})
                        if pos.get('sl_order_id'):
                            try:
                                await exchange.cancel_order(pos['sl_order_id'], sym)
                            except Exception:
                                pass
                        pnl_f = ((curr_p - entry) / entry if is_long
                                 else (entry - curr_p) / entry) * 100
                        mfe_t = (abs(float(pos['mfe_price']) - entry) / entry) * 100
                        mae_t = (abs(float(pos['mae_price']) - entry) / entry) * 100
                        try:
                            _ot = datetime.fromisoformat(pos['open_time'])
                            dur_m = int((datetime.now(timezone.utc) - _ot).total_seconds() / 60)
                        except Exception:
                            dur_m = 0
                        net_u = pos.get('initial_qty', 0) * entry * (pnl_f/100) * LEVERAGE
                        await tg(f"🚀 <b>[{pos.get('strategy')}] {sym}</b> трейл-выход "
                                 f"<code>{curr_p:.6f}</code>  P&L: {pnl_f:+.2f}%")
                        log_trade(pos, curr_p, pnl_f, net_u, mfe_t, -mae_t,
                                  dur_m, 'TRAIL')
                    except Exception as _e:
                        logging.error(f'MOM exit {sym}: {_e}')
                    return False
                save_all()
                return True

            # Ghost position guard: закрываем мизерные остатки принудительно
            pos_value_usdt = real_qty * curr_p
            if pos_value_usdt < MIN_LOT_USDT and pos_value_usdt > 0:
                logging.warning(
                    f'👻 [{strategy}] {sym}: ghost position ({pos_value_usdt:.4f} USDT) — '
                    f'принудительное закрытие'
                )
                try:
                    if pos.get('sl_order_id'):
                        await exchange.cancel_order(pos['sl_order_id'], sym)
                    await exchange.create_order(
                        sym, 'market', sl_side, real_qty,
                        params={'positionSide': pos_side, 'reduceOnly': True}
                    )
                    await tg(
                        f'👻 <b>[{strategy}] {sym}</b>: ghost position закрыта\n'
                        f'Остаток: {pos_value_usdt:.4f} USDT — слишком мал для SL ордера'
                    )
                except Exception as _ge:
                    logging.error(f'Ghost close error {sym}: {_ge}')
                return False  # удалить из списка

            pnl = ((curr_p - entry) / entry * 100 if is_long
                   else (entry - curr_p) / entry * 100)

            # ── Таймаут позиции ────────────────────────────────
            dur_s   = (datetime.now(timezone.utc)
                       - datetime.fromisoformat(pos['open_time'])).total_seconds()
            dur_min = dur_s / 60
            max_dur = (MAX_TRADE_MIN_SMC if strategy == 'SMC'
                       else MAX_TRADE_MIN_SA if strategy == 'SA'
                       else MAX_TRADE_MIN_RSI)

            # ── [v36] Smart Timeout (SMC): раннее закрытие мёртвых сделок ──
            # Данные: 9/21 сд закрыты Timeout с Avg -0.95% — съедают TP-профит.
            # Если 90+ мин, pnl слегка отрицательный (но не у SL) и TP50 не было —
            # CHoCH не отработал, выходим раньше вместо ожидания полных 180 мин.
            sl_dist_pct_st = float(pos.get('sl_dist_pct', 1.5))
            if (strategy == 'SMC' and dur_min > 90 and not pos.get('tp50_hit')
                    and -0.5 * sl_dist_pct_st < pnl < 0):
                logging.warning(
                    f'⏰ [SMC-SMART] {sym}: {dur_min:.0f}мин, pnl={pnl:+.2f}% '
                    f'(зона -{0.5*sl_dist_pct_st:.2f}%..0) — сделка мертва, ранний выход'
                )
                try:
                    if pos.get('sl_order_id'):
                        await exchange.cancel_order(pos['sl_order_id'], sym)
                    await exchange.create_order(
                        sym, 'market', sl_side, real_qty,
                        params={'positionSide': pos_side, 'reduceOnly': True}
                    )
                    mfe_t = abs(float(pos.get('mfe_price', entry)) - entry) / entry * 100
                    mae_t = abs(float(pos.get('mae_price', entry)) - entry) / entry * 100
                    # [TIMEOUT-USDT] чистый USDT: движение цены − комиссия round-trip + фикс TP50
                    _q     = float(pos.get('current_qty', 0))
                    _gross = (curr_p - entry) * _q if is_long else (entry - curr_p) * _q
                    _fee   = _q * (entry + curr_p) * FEE_RATE
                    _to_net = _gross - _fee + pos.get('realized_pnl_usdt', 0.0)
                    log_trade(pos, curr_p, pnl, _to_net, mfe_t, mae_t,
                              int(dur_min), 'Timeout')
                    await tg(
                        f'⏰ <b>[SMC] {sym}</b>: smart-timeout {dur_min:.0f}мин\n'
                        f'Сделка не отработала | PnL: {pnl:+.2f}% ({_to_net:+.2f} USDT)'
                    )
                except Exception as _te:
                    logging.error(f'Smart timeout close error {sym}: {_te}')
                return False
                        
            # ── [v38] SA Smart Timeout: ранний выход из мёртвых MR-сетапов ──
            # Таймауты SA держат слот до 150 мин и в среднем закрываются в минус.
            # Если за половину лимита (75 мин) цена НИ РАЗУ не прошла +0.35%
            # к VWAP, TP50 не было и сейчас ноль/минус — возврат не состоялся.
            # Пороги 75мин/0.35% ВРЕМЕННЫЕ — калибровать по mfe_time_min при n>=30.
            _sa_mfe_pct = abs(float(pos.get('mfe_price', entry)) - entry) / entry * 100
            if (strategy == 'SA' and dur_min > 75 and not pos.get('tp50_hit')
                    and pnl <= 0 and _sa_mfe_pct < 0.35):
                logging.warning(
                    f'⏰ [SA-SMART] {sym}: {dur_min:.0f}мин, pnl={pnl:+.2f}%, '
                    f'MFE={_sa_mfe_pct:.2f}%<0.35% — сетап мёртв, ранний выход'
                )
                try:
                    if pos.get('sl_order_id'):
                        await exchange.cancel_order(pos['sl_order_id'], sym)
                    await exchange.create_order(
                        sym, 'market', sl_side, real_qty,
                        params={'positionSide': pos_side, 'reduceOnly': True}
                    )
                    mae_t = abs(float(pos.get('mae_price', entry)) - entry) / entry * 100
                    _q     = float(pos.get('current_qty', 0))
                    _gross = (curr_p - entry) * _q if is_long else (entry - curr_p) * _q
                    _fee   = _q * (entry + curr_p) * FEE_RATE
                    _to_net = _gross - _fee + pos.get('realized_pnl_usdt', 0.0)
                    log_trade(pos, curr_p, pnl, _to_net, _sa_mfe_pct, mae_t,
                              int(dur_min), 'Timeout')
                    await tg(
                        f'⏰ <b>[SA] {sym}</b>: smart-timeout {dur_min:.0f}мин\n'
                        f'MFE {_sa_mfe_pct:.2f}% — возврата к VWAP нет | '
                        f'PnL: {pnl:+.2f}% ({_to_net:+.2f} USDT)'
                    )
                except Exception as _te:
                    logging.error(f'SA smart timeout close error {sym}: {_te}')
                return False
            
            if dur_min > max_dur and not pos.get('tp50_hit'):
                logging.warning(
                    f'⏰ [{strategy}] {sym}: таймаут {dur_min:.0f}мин>={max_dur}мин '
                    f'pnl={pnl:+.2f}% — принудительное закрытие'
                )
                try:
                    if pos.get('sl_order_id'):
                        await exchange.cancel_order(pos['sl_order_id'], sym)
                    await exchange.create_order(
                        sym, 'market', sl_side, real_qty,
                        params={'positionSide': pos_side, 'reduceOnly': True}
                    )
                    mfe_t = abs(float(pos.get('mfe_price', entry)) - entry) / entry * 100
                    mae_t = abs(float(pos.get('mae_price', entry)) - entry) / entry * 100
                    # [TIMEOUT-USDT] чистый USDT: движение цены − комиссия round-trip + фикс TP50
                    _q     = float(pos.get('current_qty', 0))
                    _gross = (curr_p - entry) * _q if is_long else (entry - curr_p) * _q
                    _fee   = _q * (entry + curr_p) * FEE_RATE
                    _to_net = _gross - _fee + pos.get('realized_pnl_usdt', 0.0)
                    log_trade(pos, curr_p, pnl, _to_net, mfe_t, mae_t,
                              int(dur_min), 'Timeout')
                    await tg(
                        f'⏰ <b>[{strategy}] {sym}</b>: таймаут {dur_min:.0f}мин\n'
                        f'Закрыто рыночным | PnL: {pnl:+.2f}% ({_to_net:+.2f} USDT)'
                    )
                except Exception as _te:
                    logging.error(f'Timeout close error {sym}: {_te}')
                return False

            # ── Breakeven ──────────────────────────────────────
            # BE после TP50: ждём 2.1% если TP50 ещё не сработал
            # ── Динамический BE: 0.7R (не 0.5R — давал слишком ранний BE) ──
            # SL=1.2% → BE при +0.84% | SL=2.0% → BE при +1.4%
            # После TP50 → BE сразу (защита фиксированной прибыли)
            sl_dist_pct = pos.get('sl_dist_pct', 2.0)
            be_thr_dyn  = max(sl_dist_pct * 0.9, 0.8)  # [PROP] 0.7R→0.9R: поздняя защита, больше шансов на TP
            if pos.get('tp50_hit'):
                be_thr_dyn = max(sl_dist_pct * 0.2, 0.3)  # после TP50 мгновенный BE
            if pnl >= be_thr_dyn and not pos.get('be_moved'):
                new_sl = entry * 1.0015 if is_long else entry * 0.9985  # 0.15% > 2×fee
                try:
                    if pos.get('sl_order_id'):
                        await exchange.cancel_order(pos['sl_order_id'], sym)
                    sl_ord = await exchange.create_order(
                        sym, 'STOP_MARKET', sl_side, real_qty,
                        params={'positionSide': pos_side,
                                'stopPrice': round(new_sl, 8),
                                'reduceOnly': True}
                    )
                    pos.update({'current_sl': new_sl,
                                'sl_order_id': sl_ord['id'],
                                'be_moved': True})
                    save_all()
                    await tg(f"🛡 <b>[{strategy}] {sym}</b>: SL → БУ "
                             f"<code>{new_sl:.6f}</code>  P&L: +{pnl:.2f}%")
                except Exception as e:
                    logging.error(f"BE error {sym}: {e}")

            # ── TP 50% ─────────────────────────────────────────
            if strategy == 'SA':
                # [SA-DYN v38] Частичка на ПОЛПУТИ к VWAP, а не жёсткие 0.8%.
                # Баг v37: константа 0.8% могла быть ДАЛЬШЕ самого TP (VWAP близко),
                # и частичка/БУ не срабатывали — сделка уходила в SL.
                tp_tgt = float(pos.get('tp1', 0) or 0)
                if tp_tgt > 0:
                    tp_dist_pct  = abs(tp_tgt - entry) / entry * 100
                    tp50_thr_dyn = max(tp_dist_pct * 0.5, 0.2)  # пол 0.2% — выше комиссий/шума
                else:
                    tp50_thr_dyn = SA_PARTIAL_PCT               # fallback если TP не задан
            else:
                tp50_thr_dyn = max(sl_dist_pct * 0.8, 0.8)  # [PROP] 1.0R→0.8R: ранняя фиксация, легче TP
            if pnl >= tp50_thr_dyn and not pos.get('tp50_hit'):
                close_qty = round(real_qty * 0.5, 8)
                remain    = round(real_qty - close_qty, 8)
                # [FIX] Если 50% округляется в 0 (мелкая позиция) — 
                # двигаем SL в BE без частичного закрытия
                if close_qty <= 0 or remain <= 0:
                    pos['tp50_hit'] = True  # помечаем, чтобы сработал BE
                    logging.info(f'{sym}: TP50 qty→0 (мелкая поз), только BE без фиксации')
                    try:
                        if pos.get('sl_order_id'):
                            await exchange.cancel_order(pos['sl_order_id'], sym)
                        be_price = entry * (1 + sl_dist_pct/100 * 0.2) if is_long else entry * (1 - sl_dist_pct/100 * 0.2)
                        pos['current_sl'] = be_price
                    except Exception as _e:
                        logging.warning(f'{sym}: BE move fail: {_e}')
                    save_all()
                    return True  # [FIX] continue→return (мы в функции, не в цикле)
                try:
                    await exchange.create_order(
                        sym, 'market', sl_side, close_qty,
                        params={'positionSide': pos_side, 'reduceOnly': True}
                    )
                    if pos.get('sl_order_id'):
                        await exchange.cancel_order(pos['sl_order_id'], sym)
                    # [SA-EXIT] БУ хвоста с учётом 2×комиссии (голый entry = минус на фи)
                    be_after_tp50 = entry * 1.0015 if is_long else entry * 0.9985
                    sl_ord = await exchange.create_order(
                        sym, 'STOP_MARKET', sl_side, remain,
                        params={'positionSide': pos_side,
                                'stopPrice': round(be_after_tp50, 8),
                                'reduceOnly': True}
                    )
                    # [v36] фиксируем USDT-профит от закрытых 50%
                    tp50_raw = (curr_p - entry) * close_qty if is_long else (entry - curr_p) * close_qty
                    tp50_fee = close_qty * curr_p * FEE_RATE
                    pos['realized_pnl_usdt'] = pos.get('realized_pnl_usdt', 0.0) + tp50_raw - tp50_fee
                    pos.update({'tp50_hit': True, 'current_qty': remain,
                                'sl_order_id': sl_ord['id'], 'be_moved': True,
                                'current_sl': be_after_tp50})  # [SA-EXIT] БУ ровно в момент TP50
                    save_all()
                    await tg(f"💰 <b>[{strategy}] {sym}</b>: TP50% зафиксирован "
                             f"P&L: +{pnl:.2f}% | {tp50_raw - tp50_fee:+.2f} USDT")
                except Exception as e:
                    logging.error(f"TP50 error {sym}: {e}")

            # ── [SA-FRONTRUN v38] Защитный БУ при недоходе до VWAP ──
            # Если цена прошла >=80% пути к TP, но не коснулась его — переводим
            # ОСТАТОК в БУ. Защита от разворота у самой цели (в баг-сделке цена
            # дошла до 62911 при цели 62948 и рухнула в SL).
            if strategy == 'SA' and not pos.get('be_moved'):
                tp_tgt = float(pos.get('tp1', 0) or 0)
                denom  = (tp_tgt - entry) if is_long else (entry - tp_tgt)
                if tp_tgt > 0 and denom > 0:
                    mfe_p = float(pos.get('mfe_price', entry))
                    peak  = ((mfe_p - entry) if is_long else (entry - mfe_p)) / denom
                    if 0.8 <= peak < 1.0:   # прошли 80%+, но TP (100%) не коснулись
                        be_price = entry * 1.0015 if is_long else entry * 0.9985
                        cur_sl   = float(pos.get('current_sl', 0 if is_long else 1e18))
                        better   = be_price > cur_sl if is_long else be_price < cur_sl
                        if better:
                            try:
                                if pos.get('sl_order_id'):
                                    await exchange.cancel_order(pos['sl_order_id'], sym)
                                _q = float(pos.get('current_qty', real_qty))
                                sl_ord = await exchange.create_order(
                                    sym, 'STOP_MARKET', sl_side, _q,
                                    params={'positionSide': pos_side,
                                            'stopPrice': round(be_price, 8),
                                            'reduceOnly': True})
                                pos.update({'current_sl': be_price,
                                            'sl_order_id': sl_ord['id'],
                                            'be_moved': True})
                                save_all()
                                await tg(f"🛡 <b>[SA] {sym}</b>: фронтран "
                                         f"{peak*100:.0f}% пути к VWAP — SL→БУ "
                                         f"<code>{be_price:.6f}</code>")
                            except Exception as _fe:
                                logging.warning(f"{sym} frontrun BE fail: {_fe}")

            # ── ATR Trailing Stop после TP50 (ФИЗИЧЕСКИЙ) ──────
            # [SYMMETRY] Перевыпускаем реальный STOP_MARKET на бирже (как воркер),
            # а не только pos['current_sl'] в памяти. Защищает runner при краше/
            # рестарте бота: стоп уже стоит на BingX независимо от процесса.
            # Ордер переставляем ТОЛЬКО когда трейл реально сдвинулся — экономим API.
            if pos.get('tp50_hit') and not pos.get('tp100_hit'):
                atr_v = float(pos.get('atr', entry * 0.005))
                mfe_p = float(pos['mfe_price'])
                trail_mult = SA_TRAIL_ATR if strategy == 'SA' else 1.2  # [SA-EXIT] узкий трейл для MR
                run_qty = float(pos.get('current_qty', real_qty))
                cur_sl  = float(pos.get('current_sl', 0 if is_long else float('inf')))
                new_trail = (mfe_p - atr_v * trail_mult if is_long
                             else mfe_p + atr_v * trail_mult)
                moved = new_trail > cur_sl if is_long else new_trail < cur_sl
                if moved and run_qty > 0:
                    try:
                        if pos.get('sl_order_id'):
                            await exchange.cancel_order(pos['sl_order_id'], sym)
                        sl_ord = await exchange.create_order(
                            sym, 'STOP_MARKET', sl_side, run_qty,
                            params={'positionSide': pos_side,
                                    'stopPrice': round(new_trail, 8),
                                    'reduceOnly': True}
                        )
                        pos['current_sl']  = new_trail
                        pos['sl_order_id'] = sl_ord['id']
                        logging.debug(f'{sym} trail SL (биржа) → {new_trail:.6f}')
                    except Exception as _tr:
                        # Не удалось переставить — оставляем прежний биржевой стоп,
                        # current_sl НЕ трогаем, чтобы память не разошлась с биржей.
                        logging.warning(f'{sym} trail re-issue fail: {_tr}')

            # ── TP100 + trailing ───────────────────────────────
            tp100 = float(pos.get('tp1', entry))
            if (pos.get('tp50_hit') and not pos.get('tp100_hit')
                    and ((is_long and curr_p >= tp100)
                         or (not is_long and curr_p <= tp100))):
                close_qty = round(float(pos['current_qty']) * 0.5, 8)
                remain    = round(float(pos['current_qty']) - close_qty, 8)
                atr_v     = float(pos.get('atr', entry * 0.01))
                trail_sl  = (tp100 - atr_v if is_long else tp100 + atr_v)
                # [FIX] Если 50% округляется в 0 — только трейлим SL, без закрытия
                if close_qty <= 0 or remain <= 0:
                    pos['tp100_hit'] = True
                    pos['current_sl'] = trail_sl
                    logging.info(f'{sym}: TP100 qty→0 (мелкая поз), только трейл SL')
                    save_all()
                    return True  # [FIX] continue→return
                try:
                    await exchange.create_order(
                        sym, 'market', sl_side, close_qty,
                        params={'positionSide': pos_side, 'reduceOnly': True}
                    )
                    if pos.get('sl_order_id'):
                        await exchange.cancel_order(pos['sl_order_id'], sym)
                    sl_ord = await exchange.create_order(
                        sym, 'STOP_MARKET', sl_side, remain,
                        params={'positionSide': pos_side,
                                'stopPrice': round(trail_sl, 8),
                                'reduceOnly': True}
                    )
                    # [v39] Фиксируем USDT от закрытых на TP100 25% — раньше эта
                    # частичка НЕ попадала в realized_pnl_usdt и занижала итог позиции
                    tp100_raw = (curr_p - entry) * close_qty if is_long else (entry - curr_p) * close_qty
                    tp100_fee = close_qty * curr_p * FEE_RATE
                    tp100_net = tp100_raw - tp100_fee
                    pos['realized_pnl_usdt'] = pos.get('realized_pnl_usdt', 0.0) + tp100_net
                    pos.update({'tp100_hit': True, 'current_qty': remain,
                                'sl_order_id': sl_ord['id'],
                                'current_sl': trail_sl})
                    save_all()
                    await tg(f"🏆 <b>[{strategy}] {sym}</b>: TP100 взят! "
                             f"Трейлинг включён P&L: +{pnl:.2f}% | {tp100_net:+.2f} USDT")
                except Exception as e:
                    logging.error(f"TP100 error {sym}: {e}")

            # ── Обновление трейлинга ────────────────────────────
            if pos.get('tp100_hit'):
                atr_v    = float(pos.get('atr', entry * 0.01))
                new_trail = (curr_p - atr_v if is_long else curr_p + atr_v)
                cur_sl    = float(pos.get('current_sl', 0))
                if ((is_long and new_trail > cur_sl)
                        or (not is_long and new_trail < cur_sl)):
                    try:
                        if pos.get('sl_order_id'):
                            await exchange.cancel_order(pos['sl_order_id'], sym)
                        sl_ord = await exchange.create_order(
                            sym, 'STOP_MARKET', sl_side,
                            float(pos['current_qty']),
                            params={'positionSide': pos_side,
                                    'stopPrice': round(new_trail, 8),
                                    'reduceOnly': True}
                        )
                        pos.update({'sl_order_id': sl_ord['id'],
                                    'current_sl': new_trail})
                    except:
                        pass

            return True  # позиция жива

        else:
            # Позиция закрыта
            seconds = (
                datetime.now(timezone.utc)
                - datetime.fromisoformat(pos['open_time'])
            ).total_seconds()
            if seconds < 120:   # grace period: 2 мин чтобы BingX зарегистрировал позицию
                return True

            exit_p = float(pos.get('current_sl', curr_p))
            try:
                trades = await exchange.fetch_my_trades(sym, limit=5)
                if trades:
                    open_ts = int(
                        datetime.fromisoformat(pos['open_time']).timestamp() * 1000
                    )
                    valid = [t for t in trades if t['timestamp'] >= open_ts]
                    if valid:
                        exit_p = float(valid[-1]['price'])
            except:
                exit_p = curr_p

            raw = ((exit_p - entry) * float(pos['current_qty']) if is_long
                   else (entry - exit_p) * float(pos['current_qty']))
            fee_in  = float(pos['initial_qty']) * entry * FEE_RATE
            fee_out = float(pos['current_qty']) * exit_p * FEE_RATE
            # [v36] runner-часть + зафиксированный USDT от TP50 (v34-фикс восстановлен)
            runner_pnl = raw - fee_out
            net_pnl = runner_pnl + pos.get('realized_pnl_usdt', 0.0) - fee_in

            # [v36] pnl_pct = ROE (движение цены × LEVERAGE) — совпадает с биржей
            price_move_pct = ((exit_p - entry) / entry * 100 if is_long
                              else (entry - exit_p) / entry * 100)
            pnl_pct = price_move_pct * LEVERAGE
            daily_stats['trades'] += 1
            daily_stats['pnl_pct'] += price_move_pct / 100  # DD-расчёт без плеча

            # Реальная победа = чистые деньги в плюсе после комиссий.
            # [WIN-FIX] Раньше tp50_hit форсил победу даже если раннер ушёл в SL
            # и net_pnl < 0 → отчёт показывал WR 100% при минусе по дню.
            # Теперь победа ТОЛЬКО если итоговый net_pnl строго > 0.
            is_win = net_pnl > 0
            # BE = цена не падала, но комиссия/проскальзывание съели профит в ноль/микро-минус
            is_be  = (not is_win) and pnl_pct >= 0
            if is_win:
                daily_stats['wins'] += 1
            elif is_be:
                daily_stats['be_closes'] = daily_stats.get('be_closes', 0) + 1

            mfe_pct = abs(float(pos['mfe_price']) - entry) / entry * 100
            mae_pct = abs(float(pos['mae_price']) - entry) / entry * 100
            dur_min = int(seconds / 60)

            winrate_d = (daily_stats['wins'] / daily_stats['trades'] * 100
                         if daily_stats['trades'] > 0 else 0)
            # [WIN-FIX] тег согласован с is_win — убран конфликт «(TP✓) + 🛑»
            _tp_mark = ' TP✓' if pos.get('tp50_hit') else ''
            result_tag = (f' (WIN{_tp_mark})' if is_win
                          else ' (BE)' if is_be
                          else f' (SL{_tp_mark})')
            await tg(
                f"{'✅' if is_win else ('⚖️' if is_be else '🛑')} <b>[{strategy}] {sym}</b> закрыта{result_tag}\n"
                f"PnL: <code>{pnl_pct:+.2f}%</code> | Net: <code>{net_pnl:+.2f} USDT</code>\n"
                f"📈 MFE {mfe_pct:.2f}% | 📉 MAE {mae_pct:.2f}% | ⏱ {dur_min}мин\n"
                f"Вход: {entry:.6f} | Выход: {exit_p:.6f}\n"
                f"День: {daily_stats['trades']} сделок | "
                f"{daily_stats['wins']} побед | WR {winrate_d:.0f}%"
            )
            # Determine close reason
            _close_reason = ('TP' if pos.get('tp50_hit') and pnl_pct >= 0.5 * LEVERAGE
                             else 'BE' if is_be
                             else 'SL' if pnl_pct < 0
                             else 'WIN')
            log_trade(
                pos, exit_p, pnl_pct, net_pnl,
                mfe_pct, mae_pct, dur_min, _close_reason
            )
            save_all()
            notified[sym] = time.time()   # cooldown после закрытия
            return False   # удалить из списка

    # Обработка SMC, RSI, SA и PB позиций
    new_smc, new_rsi, new_sa, new_pb = [], [], [], []
    for p in smc_positions:
        keep = await process_pos(p, smc_positions)
        if keep:
            new_smc.append(p)
    for p in rsi_positions:
        keep = await process_pos(p, rsi_positions)
        if keep:
            new_rsi.append(p)
    for p in sa_positions:   # [v24]
        keep = await process_pos(p, sa_positions)
        if keep:
            new_sa.append(p)
    for p in pb_positions:   # [PB]
        keep = await process_pos(p, pb_positions)
        if keep:
            new_pb.append(p)
    smc_positions[:] = new_smc
    rsi_positions[:] = new_rsi
    sa_positions[:]  = new_sa   # [v24]
    pb_positions[:]  = new_pb   # [PB]

# ═══════════════════════════════════════════════════════
#  СКАНЕРЫ
# ═══════════════════════════════════════════════════════
async def get_tickers_cached() -> dict:
    """Единый кэш тикеров для SMC и RSI — обновляется раз в 5 минут."""
    global _tickers_cache, _tickers_cache_ts
    if _tickers_cache and time.time() - _tickers_cache_ts < TICKERS_CACHE_TTL:
        return _tickers_cache
    try:
        _tickers_cache = await exchange.fetch_tickers()
        _tickers_cache_ts = time.time()
        logging.debug(f'[TICKERS] Кэш обновлён: {len(_tickers_cache)} инструментов')
    except Exception as e:
        logging.warning(f'[TICKERS] Ошибка обновления: {e}')
    return _tickers_cache


async def scan_smc():
    """Сканер SMC: запускается каждые 60 сек в торговые сессии."""
    if not is_session():
        return  # вне торговой сессии — молчим
    if not check_circuit_breaker():
        logging.debug('[SMC] Circuit breaker активен — скан пропущен')
        return

    # [v16.1] btc-контекст один раз на скан (для alt_score в /stats_analyze)
    smc_btc_ctx = await get_btc_context()

    markets = await get_markets()
    # Предфильтр: получаем объёмы всех монет ОДНИМ запросом
    # Это в 50x быстрее чем 250 отдельных fetch_ticker
    all_tickers = await get_tickers_cached()
    # Предпорог 30% от MIN_VOL — отсекает мусор, но не режет живые монеты
    vol_pre = MIN_VOL_USDT * 0.3
    scan = [
        s for s in list(markets.keys())
        if sym_allowed(s)
        and float((all_tickers.get(s) or {}).get('quoteVolume', 0) or 0) >= vol_pre
    ][:SCAN_LIMIT]
    sem  = asyncio.Semaphore(SCAN_SEM)
    st   = {k: 0 for k in ['session','news','vol','structure','choch','short_blocked',
                            'vwap','rsi','adx_flat','fvg','fvg_test','ok']}

    async def check(sym):
        if sym in notified:
            return
        try:
            async with sem:
                sig, reason = await smc_signal(sym, smc_btc_ctx)
            st[reason] = st.get(reason, 0) + 1
            if sig:
                notified[sym] = time.time()
                # [v16] передаём alt_score для /stats_analyze
                sig['alt_score'] = smc_btc_ctx.get('alt_score', 0)
                await execute(sym, sig, 'SMC', smc_positions,
                              f"RSI: {sig['rsi']:.1f}")
        except Exception as _e:
            st['error'] = st.get('error', 0) + 1
            if st['error'] <= 2:  # логируем только первые 2 (не спамим)
                logging.warning(f'[SMC] {sym} error: {type(_e).__name__}: {_e}')

    await asyncio.gather(*[check(s) for s in scan])
    logging.info(
        f"[SMC SCAN] news:{st['news']} vol:{st['vol']} struct:{st['structure']} "
        f"choch:{st['choch']} short_blk:{st.get('short_blocked',0)} "
        f"vwap:{st['vwap']} rsi:{st['rsi']} "
        f"adx:{st.get('adx_flat',0)} "
        f"rsi_exh:{st.get('rsi_exhaustion',0)} alt_hi:{st.get('alt_high',0)} "
        f"fvg:{st.get('fvg',0)+st.get('fvg_test',0)} "
        f"err:{st.get('error',0)} → ВХОДЫ:{st['ok']}"
    )

async def scan_rsi():
    """Сканер RSI MR: запускается каждые 60 сек."""
    if not check_circuit_breaker():
        logging.debug('[RSI] Circuit breaker активен — скан пропущен')
        return

    markets   = await get_markets()
    btc_ctx   = await get_btc_context()
    # Предфильтр — используем тот же кэш что и SMC (без повторного запроса)
    all_tickers = await get_tickers_cached()
    vol_pre = MIN_VOL_USDT * 0.3
    scan = [
        s for s in list(markets.keys())
        if sym_allowed(s)
        and float((all_tickers.get(s) or {}).get('quoteVolume', 0) or 0) >= vol_pre
    ][:SCAN_LIMIT]
    sem  = asyncio.Semaphore(SCAN_SEM)
    st   = {k: 0 for k in ['news','vol','rsi_mid','trend','hook',
                            'momentum','sma_range','vwap','sl_wide',
                            'squeeze','no_pattern','ok']}

    async def check(sym):
        if sym in notified:
            return
        try:
            # Быстрый pre-фильтр по тикеру (спред)
            async with sem:
                sig, reason = await rsi_signal(sym, btc_ctx)
            st[reason] = st.get(reason, 0) + 1
            if sig:
                notified[sym] = time.time()
                extra = (
                    f"RSI: {sig['rsi']:.1f}→{sig['rsi_prev']:.1f}  "
                    f"Vol: {sig['vol_ratio']:.1f}x  "
                    f"SMA-dist: {sig['sma_dist']:+.1f}%  "
                    f"TP-mult: {sig['tp_mult']}x"
                )
                await execute(sym, sig, 'RSI', rsi_positions, extra)
            # [v37] MOMENTUM ПОЛНОСТЬЮ ОТКЛЮЧЁН: 229 сд, PF 0.27-0.74 во всех
            # сегментах кроме RSI 40-60 (n=18, PF 1.14 — недостаточно для edge
            # на фоне тотальной убыточности). Сканер выключен для экономии
            # API-лимитов и CPU. Код momentum_signal сохранён для истории.
            # elif MOMENTUM_ENABLED and sym not in notified:
            #     async with sem:
            #         msig, mreason = await momentum_signal(sym, btc_ctx)
            #     if msig:
            #         ... (см. git-историю / v36)
            # [PULLBACK] независимый shadow-детект входа по откату (не торгует)
            if PB_ENABLED:
                async with sem:
                    psig, preason = await pullback_signal(sym, btc_ctx)
                if psig:
                    _alt = '🔥ALT' if btc_ctx.get('altseason') else 'no-alt'
                    _ascore = btc_ctx.get('alt_score', 50)
                    _spread = btc_ctx.get('eth_btc_spread', 0.0)
                    logging.info(
                        f"🎯 [PB SHADOW] {sym} {psig['mode']} @ {psig['rsi']:.0f}rsi "
                        f"ADX:{psig['adx']:.0f} Vol:{psig['vol_ratio']:.1f}x "
                        f"near-EMA20 [{_alt} score:{_ascore} ETH/BTC:{_spread:+.1f}%]"
                    )
                    shadow_record(sym, psig['mode'], psig['entry'], psig, btc_ctx, 'PB')

                    # [PB GATED LIVE] микро-размер, только пока набираем n>=30.
                    # Фильтры уже жёсткие в pullback_signal: ADX>60, Vol 1.5-3x, alt<40, Long-only.
                    if PB_LIVE and sym not in notified:
                        psig['price'] = psig['entry']       # execute() требует sig['price']
                        await execute(sym, psig, 'PB', pb_positions,
                                      risk_mult=PB_RISK_MULT)
                        notified[sym] = time.time()
        except Exception as _e:
            st['error'] = st.get('error', 0) + 1
            if st['error'] <= 2:
                logging.warning(f'[RSI] {sym} error: {type(_e).__name__}: {_e}')

    await asyncio.gather(*[check(s) for s in scan])
    total_r = sum(st.values())
    logging.info(
        f"[RSI SCAN] BTC:{btc_ctx['btc_trend']} Alt:{btc_ctx['altseason']} | "
        f"total:{total_r} news:{st['news']} vol:{st['vol']} mid:{st['rsi_mid']} hook:{st['hook']} "
        f"mom:{st['momentum']} sma:{st['sma_range']} vwap:{st['vwap']} "
        f"trend:{st['trend']} sl:{st['sl_wide']} sqz:{st['squeeze']} "
        f"pat:{st['no_pattern']} err:{st.get('error',0)} → ВХОДЫ:{st['ok']}"
    )

# ═══════════════════════════════════════════════════════
#  ЕЖЕДНЕВНЫЙ ОТЧЁТ + СБРОС СТАТИСТИКИ
# ═══════════════════════════════════════════════════════
async def send_daily_report():
    """Отправляет итоги дня в Telegram. Триггер hour>=19 UTC (22:00 Киев).
    Персистентный гард по дате — отправляет ровно один раз в день,
    переживает рестарт Render."""
    global _daily_report_sent
    today_str = datetime.now(timezone.utc).date().isoformat()
    try:
        con = sqlite3.connect(TRADES_DB)
        r = con.execute("SELECT value FROM meta WHERE key='last_report_date'").fetchone()
        con.close()
        if r and r[0] == today_str:
            _daily_report_sent = True
            return  # уже отправлен сегодня (переживает рестарт)
    except Exception:
        pass
    _daily_report_sent = True
    try:
        con = sqlite3.connect(TRADES_DB)
        con.execute("INSERT OR REPLACE INTO meta (key,value) VALUES ('last_report_date',?)",
                    (today_str,))
        con.commit(); con.close()
    except Exception:
        pass

    trades = daily_stats.get('trades', 0)
    wins   = daily_stats.get('wins', 0)
    wrate  = wins / trades * 100 if trades > 0 else 0

    try:
        bal = await exchange.fetch_balance()
        bal_usdt = float(bal.get('USDT', {}).get('total', 0))
    except:
        bal_usdt = daily_stats.get('start_balance', 0)

    start    = daily_stats.get('start_balance', 0)
    day_pct  = (bal_usdt - start) / start * 100 if start > 0 else 0
    day_usdt = bal_usdt - start

    await tg(
        f"📊 <b>BingX — Итоги дня {daily_stats.get('stat_date', '')} 22:00</b>\n"
        f"Сделок: {trades} | WR: {wrate:.1f}% ({wins}/{trades})\n"
        f"SMC: {daily_stats.get('smc_trades',0)} | RSI: {daily_stats.get('rsi_trades',0)} | "
        f"BE: {daily_stats.get('be_closes',0)}\n"
        f"PnL: <code>{day_pct:+.2f}%</code> | <code>{day_usdt:+.2f} USDT</code>\n"
        f"Баланс: <code>{bal_usdt:.2f} USDT</code>"
    )
    logging.info(f"📊 Итоги дня BingX отправлены: {day_pct:+.2f}% ({day_usdt:+.2f} USDT)")


async def daily_reset():
    global daily_stats, circuit_open, _daily_report_sent
    today = datetime.now(timezone.utc).date()
    if daily_stats.get('stat_date') == today:
        return

    # Отчёт отправляется отдельно в 22:00 Киев через send_daily_report()
    pass  # сброс stats происходит ниже

    # Сброс
    try:
        bal = await exchange.fetch_balance()
        new_start = float(bal.get('USDT', {}).get('total', 0))
    except:
        new_start = daily_stats.get('start_balance', 0.0)

    daily_stats = {
        'pnl_pct': 0.0, 'trades': 0, 'wins': 0,
        'start_balance': new_start, 'stat_date': today,
        'smc_trades': 0, 'rsi_trades': 0,
    }
    circuit_open = False
    _daily_report_sent = False  # сброс флага на новый день
    save_all()
    logging.info(f"📅 Daily stats reset for {today}")

# ═══════════════════════════════════════════════════════
#  HEALTH CHECK (Render keep-alive)
# ═══════════════════════════════════════════════════════
class HealthCheck(BaseHTTPRequestHandler):
    def do_GET(self):
        body = (
            f"Unified SMC+RSI Bot v10.0 | "
            f"SMC:{len(smc_positions)} RSI:{len(rsi_positions)} | "
            f"PnL:{daily_stats['pnl_pct']*100:+.2f}%"
        ).encode()
        self.send_response(200)
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *a):
        return

def run_health():
    HTTPServer(('0.0.0.0', int(os.environ.get('PORT', 10000))),
               HealthCheck).serve_forever()

# ═══════════════════════════════════════════════════════
#  ГЛАВНЫЙ ЦИКЛ
# ═══════════════════════════════════════════════════════
async def sync_positions_with_exchange() -> int:
    """
    Синхронизирует локальный список позиций с реальными на BingX.
    Удаляет ghost positions (закрытые вручную или по SL без уведомления).
    Возвращает количество удалённых позиций.
    """
    global smc_positions, rsi_positions
    removed = 0
    try:
        # Получаем все реально открытые позиции на BingX
        real_positions = await exchange.fetch_positions()
        # Множество символов с реально открытыми позициями
        # Строим set (символ, сторона) — BCH Short ≠ BCH Long!
        real_pos_set: set = set()
        for rp in real_positions:
            qty = abs(float(rp.get('contracts', 0) or 0))
            if qty > 0:
                sym_r  = rp.get('symbol', '')
                side_r = (rp.get('side') or rp.get('info', {}).get('side', '')).lower()
                real_pos_set.add((sym_r, side_r))

        real_syms = {s for s, _ in real_pos_set}
        logging.info(f'🔄 [SYNC] BingX: {len(real_syms)} позиций: {real_syms}')
        logging.info(f'🔄 [SYNC] (sym,side): {real_pos_set}')

        def _is_real(p: dict) -> bool:
            sym_l  = p['symbol']
            side_l = 'long' if p['direction'] == 'Long' else 'short'
            age    = (datetime.now(timezone.utc)
                      - datetime.fromisoformat(p['open_time'])).total_seconds()
            if age < 120:
                return True  # grace period
            # Проверяем точное совпадение символа И стороны
            if (sym_l, side_l) in real_pos_set:
                return True
            # Fallback: только символ (если биржа не вернула side)
            if sym_l in real_syms and not any(s == sym_l for s, _ in real_pos_set if _):
                return True
            return False

        new_smc, new_rsi, new_sa, new_pb = [], [], [], []
        for p in smc_positions:
            if _is_real(p): new_smc.append(p)
            else:
                logging.warning(f'👻 [SYNC] SMC ghost: {p["symbol"]} {p["direction"]}')
                removed += 1
        for p in rsi_positions:
            if _is_real(p): new_rsi.append(p)
            else:
                logging.warning(f'👻 [SYNC] RSI ghost: {p["symbol"]} {p["direction"]}')
                removed += 1
        for p in sa_positions:   # [v24]
            if _is_real(p): new_sa.append(p)
            else:
                logging.warning(f'👻 [SYNC] SA ghost: {p["symbol"]} {p["direction"]}')
                removed += 1
        for p in pb_positions:   # [PB]
            if _is_real(p): new_pb.append(p)
            else:
                logging.warning(f'👻 [SYNC] PB ghost: {p["symbol"]} {p["direction"]}')
                removed += 1

        smc_positions[:] = new_smc
        rsi_positions[:] = new_rsi
        sa_positions[:]  = new_sa   # [v24]
        pb_positions[:]  = new_pb   # [PB]
        save_all()
        logging.info(f'✅ [SYNC] Удалено ghost: {removed} | '
                     f'Осталось: SMC={len(smc_positions)} RSI={len(rsi_positions)}')
    except Exception as e:
        logging.error(f'❌ [SYNC] Ошибка синхронизации: {e}')
    return removed


# ═══════════════════════════════════════════════════════
#  TRADE LOGGER — SQLite + CSV экспорт
# ═══════════════════════════════════════════════════════
TRADES_DB = '/data/trades.db' if os.path.exists('/data') else '/tmp/trades.db'  # [DISK] на /data переживает деплой

def _init_trades_db():
    """Создаёт таблицу trades если не существует."""
    con = sqlite3.connect(TRADES_DB)
    con.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            close_time  TEXT,
            symbol      TEXT,
            strategy    TEXT,
            direction   TEXT,
            close_reason TEXT,
            entry_price REAL,
            exit_price  REAL,
            pnl_pct     REAL,
            net_usdt    REAL,
            mfe_pct     REAL,
            mae_pct     REAL,
            dur_min     INTEGER,
            rsi_val     REAL,
            vol_ratio   REAL,
            sma_dist    REAL,
            vwap_dist   REAL,
            btc_trend   TEXT,
            ai_conf     INTEGER,
            ai_comment  TEXT,
            tp_mult     REAL,
            be_moved    INTEGER,
            tp50_hit    INTEGER
        )
    """)
    # [SHADOW] виртуальные momentum-сигналы (без риска) для расчёта винрейта
    con.execute("""
        CREATE TABLE IF NOT EXISTS shadow_signals (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            open_time   TEXT,
            symbol      TEXT,
            direction   TEXT,
            entry_price REAL,
            sl_price    REAL,
            atr         REAL,
            adx         REAL,
            vol_ratio   REAL,
            alt_score   INTEGER,
            eth_btc     REAL,
            mfe_price   REAL,
            trail_sl    REAL,
            status      TEXT,
            close_time  TEXT,
            exit_price  REAL,
            pnl_pct     REAL,
            bars_held   INTEGER
        )
    """)
    # [PB] миграция: колонка strategy (MOM | PB) для раздельной статистики
    try:
        con.execute("ALTER TABLE shadow_signals ADD COLUMN strategy TEXT DEFAULT 'MOM'")
    except Exception:
        pass  # колонка уже существует
    try:
        con.execute("ALTER TABLE shadow_signals ADD COLUMN entry_rsi REAL DEFAULT 0")
    except Exception:
        pass  # колонка уже существует
    try:
        con.execute("ALTER TABLE shadow_signals ADD COLUMN tp_price REAL DEFAULT 0")
    except Exception:
        pass  # [v19] TP для SA mean-reversion (выход по VWAP)
    # [v16] дополнительные признаки входа для анализа реальных сделок
    for _col in ['adx_val REAL DEFAULT 0',
                 'alt_score INTEGER DEFAULT 0',
                 'entry_hour INTEGER DEFAULT -1',
                 'open_time TEXT DEFAULT ""',
                 'htf_trend TEXT DEFAULT ""',         # [v38] наклон EMA200(15m) на входе
                 'mfe_time_min INTEGER DEFAULT -1']:  # [v38] минута пика MFE
        try:
            con.execute(f'ALTER TABLE trades ADD COLUMN {_col}')
        except Exception:
            pass
    # [EPOCH] таблица meta: время последнего деплоя (для статистики 'Последнее')
    con.execute("CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT)")
    con.commit()
    con.close()

_init_trades_db()


# ═══════════════════════════════════════════════════════
#  ВЕРСИЯ КОДА И ЖУРНАЛ ИЗМЕНЕНИЙ
#  Бампай CODE_VERSION при КАЖДОМ деплое + добавляй строку в CHANGELOG.
#  При смене версии бот сбрасывает метку 'Последнее' и пишет изменения в лог,
#  чтобы видеть эффект каждого деплоя и не повторять прошлых ошибок.
# ═══════════════════════════════════════════════════════
CODE_VERSION = '2026-07-11-v38'
CHANGELOG = [
    ('2026-07-11-v38', 'SA Smart Timeout 75мин/MFE<0.35% (времен., калибр. по mfe_time_min); лог htf_trend+mfe_time_min; SA-отчёт: причина закрытия/час/VWAP-дист/HTF'),
    ('2026-07-04-v37', 'SA vol 1.3-2.0 (n=16); SMC RSI 55-65 + alt<45 [EARLY n<15!]; PB ADX>60 shadow; MOM сканер отключён'),
    ('2026-07-02-v36', 'v34 PNL-фиксы восстановлены (realized_pnl+ROE); MAX_TRADE_MIN_SA=150; Smart Timeout SMC 90мин; ATR→worker payload'),
    ('2026-07-02-v35', 'stats: SA выделена в отдельный блок с SA_HIST_OFFSET=37; SA убрана из shadow; шапка v10→v10+SA'),
    ('2026-06-29-v33', 'execute: SA margin лимит 20%→30%'),
    ('2026-06-29-v32', 'AI oracle: SA правила директивные — MUST APPROVE для RSI экстремумов'),
    ('2026-06-28-v31', 'execute: notional guard учитывает LEVERAGE; SA=20% маржи, SMC/RSI=15% маржи'),
    ('2026-06-28-v30', 'init_db: sa_pos таблица; AI prompt: SA/SMC правила разделены; notional лимит 60%→95%'),
    ('2026-06-27-v29', "SA bugfix: 'price' в sig dict + vol_climax 2.0→1.3 + exception WARNING"),
    ('2026-06-24-v27', 'SA: score_setup_local MR-логика; shadow_record убран из LIVE пути'),
    ('2026-06-23-v25', 'SA LIVE bugfix: sa_positions отдельный список + cooldown вместо notified (BTC всегда в notified)'),
    ('2026-06-22-v24', 'SMC: только Long (Short blocked); PB: ADX>=40 + vol 1.5-3x; SA LIVE активирован; Worker SA-статистика + self-ping'),
    ('2026-06-19-v23', 'SMC hard-filters: RSI 40-65 exhaustion + Short блок при alt_score>=40 (18 сд, PF 2.22)'),
    ('2026-06-13-v20', '/stats_analyze: разбивка причина-закрытия x направление (диагностика)'),
    ('2026-06-12-v19', 'Single-Asset алгоритм: BTC mean-reversion от VWAP (shadow, выход по TP=VWAP)'),
    ('2026-06-12-v18', 'REPORT_HOUR_UTC настраиваемый + стартовый лог расписания отчёта'),
    ('2026-06-11-v17', 'Фикс Итоги дня (триггер hour>=19, персист-гард) + /help + /report'),
    ('2026-06-10-v16.1', 'Фикс NameError btc_ctx в scan_smc (alt_score для реальных сделок)'),
    ('2026-06-10-v16', '/stats_analyze: анализ реальных сделок по ADX/RSI/alt_score/час/объём'),
    ('2026-06-10-v15', 'DATA-DRIVEN: PB vol>=1.5x + alt_score<55; 1h shadow отключён (MOM WR4%/PB WR18%)'),
    ('2026-06-10-v14', 'Фикс /shadow_analyze (HTML 400 ошибка Telegram)'),
    ('2026-06-09-v13', "Кулдаун повторных сигналов (анти-овертрейд) + /shadow_analyze (мина данных по признакам) + entry_rsi"),
    ('2026-06-08-v12', "Split статистики Последнее/Всего (shadow + /stats) + журнал версий"),
    ('2026-06-08-v11', "Дышащий стоп shadow+live MOM (трейл после +1%) + тег TF в /shadow"),
    ('2026-06-07-v10', "Pullback shadow-стратегия + раздельная stats MOM/PB"),
    ('2026-06-07-v9',  "RSI-фильтр входа momentum — ОТКАЧЕН (конфликт с пробоем, обнулял сигналы)"),
    ('2026-06-06-v8',  "Трейл MOM_TRAIL_ATR 3.0 -> 1.5 (env-настройка)"),
    ('2026-06-05-v7',  "Shadow-трекинг momentum: запись + симуляция + /shadow"),
]
# ⚠ НЕ ВОЗВРАЩАТЬ: RSI-фильтр входа momentum (>60/<40) — несовместим с пробоем экстремума.
# ⚠ НЕ МЕНЯТЬ MR/SMC-логику на малой выборке (<30-50 сделок) — переобучение.


def set_deploy_epoch():
    """Сдвигает метку 'Последнее' ТОЛЬКО при смене CODE_VERSION (новый деплой),
    а не на каждом рестарте. Пишет журнал изменений в лог."""
    try:
        con = sqlite3.connect(TRADES_DB)
        r = con.execute("SELECT value FROM meta WHERE key='code_version'").fetchone()
        stored = r[0] if r else None
        if stored != CODE_VERSION:
            now = datetime.now(timezone.utc).isoformat()
            con.execute("INSERT OR REPLACE INTO meta (key,value) VALUES ('deploy_epoch',?)", (now,))
            con.execute("INSERT OR REPLACE INTO meta (key,value) VALUES ('code_version',?)", (CODE_VERSION,))
            con.commit()
            note = next((d for v, d in CHANGELOG if v == CODE_VERSION), '—')
            logging.info(f"🆕 [DEPLOY] {CODE_VERSION}: {note}")
            logging.info(f"📍 [EPOCH] метка 'Последнее' сброшена (предыдущая версия: {stored or 'нет'})")
        else:
            logging.info(f"♻️ [RESTART] {CODE_VERSION} без изменений кода — статистика 'Последнее' сохранена")
        con.close()
    except Exception as _e:
        logging.warning(f'[EPOCH] set fail: {_e}')


def get_deploy_epoch():
    """Возвращает ISO-время последнего деплоя или None."""
    try:
        con = sqlite3.connect(TRADES_DB)
        r = con.execute("SELECT value FROM meta WHERE key='deploy_epoch'").fetchone()
        con.close()
        return r[0] if r else None
    except Exception:
        return None

def log_trade(pos: dict, exit_p: float, pnl_pct: float,
              net_usdt: float, mfe_pct: float, mae_pct: float,
              dur_min: int, close_reason: str):
    """
    Логирует закрытую сделку в SQLite.
    close_reason: 'SL' | 'TP' | 'BE' | 'Timeout' | 'Manual'
    """
    try:
        con = sqlite3.connect(TRADES_DB)
        con.execute("""
            INSERT INTO trades (
                close_time, symbol, strategy, direction, close_reason,
                entry_price, exit_price, pnl_pct, net_usdt,
                mfe_pct, mae_pct, dur_min,
                rsi_val, vol_ratio, sma_dist, vwap_dist, btc_trend,
                ai_conf, ai_comment, tp_mult, be_moved, tp50_hit,
                adx_val, alt_score, entry_hour, open_time,
                htf_trend, mfe_time_min
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M'),
            pos.get('symbol', ''),
            pos.get('strategy', ''),
            pos.get('direction', ''),
            close_reason,
            round(float(pos.get('entry_price', 0)), 8),
            round(exit_p, 8),
            round(pnl_pct, 3),
            round(net_usdt, 4),
            round(mfe_pct, 3),
            round(mae_pct, 3),
            dur_min,
            pos.get('rsi_val', 0),
            pos.get('vol_ratio', 0),
            pos.get('sma_dist', 0),
            pos.get('vwap_dist', 0),
            pos.get('btc_trend', ''),
            pos.get('ai_conf', 0),
            pos.get('ai_comment', ''),
            pos.get('tp_mult', 1.5),
            int(pos.get('be_moved', False)),
            int(pos.get('tp50_hit', False)),
            pos.get('adx_val', 0),
            pos.get('alt_score', 0),
            pos.get('entry_hour', -1),
            pos.get('open_time', ''),
            pos.get('htf_trend', ''),       # [v38]
            pos.get('mfe_time_min', -1),    # [v38]
        ))
        con.commit()
        con.close()
        logging.info(f'📊 [LOG] {pos["symbol"]} {close_reason} {pnl_pct:+.2f}% записана')
    except Exception as e:
        logging.error(f'❌ [LOG] Ошибка записи сделки: {e}')


# ═══════════════════════════════════════════════════════
#  SHADOW TRACKING — виртуальные momentum-сигналы [нулевой риск]
#  Записывает сигнал, симулирует чандельер-трейл как в live,
#  считает виртуальный PnL → реальный винрейт без риска.
# ═══════════════════════════════════════════════════════
def shadow_record(sym, mode, price, msig, btc_ctx, strategy='MOM'):
    """Записывает shadow-сигнал, если по symbol+mode+strategy нет открытого."""
    try:
        con = sqlite3.connect(TRADES_DB)
        cur = con.cursor()
        cur.execute(
            "SELECT 1 FROM shadow_signals WHERE symbol=? AND direction=? "
            "AND strategy=? AND status='open' LIMIT 1",
            (sym, mode, strategy))
        if cur.fetchone():
            con.close(); return  # уже отслеживается — дубль не пишем
        # [COOLDOWN] не пересэмплировать тот же сетап сразу после закрытия
        tf_min = 60 if RSI_TF == '1h' else 15
        cutoff = (datetime.now(timezone.utc)
                  - timedelta(minutes=SHADOW_COOLDOWN_BARS * tf_min)).isoformat()
        cur.execute(
            "SELECT 1 FROM shadow_signals WHERE symbol=? AND direction=? "
            "AND strategy=? AND status='closed' AND close_time > ? LIMIT 1",
            (sym, mode, strategy, cutoff))
        if cur.fetchone():
            con.close(); return  # недавно закрыт — ждём кулдаун
        con.execute(
            "INSERT INTO shadow_signals (open_time,symbol,direction,entry_price,"
            "sl_price,atr,adx,vol_ratio,alt_score,eth_btc,mfe_price,trail_sl,status,strategy,entry_rsi,tp_price) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,'open',?,?,?)",
            (datetime.now(timezone.utc).isoformat(), sym, mode, price,
             float(msig.get('sl', 0)), float(msig.get('atr', 0)),
             float(msig.get('adx', 0)), float(msig.get('vol_ratio', 0)),
             int(btc_ctx.get('alt_score', 50)), float(btc_ctx.get('eth_btc_spread', 0)),
             price, float(msig.get('sl', 0)), strategy, float(msig.get('rsi', 0)),
             float(msig.get('tp', 0))))
        con.commit(); con.close()
    except Exception as _e:
        logging.warning(f'[SHADOW] record fail {sym}: {_e}')


async def shadow_check():
    """Симулирует чандельер-трейл по открытым shadow-сигналам, закрывает виртуально."""
    try:
        con = sqlite3.connect(TRADES_DB)
        cur = con.cursor()
        rows = cur.execute(
            "SELECT id,symbol,direction,entry_price,atr,mfe_price,trail_sl,open_time,"
            "strategy,tp_price,sl_price FROM shadow_signals WHERE status='open'").fetchall()
        con.close()
    except Exception as _e:
        logging.warning(f'[SHADOW] read fail: {_e}')
        return
    if not rows:
        return

    MAX_HOLD_BARS = 100   # таймаут симуляции
    for (sid, sym, mode, entry, atr, mfe_p, trail_sl, open_t, strat, tp_p, sl_p) in rows:
        try:
            ohlcv = await exchange.fetch_ohlcv(sym, RSI_TF, limit=3)
            if not ohlcv:
                continue
            last = ohlcv[-1]
            hi, lo, curr = float(last[2]), float(last[3]), float(last[4])
            is_long = (mode == 'Long')
            atr = atr if atr > 0 else entry * 0.01

            # [SA] mean-reversion: выход по TP (VWAP) или SL, без трейлинга
            if strat == 'SA':
                bars_sa = 0
                try:
                    _ot = datetime.fromisoformat(open_t)
                    tf_m = 60 if RSI_TF == '1h' else 15
                    bars_sa = int((datetime.now(timezone.utc) - _ot).total_seconds()/60/tf_m)
                except Exception:
                    pass
                if is_long:
                    tp_hit = hi >= tp_p
                    sl_hit = lo <= sl_p
                else:
                    tp_hit = lo <= tp_p
                    sl_hit = hi >= sl_p
                sa_timeout = bars_sa >= 40   # mean-rev должен сработать быстро
                con = sqlite3.connect(TRADES_DB)
                if tp_hit or sl_hit or sa_timeout:
                    exit_p = tp_p if tp_hit else (sl_p if sl_hit else curr)
                    pnl = ((exit_p-entry)/entry if is_long else (entry-exit_p)/entry) * 100
                    rsn = 'TP-VWAP' if tp_hit else ('SL' if sl_hit else 'TIMEOUT')
                    con.execute(
                        "UPDATE shadow_signals SET status='closed',close_time=?,exit_price=?,"
                        "pnl_pct=?,bars_held=? WHERE id=?",
                        (datetime.now(timezone.utc).isoformat(), exit_p, round(pnl,3),
                         bars_sa, sid))
                    logging.info(f"👁 [SA CLOSE] {sym} {mode} → {rsn} "
                                 f"PnL: {pnl:+.2f}% ({bars_sa} баров)")
                con.commit(); con.close()
                continue

            # [BREATHING STOP] Трейлинг включается ТОЛЬКО после +1% профита.
            # До этого работает исходный широкий SL — сделка 'дышит',
            # не выбивается шумом свечи входа (фикс '0 баров' закрытий).
            mfe_pct = ((mfe_p - entry)/entry if is_long else (entry - mfe_p)/entry) * 100
            if is_long:
                mfe_p = max(mfe_p, hi)
                mfe_pct = (mfe_p - entry)/entry * 100
                if mfe_pct >= 1.0:   # профит дошёл до +1% → активируем трейл
                    new_trail = mfe_p - atr * MOM_TRAIL_ATR
                    trail_sl = max(trail_sl, new_trail)
                hit = lo <= trail_sl   # до +1% trail_sl = исходный широкий SL
            else:
                mfe_p = min(mfe_p, lo)
                mfe_pct = (entry - mfe_p)/entry * 100
                if mfe_pct >= 1.0:
                    new_trail = mfe_p + atr * MOM_TRAIL_ATR
                    trail_sl = min(trail_sl, new_trail)
                hit = hi >= trail_sl

            # Таймаут по числу баров
            bars = 0
            try:
                _ot = datetime.fromisoformat(open_t)
                tf_min = 60 if RSI_TF == '1h' else 15
                bars = int((datetime.now(timezone.utc) - _ot).total_seconds() / 60 / tf_min)
            except Exception:
                pass
            timeout = bars >= MAX_HOLD_BARS

            con = sqlite3.connect(TRADES_DB)
            if hit or timeout:
                exit_p = trail_sl if hit else curr
                pnl = ((exit_p - entry)/entry if is_long else (entry - exit_p)/entry) * 100
                con.execute(
                    "UPDATE shadow_signals SET status='closed',close_time=?,exit_price=?,"
                    "pnl_pct=?,bars_held=?,mfe_price=?,trail_sl=? WHERE id=?",
                    (datetime.now(timezone.utc).isoformat(), exit_p, round(pnl, 3),
                     bars, mfe_p, trail_sl, sid))
                logging.info(f"👁 [SHADOW CLOSE] {sym} {mode} → {'TRAIL' if hit else 'TIMEOUT'} "
                             f"PnL: {pnl:+.2f}% ({bars} баров)")
            else:
                con.execute("UPDATE shadow_signals SET mfe_price=?,trail_sl=? WHERE id=?",
                            (mfe_p, trail_sl, sid))
            con.commit(); con.close()
        except Exception as _e:
            logging.debug(f'[SHADOW] check {sym}: {_e}')


def _bucket_stats(rows):
    """rows = [(pnl,)] → (n, wr, avg, pf)."""
    n = len(rows)
    if n == 0:
        return (0, 0, 0, 0)
    wins = sum(1 for r in rows if r[0] > 0)
    wr = wins / n * 100
    avg = sum(r[0] for r in rows) / n
    gw = sum(r[0] for r in rows if r[0] > 0)
    gl = abs(sum(r[0] for r in rows if r[0] < 0))
    pf = (gw / gl) if gl > 0 else 0
    return (n, wr, avg, pf)


def _analyze_feature(con, strategy, col, buckets):
    """Разбивка closed-сделок стратегии по диапазонам признака col.
    buckets = [(label, lo, hi)]. Возвращает строки отчёта."""
    out = []
    for label, lo, hi in buckets:
        rows = con.execute(
            f"SELECT pnl_pct FROM shadow_signals WHERE status='closed' "
            f"AND strategy=? AND {col} >= ? AND {col} < ?",
            (strategy, lo, hi)).fetchall()
        n, wr, avg, pf = _bucket_stats(rows)
        if n == 0:
            continue
        flag = ' ⭐' if (pf > 1.0 and n >= 15) else ''
        out.append(f"   {label}: {n} сд | WR {wr:.0f}% | Avg {avg:+.2f}% | PF {pf:.2f}{flag}")
    return out


def shadow_analyze() -> str:
    """Мина данных: ищет, какие условия отделяют победителей.
    ⭐ = PF>1 при выборке >=15 (кандидат в фильтр; проверять форвардом)."""
    def _fmt(label, rows):
        n, wr, avg, pf = _bucket_stats(rows)
        if n == 0:
            return None
        flag = ' ⭐' if (pf > 1.0 and n >= 15) else ''
        # escape label для Telegram HTML — заменяем < на &lt; > на &gt;
        safe = label.replace('<', '&lt;').replace('>', '&gt;')
        return f"  {safe}: {n} сд | WR {wr:.0f}% | Avg {avg:+.2f}% | PF {pf:.2f}{flag}"

    def _feature(con, strat, col, buckets):
        out = []
        for label, lo, hi in buckets:
            rows = con.execute(
                f"SELECT pnl_pct FROM shadow_signals WHERE status='closed' "
                f"AND strategy=? AND {col} >= ? AND {col} < ?",
                (strat, lo, hi)).fetchall()
            line = _fmt(label, rows)
            if line:
                out.append(line)
        return out

    try:
        con = sqlite3.connect(TRADES_DB)
        parts = [f'🔬 Анализ {RSI_TF} (closed shadow)']
        # [v37] MOM отключён; SA в live — в shadow остался только PB
        for strat, emoji in [('PB', '🎯')]:
            total = con.execute(
                "SELECT COUNT(*) FROM shadow_signals WHERE status='closed' AND strategy=?",
                (strat,)).fetchone()[0]
            if total == 0:
                continue
            parts.append(f'\n{emoji} {strat} (всего {total})')
            for d in ('Long', 'Short'):
                rows = con.execute(
                    "SELECT pnl_pct FROM shadow_signals WHERE status='closed' "
                    "AND strategy=? AND direction=?", (strat, d)).fetchall()
                line = _fmt(d, rows)
                if line:
                    parts.append(line)
            parts.append('  ADX:')
            parts += _feature(con, strat, 'adx',
                [('25-40', 25, 40), ('40-60', 40, 60), ('60+', 60, 999)])
            parts.append('  Vol:')
            parts += _feature(con, strat, 'vol_ratio',
                [('1.0-1.5x', 1.0, 1.5), ('1.5-3x', 1.5, 3.0), ('3x+', 3.0, 99)])
            parts.append('  Alt-score:')
            parts += _feature(con, strat, 'alt_score',
                [('lt40', 0, 40), ('40-55', 40, 55), ('55+', 55, 999)])
            parts.append('  Entry RSI:')
            parts += _feature(con, strat, 'entry_rsi',
                [('20-40', 20, 40), ('40-60', 40, 60), ('60-80', 60, 80)])
        con.close()
    except Exception as _e:
        return f'[ANALYZE] fail: {_e}'
    parts.append('\n⭐ = PF&gt;1 при n&gt;=15 (кандидат в фильтр)')
    return '\n'.join(parts)


def shadow_reset() -> str:
    """Очищает таблицу shadow_signals для чистого замера."""
    try:
        con = sqlite3.connect(TRADES_DB)
        n = con.execute('SELECT COUNT(*) FROM shadow_signals').fetchone()[0]
        con.execute('DELETE FROM shadow_signals')
        con.commit(); con.close()
        return f'🗑 Shadow-таблица очищена ({n} записей удалено). Замер с нуля.'
    except Exception as _e:
        return f'[SHADOW] reset fail: {_e}'


def _shadow_block(rows):
    """Формирует строку статистики по списку закрытых сделок."""
    if not rows:
        return 'нет сделок'
    n = len(rows)
    wins = sum(1 for r in rows if r[0] > 0)
    wr = wins / n * 100
    avg = sum(r[0] for r in rows) / n
    gw = sum(r[0] for r in rows if r[0] > 0)
    gl = abs(sum(r[0] for r in rows if r[0] < 0))
    pf = (gw / gl) if gl > 0 else 0
    return (f'{n} сделок | WR {wr:.1f}% ({wins}W/{n-wins}L) | '
            f'Avg {avg:+.2f}% | PF {pf:.2f}')


def _shadow_strat(con, strat, epoch):
    """Возвращает (rows_last, rows_all, n_open) по стратегии."""
    rows_all = con.execute(
        "SELECT pnl_pct FROM shadow_signals WHERE status='closed' AND strategy=?",
        (strat,)).fetchall()
    if epoch:
        rows_last = con.execute(
            "SELECT pnl_pct FROM shadow_signals WHERE status='closed' "
            "AND strategy=? AND close_time > ?", (strat, epoch)).fetchall()
    else:
        rows_last = []
    n_open = con.execute(
        "SELECT COUNT(*) FROM shadow_signals WHERE status='open' AND strategy=?",
        (strat,)).fetchone()[0]
    return rows_last, rows_all, n_open


def shadow_stats() -> str:
    """Сводка shadow: 'Последнее' (с деплоя) + 'Всего'.
    [v37] MOM отключён, SA в live — в shadow остался только PB."""
    try:
        epoch = get_deploy_epoch()
        con = sqlite3.connect(TRADES_DB)
        p_last, p_all, p_open = _shadow_strat(con, 'PB', epoch)
        con.close()
    except Exception as _e:
        return f'[SHADOW] stats fail: {_e}'
    return (
        f'👁 <b>Shadow 15м (виртуально, без риска)</b>\n'
        f'\n🎯 <b>Pullback (откат)</b> | открыто: {p_open}\n'
        f'  Последнее: {_shadow_block(p_last)}\n'
        f'  Всего: {_shadow_block(p_all)}')


def export_trades_csv() -> str:
    """Экспортирует все сделки из SQLite в CSV строку."""
    try:
        con = sqlite3.connect(TRADES_DB)
        cur = con.execute('SELECT * FROM trades ORDER BY close_time DESC LIMIT 500')
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        con.close()
        if not rows:
            return 'Нет данных. Бот ещё не закрыл ни одной сделки.'
        lines = [','.join(cols)]
        for row in rows:
            lines.append(','.join(str(v) for v in row))
        return '\n'.join(lines)
    except Exception as e:
        return f'Ошибка: {e}'

def _trades_row(con, since=None):
    """Агрегаты по trades; since=ISO время → только сделки после него."""
    where = "WHERE close_time > ?" if since else ""
    args = (since,) if since else ()
    return con.execute(f"""
        SELECT
            COUNT(*) total,
            -- [v36] порог {0.5 * LEVERAGE}: pnl_pct теперь ROE. Старые записи (до v36)
            -- хранят % без плеча — их wins могут занижаться, форвард-данные корректны.
            SUM(CASE WHEN pnl_pct >= {0.5 * LEVERAGE} OR tp50_hit=1 THEN 1 ELSE 0 END) wins,
            ROUND(AVG(pnl_pct),2) avg_pnl,
            ROUND(AVG(mfe_pct),2) avg_mfe,
            ROUND(AVG(mae_pct),2) avg_mae,
            SUM(CASE WHEN strategy='SMC' THEN 1 ELSE 0 END) smc_cnt,
            SUM(CASE WHEN strategy='RSI' THEN 1 ELSE 0 END) rsi_cnt,
            SUM(CASE WHEN close_reason='Timeout' THEN 1 ELSE 0 END) timeout_cnt
        FROM trades {where}
    """, args).fetchone()


def _trades_line(r):
    """Форматирует строку из агрегата _trades_row."""
    if not r or r[0] == 0:
        return 'нет сделок'
    total, wins = r[0], r[1]
    wr = wins/total*100 if total else 0
    return (f'{total} сделок | WR {wr:.1f}% ({wins}/{total}) | '
            f'Avg {r[2]:+.2f}% | MFE {r[3]:.2f}% | '
            f'SMC:{r[5]} RSI:{r[6]} TO:{r[7]}')


# ═══════════════════════════════════════════════════════
#  АНАЛИЗАТОР РЕАЛЬНЫХ СДЕЛОК SMC/RSI
#  Та же логика что /shadow_analyze, но по реальным trades.
#  ⭐ = PF>1 при n>=10 — с реальными сделками порог ниже.
#  ВАЖНО: выводы делать только при n>=30 по каждому сегменту.
#  При n<30 — ориентировочно, не менять фильтры.
# ═══════════════════════════════════════════════════════
def _trades_bucket(con, col, lo, hi, strategy=None, since=None):
    """Статистика по диапазону признака в реальных trades."""
    where = f"WHERE {col} >= ? AND {col} < ?"
    args = [lo, hi]
    if strategy:
        where += " AND strategy = ?"
        args.append(strategy)
    if since:
        where += " AND close_time > ?"
        args.append(since)
    rows = con.execute(
        f"SELECT pnl_pct FROM trades {where}", args).fetchall()
    return _bucket_stats(rows)


def _trades_bucket_strat(con, strategies: tuple, col, lo, hi):
    """Статистика по диапазону признака для набора стратегий (tuple)."""
    placeholders = ','.join('?' * len(strategies))
    rows = con.execute(
        f"SELECT pnl_pct FROM trades "
        f"WHERE strategy IN ({placeholders}) AND {col} >= ? AND {col} < ?",
        (*strategies, lo, hi)).fetchall()
    return _bucket_stats(rows)


def stats_analyze() -> str:
    """Анализ реальных сделок по признакам входа.
    Блок 1: SMC + RSI. Блок 2: SA (live + SA_HIST_OFFSET исторических)."""
    try:
        epoch = get_deploy_epoch()
        con = sqlite3.connect(TRADES_DB)

        # ── БЛОК 1: SMC / RSI ────────────────────────────────────────
        total_smc_rsi = con.execute(
            "SELECT COUNT(*) FROM trades WHERE strategy IN ('SMC','RSI')"
        ).fetchone()[0]
        total_last_smc_rsi = con.execute(
            "SELECT COUNT(*) FROM trades WHERE strategy IN ('SMC','RSI') AND close_time > ?",
            (epoch,)).fetchone()[0] if epoch else 0

        lines = [f'🔬 <b>Анализ реальных сделок SMC/RSI</b>']
        lines.append(f'Всего: {total_smc_rsi} | С деплоя: {total_last_smc_rsi}')
        if total_smc_rsi < 15:
            lines.append('\n⚠️ Мало данных (нужно 30+ по сегменту).')
            lines.append('Признаки уже копятся — анализ улучшится.')
        else:
            lines.append('')
            for strat in ('SMC', 'RSI'):
                rows = con.execute(
                    "SELECT pnl_pct FROM trades WHERE strategy=?",
                    (strat,)).fetchall()
                n, wr, avg, pf = _bucket_stats(rows)
                if n == 0:
                    continue
                flag = ' ⭐' if (pf > 1.0 and n >= 10) else ''
                lines.append(f'<b>{strat}</b>: {n} сд | WR {wr:.0f}% | Avg {avg:+.2f}% | PF {pf:.2f}{flag}')

            lines.append('\n<b>Направление:</b>')
            for d in ('Long', 'Short'):
                rows = con.execute(
                    "SELECT pnl_pct FROM trades WHERE strategy IN ('SMC','RSI') AND direction=?",
                    (d,)).fetchall()
                n, wr, avg, pf = _bucket_stats(rows)
                if n:
                    flag = ' ⭐' if (pf > 1.0 and n >= 10) else ''
                    lines.append(f'  {d}: {n} сд | WR {wr:.0f}% | Avg {avg:+.2f}% | PF {pf:.2f}{flag}')

            lines.append('\n<b>Причина закрытия:</b>')
            for reason in ('SL', 'TP', 'BE', 'Timeout'):
                rows = con.execute(
                    "SELECT pnl_pct FROM trades WHERE strategy IN ('SMC','RSI') AND close_reason=?",
                    (reason,)).fetchall()
                n, wr, avg, pf = _bucket_stats(rows)
                if n:
                    lines.append(f'  {reason}: {n} сд | Avg {avg:+.2f}%')

            lines.append('\n<b>Причина × Направление:</b>')
            for d in ('Long', 'Short'):
                for reason in ('SL', 'TP', 'BE', 'Timeout'):
                    rows = con.execute(
                        "SELECT pnl_pct FROM trades WHERE strategy IN ('SMC','RSI') "
                        "AND direction=? AND close_reason=?",
                        (d, reason)).fetchall()
                    n, wr, avg, pf = _bucket_stats(rows)
                    if n:
                        lines.append(f'  {d}+{reason}: {n} сд | Avg {avg:+.2f}%')

            def _sr_bucket(col, lo, hi):
                return _trades_bucket_strat(con, ('SMC', 'RSI'), col, lo, hi)

            lines.append('\n<b>ADX входа:</b>')
            for lbl, lo, hi in [('25-40', 25, 40), ('40-60', 40, 60), ('60+', 60, 999)]:
                n, wr, avg, pf = _sr_bucket('adx_val', lo, hi)
                if n:
                    flag = ' ⭐' if (pf > 1.0 and n >= 10) else ''
                    lines.append(f'  {lbl}: {n} сд | WR {wr:.0f}% | Avg {avg:+.2f}% | PF {pf:.2f}{flag}')

            lines.append('\n<b>RSI входа:</b>')
            for lbl, lo, hi in [('lt40', 0, 40), ('40-55', 40, 55), ('55-65', 55, 65), ('65+', 65, 100)]:
                safe = lbl.replace('<', '&lt;')
                n, wr, avg, pf = _sr_bucket('rsi_val', lo, hi)
                if n:
                    flag = ' ⭐' if (pf > 1.0 and n >= 10) else ''
                    lines.append(f'  {safe}: {n} сд | WR {wr:.0f}% | Avg {avg:+.2f}% | PF {pf:.2f}{flag}')

            lines.append('\n<b>Alt-score:</b>')
            for lbl, lo, hi in [('lt40', 0, 40), ('40-55', 40, 55), ('55+', 55, 999)]:
                safe = lbl.replace('<', '&lt;')
                n, wr, avg, pf = _sr_bucket('alt_score', lo, hi)
                if n:
                    flag = ' ⭐' if (pf > 1.0 and n >= 10) else ''
                    lines.append(f'  {safe}: {n} сд | WR {wr:.0f}% | Avg {avg:+.2f}% | PF {pf:.2f}{flag}')

            lines.append('\n<b>Час входа (UTC):</b>')
            for lbl, lo, hi in [('06-10', 6, 10), ('10-14', 10, 14),
                                 ('14-17', 14, 17), ('17-22', 17, 22)]:
                n, wr, avg, pf = _sr_bucket('entry_hour', lo, hi)
                if n:
                    flag = ' ⭐' if (pf > 1.0 and n >= 10) else ''
                    lines.append(f'  {lbl}h: {n} сд | WR {wr:.0f}% | Avg {avg:+.2f}% | PF {pf:.2f}{flag}')

            lines.append('\n<b>Объём:</b>')
            for lbl, lo, hi in [('1-2x', 1.0, 2.0), ('2-4x', 2.0, 4.0), ('4x+', 4.0, 99)]:
                n, wr, avg, pf = _sr_bucket('vol_ratio', lo, hi)
                if n:
                    flag = ' ⭐' if (pf > 1.0 and n >= 10) else ''
                    lines.append(f'  {lbl}: {n} сд | WR {wr:.0f}% | Avg {avg:+.2f}% | PF {pf:.2f}{flag}')

        lines.append('\n⭐ = PF&gt;1 при n&gt;=10 | Доверять при n&gt;=30')

        # ── БЛОК 2: SA (live-сделки + SA_HIST_OFFSET) ────────────────
        sa_new = con.execute(
            "SELECT pnl_pct, direction, rsi_val, alt_score, vol_ratio, "
            "close_reason, entry_hour, vwap_dist, mfe_pct, dur_min, "
            "htf_trend, mfe_time_min FROM trades "
            "WHERE strategy='SA'"
        ).fetchall()
        sa_deploy = con.execute(
            "SELECT COUNT(*) FROM trades WHERE strategy='SA' AND close_time > ?",
            (epoch,)).fetchone()[0] if epoch else len(sa_new)

        lines.append(f'\n\n📈 <b>Анализ реальных сделок SA</b>')
        sa_total = SA_HIST_OFFSET + len(sa_new)
        lines.append(f'Всего: {sa_total} | С деплоя: {sa_deploy}')
        lines.append(f'(включает {SA_HIST_OFFSET} исторических shadow + {len(sa_new)} live)')

        if len(sa_new) >= 5:
            n_sa, wr_sa, avg_sa, pf_sa = _bucket_stats([(r[0],) for r in sa_new])
            flag_sa = ' ⭐' if (pf_sa > 1.0 and n_sa >= 10) else ''
            lines.append(f'\nLive: {n_sa} сд | WR {wr_sa:.0f}% | Avg {avg_sa:+.2f}% | PF {pf_sa:.2f}{flag_sa}')

            lines.append('\n<b>Направление:</b>')
            for d in ('Long', 'Short'):
                rows_d = [(r[0],) for r in sa_new if r[1] == d]
                n, wr, avg, pf = _bucket_stats(rows_d)
                if n:
                    flag = ' ⭐' if (pf > 1.0 and n >= 10) else ''
                    lines.append(f'  {d}: {n} сд | WR {wr:.0f}% | Avg {avg:+.2f}% | PF {pf:.2f}{flag}')

            lines.append('\n<b>Entry RSI:</b>')
            for lbl, lo, hi in [('lt35', 0, 35), ('35-50', 35, 50), ('50-65', 50, 65), ('65+', 65, 100)]:
                rows_r = [(r[0],) for r in sa_new if lo <= (r[2] or 50) < hi]
                n, wr, avg, pf = _bucket_stats(rows_r)
                if n:
                    flag = ' ⭐' if (pf > 1.0 and n >= 10) else ''
                    lines.append(f'  {lbl}: {n} сд | WR {wr:.0f}% | Avg {avg:+.2f}% | PF {pf:.2f}{flag}')

            lines.append('\n<b>Alt-score:</b>')
            for lbl, lo, hi in [('lt40', 0, 40), ('40-55', 40, 55), ('55+', 55, 999)]:
                rows_a = [(r[0],) for r in sa_new if lo <= (r[3] or 0) < hi]
                n, wr, avg, pf = _bucket_stats(rows_a)
                if n:
                    flag = ' ⭐' if (pf > 1.0 and n >= 10) else ''
                    lines.append(f'  {lbl}: {n} сд | WR {wr:.0f}% | Avg {avg:+.2f}% | PF {pf:.2f}{flag}')

            lines.append('\n<b>Объём:</b>')
            for lbl, lo, hi in [('1.3-2x', 1.3, 2.0), ('2-4x', 2.0, 4.0), ('4x+', 4.0, 99)]:
                rows_v = [(r[0],) for r in sa_new if lo <= (r[4] or 0) < hi]
                n, wr, avg, pf = _bucket_stats(rows_v)
                if n:
                    flag = ' ⭐' if (pf > 1.0 and n >= 10) else ''
                    lines.append(f'  {lbl}: {n} сд | WR {wr:.0f}% | Avg {avg:+.2f}% | PF {pf:.2f}{flag}')

            # [v38] Причина закрытия: avg MFE + длительность = диагностика висяков.
            # MFE таймаутов >=0.5% → проблема выхода; ~0.1-0.2% → проблема входа.
            lines.append('\n<b>Причина закрытия (avg MFE | мин):</b>')
            for reas in ('TP', 'WIN', 'BE', 'SL', 'Timeout', 'Manual'):
                rows_c = [r for r in sa_new if r[5] == reas]
                if rows_c:
                    _n = len(rows_c)
                    _avg = sum(x[0] for x in rows_c) / _n
                    _mfe = sum((x[8] or 0) for x in rows_c) / _n
                    _dur = sum((x[9] or 0) for x in rows_c) / _n
                    lines.append(f'  {reas}: {_n} сд | Avg {_avg:+.2f}% | '
                                 f'MFE {_mfe:.2f}% | {_dur:.0f}мин')

            # [v38] Причина × Направление — где именно вязнут шорты
            lines.append('\n<b>Причина × Направление:</b>')
            for d in ('Long', 'Short'):
                for reas in ('TP', 'WIN', 'BE', 'SL', 'Timeout'):
                    rows_cd = [r for r in sa_new if r[5] == reas and r[1] == d]
                    if rows_cd:
                        _n = len(rows_cd)
                        _avg = sum(x[0] for x in rows_cd) / _n
                        _mfe = sum((x[8] or 0) for x in rows_cd) / _n
                        lines.append(f'  {d}+{reas}: {_n} сд | Avg {_avg:+.2f}% | MFE {_mfe:.2f}%')

            # [v38] Час входа UTC (сессии: Азия / EU / US / вечер)
            lines.append('\n<b>Час входа (UTC):</b>')
            for lbl, lo, hi in [('00-06h', 0, 6), ('06-12h', 6, 12),
                                 ('12-18h', 12, 18), ('18-24h', 18, 24)]:
                rows_h = [(r[0],) for r in sa_new
                          if r[6] is not None and lo <= r[6] < hi]
                n, wr, avg, pf = _bucket_stats(rows_h)
                if n:
                    flag = ' ⭐' if (pf > 1.0 and n >= 10) else ''
                    lines.append(f'  {lbl}: {n} сд | WR {wr:.0f}% | Avg {avg:+.2f}% | PF {pf:.2f}{flag}')

            # [v38] |VWAP-дист| в ATR на входе: не слишком ли близко заходим
            lines.append('\n<b>VWAP-дист (ATR):</b>')
            for lbl, lo, hi in [('1.5-1.8', 1.5, 1.8), ('1.8-2.2', 1.8, 2.2), ('2.2+', 2.2, 99)]:
                rows_vd = [(r[0],) for r in sa_new if lo <= abs(r[7] or 0) < hi]
                n, wr, avg, pf = _bucket_stats(rows_vd)
                if n:
                    flag = ' ⭐' if (pf > 1.0 and n >= 10) else ''
                    lines.append(f'  {lbl}: {n} сд | WR {wr:.0f}% | Avg {avg:+.2f}% | PF {pf:.2f}{flag}')

            # [v38] HTF-наклон EMA200 (копится с v38, старые сделки = пусто)
            _htf_rows = [r for r in sa_new if r[10]]
            if _htf_rows:
                lines.append('\n<b>HTF-наклон × Направление:</b>')
                for d in ('Long', 'Short'):
                    for hs in ('Up', 'Flat', 'Down'):
                        rows_hd = [(r[0],) for r in _htf_rows if r[1] == d and r[10] == hs]
                        n, wr, avg, pf = _bucket_stats(rows_hd)
                        if n:
                            lines.append(f'  {d}+{hs}: {n} сд | WR {wr:.0f}% | '
                                         f'Avg {avg:+.2f}% | PF {pf:.2f}')
        else:
            lines.append(f'⚠️ Мало live-данных ({len(sa_new)} сд). Нужно 5+ для анализа.')

        lines.append('\n⭐ = PF&gt;1 при n&gt;=10 | Доверять при n&gt;=30')
        con.close()
    except Exception as _e:
        return f'[STATS_ANALYZE] fail: {_e}'

    return '\n'.join(lines)



def get_trades_stats() -> str:
    """Статистика /stats: 'Последнее' (с деплоя) + 'Всего'."""
    try:
        epoch = get_deploy_epoch()
        con = sqlite3.connect(TRADES_DB)
        r_all = _trades_row(con)
        r_last = _trades_row(con, epoch) if epoch else None
        con.close()
    except Exception as e:
        return f'Ошибка: {e}'
    if not r_all or r_all[0] == 0:
        return '📊 Нет данных по реальным сделкам'
    return (
        f'📊 <b>Статистика реальных сделок (SMC/RSI)</b>\n'
        f'Последнее: {_trades_line(r_last)}\n'
        f'Всего: {_trades_line(r_all)}'
    )


async def check_tg_commands():
    """
    Обработка команд управления из Telegram группы.
    Работает ВСЕГДА — даже при circuit_open=True.
    Команды: /reset  /status  /stop
    """
    global circuit_open, daily_stats, _tg_offset
    if not TOKEN or CHAT_ID == -1 or not http:
        return
    try:
        params = {'offset': _tg_offset, 'limit': 10, 'timeout': 0}
        async with http.get(
            f'https://api.telegram.org/bot{TOKEN}/getUpdates',
            params=params,
            timeout=aiohttp.ClientTimeout(total=5)
        ) as resp:
            if resp.status != 200:
                return
            updates = (await resp.json()).get('result', [])

        for upd in updates:
            _tg_offset = upd['update_id'] + 1
            msg  = upd.get('message') or upd.get('edited_message', {})
            if not msg:
                continue
            text = (msg.get('text') or '').strip()
            chat = msg.get('chat', {}).get('id')

            # Принимаем из нашей группы (int или str сравнение)
            if str(chat) != str(CHAT_ID):
                continue

            cmd = text.lower().split('@')[0].strip()
            logging.info(f'📨 [CMD] команда: {repr(cmd)} от chat={chat}')

            if cmd == '/reset':
                circuit_open = False
                daily_stats['pnl_pct'] = 0.0
                daily_stats['be_closes'] = 0
                save_all()
                logging.info('✅ [CMD] /reset выполнен')
                await tg(
                    f'✅ <b>Circuit Breaker сброшен</b>\n'
                    f'DD обнулён. Торговля возобновлена.\n'
                    f'Лимит: {DAILY_DD_LIMIT*100:.1f}%/день'
                )

            elif cmd == '/status':
                all_pos = all_positions()
                status_cb = '🔴 СТОП' if circuit_open else '🟢 OK'
                await tg(
                    f'📊 <b>Статус</b>\n'
                    f'Circuit: {status_cb}\n'
                    f'DD сегодня: {daily_stats["pnl_pct"]*100:+.2f}% '
                    f'(лимит: {DAILY_DD_LIMIT*100:.1f}%)\n'
                    f'Позиций: {len(all_pos)} '
                    f'(SMC:{len(smc_positions)} RSI:{len(rsi_positions)})\n'
                    f'Баланс: проверьте на бирже\n'
                    f'БД сделок: /stats | CSV: /csv'
                )

            elif cmd == '/stop':
                circuit_open = True
                logging.info('⏸ [CMD] /stop — торговля остановлена вручную')
                await tg('⏸ Торговля остановлена. /reset для возобновления.')

            elif cmd == '/csv':
                # Экспорт всех сделок в CSV для анализа
                csv_data = export_trades_csv()
                # Telegram лимит 4096 символов — отправляем частями
                if len(csv_data) <= 4000:
                    await tg(f'📊 <b>Все сделки (CSV):</b>\n<code>{csv_data}</code>')
                else:
                    # Первые 30 строк
                    lines_csv = csv_data.split('\n')
                    header = lines_csv[0]
                    recent = '\n'.join([header] + lines_csv[1:31])
                    await tg(
                        f'📊 <b>Последние 30 сделок (CSV):</b>\n'
                        f'<code>{recent}</code>\n'
                        f'Всего строк: {len(lines_csv)-1}'
                    )

            elif cmd == '/stats':
                stats_text = get_trades_stats()
                await tg(stats_text)

            elif cmd == '/stats_analyze':
                await tg(stats_analyze())

            elif cmd == '/report':
                # ручной вызов итогов дня (сбрасываем гард чтобы отправить сейчас)
                global _daily_report_sent
                _daily_report_sent = False
                try:
                    _con = sqlite3.connect(TRADES_DB)
                    _con.execute("DELETE FROM meta WHERE key='last_report_date'")
                    _con.commit(); _con.close()
                except Exception:
                    pass
                await send_daily_report()

            elif cmd in ('/help', '/commands'):
                await tg(
                    '🤖 <b>Команды бота</b>\n\n'
                    '<b>Статистика реальных сделок:</b>\n'
                    '/stats — итоги SMC/RSI (Последнее/Всего)\n'
                    '/stats_analyze — анализ по признакам (ADX/RSI/час/объём)\n'
                    '/report — прислать Итоги дня сейчас\n'
                    '/status — открытые позиции и состояние\n'
                    '/csv — экспорт всех сделок\n\n'
                    '<b>Shadow (виртуальные стратегии):</b>\n'
                    '/shadow — статистика Momentum/Pullback\n'
                    '/shadow_analyze — мина данных по признакам\n'
                    '/shadow_reset — очистить shadow-статистику\n\n'
                    '<b>Управление:</b>\n'
                    '/sync — синхронизация позиций с биржей\n'
                    '/reset — сброс дневной статистики\n'
                    '/stop — остановка торговли\n'
                    '/help — этот список'
                )

            elif cmd == '/shadow':
                await tg(shadow_stats())

            elif cmd == '/shadow_reset':
                await tg(shadow_reset())

            elif cmd == '/shadow_analyze':
                await tg(shadow_analyze())

            elif cmd == '/sync':
                # Синхронизация локального списка позиций с реальными на BingX
                # Используется когда позиция закрыта вручную на бирже
                await tg('🔄 Синхронизация позиций с BingX...')
                removed = await sync_positions_with_exchange()
                all_pos = all_positions()
                await tg(
                    f'✅ <b>Синхронизация завершена</b>\n'
                    f'Удалено ghost позиций: {removed}\n'
                    f'Актуальных позиций: {len(all_pos)} '
                    f'(SMC:{len(smc_positions)} RSI:{len(rsi_positions)})'
                )

    except asyncio.TimeoutError:
        pass  # таймаут — не критично
    except Exception as e:
        logging.warning(f'⚠️ [CMD] check_tg_commands error: {e}')


async def main():
    global http

    init_db()
    load_all()
    set_deploy_epoch()   # [EPOCH] метка времени деплоя для статистики 'Последнее'
    logging.info(f'⏰ [REPORT] Итоги дня в {REPORT_HOUR_UTC}:00 UTC ежедневно')
    http = aiohttp.ClientSession()

    Thread(target=run_health, daemon=True).start()
    await refresh_news()

    # [P-4] Диагностика env-переменных при старте
    logging.info("=" * 60)
    logging.info(f"🔑 TELEGRAM_TOKEN:  {'✅ задан' if TOKEN else '❌ НЕ ЗАДАН'}")
    logging.info(
        f"🔑 GROUP_CHAT_ID:   "
        + (f'✅ {CHAT_ID}' if CHAT_ID != -1 else
           '❌ НЕ ЗАДАН (= -1) → установите в Render Environment Variables')
    )
    if CHAT_ID != -1 and not str(abs(CHAT_ID)).startswith('100'):
        logging.warning(
            f"⚠️ [TG] CHAT_ID={CHAT_ID} может быть неверным!\n"
            "    Supergroup IDs начинаются с -100XXXXXXXXXX\n"
            "    Возможно нужно: -100" + str(abs(CHAT_ID))
        )
    logging.info(f"🔑 BINGX_API_KEY:   {'✅ задан' if BINGX_KEY else '❌ НЕ ЗАДАН'}")
    logging.info(f"🔑 BINGX_SECRET:    {'✅ задан' if BINGX_SECRET else '❌ НЕ ЗАДАН'}")
    logging.info(f"🔑 GEMINI_API_KEY:  {'✅ задан' if GEMINI_KEY else '⚠️ не задан (AI oracle выключен)'}")
    logging.info("=" * 60)

    # Инициализация баланса
    try:
        bal = await exchange.fetch_balance()
        if daily_stats['start_balance'] == 0.0:
            daily_stats['start_balance'] = float(bal.get('USDT', {}).get('total', 0))
    except:
        pass

    await tg(
        f"🟢 <b>Unified SMC+RSI+SA Bot v10.0 PROP</b> запущен\n"
        f"[SMC] Smart Money Concepts | [RSI] Mean Reversion | [SA] BTC VWAP MR\n"
        f"Риск: {RISK_PER_TRADE*100:.2f}%/сделку  "
        f"Max поз: {MAX_TOTAL_POS}  Плечо: {LEVERAGE}x\n"
        f"Сессия: 06:30–17:00 UTC (Киев 09:30–20:00)\n"
        f"Circuit breaker: при DD >{DAILY_DD_LIMIT*100:.1f}%/день"
    )
    logging.info("🚀 Unified SMC+RSI+SA Bot v10.0 started")

    cycle = 0
    try:
        while True:
            cycle += 1
            t0 = time.time()
            try:
                await daily_reset()

                # Команды Telegram — вызывается ВСЕГДА (включая circuit breaker)
                await check_tg_commands()

                # Обновление новостей каждые 6 часов
                if time.time() - news_ts > 21600:
                    await refresh_news()

                # Очистка cooldown-кэша (4 часа)
                now_t = time.time()
                expired = [k for k, v in list(notified.items()) if now_t - v > 14400]
                for k in expired:
                    del notified[k]

                # Чистка кэша Gemini (устаревшие записи)
                gem_expired = [k for k, (ts, _) in list(_gemini_cache.items())
                               if now_t - ts > GEMINI_CACHE_TTL]
                for k in gem_expired:
                    del _gemini_cache[k]

                # Чистка кэша объёмов
                vol_expired = [k for k, (ts, _) in list(_vol_cache.items())
                               if now_t - ts > VOL_CACHE_TTL]
                for k in vol_expired:
                    del _vol_cache[k]

                # Итоги дня в 22:00 Киев (19:00 UTC)
                _now_utc = datetime.now(timezone.utc)
                # [v17] hour>=19 вместо узкого окна minute<2 — цикл ~5мин
                # перепрыгивал 2-минутное окно и отчёт терялся. Персист-гард
                # в send_daily_report не даёт продублировать.
                if _now_utc.hour >= REPORT_HOUR_UTC:
                    await send_daily_report()

                # Мониторинг позиций
                await monitor_all()

                # [SHADOW] симуляция momentum-сигналов (виртуально)
                if MOMENTUM_ENABLED:
                    await shadow_check()

                # [SA] single-asset BTC mean-reversion (SA_LIVE = реальная торговля)
                if SA_ENABLED:
                    try:
                        _sa_ctx = await get_btc_context()
                        _sasig, _sa_reason = await single_asset_signal(_sa_ctx)
                        if _sasig:
                            _sa_live_str = 'LIVE' if SA_LIVE else 'SHADOW'
                            logging.info(
                                f"📈 [SA {_sa_live_str}] {SA_SYMBOL.split('/')[0]} {_sasig['mode']} "
                                f"@ {_sasig['rsi']:.0f}rsi VWAP-dist:{_sasig['vwap_dist']:+.1f}ATR "
                                f"Vol:{_sasig['vol_ratio']:.1f}x → TP:VWAP"
                            )
                            if SA_LIVE:
                                # [v28 FIX] cooldown обновляется ТОЛЬКО если execute
                                # реально открыл позицию (проверка по длине sa_positions).
                                # Раньше _sa_last_entry = time.time() стояло ДО execute —
                                # любой внутренний reject (AI, volume, риск) вешал 1ч блок.
                                global _sa_last_entry
                                _sa_open = any(
                                    p.get('mode') == _sasig['mode']
                                    for p in sa_positions
                                )
                                _sa_cooldown_ok = (time.time() - _sa_last_entry) > 3600
                                if not _sa_open and _sa_cooldown_ok:
                                    _sa_len_before = len(sa_positions)
                                    await execute(SA_SYMBOL, _sasig, 'SA', sa_positions,
                                                  f"VWAP-dist:{_sasig['vwap_dist']:+.2f}ATR RSI:{_sasig['rsi']:.0f}")
                                    # cooldown только если позиция реально добавлена
                                    if len(sa_positions) > _sa_len_before:
                                        _sa_last_entry = time.time()
                                        logging.info(f"[SA] позиция открыта → cooldown запущен")
                                    else:
                                        logging.info(f"[SA] execute отклонил сигнал → cooldown НЕ запускается")
                                else:
                                    logging.info(
                                        f"[SA] пропуск: open={_sa_open} "
                                        f"cooldown={'OK' if _sa_cooldown_ok else f'{int((3600-(time.time()-_sa_last_entry))/60)}мин'}"
                                    )
                            else:
                                # SHADOW режим: пишем виртуальную сделку
                                shadow_record(SA_SYMBOL, _sasig['mode'], _sasig['entry'],
                                              _sasig, _sa_ctx, 'SA')
                        else:
                            logging.debug(f'[SA] фильтр: {_sa_reason}')
                    except Exception as _e:
                        logging.warning(f'[SA] exception: {_e}')

                # Авто-синхронизация с BingX каждые 10 циклов (~10 мин)
                global _sync_counter
                _sync_counter += 1
                if _sync_counter % 10 == 0 and all_positions():
                    removed = await sync_positions_with_exchange()
                    if removed > 0:
                        await tg(
                            f'👻 <b>Авто-синхронизация</b>: удалено {removed} ghost позиций\n'
                            f'Проверьте биржу — возможно позиции закрыты вручную'
                        )

                # Сканеры (параллельно)
                scan_t0 = time.time()
                results = await asyncio.gather(
                    scan_smc(),
                    scan_rsi(),
                    return_exceptions=True
                )
                # Логируем исключения из сканеров (ранее проглатывались молча)
                for _i, _r in enumerate(results):
                    if isinstance(_r, Exception):
                        _name = ['scan_smc', 'scan_rsi'][_i]
                        logging.error(f'❌ {_name} exception: {_r}', exc_info=_r)
                scan_elapsed = time.time() - scan_t0
                cb_status = '🔴CB' if circuit_open else ''
                sess_status = '🟢сессия' if is_session() else '⏸вне'
                logging.info(
                    f'⏱ Цикл #{cycle} завершён за {scan_elapsed:.1f}с | '
                    f'SMC:{len(smc_positions)} RSI:{len(rsi_positions)} поз | '
                    f'{sess_status} {cb_status}'
                )

            except Exception as e:
                logging.error(f"Main loop #{cycle} error: {e}", exc_info=True)

            # Динамическая пауза до следующего цикла
            elapsed = time.time() - t0
            await asyncio.sleep(max(10.0, 60.0 - elapsed))

    finally:
        logging.info("Shutting down...")
        await tg("🔴 <b>Bot v10.0</b> остановлен")
        if http:
            await http.close()
        await exchange.close()
        await binance.close()
        await bybit.close()
        await mexc.close()

if __name__ == '__main__':
    asyncio.run(main())
