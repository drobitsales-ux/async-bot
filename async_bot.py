"""
╔══════════════════════════════════════════════════════════════════════╗
║  UNIFIED SMC + RSI MEAN REVERSION BOT  v10.0  PROP FIRM EDITION     ║
║  BingX Perpetual Futures | Render-Ready | $10k → $100k Path         ║
║                                                                      ║
║  СТРАТЕГИИ:                                                          ║
║  [SMC]  Smart Money Concepts — CHoCH + FVG + Killzones              ║
║  [RSI]  Mean Reversion — RSI Extreme + Pattern + SMA/VWAP Magnet    ║
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
from datetime import datetime, timezone
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
RISK_PER_TRADE   = 0.02     # [USER] 2% на сделку (тест; для проп → 0.0075)
RISK_WEEKEND     = 0.01     # [USER] 1% в выходные (тест; для проп → 0.00375)
MAX_TOTAL_POS    = 3        # [R-FIX-2] суммарно SMC+RSI
MAX_PER_DIR      = 2        # макс 2 лонга или 2 шорта
LEVERAGE         = 5
MIN_VOL_USDT     = 500_000    # [TEST] снижен для проверки (было 3_000_000)
MAX_SL_PCT       = 2.5        # [R-FIX-1] жёсткий лимит SL %
MIN_SL_PCT       = 1.0        # мин. SL чтобы не убивало спредом
FEE_RATE         = 0.0005
DAILY_DD_LIMIT   = 0.025    # [R-FIX-11] стоп торговли при -2.5% за день
SCAN_LIMIT       = 60       # [PERF-3] 100→60: топ-60 ликвидных, цель цикла <90с
SCAN_SEM         = 50       # [PERF-3] 40→50 параллельных (60 символов)
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
RSI_LONG_MAX    = 27    # [P-5] чуть шире — 25 давал 0 сигналов в логах
RSI_SHORT_MIN   = 73    # [P-5] симметрично
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
    c.execute("""CREATE TABLE IF NOT EXISTS stats
                 (id INTEGER PRIMARY KEY, pnl_pct REAL, trades INTEGER,
                  wins INTEGER, start_balance REAL, stat_date TEXT,
                  smc_trades INTEGER, rsi_trades INTEGER)""")
    conn.commit()
    conn.close()

def _save(table: str, data: list):
    try:
        conn = get_db()
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
    global smc_positions, rsi_positions, daily_stats
    smc_positions = _load('smc_pos')
    rsi_positions = _load('rsi_pos')
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
    """London 07:00–10:30 + NY 13:00–16:30 UTC."""
    t = datetime.now(timezone.utc).time()
    return (
        datetime.strptime("07:00", "%H:%M").time() <= t <=
        datetime.strptime("10:30", "%H:%M").time()
    ) or (
        datetime.strptime("13:00", "%H:%M").time() <= t <=
        datetime.strptime("16:30", "%H:%M").time()
    )

def is_weekend() -> bool:
    return datetime.now(timezone.utc).weekday() >= 5

def current_risk() -> float:
    return RISK_WEEKEND if is_weekend() else RISK_PER_TRADE

def all_positions() -> list:
    return smc_positions + rsi_positions

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

    # Добавляем контекст BTC в промпт для лучшего решения
    btc_ctx_str = context.get('btc_trend', 'Flat')
    alt_ctx_str = 'altseason (altcoins outperform BTC)' if context.get('altseason') else 'no altseason'

    prompt = (
        'You are a crypto futures risk manager specializing in SMC and RSI mean reversion. '
        'Evaluate this trade and respond ONLY with valid JSON: '
        '{"decision":"approve"|"reject","confidence":0-100,"comment":"brief reason"}. '
        'IMPORTANT RULES: '
        '1. BTC:Flat does NOT invalidate altcoin setups — evaluate the altcoin independently. '
        '2. During altseason, long setups on altcoins are MORE valid even with BTC flat. '
        '3. RSI below 25 = strong oversold → approve with confidence >= 65. '
        '4. RSI above 75 = strong overbought → approve shorts with confidence >= 65. '
        '5. Reject ONLY if: RSI is in neutral zone (35-65), OR MAE risk is extreme. '
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
        "SMC: ищем CHoCH+FVG в сессию. RSI MR: перепроданность/перекупленность+паттерн."
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

            ok = str(parsed.get('decision', '')).lower() == 'approve'
            conf = int(parsed.get('confidence', 0))
            comment = parsed.get('comment', '')
            # Низкая уверенность тоже блокирует
            if conf < 55:
                ok = False
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
            return {'btc_trend': 'Flat', 'altseason': False}
        btc_c = np.array([x[4] for x in btc_ohlcv], dtype=float)
        ema200 = calc_ema(btc_c, 200)
        dist = (btc_c[-1] - ema200) / ema200 * 100
        trend = 'Long' if dist > 0.5 else ('Short' if dist < -0.5 else 'Flat')

        altseason = False
        try:
            eth_ohlcv = await exchange.fetch_ohlcv('ETH/USDT:USDT', SMC_TF, limit=55)
            if eth_ohlcv and len(eth_ohlcv) >= 50:
                eth_c = np.array([x[4] for x in eth_ohlcv], dtype=float)
                eth_ret = (eth_c[-1] - eth_c[-50]) / eth_c[-50] * 100
                btc_ret = (btc_c[-1] - btc_c[-50]) / btc_c[-50] * 100
                if eth_ret - btc_ret > 0.5 and eth_c[-1] > calc_ema(eth_c, 50):
                    altseason = True
        except:
            pass

        return {'btc_trend': trend, 'altseason': altseason}
    except:
        return {'btc_trend': 'Flat', 'altseason': False}

# ═══════════════════════════════════════════════════════
#  SMC СИГНАЛ
# ═══════════════════════════════════════════════════════
async def smc_signal(sym: str):
    """
    Полный SMC пайплайн:
    1. Killzone + News
    2. Volume (закрытая свеча v[-2])
    3. CHoCH с контекстом структуры
    4. VWAP (объёмно-взвешенный)
    5. RSI 85/15
    6. FVG — тест зоны
    """
    if not is_session():
        return None, 'session'
    if is_news_now():
        return None, 'news'

    try:
        ohlcv = await exchange.fetch_ohlcv(sym, SMC_TF, limit=60)  # [PERF-3] 100→60
    except:
        return None, 'fetch_err'
    if not ohlcv or len(ohlcv) < 55:
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

    # VWAP
    vwap = calc_vwap(h, l, c, v)
    if mode == 'Long'  and price < vwap * 0.995:
        return None, 'vwap'
    if mode == 'Short' and price > vwap * 1.005:
        return None, 'vwap'

    # RSI
    rsi = calc_rsi(c[:-1])
    if mode == 'Long'  and rsi > 85:
        return None, 'rsi'
    if mode == 'Short' and rsi < 15:
        return None, 'rsi'

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
    sl  = price * (1 - MAX_SL_PCT / 100) if mode == 'Long' else price * (1 + MAX_SL_PCT / 100)
    tp  = price * (1 + MAX_SL_PCT / 100 * 1.5) if mode == 'Long' else price * (1 - MAX_SL_PCT / 100 * 1.5)  # [FIX-TP] 2.0x→1.5x

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
        ohlcv = await exchange.fetch_ohlcv(sym, RSI_TF, limit=100)  # было 250 → ускорение цикла
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

    # Volume: закрытая [-2]
    avg_v = np.mean(v[-22:-2]) if len(v) >= 22 else float(np.mean(v[:-2]))
    if avg_v <= 0:
        return None, 'vol'
    vol_ratio = float(v[-2]) / avg_v
    if vol_ratio < 2.0 or vol_ratio > 12.0:  # [R-FIX-7] потолок 12x
        return None, 'vol'

    # RSI текущий и предыдущий (только закрытые свечи)
    rsi_now  = calc_rsi(c[:-1],  RSI_PERIOD)
    rsi_prev = calc_rsi(c[:-2], RSI_PERIOD)

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
    # SMA100 вместо SMA200 (limit=100, больше данных нет)
    sma200    = float(np.mean(c[-100:]))
    vwap      = calc_vwap(h, l, c, v)
    sma_dist  = (price - sma200) / sma200 * 100
    vwap_dist = (price - vwap) / vwap * 100

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
    rsi_extreme_long  = rsi_now <= 20
    rsi_extreme_short = rsi_now >= 80

    if (rsi_now <= RSI_LONG_MAX) and (bull_pat or rsi_extreme_long):
        # [R-FIX-6] Flat BTC больше не блокирует лонг
        if btc_trend == 'Short' and not altseason:
            return None, 'trend'
        # Hook: RSI в зоне ИЛИ разворачивается (допуск 2.0 пункта)
        # rsi_now < 25 уже подтверждает перепроданность — hook опциональный
        if rsi_now > rsi_prev + 2.0:  # RSI растёт быстро — не разворот
            return None, 'hook'
        if sma_dist > 2.0 or sma_dist < -15.0:   # [FIX-SMA] смягчено: -1→+2%, -8→-15%
            return None, 'sma_range'
        if vwap_dist > -1.2:              # должен быть под VWAP
            return None, 'vwap'
        # [R-FIX-3] SL под минимумом последних 3 свечей + 1.5x ATR
        raw_sl = float(np.min(l[-4:-1])) - atr * 1.5
        sl_pct = abs(price - raw_sl) / price * 100
        if sl_pct > MAX_SL_PCT:
            return None, 'sl_wide'
        sl   = raw_sl
        mode = 'Long'

    elif (rsi_now >= RSI_SHORT_MIN) and (bear_pat or rsi_extreme_short):
        if btc_trend == 'Long' or altseason:
            return None, 'trend'
        # Hook: RSI в зоне ИЛИ разворачивается
        if rsi_now < rsi_prev - 2.0:  # RSI падает быстро — не разворот
            return None, 'hook'
        if sma_dist < -2.0 or sma_dist > 15.0:  # [FIX-SMA] смягчено: 1→-2%, 8→15%
            return None, 'sma_range'
        if vwap_dist < 1.2:
            return None, 'vwap'
        # [R-FIX-3] SL над максимумом + 1.5x ATR
        raw_sl = float(np.max(h[-4:-1])) + atr * 1.5
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
        'funding': funding,
        'bingx_vol': float(v[-2]) * price,  # USD объём закрытой свечи
    }, 'ok'

# ═══════════════════════════════════════════════════════
#  ИСПОЛНЕНИЕ ОРДЕРА (общий для обоих стратегий)
# ═══════════════════════════════════════════════════════
async def publish_to_workers(sym: str, mode: str, price: float,
                              sl: float, tp: float, strategy: str,
                              risk_usdt: float):
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
                  pos_list: list, extra_tg: str = ""):
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
        logging.info(f"[{strategy}] {sym}: Gemini reject (conf={ai['conf']}): {ai['comment']}")
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

    risk_usdt = free_usdt * current_risk()
    sl_dist   = abs(price - sl)
    if sl_dist <= 0:
        return
    qty = risk_usdt / sl_dist
    qty = round(qty, 4)
    if qty <= 0:
        logging.warning(f'[{strategy}] {sym}: qty={qty} <= 0, пропуск')
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
        'open_time':   datetime.now(timezone.utc).isoformat(),
        'mfe_price':   price,
        'mae_price':   price,
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
        f"RR: <b>1:{rr:.2f}</b>  Риск: <b>${risk_usdt:.2f}</b>\n"
        + (f"🤖 AI: bypass\n" if 'bypass' in ai['comment']
           else f"🧠 AI({provider}): {ai['conf']}/100 | {ai['comment']}\n")
        + extra_tg
    )
    await tg(msg)
    logging.info(f"✅ [{strategy}] {sym} {mode} @ {price:.6f} | SL:{sl:.6f} | Risk:${risk_usdt:.2f}")
    # Публикуем сигнал воркерам (копи-трейдинг на Bybit и др.)
    asyncio.create_task(publish_to_workers(sym, mode, price, sl, tp, strategy, risk_usdt))

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

        # Обновить MFE/MAE
        if is_long:
            pos['mfe_price'] = max(float(pos.get('mfe_price', entry)), curr_p)
            pos['mae_price'] = min(float(pos.get('mae_price', entry)), curr_p)
        else:
            pos['mfe_price'] = min(float(pos.get('mfe_price', entry)), curr_p)
            pos['mae_price'] = max(float(pos.get('mae_price', entry)), curr_p)

        if live:
            real_qty = abs(float(live.get('contracts', 0)))
            pos['current_qty'] = real_qty

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

            # ── Breakeven ──────────────────────────────────────
            # BE после TP50: ждём 2.1% если TP50 ещё не сработал
            be_thr = 2.1 if not pos.get('tp50_hit') else 1.5
            if pnl >= be_thr and not pos.get('be_moved'):
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
            if pnl >= 2.0 and not pos.get('tp50_hit'):  # [TP] 2.5→2.0%
                close_qty = round(real_qty * 0.5, 8)
                remain    = round(real_qty - close_qty, 8)
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
                                'stopPrice': round(entry, 8),
                                'reduceOnly': True}
                    )
                    pos.update({'tp50_hit': True, 'current_qty': remain,
                                'sl_order_id': sl_ord['id']})
                    save_all()
                    await tg(f"💰 <b>[{strategy}] {sym}</b>: TP50% зафиксирован "
                             f"P&L: +{pnl:.2f}%")
                except Exception as e:
                    logging.error(f"TP50 error {sym}: {e}")

            # ── TP100 + trailing ───────────────────────────────
            tp100 = float(pos.get('tp1', entry))
            if (pos.get('tp50_hit') and not pos.get('tp100_hit')
                    and ((is_long and curr_p >= tp100)
                         or (not is_long and curr_p <= tp100))):
                close_qty = round(float(pos['current_qty']) * 0.5, 8)
                remain    = round(float(pos['current_qty']) - close_qty, 8)
                atr_v     = float(pos.get('atr', entry * 0.01))
                trail_sl  = (tp100 - atr_v if is_long else tp100 + atr_v)
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
                    pos.update({'tp100_hit': True, 'current_qty': remain,
                                'sl_order_id': sl_ord['id'],
                                'current_sl': trail_sl})
                    save_all()
                    await tg(f"🏆 <b>[{strategy}] {sym}</b>: TP100 взят! "
                             f"Трейлинг включён P&L: +{pnl:.2f}%")
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
            if seconds < 45:    # grace period для открытия
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
            net_pnl = raw - (fee_in + fee_out)

            pnl_pct = (exit_p - entry) / entry * 100 if is_long else (entry - exit_p) / entry * 100
            daily_stats['trades'] += 1
            daily_stats['pnl_pct'] += pnl_pct / 100  # в долях

            # Реальная победа = ощутимая прибыль (не BE)
            # pnl < 0.5% = BE-close (LINK+0.21%, APE+0.21%) — НЕ победа
            min_win_pct = 0.5
            is_win = pos.get('tp50_hit', False) or pnl_pct >= min_win_pct
            is_be  = 0 < pnl_pct < min_win_pct
            if is_win:
                daily_stats['wins'] += 1
            elif is_be:
                daily_stats['be_closes'] = daily_stats.get('be_closes', 0) + 1

            mfe_pct = abs(float(pos['mfe_price']) - entry) / entry * 100
            mae_pct = abs(float(pos['mae_price']) - entry) / entry * 100
            dur_min = int(seconds / 60)

            winrate_d = (daily_stats['wins'] / daily_stats['trades'] * 100
                         if daily_stats['trades'] > 0 else 0)
            result_tag = ' (TP✓)' if pos.get('tp50_hit') else (' (BE)' if is_be else (' (WIN)' if is_win else ' (SL)'))
            await tg(
                f"{'✅' if is_win else ('⚖️' if is_be else '🛑')} <b>[{strategy}] {sym}</b> закрыта{result_tag}\n"
                f"PnL: <code>{pnl_pct:+.2f}%</code> | Net: <code>{net_pnl:+.2f} USDT</code>\n"
                f"📈 MFE {mfe_pct:.2f}% | 📉 MAE {mae_pct:.2f}% | ⏱ {dur_min}мин\n"
                f"Вход: {entry:.6f} | Выход: {exit_p:.6f}\n"
                f"День: {daily_stats['trades']} сделок | "
                f"{daily_stats['wins']} побед | WR {winrate_d:.0f}%"
            )
            save_all()
            notified[sym] = time.time()   # cooldown после закрытия
            return False   # удалить из списка

    # Обработка SMC и RSI позиций
    new_smc, new_rsi = [], []
    for p in smc_positions:
        keep = await process_pos(p, smc_positions)
        if keep:
            new_smc.append(p)
    for p in rsi_positions:
        keep = await process_pos(p, rsi_positions)
        if keep:
            new_rsi.append(p)
    smc_positions[:] = new_smc
    rsi_positions[:] = new_rsi

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
        return
    if not check_circuit_breaker():
        return

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
    st   = {k: 0 for k in ['session','news','vol','structure','choch',
                            'vwap','rsi','fvg','fvg_test','ok']}

    async def check(sym):
        if sym in notified:
            return
        async with sem:
            sig, reason = await smc_signal(sym)
        st[reason] = st.get(reason, 0) + 1
        if sig:
            notified[sym] = time.time()
            await execute(sym, sig, 'SMC', smc_positions,
                          f"RSI: {sig['rsi']:.1f}")

    await asyncio.gather(*[check(s) for s in scan])
    logging.info(
        f"[SMC SCAN] choch:{st['choch']} vwap:{st['vwap']} "
        f"rsi:{st['rsi']} fvg:{st.get('fvg',0)+st.get('fvg_test',0)} "
        f"→ ВХОДЫ:{st['ok']}"
    )

async def scan_rsi():
    """Сканер RSI MR: запускается каждые 60 сек."""
    if not check_circuit_breaker():
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
                            'sma_range','vwap','sl_wide','squeeze',
                            'no_pattern','ok']}

    async def check(sym):
        if sym in notified:
            return
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

    await asyncio.gather(*[check(s) for s in scan])
    total_r = sum(st.values())
    logging.info(
        f"[RSI SCAN] BTC:{btc_ctx['btc_trend']} Alt:{btc_ctx['altseason']} | "
        f"total:{total_r} mid:{st['rsi_mid']} hook:{st['hook']} "
        f"sma:{st['sma_range']} vwap:{st['vwap']} trend:{st['trend']} "
        f"sl:{st['sl_wide']} sqz:{st['squeeze']} pat:{st['no_pattern']} "
        f"→ ВХОДЫ:{st['ok']}"
    )

# ═══════════════════════════════════════════════════════
#  ЕЖЕДНЕВНЫЙ ОТЧЁТ + СБРОС СТАТИСТИКИ
# ═══════════════════════════════════════════════════════
async def daily_reset():
    global daily_stats, circuit_open
    today = datetime.now(timezone.utc).date()
    if daily_stats.get('stat_date') == today:
        return

    if daily_stats.get('stat_date'):
        trades = daily_stats['trades']
        wins   = daily_stats['wins']
        wrate  = wins / trades * 100 if trades > 0 else 0

        try:
            bal = await exchange.fetch_balance()
            bal_usdt = float(bal.get('USDT', {}).get('total', 0))
        except:
            bal_usdt = daily_stats['start_balance']

        start = daily_stats['start_balance']
        day_pct = (bal_usdt - start) / start * 100 if start > 0 else 0

        await tg(
            f"📊 <b>Итоги дня {daily_stats['stat_date']}</b>\n"
            f"Сделок: {trades}  WR: {wrate:.1f}% (реальн.)  "
            f"BE: {daily_stats.get('be_closes',0)}  "
            f"SMC: {daily_stats.get('smc_trades',0)}  RSI: {daily_stats.get('rsi_trades',0)}\n"
            f"PnL: <code>{day_pct:+.2f}%</code>  "
            f"Баланс: <code>{bal_usdt:.2f} USDT</code>"
        )

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
async def main():
    global http

    init_db()
    load_all()
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
        f"🟢 <b>Unified SMC+RSI Bot v10.0 PROP</b> запущен\n"
        f"Риск: {RISK_PER_TRADE*100:.2f}%/сделку  "
        f"Max поз: {MAX_TOTAL_POS}  Плечо: {LEVERAGE}x\n"
        f"Сессии: London 07:00–10:30 | NY 13:00–16:30 UTC\n"
        f"Circuit breaker: при DD >{DAILY_DD_LIMIT*100:.1f}%/день"
    )
    logging.info("🚀 Unified SMC+RSI Bot v10.0 started")

    cycle = 0
    try:
        while True:
            cycle += 1
            t0 = time.time()
            try:
                await daily_reset()

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

                # Мониторинг позиций
                await monitor_all()

                # Сканеры (параллельно)
                scan_t0 = time.time()
                await asyncio.gather(
                    scan_smc(),
                    scan_rsi(),
                    return_exceptions=True
                )
                scan_elapsed = time.time() - scan_t0
                logging.info(
                    f'⏱ Цикл #{cycle} завершён за {scan_elapsed:.1f}с | '
                    f'SMC:{len(smc_positions)} RSI:{len(rsi_positions)} поз | '
                    f'Кэш Gemini: {len(_gemini_cache)} записей'
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
