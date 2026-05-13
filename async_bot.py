import asyncio
import json
import os
import logging
import sqlite3
import time
import signal
import ccxt.async_support as ccxt_async
import numpy as np
import aiohttp
from datetime import datetime, timezone, timedelta
from threading import Thread
from http.server import BaseHTTPRequestHandler, HTTPServer
 
# ══════════════════════════════════════════════════════════
#  ГЛОБАЛЬНЫЕ НАСТРОЙКИ
# ══════════════════════════════════════════════════════════
DB_PATH          = '/data/bot.db' if os.path.exists('/data') else 'bot.db'
TOKEN            = os.getenv('TELEGRAM_TOKEN')
GROUP_CHAT_ID    = int(os.getenv('GROUP_CHAT_ID', '-1'))   # [FIX-6] убрали хардкод
BINGX_API_KEY    = os.getenv('BINGX_API_KEY')
BINGX_SECRET     = os.getenv('BINGX_SECRET')
GEMINI_API_KEY   = os.getenv('GEMINI_API_KEY')
 
# ── Параметры Проп $10k ──────────────────────────────────
BASE_RISK_PER_TRADE         = 0.0075   # 0.75% риск на сделку
BASE_RISK_WEEKEND           = 0.00375  # [FIX-4] 0.375% в выходные (риск/2)
MAX_POSITIONS               = 2
MAX_POSITIONS_PER_DIRECTION = 1        # не более 1 лонга и 1 шорта одновременно
LEVERAGE                    = 5
MIN_VOLUME_USDT             = 3_000_000  # рекомендованный порог для $10k
MAX_SL_PCT                  = 2.5
FEE_RATE                    = 0.0005
SMC_TIMEFRAME               = '15m'
SCAN_LIMIT                  = 250      # оптимально для 60-сек цикла
SCAN_CONCURRENCY            = 20       # [FIX-6] параллельных запросов
 
# [FIX-1] ИСПРАВЛЕН ФИЛЬТР: проверка через 'in', а не точное совпадение
# Перечислены ЧАСТИ имени — если часть встречается в символе, монета исключается
EXCLUDED_PARTS = [
    'BTC', 'ETH', 'SOL', 'BNB', 'XRP',
    'NCS', 'NCFX', 'NCCO', 'NCSI', 'NIKKEI', 'NASDAQ', 'SP500',
    'GOLD', 'SILVER', 'XAU', 'PAXG', 'EUR', 'LUNC', 'USTC',
    'USDC', 'FART', 'PEPE', 'SHIB', 'DOGE', 'WIF', 'BONK',
    'FLOKI', 'BOME', 'MEME', 'TURBO', 'SATS', 'RATS', 'ORDI', '1000'
]
 
# ── Состояние ────────────────────────────────────────────
daily_stats      = {'pnl': 0.0, 'trades': 0, 'wins': 0, 'start_balance': 0.0}
active_positions = []
NOTIFIED_SYMBOLS = {}   # {sym: timestamp}
NEWS_EVENTS      = []
global_session   = None
_markets_cache   = None
_markets_last_refresh = 0.0
_last_news_refresh    = 0.0
_last_stats_date      = None   # [FIX-7] для ежесуточного сброса
 
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s'
)
 
# ── Биржи ────────────────────────────────────────────────
exchange = ccxt_async.bingx({
    'apiKey':  BINGX_API_KEY,
    'secret':  BINGX_SECRET,
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
 
# ══════════════════════════════════════════════════════════
#  БАЗА ДАННЫХ
# ══════════════════════════════════════════════════════════
def get_db_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)
 
def init_db():
    conn = get_db_conn()
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS active_positions
                 (id INTEGER PRIMARY KEY, data TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS daily_stats
                 (id INTEGER PRIMARY KEY, pnl REAL, trades INTEGER,
                  wins INTEGER, start_balance REAL, stat_date TEXT)''')
    conn.commit()
    conn.close()
 
def save_positions():
    try:
        conn = get_db_conn()
        c = conn.cursor()
        c.execute("INSERT OR REPLACE INTO active_positions (id, data) VALUES (1, ?)",
                  (json.dumps(active_positions),))
        c.execute("""INSERT OR REPLACE INTO daily_stats
                     (id, pnl, trades, wins, start_balance, stat_date)
                     VALUES (1, ?, ?, ?, ?, ?)""",
                  (daily_stats['pnl'], daily_stats['trades'],
                   daily_stats['wins'], daily_stats['start_balance'],
                   str(_last_stats_date)))
        conn.commit()
        conn.close()
    except Exception as e:
        logging.error(f"DB save error: {e}")
 
def load_positions():
    global active_positions, daily_stats, _last_stats_date
    try:
        conn = get_db_conn()
        c = conn.cursor()
        c.execute("SELECT data FROM active_positions WHERE id = 1")
        row = c.fetchone()
        if row:
            active_positions = json.loads(row[0])
 
        c.execute("SELECT pnl, trades, wins, start_balance, stat_date FROM daily_stats WHERE id = 1")
        stat_row = c.fetchone()
        if stat_row:
            daily_stats['pnl']           = stat_row[0]
            daily_stats['trades']        = stat_row[1]
            daily_stats['wins']          = stat_row[2]
            daily_stats['start_balance'] = stat_row[3]
            stored_date = stat_row[4]
            try:
                _last_stats_date = datetime.strptime(stored_date, "%Y-%m-%d").date()
            except Exception:
                _last_stats_date = None
        conn.close()
    except Exception:
        pass
 
# ══════════════════════════════════════════════════════════
#  ТЕЛЕГРАМ
# ══════════════════════════════════════════════════════════
async def send_tg_msg(text: str):
    if not TOKEN or GROUP_CHAT_ID == -1 or not global_session:
        return
    url     = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    payload = {"chat_id": GROUP_CHAT_ID, "text": text, "parse_mode": "HTML"}
    try:
        async with global_session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=5)) as resp:
            pass
    except Exception:
        pass
 
# ══════════════════════════════════════════════════════════
#  НОВОСТНОЙ ФИЛЬТР  [FIX-3] [FIX-8]
# ══════════════════════════════════════════════════════════
async def update_macro_news():
    """Обновление макро-новостей. Заглушка — заменить на реальный API."""
    global NEWS_EVENTS, _last_news_refresh
    NEWS_EVENTS = [
        # Формат: {'time': 'HH:MM', 'impact': 'High', 'currency': 'USD'}
        # Здесь подключить https://nfs.faireconomy.media/ff_calendar_thisweek.json
    ]
    _last_news_refresh = time.time()
    logging.info(f"📰 [NEWS] Обновлено: {len(NEWS_EVENTS)} событий")
 
def is_news_spike() -> bool:
    """[FIX-3] Блокировка ±15 минут вокруг красного события."""
    if not NEWS_EVENTS:
        return False
    now_utc = datetime.now(timezone.utc)
    for ev in NEWS_EVENTS:
        if ev.get('impact') != 'High':
            continue
        try:
            ev_time = datetime.strptime(ev['time'], "%H:%M").replace(
                year=now_utc.year, month=now_utc.month, day=now_utc.day,
                tzinfo=timezone.utc)
            if abs((now_utc - ev_time).total_seconds()) <= 900:  # 15 мин
                return True
        except Exception:
            pass
    return False
 
# ══════════════════════════════════════════════════════════
#  ОРАКУЛЫ
# ══════════════════════════════════════════════════════════
async def check_global_volume_oracle(sym: str) -> bool:
    """Проверка объёма на Binance + Bybit."""
    try:
        base = sym.split('/')[0]
        bin_sym = f"{base}/USDT"
        results = await asyncio.gather(
            binance.fetch_ticker(bin_sym),
            bybit.fetch_ticker(bin_sym),
            return_exceptions=True
        )
        vol_bin = results[0]['quoteVolume'] if not isinstance(results[0], Exception) else 0
        vol_bbt = results[1]['quoteVolume'] if not isinstance(results[1], Exception) else 0
        return vol_bin > MIN_VOLUME_USDT or vol_bbt > MIN_VOLUME_USDT
    except Exception:
        return True  # при ошибке — пропускаем (не блокируем сделку)
 
async def ask_gemini_oracle(sym: str, mode: str, price: float) -> bool:
    """AI-фильтр через Gemini 1.5 Flash."""
    if not GEMINI_API_KEY or not global_session:
        return True
    prompt = (
        f"Crypto futures analysis: {sym}, timeframe {SMC_TIMEFRAME}, "
        f"direction {mode}, entry price {price:.6f}. "
        f"Using Smart Money Concepts (CHoCH, FVG, liquidity). "
        f"Is this a valid SMC setup? Reply ONLY with YES or NO."
    )
    url = (
        f"https://generativelanguage.googleapis.com/v1beta/"
        f"models/gemini-1.5-flash:generateContent?key={GEMINI_API_KEY}"
    )
    payload = {"contents": [{"parts": [{"text": prompt}]}]}
    try:
        async with global_session.post(
            url, json=payload, timeout=aiohttp.ClientTimeout(total=6)
        ) as resp:
            data = await resp.json()
            text = data['candidates'][0]['content']['parts'][0]['text'].strip().upper()
            approved = 'YES' in text
            if not approved:
                logging.info(f"🤖 [GEMINI] {sym} {mode} — REJECT")
            return approved
    except Exception:
        return True
 
# ══════════════════════════════════════════════════════════
#  SMC МАТЕМАТИКА
# ══════════════════════════════════════════════════════════
def is_trade_session() -> bool:
    """Killzones: London 07:00–10:30 + NY 13:00–16:30 UTC."""
    now    = datetime.now(timezone.utc).time()
    london = (datetime.strptime("07:00", "%H:%M").time(),
              datetime.strptime("10:30", "%H:%M").time())
    ny     = (datetime.strptime("13:00", "%H:%M").time(),
              datetime.strptime("16:30", "%H:%M").time())
    return (london[0] <= now <= london[1]) or (ny[0] <= now <= ny[1])
 
def is_weekend() -> bool:
    """[FIX-4] Суббота или воскресенье (UTC)."""
    return datetime.now(timezone.utc).weekday() >= 5
 
def calculate_vwap(h, l, c, v) -> float:
    """Объёмно-взвешенный VWAP."""
    denom = np.sum(v)
    if denom == 0:
        return float(c[-1])
    return float(np.sum(v * (h + l + c) / 3) / denom)
 
def calculate_rsi(prices, window: int = 14) -> float:
    """RSI по последним `window` барам."""
    if len(prices) < window + 1:
        return 50.0
    diffs  = np.diff(prices[-window - 1:])
    gains  = np.maximum(diffs, 0)
    losses = np.abs(np.minimum(diffs, 0))
    avg_g  = np.mean(gains)
    avg_l  = np.mean(losses)
    if avg_l == 0:
        return 100.0
    return float(100 - (100 / (1 + avg_g / avg_l)))
 
def get_pivots(highs, lows, order: int = 5):
    """Swing High / Swing Low (чистый NumPy, без scipy)."""
    h_idx, l_idx = [], []
    n = len(highs)
    for i in range(order, n - order):
        window_h = highs[i - order: i + order + 1]
        if highs[i] == np.max(window_h) and np.sum(window_h == highs[i]) == 1:
            h_idx.append(i)
        window_l = lows[i - order: i + order + 1]
        if lows[i] == np.min(window_l) and np.sum(window_l == lows[i]) == 1:
            l_idx.append(i)
    return h_idx, l_idx
 
def find_fvg(h, l, mode: str, lookback: int = 10):
    """
    Истинный Fair Value Gap:
    Bullish FVG: high[i-1] < low[i+1]  (разрыв вверх через свечу i)
    Bearish FVG: low[i-1]  > high[i+1] (разрыв вниз через свечу i)
    Возвращает самый свежий FVG, не старше `lookback` баров.
    """
    for i in range(1, min(lookback, len(h) - 1)):
        idx = len(h) - 1 - i
        if idx < 1 or idx + 1 >= len(h):
            continue
        if mode == 'Long' and h[idx - 1] < l[idx + 1]:
            return {'top': float(l[idx + 1]), 'bottom': float(h[idx - 1])}
        if mode == 'Short' and l[idx - 1] > h[idx + 1]:
            return {'top': float(l[idx - 1]), 'bottom': float(h[idx + 1])}
    return None
 
# ══════════════════════════════════════════════════════════
#  ОСНОВНАЯ SMC-ЛОГИКА
# ══════════════════════════════════════════════════════════
async def process_smc_coin(sym: str):
    """
    Полный SMC-пайплайн:
    1. Killzone + Новости
    2. Объём (закрытая свеча v[-2])
    3. CHoCH с контекстом структуры
    4. VWAP (объёмно-взвешенный)
    5. RSI 85/15
    6. FVG (тест зоны, не пробой)
    """
    # 1. Временные фильтры
    if not is_trade_session():
        return None, 'out_of_session'
    if is_news_spike():
        return None, 'news_spike'
 
    # 2. Данные OHLCV
    try:
        ohlcv = await exchange.fetch_ohlcv(sym, SMC_TIMEFRAME, limit=100)
    except Exception:
        return None, 'no_data'
 
    if not ohlcv or len(ohlcv) < 55:   # запас для order=5 + lookback
        return None, 'no_data'
 
    c = np.array([float(x[4]) for x in ohlcv])
    h = np.array([float(x[2]) for x in ohlcv])
    l = np.array([float(x[3]) for x in ohlcv])
    v = np.array([float(x[5]) for x in ohlcv])
    current_price = float(c[-1])
 
    # 3. Объём — проверяем закрытую свечу [-2]
    avg_vol = np.mean(v[-21:-2]) if len(v) > 21 else 0.0
    if avg_vol <= 0 or v[-2] < avg_vol * 1.5:
        return None, 'no_volume'
 
    # 4. CHoCH (настоящий: учитываем контекст структуры)
    h_idx, l_idx = get_pivots(h, l, order=5)
    if len(h_idx) < 3 or len(l_idx) < 3:
        return None, 'no_structure'
 
    recent_highs = [h[i] for i in h_idx[-3:]]
    recent_lows  = [l[i] for i in l_idx[-3:]]
 
    mode = None
    # Bullish CHoCH: предыдущая структура была нисходящей (LH), цена ломает последний LH
    if recent_highs[-1] < recent_highs[-2] and current_price > recent_highs[-1]:
        mode = 'Long'
    # Bearish CHoCH: предыдущая структура была восходящей (HL), цена ломает последний HL
    elif recent_lows[-1] > recent_lows[-2] and current_price < recent_lows[-1]:
        mode = 'Short'
 
    if not mode:
        return None, 'no_choch'
 
    # 5. VWAP — цена должна быть на стороне тренда
    vwap = calculate_vwap(h, l, c, v)
    if mode == 'Long'  and current_price < vwap * 0.995:
        return None, 'vwap_reject'
    if mode == 'Short' and current_price > vwap * 1.005:
        return None, 'vwap_reject'
 
    # 6. RSI — исключаем настоящие экстремумы, не CHoCH-импульсы
    rsi = calculate_rsi(c[:-1])   # только закрытые свечи
    if mode == 'Long'  and rsi > 85:
        return None, 'rsi_exhausted'
    if mode == 'Short' and rsi < 15:
        return None, 'rsi_exhausted'
 
    # 7. FVG — вход на тесте зоны (не после пробоя!)
    fvg = find_fvg(h, l, mode)
    if not fvg:
        return None, 'no_fvg'
 
    if mode == 'Long':
        if not (fvg['bottom'] <= current_price <= fvg['top'] * 1.002):
            return None, 'no_fvg_test'
    else:
        if not (fvg['bottom'] * 0.998 <= current_price <= fvg['top']):
            return None, 'no_fvg_test'
 
    return {'symbol': sym, 'mode': mode, 'price': current_price}, 'success'
 
# ══════════════════════════════════════════════════════════
#  ИСПОЛНЕНИЕ ОРДЕРОВ
# ══════════════════════════════════════════════════════════
async def execute_trade(sym: str, signal: dict):
    """Открывает позицию: market entry + stopMarket SL + Telegram уведомление."""
    global active_positions
 
    mode          = signal['mode']
    current_price = signal['price']
 
    # Проверка лимитов позиций
    if len(active_positions) >= MAX_POSITIONS:
        return
    if sum(1 for p in active_positions if p['direction'] == mode) >= MAX_POSITIONS_PER_DIRECTION:
        return
 
    # Финальные проверки через оракулы (объём + AI)
    has_volume = await check_global_volume_oracle(sym)
    if not has_volume:
        logging.info(f"⚠️ [ORACLE] {sym}: недостаточно объёма на Binance/Bybit")
        return
 
    ai_approved = await ask_gemini_oracle(sym, mode, current_price)
    if not ai_approved:
        return
 
    # Баланс
    try:
        bal        = await exchange.fetch_balance()
        free_usdt  = float(bal.get('USDT', {}).get('free', 0))
    except Exception as e:
        logging.error(f"Balance error: {e}")
        return
 
    if free_usdt < 100:
        logging.warning(f"⚠️ Недостаточно баланса: {free_usdt} USDT")
        return
 
    # [FIX-4] Риск в выходные вдвое ниже
    risk_pct    = BASE_RISK_WEEKEND if is_weekend() else BASE_RISK_PER_TRADE
    risk_amount = free_usdt * risk_pct         # напр. $10000 * 0.0075 = $75
 
    sl_dist_pct = MAX_SL_PCT / 100.0           # 0.025
    sl_price    = (current_price * (1 - sl_dist_pct) if mode == 'Long'
                   else current_price * (1 + sl_dist_pct))
    tp_price    = (current_price * (1 + sl_dist_pct * 2.0) if mode == 'Long'
                   else current_price * (1 - sl_dist_pct * 2.0))
 
    # qty = рискуемая сумма / (SL_дистанция_в_$_за_1_контракт)
    # SL_дистанция_за_1 = entry * sl_dist_pct
    qty         = risk_amount / (current_price * sl_dist_pct)
 
    pos_side    = 'LONG'  if mode == 'Long'  else 'SHORT'
    order_side  = 'buy'   if mode == 'Long'  else 'sell'
    sl_side     = 'sell'  if mode == 'Long'  else 'buy'
 
    try:
        await exchange.set_margin_mode('isolated', sym)
        await exchange.set_leverage(LEVERAGE, sym)
 
        # Основной ордер
        await exchange.create_order(
            sym, 'market', order_side, qty,
            params={'positionSide': pos_side}
        )
 
        # Стоп-лосс
        sl_ord = await exchange.create_order(
            sym, 'stopMarket', sl_side, qty,
            params={
                'positionSide': pos_side,
                'stopPrice':    round(sl_price, 8),
                'reduceOnly':   True,
            }
        )
 
        position_record = {
            'symbol':       sym,
            'direction':    mode,
            'entry_price':  current_price,
            'initial_qty':  qty,
            'current_qty':  qty,
            'sl_price':     sl_price,
            'current_sl':   sl_price,
            'tp1':          tp_price,
            'sl_order_id':  sl_ord['id'],
            'be_moved':     False,
            'tp50_hit':     False,
            'risk_usdt':    risk_amount,
        }
        active_positions.append(position_record)
        save_positions()
 
        weekend_tag = ' (Weekend Risk)' if is_weekend() else ''
        await send_tg_msg(
            f"🟢 <b>{sym}</b> Открыт <b>{mode}</b>{weekend_tag}\n"
            f"Цена входа: <code>{current_price:.6f}</code>\n"
            f"SL: <code>{sl_price:.6f}</code>  TP: <code>{tp_price:.6f}</code>\n"
            f"Риск: <b>${risk_amount:.2f}</b>  RR: <b>1:2</b>\n"
            f"✅ Одобрено: Gemini AI + Global Volume Oracle"
        )
        logging.info(f"✅ [TRADE] {sym} {mode} @ {current_price:.6f} | SL:{sl_price:.6f} | Risk:${risk_amount:.2f}")
 
    except Exception as e:
        logging.error(f"❌ [TRADE ERROR] {sym}: {e}")
 
# ══════════════════════════════════════════════════════════
#  МОНИТОРИНГ ПОЗИЦИЙ
# ══════════════════════════════════════════════════════════
async def monitor_positions_job():
    """
    Управление открытыми позициями:
    - pnl >= 1.5%: Breakeven (SL → вход +0.2%)
    - pnl >= 2.5%: Зафиксировать 50%, SL → вход
    - Позиция закрыта на бирже → удалить из списка
    """
    global active_positions
 
    if not active_positions:
        return
 
    # [FIX-5] Guard для пустого списка тикеров
    syms = list({p['symbol'] for p in active_positions})
    if not syms:
        return
 
    try:
        positions_raw = await exchange.fetch_positions(syms)
        tickers       = await exchange.fetch_tickers(syms)
    except Exception as e:
        logging.error(f"Monitor fetch error: {e}")
        return
 
    for pos in active_positions[:]:   # копия для безопасной итерации
        sym       = pos['symbol']
        is_long   = pos['direction'] == 'Long'
        entry_price = float(pos['entry_price'])
 
        # Текущая цена из тикера
        ticker_data = tickers.get(sym, {})
        curr_price  = float(ticker_data.get('last', entry_price))
 
        # Проверка живой позиции на бирже
        live = next(
            (r for r in positions_raw
             if r.get('symbol') == sym and abs(float(r.get('contracts', 0))) > 0),
            None
        )
 
        pos_side = 'LONG' if is_long else 'SHORT'
        sl_side  = 'sell' if is_long else 'buy'
 
        if live:
            real_qty = abs(float(live.get('contracts', 0)))
            pnl_pct  = ((curr_price - entry_price) / entry_price * 100 if is_long
                        else (entry_price - curr_price) / entry_price * 100)
 
            # ── Breakeven при +1.5% ──────────────────────────────
            if pnl_pct >= 1.5 and not pos.get('be_moved'):
                new_sl = (entry_price * 1.002 if is_long else entry_price * 0.998)
                try:
                    if pos.get('sl_order_id'):
                        await exchange.cancel_order(pos['sl_order_id'], sym)
                    sl_ord = await exchange.create_order(
                        sym, 'stopMarket', sl_side, real_qty,
                        params={'positionSide': pos_side,
                                'stopPrice':    round(new_sl, 8),
                                'reduceOnly':   True}
                    )
                    pos['current_sl']  = new_sl
                    pos['sl_order_id'] = sl_ord['id']
                    pos['be_moved']    = True
                    save_positions()
                    await send_tg_msg(
                        f"🛡 <b>{sym}</b>: SL перенесён в БУ\n"
                        f"Новый SL: <code>{new_sl:.6f}</code>  P&L: +{pnl_pct:.2f}%"
                    )
                except Exception as e:
                    logging.error(f"BE error {sym}: {e}")
 
            # ── TP 50% при +2.5% ─────────────────────────────────
            if pnl_pct >= 2.5 and not pos.get('tp50_hit'):
                close_qty = round(real_qty * 0.5, 8)
                remain_qty = round(real_qty - close_qty, 8)
                try:
                    # Закрываем 50%
                    await exchange.create_order(
                        sym, 'market', sl_side, close_qty,
                        params={'positionSide': pos_side, 'reduceOnly': True}
                    )
                    pos['tp50_hit']  = True
                    pos['current_qty'] = remain_qty
 
                    # Переставляем SL для оставшихся 50%
                    if pos.get('sl_order_id'):
                        await exchange.cancel_order(pos['sl_order_id'], sym)
                    sl_ord = await exchange.create_order(
                        sym, 'stopMarket', sl_side, remain_qty,
                        params={'positionSide': pos_side,
                                'stopPrice':    round(entry_price, 8),
                                'reduceOnly':   True}
                    )
                    pos['sl_order_id'] = sl_ord['id']
                    save_positions()
                    await send_tg_msg(
                        f"💰 <b>{sym}</b>: Зафиксировано 50% позиции\n"
                        f"P&L: +{pnl_pct:.2f}% | Остаток работает"
                    )
                except Exception as e:
                    logging.error(f"TP50 error {sym}: {e}")
 
        else:
            # Позиция закрыта на бирже (SL или TP сработал)
            pnl_approx = ((curr_price - entry_price) / entry_price * 100 if is_long
                          else (entry_price - curr_price) / entry_price * 100)
            result_emoji = "✅" if pnl_approx > 0 else "🔴"
            active_positions.remove(pos)
            daily_stats['trades'] += 1
            if pnl_approx > 0:
                daily_stats['wins'] += 1
            daily_stats['pnl'] += pnl_approx
            save_positions()
            await send_tg_msg(
                f"🏁 <b>{sym}</b>: Позиция закрыта\n"
                f"Результат: {result_emoji} <code>{pnl_approx:+.2f}%</code>\n"
                f"Сделок сегодня: {daily_stats['trades']} | Побед: {daily_stats['wins']}"
            )
 
# ══════════════════════════════════════════════════════════
#  HEALTH CHECK (для Render)
# ══════════════════════════════════════════════════════════
class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"BingX SMC Bot v9.0 PROP | Status: OK")
 
    def log_message(self, format, *args):
        return  # подавляем HTTP-логи
 
def run_health_server():
    port = int(os.environ.get('PORT', 10000))
    HTTPServer(('0.0.0.0', port), HealthCheckHandler).serve_forever()
 
# ══════════════════════════════════════════════════════════
#  ЗАГРУЗКА РЫНКОВ С КЭШЕМ [FIX-2]
# ══════════════════════════════════════════════════════════
async def get_cached_markets():
    """[FIX-2] Кэш рынков: обновление раз в час, не каждый цикл."""
    global _markets_cache, _markets_last_refresh
    if _markets_cache and (time.time() - _markets_last_refresh < 3600):
        return _markets_cache
    _markets_cache        = await exchange.load_markets()
    _markets_last_refresh = time.time()
    logging.info(f"🗺 [MARKETS] Кэш обновлён: {len(_markets_cache)} инструментов")
    return _markets_cache
 
# ══════════════════════════════════════════════════════════
#  ВСПОМОГАТЕЛЬНЫЕ ЗАДАЧИ
# ══════════════════════════════════════════════════════════
async def daily_stats_reset():
    """[FIX-7] Сброс статистики в полночь UTC."""
    global daily_stats, _last_stats_date
    today = datetime.now(timezone.utc).date()
    if _last_stats_date != today:
        if _last_stats_date is not None:
            # Отчёт за прошедший день
            winrate = (daily_stats['wins'] / daily_stats['trades'] * 100
                       if daily_stats['trades'] > 0 else 0)
            await send_tg_msg(
                f"📊 <b>Итоги дня {_last_stats_date}</b>\n"
                f"Сделок: {daily_stats['trades']} | Побед: {daily_stats['wins']} "
                f"({winrate:.1f}%)\n"
                f"PnL: <code>{daily_stats['pnl']:+.2f}%</code>"
            )
        daily_stats     = {'pnl': 0.0, 'trades': 0, 'wins': 0,
                           'start_balance': daily_stats['start_balance']}
        _last_stats_date = today
        save_positions()
        logging.info(f"📅 [STATS] Сброс суточной статистики ({today})")
 
# ══════════════════════════════════════════════════════════
#  ГЛАВНЫЙ ЦИКЛ
# ══════════════════════════════════════════════════════════
async def main():
    global global_session, _last_stats_date
 
    # Инициализация
    init_db()
    load_positions()
    global_session  = aiohttp.ClientSession()
    _last_stats_date = datetime.now(timezone.utc).date()
 
    # Health-check сервер (отдельный поток — не блокирует asyncio)
    Thread(target=run_health_server, daemon=True).start()
 
    # Первоначальная загрузка новостей
    await update_macro_news()
 
    logging.info("🚀 BingX SMC Bot v9.0 PROP EDITION запущен")
    await send_tg_msg(
        "🟢 <b>BingX SMC Bot v9.0 PROP</b> запущен\n"
        f"Сессии: London 07:00–10:30 | NY 13:00–16:30 UTC\n"
        f"Риск: {BASE_RISK_PER_TRADE*100:.2f}% / {BASE_RISK_WEEKEND*100:.3f}% (WE) | "
        f"MaxPos: {MAX_POSITIONS} | Leverage: {LEVERAGE}x"
    )
 
    # Счётчик для периодических задач
    cycle = 0
 
    try:
        while True:
            cycle += 1
            cycle_start = time.time()
 
            try:
                # ── Периодические задачи ─────────────────────────────
                await daily_stats_reset()
 
                # [FIX-8] Обновление новостей каждые 30 минут
                if time.time() - _last_news_refresh > 1800:
                    await update_macro_news()
 
                # ── Фильтр сессий ────────────────────────────────────
                if not is_trade_session():
                    if cycle % 5 == 0:
                        logging.info("💤 [SESSION] Вне торговой сессии — ожидание...")
                    await asyncio.sleep(60)
                    continue
 
                # ── Загрузка рынков (с кэшем) ────────────────────────
                markets   = await get_cached_markets()
                # [FIX-1] ИСПРАВЛЕНО: any(kw in sym) вместо sym not in list
                all_syms  = [
                    s for s in markets.keys()
                    if s.endswith(':USDT')
                    and not any(kw in s for kw in EXCLUDED_PARTS)
                ]
                scan_list = all_syms[:SCAN_LIMIT]
 
                logging.info(
                    f"⏳ [РАДАР] Цикл #{cycle} | "
                    f"Монет: {len(scan_list)} | "
                    f"Позиций: {len(active_positions)}/{MAX_POSITIONS}"
                )
 
                # ── [FIX-6] Параллельный скан с Semaphore ────────────
                sem     = asyncio.Semaphore(SCAN_CONCURRENCY)
                stats   = {k: 0 for k in
                           ['session', 'news', 'vol', 'struct', 'choch',
                            'vwap', 'rsi', 'fvg', 'inputs']}
 
                async def scan_one(sym):
                    if sym in NOTIFIED_SYMBOLS:
                        return
                    async with sem:
                        signal, status = await process_smc_coin(sym)
                    if signal:
                        stats['inputs'] += 1
                        NOTIFIED_SYMBOLS[sym] = time.time()
                        await execute_trade(sym, signal)
                    else:
                        key = {
                            'out_of_session': 'session',
                            'news_spike':     'news',
                            'no_volume':      'vol',
                            'no_structure':   'struct',
                            'no_choch':       'choch',
                            'vwap_reject':    'vwap',
                            'rsi_exhausted':  'rsi',
                            'no_fvg':         'fvg',
                            'no_fvg_test':    'fvg',
                        }.get(status, '')
                        if key:
                            stats[key] += 1
 
                await asyncio.gather(*[scan_one(s) for s in scan_list])
 
                # Очистка устаревших кулдаунов (4 часа)
                now_t   = time.time()
                expired = [k for k, v in NOTIFIED_SYMBOLS.items() if now_t - v > 14400]
                for k in expired:
                    del NOTIFIED_SYMBOLS[k]
 
                logging.info(
                    f"🔎 [SMC] Сессия({stats['session']}) Новости({stats['news']}) "
                    f"Объём({stats['vol']}) Структура({stats['struct']}) "
                    f"CHoCH({stats['choch']}) VWAP({stats['vwap']}) "
                    f"RSI({stats['rsi']}) FVG({stats['fvg']}) "
                    f"→ ВХОДЫ: {stats['inputs']}"
                )
 
                # ── Мониторинг открытых позиций ──────────────────────
                await monitor_positions_job()
 
            except Exception as e:
                logging.error(f"❌ [MAIN LOOP] Ошибка: {e}", exc_info=True)
 
            # Пауза до следующего цикла (60 сек минус время выполнения)
            elapsed = time.time() - cycle_start
            sleep_t = max(10.0, 60.0 - elapsed)
            await asyncio.sleep(sleep_t)
 
    finally:
        # [FIX-9] Graceful shutdown
        logging.info("🛑 Остановка бота — закрытие соединений...")
        if global_session:
            await global_session.close()
        await exchange.close()
        await binance.close()
        await bybit.close()
        logging.info("✅ Все соединения закрыты.")
 
# ══════════════════════════════════════════════════════════
#  ТОЧКА ВХОДА
# ══════════════════════════════════════════════════════════
if __name__ == '__main__':
    asyncio.run(main())
