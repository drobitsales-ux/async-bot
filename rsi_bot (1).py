import telebot
import numpy as np
import time
import os
import logging
import sqlite3
import json
import ccxt
from datetime import datetime, timezone, timedelta
from threading import Thread
from flask import Flask
from apscheduler.schedulers.background import BackgroundScheduler

# === НАСТРОЙКИ v5.2 (SMC v5.2: Volume, BTC, Altseason, Premium/Discount + MTF (4H Trend)) ===
DB_PATH = '/data/bot.db' 
TOKEN = os.getenv('TELEGRAM_TOKEN')
GROUP_CHAT_ID = int(os.getenv('GROUP_CHAT_ID', -1003407154454))
BINGX_API_KEY = os.getenv('BINGX_API_KEY')
BINGX_SECRET = os.getenv('BINGX_SECRET')

# Умный риск-менеджмент
RISK_PER_TRADE = 0.01       
MAX_POSITIONS = 3           
LEVERAGE = 10               
MAX_SPREAD_PERCENT = 0.002  
MIN_VOLUME_USDT = 5000000  

# Умный таймаут (Даем сделкам на 1H графике время дойти до тейков)
TRADE_TIMEOUT_PROFIT_HOURS = 24  # Ждем сутки, если сделка в плюсе
TRADE_TIMEOUT_ANY_HOURS = 48     # Максимум 2 суток на сделку

# SMC Настройки
SMC_TIMEFRAME = '1h'        
MIN_RR_RATIO = 2.0          

EXCLUDED_KEYWORDS = [
    'NCCO', 'NCSI', 
    'NIKKEI', 'NASDAQ', 'SP500', 'GOLD', 'SILVER', 'AUT', 'XAU', 'PAXG', 'EUR', 'JPY',
    'PEPE', 'SHIB', '1000', 'FLOKI', 'DOGE', 'BONK', 'MEME', 'LUNC', 'USTC', 'WIF', 'POPCAT',
    'BTC/', 'ETH/', 'BNB/', 'SOL/', 'XRP/', 'ADA/', 'TRX/'
]

GLOBAL_STOP_UNTIL = None
CONSECUTIVE_LOSSES = 0
last_signals = {} 

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(message)s')
bot = telebot.TeleBot(TOKEN)

bingx = ccxt.bingx({
    'apiKey': BINGX_API_KEY,
    'secret': BINGX_SECRET,
    'options': {'defaultType': 'swap', 'marginMode': 'isolated'},
    'enableRateLimit': True
})

active_positions = []
daily_stats = {'pnl': 0.0, 'trades': 0, 'wins': 0}

def get_db_conn(): return sqlite3.connect(DB_PATH, check_same_thread=False)

def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = get_db_conn(); c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS active_positions (id INTEGER PRIMARY KEY, data TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS daily_stats (id INTEGER PRIMARY KEY, pnl REAL, trades INTEGER, wins INTEGER)''')
    
    try:
        c.execute("ALTER TABLE daily_stats ADD COLUMN wins INTEGER DEFAULT 0")
        logging.info("✅ База данных обновлена: добавлена колонка 'wins'")
    except sqlite3.OperationalError: pass
        
    conn.commit(); conn.close()

def save_positions():
    conn = get_db_conn(); c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO active_positions (id, data) VALUES (1, ?)", (json.dumps(active_positions),))
    c.execute("INSERT OR REPLACE INTO daily_stats (id, pnl, trades, wins) VALUES (1, ?, ?, ?)", 
              (daily_stats['pnl'], daily_stats['trades'], daily_stats.get('wins', 0)))
    conn.commit(); conn.close()

def load_positions():
    global active_positions, daily_stats
    try:
        conn = get_db_conn(); c = conn.cursor()
        c.execute("SELECT data FROM active_positions WHERE id = 1"); row = c.fetchone()
        if row: active_positions = json.loads(row[0])
        c.execute("SELECT pnl, trades, wins FROM daily_stats WHERE id = 1"); stat_row = c.fetchone()
        if stat_row: daily_stats['pnl'], daily_stats['trades'], daily_stats['wins'] = stat_row
        conn.close()
    except: pass

def calculate_ema(data, window):
    if len(data) < window: return data[-1]
    alpha = 2 / (window + 1)
    ema = data[0]
    for price in data[1:]: ema = (price * alpha) + (ema * (1 - alpha))
    return ema

def detect_smc_setup(sym, ohlcv, btc_trend, altseason):
    """SMC v5.2: Volume, BTC, Altseason, Premium/Discount + MTF (4H Trend)"""
    try:
        d = list(zip(*ohlcv))
        o, h, l, c, v = np.array(d[1], dtype=float), np.array(d[2], dtype=float), np.array(d[3], dtype=float), np.array(d[4], dtype=float), np.array(d[5], dtype=float)
        
        if len(c) < 200: return None
        
        current_price = c[-1]
        ema_200 = calculate_ema(c, 200)
        atr = np.mean(np.maximum(h[1:]-l[1:], np.maximum(np.abs(h[1:]-c[:-1]), np.abs(l[1:]-c[:-1])))[-24:])
        
        avg_volume = np.mean(v[-21:-1])

        # --- ОПРЕДЕЛЕНИЕ СТРУКТУРНОГО ИМПУЛЬСА (Premium / Discount) ---
        leg_high = np.max(h[-50:-1])
        leg_low = np.min(l[-50:-1])
        eq_level = leg_low + (leg_high - leg_low) * 0.5  

        # --- ЛОГИКА LONG ---
        allow_long = (btc_trend != 'Short') or altseason
        if current_price > ema_200 and allow_long:
            recent_highs = h[-15:-1]
            swing_high = np.max(recent_highs)
            
            is_valid_choch = (c[-1] > swing_high) and (v[-1] > avg_volume * 1.2)
            is_in_discount = current_price <= eq_level
            
            if is_valid_choch and is_in_discount:
                
                # --- ИНТЕГРАЦИЯ MTF (СИНХРОНИЗАЦИЯ С 4H ТРЕНДОМ) ---
                try:
                    # Запрашиваем 4H график ТОЛЬКО если 1H сетап идеален (экономия лимитов биржи)
                    ohlcv_4h = bingx.fetch_ohlcv(sym, timeframe='4h', limit=205)
                    c_4h = np.array([x[4] for x in ohlcv_4h], dtype=float)
                    if len(c_4h) >= 200:
                        ema_200_4h = calculate_ema(c_4h, 200)
                        if current_price < ema_200_4h:
                            return None  # Отмена входа: на старшем 4H графике идет медвежий тренд
                except Exception as e:
                    logging.error(f"MTF 4H Error {sym}: {e}")
                    return None
                # ----------------------------------------------------

                for i in range(len(c)-2, len(c)-15, -1):
                    if c[i] < o[i]:  
                        ob_low = l[i]
                        ob_high = h[i]
                        
                        if current_price > ob_high and (current_price - ob_high) < (atr * 0.5):
                            sl = ob_low - (atr * 0.3)  
                            tp1 = current_price + (current_price - sl) * 1.5
                            tp2 = current_price + (current_price - sl) * 3.0
                            
                            return {
                                'symbol': sym,
                                'direction': 'Long',
                                'entry_price': current_price,
                                'sl': round(sl, 6),
                                'tp1': round(tp1, 6),
                                'tp2': round(tp2, 6),
                                'pattern': 'MTF Sync + 1H OB + Discount'
                            }
                            break

        # --- ЛОГИКА SHORT ---
        if current_price < ema_200 and btc_trend != 'Long':
            recent_lows = l[-15:-1]
            swing_low = np.min(recent_lows)
            
            is_valid_choch_short = (c[-1] < swing_low) and (v[-1] > avg_volume * 1.2)
            is_in_premium = current_price >= eq_level
            
            if is_valid_choch_short and is_in_premium:
                
                # --- ИНТЕГРАЦИЯ MTF (СИНХРОНИЗАЦИЯ С 4H ТРЕНДОМ) ---
                try:
                    ohlcv_4h = bingx.fetch_ohlcv(sym, timeframe='4h', limit=205)
                    c_4h = np.array([x[4] for x in ohlcv_4h], dtype=float)
                    if len(c_4h) >= 200:
                        ema_200_4h = calculate_ema(c_4h, 200)
                        if current_price > ema_200_4h:
                            return None  # Отмена входа: на старшем 4H графике идет бычий тренд
                except Exception as e:
                    logging.error(f"MTF 4H Error {sym}: {e}")
                    return None
                # ----------------------------------------------------

                for i in range(len(c)-2, len(c)-15, -1):
                    if c[i] > o[i]:  
                        ob_high = h[i]
                        ob_low = l[i]
                        
                        if current_price < ob_low and (ob_low - current_price) < (atr * 0.5):
                            sl = ob_high + (atr * 0.3)
                            tp1 = current_price - (sl - current_price) * 1.5
                            tp2 = current_price - (sl - current_price) * 3.0
                            
                            return {
                                'symbol': sym,
                                'direction': 'Short',
                                'entry_price': current_price,
                                'sl': round(sl, 6),
                                'tp1': round(tp1, 6),
                                'tp2': round(tp2, 6),
                                'pattern': 'MTF Sync + 1H OB + Premium'
                            }
                            break
                            
    except Exception as e:
        logging.error(f"SMC Detector Error: {e}")
    return None

def generate_signal():
    global GLOBAL_STOP_UNTIL, last_signals
    if GLOBAL_STOP_UNTIL and datetime.now(timezone.utc) < GLOBAL_STOP_UNTIL: return None, None
    if len(active_positions) >= MAX_POSITIONS: return None, None
    
    try:
        # --- ГЛОБАЛЬНЫЙ ФИЛЬТР BTC И СИНТЕТИЧЕСКИЙ ДЕТЕКТОР АЛЬТСЕЗОНА ---
        btc_trend = None
        altseason = False
        try:
            # Получаем график BTC
            btc_ohlcv = bingx.fetch_ohlcv('BTC/USDT', timeframe=SMC_TIMEFRAME, limit=205)
            btc_c = np.array([x[4] for x in btc_ohlcv], dtype=float)
            btc_ema_200 = calculate_ema(btc_c, 200)
            btc_trend = 'Long' if btc_c[-1] > btc_ema_200 else 'Short'
            
            # Считаем силу Альтов (ETH/BTC)
            eth_ohlcv = bingx.fetch_ohlcv('ETH/USDT', timeframe=SMC_TIMEFRAME, limit=205)
            eth_c = np.array([x[4] for x in eth_ohlcv], dtype=float)
            
            # Высчитываем синтетический график ETH/BTC и его EMA
            eth_btc_ratio = eth_c / btc_c
            eth_btc_ema = calculate_ema(eth_btc_ratio, 200)
            altseason = True if eth_btc_ratio[-1] > eth_btc_ema else False
            
        except Exception as e:
            logging.error(f"Global Filter Error: {e}")

        markets = bingx.load_markets()
        tickers = bingx.fetch_tickers()
        
        valid_symbols = []
        for sym, m in markets.items():
            if m.get('type') != 'swap' or not m.get('active'): continue
            if any(kw in sym.upper() for kw in EXCLUDED_KEYWORDS): continue
            
            tick = tickers.get(sym)
            if not tick or not tick.get('quoteVolume') or float(tick['quoteVolume']) < MIN_VOLUME_USDT: continue
            
            ask, bid = float(tick.get('ask', 0)), float(tick.get('bid', 0))
            if ask > 0 and bid > 0:
                spread = (ask - bid) / ask
                if spread > MAX_SPREAD_PERCENT: continue
                
            base_coin = sym.split('/')[0].split('-')[0].split(':')[0]
            if any(pos['symbol'].split('/')[0].split('-')[0].split(':')[0] == base_coin for pos in active_positions): continue
                
            valid_symbols.append(sym)

        for sym in valid_symbols:
            clean_name = sym.split(':')[0]
            if clean_name in last_signals and (datetime.now(timezone.utc) - last_signals[clean_name] < timedelta(hours=4)):
                continue

            ohlcv = bingx.fetch_ohlcv(sym, timeframe=SMC_TIMEFRAME, limit=205)
            signal = detect_smc_setup(sym, ohlcv, btc_trend, altseason) # Передаем altseason
            
            if signal:
                logging.info(f"💎 SMC Сигнал найден: {clean_name}")
                return sym, signal
                
        alt_status = "🔥 АКТИВЕН" if altseason else "🔴 Выключен"
        logging.info(f"🔎 Сканирование: {len(valid_symbols)} монет. Тренд BTC: {btc_trend}. Альтсезон: {alt_status}. Сетапов: 0")
                
    except Exception as e: logging.error(f"Scan error: {e}")
    return None, None

def execute_trade(signal):
    sym = signal['symbol']
    clean_name = sym.split(':')[0]
    try:
        bal = bingx.fetch_balance()
        free_usdt = float(bal['USDT']['free'])
        
        risk_amount_usdt = free_usdt * RISK_PER_TRADE  
        sl_distance = abs(signal['entry_price'] - signal['sl'])
        if sl_distance == 0: return
        
        ideal_qty = risk_amount_usdt / sl_distance
        required_margin = (ideal_qty * signal['entry_price']) / LEVERAGE
        if required_margin > (free_usdt * 0.3): 
            ideal_qty = (free_usdt * 0.3 * LEVERAGE) / signal['entry_price']
            
        qty = float(bingx.amount_to_precision(sym, ideal_qty))
        if qty <= 0: return 

        pos_side = 'LONG' if signal['direction'] == 'Long' else 'SHORT'
        side = 'buy' if signal['direction'] == 'Long' else 'sell'
        
        bingx.set_leverage(LEVERAGE, sym, params={'side': pos_side})
        bingx.create_market_order(sym, side, qty, params={'positionSide': pos_side})
        sl_side = 'sell' if side == 'buy' else 'buy'
        sl_ord = bingx.create_order(sym, 'stop_market', sl_side, qty, params={
            'triggerPrice': signal['sl'], 'positionSide': pos_side, 'stopLossPrice': signal['sl']
        })
        
        # ДОБАВЛЕН ФЛАГ be_hit
        active_positions.append({
            'symbol': sym, 'direction': signal['direction'], 'mode': 'SMC',
            'entry_price': signal['entry_price'], 'initial_qty': qty, 
            'sl_price': signal['sl'], 'tp1': signal['tp1'], 'tp2': signal['tp2'], 
            'tp1_hit': False, 'tp2_hit': False, 'be_level': 0, # <-- ВОТ ЭТОТ КЛЮЧ
            'sl_order_id': sl_ord['id'], 'position_side': pos_side,
            'open_time': datetime.now(timezone.utc).isoformat()
        })
        
        last_signals[clean_name] = datetime.now(timezone.utc)
        save_positions()
        
        try:
            msg = (f"🎯 **ВХОД (Smart Money v5): {clean_name}**\nНаправление: **#{signal['direction'].upper()}**\n"
                   f"Паттерн: 1H OB + Volume CHoCH\n\n"
                   f"Цена: {signal['entry_price']}\nОбъем: {qty}\n"
                   f"SL: {signal['sl']:.4f}\nTP1 (50% BSL): {signal['tp1']:.4f}\nTP2 (Target): {signal['tp2']:.4f}")
            bot.send_message(GROUP_CHAT_ID, msg)
        except Exception as tg_error: 
            logging.error(f"Telegram notification error {clean_name}: {tg_error}")
            
    except Exception as e: 
        logging.error(f"Trade error {clean_name}: {e}")

def monitor_positions_job():
    global active_positions, daily_stats, CONSECUTIVE_LOSSES, GLOBAL_STOP_UNTIL
    if not active_positions: return
    
    updated = []
    try:
        positions_raw = bingx.fetch_positions()
        for pos in active_positions:
            sym = pos['symbol']
            curr = next((r for r in positions_raw if r['symbol'] == sym and float(r.get('contracts', 0)) > 0), None)
            
            # --- 1. ПРОВЕРКА НА ПОЛНОЕ ЗАКРЫТИЕ ---
            if not curr:
                ticker = bingx.fetch_ticker(sym); last_p = ticker['last']
                pnl = (last_p - pos['entry_price']) * pos['initial_qty'] if pos['direction'] == 'Long' else (pos['entry_price'] - last_p) * pos['initial_qty']
                daily_stats['trades'] += 1; daily_stats['pnl'] += pnl
                if pnl > 0:
                    daily_stats['wins'] += 1; CONSECUTIVE_LOSSES = 0
                    bot.send_message(GROUP_CHAT_ID, f"✅ **{sym.split(':')[0]} закрыта в плюс!**\nPNL: {pnl:.2f} USDT")
                else:
                    CONSECUTIVE_LOSSES += 1
                    bot.send_message(GROUP_CHAT_ID, f"🛑 **{sym.split(':')[0]} выбита по SL.**\nPNL: {pnl:.2f} USDT")
                    if CONSECUTIVE_LOSSES >= 3: 
                        GLOBAL_STOP_UNTIL = datetime.now(timezone.utc) + timedelta(hours=3)
                        bot.send_message(GROUP_CHAT_ID, "⚠️ **Drawdown Защита:** 3 минуса подряд. Отдыхаем 3 часа.")
                continue

            # --- 1.5 УМНЫЙ TIME STOP ---
            if not pos.get('tp1_hit'):
                if 'open_time' not in pos: pos['open_time'] = datetime.now(timezone.utc).isoformat()
                hours_passed = (datetime.now(timezone.utc) - datetime.fromisoformat(pos['open_time'])).total_seconds() / 3600
                
                ticker = bingx.fetch_ticker(sym); last_p = ticker['last']
                pnl = (last_p - pos['entry_price']) * float(curr['contracts']) if pos['direction'] == 'Long' else (pos['entry_price'] - last_p) * float(curr['contracts'])
                
                close_by_timeout, timeout_reason = False, ""
                if hours_passed >= TRADE_TIMEOUT_ANY_HOURS:
                    close_by_timeout, timeout_reason = True, f"{TRADE_TIMEOUT_ANY_HOURS}ч (Жесткий лимит)"
                elif hours_passed >= TRADE_TIMEOUT_PROFIT_HOURS and pnl > 0:
                    close_by_timeout, timeout_reason = True, f"{TRADE_TIMEOUT_PROFIT_HOURS}ч (В плюсе)"
                    
                if close_by_timeout:
                    try:
                        c_side = 'sell' if pos['direction'] == 'Long' else 'buy'
                        bingx.create_market_order(sym, c_side, float(curr['contracts']), params={'positionSide': pos['position_side']})
                        bingx.cancel_order(pos['sl_order_id'], sym)
                        daily_stats['trades'] += 1; daily_stats['pnl'] += pnl
                        bot.send_message(GROUP_CHAT_ID, f"⏳ **{sym.split(':')[0]} закрыта по ТАЙМАУТУ {timeout_reason}!**\nPNL: {pnl:.2f} USDT")
                        continue 
                    except: pass

            ohlcv = bingx.fetch_ohlcv(sym, timeframe='1m', limit=2)
            if not ohlcv: 
                updated.append(pos); continue
                
            high_p = max([float(candle[2]) for candle in ohlcv])
            low_p = min([float(candle[3]) for candle in ohlcv])
            is_long = pos['direction'] == 'Long'
            step = abs(pos['tp2'] - pos['tp1']) 
            c_side = 'sell' if is_long else 'buy'

            # --- 2. МИКРО-ТРЕЙЛИНГ ДО TP1 (Защита прибыли на волатильности) ---
            if not pos.get('tp1_hit'):
                dist_to_tp1 = pos['tp1'] - pos['entry_price']
                current_dist = (high_p - pos['entry_price']) if is_long else (pos['entry_price'] - low_p)
                progress = current_dist / abs(dist_to_tp1) if dist_to_tp1 != 0 else 0
                
                current_be_level = pos.get('be_level', 0)
                new_be_price = None
                level_msg = ""
                
                if progress >= 0.80 and current_be_level < 2:
                    new_be_price = pos['entry_price'] + (dist_to_tp1 * 0.40)
                    pos['be_level'] = 2
                    level_msg = "Цена почти у TP1. Стоп подтянут в хороший плюс (зафиксировано 40% пути)."
                elif progress >= 0.50 and current_be_level < 1:
                    new_be_price = pos['entry_price'] + (dist_to_tp1 * 0.05) 
                    pos['be_level'] = 1
                    level_msg = "Экватор пройден! Стоп переведен в Б/У + комиссия."

                if new_be_price is not None:
                    try:
                        bingx.cancel_order(pos['sl_order_id'], sym)
                        new_sl = bingx.create_order(sym, 'stop_market', c_side, pos['initial_qty'], params={
                            'triggerPrice': new_be_price, 'positionSide': pos['position_side'], 'stopLossPrice': new_be_price
                        })
                        pos['sl_order_id'], pos['current_sl'] = new_sl['id'], new_be_price
                        bot.send_message(GROUP_CHAT_ID, f"🛡 **{sym.split(':')[0]} Микро-трейлинг (до TP1)!**\n{level_msg}\nНовый SL: {new_be_price:.4f}")
                    except Exception as e: logging.error(f"Micro-Trailing Error {sym}: {e}")

            # --- 3. КАСАНИЕ TP1 (Фикс 40%) ---
            if not pos.get('tp1_hit'):
                if (is_long and high_p >= pos['tp1']) or (not is_long and low_p <= pos['tp1']):
                    try:
                        close_qty = float(bingx.amount_to_precision(sym, float(curr['contracts']) * 0.40))
                        if close_qty > 0:
                            bingx.create_market_order(sym, c_side, close_qty, params={'positionSide': pos['position_side']})
                            pos['initial_qty'] = float(curr['contracts']) - close_qty 
                        
                        bingx.cancel_order(pos['sl_order_id'], sym)
                        be_price = pos['entry_price'] * (1.0025 if is_long else 0.9975)
                        new_sl = bingx.create_order(sym, 'stop_market', c_side, pos['initial_qty'], params={'triggerPrice': be_price, 'positionSide': pos['position_side'], 'stopLossPrice': be_price})
                        
                        pos['tp1_hit'], pos['sl_order_id'], pos['current_sl'] = True, new_sl['id'], be_price
                        pos['trail_trigger'] = pos['tp2'] 
                        pos['be_level'] = 3 # Переводим уровень для следующего этапа трейлинга
                        bot.send_message(GROUP_CHAT_ID, f"💰 **{sym.split(':')[0]} TP1 достигнут!** Фиксация 40%, остаток в Б/У.")
                    except Exception as e: logging.error(f"TP1 Error {sym}: {e}")

            # --- 3.5 НОВОЕ: МИКРО-ТРЕЙЛИНГ ОТ TP1 ДО TP2 ---
            if pos.get('tp1_hit') and not pos.get('tp2_hit'):
                dist_tp1_to_tp2 = pos['tp2'] - pos['tp1']
                current_dist_from_tp1 = (high_p - pos['tp1']) if is_long else (pos['tp1'] - low_p)
                
                # Считаем прогресс ТОЛЬКО если цена выше TP1 (для лонгов)
                if current_dist_from_tp1 > 0:
                    progress_to_tp2 = current_dist_from_tp1 / abs(dist_tp1_to_tp2) if dist_tp1_to_tp2 != 0 else 0
                    current_be_level = pos.get('be_level', 3)
                    new_be_price = None
                    
                    # Если цена прошла 80% пути от TP1 до TP2
                    if progress_to_tp2 >= 0.80 and current_be_level < 4:
                        # Подтягиваем стоп на середину пути между TP1 и TP2
                        new_be_price = pos['tp1'] + (dist_tp1_to_tp2 * 0.50)
                        pos['be_level'] = 4
                        level_msg = "Цена почти у TP2! Стоп подтянут к экватору между TP1 и TP2."

                    if new_be_price is not None:
                        try:
                            bingx.cancel_order(pos['sl_order_id'], sym)
                            new_sl = bingx.create_order(sym, 'stop_market', c_side, pos['initial_qty'], params={
                                'triggerPrice': new_be_price, 'positionSide': pos['position_side'], 'stopLossPrice': new_be_price
                            })
                            pos['sl_order_id'], pos['current_sl'] = new_sl['id'], new_be_price
                            bot.send_message(GROUP_CHAT_ID, f"🛡 **{sym.split(':')[0]} Микро-трейлинг (до TP2)!**\n{level_msg}\nНовый SL: {new_be_price:.4f}")
                        except Exception as e: logging.error(f"Micro-Trailing TP2 Error {sym}: {e}")

            # --- 4. КАСАНИЕ TP2 (Фикс 50% остатка, Стоп на TP1) ---
            if pos.get('tp1_hit') and not pos.get('tp2_hit'):
                if (is_long and high_p >= pos['tp2']) or (not is_long and low_p <= pos['tp2']):
                    try:
                        close_qty = float(bingx.amount_to_precision(sym, pos['initial_qty'] * 0.50))
                        if close_qty > 0:
                            bingx.create_market_order(sym, c_side, close_qty, params={'positionSide': pos['position_side']})
                            pos['initial_qty'] -= close_qty 
                        
                        bingx.cancel_order(pos['sl_order_id'], sym)
                        # Стоп переносится на уровень TP1
                        new_sl = bingx.create_order(sym, 'stop_market', c_side, pos['initial_qty'], params={'triggerPrice': pos['tp1'], 'positionSide': pos['position_side'], 'stopLossPrice': pos['tp1']})
                        
                        pos['tp2_hit'], pos['sl_order_id'], pos['current_sl'] = True, new_sl['id'], pos['tp1']
                        pos['trail_trigger'] = pos['tp2'] + step if is_long else pos['tp2'] - step
                        bot.send_message(GROUP_CHAT_ID, f"🔥 **{sym.split(':')[0]} TP2 достигнут!** Стоп на уровне TP1.")
                    except Exception as e: logging.error(f"TP2 Error {sym}: {e}")

            # --- 5. ДИНАМИЧЕСКИЙ ТРЕЙЛИНГ (После TP2) ---
            if pos.get('tp2_hit'):
                tt = pos.get('trail_trigger')
                if (is_long and high_p >= tt) or (not is_long and low_p <= tt):
                    try:
                        bingx.cancel_order(pos['sl_order_id'], sym)
                        nsl = pos.get('current_sl') + step if is_long else pos.get('current_sl') - step
                        new_sl = bingx.create_order(sym, 'stop_market', c_side, pos['initial_qty'], params={'triggerPrice': nsl, 'positionSide': pos['position_side'], 'stopLossPrice': nsl})
                        
                        pos['sl_order_id'], pos['current_sl'] = new_sl['id'], nsl
                        pos['trail_trigger'] = tt + step if is_long else tt - step
                        bot.send_message(GROUP_CHAT_ID, f"📈 **{sym.split(':')[0]} Трейлинг!** SL подтянут к {nsl:.4f}")
                    except: pass

            updated.append(pos)
        active_positions = updated; save_positions()
    except Exception as e: logging.error(f"Monitor error: {e}")

@bot.message_handler(commands=['signal'])
def manual_signal(message):
    bot.reply_to(message, "🔎 Глубокое сканирование SMC (v5.0)...")
    def run():
        sym, data = generate_signal()
        if data: execute_trade(data)
        else: bot.send_message(message.chat.id, "❌ SMC сетапов сейчас нет.")
    Thread(target=run, daemon=True).start()

@bot.message_handler(commands=['stats'])
def send_stats(message):
    bot.reply_to(message, "⏳ Собираю данные с биржи...")
    def run_stats():
        try:
            wr = (daily_stats['wins']/daily_stats['trades']*100) if daily_stats['trades'] > 0 else 0
            msg = (f"📈 **СТАТИСТИКА ЗА ДЕНЬ:**\nСделок: {daily_stats['trades']}\nWR: {wr:.1f}%\n"
                   f"Зафиксировано: {daily_stats['pnl']:.2f} USDT\n\n📊 **ОТКРЫТЫЕ:**\n")

            if not active_positions:
                bot.send_message(message.chat.id, msg + "🟢 Нет активных сделок."); return

            tot_pnl = 0.0
            pos_raw = bingx.fetch_positions()
            for pos in active_positions:
                sym = pos['symbol']
                cl_name = sym.split(':')[0]
                d_icon = "🟩 LONG" if pos['direction'] == 'Long' else "🟥 SHORT"
                
                st = "⏳ В процессе"
                if pos.get('tp2_hit'): st = "🔥 TP2 (Трейл)"
                elif pos.get('tp1_hit'): st = "✅ TP1 (Б/У)"
                elif pos.get('be_hit'): st = "🛡 Ранний Б/У"

                curr = next((r for r in pos_raw if r['symbol'] == sym and float(r.get('contracts', 0)) > 0), None)
                if curr:
                    mp, qty = float(curr['markPrice']), float(curr['contracts'])
                    pnl = (mp - pos['entry_price']) * qty if pos['direction'] == 'Long' else (pos['entry_price'] - mp) * qty
                    tot_pnl += pnl
                    msg += f"{d_icon} **{cl_name}** | {st}\nВход: {pos['entry_price']} ➡️ {mp:.4f}\n{'🟢' if pnl>0 else '🔴'} PNL: **{pnl:.2f} $**\n\n"
            
            bot.send_message(message.chat.id, msg + f"💰 **ОБЩИЙ ПЛАВАЮЩИЙ PNL: {tot_pnl:.2f} $**")
        except: bot.send_message(message.chat.id, "❌ Ошибка БД.")
    Thread(target=run_stats, daemon=True).start()

@bot.message_handler(commands=['unblock'])
def manual_unblock(message):
    global GLOBAL_STOP_UNTIL, CONSECUTIVE_LOSSES
    if GLOBAL_STOP_UNTIL is not None or CONSECUTIVE_LOSSES > 0:
        GLOBAL_STOP_UNTIL = None
        CONSECUTIVE_LOSSES = 0
        bot.reply_to(message, "🔓 **Бот разблокирован!**\nDrawdown-защита снята, счетчик убытков обнулен. Возвращаюсь к активной охоте на графиках 🫡")
    else:
        bot.reply_to(message, "🟢 Бот и так работает в штатном режиме. Блокировок сейчас нет.")

app = Flask(__name__)
@app.route('/')
def index(): return "Bot v5.0 Active (SMC + BTC Sync)"

if __name__ == '__main__':
    init_db(); load_positions()
    scheduler = BackgroundScheduler()
    scheduler.add_job(lambda: Thread(target=monitor_positions_job).start(), 'interval', minutes=1)
    scheduler.add_job(lambda: Thread(target=lambda: (s := generate_signal()) and s[1] and execute_trade(s[1])).start(), 'interval', minutes=5)
    scheduler.start()
    bot.remove_webhook()
    Thread(target=bot.infinity_polling, kwargs={'skip_pending': True}, daemon=True).start()
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 10000)))
