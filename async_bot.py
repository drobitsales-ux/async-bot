import asyncio
import websockets
import json
import gzip
import io
import time
import os
import logging
import sqlite3
import ccxt.async_support as ccxt_async
import numpy as np
import telebot
from datetime import datetime, timezone, timedelta
from threading import Thread
from flask import Flask

# === НАСТРОЙКИ v6.0 Async (SMC + WSS Footprint) ===
DB_PATH = '/data/bot.db' 
TOKEN = os.getenv('TELEGRAM_TOKEN')
GROUP_CHAT_ID = int(os.getenv('GROUP_CHAT_ID', -1003407154454))
BINGX_API_KEY = os.getenv('BINGX_API_KEY')
BINGX_SECRET = os.getenv('BINGX_SECRET')
WSS_URL = "wss://open-api-swap.bingx.com/swap-market"

# Риск-менеджмент
RISK_PER_TRADE = 0.01       
MAX_POSITIONS = 3           
LEVERAGE = 10               
MAX_SPREAD_PERCENT = 0.002  
MIN_VOLUME_USDT = 5000000  

# Тайм-менеджмент
TRADE_TIMEOUT_PROFIT_HOURS = 24  
TRADE_TIMEOUT_ANY_HOURS = 48     
SMC_TIMEFRAME = '1h'        

EXCLUDED_KEYWORDS = [
    'NCCO', 'NCSI', 'NIKKEI', 'NASDAQ', 'SP500', 'GOLD', 'SILVER', 'AUT', 'XAU', 'PAXG', 'EUR', 'JPY',
    'PEPE', 'SHIB', '1000', 'FLOKI', 'DOGE', 'BONK', 'MEME', 'LUNC', 'USTC', 'WIF', 'POPCAT',
    'BTC/', 'ETH/', 'BNB/', 'SOL/', 'XRP/', 'ADA/', 'TRX/'
]

GLOBAL_STOP_UNTIL = None
CONSECUTIVE_LOSSES = 0
last_signals = {} 

# В оперативной памяти
HOT_LIST = {}  
ACTIVE_WSS_CONNECTIONS = set()
active_positions = []
daily_stats = {'pnl': 0.0, 'trades': 0, 'wins': 0}

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(message)s')
bot = telebot.TeleBot(TOKEN)

exchange = ccxt_async.bingx({
    'apiKey': BINGX_API_KEY,
    'secret': BINGX_SECRET,
    'options': {'defaultType': 'swap', 'marginMode': 'isolated'},
    'enableRateLimit': True
})

# --- БАЗА ДАННЫХ ---
def get_db_conn(): return sqlite3.connect(DB_PATH, check_same_thread=False)

def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = get_db_conn(); c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS active_positions (id INTEGER PRIMARY KEY, data TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS daily_stats (id INTEGER PRIMARY KEY, pnl REAL, trades INTEGER, wins INTEGER)''')
    try: c.execute("ALTER TABLE daily_stats ADD COLUMN wins INTEGER DEFAULT 0")
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

async def send_tg_msg(text):
    """Асинхронная отправка сообщений в Telegram"""
    try: await asyncio.to_thread(bot.send_message, GROUP_CHAT_ID, text)
    except Exception as e: logging.error(f"TG Error: {e}")

def calculate_ema(data, window):
    if len(data) < window: return data[-1]
    alpha = 2 / (window + 1)
    ema = data[0]
    for price in data[1:]: ema = (price * alpha) + (ema * (1 - alpha))
    return ema

# --- АСИНХРОННЫЙ РАДАР (REST API) ---
async def detect_smc_setup(sym, btc_trend, altseason):
    try:
        ohlcv = await exchange.fetch_ohlcv(sym, timeframe=SMC_TIMEFRAME, limit=205)
        if not ohlcv or len(ohlcv) < 200: return None
        
        d = list(zip(*ohlcv))
        o, h, l, c, v = np.array(d[1], dtype=float), np.array(d[2], dtype=float), np.array(d[3], dtype=float), np.array(d[4], dtype=float), np.array(d[5], dtype=float)
        
        current_price = c[-1]
        ema_200 = calculate_ema(c, 200)
        atr = np.mean(np.maximum(h[1:]-l[1:], np.maximum(np.abs(h[1:]-c[:-1]), np.abs(l[1:]-c[:-1])))[-24:])
        avg_volume = np.mean(v[-21:-1])

        leg_high, leg_low = np.max(h[-50:-1]), np.min(l[-50:-1])
        eq_level = leg_low + (leg_high - leg_low) * 0.5  

        # LONG
        allow_long = (btc_trend != 'Short') or altseason
        if current_price > ema_200 and allow_long:
            is_valid_choch = (c[-1] > np.max(h[-15:-1])) and (v[-1] > avg_volume * 1.2)
            if is_valid_choch and current_price <= eq_level:
                try:
                    ohlcv_4h = await exchange.fetch_ohlcv(sym, timeframe='4h', limit=205)
                    c_4h = np.array([x[4] for x in ohlcv_4h], dtype=float)
                    if len(c_4h) >= 200 and current_price < calculate_ema(c_4h, 200): return None
                except: return None

                for i in range(len(c)-2, len(c)-15, -1):
                    if c[i] < o[i]:  
                        ob_low, ob_high = l[i], h[i]
                        sl = ob_low - (atr * 0.3)  
                        tp1 = current_price + (current_price - sl) * 1.5
                        tp2 = current_price + (current_price - sl) * 3.0
                        return {'symbol': sym, 'direction': 'Long', 'entry_price': current_price, 'sl': round(sl, 6), 'tp1': round(tp1, 6), 'tp2': round(tp2, 6), 'ob_low': ob_low, 'ob_high': ob_high, 'pattern': '1H OB + Discount'}

        # SHORT
        if current_price < ema_200 and btc_trend != 'Long':
            is_valid_choch_short = (c[-1] < np.min(l[-15:-1])) and (v[-1] > avg_volume * 1.2)
            if is_valid_choch_short and current_price >= eq_level:
                try:
                    ohlcv_4h = await exchange.fetch_ohlcv(sym, timeframe='4h', limit=205)
                    c_4h = np.array([x[4] for x in ohlcv_4h], dtype=float)
                    if len(c_4h) >= 200 and current_price > calculate_ema(c_4h, 200): return None
                except: return None

                for i in range(len(c)-2, len(c)-15, -1):
                    if c[i] > o[i]:  
                        ob_high, ob_low = h[i], l[i]
                        sl = ob_high + (atr * 0.3)
                        tp1 = current_price - (sl - current_price) * 1.5
                        tp2 = current_price - (sl - current_price) * 3.0
                        return {'symbol': sym, 'direction': 'Short', 'entry_price': current_price, 'sl': round(sl, 6), 'tp1': round(tp1, 6), 'tp2': round(tp2, 6), 'ob_low': ob_low, 'ob_high': ob_high, 'pattern': '1H OB + Premium'}
    except Exception as e: logging.error(f"SMC Detector Error {sym}: {e}")
    return None

async def radar_task():
    global HOT_LIST, GLOBAL_STOP_UNTIL
    await asyncio.sleep(5)
    
    while True:
        try:
            if GLOBAL_STOP_UNTIL and datetime.now(timezone.utc) < GLOBAL_STOP_UNTIL:
                await asyncio.sleep(60); continue
            if len(active_positions) >= MAX_POSITIONS:
                await asyncio.sleep(60); continue

            btc_trend, altseason = 'Short', False
            try:
                btc_ohlcv = await exchange.fetch_ohlcv('BTC/USDT', timeframe=SMC_TIMEFRAME, limit=205)
                btc_c = np.array([x[4] for x in btc_ohlcv], dtype=float)
                btc_trend = 'Long' if btc_c[-1] > calculate_ema(btc_c, 200) else 'Short'
                
                eth_ohlcv = await exchange.fetch_ohlcv('ETH/USDT', timeframe=SMC_TIMEFRAME, limit=205)
                eth_c = np.array([x[4] for x in eth_ohlcv], dtype=float)
                altseason = True if (eth_c / btc_c)[-1] > calculate_ema((eth_c / btc_c), 200) else False
            except Exception as e: logging.error(f"Global Filter Error: {e}")

            markets = await exchange.load_markets()
            tickers = await exchange.fetch_tickers()
            
            valid_symbols = []
            for sym, m in markets.items():
                if m.get('type') != 'swap' or not m.get('active'): continue
                if any(kw in sym.upper() for kw in EXCLUDED_KEYWORDS): continue
                tick = tickers.get(sym)
                if not tick or not tick.get('quoteVolume') or float(tick['quoteVolume']) < MIN_VOLUME_USDT: continue
                
                ask, bid = float(tick.get('ask', 0)), float(tick.get('bid', 0))
                if ask > 0 and bid > 0 and ((ask - bid) / ask) <= MAX_SPREAD_PERCENT:
                    base_coin = sym.split('/')[0].split('-')[0].split(':')[0]
                    if not any(pos['symbol'].split('/')[0].split('-')[0].split(':')[0] == base_coin for pos in active_positions):
                        valid_symbols.append(sym)

            new_hot_list = {}
            for sym in valid_symbols:
                clean_name = sym.split(':')[0]
                if clean_name in last_signals and (datetime.now(timezone.utc) - last_signals[clean_name] < timedelta(hours=4)):
                    continue
                
                signal = await detect_smc_setup(sym, btc_trend, altseason)
                if signal:
                    ws_sym = sym.replace('/', '-')
                    new_hot_list[ws_sym] = signal
                    logging.info(f"🎯 Радар: {clean_name} добавлен в HOT_LIST.")

            HOT_LIST = new_hot_list

# --- ДОБАВИТЬ ЭТУ СТРОКУ ---
            logging.info(f"🔎 [РАДАР] Скан завершен. Монет на мушке (HOT_LIST): {len(HOT_LIST)}. Жду 5 минут...")
            
            await asyncio.sleep(300) # Радар спит 5 минут
            
        except Exception as e:
            logging.error(f"Radar error: {e}")
            await asyncio.sleep(60)

# --- АСИНХРОННЫЙ СНАЙПЕР И ЭКЗЕКЬЮТОР ---
async def execute_trade(signal):
    sym = signal['symbol']
    clean_name = sym.split(':')[0]
    try:
        bal = await exchange.fetch_balance()
        free_usdt = float(bal['USDT']['free'])
        
        risk_amount_usdt = free_usdt * RISK_PER_TRADE  
        sl_distance = abs(signal['entry_price'] - signal['sl'])
        if sl_distance == 0: return
        
        ideal_qty = risk_amount_usdt / sl_distance
        required_margin = (ideal_qty * signal['entry_price']) / LEVERAGE
        if required_margin > (free_usdt * 0.3): 
            ideal_qty = (free_usdt * 0.3 * LEVERAGE) / signal['entry_price']
            
        qty = float(exchange.amount_to_precision(sym, ideal_qty))
        if qty <= 0: return 

        pos_side = 'LONG' if signal['direction'] == 'Long' else 'SHORT'
        side = 'buy' if signal['direction'] == 'Long' else 'sell'
        
        await exchange.set_leverage(LEVERAGE, sym, params={'side': pos_side})
        await exchange.create_market_order(sym, side, qty, params={'positionSide': pos_side})
        
        sl_side = 'sell' if side == 'buy' else 'buy'
        sl_ord = await exchange.create_order(sym, 'stop_market', sl_side, qty, params={
            'triggerPrice': signal['sl'], 'positionSide': pos_side, 'stopLossPrice': signal['sl']
        })
        
        active_positions.append({
            'symbol': sym, 'direction': signal['direction'], 'mode': 'SMC_WSS',
            'entry_price': signal['entry_price'], 'initial_qty': qty, 
            'sl_price': signal['sl'], 'tp1': signal['tp1'], 'tp2': signal['tp2'], 
            'tp1_hit': False, 'tp2_hit': False, 'be_level': 0,
            'sl_order_id': sl_ord['id'], 'position_side': pos_side,
            'open_time': datetime.now(timezone.utc).isoformat()
        })
        
        last_signals[clean_name] = datetime.now(timezone.utc)
        await asyncio.to_thread(save_positions)
        
        msg = (f"🎯 **ВХОД (Снайпер v6.0): {clean_name}**\nНаправление: **#{signal['direction'].upper()}**\n"
               f"Паттерн: {signal['pattern']} + Footprint Confirm\n\n"
               f"Цена: {signal['entry_price']}\nОбъем: {qty}\n"
               f"SL: {signal['sl']:.4f}\nTP1: {signal['tp1']:.4f}\nTP2: {signal['tp2']:.4f}")
        await send_tg_msg(msg)
            
    except Exception as e: 
        logging.error(f"Trade error {clean_name}: {e}")

async def wss_sniper_worker(ws_sym, setup_data):
    buy_vol, sell_vol = 0.0, 0.0
    start_time = time.time()
    
    try:
        async for ws in websockets.connect(WSS_URL):
            await ws.send(json.dumps({"id": f"sub_{ws_sym}", "reqType": "sub", "dataType": f"{ws_sym}@trade"}))
            
            while ws_sym in HOT_LIST and len(active_positions) < MAX_POSITIONS:
                message = await ws.recv()
                data = json.loads(gzip.GzipFile(fileobj=io.BytesIO(message)).read().decode('utf-8'))

                if data.get("ping"):
                    await ws.send(json.dumps({"pong": data.get("ping")})); continue

                if "data" in data and isinstance(data["data"], list):
                    for trade in data["data"]:
                        price_str = trade.get("p") or trade.get("price")
                        qty_str = trade.get("q") or trade.get("v") or trade.get("amount") or trade.get("vol")
                        if not price_str or not qty_str: continue
                        
                        price, qty = float(price_str), float(qty_str)
                        trade_usdt = price * qty
                        is_sell = trade.get("m", False)
                        
                        if is_sell: sell_vol += trade_usdt
                        else: buy_vol += trade_usdt

                if time.time() - start_time >= 5:
                    total_vol = buy_vol + sell_vol
                    if total_vol > 5000: # Минимальный объем в кластере 5k
                        buy_pct = (buy_vol / total_vol) * 100
                        sell_pct = 100 - buy_pct
                        
                        in_zone = setup_data['ob_low'] <= price <= setup_data['ob_high']
                        
                        if in_zone:
                            if setup_data['direction'] == 'Long' and buy_pct >= 75:
                                await execute_trade(setup_data)
                                if ws_sym in HOT_LIST: del HOT_LIST[ws_sym]
                                break
                            elif setup_data['direction'] == 'Short' and sell_pct >= 75:
                                await execute_trade(setup_data)
                                if ws_sym in HOT_LIST: del HOT_LIST[ws_sym]
                                break
                            
                    buy_vol, sell_vol = 0.0, 0.0
                    start_time = time.time()
            break
    except Exception as e: logging.error(f"Sniper error {ws_sym}: {e}")
    finally: ACTIVE_WSS_CONNECTIONS.discard(ws_sym)

async def sniper_manager():
    while True:
        for ws_sym, setup_data in list(HOT_LIST.items()):
            if ws_sym not in ACTIVE_WSS_CONNECTIONS:
                ACTIVE_WSS_CONNECTIONS.add(ws_sym)
                asyncio.create_task(wss_sniper_worker(ws_sym, setup_data))
        await asyncio.sleep(5)

# --- АСИНХРОННЫЙ МОНИТОР ---
async def monitor_positions_job():
    global active_positions, daily_stats, CONSECUTIVE_LOSSES, GLOBAL_STOP_UNTIL
    while True:
        await asyncio.sleep(60)
        if not active_positions: continue
        
        updated = []
        try:
            positions_raw = await exchange.fetch_positions()
            for pos in active_positions:
                sym = pos['symbol']
                curr = next((r for r in positions_raw if r['symbol'] == sym and float(r.get('contracts', 0)) > 0), None)
                
                if not curr:
                    ticker = await exchange.fetch_ticker(sym); last_p = ticker['last']
                    pnl = (last_p - pos['entry_price']) * pos['initial_qty'] if pos['direction'] == 'Long' else (pos['entry_price'] - last_p) * pos['initial_qty']
                    daily_stats['trades'] += 1; daily_stats['pnl'] += pnl
                    if pnl > 0:
                        daily_stats['wins'] += 1; CONSECUTIVE_LOSSES = 0
                        await send_tg_msg(f"✅ **{sym.split(':')[0]} закрыта в плюс!**\nPNL: {pnl:.2f} USDT")
                    else:
                        CONSECUTIVE_LOSSES += 1
                        await send_tg_msg(f"🛑 **{sym.split(':')[0]} выбита по SL.**\nPNL: {pnl:.2f} USDT")
                        if CONSECUTIVE_LOSSES >= 3: 
                            GLOBAL_STOP_UNTIL = datetime.now(timezone.utc) + timedelta(hours=3)
                            await send_tg_msg("⚠️ **Drawdown Защита:** 3 минуса подряд. Отдыхаем 3 часа.")
                    continue

                if not pos.get('tp1_hit'):
                    if 'open_time' not in pos: pos['open_time'] = datetime.now(timezone.utc).isoformat()
                    hours_passed = (datetime.now(timezone.utc) - datetime.fromisoformat(pos['open_time'])).total_seconds() / 3600
                    
                    ticker = await exchange.fetch_ticker(sym); last_p = ticker['last']
                    pnl = (last_p - pos['entry_price']) * float(curr['contracts']) if pos['direction'] == 'Long' else (pos['entry_price'] - last_p) * float(curr['contracts'])
                    
                    close_by_timeout, timeout_reason = False, ""
                    if hours_passed >= TRADE_TIMEOUT_ANY_HOURS: close_by_timeout, timeout_reason = True, f"{TRADE_TIMEOUT_ANY_HOURS}ч"
                    elif hours_passed >= TRADE_TIMEOUT_PROFIT_HOURS and pnl > 0: close_by_timeout, timeout_reason = True, f"{TRADE_TIMEOUT_PROFIT_HOURS}ч (В плюсе)"
                        
                    if close_by_timeout:
                        try:
                            c_side = 'sell' if pos['direction'] == 'Long' else 'buy'
                            await exchange.create_market_order(sym, c_side, float(curr['contracts']), params={'positionSide': pos['position_side']})
                            await exchange.cancel_order(pos['sl_order_id'], sym)
                            daily_stats['trades'] += 1; daily_stats['pnl'] += pnl
                            await send_tg_msg(f"⏳ **{sym.split(':')[0]} закрыта по ТАЙМАУТУ {timeout_reason}!**\nPNL: {pnl:.2f} USDT")
                            continue 
                        except: pass

                ohlcv = await exchange.fetch_ohlcv(sym, timeframe='1m', limit=2)
                if not ohlcv: updated.append(pos); continue
                    
                high_p = max([float(candle[2]) for candle in ohlcv])
                low_p = min([float(candle[3]) for candle in ohlcv])
                is_long = pos['direction'] == 'Long'
                step = abs(pos['tp2'] - pos['tp1']) 
                c_side = 'sell' if is_long else 'buy'

                # Микро-трейлинг до TP1
                if not pos.get('tp1_hit'):
                    dist_to_tp1 = pos['tp1'] - pos['entry_price']
                    current_dist = (high_p - pos['entry_price']) if is_long else (pos['entry_price'] - low_p)
                    progress = current_dist / abs(dist_to_tp1) if dist_to_tp1 != 0 else 0
                    
                    current_be_level = pos.get('be_level', 0)
                    new_be_price = None
                    
                    if progress >= 0.80 and current_be_level < 2:
                        new_be_price, pos['be_level'], level_msg = pos['entry_price'] + (dist_to_tp1 * 0.40), 2, "Цена почти у TP1."
                    elif progress >= 0.50 and current_be_level < 1:
                        new_be_price, pos['be_level'], level_msg = pos['entry_price'] + (dist_to_tp1 * 0.05), 1, "Экватор пройден."

                    if new_be_price is not None:
                        try:
                            await exchange.cancel_order(pos['sl_order_id'], sym)
                            new_sl = await exchange.create_order(sym, 'stop_market', c_side, pos['initial_qty'], params={'triggerPrice': new_be_price, 'positionSide': pos['position_side'], 'stopLossPrice': new_be_price})
                            pos['sl_order_id'], pos['current_sl'] = new_sl['id'], new_be_price
                            await send_tg_msg(f"🛡 **{sym.split(':')[0]} Трейлинг!**\n{level_msg}\nНовый SL: {new_be_price:.4f}")
                        except: pass

                # TP1
                if not pos.get('tp1_hit'):
                    if (is_long and high_p >= pos['tp1']) or (not is_long and low_p <= pos['tp1']):
                        try:
                            close_qty = float(exchange.amount_to_precision(sym, float(curr['contracts']) * 0.40))
                            if close_qty > 0:
                                await exchange.create_market_order(sym, c_side, close_qty, params={'positionSide': pos['position_side']})
                                pos['initial_qty'] = float(curr['contracts']) - close_qty 
                            
                            await exchange.cancel_order(pos['sl_order_id'], sym)
                            be_price = pos['entry_price'] * (1.0025 if is_long else 0.9975)
                            new_sl = await exchange.create_order(sym, 'stop_market', c_side, pos['initial_qty'], params={'triggerPrice': be_price, 'positionSide': pos['position_side'], 'stopLossPrice': be_price})
                            pos['tp1_hit'], pos['sl_order_id'], pos['current_sl'], pos['trail_trigger'], pos['be_level'] = True, new_sl['id'], be_price, pos['tp2'], 3
                            await send_tg_msg(f"💰 **{sym.split(':')[0]} TP1 достигнут!** Фиксация 40%.")
                        except: pass

                # Микро-трейлинг TP1 -> TP2
                if pos.get('tp1_hit') and not pos.get('tp2_hit'):
                    dist_tp1_to_tp2 = pos['tp2'] - pos['tp1']
                    current_dist_from_tp1 = (high_p - pos['tp1']) if is_long else (pos['tp1'] - low_p)
                    
                    if current_dist_from_tp1 > 0:
                        progress_to_tp2 = current_dist_from_tp1 / abs(dist_tp1_to_tp2) if dist_tp1_to_tp2 != 0 else 0
                        if progress_to_tp2 >= 0.80 and pos.get('be_level', 3) < 4:
                            new_be_price = pos['tp1'] + (dist_tp1_to_tp2 * 0.50)
                            try:
                                await exchange.cancel_order(pos['sl_order_id'], sym)
                                new_sl = await exchange.create_order(sym, 'stop_market', c_side, pos['initial_qty'], params={'triggerPrice': new_be_price, 'positionSide': pos['position_side'], 'stopLossPrice': new_be_price})
                                pos['sl_order_id'], pos['current_sl'], pos['be_level'] = new_sl['id'], new_be_price, 4
                                await send_tg_msg(f"🛡 **{sym.split(':')[0]} Трейлинг до TP2!**\nНовый SL: {new_be_price:.4f}")
                            except: pass

                # TP2
                if pos.get('tp1_hit') and not pos.get('tp2_hit'):
                    if (is_long and high_p >= pos['tp2']) or (not is_long and low_p <= pos['tp2']):
                        try:
                            close_qty = float(exchange.amount_to_precision(sym, pos['initial_qty'] * 0.50))
                            if close_qty > 0:
                                await exchange.create_market_order(sym, c_side, close_qty, params={'positionSide': pos['position_side']})
                                pos['initial_qty'] -= close_qty 
                            
                            await exchange.cancel_order(pos['sl_order_id'], sym)
                            new_sl = await exchange.create_order(sym, 'stop_market', c_side, pos['initial_qty'], params={'triggerPrice': pos['tp1'], 'positionSide': pos['position_side'], 'stopLossPrice': pos['tp1']})
                            pos['tp2_hit'], pos['sl_order_id'], pos['current_sl'], pos['trail_trigger'] = True, new_sl['id'], pos['tp1'], pos['tp2'] + step if is_long else pos['tp2'] - step
                            await send_tg_msg(f"🔥 **{sym.split(':')[0]} TP2 достигнут!** Стоп на TP1.")
                        except: pass

                # Динамический Трейлинг
                if pos.get('tp2_hit'):
                    tt = pos.get('trail_trigger')
                    if (is_long and high_p >= tt) or (not is_long and low_p <= tt):
                        try:
                            await exchange.cancel_order(pos['sl_order_id'], sym)
                            nsl = pos.get('current_sl') + step if is_long else pos.get('current_sl') - step
                            new_sl = await exchange.create_order(sym, 'stop_market', c_side, pos['initial_qty'], params={'triggerPrice': nsl, 'positionSide': pos['position_side'], 'stopLossPrice': nsl})
                            pos['sl_order_id'], pos['current_sl'], pos['trail_trigger'] = new_sl['id'], nsl, tt + step if is_long else tt - step
                            await send_tg_msg(f"📈 **{sym.split(':')[0]} Трейлинг!** SL подтянут к {nsl:.4f}")
                        except: pass

                updated.append(pos)
            active_positions = updated
            await asyncio.to_thread(save_positions)
        except Exception as e: logging.error(f"Monitor error: {e}")

# --- TELEGRAM ---
@bot.message_handler(commands=['stats'])
def send_stats(message):
    bot.reply_to(message, "⏳ Сбор данных... (В асинхронной версии ответ придет чуть позже)")

app = Flask(__name__)
@app.route('/')
def index(): return "Async Bot v6.0 Active"

async def main():
    init_db(); load_positions()
    logging.info("🚀 Запуск асинхронного ядра SMC + WSS Footprint...")
    await asyncio.gather(
        radar_task(),
        sniper_manager(),
        monitor_positions_job()
    )

if __name__ == '__main__':
    Thread(target=lambda: app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 10000))), daemon=True).start()
    # Отключаем прослушивание команд, чтобы не конфликтовать со старым ботом
    # Thread(target=bot.infinity_polling, kwargs={'skip_pending': True}, daemon=True).start() 
    try: asyncio.run(main())
    except KeyboardInterrupt: logging.info("\nОстановка.")
