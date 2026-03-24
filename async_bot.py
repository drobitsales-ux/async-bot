import asyncio
import websockets
import json
import gzip
import os
import logging
import sqlite3
import gc
import ccxt.async_support as ccxt_async
import numpy as np
import aiohttp
from datetime import datetime, timezone, timedelta
from threading import Thread
from http.server import BaseHTTPRequestHandler, HTTPServer

# === НАСТРОЙКИ v7.3.1 (Ultra-Light Memory + Top-150 Limit) ===
DB_PATH = '/data/bot.db' 
TOKEN = os.getenv('TELEGRAM_TOKEN')
GROUP_CHAT_ID = int(os.getenv('GROUP_CHAT_ID', -1003407154454))
BINGX_API_KEY = os.getenv('BINGX_API_KEY')
BINGX_SECRET = os.getenv('BINGX_SECRET')

RISK_PER_TRADE = 0.01       
MAX_POSITIONS = 3           
LEVERAGE = 10               
MAX_SPREAD_PERCENT = 0.002  
MIN_VOLUME_USDT = 2000000  

TRADE_TIMEOUT_PROFIT_HOURS = 24  
TRADE_TIMEOUT_ANY_HOURS = 48     
SMC_TIMEFRAME = '1h'

EXCLUDED_KEYWORDS = ['NCCO', 'NCSI', 'NIKKEI', 'NASDAQ', 'SP500', 'GOLD', 'SILVER', 'AUT', 'XAU', 'PAXG', 'EUR', 'JPY', 'PEPE', 'SHIB', '1000', 'FLOKI', 'DOGE', 'BONK', 'MEME', 'LUNC', 'USTC', 'WIF', 'POPCAT', 'BTC/', 'ETH/', 'BNB/', 'SOL/', 'XRP/', 'ADA/', 'TRX/']

GLOBAL_STOP_UNTIL = None
CONSECUTIVE_LOSSES = 0
last_signals = {} 
NOTIFIED_SYMBOLS = set() 

HOT_LIST = {}  
ACTIVE_WSS_CONNECTIONS = set()
active_positions = []
daily_stats = {'pnl': 0.0, 'trades': 0, 'wins': 0}

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(message)s')

exchange = ccxt_async.bingx({
    'apiKey': BINGX_API_KEY, 'secret': BINGX_SECRET,
    'options': {'defaultType': 'swap', 'marginMode': 'isolated'}, 'enableRateLimit': True
})

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
    c.execute("INSERT OR REPLACE INTO daily_stats (id, pnl, trades, wins) VALUES (1, ?, ?, ?)", (daily_stats['pnl'], daily_stats['trades'], daily_stats.get('wins', 0)))
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
    except Exception as e: 
        logging.error(f"Ошибка загрузки БД: {e}")

async def send_tg_msg(text):
    if not TOKEN: return
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    payload = {"chat_id": GROUP_CHAT_ID, "text": text, "parse_mode": "Markdown"}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload) as resp:
                pass
    except Exception as e: 
        logging.error(f"TG Error: {e}")

def calculate_ema(data, window):
    if len(data) < window: return data[-1]
    alpha = 2 / (window + 1)
    ema = data[0]
    for price in data[1:]: ema = (price * alpha) + (ema * (1 - alpha))
    return ema

def find_fvg(h, l, direction):
    for i in range(len(h)-1, len(h)-6, -1):
        if direction == 'Long' and l[i] > h[i-2]: return True
        elif direction == 'Short' and h[i] < l[i-2]: return True
    return False

# --- РАДАР И ОПТИМИЗИРОВАННЫЙ SMC ---
async def detect_smc_setup(sym, btc_trend, altseason):
    try:
        ohlcv = await exchange.fetch_ohlcv(sym, timeframe=SMC_TIMEFRAME, limit=205)
        if not ohlcv or len(ohlcv) < 200: return None
        
        d = list(zip(*ohlcv))
        o, h, l, c, v = np.array(d[1], dtype=float), np.array(d[2], dtype=float), np.array(d[3], dtype=float), np.array(d[4], dtype=float), np.array(d[5], dtype=float)
        
        current_price = c[-1]
        ema_200 = calculate_ema(c, 200)
        atr = np.mean(np.maximum(h[1:]-l[1:], np.maximum(np.abs(h[1:]-c[:-1]), np.abs(l[1:]-c[:-1])))[-24:])
        
        avg_volume = np.mean(v[-22:-2])
        volume_threshold = avg_volume * 1.0 
        has_volume = (v[-1] > volume_threshold) or (v[-2] > volume_threshold)

        leg_high, leg_low = np.max(h[-50:-1]), np.min(l[-50:-1])
        
        eq_level_long = leg_low + (leg_high - leg_low) * 0.6  
        eq_level_short = leg_low + (leg_high - leg_low) * 0.4 

        result = None
        if current_price > ema_200:
            is_valid_choch = (c[-1] > np.max(h[-6:-1])) and has_volume
            if is_valid_choch and current_price <= eq_level_long:
                if find_fvg(h, l, 'Long'):
                    for i in range(len(c)-2, len(c)-6, -1):
                        if c[i] < o[i]:  
                            ob_low, ob_high = l[i], h[i]
                            if current_price > ob_high and (current_price - ob_high) < (atr * 0.5):
                                sl = ob_low - (atr * 0.3)  
                                result = {'symbol': sym, 'direction': 'Long', 'entry_price': current_price, 'sl': sl, 'tp1': current_price + (current_price-sl)*1.5, 'tp2': current_price + (current_price-sl)*3, 'ob_low': ob_low, 'ob_high': ob_high, 'pattern': '1H OB + FVG'}
                                break

        if not result and current_price < ema_200:
            is_valid_choch_short = (c[-1] < np.min(l[-6:-1])) and has_volume
            if is_valid_choch_short and current_price >= eq_level_short:
                if find_fvg(h, l, 'Short'):
                    for i in range(len(c)-2, len(c)-6, -1):
                        if c[i] > o[i]:  
                            ob_high, ob_low = h[i], l[i]
                            if current_price < ob_low and (ob_low - current_price) < (atr * 0.5):
                                sl = ob_high + (atr * 0.3)
                                result = {'symbol': sym, 'direction': 'Short', 'entry_price': current_price, 'sl': sl, 'tp1': current_price - (sl-current_price)*1.5, 'tp2': current_price - (sl-current_price)*3, 'ob_low': ob_low, 'ob_high': ob_high, 'pattern': '1H OB + FVG'}
                                break
        
        del o, h, l, c, v, ohlcv, d
        return result
    except Exception as e: 
        logging.error(f"SMC Error {sym}: {e}")
        return None

async def radar_task():
    global HOT_LIST, NOTIFIED_SYMBOLS
    await exchange.load_markets() 
    await asyncio.sleep(5)
    while True:
        try:
            if GLOBAL_STOP_UNTIL and datetime.now(timezone.utc) < GLOBAL_STOP_UNTIL:
                await asyncio.sleep(60); continue
            if len(active_positions) >= MAX_POSITIONS:
                await asyncio.sleep(60); continue

            tickers = await exchange.fetch_tickers()
            temp_symbols = []
            
            for sym, m in exchange.markets.items():
                if m.get('type') != 'swap' or not m.get('active'): continue
                if any(kw in sym.upper() for kw in EXCLUDED_KEYWORDS): continue
                
                tick = tickers.get(sym)
                vol = float(tick.get('quoteVolume', 0)) if tick else 0
                if vol < MIN_VOLUME_USDT: continue
                
                base_coin = sym.split('/')[0].split('-')[0].split(':')[0]
                if not any(pos['symbol'].split('/')[0].split('-')[0].split(':')[0] == base_coin for pos in active_positions):
                    temp_symbols.append((sym, vol))

            del tickers 
            
            # ОПТИМИЗАЦИЯ: Берем ТОП-150 монет для увеличения числа сигналов
            temp_symbols.sort(key=lambda x: x[1], reverse=True)
            valid_symbols = [x[0] for x in temp_symbols[:150]]

            new_hot_list = {}
            for sym in valid_symbols:
                clean_name = sym.split(':')[0]
                if clean_name in last_signals and (datetime.now(timezone.utc) - last_signals[clean_name] < timedelta(hours=4)): continue
                
                signal = await detect_smc_setup(sym, 'Long', True) 
                if signal:
                    new_hot_list[sym] = signal
                    if sym not in HOT_LIST and sym not in NOTIFIED_SYMBOLS:
                        NOTIFIED_SYMBOLS.add(sym)
                        await send_tg_msg(f"🎯 **РАДАР:** {clean_name} взят на мушку!\nЗапускаю Bybit+BingX Снайпер...")
                
                # Защитный сон 0.2 сек для 150 монет, чтобы память не "прыгала"
                await asyncio.sleep(0.2) 

            HOT_LIST = new_hot_list
            NOTIFIED_SYMBOLS = {s for s in NOTIFIED_SYMBOLS if s in HOT_LIST}
            
            logging.info(f"🔎 [РАДАР] Скан завершен (Топ-150 ликвидных). На мушке: {len(HOT_LIST)}")
            gc.collect() 
            await asyncio.sleep(300) 
        except Exception as e:
            logging.error(f"Radar error: {e}"); await asyncio.sleep(60)

# --- ЭКЗЕКЬЮТОР ---
async def execute_trade(signal):
    sym = signal['symbol']
    clean_name = sym.split(':')[0]
    
    if any(p['symbol'] == sym for p in active_positions):
        return

    try:
        bal = await exchange.fetch_balance()
        free_usdt = float(bal['USDT']['free'])
        
        risk_multiplier = signal.get('dynamic_risk', 1.0)
        actual_risk_percent = RISK_PER_TRADE * risk_multiplier
        
        ideal_qty = (free_usdt * actual_risk_percent) / abs(signal['entry_price'] - signal['sl'])
        qty = float(exchange.amount_to_precision(sym, ideal_qty))
        if qty <= 0: return 

        sl_price = float(exchange.price_to_precision(sym, signal['sl']))
        tp1_price = float(exchange.price_to_precision(sym, signal['tp1']))
        tp2_price = float(exchange.price_to_precision(sym, signal['tp2']))

        pos_side = 'LONG' if signal['direction'] == 'Long' else 'SHORT'
        side = 'buy' if signal['direction'] == 'Long' else 'sell'
        sl_side = 'sell' if side == 'buy' else 'buy'
        
        await exchange.set_leverage(LEVERAGE, sym, params={'side': pos_side})
        await exchange.create_market_order(sym, side, qty, params={'positionSide': pos_side})
        sl_ord = await exchange.create_order(sym, 'stop_market', sl_side, qty, params={'triggerPrice': sl_price, 'positionSide': pos_side, 'stopLossPrice': sl_price})
        
        active_positions.append({
            'symbol': sym, 'direction': signal['direction'], 'entry_price': signal['entry_price'], 'initial_qty': qty, 
            'sl_price': sl_price, 'tp1': tp1_price, 'tp2': tp2_price, 
            'tp1_hit': False, 'tp2_hit': False, 'pre_tp1_hit': False, 'pre_tp2_hit': False,
            'sl_order_id': sl_ord['id'], 'position_side': pos_side, 'open_time': datetime.now(timezone.utc).isoformat()
        })
        
        last_signals[clean_name] = datetime.now(timezone.utc)
        await asyncio.to_thread(save_positions)
        
        msg = (f"💥 **ВЫСТРЕЛ (Bybit+BingX): {clean_name}**\nНаправление: **#{signal['direction'].upper()}**\n"
               f"Рейтинг: {signal.get('score_info', 'Базовый')} (Риск: {actual_risk_percent*100:.1f}%)\n"
               f"📊 {signal.get('vol_info', 'Нет данных')}\n\n"
               f"Цена: {signal['entry_price']}\nОбъем: {qty}\nSL: {sl_price}\nTP1: {tp1_price}")
        await send_tg_msg(msg)
    except Exception as e: 
        logging.error(f"Trade error {clean_name}: {e}")

# --- MULTI-EXCHANGE SNIPER CORE ---
async def wss_sniper_worker(sym, setup_data):
    base_coin = sym.split('/')[0].split('-')[0].split(':')[0]
    sym_bingx = sym.replace('/', '-')
    sym_bybit = f"{base_coin}USDT"

    state = {'buy_vol': 0.0, 'sell_vol': 0.0, 'current_price': setup_data['entry_price'], 'active': True}

    async def l_bingx():
        url = "wss://open-api-swap.bingx.com/swap-market"
        while state['active']:
            try:
                async for ws in websockets.connect(url, max_size=1048576, max_queue=16):
                    await ws.send(json.dumps({"id": "1", "reqType": "sub", "dataType": f"{sym_bingx}@trade"}))
                    while state['active']:
                        msg = await ws.recv()
                        data = json.loads(gzip.decompress(msg).decode('utf-8'))
                        if data.get("ping"): await ws.send(json.dumps({"pong": data.get("ping")})); continue
                        if "data" in data:
                            for t in data["data"]:
                                v = float(t['p']) * float(t['q'])
                                if t.get('m'): state['sell_vol'] += v
                                else: state['buy_vol'] += v
                            state['current_price'] = float(data["data"][-1]['p'])
            except Exception as e: 
                logging.debug(f"BingX WSS Reconnect: {e}"); await asyncio.sleep(1)

    async def l_bybit():
        url = "wss://stream.bybit.com/v5/public/linear"
        while state['active']:
            try:
                async for ws in websockets.connect(url, max_size=1048576, max_queue=16):
                    await ws.send(json.dumps({"op": "subscribe", "args": [f"publicTrade.{sym_bybit}"]}))
                    while state['active']:
                        msg = await ws.recv()
                        data = json.loads(msg)
                        if "data" in data and isinstance(data["data"], list):
                            for t in data["data"]:
                                v = float(t['p']) * float(t['v'])
                                if t['S'] == "Sell": state['sell_vol'] += v
                                else: state['buy_vol'] += v
            except Exception as e: 
                logging.debug(f"Bybit WSS Reconnect: {e}"); await asyncio.sleep(1)

    tasks = [asyncio.create_task(l_bingx()), asyncio.create_task(l_bybit())]

    try:
        while sym in HOT_LIST and len(active_positions) < MAX_POSITIONS:
            await asyncio.sleep(5)
            total = state['buy_vol'] + state['sell_vol']
            
            if total > 2500:
                buy_pct = (state['buy_vol'] / total) * 100
                sell_pct = 100 - buy_pct
                
                ob_range = abs(setup_data['ob_high'] - setup_data['ob_low'])
                in_zone = (setup_data['ob_low'] - ob_range*0.3) <= state['current_price'] <= (setup_data['ob_high'] + ob_range*0.3)
                
                if in_zone:
                    is_long = (setup_data['direction'] == 'Long' and buy_pct >= 75)
                    is_short = (setup_data['direction'] == 'Short' and sell_pct >= 75)
                    
                    if is_long or is_short:
                        if sym in HOT_LIST: del HOT_LIST[sym]
                            
                        dom_pct = buy_pct if setup_data['direction'] == 'Long' else sell_pct
                        
                        if dom_pct >= 90: setup_data['dynamic_risk'], setup_data['score_info'] = 2.5, f"10/10 🔥 ({dom_pct:.1f}%)"
                        elif dom_pct >= 85: setup_data['dynamic_risk'], setup_data['score_info'] = 1.5, f"8/10 ⚡ ({dom_pct:.1f}%)"
                        else: setup_data['dynamic_risk'], setup_data['score_info'] = 1.0, f"6/10 📊 ({dom_pct:.1f}%)"

                        setup_data['vol_info'] = f"Объем за 5с: ${total:.0f} ({dom_pct:.0f}% {'покупок' if setup_data['direction']=='Long' else 'продаж'})"
                        await execute_trade(setup_data)
                        break
            state['buy_vol'], state['sell_vol'] = 0.0, 0.0
    finally:
        state['active'] = False
        for t in tasks: t.cancel()
        ACTIVE_WSS_CONNECTIONS.discard(sym)

async def sniper_manager():
    while True:
        for sym, setup_data in list(HOT_LIST.items()):
            if sym not in ACTIVE_WSS_CONNECTIONS:
                ACTIVE_WSS_CONNECTIONS.add(sym)
                asyncio.create_task(wss_sniper_worker(sym, setup_data))
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
                        except Exception as e:
                            logging.error(f"Ошибка закрытия по таймауту {sym}: {e}")
                            updated.append(pos)
                            continue

                ohlcv = await exchange.fetch_ohlcv(sym, timeframe='1m', limit=2)
                if not ohlcv: 
                    updated.append(pos)
                    continue
                    
                high_p = max([float(candle[2]) for candle in ohlcv])
                low_p = min([float(candle[3]) for candle in ohlcv])
                is_long = pos['direction'] == 'Long'
                c_side = 'sell' if is_long else 'buy'

                if not pos.get('tp1_hit') and not pos.get('pre_tp1_hit'):
                    dist_to_tp1 = pos['tp1'] - pos['entry_price']
                    tp1_80 = pos['entry_price'] + dist_to_tp1 * 0.8 if is_long else pos['entry_price'] - dist_to_tp1 * 0.8
                    if (is_long and high_p >= tp1_80) or (not is_long and low_p <= tp1_80):
                        try:
                            await exchange.cancel_order(pos['sl_order_id'], sym)
                            raw_be = pos['entry_price'] * (1.001 if is_long else 0.999)
                            be_price = float(exchange.price_to_precision(sym, raw_be))
                            new_sl = await exchange.create_order(sym, 'stop_market', c_side, pos['initial_qty'], params={'triggerPrice': be_price, 'positionSide': pos['position_side'], 'stopLossPrice': be_price})
                            pos['sl_order_id'] = new_sl['id']
                            pos['current_sl'] = be_price
                            pos['pre_tp1_hit'] = True
                            await send_tg_msg(f"🛡 **{sym.split(':')[0]} Защита!** Цена прошла 80% к TP1.\nСтоп переведен в БУ: {be_price}")
                        except Exception as e: logging.error(f"Error pre-TP1 {sym}: {e}")

                if not pos.get('tp1_hit'):
                    if (is_long and high_p >= pos['tp1']) or (not is_long and low_p <= pos['tp1']):
                        try:
                            close_qty = float(exchange.amount_to_precision(sym, float(curr['contracts']) * 0.40))
                            if close_qty > 0:
                                await exchange.create_market_order(sym, c_side, close_qty, params={'positionSide': pos['position_side']})
                                pos['initial_qty'] = float(curr['contracts']) - close_qty 
                            
                            await exchange.cancel_order(pos['sl_order_id'], sym)
                            raw_be = pos['entry_price'] * (1.001 if is_long else 0.999)
                            be_price = float(exchange.price_to_precision(sym, raw_be))
                            new_sl = await exchange.create_order(sym, 'stop_market', c_side, pos['initial_qty'], params={'triggerPrice': be_price, 'positionSide': pos['position_side'], 'stopLossPrice': be_price})
                            
                            pos['tp1_hit'] = True
                            pos['sl_order_id'] = new_sl['id']
                            pos['current_sl'] = be_price
                            await send_tg_msg(f"💰 **{sym.split(':')[0]} TP1 достигнут!** Фиксация 40%.")
                        except Exception as e: logging.error(f"Ошибка выполнения TP1 для {sym}: {e}")

                if pos.get('tp1_hit') and not pos.get('tp2_hit') and not pos.get('pre_tp2_hit'):
                    dist_to_tp2 = pos['tp2'] - pos['tp1']
                    tp2_80 = pos['tp1'] + dist_to_tp2 * 0.8 if is_long else pos['tp1'] - dist_to_tp2 * 0.8
                    if (is_long and high_p >= tp2_80) or (not is_long and low_p <= tp2_80):
                        try:
                            await exchange.cancel_order(pos['sl_order_id'], sym)
                            safe_price = pos['tp1'] + (dist_to_tp2 * 0.2) if is_long else pos['tp1'] - (dist_to_tp2 * 0.2)
                            safe_p = float(exchange.price_to_precision(sym, safe_price))
                            new_sl = await exchange.create_order(sym, 'stop_market', c_side, pos['initial_qty'], params={'triggerPrice': safe_p, 'positionSide': pos['position_side'], 'stopLossPrice': safe_p})
                            pos['sl_order_id'] = new_sl['id']
                            pos['current_sl'] = safe_p
                            pos['pre_tp2_hit'] = True
                            await send_tg_msg(f"🛡 **{sym.split(':')[0]} Защита!** Цена прошла 80% к TP2.\nСтоп подтянут в профит: {safe_p}")
                        except Exception as e: logging.error(f"Error pre-TP2 {sym}: {e}")

                if pos.get('tp1_hit') and not pos.get('tp2_hit'):
                    if (is_long and high_p >= pos['tp2']) or (not is_long and low_p <= pos['tp2']):
                        try:
                            close_qty = float(exchange.amount_to_precision(sym, float(curr['contracts']) * 0.50))
                            if close_qty > 0:
                                await exchange.create_market_order(sym, c_side, close_qty, params={'positionSide': pos['position_side']})
                                pos['initial_qty'] -= close_qty 
                            
                            await exchange.cancel_order(pos['sl_order_id'], sym)
                            tp1_safe = float(exchange.price_to_precision(sym, pos['tp1']))
                            new_sl = await exchange.create_order(sym, 'stop_market', c_side, pos['initial_qty'], params={'triggerPrice': tp1_safe, 'positionSide': pos['position_side'], 'stopLossPrice': tp1_safe})
                            
                            micro_step = abs(pos['tp1'] - pos['entry_price']) * 0.5
                            pos['tp2_hit'] = True
                            pos['sl_order_id'] = new_sl['id']
                            pos['current_sl'] = tp1_safe
                            pos['micro_step'] = micro_step
                            pos['trail_trigger'] = pos['tp2'] + micro_step if is_long else pos['tp2'] - micro_step
                            
                            await send_tg_msg(f"🔥 **{sym.split(':')[0]} TP2 достигнут!** Фиксация 50% остатка.\nСтоп перенесен на TP1: {tp1_safe}\nВключен Микро-Трейлинг!")
                        except Exception as e: logging.error(f"Ошибка выполнения TP2 для {sym}: {e}")

                if pos.get('tp2_hit'):
                    tt = pos.get('trail_trigger')
                    m_step = pos.get('micro_step')
                    if (is_long and high_p >= tt) or (not is_long and low_p <= tt):
                        try:
                            await exchange.cancel_order(pos['sl_order_id'], sym)
                            raw_nsl = pos['current_sl'] + m_step if is_long else pos['current_sl'] - m_step
                            nsl = float(exchange.price_to_precision(sym, raw_nsl))
                            new_sl = await exchange.create_order(sym, 'stop_market', c_side, pos['initial_qty'], params={'triggerPrice': nsl, 'positionSide': pos['position_side'], 'stopLossPrice': nsl})
                            
                            pos['sl_order_id'] = new_sl['id']
                            pos['current_sl'] = nsl
                            pos['trail_trigger'] = tt + m_step if is_long else tt - m_step
                            await send_tg_msg(f"📈 **{sym.split(':')[0]} Микро-Трейлинг!** SL подтянут к {nsl}")
                        except Exception as e: logging.error(f"Ошибка трейлинга SL для {sym}: {e}")

                updated.append(pos)
            active_positions = updated
            await asyncio.to_thread(save_positions)
        except Exception as e: 
            logging.error(f"Критическая ошибка Монитора: {e}")

# --- УЛЬТРА-ЛЕГКИЙ ВЕБ-СЕРВЕР ---
class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"Bot v7.3.1 Active (Top-150 Limit, Memory Optimized)")
    def log_message(self, format, *args): return 

def run_server():
    server = HTTPServer(('0.0.0.0', int(os.environ.get('PORT', 10000))), HealthCheckHandler)
    server.serve_forever()

async def main():
    init_db(); load_positions()
    logging.info("🚀 Запуск ядра v7.3.1: Ultra-Light Memory + Top-150 Limit...")
    await asyncio.gather(radar_task(), sniper_manager(), monitor_positions_job())

if __name__ == '__main__':
    Thread(target=run_server, daemon=True).start()
    try: asyncio.run(main())
    except KeyboardInterrupt: logging.info("\nОстановка.")
