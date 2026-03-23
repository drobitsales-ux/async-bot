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

# === НАСТРОЙКИ v7.0 Async (Multi-Exchange WSS + Test Mode) ===
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

HOT_LIST = {}  
ACTIVE_WSS_CONNECTIONS = set()
active_positions = []
daily_stats = {'pnl': 0.0, 'trades': 0, 'wins': 0}

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(message)s')
bot = telebot.TeleBot(TOKEN)

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
    except: pass

async def send_tg_msg(text):
    try: await asyncio.to_thread(bot.send_message, GROUP_CHAT_ID, text)
    except Exception as e: logging.error(f"TG Error: {e}")

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

# --- РАДАР (ТЕСТОВЫЙ РЕЖИМ) ---
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
        volume_threshold = avg_volume * 1.0 # Тестовый фильтр (1.0)
        has_volume = (v[-1] > volume_threshold) or (v[-2] > volume_threshold)

        leg_high, leg_low = np.max(h[-50:-1]), np.min(l[-50:-1])
        eq_level = leg_low + (leg_high - leg_low) * 0.5  

        # LONG (Всегда включен для теста)
        if current_price > ema_200:
            is_valid_choch = (c[-1] > np.max(h[-6:-1])) and has_volume
            if is_valid_choch and current_price <= eq_level:
                if not find_fvg(h, l, 'Long'): return None
                for i in range(len(c)-2, len(c)-6, -1):
                    if c[i] < o[i]:  
                        ob_low, ob_high = l[i], h[i]
                        if current_price > ob_high and (current_price - ob_high) < (atr * 0.5):
                            sl = ob_low - (atr * 0.3)  
                            return {'symbol': sym, 'direction': 'Long', 'entry_price': current_price, 'sl': round(sl, 6), 'tp1': round(current_price + (current_price-sl)*1.5, 6), 'tp2': round(current_price + (current_price-sl)*3, 6), 'ob_low': ob_low, 'ob_high': ob_high, 'pattern': '1H Micro-OB + FVG'}

        # SHORT (Всегда включен для теста)
        if current_price < ema_200:
            is_valid_choch_short = (c[-1] < np.min(l[-6:-1])) and has_volume
            if is_valid_choch_short and current_price >= eq_level:
                if not find_fvg(h, l, 'Short'): return None
                for i in range(len(c)-2, len(c)-6, -1):
                    if c[i] > o[i]:  
                        ob_high, ob_low = h[i], l[i]
                        if current_price < ob_low and (ob_low - current_price) < (atr * 0.5):
                            sl = ob_high + (atr * 0.3)
                            return {'symbol': sym, 'direction': 'Short', 'entry_price': current_price, 'sl': round(sl, 6), 'tp1': round(current_price - (sl-current_price)*1.5, 6), 'tp2': round(current_price - (sl-current_price)*3, 6), 'ob_low': ob_low, 'ob_high': ob_high, 'pattern': '1H Micro-OB + FVG'}
    except Exception as e: logging.error(f"SMC Error {sym}: {e}")
    return None

async def radar_task():
    global HOT_LIST
    await asyncio.sleep(5)
    while True:
        try:
            if GLOBAL_STOP_UNTIL and datetime.now(timezone.utc) < GLOBAL_STOP_UNTIL:
                await asyncio.sleep(60); continue
            if len(active_positions) >= MAX_POSITIONS:
                await asyncio.sleep(60); continue

            markets = await exchange.load_markets()
            tickers = await exchange.fetch_tickers()
            
            valid_symbols = []
            for sym, m in markets.items():
                if m.get('type') != 'swap' or not m.get('active'): continue
                if any(kw in sym.upper() for kw in EXCLUDED_KEYWORDS): continue
                tick = tickers.get(sym)
                if not tick or float(tick.get('quoteVolume', 0)) < MIN_VOLUME_USDT: continue
                base_coin = sym.split('/')[0].split('-')[0].split(':')[0]
                if not any(pos['symbol'].split('/')[0].split('-')[0].split(':')[0] == base_coin for pos in active_positions):
                    valid_symbols.append(sym)

            new_hot_list = {}
            for sym in valid_symbols:
                clean_name = sym.split(':')[0]
                if clean_name in last_signals and (datetime.now(timezone.utc) - last_signals[clean_name] < timedelta(hours=4)): continue
                
                signal = await detect_smc_setup(sym, 'Long', True) # Фейковые данные для теста
                if signal:
                    new_hot_list[sym] = signal
                    if sym not in HOT_LIST:
                        await send_tg_msg(f"🎯 **РАДАР:** {clean_name} взят на мушку!\nЗапускаю 4x Мульти-Сканер...")

            HOT_LIST = new_hot_list
            logging.info(f"🔎 [РАДАР] Скан завершен. На мушке: {len(HOT_LIST)}")
            await asyncio.sleep(300) 
        except Exception as e:
            logging.error(f"Radar error: {e}"); await asyncio.sleep(60)

# --- ЭКЗЕКЬЮТОР ---
async def execute_trade(signal):
    sym = signal['symbol']
    clean_name = sym.split(':')[0]
    try:
        bal = await exchange.fetch_balance()
        free_usdt = float(bal['USDT']['free'])
        
        risk_multiplier = signal.get('dynamic_risk', 1.0)
        actual_risk_percent = RISK_PER_TRADE * risk_multiplier
        qty = float(exchange.amount_to_precision(sym, (free_usdt * actual_risk_percent) / abs(signal['entry_price'] - signal['sl'])))
        if qty <= 0: return 

        pos_side = 'LONG' if signal['direction'] == 'Long' else 'SHORT'
        side = 'buy' if signal['direction'] == 'Long' else 'sell'
        sl_side = 'sell' if side == 'buy' else 'buy'
        
        await exchange.set_leverage(LEVERAGE, sym, params={'side': pos_side})
        await exchange.create_market_order(sym, side, qty, params={'positionSide': pos_side})
        sl_ord = await exchange.create_order(sym, 'stop_market', sl_side, qty, params={'triggerPrice': signal['sl'], 'positionSide': pos_side, 'stopLossPrice': signal['sl']})
        
        active_positions.append({
            'symbol': sym, 'direction': signal['direction'], 'entry_price': signal['entry_price'], 'initial_qty': qty, 
            'sl_price': signal['sl'], 'tp1': signal['tp1'], 'tp2': signal['tp2'], 'tp1_hit': False, 'tp2_hit': False, 'be_level': 0,
            'sl_order_id': sl_ord['id'], 'position_side': pos_side, 'open_time': datetime.now(timezone.utc).isoformat()
        })
        
        last_signals[clean_name] = datetime.now(timezone.utc)
        await asyncio.to_thread(save_positions)
        
        msg = (f"💥 **ВЫСТРЕЛ (Multi-WSS): {clean_name}**\nНаправление: **#{signal['direction'].upper()}**\n"
               f"Рейтинг: {signal.get('score_info', 'Базовый')} (Риск: {actual_risk_percent*100:.1f}%)\n"
               f"Цена: {signal['entry_price']}\nОбъем: {qty}\nSL: {signal['sl']:.4f}\nTP1: {signal['tp1']:.4f}")
        await send_tg_msg(msg)
    except Exception as e: logging.error(f"Trade error {clean_name}: {e}")

# --- MULTI-EXCHANGE SNIPER CORE ---
async def wss_sniper_worker(sym, setup_data):
    base_coin = sym.split('/')[0].split('-')[0].split(':')[0]
    
    sym_bingx = sym.replace('/', '-')
    sym_bybit = f"{base_coin}USDT"
    sym_bitget = f"{base_coin}USDT"
    sym_mexc = f"{base_coin}_USDT"

    state = {'buy_vol': 0.0, 'sell_vol': 0.0, 'current_price': setup_data['entry_price'], 'active': True}

    async def l_bingx():
        url = "wss://open-api-swap.bingx.com/swap-market"
        while state['active']:
            try:
                async for ws in websockets.connect(url):
                    await ws.send(json.dumps({"id": "1", "reqType": "sub", "dataType": f"{sym_bingx}@trade"}))
                    while state['active']:
                        msg = await ws.recv()
                        data = json.loads(gzip.GzipFile(fileobj=io.BytesIO(msg)).read().decode('utf-8'))
                        if data.get("ping"): await ws.send(json.dumps({"pong": data.get("ping")})); continue
                        if "data" in data:
                            for t in data["data"]:
                                v = float(t['p']) * float(t['q'])
                                if t.get('m'): state['sell_vol'] += v
                                else: state['buy_vol'] += v
                            state['current_price'] = float(data["data"][-1]['p'])
            except: await asyncio.sleep(1)

    async def l_bybit():
        url = "wss://stream.bybit.com/v5/public/linear"
        while state['active']:
            try:
                async for ws in websockets.connect(url):
                    await ws.send(json.dumps({"op": "subscribe", "args": [f"publicTrade.{sym_bybit}"]}))
                    while state['active']:
                        msg = await ws.recv()
                        data = json.loads(msg)
                        if "data" in data and isinstance(data["data"], list):
                            for t in data["data"]:
                                v = float(t['p']) * float(t['v'])
                                if t['S'] == "Sell": state['sell_vol'] += v
                                else: state['buy_vol'] += v
            except: await asyncio.sleep(1)

    async def l_bitget():
        url = "wss://ws.bitget.com/v2/ws/public"
        while state['active']:
            try:
                async for ws in websockets.connect(url):
                    await ws.send(json.dumps({"op": "subscribe", "args": [{"instType": "USDT-FUTURES", "channel": "trade", "instId": sym_bitget}]}))
                    async def ping_bg():
                        while state['active']:
                            await asyncio.sleep(25)
                            try: await ws.send("ping")
                            except: break
                    ptask = asyncio.create_task(ping_bg())
                    try:
                        while state['active']:
                            msg = await ws.recv()
                            if msg == "pong": continue
                            data = json.loads(msg)
                            if "data" in data and isinstance(data["data"], list):
                                for t in data["data"]:
                                    v = float(t['price']) * float(t['size'])
                                    if t['side'] == "sell": state['sell_vol'] += v
                                    else: state['buy_vol'] += v
                    finally: ptask.cancel()
            except: await asyncio.sleep(1)

    async def l_mexc():
        url = "wss://wbs.mexc.com/ws"
        while state['active']:
            try:
                async for ws in websockets.connect(url):
                    await ws.send(json.dumps({"method": "SUBSCRIPTION", "params": [f"spot@public.deals.v3.api@{sym_mexc}"]}))
                    async def ping_mexc():
                        while state['active']:
                            await asyncio.sleep(15)
                            try: await ws.send(json.dumps({"method": "PING"}))
                            except: break
                    ptask = asyncio.create_task(ping_mexc())
                    try:
                        while state['active']:
                            msg = await ws.recv()
                            data = json.loads(msg)
                            if "d" in data and "deals" in data["d"]:
                                for t in data["d"]["deals"]:
                                    v = float(t['p']) * float(t['v'])
                                    if t['S'] == 2: state['sell_vol'] += v
                                    else: state['buy_vol'] += v
                    finally: ptask.cancel()
            except: await asyncio.sleep(1)

    tasks = [asyncio.create_task(l_bingx()), asyncio.create_task(l_bybit()), asyncio.create_task(l_bitget()), asyncio.create_task(l_mexc())]

    try:
        while sym in HOT_LIST and len(active_positions) < MAX_POSITIONS:
            await asyncio.sleep(5)
            total = state['buy_vol'] + state['sell_vol']
            
            # Порог увеличен до 10k$ за 5 сек, так как слушаем 4 биржи
            if total > 10000:
                buy_pct = (state['buy_vol'] / total) * 100
                sell_pct = 100 - buy_pct
                
                in_zone = setup_data['ob_low'] <= state['current_price'] <= setup_data['ob_high']
                
                if in_zone:
                    is_long = (setup_data['direction'] == 'Long' and buy_pct >= 75)
                    is_short = (setup_data['direction'] == 'Short' and sell_pct >= 75)
                    
                    if is_long or is_short:
                        dom_pct = buy_pct if setup_data['direction'] == 'Long' else sell_pct
                        
                        if dom_pct >= 90: setup_data['dynamic_risk'], setup_data['score_info'] = 2.5, f"10/10 🔥 ({dom_pct:.1f}%)"
                        elif dom_pct >= 85: setup_data['dynamic_risk'], setup_data['score_info'] = 1.5, f"8/10 ⚡ ({dom_pct:.1f}%)"
                        else: setup_data['dynamic_risk'], setup_data['score_info'] = 1.0, f"6/10 📊 ({dom_pct:.1f}%)"

                        await execute_trade(setup_data)
                        del HOT_LIST[sym]
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
def index(): return "Multi-WSS Async Bot v7.0 Active"

async def main():
    init_db(); load_positions()
    logging.info("🚀 Запуск ядра v7.0: 4x Мульти-Биржевой Снайпер...")
    await asyncio.gather(radar_task(), sniper_manager(), monitor_positions_job()
    )

if __name__ == '__main__':
    Thread(target=lambda: app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 10000))), daemon=True).start()
    # Отключаем прослушивание команд, чтобы не конфликтовать со старым ботом
    # Thread(target=bot.infinity_polling, kwargs={'skip_pending': True}, daemon=True).start() 
    try: asyncio.run(main())
    except KeyboardInterrupt: logging.info("\nОстановка.")
