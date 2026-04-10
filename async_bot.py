import asyncio
import json
import gzip
import os
import logging
import sqlite3
import gc
import time
import ccxt.async_support as ccxt_async
import numpy as np
import aiohttp
from datetime import datetime, timezone, timedelta
from threading import Thread
from http.server import BaseHTTPRequestHandler, HTTPServer

# === НАСТРОЙКИ v8.12 (True Sandbox, Fee Guard, Smart Filters) ===
DB_PATH = '/data/bot.db' 
TOKEN = os.getenv('TELEGRAM_TOKEN')
GROUP_CHAT_ID = int(os.getenv('GROUP_CHAT_ID', -1003407154454))
BINGX_API_KEY = os.getenv('BINGX_API_KEY')
BINGX_SECRET = os.getenv('BINGX_SECRET')

RISK_PER_TRADE = 0.02      
MAX_POSITIONS = 3           
LEVERAGE = 10               
MAX_SPREAD_PERCENT = 0.002  
MIN_VOLUME_USDT = 1000000  
MIN_NOTIONAL_USDT = 10.0    
WHALE_VOLUME_THRESHOLD = 200000 

SMC_TIMEFRAME = '15m'

CRITICAL_CVD_USDT = 150000  
GLOBAL_CVD = {}             
COOLDOWN_CACHE = {}         

MAX_CONCURRENT_TASKS = 10  
SCAN_LIMIT = 300           

EXCLUDED_KEYWORDS = ['NCS', 'NCFX', 'NCCO', 'NCSI', 'NIKKEI', 'NASDAQ', 'SP500', 'GOLD', 'SILVER', 'AUT', 'XAU', 'PAXG', 'EUR', '1000', 'LUNC', 'USTC', 'BTC/', 'ETH/', 'BNB/', 'SOL/', 'XRP/', 'ADA/', 'TRX/']

GLOBAL_STOP_UNTIL = None
CONSECUTIVE_LOSSES = 0
last_signals = {} 
NOTIFIED_SYMBOLS = set() 

HOT_LIST = {}  
ACTIVE_WSS_CONNECTIONS = set()
active_positions = []
daily_stats = {'pnl': 0.0, 'trades': 0, 'wins': 0}

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(message)s')

def get_mem_usage():
    try:
        with open('/proc/self/status') as f:
            for line in f:
                if 'VmRSS' in line: return f"{int(line.split()[1]) / 1024:.1f} MB"
    except: pass
    return "N/A"

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
        if row: 
            active_positions = json.loads(row[0])
            logging.info(f"✅ База данных загружена. Активных сделок: {len(active_positions)}")
        else:
            logging.info("ℹ️ База данных пуста. Активных сделок нет.")
            
        c.execute("SELECT pnl, trades, wins FROM daily_stats WHERE id = 1"); stat_row = c.fetchone()
        if stat_row: daily_stats['pnl'], daily_stats['trades'], daily_stats['wins'] = stat_row
        conn.close()
    except Exception: pass

async def send_tg_msg(text):
    if not TOKEN: return
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    payload = {"chat_id": GROUP_CHAT_ID, "text": text, "parse_mode": "Markdown"}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload) as resp: pass
    except Exception: pass

def calculate_ema(data, window):
    if len(data) < window: return data[-1]
    alpha = 2 / (window + 1)
    ema = data[0]
    for price in data[1:]: ema = (price * alpha) + (ema * (1 - alpha))
    return ema

def calculate_rsi(data, window=14):
    if len(data) <= window: return 50
    diffs = np.diff(data)
    gains = np.maximum(diffs, 0)
    losses = np.abs(np.minimum(diffs, 0))
    avg_gain = np.mean(gains[-window:])
    avg_loss = np.mean(losses[-window:])
    if avg_loss == 0: return 100
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

def find_fvg_details(h, l, direction):
    for i in range(len(h)-1, len(h)-12, -1):
        if direction == 'Long' and l[i] > h[i-2]: return {'found': True, 'top': l[i], 'bottom': h[i-2]}
        elif direction == 'Short' and h[i] < l[i-2]: return {'found': True, 'top': l[i-2], 'bottom': h[i]}
    return {'found': False}

async def detect_setups(sym, btc_trend, altseason, btc_volatility_pct):
    try:
        ohlcv = await exchange.fetch_ohlcv(sym, timeframe=SMC_TIMEFRAME, limit=205)
        if not ohlcv or len(ohlcv) < 200: return None
        
        o = np.array([x[1] for x in ohlcv], dtype=float)
        h = np.array([x[2] for x in ohlcv], dtype=float)
        l = np.array([x[3] for x in ohlcv], dtype=float)
        c = np.array([x[4] for x in ohlcv], dtype=float)
        v = np.array([x[5] for x in ohlcv], dtype=float)
        
        current_price = c[-1]
        ema_200 = calculate_ema(c, 200)
        rsi_15m = calculate_rsi(c, 14)
        atr = np.mean(np.maximum(h[1:]-l[1:], np.maximum(np.abs(h[1:]-c[:-1]), np.abs(l[1:]-c[:-1])))[-24:])
        
        avg_volume = np.mean(v[-22:-2])
        has_volume = (v[-1] > avg_volume * 1.05) or (v[-2] > avg_volume * 1.05)
        vol_increase_pct = ((max(v[-1], v[-2]) / avg_volume) - 1) * 100 if avg_volume > 0 else 0

        leg_high, leg_low = np.max(h[-50:-1]), np.min(l[-50:-1])
        
        is_high_momentum = btc_volatility_pct > 2.0
        discount_ratio = 0.85 if is_high_momentum else 0.6  
        premium_ratio = 0.15 if is_high_momentum else 0.4
        
        eq_level_long = leg_low + (leg_high - leg_low) * discount_ratio  
        eq_level_short = leg_low + (leg_high - leg_low) * premium_ratio 

        # --- 1. АДАПТИВНЫЙ SMC МОДУЛЬ ---
        if ((btc_trend == 'Long') or altseason) and current_price > ema_200:
            if (c[-1] > np.max(h[-11:-1])) and has_volume and current_price <= eq_level_long:
                fvg = find_fvg_details(h, l, 'Long')
                if fvg['found']:
                    if is_high_momentum and current_price <= fvg['top'] and current_price >= fvg['bottom']:
                        sl = fvg['bottom'] - (atr * 0.8)
                        return {'symbol': sym, 'direction': 'Long', 'entry_price': current_price, 'sl': sl, 'atr': atr, 'ob_low': fvg['bottom'], 'ob_high': fvg['top'], 'pattern': '15m Momentum FVG', 'vol_pct': vol_increase_pct, 'mode': 'SMC'}
                    
                    for i in range(len(c)-2, len(c)-11, -1):
                        if c[i] < o[i]:  
                            ob_low, ob_high = l[i], h[i]
                            if i >= 15:
                                local_min_past = np.min(l[i-15:i])
                                is_valid_ob = (ob_low <= local_min_past) if not is_high_momentum else True 
                                
                                if is_valid_ob:
                                    if current_price > ob_high and (current_price - ob_high) < (atr * 1.0):
                                        sl = ob_low - (atr * 0.8)  
                                        patt = '15m OB+FVG (Sweep)' if not is_high_momentum else '15m OB Breaker'
                                        return {'symbol': sym, 'direction': 'Long', 'entry_price': current_price, 'sl': sl, 'atr': atr, 'ob_low': ob_low, 'ob_high': ob_high, 'pattern': patt, 'vol_pct': vol_increase_pct, 'mode': 'SMC'}

        if (btc_trend == 'Short') and current_price < ema_200:
            if (c[-1] < np.min(l[-11:-1])) and has_volume and current_price >= eq_level_short:
                fvg = find_fvg_details(h, l, 'Short')
                if fvg['found']:
                    if is_high_momentum and current_price >= fvg['bottom'] and current_price <= fvg['top']:
                        sl = fvg['top'] + (atr * 0.8)
                        return {'symbol': sym, 'direction': 'Short', 'entry_price': current_price, 'sl': sl, 'atr': atr, 'ob_low': fvg['bottom'], 'ob_high': fvg['top'], 'pattern': '15m Momentum FVG', 'vol_pct': vol_increase_pct, 'mode': 'SMC'}
                        
                    for i in range(len(c)-2, len(c)-11, -1):
                        if c[i] > o[i]:  
                            ob_high, ob_low = h[i], l[i]
                            if i >= 15:
                                local_max_past = np.max(h[i-15:i])
                                is_valid_ob = (ob_high >= local_max_past) if not is_high_momentum else True
                                
                                if is_valid_ob:
                                    if current_price < ob_low and (ob_low - current_price) < (atr * 1.0):
                                        sl = ob_high + (atr * 0.8)
                                        patt = '15m OB+FVG (Sweep)' if not is_high_momentum else '15m OB Breaker'
                                        return {'symbol': sym, 'direction': 'Short', 'entry_price': current_price, 'sl': sl, 'atr': atr, 'ob_low': ob_low, 'ob_high': ob_high, 'pattern': patt, 'vol_pct': vol_increase_pct, 'mode': 'SMC'}

        # --- 2. GRID МОДУЛЬ ---
        bb_window = 20
        sma_20 = calculate_ema(c, bb_window) 
        std_dev = np.std(c[-bb_window:])
        upper_bb = sma_20 + (2 * std_dev)
        lower_bb = sma_20 - (2 * std_dev)
        bb_width = (upper_bb - lower_bb) / sma_20 * 100
        
        grid_volume_threshold = avg_volume * 1.50
        grid_has_volume = (v[-1] > grid_volume_threshold) or (v[-2] > grid_volume_threshold)

        now_utc = datetime.now(timezone.utc)
        hour, minute = now_utc.hour, now_utc.minute
        
        is_us_open = (hour == 13 and minute >= 30) or (hour == 14 and minute <= 30)
        is_funding = (hour in [23, 7, 15] and minute >= 45) or (hour in [0, 8, 16] and minute <= 15)
        skip_grid = is_us_open or is_funding

        grid_width_limit = max(1.5, min(3.0, btc_volatility_pct * 1.5))
        
        candle_range = h[-1] - l[-1] if h[-1] - l[-1] > 0 else 0.0001
        lower_wick_ratio = (np.minimum(o[-1], c[-1]) - l[-1]) / candle_range
        upper_wick_ratio = (h[-1] - np.maximum(o[-1], c[-1])) / candle_range

        if not skip_grid and bb_width < grid_width_limit and grid_has_volume:
            allow_long = btc_volatility_pct < 2.0 or current_price > ema_200
            allow_short = btc_volatility_pct < 2.0 or current_price < ema_200
            
            if allow_long and current_price <= lower_bb * 1.002 and rsi_15m < 45 and lower_wick_ratio >= 0.3: 
                sl = lower_bb - (atr * 0.5)
                return {'symbol': sym, 'direction': 'Long', 'entry_price': current_price, 'sl': sl, 'atr': atr, 'ob_low': lower_bb, 'ob_high': upper_bb, 'pattern': f'BB Scalp (Pinbar, Limit {grid_width_limit:.1f}%)', 'vol_pct': vol_increase_pct, 'mode': 'GRID'}
            
            elif allow_short and current_price >= upper_bb * 0.998 and rsi_15m > 55 and upper_wick_ratio >= 0.3: 
                sl = upper_bb + (atr * 0.5)
                return {'symbol': sym, 'direction': 'Short', 'entry_price': current_price, 'sl': sl, 'atr': atr, 'ob_low': lower_bb, 'ob_high': upper_bb, 'pattern': f'BB Scalp (Pinbar, Limit {grid_width_limit:.1f}%)', 'vol_pct': vol_increase_pct, 'mode': 'GRID'}

        return None
    except Exception: return None

async def process_single_coin(sym, btc_trend, altseason, btc_volatility_pct, sem):
    async with sem: 
        try:
            signal = await detect_setups(sym, btc_trend, altseason, btc_volatility_pct)
            await asyncio.sleep(0.05) 
            return sym, signal
        except Exception: return sym, None

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

            btc_trend, altseason = 'Short', False
            btc_volatility_pct = 2.0
            try:
                btc_ohlcv = await exchange.fetch_ohlcv('BTC/USDT', timeframe=SMC_TIMEFRAME, limit=205)
                btc_c = np.array([x[4] for x in btc_ohlcv], dtype=float)
                btc_trend = 'Long' if btc_c[-1] > calculate_ema(btc_c, 200) else 'Short'
                eth_ohlcv = await exchange.fetch_ohlcv('ETH/USDT', timeframe=SMC_TIMEFRAME, limit=205)
                altseason = True if (np.array([x[4] for x in eth_ohlcv], dtype=float)[-1] / btc_c[-1]) > calculate_ema((np.array([x[4] for x in eth_ohlcv], dtype=float) / btc_c), 200) else False
                
                btc_bb_window = 20
                btc_sma = np.mean(btc_c[-btc_bb_window:])
                btc_std = np.std(btc_c[-btc_bb_window:])
                btc_volatility_pct = (4 * btc_std) / btc_sma * 100
            except: pass

            tickers = await exchange.fetch_tickers()
            temp_symbols = []
            
            for sym, m in exchange.markets.items():
                if m.get('type') != 'swap' or not m.get('active'): continue
                if any(kw in sym.upper() for kw in EXCLUDED_KEYWORDS): continue
                if sym in COOLDOWN_CACHE and time.time() < COOLDOWN_CACHE[sym]: continue
                
                tick = tickers.get(sym)
                if not tick or float(tick.get('quoteVolume') or 0) < MIN_VOLUME_USDT: continue
                ask, bid = float(tick.get('ask') or 0), float(tick.get('bid') or 0)
                if ask > 0 and bid > 0 and (ask - bid) / ask > MAX_SPREAD_PERCENT: continue
                if not any(pos['symbol'].split('/')[0].split('-')[0].split(':')[0] == sym.split('/')[0].split('-')[0].split(':')[0] for pos in active_positions):
                    temp_symbols.append((sym, float(tick.get('quoteVolume') or 0)))

            temp_symbols.sort(key=lambda x: x[1], reverse=True)
            valid_symbols = [x[0] for x in temp_symbols[:SCAN_LIMIT]]
            
            sem = asyncio.Semaphore(MAX_CONCURRENT_TASKS)
            results = []
            for i in range(0, len(valid_symbols), 10):
                chunk = valid_symbols[i:i+10]
                tasks = [process_single_coin(s, btc_trend, altseason, btc_volatility_pct, sem) for s in chunk if s.split(':')[0] not in last_signals or (datetime.now(timezone.utc) - last_signals[s.split(':')[0]] > timedelta(hours=4))]
                if tasks: results.extend(await asyncio.gather(*tasks))
                await asyncio.sleep(0.1)

            new_hot_list = {}
            for sym, signal in results:
                if signal:
                    new_hot_list[sym] = signal
                    if sym not in HOT_LIST and sym not in NOTIFIED_SYMBOLS:
                        NOTIFIED_SYMBOLS.add(sym)
                        await send_tg_msg(f"🎯 **РАДАР [{signal['mode']}]:** {sym.split(':')[0]} взят на мушку!\nЗапускаю OMNI-CORE Снайпер (v8.12)...")

            HOT_LIST = new_hot_list
            NOTIFIED_SYMBOLS = {s for s in NOTIFIED_SYMBOLS if s in HOT_LIST}
            logging.info(f"🔎 [РАДАР] Скан завершен. На мушке: {len(HOT_LIST)} | ОЗУ: {get_mem_usage()} | BTC Vol: {btc_volatility_pct:.2f}%")
            gc.collect() 
            await asyncio.sleep(300) 
        except Exception: await asyncio.sleep(60)

async def execute_trade(signal):
    global COOLDOWN_CACHE
    sym = signal['symbol']
    clean_name = sym.split(':')[0]
    
    if len(active_positions) >= MAX_POSITIONS: return
    if any(p['symbol'] == sym for p in active_positions): return
    if sym in COOLDOWN_CACHE and time.time() < COOLDOWN_CACHE[sym]: return

    try:
        market_info = exchange.market(sym)
        contract_size = float(market_info.get('contractSize', 1.0))
        
        bal = await exchange.fetch_balance()
        free_usdt = float(bal['USDT']['free'])
        ticker = await exchange.fetch_ticker(sym)
        current_market_price = ticker['last']
        
        atr = signal.get('atr', current_market_price * 0.015)
        logical_sl_dist = abs(signal['entry_price'] - signal['sl'])
        
        # --- FEE GUARD: Защита от микроскопических стоп-лоссов ---
        # Устанавливаем минимальную дистанцию в 0.4%, чтобы спред не съел сделку
        min_allowed_sl_dist = current_market_price * 0.004
        actual_sl_dist = max(logical_sl_dist, min_allowed_sl_dist)
        
        is_long = signal['direction'] == 'Long'
        
        if is_long:
            sl_raw = current_market_price - actual_sl_dist
            tp1_raw = current_market_price + (actual_sl_dist * 1.5)
            tp2_raw = current_market_price + (actual_sl_dist * 3.0)
        else:
            sl_raw = current_market_price + actual_sl_dist
            tp1_raw = current_market_price - (actual_sl_dist * 1.5)
            tp2_raw = current_market_price - (actual_sl_dist * 3.0)
            
        sl_price = float(exchange.price_to_precision(sym, sl_raw))
        tp1_price = float(exchange.price_to_precision(sym, tp1_raw))
        tp2_price = float(exchange.price_to_precision(sym, tp2_raw))

        if is_long and sl_price >= current_market_price: return
        if not is_long and sl_price <= current_market_price: return

        risk_multiplier = signal.get('dynamic_risk', 1.0)
        actual_risk_percent = RISK_PER_TRADE * risk_multiplier
        risk_amount = free_usdt * actual_risk_percent
        ideal_qty_coins = risk_amount / actual_sl_dist if actual_sl_dist > 0 else 0
        
        max_margin_per_trade = (free_usdt * 0.95) / MAX_POSITIONS
        max_notional_allowed = max_margin_per_trade * LEVERAGE
        max_qty_allowed_coins = max_notional_allowed / current_market_price
        
        final_qty_coins = min(ideal_qty_coins, max_qty_allowed_coins)
        
        final_contracts = final_qty_coins / contract_size if contract_size > 0 else final_qty_coins
        qty = float(exchange.amount_to_precision(sym, final_contracts))
        
        if qty <= 0: return 
        
        notional_value = qty * contract_size * current_market_price
        if notional_value < MIN_NOTIONAL_USDT:
            logging.warning(f"🚫 {clean_name} проигнорирован: Объем {notional_value:.2f}$ меньше минимального {MIN_NOTIONAL_USDT}$.")
            return

        pos_side = 'LONG' if is_long else 'SHORT'
        side = 'buy' if is_long else 'sell'
        sl_side = 'sell' if side == 'buy' else 'buy'
        
        bot_client_id = f"OMNI_{clean_name}_{int(time.time()*100)}"
        
        await exchange.set_leverage(LEVERAGE, sym, params={'side': pos_side})
        await exchange.create_market_order(sym, side, qty, params={'positionSide': pos_side, 'clientOrderId': bot_client_id})
        sl_ord = await exchange.create_order(sym, 'stop_market', sl_side, qty, params={'triggerPrice': sl_price, 'positionSide': pos_side, 'stopLossPrice': sl_price, 'clientOrderId': f"{bot_client_id}_SL"})
        
        timeout_profit = 0.75 if signal['mode'] == 'GRID' else 3.0
        timeout_any = 1.5 if signal['mode'] == 'GRID' else 6.0

        active_positions.append({
            'symbol': sym, 'direction': signal['direction'], 'entry_price': current_market_price, 'initial_qty': qty, 
            'sl_price': sl_price, 'tp1': tp1_price, 'tp2': tp2_price, 'atr': atr,
            'tp1_hit': False, 'pre_tp1_hit': False, 'sl_order_id': sl_ord['id'], 'position_side': pos_side, 
            'open_time': datetime.now(timezone.utc).isoformat(), 'mode': signal['mode'],
            'timeout_profit': timeout_profit, 'timeout_any': timeout_any, 'client_tag': bot_client_id
        })
        
        last_signals[clean_name] = datetime.now(timezone.utc)
        await asyncio.to_thread(save_positions)
        
        cap_note = " (Margin Capped ⚠️)" if ideal_qty_coins > max_qty_allowed_coins else ""
        msg = (f"💥 **ВЫСТРЕЛ [{signal['mode']}]: {clean_name}**\nНаправление: **#{signal['direction'].upper()}**\n"
               f"{signal.get('score_info', 'ТОП Сигнал')} (Риск: {actual_risk_percent*100:.1f}%)\n"
               f"Паттерн: {signal.get('pattern', 'N/A')}\n\n"
               f"📊 **Анализ 60s CVD:**\n{signal.get('vol_info', 'Нет данных')}\n\n"
               f"Цена: {current_market_price}\nОбъем: {qty} контр. (~{notional_value:.1f}$){cap_note}\nSL (Smart-Cap): {sl_price}\nTP1: {tp1_price}\nTP2: {tp2_price}")
        await send_tg_msg(msg)
        
    except Exception as e: 
        if "101204" in str(e) or "Insufficient margin" in str(e):
            COOLDOWN_CACHE[sym] = time.time() + 1800 
            logging.error(f"🚫 Нехватка маржи для {clean_name}. Кулдаун тикера на 30 мин.")
        else:
            logging.error(f"Ошибка при входе в {clean_name}: {e}")

async def wss_sniper_worker(sym, setup_data):
    global GLOBAL_CVD
    base_coin = sym.split('/')[0].split('-')[0].split(':')[0]
    clean_name = base_coin  
    
    sym_bingx_s, sym_bingx_f = f"{base_coin}-USDT", f"{base_coin}-USDT"
    sym_mexc, sym_bybit, sym_bybit_1000 = f"{base_coin}USDT", f"{base_coin}USDT", f"1000{base_coin}USDT"
    sym_binance, sym_binance_1000 = f"{base_coin}usdt".lower(), f"1000{base_coin}usdt".lower()

    state = {'trades': [], 'start_time': time.time(), 'max_bn_f_60s': 0.0, 'current_price': setup_data['entry_price'], 'active': True}

    async def l_ws(url, payload, ex_code, is_binance=False):
        while state['active']:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(url, heartbeat=20) as ws:
                        if payload: await ws.send_str(json.dumps(payload))
                        async for msg in ws:
                            if not state['active']: break
                            try:
                                if is_binance and msg.type == aiohttp.WSMsgType.TEXT:
                                    data = json.loads(msg.data)
                                    if 'p' in data and 'q' in data:
                                        state['trades'].append({'ts': time.time(), 'ex': ex_code, 'side': 's' if data.get('m') else 'b', 'v': float(data['p']) * float(data['q'])})
                                        state['current_price'] = float(data['p'])
                                elif msg.type == aiohttp.WSMsgType.BINARY:
                                    d = gzip.decompress(msg.data).decode('utf-8')
                                    if not d.strip(): continue
                                    data = json.loads(d)
                                    if "ping" in data: await ws.send_str(json.dumps({"pong": data["ping"]}))
                                    elif "data" in data and data["data"]:
                                        for t in (data["data"] if isinstance(data["data"], list) else [data["data"]]):
                                            if 'p' in t and (t.get('q') or t.get('v')):
                                                state['trades'].append({'ts': time.time(), 'ex': ex_code, 'side': 's' if t.get('m') else 'b', 'v': float(t['p']) * float(t.get('q', t.get('v', 0)))})
                                                if ex_code == 'bx_f': state['current_price'] = float(t['p'])
                                elif msg.type == aiohttp.WSMsgType.TEXT:
                                    data = json.loads(msg.data)
                                    if "data" in data and isinstance(data["data"], list):
                                        for t in data["data"]:
                                            if 'p' in t and 'v' in t:
                                                state['trades'].append({'ts': time.time(), 'ex': ex_code, 'side': 's' if t.get('S') == "Sell" else 'b', 'v': float(t['p']) * float(t['v'])})
                                    elif "d" in data and "deals" in data["d"]:
                                        for t in (data["d"]["deals"] if isinstance(data["d"]["deals"], list) else [data["d"]["deals"]]):
                                            if 'p' in t and 'v' in t:
                                                state['trades'].append({'ts': time.time(), 'ex': ex_code, 'side': 's' if str(t.get('S')) == '2' or str(t.get('T')) == '2' else 'b', 'v': float(t['p']) * float(t['v'])})
                            except Exception: pass
            except Exception: await asyncio.sleep(1)

    tasks = [
        asyncio.create_task(l_ws("wss://open-api-ws.bingx.com/market", {"id": "1", "reqType": "sub", "dataType": f"{sym_bingx_s}@trade"}, 'bx_s')),
        asyncio.create_task(l_ws("wss://open-api-swap.bingx.com/swap-market", {"id": "1", "reqType": "sub", "dataType": f"{sym_bingx_f}@trade"}, 'bx_f')),
        asyncio.create_task(l_ws("wss://wbs-api.mexc.com/ws", {"method": "SUBSCRIPTION", "params": [f"spot@public.deals.v3.api@{sym_mexc}"]}, 'mc_s')),
        asyncio.create_task(l_ws("wss://contract.mexc.com/edge", {"method": "sub.deal", "param": {"symbol": f"{base_coin}_USDT"}}, 'mc_f')),
        asyncio.create_task(l_ws("wss://stream.bybit.com/v5/public/spot", {"op": "subscribe", "args": [f"publicTrade.{sym_bybit}"]}, 'bb_s')),
        asyncio.create_task(l_ws("wss://stream.bybit.com/v5/public/linear", {"op": "subscribe", "args": [f"publicTrade.{sym_bybit}"]}, 'bb_f')),
        asyncio.create_task(l_ws("wss://stream.bybit.com/v5/public/linear", {"op": "subscribe", "args": [f"publicTrade.{sym_bybit_1000}"]}, 'bb_f')),
        asyncio.create_task(l_ws(f"wss://data-stream.binance.vision/ws/{sym_binance}@aggTrade", None, 'bn_s', True)),
        asyncio.create_task(l_ws(f"wss://fstream.binance.com/ws/{sym_binance}@aggTrade", None, 'bn_f', True)),
        asyncio.create_task(l_ws(f"wss://fstream.binance.com/ws/{sym_binance_1000}@aggTrade", None, 'bn_f', True))
    ]

    try:
        is_long = (setup_data['direction'] == 'Long')
        
        while (sym in HOT_LIST or any(p['symbol'] == sym for p in active_positions)) and state['active']:
            await asyncio.sleep(3) 
            now = time.time()
            state['trades'] = [t for t in state['trades'] if now - t['ts'] <= 60]
            
            trades_30s = [t for t in state['trades'] if now - t['ts'] <= 30]
            GLOBAL_CVD[sym] = sum(t['v'] for t in trades_30s if t['side'] == 'b') - sum(t['v'] for t in trades_30s if t['side'] == 's')
            
            if sym not in HOT_LIST or len(active_positions) >= MAX_POSITIONS or now - state['start_time'] < 15: continue
                
            vols = {ex: {'b': sum(t['v'] for t in state['trades'] if t['ex'] == ex and t['side'] == 'b'), 's': sum(t['v'] for t in state['trades'] if t['ex'] == ex and t['side'] == 's')} for ex in ['bx_s', 'bx_f', 'mc_s', 'mc_f', 'bb_s', 'bb_f', 'bn_s', 'bn_f']}
            tots = {ex: vols[ex]['b'] + vols[ex]['s'] for ex in vols}
            
            if tots['bn_f'] > state['max_bn_f_60s']: state['max_bn_f_60s'] = tots['bn_f']
            
            valid_exchanges = []
            # Снижены требования для Multi-Exchange фильтра (даем больше гибкости)
            thresh = {'bx_s': 8000, 'bx_f': 8000, 'mc_s': 8000, 'mc_f': 8000, 'bb_s': 8000, 'bn_s': 15000, 'bb_f': 20000, 'bn_f': 25000}
            labels = {'bx_s': 'BingX (S)', 'bx_f': 'BingX (F)', 'mc_s': 'MEXC (S)', 'mc_f': 'MEXC (F)', 'bb_s': 'Bybit (S)', 'bn_s': 'Binance (S)', 'bb_f': 'Bybit (F)', 'bn_f': 'Binance (F)'}
            
            for ex, limit in thresh.items():
                if tots[ex] > limit:
                    pct = (vols[ex]['b'] / tots[ex]) * 100 if is_long else (vols[ex]['s'] / tots[ex]) * 100
                    if pct >= 65: valid_exchanges.append((labels[ex], tots[ex], pct))

            if len(valid_exchanges) > 0:
                is_whale_override = False
                
                if len(valid_exchanges) < 2:
                    max_single_vol = max([tots[ex] for ex in tots]) if tots else 0
                    global_delta = sum(vols[ex]['b'] for ex in vols) - sum(vols[ex]['s'] for ex in vols)
                    
                    if max_single_vol >= WHALE_VOLUME_THRESHOLD:
                        if (is_long and global_delta > WHALE_VOLUME_THRESHOLD * 0.4) or (not is_long and global_delta < -WHALE_VOLUME_THRESHOLD * 0.4):
                            is_whale_override = True
                            logging.info(f"🐳 [КИТОВЫЙ ОБЪЕМ] {clean_name} игнорирует правило 2-х бирж! Объем: {max_single_vol:,.0f}$")
                            valid_exchanges.append(("WHALE_OVERRIDE", max_single_vol, 100))
                
                if len(valid_exchanges) < 2 and not is_whale_override:
                    if sym in HOT_LIST: del HOT_LIST[sym]
                    continue
                    
                ob_range = abs(setup_data['ob_high'] - setup_data['ob_low'])
                in_zone = (setup_data['ob_low'] - ob_range*0.5) <= state['current_price'] <= (setup_data['ob_high'] + ob_range*0.5)
                
                if in_zone:
                    price_shift_pct = ((state['current_price'] - setup_data['entry_price']) / setup_data['entry_price']) * 100
                    global_delta = sum(vols[ex]['b'] for ex in vols) - sum(vols[ex]['s'] for ex in vols)
                    
                    if (is_long and global_delta < 0) or (not is_long and global_delta > 0):
                        if sym in HOT_LIST: del HOT_LIST[sym]
                        continue
                    if (is_long and price_shift_pct < -0.05) or (not is_long and price_shift_pct > 0.05):
                        if sym in HOT_LIST: del HOT_LIST[sym]
                        continue
                    if (is_long and price_shift_pct > 2.5) or (not is_long and price_shift_pct < -2.5):
                        if sym in HOT_LIST: del HOT_LIST[sym]
                        continue
                    
                    if sym in HOT_LIST: del HOT_LIST[sym]
                    setup_data['score_info'] = f"🐳 КИТОВЫЙ СИГНАЛ" if is_whale_override else f"🔥 ТОП СИГНАЛ (Пулов: {len(valid_exchanges)})" 
                    setup_data['dynamic_risk'] = 1.0 
                    setup_data['vol_info'] = "\n".join([f"✅ {ex}: {tot:.0f}$ ({pct:.0f}%)" for ex, tot, pct in valid_exchanges if ex != "WHALE_OVERRIDE"])
                    await execute_trade(setup_data)
                    
    finally:
        state['active'] = False
        for t in tasks: t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        ACTIVE_WSS_CONNECTIONS.discard(sym)
        if sym in GLOBAL_CVD: del GLOBAL_CVD[sym]

async def sniper_manager():
    while True:
        for sym, setup_data in list(HOT_LIST.items()):
            if sym not in ACTIVE_WSS_CONNECTIONS:
                ACTIVE_WSS_CONNECTIONS.add(sym)
                asyncio.create_task(wss_sniper_worker(sym, setup_data))
        await asyncio.sleep(5)

async def monitor_positions_job():
    global active_positions, daily_stats, CONSECUTIVE_LOSSES
    while True:
        await asyncio.sleep(5) 
        try:
            positions_raw = await exchange.fetch_positions()
            
            # --- ИСТИННАЯ ПЕСОЧНИЦА (Удалено слепое перехватывание сделок) ---
            # Бот больше не будет воровать сделки RSI-бота или открытые вручную.

            if not active_positions: continue
            updated = []
            
            for pos in active_positions:
                sym = pos['symbol']
                clean_name = sym.split(':')[0]
                is_long = pos['direction'] == 'Long'
                curr = next((r for r in positions_raw if r['symbol'] == sym and float(r.get('contracts', 0)) > 0), None)
                
                if not curr:
                    ticker = await exchange.fetch_ticker(sym); pnl = (ticker['last'] - pos['entry_price']) * pos['initial_qty'] if is_long else (pos['entry_price'] - ticker['last']) * pos['initial_qty']
                    pnl_pct = (pnl / ((pos['entry_price'] * pos['initial_qty']) / LEVERAGE)) * 100 if (pos['entry_price'] * pos['initial_qty']) > 0 else 0
                    daily_stats['trades'] += 1; daily_stats['pnl'] += pnl
                    if pnl > 0: daily_stats['wins'] += 1; CONSECUTIVE_LOSSES = 0; await send_tg_msg(f"✅ **[{pos.get('mode', 'SMC')}] {clean_name} закрыта в плюс!**\nPNL: +{pnl:.2f} USDT (+{pnl_pct:.2f}%)")
                    else: CONSECUTIVE_LOSSES += 1; await send_tg_msg(f"🛑 **[{pos.get('mode', 'SMC')}] {clean_name} выбита по SL.**\nPNL: {pnl:.2f} USDT ({pnl_pct:.2f}%)")
                    continue

                ticker = await exchange.fetch_ticker(sym); last_p = ticker['last']
                current_cvd = GLOBAL_CVD.get(sym, 0)
                
                if (is_long and last_p < pos['entry_price'] and current_cvd < -CRITICAL_CVD_USDT) or (not is_long and last_p > pos['entry_price'] and current_cvd > CRITICAL_CVD_USDT):
                    try:
                        await exchange.create_market_order(sym, 'sell' if is_long else 'buy', float(curr['contracts']), params={'positionSide': pos['position_side'], 'clientOrderId': f"EXIT_{pos.get('client_tag', 'OMNI')}"})
                        if pos.get('sl_order_id'): await exchange.cancel_order(pos['sl_order_id'], sym)
                        pnl = (last_p - pos['entry_price']) * float(curr['contracts']) if is_long else (pos['entry_price'] - last_p) * float(curr['contracts'])
                        pnl_pct = (pnl / ((pos['entry_price'] * float(curr['contracts'])) / LEVERAGE)) * 100
                        daily_stats['trades'] += 1; daily_stats['pnl'] += pnl; CONSECUTIVE_LOSSES += 1
                        await send_tg_msg(f"🚨 **[CVD STOP] Экстренный выход из {clean_name}!**\nДельта: {current_cvd:,.0f} $\nPNL: {pnl:.2f} USDT ({pnl_pct:.2f}%)")
                        continue
                    except: pass

                if 'open_time' not in pos: pos['open_time'] = datetime.now(timezone.utc).isoformat()
                hours_passed = (datetime.now(timezone.utc) - datetime.fromisoformat(pos['open_time'])).total_seconds() / 3600
                pnl = (last_p - pos['entry_price']) * float(curr['contracts']) if is_long else (pos['entry_price'] - last_p) * float(curr['contracts'])

                if hours_passed >= pos.get('timeout_any', 4.0) or (hours_passed >= pos.get('timeout_profit', 2.0) and pnl > 0):
                    try:
                        await exchange.create_market_order(sym, 'sell' if is_long else 'buy', float(curr['contracts']), params={'positionSide': pos['position_side'], 'clientOrderId': f"TIME_{pos.get('client_tag', 'OMNI')}"})
                        if pos.get('sl_order_id'): await exchange.cancel_order(pos['sl_order_id'], sym)
                        pnl_pct = (pnl / ((pos['entry_price'] * float(curr['contracts'])) / LEVERAGE)) * 100
                        await send_tg_msg(f"{'✅' if pnl > 0 else '🛑'} **[{pos.get('mode', 'SMC')}] {clean_name} закрыта по ТАЙМАУТУ!**\nPNL: {pnl:.2f} USDT ({pnl_pct:.2f}%)")
                        continue 
                    except: pass

                ohlcv = await exchange.fetch_ohlcv(sym, timeframe='1m', limit=2)
                if not ohlcv: updated.append(pos); continue
                high_p, low_p = max([float(c[2]) for c in ohlcv]), min([float(c[3]) for c in ohlcv])

                if not pos.get('tp1_hit') and not pos.get('pre_tp1_hit'):
                    trigger = pos['entry_price'] + abs(pos['tp1'] - pos['entry_price']) * 0.8 * (1 if is_long else -1)
                    if (is_long and high_p >= trigger) or (not is_long and low_p <= trigger):
                        try:
                            if pos.get('sl_order_id'): await exchange.cancel_order(pos['sl_order_id'], sym)
                            be_p = float(exchange.price_to_precision(sym, pos['entry_price'] * (1.001 if is_long else 0.999)))
                            new_sl = await exchange.create_order(sym, 'stop_market', 'sell' if is_long else 'buy', pos['initial_qty'], params={'triggerPrice': be_p, 'positionSide': pos['position_side'], 'stopLossPrice': be_p, 'clientOrderId': f"SL_{pos.get('client_tag', 'OMNI')}"})
                            pos.update({'sl_order_id': new_sl['id'], 'current_sl': be_p, 'pre_tp1_hit': True})
                        except: pass

                if not pos.get('tp1_hit'):
                    if (is_long and high_p >= pos['tp1']) or (not is_long and low_p <= pos['tp1']):
                        try:
                            close_qty = float(exchange.amount_to_precision(sym, float(curr['contracts']) * 0.50))
                            if close_qty > 0: await exchange.create_market_order(sym, 'sell' if is_long else 'buy', close_qty, params={'positionSide': pos['position_side'], 'clientOrderId': f"TP1_{pos.get('client_tag', 'OMNI')}"}); pos['initial_qty'] = float(curr['contracts']) - close_qty 
                            if pos.get('sl_order_id'): await exchange.cancel_order(pos['sl_order_id'], sym)
                            be_p = float(exchange.price_to_precision(sym, pos['entry_price'] * (1.002 if is_long else 0.998)))
                            new_sl = await exchange.create_order(sym, 'stop_market', 'sell' if is_long else 'buy', pos['initial_qty'], params={'triggerPrice': be_p, 'positionSide': pos['position_side'], 'stopLossPrice': be_p, 'clientOrderId': f"SL_{pos.get('client_tag', 'OMNI')}"})
                            
                            atr_step = pos.get('atr', abs(pos['entry_price'] - pos['sl_price']) * 0.5)
                            pos.update({'tp1_hit': True, 'sl_order_id': new_sl['id'], 'micro_step': atr_step*0.3, 'trail_distance': atr_step*0.8, 'trail_trigger': pos['tp1'] + atr_step*0.3 * (1 if is_long else -1)})
                            await send_tg_msg(f"💰 **[{pos.get('mode', 'SMC')}] {clean_name} TP1 взят!** (50% закрыто).\n🛡 Запущен Агрессивный Трейлинг.")
                        except: pass

                if pos.get('tp1_hit'):
                    tt, ms, td = pos.get('trail_trigger'), pos.get('micro_step'), pos.get('trail_distance')
                    if (is_long and high_p >= tt) or (not is_long and low_p <= tt):
                        try:
                            if pos.get('sl_order_id'): await exchange.cancel_order(pos['sl_order_id'], sym)
                            nsl = float(exchange.price_to_precision(sym, tt - td if is_long else tt + td))
                            new_sl_order = await exchange.create_order(sym, 'stop_market', 'sell' if is_long else 'buy', pos['initial_qty'], params={'triggerPrice': nsl, 'positionSide': pos['position_side'], 'stopLossPrice': nsl, 'clientOrderId': f"TRAIL_{pos.get('client_tag', 'OMNI')}"})
                            pos.update({'sl_order_id': new_sl_order['id'], 'trail_trigger': tt + ms if is_long else tt - ms})
                            await send_tg_msg(f"📈 **[{pos.get('mode', 'SMC')}] {clean_name} Трейлинг:** SL -> {nsl}")
                        except: pass
                updated.append(pos)
            active_positions = updated
            await asyncio.to_thread(save_positions)
        except Exception: pass

class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self): self.send_response(200); self.end_headers(); self.wfile.write(b"Bot v8.12 Active (True Sandbox, Fee Guard, Smart Filters)")
    def log_message(self, format, *args): return 

def run_server():
    server = HTTPServer(('0.0.0.0', int(os.environ.get('PORT', 10000))), HealthCheckHandler)
    server.serve_forever()

async def main():
    init_db()
    load_positions()
    logging.info("🚀 Запуск ядра v8.12: True Sandbox + Fee Guard + Smart Filters...")
    await asyncio.gather(radar_task(), sniper_manager(), monitor_positions_job())

if __name__ == '__main__':
    Thread(target=run_server, daemon=True).start()
    try: 
        asyncio.run(main())
    except KeyboardInterrupt: 
        logging.info("\nОстановка.")
