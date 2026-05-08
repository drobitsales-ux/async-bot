import asyncio
import json
import os
import logging
import sqlite3
import gc
import time
import ccxt.async_support as ccxt_async
import numpy as np
import aiohttp
from datetime import datetime, timezone
from threading import Thread
from http.server import BaseHTTPRequestHandler, HTTPServer

DB_PATH = '/data/bot.db' if os.path.exists('/data') else 'bot.db'

TOKEN = os.getenv('TELEGRAM_TOKEN')
GROUP_CHAT_ID = int(os.getenv('GROUP_CHAT_ID', -1003407154454))
BINGX_API_KEY = os.getenv('BINGX_API_KEY')
BINGX_SECRET = os.getenv('BINGX_SECRET')

# --- НАСТРОЙКИ ДЛЯ ПРОП-КОМПАНИЙ (Tier-1) ---
BASE_RISK_PER_TRADE = 0.02  # Для тестов. Перед реальным пропом изменить на 0.005
MAX_POSITIONS = 3           
MAX_POSITIONS_PER_DIRECTION = 2  
LEVERAGE = 5                
MAX_MARGIN_PCT = 0.15       
MIN_VOLUME_USDT = 1000000   
MIN_SL_PCT = 1.5            
MAX_SL_PCT = 4.5            
FEE_RATE = 0.0005           

SMC_TIMEFRAME = '15m'
COOLDOWN_CACHE = {}         
SCAN_LIMIT = 300           
NEWS_EVENTS = []

EXCLUDED_KEYWORDS = [
    'BTC/USDT', 'ETH/USDT', 'SOL/USDT', 'BNB/USDT', 'XRP/USDT', 
    'NCS', 'NCFX', 'NCCO', 'NCSI', 'NIKKEI', 'NASDAQ', 'SP500', 
    'GOLD', 'SILVER', 'AUT', 'XAU', 'PAXG', 'EUR', '1000', 'LUNC', 
    'USTC', 'USDC', 'FART', 'PEPE', 'SHIB', 'DOGE', 'WIF', 'BONK', 
    'FLOKI', 'BOME', 'MEME', 'TURBO', 'SATS', 'RATS', 'ORDI'
]

daily_stats = {'pnl': 0.0, 'trades': 0, 'wins': 0, 'prev_winrate': 0.0, 'start_balance': 0.0, 'gross_profit': 0.0, 'gross_loss': 0.0}
active_positions = []
NOTIFIED_SYMBOLS = set() 
REPORTED_TODAY = False

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(message)s')

exchange = ccxt_async.bingx({
    'apiKey': BINGX_API_KEY, 
    'secret': BINGX_SECRET,
    'options': {'defaultType': 'swap', 'marginMode': 'isolated'}, 
    'enableRateLimit': True
})

async def fetch_news_task():
    global NEWS_EVENTS
    while True:
        try:
            url = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=15) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        events = []
                        for item in data:
                            if item.get('country') == 'USD' and item.get('impact') == 'High':
                                try:
                                    dt = datetime.fromisoformat(item['date'])
                                    events.append(dt.timestamp())
                                except: pass
                        NEWS_EVENTS = events
                        logging.info(f"📰 [NEWS] Загружено {len(NEWS_EVENTS)} макро-событий по USD на неделю.")
        except Exception as e:
            pass
        await asyncio.sleep(43200)

def get_db_conn(): return sqlite3.connect(DB_PATH, check_same_thread=False)

def init_db():
    conn = get_db_conn(); c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS active_positions (id INTEGER PRIMARY KEY, data TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS daily_stats (id INTEGER PRIMARY KEY, pnl REAL, trades INTEGER, wins INTEGER)''')
    for col in ['wins', 'prev_winrate', 'start_balance', 'gross_profit', 'gross_loss']:
        try: c.execute(f"ALTER TABLE daily_stats ADD COLUMN {col} REAL DEFAULT 0.0")
        except: pass
    conn.commit(); conn.close()

def save_positions():
    conn = get_db_conn(); c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO active_positions (id, data) VALUES (1, ?)", (json.dumps(active_positions),))
    c.execute("INSERT OR REPLACE INTO daily_stats (id, pnl, trades, wins, prev_winrate, start_balance, gross_profit, gross_loss) VALUES (1, ?, ?, ?, ?, ?, ?, ?)", 
              (daily_stats.get('pnl', 0.0), daily_stats.get('trades', 0), daily_stats.get('wins', 0), 
               daily_stats.get('prev_winrate', 0.0), daily_stats.get('start_balance', 0.0),
               daily_stats.get('gross_profit', 0.0), daily_stats.get('gross_loss', 0.0)))
    conn.commit(); conn.close()

def load_positions():
    global active_positions, daily_stats
    try:
        conn = get_db_conn(); c = conn.cursor()
        c.execute("SELECT data FROM active_positions WHERE id = 1"); row = c.fetchone()
        if row: active_positions = json.loads(row[0])
        c.execute("SELECT pnl, trades, wins, prev_winrate, start_balance, gross_profit, gross_loss FROM daily_stats WHERE id = 1")
        stat_row = c.fetchone()
        if stat_row: 
            daily_stats['pnl'], daily_stats['trades'], daily_stats['wins'], daily_stats['prev_winrate'], daily_stats['start_balance'], daily_stats['gross_profit'], daily_stats['gross_loss'] = stat_row
        conn.close()
    except Exception: pass

async def send_tg_msg(text):
    if not TOKEN: return
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    payload = {"chat_id": GROUP_CHAT_ID, "text": text, "parse_mode": "HTML"} 
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload) as resp: pass
    except: pass

def calculate_ema(data, window):
    if len(data) < window: return data[-1]
    alpha = 2 / (window + 1); ema = data[0]
    for price in data[1:]: ema = (price * alpha) + (ema * (1 - alpha))
    return ema

def calculate_ema_array(data, window):
    alpha = 2 / (window + 1)
    emas = np.zeros_like(data)
    emas[0] = data[0]
    for i in range(1, len(data)): emas[i] = (data[i] * alpha) + (emas[i-1] * (1 - alpha))
    return emas

def calculate_rsi(prices, window=14):
    if len(prices) < window: return 50
    diffs = np.diff(prices); gains = np.maximum(diffs, 0); losses = np.abs(np.minimum(diffs, 0))
    avg_gain = np.mean(gains[-window:]); avg_loss = np.mean(losses[-window:])
    if avg_loss == 0: return 100
    if avg_gain == 0: return 0
    return 100 - (100 / (1 + (avg_gain / avg_loss)))

def calculate_vwap(h, l, c, v):
    if len(c) < 50: return c[-1]
    typ_price = (h[-50:] + l[-50:] + c[-50:]) / 3
    vol_sum = np.sum(v[-50:])
    if vol_sum == 0: return c[-1]
    return np.sum(typ_price * v[-50:]) / vol_sum

def analyze_fvg(o, h, l, c):
    fvg_list = []
    for i in range(1, len(c)-1):
        if l[i-1] > h[i+1]: fvg_list.append({'type': 'Bearish', 'top': l[i-1], 'bottom': h[i+1], 'index': i})
        elif h[i-1] < l[i+1]: fvg_list.append({'type': 'Bullish', 'top': l[i+1], 'bottom': h[i-1], 'index': i})
    return fvg_list

def analyze_structure(h, l, c):
    pivots = []
    for i in range(2, len(c)-2):
        if h[i] == max(h[i-2:i+3]): pivots.append({'type': 'HH' if not pivots or h[i] > pivots[-1]['price'] else 'LH', 'price': h[i], 'index': i})
        elif l[i] == min(l[i-2:i+3]): pivots.append({'type': 'HL' if not pivots or l[i] > pivots[-1]['price'] else 'LL', 'price': l[i], 'index': i})
    
    trend = 'Neutral'; bos_choch = None
    if len(pivots) >= 4:
        recent = pivots[-4:]
        if recent[-1]['type'] in ['HH', 'HL'] and recent[-3]['type'] in ['HH', 'HL']: trend = 'Bullish'
        elif recent[-1]['type'] in ['LL', 'LH'] and recent[-3]['type'] in ['LL', 'LH']: trend = 'Bearish'
        if trend == 'Bearish' and c[-1] > recent[-1]['price']: bos_choch = 'CHoCH_Bullish'
        elif trend == 'Bullish' and c[-1] < recent[-1]['price']: bos_choch = 'CHoCH_Bearish'
    return trend, bos_choch

async def safe_create_order(sym, order_type, side, qty, params):
    try:
        return await exchange.create_order(sym, order_type, side, qty, params=params)
    except Exception as e:
        error_msg = str(e)
        if "109400" in error_msg or "Hedge mode" in error_msg or "ReduceOnly" in error_msg:
            clean_params = {k: v for k, v in params.items() if k != 'reduceOnly'}
            return await exchange.create_order(sym, order_type, side, qty, params=clean_params)
        raise e

async def execute_trade(sym, signal_data, strategy_name="SMC Async"):
    global active_positions, COOLDOWN_CACHE
    clean_sym = sym.split(':')[0].split('/')[0]
    
    if len(active_positions) >= MAX_POSITIONS or any(p['symbol'] == sym for p in active_positions): return
    if clean_sym in COOLDOWN_CACHE and time.time() < COOLDOWN_CACHE[clean_sym]: return
        
    direction, current_price, sl_price, tp_price = signal_data['mode'], signal_data['price'], signal_data['sl_price'], signal_data['tp_price']
    
    same_direction_count = len([p for p in active_positions if p['direction'] == direction])
    if same_direction_count >= MAX_POSITIONS_PER_DIRECTION:
        logging.info(f"🚫 [РИСК] {clean_sym} отклонен: Лимит корреляции ({MAX_POSITIONS_PER_DIRECTION} {direction} уже в рынке).")
        return

    is_weekend = datetime.now(timezone.utc).weekday() >= 5
    active_risk = BASE_RISK_PER_TRADE / 2.0 if is_weekend else BASE_RISK_PER_TRADE
    
    try:
        bal = await exchange.fetch_balance()
        free_usdt = float(bal.get('USDT', {}).get('free', 0))
        if free_usdt <= 0: return

        actual_sl_dist = abs(current_price - sl_price)
        if actual_sl_dist <= 0: return
        sl_pct = (actual_sl_dist / current_price) * 100
        if sl_pct > MAX_SL_PCT:
            COOLDOWN_CACHE[clean_sym] = time.time() + 3600
            return
            
        risk_amount = free_usdt * active_risk
        qty_coins = risk_amount / actual_sl_dist
        max_margin_lock = free_usdt * MAX_MARGIN_PCT 
        max_notional_usdt = max_margin_lock * LEVERAGE
        target_notional = qty_coins * current_price
        
        if target_notional > max_notional_usdt: qty_coins = max_notional_usdt / current_price
        qty = float(exchange.amount_to_precision(sym, qty_coins))
        if qty <= 0: return
        
        pos_side = 'LONG' if direction == 'Long' else 'SHORT'
        try: await exchange.set_leverage(LEVERAGE, sym, params={'side': pos_side})
        except: pass
        
        side = 'buy' if direction == 'Long' else 'sell'
        sl_side = 'sell' if direction == 'Long' else 'buy'
        
        order = await exchange.create_market_order(sym, side, qty, params={'positionSide': pos_side})
    except Exception as e: 
        logging.error(f"Trade Open Error {sym}: {e}")
        COOLDOWN_CACHE[clean_sym] = time.time() + 3600 
        return

    sl_id = None
    try:
        sl_ord = await safe_create_order(sym, 'stop_market', sl_side, qty, {'triggerPrice': sl_price, 'reduceOnly': True, 'positionSide': pos_side})
        sl_id = sl_ord['id']
    except Exception as e:
        COOLDOWN_CACHE[clean_sym] = time.time() + 14400 
        await send_tg_msg(f"🚨 <b>АВАРИЯ ПО {clean_sym}</b>\nОшибка установки SL: <code>{e}</code>\nЭкстренное закрытие...")
        try: await safe_create_order(sym, 'market', sl_side, qty, {'positionSide': pos_side, 'reduceOnly': True})
        except: pass
        return 

    active_positions.append({
        'symbol': sym, 'direction': direction, 'entry_price': current_price, 
        'initial_qty': qty, 'current_qty': qty, 'sl_price': sl_price, 'current_sl': sl_price, 
        'tp1': tp_price, 'sl_order_id': sl_id, 'open_time': datetime.now(timezone.utc).isoformat(),
        'strategy': strategy_name, 'be_moved': False, 'tp80_hit': False, 'tp100_hit': False,
        'atr': actual_sl_dist * 0.5,
        'mfe_price': current_price, 'mae_price': current_price
    })
    await asyncio.to_thread(save_positions)
    
    session_text = "🟠 Выходные (Риск /2)" if is_weekend else "🟢 Полный объем"
    
    btc_dist = signal_data.get('btc_ema_dist', 0)
    btc_eval = "Сильный" if abs(btc_dist) > 1.5 else "Умеренный" if abs(btc_dist) > 0.5 else "Флэт/Слабый"
    btc_trend_str = f"{signal_data.get('btc_trend', 'N/A')} ({btc_eval}, откл. {btc_dist:+.2f}%)"

    alt_str = f"🟢 Активен (Перевес ETH {signal_data.get('eth_btc_diff', 0):+.2f}%)" if signal_data.get('altseason') else "🔴 Выключен"
    setup_eval = "💎 Топовый (Высокий объем)" if signal_data.get('vol_ratio', 0) > 2.0 else "👍 Хороший (Средний объем)"
    tp_mult = signal_data.get('tp_mult', 1.5)
    tp_eval = "Высокая" if tp_mult > 1.5 else "Норма"

    analytics_text = (f"📊 <b>Аналитика сетапа (SMC):</b>\n"
                      f"• Тренд BTC: {btc_trend_str}\n"
                      f"• Альтсезон: {alt_str}\n"
                      f"• Защита: {session_text}\n"
                      f"• Подтверждение: {signal_data.get('confirm_type', 'N/A')} ✅\n"
                      f"• Слом структуры: {signal_data.get('choch_type', 'N/A')} [{setup_eval}]\n"
                      f"• Осциллятор (RSI): {signal_data.get('rsi', 0):.1f}\n"
                      f"• Динамический TP: {tp_mult}x (Волатильность: {tp_eval})")

    msg = (f"💥 <b>ВЫСТРЕЛ [SMC Async v8.65 PROP]: {clean_sym}</b>\n"
           f"Направление: <b>#{direction}</b>\nЦена: {current_price}\n"
           f"Объем: {qty}\nSL: {sl_price} ({sl_pct:.2f}%)\n"
           f"Smart TP Цель: {tp_price}\n\n{analytics_text}")
    
    await send_tg_msg(msg)

async def monitor_positions_task():
    global active_positions, daily_stats, COOLDOWN_CACHE
    while True:
        try:
            if not active_positions: 
                await asyncio.sleep(15)
                continue
                
            symbols_to_fetch = [p['symbol'] for p in active_positions]
            positions_raw = await exchange.fetch_positions(symbols_to_fetch)
            tickers = await exchange.fetch_tickers(symbols_to_fetch)
            
            updated = []
            for pos in active_positions:
                sym = pos['symbol']
                clean_name = sym.split(':')[0].split('/')[0]
                is_long = pos['direction'] == 'Long'
                entry_price = pos['entry_price']
                
                if 'current_qty' not in pos: pos['current_qty'] = pos['initial_qty']
                if 'be_moved' not in pos: pos['be_moved'] = False
                if 'tp80_hit' not in pos: pos['tp80_hit'] = False
                if 'tp100_hit' not in pos: pos['tp100_hit'] = False
                if 'atr' not in pos: pos['atr'] = abs(entry_price - pos['sl_price']) * 0.5
                if 'current_sl' not in pos: pos['current_sl'] = pos['sl_price']
                if 'open_time' not in pos: pos['open_time'] = datetime.now(timezone.utc).isoformat()
                if 'mfe_price' not in pos: pos['mfe_price'] = entry_price
                if 'mae_price' not in pos: pos['mae_price'] = entry_price

                curr = next((r for r in positions_raw if r['symbol'] == sym and abs(float(r.get('contracts', 0))) > 0), None)
                ticker = tickers.get(sym, {}).get('last', entry_price)
                pos_side = 'LONG' if is_long else 'SHORT'
                sl_side = 'sell' if is_long else 'buy'
                
                # --- АБСОЛЮТНАЯ СИНХРОНИЗАЦИЯ ОБЪЕМОВ (DUST FIX) ---
                if curr:
                    real_qty = abs(float(curr.get('contracts', 0)))
                    if real_qty > 0:
                        pos['current_qty'] = real_qty
                # ---------------------------------------------------

                try: 
                    ohlcv = await exchange.fetch_ohlcv(sym, timeframe='1m', limit=2)
                    high_p = max([float(c[2]) for c in ohlcv]); low_p = min([float(c[3]) for c in ohlcv])
                except: high_p = low_p = ticker

                if is_long:
                    pos['mfe_price'] = max(pos['mfe_price'], high_p)
                    pos['mae_price'] = min(pos['mae_price'], low_p)
                else:
                    pos['mfe_price'] = min(pos['mfe_price'], low_p)
                    pos['mae_price'] = max(pos['mae_price'], high_p)
                
                if not curr:
                    seconds_passed = (datetime.now(timezone.utc) - datetime.fromisoformat(pos['open_time'])).total_seconds()
                    if seconds_passed < 60:
                        updated.append(pos)
                        continue

                    exit_price = pos['current_sl']
                    real_close_type = "SL (Синтетика)"
                    try:
                        trades = await exchange.fetch_my_trades(sym, limit=5)
                        if trades:
                            open_ts = int(datetime.fromisoformat(pos['open_time']).timestamp() * 1000)
                            valid_trades = [t for t in trades if t['timestamp'] >= open_ts]
                            if valid_trades:
                                exit_price = valid_trades[-1]['price']
                                real_close_type = "Биржа (Реал. цена)"
                    except Exception:
                        exit_price = ticker
                        real_close_type = "Тикер"

                    if is_long:
                        pos['mfe_price'] = max(pos['mfe_price'], exit_price)
                        pos['mae_price'] = min(pos['mae_price'], exit_price)
                    else:
                        pos['mfe_price'] = min(pos['mfe_price'], exit_price)
                        pos['mae_price'] = max(pos['mae_price'], exit_price)

                    raw_pnl = (exit_price - entry_price) * pos['current_qty'] if is_long else (entry_price - exit_price) * pos['current_qty']
                    entry_fee = pos['initial_qty'] * entry_price * FEE_RATE
                    exit_fee = pos['current_qty'] * exit_price * FEE_RATE
                    chunk_pnl = raw_pnl - (entry_fee + exit_fee)
                    
                    mfe_pct = abs(pos['mfe_price'] - entry_price) / entry_price * 100
                    mae_pct = abs(pos['mae_price'] - entry_price) / entry_price * 100
                    duration_min = seconds_passed / 60
                    metrics_str = f"\n⏱ Время в сделке: {int(duration_min)} мин.\n📈 MFE: +{mfe_pct:.2f}%\n📉 MAE: -{mae_pct:.2f}%"

                    daily_stats['trades'] = daily_stats.get('trades', 0) + 1
                    daily_stats['pnl'] = daily_stats.get('pnl', 0.0) + chunk_pnl
                    COOLDOWN_CACHE[clean_name] = time.time() + 14400
                    
                    if chunk_pnl < 0 and not pos['be_moved']:
                        daily_stats['gross_loss'] = daily_stats.get('gross_loss', 0.0) + abs(chunk_pnl)
                        await send_tg_msg(f"🛑 <b>{clean_name} закрыта ({real_close_type}).</b>\nЧистый PNL: {chunk_pnl:.2f} USDT{metrics_str}")
                    else:
                        daily_stats['wins'] = daily_stats.get('wins', 0) + 1
                        daily_stats['gross_profit'] = daily_stats.get('gross_profit', 0.0) + chunk_pnl
                        await send_tg_msg(f"✅ <b>{clean_name} закрыта ({real_close_type}).</b>\nЧистый PNL: {chunk_pnl:+.2f} USDT{metrics_str}")
                    continue

                hours_passed = (datetime.now(timezone.utc) - datetime.fromisoformat(pos['open_time'])).total_seconds() / 3600
                
                raw_pnl_timeout = (ticker - entry_price) * pos['current_qty'] if is_long else (entry_price - ticker) * pos['current_qty']
                est_fee_timeout = (pos['initial_qty'] * entry_price * FEE_RATE) + (pos['current_qty'] * ticker * FEE_RATE)
                net_pnl_timeout = raw_pnl_timeout - est_fee_timeout

                mfe_pct = abs(pos['mfe_price'] - entry_price) / entry_price * 100

                if hours_passed >= 3.0 or (hours_passed >= 2.5 and net_pnl_timeout > 0) or (hours_passed >= 1.0 and mfe_pct < 0.5):
                    try:
                        await safe_create_order(sym, 'market', sl_side, pos['current_qty'], {'positionSide': pos_side, 'reduceOnly': True})
                        if pos.get('sl_order_id'): await exchange.cancel_order(pos['sl_order_id'], sym)
                        
                        if is_long:
                            pos['mfe_price'] = max(pos['mfe_price'], ticker)
                            pos['mae_price'] = min(pos['mae_price'], ticker)
                        else:
                            pos['mfe_price'] = min(pos['mfe_price'], ticker)
                            pos['mae_price'] = max(pos['mae_price'], ticker)

                        mfe_pct = abs(pos['mfe_price'] - entry_price) / entry_price * 100
                        mae_pct = abs(pos['mae_price'] - entry_price) / entry_price * 100
                        duration_min = hours_passed * 60
                        metrics_str = f"\n⏱ Время в сделке: {int(duration_min)} мин.\n📈 MFE: +{mfe_pct:.2f}%\n📉 MAE: -{mae_pct:.2f}%"

                        daily_stats['trades'] = daily_stats.get('trades', 0) + 1
                        daily_stats['pnl'] = daily_stats.get('pnl', 0.0) + net_pnl_timeout
                        COOLDOWN_CACHE[clean_name] = time.time() + 14400
                        if net_pnl_timeout > 0: daily_stats['wins'] = daily_stats.get('wins', 0) + 1; daily_stats['gross_profit'] = daily_stats.get('gross_profit', 0.0) + net_pnl_timeout
                        else: daily_stats['gross_loss'] = daily_stats.get('gross_loss', 0.0) + abs(net_pnl_timeout)
                        await send_tg_msg(f"{'✅' if net_pnl_timeout > 0 else '🛑'} <b>{clean_name} закрыта по ТАЙМАУТУ!</b>\nЧистый PNL: {net_pnl_timeout:+.2f} USDT{metrics_str}")
                        continue
                    except: pass

                entry = pos['entry_price']
                dist = pos['tp1'] - entry
                
                tp50 = entry + dist * 0.50
                tp70 = entry + dist * 0.70
                tp100 = pos['tp1']

                abs_1_5_pct = entry * 1.015 if is_long else entry * 0.985
                abs_2_5_pct = entry * 1.025 if is_long else entry * 0.975

                if not pos['be_moved']:
                    if (is_long and (high_p >= tp50 or high_p >= abs_1_5_pct)) or (not is_long and (low_p <= tp50 or low_p <= abs_1_5_pct)):
                        be_price = float(exchange.price_to_precision(sym, entry * (1.002 if is_long else 0.998)))
                        if pos.get('sl_order_id'): 
                            try: await exchange.cancel_order(pos['sl_order_id'], sym)
                            except: pass
                        new_sl = await safe_create_order(sym, 'stop_market', sl_side, pos['current_qty'], {'triggerPrice': be_price, 'reduceOnly': True, 'positionSide': pos_side})
                        pos.update({'be_moved': True, 'sl_order_id': new_sl['id'], 'current_sl': be_price})
                        await send_tg_msg(f"🛡 <b>{clean_name}</b>: Сработал Smart Б/У (+1.5%). SL перенесен в безубыток.")

                if pos['be_moved'] and not pos['tp80_hit']:
                    if (is_long and (high_p >= tp70 or high_p >= abs_2_5_pct)) or (not is_long and (low_p <= tp70 or low_p <= abs_2_5_pct)):
                        close_qty = float(exchange.amount_to_precision(sym, pos['initial_qty'] * 0.25))
                        if close_qty > 0:
                            await safe_create_order(sym, 'market', sl_side, close_qty, {'positionSide': pos_side, 'reduceOnly': True})
                            if pos.get('sl_order_id'): 
                                try: await exchange.cancel_order(pos['sl_order_id'], sym)
                                except: pass
                            pos['current_qty'] = float(exchange.amount_to_precision(sym, pos['current_qty'] - close_qty))
                            new_sl = await safe_create_order(sym, 'stop_market', sl_side, pos['current_qty'], {'triggerPrice': pos['current_sl'], 'reduceOnly': True, 'positionSide': pos_side})
                            
                            exec_price = abs_2_5_pct if (is_long and high_p >= abs_2_5_pct) or (not is_long and low_p <= abs_2_5_pct) else tp70
                            raw_chunk = (exec_price - entry) * close_qty if is_long else (entry - exec_price) * close_qty
                            chunk_net = raw_chunk - (close_qty * exec_price * FEE_RATE)
                            daily_stats['pnl'] = daily_stats.get('pnl', 0.0) + chunk_net
                            pos.update({'tp80_hit': True, 'sl_order_id': new_sl['id']})
                            await send_tg_msg(f"💸 <b>{clean_name}</b>: Цена прошла +2.5%. Ранняя фиксация 25% объема. (Чистый PNL: +{chunk_net:.2f})")

                if pos['tp80_hit'] and not pos['tp100_hit']:
                    if (is_long and high_p >= tp100) or (not is_long and low_p <= tp100):
                        close_qty = float(exchange.amount_to_precision(sym, pos['initial_qty'] * 0.50))
                        if close_qty > 0:
                            await safe_create_order(sym, 'market', sl_side, close_qty, {'positionSide': pos_side, 'reduceOnly': True})
                            if pos.get('sl_order_id'): 
                                try: await exchange.cancel_order(pos['sl_order_id'], sym)
                                except: pass
                            pos['current_qty'] = float(exchange.amount_to_precision(sym, pos['current_qty'] - close_qty))
                            
                            trail_sl = float(exchange.price_to_precision(sym, tp100 - pos['atr'] if is_long else tp100 + pos['atr']))
                            new_sl = await safe_create_order(sym, 'stop_market', sl_side, pos['current_qty'], {'triggerPrice': trail_sl, 'reduceOnly': True, 'positionSide': pos_side})
                            
                            raw_chunk = (tp100 - entry) * close_qty if is_long else (entry - tp100) * close_qty
                            chunk_net = raw_chunk - (close_qty * tp100 * FEE_RATE)
                            daily_stats['pnl'] = daily_stats.get('pnl', 0.0) + chunk_net
                            daily_stats['wins'] = daily_stats.get('wins', 0) + 1 
                            pos.update({'tp100_hit': True, 'sl_order_id': new_sl['id'], 'current_sl': trail_sl})
                            await send_tg_msg(f"💰 <b>{clean_name}</b>: Тейк 100% взят! Закрыто еще 50%. Включен Трейлинг. (Чистый PNL: +{chunk_net:.2f})")

                if pos['tp100_hit']:
                    trail_sl = float(exchange.price_to_precision(sym, high_p - pos['atr'] if is_long else low_p + pos['atr']))
                    if (is_long and trail_sl > pos['current_sl']) or (not is_long and trail_sl < pos['current_sl']):
                        if pos.get('sl_order_id'): 
                            try: await exchange.cancel_order(pos['sl_order_id'], sym)
                            except: pass
                        try:
                            new_sl = await safe_create_order(sym, 'stop_market', sl_side, pos['current_qty'], {'triggerPrice': trail_sl, 'reduceOnly': True, 'positionSide': pos_side})
                            pos.update({'sl_order_id': new_sl['id'], 'current_sl': trail_sl})
                        except: pass

                updated.append(pos)
            active_positions = updated; await asyncio.to_thread(save_positions)
        except Exception as e: pass
        await asyncio.sleep(15)

async def process_smc_coin(sym, ctx, sem):
    async with sem:
        try:
            await asyncio.sleep(0.3)
            ohlcv = await exchange.fetch_ohlcv(sym, timeframe=SMC_TIMEFRAME, limit=200)
            if not ohlcv or len(ohlcv) < 50: return sym, 'no_data'
            
            o, h, l, c, v = np.array([x[1] for x in ohlcv], dtype=float), np.array([x[2] for x in ohlcv], dtype=float), np.array([x[3] for x in ohlcv], dtype=float), np.array([x[4] for x in ohlcv], dtype=float), np.array([x[5] for x in ohlcv], dtype=float)
            trend, bos_choch = analyze_structure(h, l, c)
            fvgs = analyze_fvg(o, h, l, c)
            current_price = c[-1]; ema200 = calculate_ema(c, 200)
            ema_dist = ((current_price - ema200) / ema200) * 100

            if abs(ema_dist) < 0.8: return sym, 'ema_too_close'
            
            emas_array = calculate_ema_array(c, 200)
            trend_candles = 0
            if current_price > emas_array[-1]:
                for i in range(1, min(100, len(c))):
                    if c[-i] > emas_array[-i]: trend_candles += 1
                    else: break
            else:
                for i in range(1, min(100, len(c))):
                    if c[-i] < emas_array[-i]: trend_candles += 1
                    else: break
                    
            if trend_candles > 80: return sym, 'trend_exhausted'
            
            is_green_candle = c[-1] > o[-1]
            is_red_candle = c[-1] < o[-1]
            
            vwap = calculate_vwap(h, l, c, v)
            rsi_curr = calculate_rsi(c[:-1], 14)
            rsi_prev = calculate_rsi(c[:-2], 14)
            
            # --- DYNAMIC TP (ATR MULTIPLIER) ---
            tr = np.maximum(h[1:]-l[1:], np.maximum(np.abs(h[1:]-c[:-1]), np.abs(l[1:]-c[:-1])))
            current_atr = np.mean(tr[-14:])
            baseline_atr = np.mean(tr[-50:-14]) if len(tr) >= 50 else current_atr
            
            tp_multiplier = 1.5
            if baseline_atr > 0:
                if current_atr > baseline_atr * 2.0: tp_multiplier = 2.5
                elif current_atr > baseline_atr * 1.5: tp_multiplier = 2.0
            # -----------------------------------
            
            active_fvg = next((fvg for fvg in reversed(fvgs) if (
                (fvg['type'] == 'Bullish' and current_price > fvg['top'] and (len(c) - fvg['index']) <= 15) or 
                (fvg['type'] == 'Bearish' and current_price < fvg['bottom'] and (len(c) - fvg['index']) <= 15)
            )), None)
            
            if not bos_choch: return sym, 'no_choch'
            if not active_fvg: return sym, 'no_fvg'
            
            avg_vol = np.mean(v[-22:-2])
            vol_ratio = v[-2] / avg_vol if avg_vol > 0 else 0
            if vol_ratio < 1.20 or vol_ratio > 5.0: return sym, 'no_volume'
            
            candle_body = abs(c[-2] - o[-2]) / o[-2] * 100
            if candle_body < 0.5: return sym, 'weak_candle'

            mode = 'Long' if bos_choch == 'CHoCH_Bullish' and active_fvg['type'] == 'Bullish' else 'Short' if bos_choch == 'CHoCH_Bearish' and active_fvg['type'] == 'Bearish' else None
            if not mode: return sym, 'no_setup'
            
            if mode == 'Long' and rsi_curr > 55: return sym, 'rsi_exhausted'
            if mode == 'Short' and rsi_curr < 45: return sym, 'rsi_exhausted'
            
            btc_trend = ctx['btc_trend']
            altseason = ctx['altseason']
            
            if mode == 'Long' and btc_trend == 'Short' and not altseason: return sym, 'wrong_trend'
            if mode == 'Short' and btc_trend == 'Long': return sym, 'wrong_trend'

            vwap_dist = ((current_price - vwap) / vwap) * 100
            if mode == 'Long' and vwap_dist > -0.8: return sym, 'vwap_reject' 
            if mode == 'Short' and vwap_dist < 0.8: return sym, 'vwap_reject' 
            
            confirm_type = ""
            if mode == 'Long':
                if not is_green_candle: return sym, 'no_confirm'
                if rsi_curr < rsi_prev: return sym, 'rsi_falling'
                if ema_dist > 8.0: return sym, 'overextended' 
                confirm_type = "Зеленая свеча + Загиб RSI"
            else:
                if not is_red_candle: return sym, 'no_confirm'
                if rsi_curr > rsi_prev: return sym, 'rsi_falling'
                if ema_dist < -8.0: return sym, 'overextended'
                confirm_type = "Красная свеча + Загиб RSI"

            try:
                fr = await exchange.fetch_funding_rate(sym)
                funding_rate = float(fr.get('fundingRate', 0))
            except: funding_rate = 0.0
            
            if mode == 'Short' and funding_rate < -0.00015:
                return sym, 'short_squeeze_risk'

            if mode == 'Long':
                sl_price = min(l[active_fvg['index']:]) - (current_atr * 3.0) 
                if (current_price - sl_price) / current_price * 100 < MIN_SL_PCT: sl_price = current_price * (1 - MIN_SL_PCT/100)
                tp_price = current_price + (current_price - sl_price) * tp_multiplier
            else:
                sl_price = max(h[active_fvg['index']:]) + (current_atr * 3.0)
                if (sl_price - current_price) / current_price * 100 < MIN_SL_PCT: sl_price = current_price * (1 + MIN_SL_PCT/100)
                tp_price = current_price - (sl_price - current_price) * tp_multiplier

            return sym, {
                'mode': mode, 'price': current_price, 'sl_price': sl_price, 'tp_price': tp_price,
                'btc_trend': btc_trend, 'btc_ema_dist': ctx['btc_ema_dist'],
                'altseason': altseason, 'eth_btc_diff': ctx['eth_btc_diff'],
                'fvg_type': active_fvg['type'], 'choch_type': bos_choch,
                'ema_dist': ema_dist, 'rsi': rsi_curr, 'vwap': vwap, 'vol_ratio': vol_ratio,
                'confirm_type': confirm_type, 'tp_mult': tp_multiplier
            }
        except: return sym, 'error'

async def smc_radar_task():
    global NOTIFIED_SYMBOLS, NEWS_EVENTS
    await exchange.load_markets() 
    while True:
        try:
            # --- NEWS FILTER (KILL-SWITCH) ---
            now_ts = datetime.now(timezone.utc).timestamp()
            is_news_pause = any(abs(ev - now_ts) < 15 * 60 for ev in NEWS_EVENTS)
            if is_news_pause:
                logging.info("🛑 [NEWS FILTER] Обнаружен выход макро-новостей! Пауза торговли на 15 мин...")
                await asyncio.sleep(60)
                continue
            # ---------------------------------

            if len(active_positions) >= MAX_POSITIONS: await asyncio.sleep(60); continue
            
            btc_trend = 'Long'
            altseason = False
            btc_ema_dist = 0.0
            eth_btc_diff = 0.0
            
            try:
                try: btc_ohlcv = await exchange.fetch_ohlcv('BTC/USDT:USDT', timeframe=SMC_TIMEFRAME, limit=205)
                except: btc_ohlcv = await exchange.fetch_ohlcv('BTC/USDT', timeframe=SMC_TIMEFRAME, limit=205)
                btc_c = np.array([x[4] for x in btc_ohlcv], dtype=float)
                btc_ema = calculate_ema(btc_c, 200)
                btc_trend = 'Long' if btc_c[-1] > btc_ema else 'Short'
                btc_ema_dist = (btc_c[-1] - btc_ema) / btc_ema * 100

                try: eth_ohlcv = await exchange.fetch_ohlcv('ETH/USDT:USDT', timeframe=SMC_TIMEFRAME, limit=205)
                except: eth_ohlcv = await exchange.fetch_ohlcv('ETH/USDT', timeframe=SMC_TIMEFRAME, limit=205)
                eth_c = np.array([x[4] for x in eth_ohlcv], dtype=float)
                
                eth_return = (eth_c[-1] - eth_c[-50]) / eth_c[-50] * 100
                btc_return = (btc_c[-1] - btc_c[-50]) / btc_c[-50] * 100
                eth_btc_diff = eth_return - btc_return
                
                if eth_btc_diff > 0.5 and eth_c[-1] > calculate_ema(eth_c, 200):
                    altseason = True
            except: pass
            
            global_ctx = {
                'btc_trend': btc_trend,
                'btc_ema_dist': btc_ema_dist,
                'altseason': altseason,
                'eth_btc_diff': eth_btc_diff
            }

            tickers = await exchange.fetch_tickers()
            temp_symbols = []
            stats = {'total': len(tickers), 'high_spread': 0, 'no_choch': 0, 'no_fvg': 0, 'no_volume': 0, 'wrong_trend': 0, 'vwap_reject': 0, 'overextended': 0, 'no_confirm': 0, 'rsi_exhausted': 0, 'passed': 0, 'ema_too_close': 0, 'trend_exhausted': 0, 'short_squeeze_risk': 0}
            
            for sym, tick in tickers.items():
                clean_sym = sym.split(':')[0].split('/')[0]
                if sym.endswith(':USDT') and exchange.markets.get(sym, {}).get('active') is not False and not any(kw in sym.upper() for kw in EXCLUDED_KEYWORDS):
                    
                    # --- SPREAD PROTECTION ---
                    ask = float(tick.get('ask') or 0)
                    bid = float(tick.get('bid') or 0)
                    if bid > 0 and ask > 0 and ((ask - bid) / bid * 100) > 0.3:
                        stats['high_spread'] += 1
                        continue
                    # -------------------------

                    if clean_sym in COOLDOWN_CACHE and time.time() < COOLDOWN_CACHE[clean_sym]: continue
                    if any(pos['symbol'].split(':')[0].split('/')[0] == clean_sym for pos in active_positions): continue
                    temp_symbols.append((sym, float(tick.get('quoteVolume') or 0)))
            
            valid_symbols_data = [sym for sym, vol in temp_symbols if vol >= MIN_VOLUME_USDT][:SCAN_LIMIT]
            logging.info(f"⏳ [SMC РАДАР] Опрос {len(valid_symbols_data)} монет (Спред-фильтр отсек: {stats['high_spread']})...")
            
            sem = asyncio.Semaphore(10); tasks = [process_smc_coin(s, global_ctx, sem) for s in valid_symbols_data]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            valid_results = []
            for r in results:
                if isinstance(r, tuple) and len(r) == 2:
                    sym, signal = r
                    if signal == 'no_choch': stats['no_choch'] += 1
                    elif signal == 'no_fvg': stats['no_fvg'] += 1
                    elif signal == 'no_volume' or signal == 'weak_candle': stats['no_volume'] += 1
                    elif signal == 'wrong_trend': stats['wrong_trend'] += 1
                    elif signal == 'vwap_reject': stats['vwap_reject'] += 1
                    elif signal == 'overextended': stats['overextended'] += 1
                    elif signal == 'no_confirm' or signal == 'rsi_falling': stats['no_confirm'] += 1
                    elif signal == 'rsi_exhausted': stats['rsi_exhausted'] += 1
                    elif signal == 'ema_too_close': stats['ema_too_close'] += 1 
                    elif signal == 'trend_exhausted': stats['trend_exhausted'] += 1
                    elif signal == 'short_squeeze_risk': stats['short_squeeze_risk'] += 1
                    elif isinstance(signal, dict): 
                        stats['passed'] += 1
                        valid_results.append((sym, signal))

            logging.info(f"🔎 [SMC] Пила({stats['ema_too_close']}) Слом({stats['no_choch']}) FVG({stats['no_fvg']}) Усталость Тренда({stats['trend_exhausted']}) Объем({stats['no_volume']}) Макро({stats['wrong_trend']}) Фандинг({stats['short_squeeze_risk']}) -> ВХОДЫ: {stats['passed']}")

            for sym, signal in valid_results:
                if sym not in NOTIFIED_SYMBOLS: 
                    NOTIFIED_SYMBOLS.add(sym)
                    await execute_trade(sym, signal, "SMC Async")
            gc.collect(); await asyncio.sleep(60) 
        except Exception as e: logging.error(f"SMC Radar Error: {e}"); await asyncio.sleep(60)

async def print_stats_hourly():
    global daily_stats, REPORTED_TODAY
    while True:
        try:
            now = datetime.now(timezone.utc)
            if now.hour == 20 and not REPORTED_TODAY:
                bal = await exchange.fetch_balance(); current_balance = float(bal.get('USDT', {}).get('total', 0))
                start_bal = daily_stats.get('start_balance', 0.0)
                pct_change = ((current_balance - start_bal) / start_bal * 100) if start_bal > 0 else 0.0
                winrate = (daily_stats['wins'] / daily_stats['trades'] * 100) if daily_stats['trades'] > 0 else 0
                await send_tg_msg(f"🗓 <b>ИТОГИ ДНЯ (DUAL Async v8.65 PROP):</b> {now.strftime('%d.%m.%Y')}\n\n📉 Закрыто сделок: {daily_stats['trades']}\n🎯 Винрейт: {winrate:.1f}%\n💵 Net PNL: {daily_stats['pnl']:+.2f} USDT\n\n🏦 <b>Баланс:</b> {current_balance:.2f} USDT\n📊 <b>Изменение:</b> {pct_change:+.2f}%\n<i>*В работе: {len(active_positions)}</i>")
                daily_stats = {'pnl': 0.0, 'trades': 0, 'wins': 0, 'prev_winrate': winrate, 'start_balance': current_balance, 'gross_profit': 0.0, 'gross_loss': 0.0}
                await asyncio.to_thread(save_positions); REPORTED_TODAY = True
            elif now.hour != 20: REPORTED_TODAY = False
            
            if now.minute == 0:
                active = ", ".join([p['symbol'].split(':')[0].split('/')[0] for p in active_positions]) or "Нет"
                winrate = (daily_stats['wins'] / daily_stats['trades'] * 100) if daily_stats['trades'] > 0 else 0
                logging.info(f"📊 [ТЕЛЕМЕТРИЯ v8.65 PROP] В работе: {active} | Винрейт: {winrate:.1f}%\n")
        except: pass
        await asyncio.sleep(60)

class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self): self.send_response(200); self.end_headers(); self.wfile.write(b"BingX Async Bot Active v8.65 PROP")
    def log_message(self, format, *args): return 

def run_server():
    server = HTTPServer(('0.0.0.0', int(os.environ.get('PORT', 10000))), HealthCheckHandler); server.serve_forever()

async def main():
    init_db(); load_positions()
    if daily_stats.get('start_balance', 0.0) == 0.0:
        try: bal = await exchange.fetch_balance(); daily_stats['start_balance'] = float(bal.get('USDT', {}).get('total', 0)); await asyncio.to_thread(save_positions)
        except: pass
        
    logging.info("🚀 Запуск BINGX ASYNC БОТА v8.65 (Tier-1.5: News Filter, Spread Protection, Dynamic TP)...")
    await send_tg_msg("🟢 <b>BINGX ASYNC БОТ v8.65 PROP</b> запущен (Внедрен News Filter, защита от спреда и Динамический TP)!")
    Thread(target=run_server, daemon=True).start()
    
    asyncio.create_task(fetch_news_task())
    asyncio.create_task(monitor_positions_task())
    asyncio.create_task(print_stats_hourly())
    asyncio.create_task(smc_radar_task())
    while True: await asyncio.sleep(3600)

if __name__ == '__main__':
    try: asyncio.run(main())
    except KeyboardInterrupt: pass
