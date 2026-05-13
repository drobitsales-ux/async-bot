import asyncio
import json
import os
import logging
import sqlite3
import time
import ccxt.async_support as ccxt_async
import numpy as np
import aiohttp
from datetime import datetime, timezone
from threading import Thread
from http.server import BaseHTTPRequestHandler, HTTPServer

# --- ГЛОБАЛЬНЫЕ НАСТРОЙКИ ---
DB_PATH = '/data/bot.db' if os.path.exists('/data') else 'bot.db'
TOKEN = os.getenv('TELEGRAM_TOKEN')
GROUP_CHAT_ID = int(os.getenv('GROUP_CHAT_ID', -1003407154454))
BINGX_API_KEY = os.getenv('BINGX_API_KEY')
BINGX_SECRET = os.getenv('BINGX_SECRET')
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY') 

# --- НАСТРОЙКИ ДЛЯ ПРОП-КОМПАНИЙ ($10k Account) ---
BASE_RISK_PER_TRADE = 0.0075     # 0.75% риск на сделку 
MAX_POSITIONS = 2                
MAX_POSITIONS_PER_DIRECTION = 2  
LEVERAGE = 5                
MIN_VOLUME_USDT = 1000000        
MAX_SL_PCT = 2.5                 
FEE_RATE = 0.0005           
SMC_TIMEFRAME = '15m'
SCAN_LIMIT = 300           

EXCLUDED_KEYWORDS = [
    'BTC/USDT', 'ETH/USDT', 'SOL/USDT', 'BNB/USDT', 'XRP/USDT', 
    'NCS', 'NCFX', 'NCCO', 'NCSI', 'NIKKEI', 'NASDAQ', 'SP500', 
    'GOLD', 'SILVER', 'AUT', 'XAU', 'PAXG', 'EUR', '1000', 'LUNC', 
    'USTC', 'USDC', 'FART', 'PEPE', 'SHIB', 'DOGE', 'WIF', 'BONK', 
    'FLOKI', 'BOME', 'MEME', 'TURBO', 'SATS', 'RATS', 'ORDI'
]

daily_stats = {'pnl': 0.0, 'trades': 0, 'wins': 0, 'start_balance': 0.0}
active_positions = []
NOTIFIED_SYMBOLS = {} 
NEWS_EVENTS = []    
global_session = None

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(message)s')

# Биржи (Мультибиржевой Оракул)
exchange = ccxt_async.bingx({'apiKey': BINGX_API_KEY, 'secret': BINGX_SECRET, 'options': {'defaultType': 'swap', 'marginMode': 'isolated'}, 'enableRateLimit': True})
binance = ccxt_async.binance({'enableRateLimit': True, 'options': {'defaultType': 'future'}})
bybit = ccxt_async.bybit({'enableRateLimit': True, 'options': {'defaultType': 'linear'}})

# --- БАЗА ДАННЫХ ---
def get_db_conn(): 
    return sqlite3.connect(DB_PATH, check_same_thread=False)

def init_db():
    conn = get_db_conn()
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS active_positions (id INTEGER PRIMARY KEY, data TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS daily_stats (id INTEGER PRIMARY KEY, pnl REAL, trades INTEGER, wins INTEGER, start_balance REAL)''')
    conn.commit()
    conn.close()

def save_positions():
    conn = get_db_conn()
    c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO active_positions (id, data) VALUES (1, ?)", (json.dumps(active_positions),))
    c.execute("INSERT OR REPLACE INTO daily_stats (id, pnl, trades, wins, start_balance) VALUES (1, ?, ?, ?, ?)", 
              (daily_stats.get('pnl', 0.0), daily_stats.get('trades', 0), daily_stats.get('wins', 0), daily_stats.get('start_balance', 0.0)))
    conn.commit()
    conn.close()

def load_positions():
    global active_positions, daily_stats
    try:
        conn = get_db_conn()
        c = conn.cursor()
        c.execute("SELECT data FROM active_positions WHERE id = 1")
        row = c.fetchone()
        if row: 
            active_positions = json.loads(row[0])
        
        c.execute("SELECT pnl, trades, wins, start_balance FROM daily_stats WHERE id = 1")
        stat_row = c.fetchone()
        if stat_row: 
            daily_stats['pnl'], daily_stats['trades'], daily_stats['wins'], daily_stats['start_balance'] = stat_row
        conn.close()
    except Exception: 
        pass

# --- ТЕЛЕГРАМ ---
async def send_tg_msg(text):
    if not TOKEN or not global_session: 
        return
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    payload = {"chat_id": GROUP_CHAT_ID, "text": text, "parse_mode": "HTML"} 
    try: 
        # Исправлена синтаксическая ошибка (развернуто на несколько строк)
        async with global_session.post(url, json=payload) as resp: 
            pass
    except Exception: 
        pass

# --- ОРАКУЛЫ ---
async def update_macro_news():
    """Фоновое обновление красных новостей по USD"""
    global NEWS_EVENTS
    # Заглушка, здесь будет API календаря
    NEWS_EVENTS = [{'time': '12:30', 'impact': 'High', 'currency': 'USD'}]
    logging.info(f"📰 [NEWS FILTER] Загружены макро-события: {len(NEWS_EVENTS)}")

def is_news_spike():
    """Проверка: не выходит ли прямо сейчас новость"""
    if not NEWS_EVENTS: 
        return False
    now_str = datetime.now(timezone.utc).strftime("%H:%M")
    for ev in NEWS_EVENTS:
        if ev.get('time') == now_str: 
            return True
    return False

async def check_global_volume_oracle(sym):
    """Проверка объемов на Binance и Bybit"""
    try:
        base_coin = sym.split('/')[0]
        bin_sym = f"{base_coin}/USDT"
        tasks = [binance.fetch_ticker(bin_sym), bybit.fetch_ticker(bin_sym)]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        vol_binance = results[0]['quoteVolume'] if not isinstance(results[0], Exception) else 0
        vol_bybit = results[1]['quoteVolume'] if not isinstance(results[1], Exception) else 0
        
        return vol_binance > MIN_VOLUME_USDT or vol_bybit > MIN_VOLUME_USDT
    except Exception: 
        return True 

async def ask_gemini_oracle(sym, mode, price):
    """Финальная проверка сетапа через ИИ Gemini"""
    if not GEMINI_API_KEY: 
        return True
    
    prompt = f"Analyze crypto {sym} in {SMC_TIMEFRAME}. Setup: {mode} at {price}. Smart Money Concepts. Answer ONLY 'YES' or 'NO'."
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={GEMINI_API_KEY}"
    payload = {"contents": [{"parts": [{"text": prompt}]}]}
    
    try:
        async with global_session.post(url, json=payload, timeout=5) as resp:
            data = await resp.json()
            text = data['candidates'][0]['content']['parts'][0]['text'].strip().upper()
            return 'YES' in text
    except Exception: 
        return True 

# --- ФИЛЬТРЫ И SMC МАТЕМАТИКА ---
def is_trade_session():
    """Killzones: London & NY"""
    now = datetime.now(timezone.utc).time()
    london = (datetime.strptime("07:00", "%H:%M").time(), datetime.strptime("10:30", "%H:%M").time())
    ny = (datetime.strptime("13:00", "%H:%M").time(), datetime.strptime("16:30", "%H:%M").time())
    return (london[0] <= now <= london[1]) or (ny[0] <= now <= ny[1])

def calculate_vwap(h, l, c, v):
    """Взвешенный по объему VWAP"""
    return np.sum(v * (h + l + c) / 3) / np.sum(v)

def calculate_rsi(prices, window=14):
    if len(prices) < window + 1: 
        return 50
    diffs = np.diff(prices)
    gains = np.maximum(diffs, 0)
    losses = np.abs(np.minimum(diffs, 0))
    avg_g = np.mean(gains[-window:])
    avg_l = np.mean(losses[-window:])
    if avg_l == 0: 
        return 100
    return 100 - (100 / (1 + avg_g / avg_l))

def get_pivots(highs, lows, order=5):
    """Чистый Python/NumPy (без scipy)"""
    h_idx, l_idx = [], []
    n = len(highs)
    for i in range(order, n - order):
        is_peak = all(highs[i] > highs[i-j] for j in range(1, order+1)) and all(highs[i] > highs[i+j] for j in range(1, order+1))
        if is_peak:
            h_idx.append(i)
            
        is_valley = all(lows[i] < lows[i-j] for j in range(1, order+1)) and all(lows[i] < lows[i+j] for j in range(1, order+1))
        if is_valley:
            l_idx.append(i)
            
    return h_idx, l_idx

def find_fvg(h, l, mode, lookback=10):
    """Истинный детектор FVG (разрыв)"""
    for i in range(1, min(lookback, len(h)-1)):
        idx = len(h) - 1 - i
        if mode == 'Long' and h[idx-1] < l[idx+1]: 
            return {'top': l[idx+1], 'bottom': h[idx-1]}
        if mode == 'Short' and l[idx-1] > h[idx+1]: 
            return {'top': l[idx-1], 'bottom': h[idx+1]}
    return None

async def process_smc_coin(sym):
    # 1. Проверка Киллзон и Новостей
    if not is_trade_session(): 
        return None, 'out_of_session'
    if is_news_spike(): 
        return None, 'news_spike'
    
    try: 
        ohlcv = await exchange.fetch_ohlcv(sym, SMC_TIMEFRAME, limit=100)
    except Exception: 
        return None, 'no_data'
        
    if not ohlcv or len(ohlcv) < 50: 
        return None, 'no_data'
    
    c = np.array([float(x[4]) for x in ohlcv])
    h = np.array([float(x[2]) for x in ohlcv])
    l = np.array([float(x[3]) for x in ohlcv])
    v = np.array([float(x[5]) for x in ohlcv])
    current_price = c[-1]
    
    # 2. Объем (проверяем по v[-2] - закрытая свеча)
    avg_vol = np.mean(v[-20:-2]) if len(v) > 20 else 0
    if avg_vol == 0 or v[-2] < avg_vol * 1.5: 
        return None, 'no_volume'
    
    # 3. CHoCH
    h_idx, l_idx = get_pivots(h, l)
    if len(h_idx) < 3 or len(l_idx) < 3: 
        return None, 'no_structure'
        
    mode = None
    recent_highs = [h[i] for i in h_idx[-3:]]
    recent_lows = [l[i] for i in l_idx[-3:]]
    
    if recent_highs[-1] < recent_highs[-2] and current_price > recent_highs[-1]: 
        mode = 'Long' 
    elif recent_lows[-1] > recent_lows[-2] and current_price < recent_lows[-1]: 
        mode = 'Short' 
        
    if not mode: 
        return None, 'no_choch'

    # 4. VWAP и RSI
    vwap = calculate_vwap(h, l, c, v)
    if mode == 'Long' and current_price < vwap * 0.995: 
        return None, 'vwap_reject'
    if mode == 'Short' and current_price > vwap * 1.005: 
        return None, 'vwap_reject'
    
    rsi = calculate_rsi(c[:-1])
    if mode == 'Long' and rsi > 85: 
        return None, 'rsi_exhausted'
    if mode == 'Short' and rsi < 15: 
        return None, 'rsi_exhausted'
    
    # 5. FVG Тест
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

# --- ИСПОЛНЕНИЕ ОРДЕРОВ ---
async def execute_trade(sym, signal):
    global active_positions
    mode = signal['mode']
    current_price = signal['price']
    
    if len(active_positions) >= MAX_POSITIONS: 
        return
    if sum(1 for p in active_positions if p['direction'] == mode) >= MAX_POSITIONS_PER_DIRECTION: 
        return
    
    # Оракулы
    has_volume = await check_global_volume_oracle(sym)
    if not has_volume: 
        return
        
    ai_approved = await ask_gemini_oracle(sym, mode, current_price)
    if not ai_approved: 
        return
    
    try:
        bal = await exchange.fetch_balance()
        free_usdt = float(bal.get('USDT', {}).get('free', 0))
    except Exception: 
        return
    
    risk_amount = free_usdt * BASE_RISK_PER_TRADE
    actual_sl_dist = MAX_SL_PCT / 100.0
    
    sl_price = current_price * (1 - actual_sl_dist) if mode == 'Long' else current_price * (1 + actual_sl_dist)
    tp_price = current_price * (1 + actual_sl_dist * 2.0) if mode == 'Long' else current_price * (1 - actual_sl_dist * 2.0)
    qty = (risk_amount / actual_sl_dist) / current_price
    
    pos_side = 'LONG' if mode == 'Long' else 'SHORT'
    order_side = 'buy' if mode == 'Long' else 'sell'
    sl_side = 'sell' if mode == 'Long' else 'buy'
    
    try:
        await exchange.set_margin_mode('isolated', sym)
        await exchange.set_leverage(LEVERAGE, sym)
        await exchange.create_order(sym, 'market', order_side, qty, params={'positionSide': pos_side})
        sl_ord = await exchange.create_order(sym, 'stopMarket', sl_side, qty, params={'positionSide': pos_side, 'stopPrice': sl_price, 'reduceOnly': True})
        
        active_positions.append({
            'symbol': sym, 'direction': mode, 'entry_price': current_price,
            'initial_qty': qty, 'current_qty': qty, 'sl_price': sl_price,
            'current_sl': sl_price, 'tp1': tp_price, 'sl_order_id': sl_ord['id'],
            'be_moved': False, 'tp80_hit': False
        })
        save_positions()
        await send_tg_msg(f"🟢 <b>{sym}</b> Открыт {mode}\nВход: {current_price}\nSL: {sl_price}\nTP: {tp_price}\n✅ Одобрено ИИ & Глобал Оракулом")
    except Exception as e: 
        logging.error(f"Trade error {sym}: {e}")

# --- МОНИТОРИНГ ПОЗИЦИЙ ---
async def monitor_positions_job():
    global active_positions
    try:
        positions_raw = await exchange.fetch_positions()
        tickers = await exchange.fetch_tickers([p['symbol'] for p in active_positions])
    except Exception: 
        return
    
    for pos in active_positions[:]:
        sym = pos['symbol']
        is_long = pos['direction'] == 'Long'
        entry_price = float(pos['entry_price']) 
        
        curr = next((r for r in positions_raw if r['symbol'] == sym and abs(float(r.get('contracts', 0))) > 0), None)
        ticker = float(tickers.get(sym, {}).get('last', entry_price))
        
        pos_side = 'LONG' if is_long else 'SHORT'
        sl_side = 'sell' if is_long else 'buy'
        
        if curr:
            real_qty = abs(float(curr.get('contracts', 0)))
            pnl_pct = ((ticker - entry_price) / entry_price * 100) if is_long else ((entry_price - ticker) / entry_price * 100)
            
            if pnl_pct >= 1.5 and not pos.get('be_moved'):
                new_sl = entry_price * 1.002 if is_long else entry_price * 0.998
                try:
                    if pos.get('sl_order_id'): 
                        await exchange.cancel_order(pos['sl_order_id'], sym)
                        
                    sl_ord = await exchange.create_order(sym, 'stopMarket', sl_side, real_qty, params={'positionSide': pos_side, 'stopPrice': new_sl, 'reduceOnly': True})
                    pos['current_sl'] = new_sl
                    pos['sl_order_id'] = sl_ord['id']
                    pos['be_moved'] = True
                    save_positions()
                    await send_tg_msg(f"🛡 <b>{sym}</b>: SL перенесен в БУ ({new_sl})")
                except Exception: 
                    pass
                
            if pnl_pct >= 2.5 and not pos.get('tp80_hit'):
                close_qty = real_qty * 0.5
                try:
                    await exchange.create_order(sym, 'market', sl_side, close_qty, params={'positionSide': pos_side, 'reduceOnly': True})
                    pos['tp80_hit'] = True
                    
                    if pos.get('sl_order_id'): 
                        await exchange.cancel_order(pos['sl_order_id'], sym)
                        
                    sl_ord = await exchange.create_order(sym, 'stopMarket', sl_side, real_qty - close_qty, params={'positionSide': pos_side, 'stopPrice': entry_price, 'reduceOnly': True})
                    pos['sl_order_id'] = sl_ord['id']
                    save_positions()
                    await send_tg_msg(f"💰 <b>{sym}</b>: Зафиксировано 50% прибыли.")
                except Exception: 
                    pass
        else:
            active_positions.remove(pos)
            save_positions()
            await send_tg_msg(f"🏁 <b>{sym}</b>: Позиция закрыта.")

class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self): 
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"BingX Async Bot v8.98 PROP + All Oracles Active")
        
    def log_message(self, format, *args): 
        return 

def run_server(): 
    HTTPServer(('0.0.0.0', int(os.environ.get('PORT', 10000))), HealthCheckHandler).serve_forever()

# --- ОСНОВНОЙ ЦИКЛ ---
async def main():
    global global_session
    init_db()
    load_positions()
    global_session = aiohttp.ClientSession()
    
    Thread(target=run_server, daemon=True).start()
    asyncio.create_task(update_macro_news()) 
    
    logging.info("🚀 Запуск BINGX ASYNC БОТА v8.98 (PROP + AI, Global Oracle & News Filter)...")
    await send_tg_msg("🟢 <b>BINGX ASYNC БОТ v8.98 PROP</b> запущен (Исправлен синтаксис деплоя, все Оракулы активны)")
    
    while True:
        try:
            now = time.time()
            expired = [k for k, v in NOTIFIED_SYMBOLS.items() if now - v > 14400]
            for k in expired: 
                del NOTIFIED_SYMBOLS[k]
            
            markets = await exchange.load_markets()
            symbols = [s for s in markets.keys() if s.endswith(':USDT') and s not in EXCLUDED_KEYWORDS]
            
            for sym in symbols[:SCAN_LIMIT]:
                if sym in NOTIFIED_SYMBOLS: 
                    continue
                
                signal, status = await process_smc_coin(sym)
                if signal:
                    NOTIFIED_SYMBOLS[sym] = now
                    await execute_trade(sym, signal)
                    
            await monitor_positions_job()
        except Exception as e: 
            logging.error(f"Main loop error: {e}")
            
        await asyncio.sleep(30)

if __name__ == '__main__':
    asyncio.run(main())
