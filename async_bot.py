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
import telebot
import gc  # Сборщик мусора
from datetime import datetime, timezone, timedelta
from threading import Thread
from flask import Flask

# === НАСТРОЙКИ v7.2 (SMC + Multi-Exchange WSS + Pure Python) ===
DB_PATH = '/data/bot.db' 
TOKEN = os.getenv('TELEGRAM_TOKEN')
GROUP_CHAT_ID = int(os.getenv('GROUP_CHAT_ID', -1003407154454))
BINGX_API_KEY = os.getenv('BINGX_API_KEY')
BINGX_SECRET = os.getenv('BINGX_SECRET')

# Риск-менеджмент
RISK_PER_TRADE = 0.01       
MAX_POSITIONS = 3           
LEVERAGE = 10               
MIN_VOLUME_USDT = 2000000  # Фильтр ликвидности

# Настройки мониторинга
SMC_TIMEFRAME = '1h'
MAX_CONCURRENT_SNIPERS = 10  # ОГРАНИЧЕНИЕ: не более 10 WSS-воркеров одновременно для экономии RAM
EXCLUDED_KEYWORDS = ['NCCO', 'NCSI', 'NIKKEI', 'NASDAQ', 'SP500', 'GOLD', 'SILVER', 'EUR', 'JPY', 'PEPE', 'SHIB', '1000']

# Глобальные состояния
HOT_LIST = {}  
ACTIVE_WSS_CONNECTIONS = set()
active_positions = []
# Семафор для контроля нагрузки на память
sniper_semaphore = asyncio.Semaphore(MAX_CONCURRENT_SNIPERS)

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(message)s')
bot = telebot.TeleBot(TOKEN)

exchange = ccxt_async.bingx({
    'apiKey': BINGX_API_KEY, 'secret': BINGX_SECRET,
    'options': {'defaultType': 'swap', 'marginMode': 'isolated'},
    'enableRateLimit': True
})

# --- МАТЕМАТИКА НА ЧИСТОМ PYTHON (БЕЗ NUMPY) ---
def calculate_ema(prices, window):
    if len(prices) < window: return prices[-1]
    alpha = 2 / (window + 1)
    ema = sum(prices[:window]) / window
    for price in prices[window:]:
        ema = (price * alpha) + (ema * (1 - alpha))
    return ema

def calculate_atr(highs, lows, closes, window=14):
    if len(highs) < window + 1: return 0
    tr_list = []
    for i in range(1, len(highs)):
        tr = max(highs[i] - lows[i], abs(highs[i] - closes[i-1]), abs(lows[i] - closes[i-1]))
        tr_list.append(tr)
    return sum(tr_list[-window:]) / window

def find_fvg(h, l, direction):
    # Логика FVG из v7.0
    for i in range(len(h)-1, len(h)-4, -1):
        if direction == 'Long' and l[i] > h[i-2]: return True
        if direction == 'Short' and h[i] < l[i-2]: return True
    return False

# --- РАБОТА С БД ---
def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute('''CREATE TABLE IF NOT EXISTS positions 
                        (id TEXT PRIMARY KEY, symbol TEXT, side TEXT, entry REAL, qty REAL, sl REAL, tp REAL)''')

def save_positions():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("DELETE FROM positions")
        for p in active_positions:
            conn.execute("INSERT INTO positions VALUES (?,?,?,?,?,?,?)", 
                        (p['id'], p['symbol'], p['side'], p['entry'], p['qty'], p['current_sl'], p['tp']))

# --- ТОРГОВАЯ ЛОГИКА ---
async def send_tg_msg(text):
    try: bot.send_message(GROUP_CHAT_ID, text, parse_mode='Markdown')
    except: pass

async def execute_trade(setup):
    global active_positions
    if len(active_positions) >= MAX_POSITIONS: return
    sym = setup['symbol']
    pos_side = 'LONG' if setup['direction'] == 'Long' else 'SHORT'
    side = 'BUY' if setup['direction'] == 'Long' else 'SELL'
    
    try:
        balance = await exchange.fetch_balance()
        usdt = float(balance['total'].get('USDT', 0))
        risk_dist = abs(setup['entry_price'] - setup['sl'])
        if risk_dist == 0: return
        
        qty = (usdt * RISK_PER_TRADE / risk_dist) * LEVERAGE
        order = await exchange.create_market_order(sym, side, qty, params={'positionSide': pos_side})
        
        # Стоп-лосс
        await exchange.create_order(sym, 'stop_market', 'SELL' if side == 'BUY' else 'BUY', qty, 
                                  params={'triggerPrice': setup['sl'], 'positionSide': pos_side, 'stopLossPrice': setup['sl']})
        
        new_pos = {
            'id': order['id'], 'symbol': sym, 'side': side, 'position_side': pos_side,
            'entry': setup['entry_price'], 'qty': qty, 'current_sl': setup['sl'], 'tp': setup['tp1']
        }
        active_positions.append(new_pos)
        save_positions()
        await send_tg_msg(f"🚀 **ВХОД {pos_side}: {sym}**\nЦена: {setup['entry_price']}\nSL: {setup['sl']}\nTP: {setup['tp1']}\nПодтверждено объемом (Bybit+BingX)")
    except Exception as e:
        logging.error(f"Trade Error {sym}: {e}")

# --- МУЛЬТИ-БИРЖЕВОЙ СНАЙПЕР (Bybit + BingX) ---
async def wss_sniper_worker(sym, setup_data):
    async with sniper_semaphore: # Ограничиваем кол-во активных воркеров
        base = sym.split('/')[0].split('-')[0].split(':')[0]
        urls = {
            "Bybit": "wss://stream.bybit.com/v5/public/linear",
            "BingX": "wss://open-api-swap.bingx.com/swap-market"
        }
        
        vol_data = {'buy': 0.0, 'sell': 0.0, 'active': True}

        async def listen(name, url):
            try:
                # Оптимизация буфера: max_queue=16, ping_interval=20
                async for ws in websockets.connect(url, ping_interval=20, max_queue=16):
                    if name == "Bybit":
                        await ws.send(json.dumps({"op": "subscribe", "args": [f"publicTrade.{base}USDT"]}))
                    else:
                        await ws.send(json.dumps({"id": "1", "reqType": "sub", "dataType": f"{base}-USDT@trade"}))

                    while vol_data['active']:
                        msg = await ws.recv()
                        if name == "BingX":
                            raw = gzip.GzipFile(fileobj=io.BytesIO(msg)).read().decode('utf-8')
                            data = json.loads(raw)
                            if "data" in data:
                                for t in data["data"]:
                                    v = float(t['p']) * float(t['q'])
                                    if t.get('m'): vol_data['sell'] += v
                                    else: vol_data['buy'] += v
                        else:
                            data = json.loads(msg)
                            if "data" in data:
                                for t in data["data"]:
                                    v = float(t['p']) * float(t['v'])
                                    if t['S'] == "Sell": vol_data['sell'] += v
                                    else: vol_data['buy'] += v
            except: pass

        tasks = [asyncio.create_task(listen(k, v)) for k, v in urls.items()]
        
        try:
            for _ in range(24): # Сканируем 2 минуты (24 * 5 сек)
                await asyncio.sleep(5)
                total = vol_data['buy'] + vol_data['sell']
                if total > 5000:
                    buy_pct = (vol_data['buy'] / total) * 100
                    if (setup_data['direction'] == 'Long' and buy_pct > 65) or \
                       (setup_data['direction'] == 'Short' and buy_pct < 35):
                        await execute_trade(setup_data)
                        break
                vol_data['buy'] = vol_data['sell'] = 0.0
        finally:
            vol_data['active'] = False
            for t in tasks: t.cancel()
            ACTIVE_WSS_CONNECTIONS.discard(sym)
            gc.collect() # Чистим память после закрытия сокетов

# --- РАДАР (СКАН ВСЕГО РЫНКА) ---
async def detect_smc_setup(sym):
    try:
        # Лимит снижен до 50 свечей
        ohlcv = await exchange.fetch_ohlcv(sym, timeframe=SMC_TIMEFRAME, limit=50)
        if len(ohlcv) < 30: return None
        
        h, l, c, v = [x[2] for x in ohlcv], [x[3] for x in ohlcv], [x[4] for x in ohlcv], [x[5] for x in ohlcv]
        curr_p = c[-1]
        ema = calculate_ema(c, 25)
        atr = calculate_atr(h, l, c, 14)
        avg_v = sum(v[-10:]) / 10

        if curr_p > ema and v[-1] > avg_v:
            if find_fvg(h, l, 'Long'):
                sl = curr_p - (atr * 2)
                return {'symbol': sym, 'direction': 'Long', 'entry_price': curr_p, 'sl': sl, 'tp1': curr_p + (curr_p-sl)*1.5}
        
        if curr_p < ema and v[-1] > avg_v:
            if find_fvg(h, l, 'Short'):
                sl = curr_p + (atr * 2)
                return {'symbol': sym, 'direction': 'Short', 'entry_price': curr_p, 'sl': sl, 'tp1': curr_p - (sl-curr_p)*1.5}
    except: pass
    return None

async def radar_task():
    global HOT_LIST
    while True:
        try:
            markets = await exchange.load_markets()
            tickers = await exchange.fetch_tickers()
            
            # Сканируем весь рынок, проходя через фильтр объема
            valid_symbols = [s for s, t in tickers.items() 
                            if not any(k in s for k in EXCLUDED_KEYWORDS) 
                            and float(t.get('quoteVolume', 0)) > MIN_VOLUME_USDT]
            
            new_hot = {}
            for sym in valid_symbols:
                setup = await detect_smc_setup(sym)
                if setup:
                    new_hot[sym] = setup
                    if sym not in HOT_LIST:
                        logging.info(f"🎯 РАДАР: {sym} взят на мушку")
            
            HOT_LIST = new_hot
            logging.info(f"🔎 [РАДАР] Скан завершен. Целей: {len(HOT_LIST)}")
            gc.collect() # Принудительная очистка памяти
            await asyncio.sleep(300)
        except Exception as e:
            logging.error(f"Radar error: {e}"); await asyncio.sleep(60)

async def sniper_manager():
    while True:
        for sym, setup in list(HOT_LIST.items()):
            # Запускаем снайпер только если есть свободные слоты в семафоре
            if sym not in ACTIVE_WSS_CONNECTIONS and len(active_positions) < MAX_POSITIONS:
                ACTIVE_WSS_CONNECTIONS.add(sym)
                asyncio.create_task(wss_sniper_worker(sym, setup))
        await asyncio.sleep(10)

async def monitor_positions_job():
    global active_positions
    while True:
        try:
            await asyncio.sleep(60)
            if not active_positions: continue
            
            open_pos = await exchange.fetch_positions()
            active_syms = [p['symbol'] for p in open_pos if float(p['contracts']) > 0]
            
            updated = []
            for pos in active_positions:
                if pos['symbol'] in active_syms: updated.append(pos)
                else: logging.info(f"✅ Сделка {pos['symbol']} закрыта")
            
            active_positions = updated
            save_positions()
            gc.collect() # Чистим память после мониторинга
        except Exception as e: logging.error(f"Monitor error: {e}")

# --- WEB SERVER ---
app = Flask(__name__)
@app.route('/')
def index(): return f"v7.2 Active. RAM Optimized. HOT: {len(HOT_LIST)}"

async def main():
    init_db()
    logging.info("🚀 Запуск ядра v7.2 (Pure-Python + 2x WSS)...")
    await asyncio.gather(radar_task(), sniper_manager(), monitor_positions_job())

if __name__ == '__main__':
    Thread(target=lambda: app.run(host='0.0.0.0', port=10000), daemon=True).start()
    try: asyncio.run(main())
    except: pass
