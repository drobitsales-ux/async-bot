"""
╔══════════════════════════════════════════════════════════════╗
║         BYBIT WORKER  v1.0  —  Copy Trading Service         ║
║                                                              ║
║  Получает сигналы от BingX-бота через HTTP webhook           ║
║  и исполняет их на Bybit с независимым риск-менеджментом.   ║
║                                                              ║
║  Деплой: отдельный Web Service на Render                     ║
║  Environment Variables:                                      ║
║    BYBIT_API_KEY     = ваш ключ Bybit                       ║
║    BYBIT_SECRET      = ваш секрет Bybit                     ║
║    WORKER_SECRET     = тот же что в основном боте           ║
║    TELEGRAM_TOKEN    = токен бота (можно тот же)             ║
║    GROUP_CHAT_ID     = ID группы                            ║
║                                                              ║
║  Риск-параметры — меняйте под свою проп-компанию:           ║
║    PROP_BALANCE      = 10000  (депозит проп-компании)       ║
# Для теста $100 → 2%, для проп $10k → 0.75% (меняйте RISK_PCT в Render)
RISK_PER_TRADE   = float(os.getenv('RISK_PCT', '0.02'))       # default 2%
║    LEVERAGE          = 5                                     ║
╚══════════════════════════════════════════════════════════════╝
"""

import asyncio
import json
import logging
import os
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread
from urllib.parse import urlparse

import aiohttp
import ccxt.async_support as ccxt_async

# ══════════════════════════════════════════════════════════
#  КОНФИГУРАЦИЯ
# ══════════════════════════════════════════════════════════
BYBIT_KEY      = os.getenv('BYBIT_API_KEY', '')
BYBIT_SECRET   = os.getenv('BYBIT_SECRET', '')
WORKER_SECRET  = os.getenv('WORKER_SECRET', 'change-me-secret')
TOKEN          = os.getenv('TELEGRAM_TOKEN', '')
CHAT_ID_STR    = os.getenv('GROUP_CHAT_ID', os.getenv('CHAT_ID', '-1'))
try:
    CHAT_ID = int(CHAT_ID_STR.strip().strip('"').strip("'"))
except ValueError:
    CHAT_ID = -1

# ── Параметры проп-компании ──────────────────────────────
PROP_BALANCE     = float(os.getenv('PROP_BALANCE', '10000'))  # USDT
RISK_PER_TRADE   = float(os.getenv('RISK_PCT', '0.0075'))     # 0.75%
LEVERAGE         = int(os.getenv('LEVERAGE', '5'))
MAX_POSITIONS    = int(os.getenv('MAX_POS', '2'))             # макс одновременных
DAILY_DD_LIMIT   = float(os.getenv('DAILY_DD', '0.025'))      # 2.5%
MIN_SL_PCT       = 1.0   # минимальный SL %
MAX_SL_PCT       = 2.5   # максимальный SL %
FEE_RATE         = 0.0006  # Bybit taker fee

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | [BYBIT] %(message)s'
)

# ── Биржа ────────────────────────────────────────────────
exchange = ccxt_async.bybit({
    'apiKey':  BYBIT_KEY,
    'secret':  BYBIT_SECRET,
    'options': {
        'defaultType': 'linear',   # USDT perpetual
        'positionMode': 'hedge',   # hedge mode для SL
    },
    'enableRateLimit': True,
})

# ── Состояние ────────────────────────────────────────────
active_positions = []   # [{symbol, direction, entry_price, qty, sl_price, sl_order_id}]
daily_pnl_pct    = 0.0
day_start_time   = time.time()
circuit_open     = False
http_session     = None

# ══════════════════════════════════════════════════════════
#  TELEGRAM
# ══════════════════════════════════════════════════════════
async def tg(text: str):
    if not TOKEN or CHAT_ID == -1 or not http_session:
        return
    try:
        async with http_session.post(
            f'https://api.telegram.org/bot{TOKEN}/sendMessage',
            json={'chat_id': CHAT_ID, 'text': f'[BYBIT] {text}', 'parse_mode': 'HTML'},
            timeout=aiohttp.ClientTimeout(total=5)
        ):
            pass
    except Exception:
        pass

# ══════════════════════════════════════════════════════════
#  CIRCUIT BREAKER
# ══════════════════════════════════════════════════════════
def check_daily_reset():
    global daily_pnl_pct, day_start_time, circuit_open
    if time.time() - day_start_time > 86400:
        daily_pnl_pct = 0.0
        day_start_time = time.time()
        circuit_open   = False
        logging.info("📅 Daily stats reset")

def is_trading_allowed() -> bool:
    global circuit_open
    if daily_pnl_pct <= -DAILY_DD_LIMIT:
        if not circuit_open:
            circuit_open = True
            logging.warning(f"🔴 CIRCUIT BREAKER: DD {daily_pnl_pct*100:.2f}%")
            asyncio.create_task(
                tg(f"🔴 <b>CIRCUIT BREAKER</b>\nDD: {daily_pnl_pct*100:.2f}% > лимит {DAILY_DD_LIMIT*100:.1f}%\nТорговля остановлена до следующего дня.")
            )
        return False
    return True

# ══════════════════════════════════════════════════════════
#  ИСПОЛНЕНИЕ СИГНАЛА
# ══════════════════════════════════════════════════════════
async def execute_signal(signal: dict):
    """Исполняет сигнал от BingX-бота на Bybit."""
    global active_positions

    sym       = signal.get('symbol', '')
    mode      = signal.get('direction', '')
    entry     = float(signal.get('entry', 0))
    sl        = float(signal.get('sl', 0))
    tp        = float(signal.get('tp', 0))
    strategy  = signal.get('strategy', '?')

    if not sym or not mode or not entry or not sl:
        logging.warning(f"⚠️ Некорректный сигнал: {signal}")
        return

    # Лимиты позиций
    if len(active_positions) >= MAX_POSITIONS:
        logging.info(f"⏸ {sym}: макс позиций ({MAX_POSITIONS}) достигнут")
        return

    same_dir = sum(1 for p in active_positions if p['direction'] == mode)
    if same_dir >= 1:
        logging.info(f"⏸ {sym}: уже есть {mode} позиция")
        return

    # Circuit breaker
    check_daily_reset()
    if not is_trading_allowed():
        return

    # Проверка SL дистанции
    sl_pct = abs(entry - sl) / entry * 100
    if sl_pct < MIN_SL_PCT:
        logging.warning(f"⚠️ {sym}: SL {sl_pct:.2f}% < минимум {MIN_SL_PCT}%")
        return
    if sl_pct > MAX_SL_PCT:
        logging.warning(f"⚠️ {sym}: SL {sl_pct:.2f}% > максимум {MAX_SL_PCT}%")
        return

    # Расчёт размера позиции
    try:
        bal       = await exchange.fetch_balance()
        free_usdt = float(bal.get('USDT', {}).get('free', 0))
    except Exception as e:
        logging.error(f"Balance error: {e}")
        return

    if free_usdt < 10:
        logging.warning(f"⚠️ Недостаточно баланса: {free_usdt} USDT")
        return

    risk_amount = free_usdt * RISK_PER_TRADE
    sl_dist     = abs(entry - sl)
    qty         = round(risk_amount / sl_dist, 4)

    if qty <= 0:
        logging.warning(f"⚠️ {sym}: qty={qty} <= 0")
        return

    # Bybit minimum notional ~$5 USD
    notional = qty * entry
    if notional < 5.0:
        min_bal = 5.0 / (RISK_PER_TRADE * sl_pct / 100) if sl_pct > 0 else 999
        logging.warning(
            f"⚠️ {sym}: notional ${notional:.2f} < $5 Bybit minimum. "
            f"Balance ${free_usdt:.2f}. Need ~${min_bal:.0f} USDT."
        )
        return

    pos_side   = 'Buy'  if mode == 'Long' else 'Sell'
    order_side = 'buy'  if mode == 'Long' else 'sell'
    sl_side    = 'sell' if mode == 'Long' else 'buy'

    logging.info(
        f"⚡ Исполнение [{strategy}] {sym} {mode} | "
        f"qty={qty} | SL={sl:.6f} | Risk=${risk_amount:.2f}"
    )

    try:
        # Установка плеча
        try:
            await exchange.set_leverage(LEVERAGE, sym,
                                        params={'buyLeverage': LEVERAGE,
                                                'sellLeverage': LEVERAGE})
        except Exception:
            pass  # уже установлено

        # Рыночный вход
        await exchange.create_order(
            sym, 'market', order_side, qty,
            params={'positionIdx': 1 if mode == 'Long' else 2}
        )

        # Stop-Loss
        sl_ord = await exchange.create_order(
            sym, 'Stop', sl_side, qty,
            params={
                'triggerPrice':   round(sl, 8),
                'triggerBy':      'LastPrice',
                'reduceOnly':     True,
                'positionIdx':    1 if mode == 'Long' else 2,
            }
        )

        rec = {
            'symbol':       sym,
            'direction':    mode,
            'entry_price':  entry,
            'qty':          qty,
            'sl_price':     sl,
            'tp_price':     tp,
            'sl_order_id':  sl_ord.get('id', ''),
            'strategy':     strategy,
            'open_time':    time.time(),
            'be_moved':     False,
        }
        active_positions.append(rec)

        await tg(
            f"{'🟢' if mode=='Long' else '🔴'} <b>[{strategy}] {sym}</b> — {mode}\n"
            f"Вход: <code>{entry:.6f}</code> | SL: <code>{sl:.6f}</code>\n"
            f"Риск: <b>${risk_amount:.2f}</b> ({RISK_PER_TRADE*100:.2f}% от ${free_usdt:.0f})\n"
            f"📋 Сигнал от BingX-бота"
        )
        logging.info(f"✅ {sym} {mode} открыт на Bybit | Risk:${risk_amount:.2f}")

    except Exception as e:
        logging.error(f"❌ Order error {sym}: {e}")
        await tg(f"❌ Ошибка открытия <b>{sym}</b>: <code>{str(e)[:150]}</code>")

# ══════════════════════════════════════════════════════════
#  МОНИТОРИНГ ПОЗИЦИЙ
# ══════════════════════════════════════════════════════════
async def monitor():
    """Мониторинг открытых позиций: BE + обнаружение закрытий."""
    global active_positions, daily_pnl_pct

    if not active_positions:
        return

    syms = list({p['symbol'] for p in active_positions})
    try:
        pos_raw = await exchange.fetch_positions(syms)
        tickers = await exchange.fetch_tickers(syms)
    except Exception as e:
        logging.error(f"Monitor fetch error: {e}")
        return

    new_positions = []
    for pos in active_positions:
        sym     = pos['symbol']
        is_long = pos['direction'] == 'Long'
        entry   = float(pos['entry_price'])
        qty     = float(pos['qty'])

        curr_p = float((tickers.get(sym) or {}).get('last', entry))

        live = next(
            (r for r in pos_raw
             if r.get('symbol') == sym
             and abs(float(r.get('contracts', 0))) > 0),
            None
        )
        pos_idx = 1 if is_long else 2
        sl_side = 'sell' if is_long else 'buy'

        if live:
            real_qty = abs(float(live.get('contracts', 0)))
            pnl = ((curr_p - entry) / entry * 100 if is_long
                   else (entry - curr_p) / entry * 100)

            # Breakeven при +1.5%
            if pnl >= 1.5 and not pos.get('be_moved'):
                new_sl = entry * 1.002 if is_long else entry * 0.998
                try:
                    if pos.get('sl_order_id'):
                        await exchange.cancel_order(pos['sl_order_id'], sym)
                    sl_ord = await exchange.create_order(
                        sym, 'Stop', sl_side, real_qty,
                        params={'triggerPrice': round(new_sl, 8),
                                'triggerBy': 'LastPrice',
                                'reduceOnly': True,
                                'positionIdx': pos_idx}
                    )
                    pos['sl_price']    = new_sl
                    pos['sl_order_id'] = sl_ord.get('id', '')
                    pos['be_moved']    = True
                    await tg(f"🛡 <b>{sym}</b>: SL → БУ <code>{new_sl:.6f}</code> | P&L: +{pnl:.2f}%")
                except Exception as e:
                    logging.error(f"BE error {sym}: {e}")

            new_positions.append(pos)

        else:
            # Позиция закрыта
            pnl_pct = ((curr_p - entry) / entry * 100 if is_long
                       else (entry - curr_p) / entry * 100)
            is_win  = pnl_pct > 0

            dur_min = int((time.time() - pos['open_time']) / 60)
            daily_pnl_pct += pnl_pct / 100

            await tg(
                f"{'✅' if is_win else '🛑'} <b>[{pos['strategy']}] {sym}</b> закрыта\n"
                f"PnL: <code>{pnl_pct:+.2f}%</code> | ⏱ {dur_min}мин\n"
                f"День: {daily_pnl_pct*100:+.2f}%"
            )
            logging.info(f"{'✅' if is_win else '🛑'} {sym} закрыта | {pnl_pct:+.2f}%")

    active_positions[:] = new_positions

# ══════════════════════════════════════════════════════════
#  HTTP СЕРВЕР (принимает сигналы + health check)
# ══════════════════════════════════════════════════════════
_signal_queue: asyncio.Queue = None

class WebhookHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        """Health check для Render."""
        body = (
            f"Bybit Worker v1.0 | Positions: {len(active_positions)} | "
            f"DD: {daily_pnl_pct*100:+.2f}% | Circuit: {'OPEN' if circuit_open else 'OK'}"
        ).encode()
        self.send_response(200)
        self.send_header('Content-Type', 'text/plain')
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        """Принимает сигнал от BingX-бота."""
        if self.path != '/signal':
            self.send_response(404)
            self.end_headers()
            return

        try:
            length = int(self.headers.get('Content-Length', 0))
            body   = self.rfile.read(length)
            data   = json.loads(body)
        except Exception:
            self.send_response(400)
            self.end_headers()
            self.wfile.write(b'Bad JSON')
            return

        # Проверка секрета
        if data.get('secret') != WORKER_SECRET:
            self.send_response(403)
            self.end_headers()
            self.wfile.write(b'Forbidden')
            logging.warning("⚠️ Получен запрос с неверным WORKER_SECRET")
            return

        # Ставим сигнал в очередь для asyncio
        if _signal_queue is not None:
            _signal_queue.put_nowait(data)

        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps({'status': 'queued'}).encode())
        logging.info(f"📥 Сигнал получен: {data.get('symbol')} {data.get('direction')}")

    def log_message(self, *args):
        return  # подавляем HTTP логи


def run_http_server():
    port = int(os.environ.get('PORT', 10001))
    server = HTTPServer(('0.0.0.0', port), WebhookHandler)
    logging.info(f"🌐 HTTP сервер запущен на порту {port}")
    server.serve_forever()

# ══════════════════════════════════════════════════════════
#  ГЛАВНЫЙ ЦИКЛ
# ══════════════════════════════════════════════════════════
async def main():
    global http_session, _signal_queue

    _signal_queue = asyncio.Queue()
    http_session  = aiohttp.ClientSession()

    # HTTP сервер в отдельном потоке
    Thread(target=run_http_server, daemon=True).start()

    logging.info("🚀 Bybit Worker v1.0 запущен")
    logging.info(f"   Депозит: ${PROP_BALANCE:,.0f} | Риск: {RISK_PER_TRADE*100:.2f}%/сделку")
    logging.info(f"   Плечо: {LEVERAGE}x | Max позиций: {MAX_POSITIONS}")
    logging.info(f"   DD-лимит: {DAILY_DD_LIMIT*100:.1f}%/день")

    await tg(
        f"🟢 <b>Bybit Worker v1.0</b> запущен\n"
        f"Депозит: ${PROP_BALANCE:,.0f} | Риск: {RISK_PER_TRADE*100:.2f}%\n"
        f"Ожидаю сигналы от BingX-бота..."
    )

    try:
        cycle = 0
        while True:
            cycle += 1

            # Обработка входящих сигналов из очереди
            while not _signal_queue.empty():
                signal = await _signal_queue.get()
                await execute_signal(signal)

            # Мониторинг позиций каждые 30 сек
            if cycle % 2 == 0 and active_positions:
                await monitor()

            await asyncio.sleep(15)

    finally:
        logging.info("🛑 Bybit Worker остановлен")
        if http_session:
            await http_session.close()
        await exchange.close()


if __name__ == '__main__':
    asyncio.run(main())
