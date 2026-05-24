"""
╔══════════════════════════════════════════════════════════════╗
║     BYBIT WORKER  v2.0  —  UTA (Unified Trading Account)    ║
║                                                              ║
║  Исправления v2.0:                                          ║
║  [FIX-1] UTA: positionIdx=0 (one-way), не hedge mode       ║
║  [FIX-2] SL встроен в entry ордер через stopLoss param      ║
║  [FIX-3] Leverage: строки + category=linear                 ║
║  [FIX-4] Убран set_margin_mode (UTA cross по умолч.)        ║
║  [FIX-5] Диагностический лог каждого этапа                  ║
║  [FIX-6] UTA balance: тип UNIFIED, не CONTRACT              ║
║                                                              ║
║  Environment Variables (Render):                            ║
║    BYBIT_API_KEY     = ваш ключ                             ║
║    BYBIT_SECRET      = ваш секрет                           ║
║    WORKER_SECRET     = тот же что в основном боте           ║
║    TELEGRAM_TOKEN    = токен бота                            ║
║    GROUP_CHAT_ID     = ID группы                            ║
║    PROP_BALANCE      = 100  (депозит для теста)             ║
║    RISK_PCT          = 0.02 (2% для теста $100)             ║
║    LEVERAGE          = 5                                     ║
║    MAX_POS           = 2                                     ║
╚══════════════════════════════════════════════════════════════╝
"""

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread

import aiohttp
import ccxt.async_support as ccxt_async

# ══════════════════════════════════════════════════════════
#  КОНФИГУРАЦИЯ
# ══════════════════════════════════════════════════════════
BYBIT_KEY     = os.getenv('BYBIT_API_KEY', '')
BYBIT_SECRET  = os.getenv('BYBIT_SECRET', '')
WORKER_SECRET = os.getenv('WORKER_SECRET', 'change-me-secret')
TOKEN         = os.getenv('TELEGRAM_TOKEN', '')

_raw_cid = os.getenv('GROUP_CHAT_ID', os.getenv('CHAT_ID', '-1')).strip().strip('"').strip("'")
try:
    CHAT_ID = int(_raw_cid)
except ValueError:
    CHAT_ID = -1

PROP_BALANCE   = float(os.getenv('PROP_BALANCE', '100'))
RISK_PER_TRADE = float(os.getenv('RISK_PCT', '0.02'))   # 2% для теста $100
LEVERAGE       = int(os.getenv('LEVERAGE', '5'))
MAX_POSITIONS  = int(os.getenv('MAX_POS', '2'))
MAX_TRADE_MIN_SMC = int(os.getenv('MAX_TRADE_MIN_SMC', '180'))  # SMC: 12 свечей (3ч)
MAX_TRADE_MIN_RSI = int(os.getenv('MAX_TRADE_MIN_RSI', '240'))  # RSI: 16 свечей (4ч)
DAILY_DD_LIMIT = float(os.getenv('DAILY_DD', '0.025'))
MIN_SL_PCT     = 1.0
MAX_SL_PCT     = 2.5

logging.basicConfig(level=logging.INFO, format='%(asctime)s | [BYBIT] %(message)s')

# ── Bybit UTA exchange ────────────────────────────────────
# [FIX-2] UTA: НЕТ positionMode hedge — UTA использует one-way (positionIdx=0)
exchange = ccxt_async.bybit({
    'apiKey':  BYBIT_KEY,
    'secret':  BYBIT_SECRET,
    'options': {
        'defaultType':     'linear',
        'fetchCurrencies': False,   # [FIX-403] CloudFront fix
        'adjustForTimeDifference': False,
    },
    'enableRateLimit': True,
})

# ── Состояние ────────────────────────────────────────────
active_positions = []
daily_pnl_pct    = 0.0
daily_pnl_usdt   = 0.0
daily_trades     = 0
daily_wins       = 0
day_start_bal    = 0.0
day_start_time   = time.time()
circuit_open     = False
_daily_report_sent = False
http_session     = None
_signal_queue: asyncio.Queue = None

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
    global daily_pnl_pct, daily_pnl_usdt, daily_trades, daily_wins, day_start_time, circuit_open, _daily_report_sent
    if time.time() - day_start_time > 86400:
        daily_pnl_pct  = 0.0
        daily_pnl_usdt = 0.0
        daily_trades   = 0
        daily_wins     = 0
        day_start_time = time.time()
        circuit_open   = False
        _daily_report_sent = False
        logging.info("📅 Daily stats reset")

def is_trading_allowed() -> bool:
    global circuit_open
    if daily_pnl_pct <= -DAILY_DD_LIMIT:
        if not circuit_open:
            circuit_open = True
            logging.warning(f"🔴 CIRCUIT BREAKER: DD={daily_pnl_pct*100:.2f}%")
            asyncio.create_task(tg(
                f"🔴 <b>CIRCUIT BREAKER</b>\n"
                f"DD: {daily_pnl_pct*100:.2f}% > лимит {DAILY_DD_LIMIT*100:.1f}%"
            ))
        return False
    return True

# ══════════════════════════════════════════════════════════
#  ИСПОЛНЕНИЕ СИГНАЛА (UTA v2.0)
# ══════════════════════════════════════════════════════════
async def execute_signal(signal: dict):
    """Исполняет сигнал на Bybit UTA (Unified Trading Account)."""
    global active_positions

    raw_sym  = signal.get('symbol', '')
    # [FIX-1] BingX 'APE/USDT:USDT' → Bybit 'APEUSDT'
    base = raw_sym.split('/')[0] if '/' in raw_sym else raw_sym
    sym = f'{base}USDT'
    mode     = signal.get('direction', '')
    entry    = float(signal.get('entry', 0))
    sl       = float(signal.get('sl', 0))
    tp       = float(signal.get('tp', 0))
    strategy = signal.get('strategy', '?')

    logging.info(f"📥 Получен сигнал: [{strategy}] {sym} {mode} | entry={entry} sl={sl}")

    if not sym or mode not in ('Long', 'Short') or not entry or not sl:
        logging.warning(f"⚠️ Некорректный сигнал: {signal}")
        return

    # Лимиты позиций
    if len(active_positions) >= MAX_POSITIONS:
        logging.info(f"⏸ {sym}: максимум позиций ({MAX_POSITIONS}) достигнут")
        return
    # Разрешаем до MAX_POSITIONS позиций в одном направлении
    # (MAX_POSITIONS=2: можно XMR Short + IMX Short одновременно)
    same_dir = sum(1 for p in active_positions if p['direction'] == mode)
    if same_dir >= MAX_POSITIONS:
        logging.info(f"⏸ {sym}: лимит {mode} позиций ({same_dir}/{MAX_POSITIONS})")
        return
    # [FIX-3] Запрет противоположной позиции по тому же символу
    if any(p['symbol'] == sym and p['direction'] != mode for p in active_positions):
        opp = 'Long' if mode == 'Short' else 'Short'
        logging.warning(f"⛔ {sym}: открыт {opp} — {mode} ЗАПРЕЩЁН (нет хеджа)")
        return

    check_daily_reset()
    if not is_trading_allowed():
        return

    # Проверка SL
    sl_pct = round(abs(entry - sl) / entry * 100, 4)  # [FIX-FLOAT] 2.5000001 → 2.5
    logging.info(f"📐 {sym}: SL дистанция {sl_pct:.4f}%")
    # Допуск 0.01% для floating-point edge cases (2.5000000000001 ≈ 2.5)
    if sl_pct < MIN_SL_PCT - 0.01 or sl_pct > MAX_SL_PCT + 0.01:
        logging.warning(
            f"⚠️ {sym}: SL {sl_pct:.4f}% вне диапазона "
            f"[{MIN_SL_PCT},{MAX_SL_PCT}]% — пропуск"
        )
        return

    # Баланс
    # [FIX-6] UTA: accountType = 'UNIFIED'
    try:
        bal = await exchange.fetch_balance({'type': 'unified'})
        # Пробуем разные ключи (ccxt нормализует по-разному)
        free_usdt = (
            float(bal.get('USDT', {}).get('free', 0)) or
            float(bal.get('total', {}).get('USDT', 0)) or
            float((bal.get('info', {}).get('result', {}).get('list', [{}])[0]
                   .get('totalAvailableBalance', 0)))
        )
        logging.info(f"💰 Баланс Bybit UTA: {free_usdt:.2f} USDT")
    except Exception as e:
        logging.error(
            f"❌ Ошибка баланса: {e}\n"
            f"   Проверьте BYBIT_API_KEY/SECRET и разрешения API:\n"
            f"   Нужны: Unified Trading Account + Contract: Orders + Positions"
        )
        return

    if free_usdt < 5:
        logging.warning(f"⚠️ Недостаточно баланса: {free_usdt:.2f} USDT")
        return

    risk_amount = free_usdt * RISK_PER_TRADE
    sl_dist     = abs(entry - sl)
    qty         = round(risk_amount / sl_dist, 4)
    qty         = max(round(qty, 2), 0.01)  # Bybit min precision

    if qty <= 0:
        logging.warning(f"⚠️ {sym}: qty={qty} <= 0")
        return

    # Минимальный notional Bybit ~$5
    notional = qty * entry
    if notional < 5.0:
        min_bal_needed = 5.0 / (RISK_PER_TRADE * sl_pct / 100)
        logging.warning(
            f"⚠️ {sym}: notional ${notional:.2f} < $5 (Bybit minimum). "
            f"Баланс ${free_usdt:.2f}. Нужно ~${min_bal_needed:.0f} USDT."
        )
        return

    order_side = 'buy'  if mode == 'Long'  else 'sell'
    sl_side    = 'sell' if mode == 'Long'  else 'buy'

    # [FIX-3] UTA one-way mode: positionIdx=0
    # Hedge mode в UTA не поддерживается через ccxt — используем one-way
    position_idx = 0

    logging.info(
        f"⚡ Исполнение [{strategy}] {sym} {mode} | "
        f"qty={qty} | SL={sl:.6f} | risk=${risk_amount:.2f} | notional=${notional:.2f}"
    )

    try:
        # [FIX-3] Leverage — строки, category=linear
        try:
            await exchange.set_leverage(
                LEVERAGE, sym,
                params={
                    'category':    'linear',
                    'buyLeverage': str(LEVERAGE),
                    'sellLeverage': str(LEVERAGE),
                }
            )
            logging.info(f"✅ {sym}: leverage {LEVERAGE}x установлен")
        except Exception as e:
            # Уже установлено — не критично
            logging.debug(f"ℹ️ {sym}: leverage: {e}")

        # [FIX-2] Сначала чистый entry без SL (inline SL срабатывает мгновенно!)
        entry_ord = await exchange.create_order(
            sym, 'market', order_side, qty,
            params={'category': 'linear', 'positionIdx': position_idx}
        )
        logging.info(f"✅ {sym}: market order открыт | id={entry_ord.get('id', '?')}")
        await asyncio.sleep(2.0)  # ждём регистрации позиции
        try:
            sl_p = {'category':'linear','symbol':sym,'positionIdx':position_idx,
                    'stopLoss':str(round(sl,8)),'slTriggerBy':'LastPrice','tpslMode':'Full'}
            if tp: sl_p.update({'takeProfit':str(round(tp,8)),'tpTriggerBy':'LastPrice'})
            await exchange.private_post_v5_position_trading_stop(sl_p)
            logging.info(f"✅ {sym}: SL={sl:.6f} (trading_stop)")
        except Exception as _sle:
            logging.warning(f"⚠️ trading_stop: {_sle} — STOP_MARKET fallback")
            try:
                sl_side2 = 'sell' if mode=='Long' else 'buy'
                await exchange.create_order(sym,'STOP_MARKET',sl_side2,qty,params={
                    'category':'linear','positionIdx':position_idx,
                    'triggerPrice':round(sl,8),'triggerBy':'LastPrice','reduceOnly':True})
                logging.info(f"✅ {sym}: SL via STOP_MARKET")
            except Exception as _sle2:
                logging.error(f"❌ {sym}: SL не установлен! {_sle2}")
                await tg(f"🚨 <b>{sym}</b>: ПОЗИЦИЯ БЕЗ SL! Закройте вручную!")

        rec = {
            'symbol':       sym,
            'direction':    mode,
            'entry_price':  entry,
            'qty':          qty,
            'sl_price':     sl,
            'tp_price':     tp,
            'order_id':     entry_ord.get('id', ''),
            'strategy':     strategy,
            'open_time':    time.time(),
            'be_moved':     False,
        }
        active_positions.append(rec)

        await tg(
            f"{'🟢' if mode=='Long' else '🔴'} <b>[{strategy}] {sym}</b> — {mode}\n"
            f"Вход: <code>{entry:.6f}</code>\n"
            f"SL: <code>{sl:.6f}</code> ({sl_pct:.2f}%)\n"
            f"TP: <code>{tp:.6f}</code>\n"
            f"Риск: <b>${risk_amount:.2f}</b> ({RISK_PER_TRADE*100:.1f}% × ${free_usdt:.0f})\n"
            f"Notional: ${notional:.2f} | Qty: {qty}\n"
            f"📋 Сигнал от BingX"
        )
        logging.info(f"✅ [{strategy}] {sym} {mode} ОТКРЫТ на Bybit | Risk:${risk_amount:.2f}")

    except Exception as e:
        err_str = str(e)
        logging.error(
            f"❌ Order error [{strategy}] {sym} {mode}: {err_str}\n"
            f"   qty={qty} entry={entry:.6f} sl={sl:.6f}\n"
            f"   Bybit codes: 10001=API key, 110007=balance, "
            f"110013=min lot, 110055=symbol not found ({sym})"
        )
        await tg(
            f"❌ <b>Ошибка {sym}</b>\n"
            f"<code>{err_str[:200]}</code>\n"
            f"qty={qty} notional=${notional:.2f}"
        )

# ══════════════════════════════════════════════════════════
#  МОНИТОРИНГ ПОЗИЦИЙ
# ══════════════════════════════════════════════════════════
async def monitor():
    """Мониторинг: BE при +1.5%, обнаружение закрытий."""
    global active_positions, daily_pnl_pct

    if not active_positions:
        return

    syms = list({p['symbol'] for p in active_positions})
    try:
        # Запрашиваем ВСЕ позиции без фильтра по символу —
        # Bybit иногда не находит по конкретному символу из-за формата
        pos_raw = await exchange.fetch_positions(
            params={'category': 'linear', 'settleCoin': 'USDT'}
        )
        logging.debug(f"[MONITOR] fetch_positions: {len(pos_raw)} позиций, ищем: {syms}")

        # fetch_ticker (singular) надёжнее для Bybit — не зависит от формата символа
        tickers = {}
        for sym_t in syms:
            try:
                t = await exchange.fetch_ticker(sym_t)
                tickers[sym_t] = t
            except Exception:
                # Пробуем через unrealized PnL из позиции
                tickers[sym_t] = {}
    except Exception as e:
        logging.error(f"Monitor fetch error: {e}")
        return

    new_positions = []
    for pos in active_positions:
        sym     = pos['symbol']
        is_long = pos['direction'] == 'Long'
        entry   = float(pos['entry_price'])

        ticker_d = tickers.get(sym) or {}
        curr_p = float(ticker_d.get('last', 0) or 0)
        if curr_p <= 0:
            curr_p = float(ticker_d.get('close', 0) or 0)
        # Fallback: берём markPrice из самой позиции (если есть)
        if curr_p <= 0 and live:
            info = live.get('info') or {}
            curr_p = float(info.get('markPrice', 0) or
                           info.get('unrealisedPnl', 0) or 0)
            # unrealisedPnl не цена — вычислим из него если есть qty
            if curr_p != 0 and 'markPrice' not in info:
                curr_p = 0  # не подходит
            if 'markPrice' in info:
                curr_p = float(info.get('markPrice', 0) or 0)
        if curr_p <= 0:
            curr_p = entry
            logging.debug(f"⚠️ {sym}: curr_p=entry (ticker недоступен)")

        bybit_side = 'Buy' if is_long else 'Sell'
        # Нормализуем символ: Bybit может хранить как 'STORJUSDT' или 'STORJ/USDT'
        sym_base = sym.replace('USDT', '').replace('/', '')
        live = next(
            (r for r in pos_raw
             if (r.get('symbol', '').replace('/', '').replace(':USDT','') == sym
                 or r.get('symbol', '') == sym)
             and (abs(float(r.get('contracts', 0))) > 0
                  or abs(float((r.get('info') or {}).get('size', 0))) > 0)  # info.size fallback
             and r.get('side', bybit_side) == bybit_side),
            None
        ) or next(
            (r for r in pos_raw
             if (r.get('symbol', '').replace('/', '').replace(':USDT','') == sym
                 or r.get('symbol', '') == sym)
             and (abs(float(r.get('contracts', 0))) > 0
                  or abs(float((r.get('info') or {}).get('size', 0))) > 0)),
            None
        )
        if live:
            logging.debug(f"[MONITOR] {sym}: live ✅ contracts={live.get('contracts')}")
        else:
            live_syms = [r.get('symbol') for r in pos_raw if abs(float(r.get('contracts',0)))>0]
            logging.info(f"[MONITOR] {sym}: НЕ найдена | Bybit видит: {live_syms}")

        if live:
            real_qty = abs(float(live.get('contracts', 0)))
            pnl = ((curr_p - entry) / entry * 100 if is_long
                   else (entry - curr_p) / entry * 100)

            secs = time.time() - pos['open_time']

            # ── Таймаут живой позиции (синхронизация с BingX) ──
            _mx = MAX_TRADE_MIN_SMC if pos.get('strategy')=='SMC' else MAX_TRADE_MIN_RSI
            if secs > _mx * 60:
                logging.warning(
                    f'⏰ [{pos.get("strategy","?")}] {sym}: '
                    f'таймаут {secs/60:.0f}мин/{_mx}мин '
                    f'pnl={pnl:+.2f}% — принудительное закрытие'
                )
                try:
                    order_s = 'sell' if is_long else 'buy'
                    await exchange.create_order(
                        sym, 'market', order_s, real_qty,
                        params={'category':'linear','positionIdx':0,'reduceOnly':True}
                    )
                    await tg(
                        f'⏰ <b>[{pos.get("strategy","?")}] {sym}</b>: '
                        f'таймаут {secs/60:.0f}мин\n'
                        f'Закрыта | PnL: {pnl:+.2f}%'
                    )
                except Exception as _te:
                    logging.error(f'⏰ {sym} timeout-close: {_te}')
                    await tg(f'❌ <b>{sym}</b>: ошибка таймаута! Закройте вручную.')
                continue  # удалить из списка

            # Ghost position guard
            if real_qty * curr_p < 1.0 and real_qty > 0:
                logging.warning(f"👻 {sym}: ghost position ${real_qty*curr_p:.4f} — force close")
                try:
                    await exchange.create_order(
                        sym, 'market', 'sell' if is_long else 'buy', real_qty,
                        params={'category': 'linear', 'positionIdx': 0, 'reduceOnly': True}
                    )
                except Exception as ge:
                    logging.error(f"Ghost close error: {ge}")
                continue

            # Breakeven при +1.5% (покрывает комиссию Bybit 0.06% × 2 = +0.12%)
            if pnl >= 1.5 and not pos.get('be_moved'):
                new_sl = entry * 1.002 if is_long else entry * 0.998
                logging.info(f"🛡 {sym}: SL → BE {new_sl:.6f} (P&L +{pnl:.2f}%)")
                try:
                    await exchange.private_post_v5_position_trading_stop({
                        'category':'linear','symbol':sym,'positionIdx':0,
                        'stopLoss':str(round(new_sl,8)),
                        'slTriggerBy':'LastPrice','tpslMode':'Full'})
                    pos['sl_price'] = new_sl
                    pos['be_moved'] = True
                    await tg(f"🛡 <b>{sym}</b>: SL → БУ <code>{new_sl:.6f}</code> | +{pnl:.2f}%")
                except Exception as e:
                    logging.error(f"BE error {sym}: {e}")

            new_positions.append(pos)

        else:
            # Позиция закрыта
            secs = time.time() - pos['open_time']
            if secs < 180:
                logging.info(f"⏳ {sym}: grace {secs:.0f}с/180с — ждём")
                new_positions.append(pos)
                continue

            # Если не найдена И таймаут → принудительно закрыть
            _mx2 = MAX_TRADE_MIN_SMC if pos.get('strategy')=='SMC' else MAX_TRADE_MIN_RSI
            if secs > _mx2 * 60:
                logging.warning(f'⏰ {sym}: не найдена + таймаут {secs/60:.0f}мин/{_mx2}мин')
                try:
                    order_st = 'sell' if pos.get('direction')=='Long' else 'buy'
                    qty_st = float(pos.get('qty', pos.get('initial_qty', 0)))
                    if qty_st > 0:
                        await exchange.create_order(
                            sym, 'market', order_st, qty_st,
                            params={'category':'linear','positionIdx':0,'reduceOnly':True}
                        )
                    await tg(f'⏰ <b>{sym}</b>: таймаут, позиция закрыта принудительно')
                except Exception as _nte:
                    logging.error(f'⏰ not-live timeout {sym}: {_nte}')
                    await tg(f'❌ <b>{sym}</b>: не найдена + таймаут! Проверьте Bybit.')
                continue

            # Пробуем ещё раз получить реальную цену выхода через trades
            exit_p = curr_p  # по умолчанию текущая цена
            try:
                open_ts = int(pos['open_time'] * 1000)
                trades = await exchange.fetch_my_trades(
                    sym, since=open_ts, limit=5,
                    params={'category': 'linear'}
                )
                # Берём последнюю закрывающую сделку
                close_trades = [t for t in trades if t.get('timestamp', 0) >= open_ts]
                if len(close_trades) >= 2:  # вход + выход
                    exit_p = float(close_trades[-1]['price'])
                    logging.info(f"📋 {sym}: exit price из trades = {exit_p}")
            except Exception as te:
                logging.debug(f"fetch_my_trades {sym}: {te}")

            pnl_pct = ((exit_p - entry) / entry * 100 if is_long
                       else (entry - exit_p) / entry * 100)
            is_win  = pnl_pct > 0.5  # реальная победа > 0.5%
            daily_pnl_pct += pnl_pct / 100
            icon = '✅' if is_win else ('⚖️' if pnl_pct > 0 else '🛑')

            # PnL в USDT (notional * leverage * pnl%)
            pos_notional = float(pos.get('qty', 0)) * entry
            pnl_usdt_pos = pos_notional * LEVERAGE * pnl_pct / 100 if pos_notional > 0 else 0
            daily_pnl_usdt += pnl_usdt_pos
            daily_trades   += 1
            if pnl_pct > 0.1: daily_wins += 1

            logging.info(
                f"{icon} {sym}: закрыта | entry={entry:.6f} exit={exit_p:.6f} "
                f"pnl={pnl_pct:+.2f}% ({pnl_usdt_pos:+.2f}$) | {int(secs/60)}мин"
            )
            await tg(
                f"{icon} <b>[{pos['strategy']}] {sym}</b> закрыта\n"
                f"PnL: <code>{pnl_pct:+.2f}%</code> "
                f"(<code>{pnl_usdt_pos:+.2f} USDT</code>) | ⏱ {int(secs/60)}мин\n"
                f"Вход: {entry:.6f} | Выход: {exit_p:.6f}\n"
                f"День: {daily_pnl_pct*100:+.2f}% ({daily_pnl_usdt:+.2f} USDT)"
            )

    active_positions[:] = new_positions

# ══════════════════════════════════════════════════════════
#  HTTP WEBHOOK SERVER
# ══════════════════════════════════════════════════════════
class WebhookHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        body = (
            f"Bybit Worker v2.0 UTA | "
            f"Positions: {len(active_positions)} | "
            f"DD: {daily_pnl_pct*100:+.2f}% | "
            f"Circuit: {'OPEN' if circuit_open else 'OK'}"
        ).encode()
        self.send_response(200)
        self.send_header('Content-Type', 'text/plain')
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        if self.path != '/signal':
            self.send_response(404)
            self.end_headers()
            return
        try:
            length = int(self.headers.get('Content-Length', 0))
            data   = json.loads(self.rfile.read(length))
        except Exception:
            self.send_response(400)
            self.end_headers()
            self.wfile.write(b'Bad JSON')
            return

        if data.get('secret') != WORKER_SECRET:
            logging.warning(f"⚠️ Неверный WORKER_SECRET в запросе!")
            self.send_response(403)
            self.end_headers()
            self.wfile.write(b'Forbidden: wrong secret')
            return

        if _signal_queue is not None:
            _signal_queue.put_nowait(data)

        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps({'status': 'queued'}).encode())
        logging.info(
            f"📥 Сигнал принят: {data.get('symbol')} {data.get('direction')} "
            f"@ {data.get('entry')}"
        )

    def log_message(self, *args):
        return


def run_http():
    port = int(os.environ.get('PORT', 10001))
    HTTPServer(('0.0.0.0', port), WebhookHandler).serve_forever()

# ══════════════════════════════════════════════════════════
#  ГЛАВНЫЙ ЦИКЛ
# ══════════════════════════════════════════════════════════
async def main():
    global http_session, _signal_queue

    _signal_queue = asyncio.Queue()
    http_session  = aiohttp.ClientSession()
    Thread(target=run_http, daemon=True).start()

    # Диагностика при старте
    logging.info("=" * 55)
    logging.info(f"🔑 BYBIT_API_KEY:  {'✅ задан' if BYBIT_KEY else '❌ НЕ ЗАДАН'}")
    logging.info(f"🔑 BYBIT_SECRET:   {'✅ задан' if BYBIT_SECRET else '❌ НЕ ЗАДАН'}")
    logging.info(f"🔑 WORKER_SECRET:  {WORKER_SECRET[:8]}...")
    logging.info(f"🔑 TELEGRAM:       {'✅' if TOKEN else '❌'} | CHAT: {CHAT_ID}")
    logging.info("=" * 55)
    logging.info("🚀 Bybit Worker v2.0 UTA запущен")
    logging.info(f"   Депозит: ${PROP_BALANCE:,.0f} | Риск: {RISK_PER_TRADE*100:.1f}%")
    logging.info(f"   Leverage: {LEVERAGE}x | Max позиций: {MAX_POSITIONS}")
    logging.info(f"   Таймаут: SMC={MAX_TRADE_MIN_SMC}мин RSI={MAX_TRADE_MIN_RSI}мин")
    global day_start_bal
    try:
        _bal = await exchange.fetch_balance({'type': 'unified'})
        day_start_bal = float(_bal.get('USDT', {}).get('total', 0))
    except: pass

    await tg(
        f"🟢 <b>Bybit Worker v2.0 UTA</b> запущен\n"
        f"Депозит: ${PROP_BALANCE:,.0f} | Риск: {RISK_PER_TRADE*100:.1f}%/сделку\n"
        f"Leverage: {LEVERAGE}x | DD-лимит: {DAILY_DD_LIMIT*100:.1f}%\n"
        f"Ожидаю сигналы от BingX..."
    )

    # Проверка подключения к бирже
    try:
        bal = await exchange.fetch_balance({'type': 'unified'})
        usdt = float(bal.get('USDT', {}).get('total', 0))
        logging.info(f"💰 Bybit UTA баланс: {usdt:.2f} USDT")
        await tg(f"💰 Bybit баланс: <b>{usdt:.2f} USDT</b>")
    except Exception as e:
        logging.error(f"❌ Ошибка подключения к Bybit: {e}")
        await tg(f"❌ <b>Ошибка подключения к Bybit:</b>\n<code>{str(e)[:200]}</code>")

    try:
        cycle = 0
        while True:
            cycle += 1
            while not _signal_queue.empty():
                signal = await _signal_queue.get()
                await execute_signal(signal)

            if cycle % 4 == 0 and active_positions:
                await monitor()

            # Итоги дня в 22:00 Киев (19:00 UTC)
            now_utc = datetime.now(timezone.utc)
            if now_utc.hour == 19 and now_utc.minute < 1 and not _daily_report_sent:
                _daily_report_sent = True
                wr = daily_wins / daily_trades * 100 if daily_trades > 0 else 0
                await tg(
                    f"📊 <b>Bybit Worker — Итоги дня</b> "
                    f"{now_utc.strftime('%Y-%m-%d')}\n"
                    f"Сделок: {daily_trades} | WR: {wr:.1f}% "
                    f"({daily_wins}/{daily_trades})\n"
                    f"PnL: <code>{daily_pnl_pct*100:+.2f}%</code> | "
                    f"<code>{daily_pnl_usdt:+.2f} USDT</code>\n"
                    f"Баланс: проверьте на Bybit"
                )
                logging.info("📊 Итоги дня Worker отправлены")

            await asyncio.sleep(15)
    finally:
        if http_session:
            await http_session.close()
        await exchange.close()


if __name__ == '__main__':
    asyncio.run(main())
