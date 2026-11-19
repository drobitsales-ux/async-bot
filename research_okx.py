"""research_okx.py — публичные данные OKX БЕЗ ccxt (BOT_SPEC §2.51): прямые REST-запросы aiohttp. Интерфейс повторяет нужное подмножество ccxt
(load_markets / fetch_ohlcv / fetch_funding_rate_history / close), поэтому research_xdata.Fetcher, fetch_bars и fetch_funding работают без изменений.
Только публичные GET; ордеров и ключей нет. Причина TypeError в ccxt.okx.load_markets не выяснена — ccxt для OKX не используется.
НЕ ПРОВЕРЕНО на живом API (сети нет): формат полей взят из документации OKX v5 по памяти — первая загрузка на проде это проверит."""
import asyncio

BASE = 'https://www.okx.com'
BARS = {'1d': '1D', '1h': '1H'}
CANDLE_LIMIT = 100                      # history-candles: максимум 100 строк на запрос
FUND_LIMIT = 100                        # funding-rate-history: максимум 100
MAX_PAGES = 400


class OkxError(Exception):
    pass


def _f(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


class OkxPublic:
    """get(path, params) → dict(JSON) подменяется в тестах; по умолчанию — aiohttp (общая сессия, закрывается close())."""

    def __init__(self, get=None, sleep=asyncio.sleep, pause=None, timeout=20.0):
        self._get_fn, self.sleep, self.timeout = get, sleep, timeout
        if pause is None:
            import research_htf
            pause = research_htf.fetch_pause()
        self.pause = pause
        self._sess = None
        self._ids = {}
        self._cache = {}                # (sym, tf) → (since0, {ts: row})

    async def _get(self, path, params):
        if self._get_fn is not None:
            j = await self._get_fn(path, params)
        else:
            import aiohttp
            if self._sess is None:
                self._sess = aiohttp.ClientSession()
            async with self._sess.get(BASE + path, params=params, timeout=aiohttp.ClientTimeout(total=self.timeout)) as r:
                if r.status != 200:
                    raise OkxError(f'okx HTTP {r.status}')
                j = await r.json(content_type=None)
        if not isinstance(j, dict) or str(j.get('code')) != '0':
            raise OkxError(f'okx {j.get("code") if isinstance(j, dict) else "?"} {str(j.get("msg", ""))[:80] if isinstance(j, dict) else ""}'.strip())
        return j.get('data') or []

    async def load_markets(self):
        """Линейные USDT-перпетуалы: ccxt-подобные markets {BASE/USDT:USDT: {...}}; created = listTime (мс), active = state == 'live'."""
        out = {}
        for d in await self._get('/api/v5/public/instruments', {'instType': 'SWAP'}):
            inst = str(d.get('instId') or '')
            if not inst.endswith('-USDT-SWAP') or str(d.get('ctType')) != 'linear' or str(d.get('settleCcy')) != 'USDT':
                continue
            base = inst.split('-')[0]
            sym = f'{base}/USDT:USDT'
            lt = d.get('listTime')
            out[sym] = {'swap': True, 'linear': True, 'quote': 'USDT', 'settle': 'USDT', 'base': base, 'active': str(d.get('state')) == 'live',
                        'created': int(lt) if str(lt or '').isdigit() else None, 'id': inst}
            self._ids[sym] = inst
        return out

    def _inst(self, sym):
        return self._ids.get(sym) or sym.split('/')[0] + '-USDT-SWAP'

    async def fetch_ohlcv(self, sym, tf, since=None, limit=1000):
        """Свечи с ts ≥ since по возрастанию (до limit). OKX отдаёт history-candles только НАЗАД (after = «старше ts»): страницы собираются от
        свежих к старым до since и кэшируются на символ, поэтому последующие вызовы с большим since идут из кэша."""
        since = int(since or 0)
        key = (sym, tf)
        hit = self._cache.get(key)
        if hit is None or since < hit[0]:
            rows, after = {}, None
            for _ in range(MAX_PAGES):
                p = {'instId': self._inst(sym), 'bar': BARS[tf], 'limit': CANDLE_LIMIT}
                if after is not None:
                    p['after'] = after
                data = await self._get('/api/v5/market/history-candles', p)
                if not data:
                    break
                for r in data:
                    ts = int(r[0])
                    qv = _f(r[7]) if len(r) > 7 else None
                    vol = _f(r[6]) if len(r) > 6 and _f(r[6]) is not None else _f(r[5])
                    rows[ts] = [ts, _f(r[1]), _f(r[2]), _f(r[3]), _f(r[4]), vol if vol is not None else 0.0, qv]
                oldest = min(int(r[0]) for r in data)
                if oldest <= since or (after is not None and oldest >= after):
                    break
                after = oldest
                await self.sleep(self.pause)
            self._cache = {key: (since, rows)}
            hit = self._cache[key]
        rows = hit[1]
        return [rows[t][:6] for t in sorted(rows) if t >= since][:limit]

    async def fetch_funding_rate_history(self, sym, since=None, limit=None, params=None):
        """Страница истории фандинга: записи ≤ params['endTime'] (OKX: after = «старше fundingTime»), новые первыми; ставка — realizedRate
        (фактически начисленная), если пуста — fundingRate. Глубину истории определяет API (измеряется загрузкой, не предполагается)."""
        p = {'instId': self._inst(sym), 'limit': min(int(limit or FUND_LIMIT), FUND_LIMIT)}
        end = (params or {}).get('endTime')
        if end is not None:
            p['after'] = int(end) + 1
        out = []
        for d in await self._get('/api/v5/public/funding-rate-history', p):
            rate = _f(d.get('realizedRate'))
            if rate is None:
                rate = _f(d.get('fundingRate'))
            if rate is not None and str(d.get('fundingTime') or '').isdigit():
                out.append({'timestamp': int(d['fundingTime']), 'fundingRate': rate})
        return out

    async def close(self):
        if self._sess is not None:
            await self._sess.close()
            self._sess = None
