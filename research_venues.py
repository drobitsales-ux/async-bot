"""research_venues.py — публичные данные Gate.io, MEXC и Bitget БЕЗ ccxt (BOT_SPEC §2.59.2): прямые REST-запросы aiohttp, как OKX (research_okx, §2.51).
Интерфейс повторяет нужное подмножество ccxt (load_markets / fetch_ohlcv / fetch_funding_rate_history / close), поэтому research_xdata.Fetcher, fetch_bars_back
и fetch_funding_ex работают без изменений. Только публичные GET; ордеров и ключей нет.
НЕ ПРОВЕРЕНО на живом API (сети в среде разработки нет): адреса и поля взяты из документации бирж ПО ПАМЯТИ; ошибки разбора поднимаются с текстом ответа,
первая загрузка на проде это проверит. Свечи — страницами НАЗАД от endTime (как fetch_bars_back): окно времени [endTime − N·шаг, endTime]."""
import asyncio
import time

DAY = 86_400_000
HOUR = 3_600_000
STEP = {'1d': DAY, '1h': HOUR}
PAGE_BARS = 200                         # баров в окне одной страницы (Bitget: максимум 200; Gate/MEXC допускают до 2000 — берём общее)
MAX_FUND_PAGES = 400


class VenueError(Exception):
    pass


def _f(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _i(x):
    try:
        return int(float(x))
    except (TypeError, ValueError):
        return None


class _Rest:
    """Общая часть: get(url, params) → JSON (подменяется в тестах); коды HTTP превращаются в VenueError с телом ответа (для классификации ошибок)."""
    name = 'venue'
    BASE = ''

    def __init__(self, get=None, sleep=asyncio.sleep, pause=None, timeout=20.0, now=None):
        self._get_fn, self.sleep, self.timeout = get, sleep, timeout
        self.now = now or (lambda: int(time.time() * 1000))
        if pause is None:
            import research_htf
            pause = research_htf.fetch_pause()
        self.pause = pause
        self._sess = None
        self._fund = {}                  # sym → список (ts, rate) по убыванию времени (загружается один раз за символ)

    async def _get(self, path, params=None):
        if self._get_fn is not None:
            return await self._get_fn(path, params or {})
        import aiohttp
        if self._sess is None:
            self._sess = aiohttp.ClientSession()
        async with self._sess.get(self.BASE + path, params=params, timeout=aiohttp.ClientTimeout(total=self.timeout)) as r:
            body = await r.text()
            if r.status == 429:
                raise VenueError(f'{self.name} HTTP 429 too many requests: {body[:100]}')
            if r.status != 200:
                raise VenueError(f'{self.name} HTTP {r.status}: {body[:160]}')
            import json
            try:
                return json.loads(body)
            except ValueError:
                raise VenueError(f'{self.name} не JSON: {body[:120]}')

    async def close(self):
        if self._sess is not None:
            await self._sess.close()
            self._sess = None

    @staticmethod
    def _mk(base, sym_id, active, created, info):
        return {'swap': True, 'linear': True, 'quote': 'USDT', 'settle': 'USDT', 'base': base, 'active': bool(active), 'created': created, 'id': sym_id, 'info': info}

    def _ccxt_sym(self, sid, base):
        return f'{base}/USDT:USDT'

    def _fund_slice(self, sym, lim, end):
        rows = self._fund[sym]
        out = [{'timestamp': t, 'fundingRate': r} for t, r in rows if end is None or t <= end]
        return out[:lim]


class GatePublic(_Rest):
    """Gate.io USDT-фьючерсы: /api/v4/futures/usdt/*. Контракт `BTC_USDT`; свечи `candlesticks` (t в секундах; `sum` — оборот в USDT, если нет — v·c·quanto_multiplier);
    фандинг `funding_rate` (t в секундах, r — ставка)."""
    name = 'gateio'
    BASE = 'https://api.gateio.ws/api/v4'
    INTERVAL = {'1d': '1d', '1h': '1h'}

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self._ids, self._mult = {}, {}

    async def load_markets(self):
        out = {}
        data = await self._get('/futures/usdt/contracts')
        if not isinstance(data, list):
            raise VenueError(f'gateio contracts: неожиданный ответ {str(data)[:120]}')
        for d in data:
            nm = str(d.get('name') or '')
            if not nm.endswith('_USDT') or (d.get('type') not in (None, 'direct')):
                continue
            base = nm.split('_')[0]
            sym = self._ccxt_sym(nm, base)
            ct = _f(d.get('create_time'))
            active = (not d.get('in_delisting')) and str(d.get('status', 'trading')) in ('trading', '')
            out[sym] = self._mk(base, nm, active, int(ct * 1000) if ct and ct > 0 else None, d)
            self._ids[sym] = nm
            self._mult[sym] = _f(d.get('quanto_multiplier')) or 1.0
        return out

    def _inst(self, sym):
        return self._ids.get(sym) or sym.split('/')[0] + '_USDT'

    async def fetch_ohlcv(self, sym, tf, since=None, limit=1000, params=None):
        step = STEP[tf]
        end = (params or {}).get('endTime')
        end = int(end) if end is not None else self.now()
        lo = max(int(since or 0), end - PAGE_BARS * step)
        data = await self._get('/futures/usdt/candlesticks', {'contract': self._inst(sym), 'interval': self.INTERVAL[tf], 'from': int(lo // 1000), 'to': int(end // 1000)})
        if not isinstance(data, list):
            raise VenueError(f'gateio candlesticks: неожиданный ответ {str(data)[:120]}')
        mult = self._mult.get(sym, 1.0)
        rows = []
        for r in data:
            ts = _i(r.get('t'))
            c = _f(r.get('c'))
            if ts is None or c is None:
                continue
            qv = _f(r.get('sum'))
            if qv is None:
                qv = (_f(r.get('v')) or 0.0) * mult * c
            rows.append([ts * 1000, _f(r.get('o')), _f(r.get('h')), _f(r.get('l')), c, qv])
        rows.sort(key=lambda x: x[0])
        return rows[-int(limit):]

    async def fetch_funding_rate_history(self, sym, since=None, limit=None, params=None):
        """Одна загрузка за символ (максимум, который отдаёт API); дальше — срез по endTime. Глубина = то, что вернул API (измеряется, не предполагается)."""
        if sym not in self._fund:
            data = await self._get('/futures/usdt/funding_rate', {'contract': self._inst(sym), 'limit': 1000})
            if not isinstance(data, list):
                raise VenueError(f'gateio funding_rate: неожиданный ответ {str(data)[:120]}')
            rows = [(_i(d.get('t')) * 1000, _f(d.get('r'))) for d in data if _i(d.get('t')) is not None and _f(d.get('r')) is not None]
            self._fund[sym] = sorted(rows, key=lambda x: -x[0])
        return self._fund_slice(sym, int(limit or 1000), (params or {}).get('endTime'))


class MexcPublic(_Rest):
    """MEXC фьючерсы: contract.mexc.com/api/v1/contract/*. Символ `BTC_USDT`; свечи `kline/{symbol}?interval=Day1&start=&end=` (секунды; колонки time/open/high/low/close/vol/amount);
    фандинг `funding_rate/history?symbol=&page_num=&page_size=` (resultList: fundingRate, settleTime мс)."""
    name = 'mexc'
    BASE = 'https://contract.mexc.com'
    INTERVAL = {'1d': 'Day1', '1h': 'Min60'}

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self._ids = {}

    @staticmethod
    def _data(j, what):
        if not isinstance(j, dict) or not (j.get('success') is True or str(j.get('code')) == '0'):
            raise VenueError(f'mexc {what}: {str(j)[:160]}')
        return j.get('data')

    async def load_markets(self):
        out = {}
        data = self._data(await self._get('/api/v1/contract/detail'), 'detail')
        if isinstance(data, dict):
            data = [data]
        for d in data or []:
            nm = str(d.get('symbol') or '')
            if not nm.endswith('_USDT') or str(d.get('settleCoin', 'USDT')) != 'USDT' or str(d.get('quoteCoin', 'USDT')) != 'USDT':
                continue
            base = str(d.get('baseCoin') or nm.split('_')[0])
            sym = self._ccxt_sym(nm, base)
            ct = _i(d.get('createTime'))
            out[sym] = self._mk(base, nm, _i(d.get('state')) == 0, ct if ct and ct > 0 else None, d)
            self._ids[sym] = nm
        return out

    def _inst(self, sym):
        return self._ids.get(sym) or sym.split('/')[0] + '_USDT'

    async def fetch_ohlcv(self, sym, tf, since=None, limit=1000, params=None):
        step = STEP[tf]
        end = (params or {}).get('endTime')
        end = int(end) if end is not None else self.now()
        lo = max(int(since or 0), end - PAGE_BARS * step)
        d = self._data(await self._get(f'/api/v1/contract/kline/{self._inst(sym)}', {'interval': self.INTERVAL[tf], 'start': int(lo // 1000), 'end': int(end // 1000)}), 'kline')
        if not isinstance(d, dict) or 'time' not in d:
            raise VenueError(f'mexc kline: неожиданный ответ {str(d)[:120]}')
        n = len(d.get('time') or [])
        rows = []
        for k in range(n):
            ts = _i(d['time'][k])
            if ts is None:
                continue
            c = _f(d['close'][k])
            amt = d.get('amount')
            rows.append([ts * 1000, _f(d['open'][k]), _f(d['high'][k]), _f(d['low'][k]), c, (_f(amt[k]) if amt and k < len(amt) else None)])
        rows.sort(key=lambda x: x[0])
        return rows[-int(limit):]

    async def fetch_funding_rate_history(self, sym, since=None, limit=None, params=None):
        if sym not in self._fund:
            rows, page = {}, 1
            for _ in range(MAX_FUND_PAGES):
                d = self._data(await self._get('/api/v1/contract/funding_rate/history', {'symbol': self._inst(sym), 'page_num': page, 'page_size': 100}), 'funding_rate/history')
                lst = (d or {}).get('resultList') if isinstance(d, dict) else None
                if not lst:
                    break
                for x in lst:
                    t, r = _i(x.get('settleTime')), _f(x.get('fundingRate'))
                    if t is not None and r is not None:
                        rows[t] = r
                tp = _i((d or {}).get('totalPage'))
                if (tp is not None and page >= tp) or len(lst) < 100:
                    break
                page += 1
                await self.sleep(self.pause)
            self._fund[sym] = sorted(rows.items(), key=lambda x: -x[0])
        return self._fund_slice(sym, int(limit or 1000), (params or {}).get('endTime'))


class BitgetPublic(_Rest):
    """Bitget USDT-M: /api/v2/mix/market/*. Символ `BTCUSDT`, productType=USDT-FUTURES; свечи `history-candles` (granularity 1D/1H, startTime/endTime мс, limit ≤ 200;
    строка [ts, o, h, l, c, baseVol, quoteVol]); фандинг `history-fund-rate` (pageNo/pageSize, fundingRate, fundingTime мс)."""
    name = 'bitget'
    BASE = 'https://api.bitget.com'
    GRAN = {'1d': '1D', '1h': '1H'}
    PT = 'USDT-FUTURES'

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self._ids = {}

    @staticmethod
    def _data(j, what):
        if not isinstance(j, dict) or str(j.get('code')) != '00000':
            raise VenueError(f'bitget {what}: {str(j)[:160]}')
        return j.get('data')

    async def load_markets(self):
        out = {}
        data = self._data(await self._get('/api/v2/mix/market/contracts', {'productType': self.PT}), 'contracts')
        for d in data or []:
            sid = str(d.get('symbol') or '')
            if not sid.endswith('USDT') or str(d.get('quoteCoin', 'USDT')) != 'USDT' or str(d.get('symbolType', 'perpetual')) != 'perpetual':
                continue
            base = str(d.get('baseCoin') or sid[:-4])
            sym = self._ccxt_sym(sid, base)
            lt = _i(d.get('launchTime'))
            out[sym] = self._mk(base, sid, str(d.get('symbolStatus', 'normal')) == 'normal', lt if lt and lt > 0 else None, d)
            self._ids[sym] = sid
        return out

    def _inst(self, sym):
        return self._ids.get(sym) or sym.split('/')[0] + 'USDT'

    async def fetch_ohlcv(self, sym, tf, since=None, limit=1000, params=None):
        step = STEP[tf]
        end = (params or {}).get('endTime')
        end = int(end) if end is not None else self.now()
        lo = max(int(since or 0), end - PAGE_BARS * step)
        data = self._data(await self._get('/api/v2/mix/market/history-candles', {'symbol': self._inst(sym), 'productType': self.PT, 'granularity': self.GRAN[tf],
                                                                              'startTime': int(lo), 'endTime': int(end), 'limit': PAGE_BARS}), 'history-candles')
        rows = []
        for r in data or []:
            ts = _i(r[0])
            if ts is None:
                continue
            rows.append([ts, _f(r[1]), _f(r[2]), _f(r[3]), _f(r[4]), (_f(r[6]) if len(r) > 6 else None)])
        rows.sort(key=lambda x: x[0])
        return rows[-int(limit):]

    async def fetch_funding_rate_history(self, sym, since=None, limit=None, params=None):
        if sym not in self._fund:
            rows, page = {}, 1
            for _ in range(MAX_FUND_PAGES):
                data = self._data(await self._get('/api/v2/mix/market/history-fund-rate', {'symbol': self._inst(sym), 'productType': self.PT, 'pageSize': 100, 'pageNo': page}), 'history-fund-rate')
                if not data:
                    break
                new = 0
                for x in data:
                    t, r = _i(x.get('fundingTime')), _f(x.get('fundingRate'))
                    if t is not None and r is not None and t not in rows:
                        rows[t] = r
                        new += 1
                if new == 0 or len(data) < 100:
                    break
                page += 1
                await self.sleep(self.pause)
            self._fund[sym] = sorted(rows.items(), key=lambda x: -x[0])
        return self._fund_slice(sym, int(limit or 1000), (params or {}).get('endTime'))


CLASSES = {'gateio': GatePublic, 'mexc': MexcPublic, 'bitget': BitgetPublic}
NEW_VENUES = tuple(CLASSES)
