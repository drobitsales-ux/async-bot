"""research_venues.py — публичные данные Gate.io, MEXC и Bitget БЕЗ ccxt (BOT_SPEC §2.59.2): прямые REST-запросы aiohttp, как OKX (research_okx, §2.51).
Интерфейс повторяет нужное подмножество ccxt (load_markets / fetch_ohlcv / fetch_funding_rate_history / close), поэтому research_xdata.Fetcher, fetch_bars_back
и fetch_funding_ex работают без изменений. Только публичные GET; ордеров и ключей нет.
НЕ ПРОВЕРЕНО на живом API (сети в среде разработки нет): адреса и поля взяты из документации бирж ПО ПАМЯТИ; ошибки разбора поднимаются с текстом ответа,
первая загрузка на проде это проверит. Свечи — страницами НАЗАД от endTime (как fetch_bars_back): окно времени [endTime − N·шаг, endTime]."""
import asyncio
import re
import time

DAY = 86_400_000
HOUR = 3_600_000
STEP = {'1d': DAY, '1h': HOUR}
PAGE_BARS = 200                         # баров в окне одной страницы (Bitget: максимум 200; Gate/MEXC допускают до 2000 — берём общее)
MAX_FUND_PAGES = 400


class VenueError(Exception):
    pass


DEFECT_RE = re.compile(r'HTTP (400|422)\b|\b400\d\d\b|Parameter verification failed|INVALID_PARAM|invalid param|param(eter)? (error|invalid)', re.I)


def meta_exclusion(info):
    """[v135, BOT_SPEC §2.65.2] Пункт а — поле метаданных контракта указывает на не-крипто (по выводу /contract_probe 2026-11-26): Gate.io `contract_type` ≠ '' (stocks/indices/metals/forex/commodities);
    Bitget `isRwa` = YES; MEXC `type` = 2 либо зона `conceptPlate` с `stock` / `mc-trade-zone-tradfi`. → 'поле <ключ>=<значение>' | None. (OKX `instCategory` и NC-префикс — §2.52, research_xdata.)"""
    if not isinstance(info, dict):
        return None
    ct = info.get('contract_type')
    if ct not in (None, ''):
        return f'поле contract_type={ct}'
    if str(info.get('isRwa') or '').strip().upper() == 'YES':
        return 'поле isRwa=YES'
    if 'conceptPlate' in info:
        if str(info.get('type')).strip() == '2':
            return 'поле type=2'
        zones = info.get('conceptPlate')
        for z in (zones if isinstance(zones, list) else []):
            zl = str(z).lower()
            if 'stock' in zl or zl == 'mc-trade-zone-tradfi':
                return f'поле conceptPlate={z}'
    return None


def is_request_defect(e):
    """[v131, BOT_SPEC §2.61.2] Ответ биржи отклонил ПАРАМЕТРЫ запроса (HTTP 400/422, коды валидации: Bitget 40017/400xx, Gate INVALID_PARAM*): это дефект запроса, а не «нет данных»."""
    return isinstance(e, VenueError) and bool(DEFECT_RE.search(str(e)))


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


    async def fetch_spot_coins(self):
        """[v134, §2.64.2 г] Коды спотовых монет Gate.io с хотя бы одной блокчейн-сетью (`GET /api/v4/spot/currencies`, поле `chains`; формат по памяти, не проверен). Пустой результат — ошибка."""
        data = await self._get('/spot/currencies')
        if not isinstance(data, list):
            raise VenueError(f'gateio spot/currencies: неожиданный ответ {str(data)[:120]}')
        out = {str(d.get('currency') or '').upper() for d in data if (d.get('chains') or d.get('chain')) and d.get('currency')}
        if not out:
            raise VenueError('gateio spot/currencies: список монет с сетями пуст')
        return out


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
    """Bitget USDT-M: /api/v2/mix/market/*. Символ `BTCUSDT`, productType=USDT-FUTURES; строка свечи [ts, o, h, l, c, baseVol, quoteVol]; фандинг — `history-fund-rate`.
    v131 (BOT_SPEC §2.61.1): запрос свечей v130 (`history-candles`, окно 200 суток, startTime+endTime) на проде отклонён кодом 40017 «Parameter verification failed startTime || endTime» для всех символов.
    Документация и API Bitget из среды разработки недоступны — допустимое окно НЕ проверено, поэтому клиент перебирает ФИКСИРОВАННЫЙ список форм (CANDLE_FORMS), берёт первую успешную и запоминает её;
    если отклонены все — ошибка с текстом ответа (без перебора по символам). Постоянная форма фиксируется по выводу `/venue_probe` на проде."""
    name = 'bitget'
    BASE = 'https://api.bitget.com'
    GRAN = {'1d': '1D', '1h': '1H'}
    PT = 'USDT-FUTURES'
    # (endpoint, баров в окне/limit, режим): window — startTime+endTime; endonly — только endTime; latest — без времени
    CANDLE_FORMS = (('history-candles', 200, 'window'), ('history-candles', 90, 'window'), ('history-candles', 30, 'window'), ('history-candles', 200, 'endonly'),
                    ('candles', 90, 'window'), ('candles', 1000, 'latest'))
    FUND_FORMS = ((100, True), (20, True), (None, False))                  # (pageSize, пагинация pageNo)

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self._ids = {}
        self._cform = None                                                  # индекс успешной формы свечей
        self._cfail = None                                                  # все формы отклонены (VenueError)
        self._fform = None
        self._ffail = None

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

    async def fetch_spot_coins(self):
        """[v134, §2.64.2 г] Коды спотовых монет Bitget с хотя бы одной сетью (`GET /api/v2/spot/public/coins`, поле `chains`; формат по памяти, не проверен). Пустой результат — ошибка."""
        data = self._data(await self._get('/api/v2/spot/public/coins'), 'spot/public/coins')
        out = {str(d.get('coin') or '').upper() for d in (data or []) if d.get('chains') and d.get('coin')}
        if not out:
            raise VenueError('bitget spot/public/coins: список монет с сетями пуст')
        return out

    def candle_params(self, form, sym, tf, end):
        ep, bars, mode = form
        p = {'symbol': self._inst(sym), 'productType': self.PT, 'granularity': self.GRAN[tf], 'limit': bars}
        if mode == 'window':
            p['startTime'], p['endTime'] = int(end - bars * STEP[tf]), int(end)
        elif mode == 'endonly':
            p['endTime'] = int(end)
        return ep, p

    async def fetch_ohlcv(self, sym, tf, since=None, limit=1000, params=None):
        if self._cfail is not None:
            raise self._cfail
        end = (params or {}).get('endTime')
        end = int(end) if end is not None else self.now()
        order = [self._cform] if self._cform is not None else list(range(len(self.CANDLE_FORMS)))
        last = None
        for fi in order:
            ep, p = self.candle_params(self.CANDLE_FORMS[fi], sym, tf, end)
            if since and 'startTime' in p:
                p['startTime'] = max(int(since), p['startTime'])
            try:
                data = self._data(await self._get(f'/api/v2/mix/market/{ep}', p), ep)
            except VenueError as e:
                if is_request_defect(e):
                    last = e
                    continue
                raise
            self._cform = fi
            rows = []
            for r in data or []:
                ts = _i(r[0])
                if ts is not None:
                    rows.append([ts, _f(r[1]), _f(r[2]), _f(r[3]), _f(r[4]), (_f(r[6]) if len(r) > 6 else None)])
            rows.sort(key=lambda x: x[0])
            rows = [r for r in rows if r[0] <= end]
            return rows[-int(limit):]
        err = VenueError(f'bitget: запрос свечей отклонён ({len(order)} форм): {last}')
        if self._cform is None:
            self._cfail = err
        raise err

    async def _fund_pages(self, sym, form):
        size, paged = form
        rows, page = {}, 1
        for _ in range(MAX_FUND_PAGES):
            p = {'symbol': self._inst(sym), 'productType': self.PT}
            if size:
                p['pageSize'] = size
            if paged:
                p['pageNo'] = page
            data = self._data(await self._get('/api/v2/mix/market/history-fund-rate', p), 'history-fund-rate')
            if not data:
                break
            new = 0
            for x in data:
                t, r = _i(x.get('fundingTime')), _f(x.get('fundingRate'))
                if t is not None and r is not None and t not in rows:
                    rows[t] = r
                    new += 1
            if not paged or new == 0 or len(data) < (size or 100):
                break
            page += 1
            await self.sleep(self.pause)
        return rows

    async def fetch_funding_rate_history(self, sym, since=None, limit=None, params=None):
        if sym not in self._fund:
            if self._ffail is not None:
                raise self._ffail
            order = [self._fform] if self._fform is not None else list(range(len(self.FUND_FORMS)))
            last, rows = None, None
            for fi in order:
                try:
                    rows = await self._fund_pages(sym, self.FUND_FORMS[fi])
                except VenueError as e:
                    if is_request_defect(e):
                        last = e
                        continue
                    raise
                self._fform = fi
                break
            if rows is None:
                err = VenueError(f'bitget: запрос истории фандинга отклонён ({len(order)} форм): {last}')
                if self._fform is None:
                    self._ffail = err
                raise err
            self._fund[sym] = sorted(rows.items(), key=lambda x: -x[0])
        return self._fund_slice(sym, int(limit or 1000), (params or {}).get('endTime'))

    def form_info(self):
        """[v132] Какая форма запроса принята на прогоне (для отчёта загрузки): → текст | None."""
        if self._cform is None and self._fform is None:
            return None
        c = self.CANDLE_FORMS[self._cform] if self._cform is not None else None
        f = self.FUND_FORMS[self._fform] if self._fform is not None else None
        return (f'Bitget: принятая форма запроса — свечи: {c[0]} ({c[2]}, {c[1]} баров)' if c else 'Bitget: свечи — форма не определена') + \
               (f'; фандинг: history-fund-rate (pageSize={f[0]}, пагинация={f[1]})' if f else '; фандинг — форма не определена')

    async def probe_forms(self, sym, tf='1d'):
        """Диагностика для `/venue_probe` (§2.61.1): каждая форма свечей и фандинга на одном символе → строки текста (запрос — публичный, без секретов; HTTP-код, код/сообщение биржи, число строк, первая и последняя дата)."""
        out = []
        end = self.now()
        for fi, form in enumerate(self.CANDLE_FORMS):
            ep, p = self.candle_params(form, sym, tf, end)
            out.append(probe_line(f'свечи[{fi}] {ep} ({form[2]}, {form[1]} баров)', f'{self.BASE}/api/v2/mix/market/{ep}', p, await self._probe(f'/api/v2/mix/market/{ep}', p, lambda d: [_i(r[0]) for r in d or []])))
        for fi, (size, paged) in enumerate(self.FUND_FORMS):
            p = {'symbol': self._inst(sym), 'productType': self.PT}
            if size:
                p['pageSize'] = size
            if paged:
                p['pageNo'] = 1
            out.append(probe_line(f'фандинг[{fi}] history-fund-rate (pageSize={size}, пагинация={paged})', f'{self.BASE}/api/v2/mix/market/history-fund-rate', p,
                                  await self._probe('/api/v2/mix/market/history-fund-rate', p, lambda d: [_i(x.get('fundingTime')) for x in d or []])))
        return out

    async def _probe(self, path, params, tsf):
        try:
            data = self._data(await self._get(path, params), path.rsplit('/', 1)[-1])
            ts = [t for t in tsf(data) if t is not None]
            return {'http': 'HTTP 200', 'msg': 'успех (code 00000)', 'n': len(ts), 'first': min(ts) if ts else None, 'last': max(ts) if ts else None}
        except VenueError as e:
            m = re.search(r'HTTP (\d+)', str(e))
            return {'http': f'HTTP {m.group(1)}' if m else 'HTTP 200', 'msg': str(e)[:200], 'n': 0, 'first': None, 'last': None}


def probe_line(title, url, params, r):
    d = lambda t: time.strftime('%Y-%m-%d', time.gmtime(t / 1000)) if t else 'нет'
    q = '&'.join(f'{k}={v}' for k, v in params.items())
    return f'{title}: GET {url}?{q} → {r["http"]}; {r["msg"]}; строк {r["n"]}; первая {d(r["first"])}, последняя {d(r["last"])}'


async def probe_venue(name, base='BTC', clients=None, now=None):
    """`/venue_probe [биржа] [БАЗА]` (§2.61.1): проверка запросов биржи прямого REST на одном символе; только публичные GET. → текст."""
    name = (name or 'bitget').lower()
    if name not in CLASSES:
        return f'⛔ /venue_probe: биржа «{name}» не из списка {", ".join(CLASSES)}'
    c = (clients or {}).get(name) or CLASSES[name](**({'now': now} if now else {}))
    L = [f'🔎 <b>/venue_probe {name} {base}</b> (BOT_SPEC §2.61.1; публичные запросы без ключей)']
    try:
        mk = await c.load_markets()
        sym = f'{base.upper()}/USDT:USDT'
        L.append(f'контрактов USDT-перпетуалов: {len(mk)}; {sym}: ' + ('есть' if sym in mk else 'НЕТ в списке'))
        if hasattr(c, 'probe_forms'):
            L += await c.probe_forms(sym)
        else:
            d = lambda t: time.strftime('%Y-%m-%d', time.gmtime(t / 1000)) if t else 'нет'
            try:
                rows = await c.fetch_ohlcv(sym, '1d', None, 1000, {'endTime': c.now()})
                L.append(f'свечи 1d: HTTP 200; строк {len(rows)}; первая {d(rows[0][0] if rows else None)}, последняя {d(rows[-1][0] if rows else None)}')
            except Exception as e:
                L.append(f'свечи 1d: ОШИБКА {str(e)[:200]}')
            try:
                fr = await c.fetch_funding_rate_history(sym, None, 1000, {})
                L.append((f'фандинг: строк {len(fr)}; глубина ≈ {(fr[0]["timestamp"] - fr[-1]["timestamp"]) / DAY:.1f} сут.; первая {d(fr[-1]["timestamp"])}, последняя {d(fr[0]["timestamp"])}') if fr else 'фандинг: пустой ответ')
            except Exception as e:
                L.append(f'фандинг: ОШИБКА {str(e)[:200]}')
    except Exception as e:
        L.append(f'ОШИБКА: {type(e).__name__}: {str(e)[:200]}')
    finally:
        try:
            await c.close()
        except Exception:
            pass
    return '\n'.join(L)


CLASSES = {'gateio': GatePublic, 'mexc': MexcPublic, 'bitget': BitgetPublic}
NEW_VENUES = tuple(CLASSES)
