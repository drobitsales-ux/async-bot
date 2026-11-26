"""research_runner.py — общий запускатель исследовательских расчётов в ОТДЕЛЬНОМ процессе (BOT_SPEC §2.39).

Зачем: Render убивает ВЕСЬ контейнер при превышении 512 МБ («Ran out of memory (used over 512MB)»), а /edge_audit накапливал сделки всех символов в памяти
бота (≈850 Б на сделку × 35 тыс. сделок на символ + свечи 16 символов одновременно) — прод 2026-10-24: два рестарта бота. Теперь расчёт идёт в дочернем процессе
с лимитом адресного пространства RESEARCH_MEM_MB (RLIMIT_AS): при превышении падает ТОЛЬКО дочерний процесс (MemoryError), бот продолжает работу и сообщает причину.

Процесс запускается как СВЕЖИЙ интерпретатор (`python research_runner.py job.json`) — по смыслу то же, что multiprocessing со start method 'spawn' (нет fork-копии
памяти бота), но без главного недостатка spawn: spawn заново импортирует __main__ (async_bot.py как __mp_main__), т. е. выполняет весь модуль бота в дочернем процессе
(создание ccxt-экземпляров, инициализация БД, лог-обработчики) и не даёт уложиться в лимит.

Связь с ботом — только через файлы рабочей папки: job.json (задание), payload.pkl (данные, загруженные ботом через его exchange ДО запуска расчёта), progress.jsonl
(прогресс с RSS дочернего процесса), result.json (итог), stderr.log (хвост ошибок). Сетевые загрузки, которым нужен общий exchange бота, остаются в основном процессе
(prepare_* в research_htf/research_ws); загрузки архива data.binance.vision, расчёт и чтение кэша — в дочернем."""
import asyncio
import json
import os
import pickle
import shutil
import signal
import sys
import tempfile
import time
import traceback

DEFAULT_MEM_MB = 300
THREAD_ENV = {'OPENBLAS_NUM_THREADS': '1', 'OMP_NUM_THREADS': '1', 'MKL_NUM_THREADS': '1', 'NUMEXPR_NUM_THREADS': '1', 'VECLIB_MAXIMUM_THREADS': '1', 'MALLOC_ARENA_MAX': '2'}
BLAS_VARS = ('OPENBLAS_NUM_THREADS', 'OMP_NUM_THREADS', 'MKL_NUM_THREADS', 'NUMEXPR_NUM_THREADS', 'VECLIB_MAXIMUM_THREADS')


def child_env(mem_mb: float = None) -> dict:
    """Окружение дочернего процесса: потоки BLAS = 1 ПЕРЕД запуском интерпретатора (значения сервиса перекрываются; после import numpy менять поздно), лимит RESEARCH_MEM_MB."""
    return dict(os.environ, **THREAD_ENV, RESEARCH_MEM_MB=str(mem_mb if mem_mb is not None else mem_limit_mb()), BOT_RESEARCH_CHILD='1', PYTHONUNBUFFERED='1')


def blas_threads_text() -> str:
    """Для /status: «потоки BLAS в дочерних процессах: 1» (если все пять переменных равны) либо перечень."""
    e = child_env()
    vals = {k: e.get(k) for k in BLAS_VARS}
    if len(set(vals.values())) == 1:
        return f'потоки BLAS в дочерних процессах: {next(iter(vals.values()))}'
    return 'потоки BLAS в дочерних процессах: ' + ', '.join(f'{k.split("_")[0]} {v}' for k, v in vals.items())
EXIT_MEMORY = 3
EXIT_ERROR = 1
STATE = {'task': None, 'pid': None, 'started': 0.0, 'rss_mb': 0.0, 'last': ''}      # для /status


def mem_limit_mb() -> float:
    try:
        v = float(os.getenv('RESEARCH_MEM_MB', str(DEFAULT_MEM_MB)))
        return v if v > 0 else float(DEFAULT_MEM_MB)
    except ValueError:
        return float(DEFAULT_MEM_MB)


def rss_mb(pid=None) -> float:
    """RSS процесса в МБ (Linux: /proc; иначе максимум из getrusage); 0.0 — не удалось."""
    path = f'/proc/{pid}/status' if pid else '/proc/self/status'
    try:
        with open(path) as f:
            for ln in f:
                if ln.startswith('VmRSS:'):
                    return int(ln.split()[1]) / 1024.0
    except Exception:
        pass
    if pid is None:
        try:
            import resource
            r = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            return r / (1024.0 * 1024.0) if sys.platform == 'darwin' else r / 1024.0
        except Exception:
            pass
    return 0.0


def workdir_base() -> str:
    return os.getenv('RESEARCH_DIR') or ('/data/research_jobs' if os.path.exists('/data') else os.path.join(tempfile.gettempdir(), 'research_jobs'))


# ═══ дочерний процесс ═══════════════════════════════════════════════════════════════════════════════
def _apply_limit(mb: float):
    try:
        import resource
        lim = int(mb * 1024 * 1024)
        resource.setrlimit(resource.RLIMIT_AS, (lim, lim))
    except Exception as e:                       # платформа без resource — продолжаем без лимита (в stderr видно)
        sys.stderr.write(f'[research_runner] RLIMIT_AS не установлен: {type(e).__name__}: {e}\n')


class Ctx:
    """Контекст задачи в дочернем процессе: параметры, путь payload, прогресс-файл."""

    def __init__(self, job: dict):
        self.job = job
        self.params = job.get('params') or {}
        self.dir = job['dir']
        self._ppid = int(job.get('ppid') or 0)
        self._pf = os.path.join(self.dir, 'progress.jsonl')

    def payload(self):
        p = os.path.join(self.dir, 'payload.pkl')
        if not os.path.exists(p):
            return None
        with open(p, 'rb') as f:
            return pickle.load(f)

    def _parent_alive(self):
        return not self._ppid or os.getppid() == self._ppid

    async def progress(self, msg):
        """Строка прогресса + RSS дочернего процесса. Если бот (родитель) исчез — завершаемся: результат всё равно некому доставить."""
        if not self._parent_alive():
            os._exit(4)
        try:
            with open(self._pf, 'a', encoding='utf-8') as f:
                f.write(json.dumps({'t': time.time(), 'msg': str(msg), 'rss': round(rss_mb(), 1)}, ensure_ascii=False) + '\n')
        except Exception:
            pass


async def _task_selftest_mem(ctx: Ctx):
    """Тестовая задача: выделяет память сверх лимита (для tests/)."""
    await ctx.progress('selftest: старт')
    buf = bytearray(int(ctx.params.get('alloc_mb', 500)) * 1024 * 1024)
    await ctx.progress(f'selftest: выделено {len(buf)} байт (лимит не сработал)')
    return {'text': 'selftest ok'}


async def _task_selftest_ok(ctx: Ctx):
    await ctx.progress('selftest: шаг 1')
    await ctx.progress('selftest: шаг 2')
    return {'text': 'selftest ok ' + str(ctx.params.get('x', ''))}


async def _task_selftest_env(ctx: Ctx):
    """Тестовая задача: значения переменных потоков BLAS, как их видит дочерний процесс (до import numpy они уже в окружении)."""
    return {'text': json.dumps({k: os.environ.get(k) for k in BLAS_VARS + ('MALLOC_ARENA_MAX',)})}


async def _task_selftest_blas(ctx: Ctx):
    """Тестовая задача: имитирует падение OpenBLAS (текст как в прод-логе) с кодом возврата 1."""
    sys.stderr.write('OpenBLAS error: Memory allocation still failed after 10 retries, giving up.\n')
    sys.stderr.flush()
    os._exit(1)


async def _task_selftest_kill(ctx: Ctx):
    await ctx.progress('selftest: перед SIGKILL')
    os.kill(os.getpid(), signal.SIGKILL)


async def _task_edge_audit(ctx: Ctx):
    import aiohttp
    import async_bot as ab                      # только функции (_smc2_eval, get_pivots); main() не запускается
    import research_edge
    import research_vision
    p = ctx.params
    async with aiohttp.ClientSession() as ses:
        fetch = research_vision.make_fetcher(ses)
        text, res = await research_edge.run_edge_audit(fetch, p['db_path'], p['universe'], p['fee_maker'], p['fee_taker'], ab._smc2_eval, ab.get_pivots,
                                                       progress=ctx.progress, d=p.get('vision_dir'), now_ms=p.get('now_ms'), edge_db=p.get('edge_db'),
                                                       sync=p.get('sync', True), inline=True)
    return {'text': text, 'res': res}


async def _task_of_backtest(ctx: Ctx):
    import aiohttp
    import research_of
    import research_vision
    p = ctx.params
    async with aiohttp.ClientSession() as ses:
        fetch = research_vision.make_fetcher(ses)
        text, _res = await research_of.run_of(fetch, p['db_path'], p['universe'], p['allowed'], p['reasons'], p['fee'], progress=ctx.progress)
    return {'text': text}


async def _task_htf_backtest(ctx: Ctx):
    import research_htf
    data, info = ctx.payload()
    p = ctx.params
    text, _res, _boot = research_htf.compute(data, info, p['fee'], research_htf.SLIP, p.get('runs', research_htf.BOOT_RUNS))
    return {'text': text}


async def _task_htf_wide(ctx: Ctx):
    import research_htf
    univ, data, info = ctx.payload()
    p = ctx.params
    text, _verd, _boot = research_htf.compute_wide(univ, data, info, p['fee'], research_htf.SLIP, p.get('runs', research_htf.BOOT_RUNS))
    return {'text': text}


async def _task_htf_xsmom(ctx: Ctx):
    import research_htf
    syms, data, info = ctx.payload()
    p = ctx.params
    text, _verd, _boot = research_htf.compute_xs(syms, data, info, p['fee'], research_htf.SLIP, p.get('runs', research_htf.BOOT_RUNS))
    return {'text': text}


async def _task_wide_stop(ctx: Ctx):
    import research_ws
    pl = ctx.payload()
    p = ctx.params
    text, _res = research_ws.finish_wide_stop(pl, p['db_path'], p['fee_maker'], p['fee_taker'])
    return {'text': text}


async def _task_data_probe_ma(ctx: Ctx):
    import research_ma2                      # [v100] проба v2 (качество данных Dukascopy, спреды, стакан BingX, диагностика Stooq)
    return {'text': await research_ma2.run_probe_ma2(ctx.payload(), ctx.params, progress=ctx.progress)}


async def _task_carry_test(ctx: Ctx):
    import research_carry
    return {'text': research_carry.run_carry(ctx.payload(), ctx.params)}


async def _task_data_probe(ctx: Ctx):
    import research_probe

    class _HasOnly:                              # run_probe использует у exchange только флаги .has (без сети)
        has = ctx.params.get('has') or {}
    p = ctx.params
    text, _res, _est, _concl = await research_probe.run_probe(p['db_path'], exchange=_HasOnly(), progress=ctx.progress, version=p.get('version', ''))
    return {'text': text}


async def _task_factory_load(ctx: Ctx):
    import research_factory                      # [v103] загрузка Dukascopy для «фабрики» (BOT_SPEC §2.41.17)
    return {'text': await research_factory.run_load(ctx.params, progress=ctx.progress)}


async def _task_factory_pairs(ctx: Ctx):
    import research_factory
    return {'text': research_factory.run_pairs(ctx.params)}


async def _task_factory(ctx: Ctx):
    import research_factory
    return {'text': await research_factory.run_factory(ctx.params, progress=ctx.progress)}


async def _task_factory_final(ctx: Ctx):
    import research_factory
    return {'text': await research_factory.run_final(ctx.params, progress=ctx.progress)}


async def _task_factory_decomp(ctx: Ctx):
    import research_factory                      # [v105] декомпозиция доходности (шаг 0 поправки §2.41.23), только чтение
    return {'text': await research_factory.run_decomp(ctx.params, progress=ctx.progress)}


async def _task_factory_nc(ctx: Ctx):
    import research_factory                      # [v105] перезапуск не-крипто ячеек (поправка §2.41.23)
    return {'text': await research_factory.run_nc(ctx.params, progress=ctx.progress)}


async def _task_final_crypto4h(ctx: Ctx):
    import research_ffinal                       # [v106] одиночный финал CRYPTO 4h T-MA-10-50 (BOT_SPEC §2.42)
    return {'text': await research_ffinal.run_final4h(ctx.params, progress=ctx.progress)}


async def _task_battery_load(ctx: Ctx):
    import research_battery                      # [v108] загрузка D1 + fundingRate всех перпетуалов Binance USDT-M (BOT_SPEC §2.43)
    return {'text': await research_battery.run_load(ctx.params, progress=ctx.progress)}


async def _task_battery(ctx: Ctx):
    import research_battery                      # [v108] шаги 0–3 серии «батарея», LOCKBOX не читается
    return {'text': await research_battery.run_battery(ctx.params, progress=ctx.progress)}


async def _task_battery_final(ctx: Ctx):
    import research_battery                      # [v108] шаг 4: единственный прогон на LOCKBOX (флаг серии)
    return {'text': await research_battery.run_battery_final(ctx.params, progress=ctx.progress)}


async def _task_final_h2(ctx: Ctx):
    import research_h2final                      # [v113] одиночный финал H2-D3: k на синтетике и единственный прогон на LOCKBOX (BOT_SPEC §2.43.11, флаг final_h2_started)
    return {'text': await research_h2final.run_final_h2(ctx.params, progress=ctx.progress)}


async def _task_vrp_probe(ctx: Ctx):
    import research_vrp                          # [v113] доступность DVOL Deribit с сервера (BOT_SPEC §2.46 п. 1)
    return {'text': await research_vrp.run_probe(ctx.params, progress=ctx.progress)}


async def _task_vrp_measure(ctx: Ctx):
    import research_vrp                          # [v113] VRP_t = DVOL_t − RV(t, t+30), неперекрывающиеся точки (§2.46 п. 2)
    return {'text': await research_vrp.run_measure(ctx.params, progress=ctx.progress)}


async def _task_vrp_sim(ctx: Ctx):
    import research_vrp                          # [v113] приближённая симуляция недельного железного кондора, только при пройденном гейте (§2.46 п. 3)
    return {'text': await research_vrp.run_sim(ctx.params, progress=ctx.progress)}


async def _task_audit(ctx: Ctx):
    import audit_sim                             # [v109] аудит внешнего источника сигналов: симуляция моделей A/B и отчёт (BOT_SPEC §2.44)
    return {'text': await audit_sim.run_audit(ctx.params, progress=ctx.progress)}


async def _task_signal_study(ctx: Ctx):
    import audit_study                           # [v111] поиск по данным канала, части 1–3 на DISCOVERY (BOT_SPEC §2.45)
    return {'text': await audit_study.run_study(ctx.params, progress=ctx.progress)}


async def _task_signal_final(ctx: Ctx):
    import audit_study                           # [v111] финал на HOLDOUT: единственный прогон (флаг в БД аудита)
    return {'text': await audit_study.run_final(ctx.params, progress=ctx.progress)}


async def _task_audit_import(ctx: Ctx):
    import audit_db                              # [v109] импорт экспорта Telegram Desktop (result.json) в неизменяемую таблицу аудита
    p = ctx.params
    try:
        with open(p['path'], encoding='utf-8') as f:
            data = json.load(f)
        con = audit_db.connect(p.get('adb'))
        res = audit_db.import_export(con, data, p.get('label'))
    finally:
        try:
            os.remove(p['path'])
        except OSError:
            pass
    return {'text': ('📥 /audit_import: сообщений {messages}, сигналов {signal}, отчётов о целях {report}, нераспознано {unparsed}, прочих {other}, уже было {dup}, служебных/без даты пропущено {skipped}, чатов {chats}. '
                     'Источник «import» — только справочно (§2.44.1 п. 2). Список нераспознанных — /audit_unparsed.').format(**res)}


async def _task_h2_funding_check(ctx: Ctx):
    import research_h2repl                       # [v116] фандинг в исходном H2-D3: проверка по коду и «без / с фандингом» на TRAIN и VALID (BOT_SPEC §2.47.1)
    return {'text': research_h2repl.funding_check(ctx.params)}


async def _task_h2_replicate(ctx: Ctx):
    import research_h2repl                       # [v116] репликация H2-D3 на независимых монетах Bybit/BingX (BOT_SPEC §2.47)
    return {'text': await research_h2repl.run_h2repl(ctx.params, progress=ctx.progress)}


async def _task_h2_ext(ctx: Ctx):
    import research_h2ext                        # [v129] H2-D3 на новых биржах (H2EXT) и H2F (BOT_SPEC §2.59)
    return {'text': await research_h2ext.run_h2ext(ctx.params, progress=ctx.progress)}


async def _task_binance_bases(ctx: Ctx):
    import research_bdata                        # [v137] полный список баз архива Binance USDT-M (BOT_SPEC §2.67.2); публичный листинг data.binance.vision
    return {'text': await research_bdata.run_binance_bases(ctx.params, progress=ctx.progress)}


async def _task_h2_coins(ctx: Ctx):
    import research_coins                        # [v134] наборы монет с сетями для п. г §2.64.2 (сеть вне процесса расчёта); публичные GET
    return {'text': await research_coins.refresh_job(ctx.params)}


async def _task_contract_probe(ctx: Ctx):
    import research_cprobe                       # [v134] /contract_probe: ключи объекта контракта по реальным ответам API (BOT_SPEC §2.64.1, шаг 0); публичные GET
    return {'text': await research_cprobe.run_contract_probe(ctx.params, progress=ctx.progress)}


async def _task_venue_probe(ctx: Ctx):
    import research_venues                       # [v131] /venue_probe: формы запросов бирж прямого REST на одном символе (BOT_SPEC §2.61.1); публичные GET
    return {'text': await research_venues.probe_venue(ctx.params.get('venue'), ctx.params.get('base') or 'BTC')}


async def _task_disk_usage(ctx: Ctx):
    import research_h2ext                        # [v130] /disk_usage: структура диска и оценка H2EXT (BOT_SPEC §2.59.11); ничего не изменяет
    return {'text': await research_h2ext.disk_job(ctx.params)}


async def _task_leadlag_report(ctx: Ctx):
    import research_leadlag                      # [v129] отчёт по замеру расхождений цен OKX↔BingX (BOT_SPEC §2.60)
    d = ctx.params.get('ll_dir') or research_leadlag.leadlag_dir()
    return {'text': research_leadlag.report(d, float(ctx.params.get('fee_taker') or 0.0005))}


async def _task_xfund_1h_errors(ctx: Ctx):
    import research_h1err                       # [v125] диагностика и классификация ошибок 1h BingX (BOT_SPEC §2.57)
    return {'text': await research_h1err.run_h1_errors(ctx.params, progress=ctx.progress)}


async def _task_xfund_test(ctx: Ctx):
    import research_xfund                        # [v116] X1: межбиржевой арбитраж фандинга BingX/Bybit (BOT_SPEC §2.48)
    return {'text': await research_xfund.run_xfund(ctx.params, progress=ctx.progress)}


TASKS = {
    '_selftest_mem': _task_selftest_mem, '_selftest_ok': _task_selftest_ok, '_selftest_kill': _task_selftest_kill, '_selftest_env': _task_selftest_env, '_selftest_blas': _task_selftest_blas,
    'edge_audit': _task_edge_audit, 'of_backtest': _task_of_backtest, 'htf_backtest': _task_htf_backtest, 'htf_wide': _task_htf_wide,
    'htf_xsmom': _task_htf_xsmom, 'wide_stop': _task_wide_stop, 'data_probe': _task_data_probe, 'carry_test': _task_carry_test, 'data_probe_ma': _task_data_probe_ma,
    'factory_load': _task_factory_load, 'factory_pairs': _task_factory_pairs, 'factory': _task_factory, 'factory_final': _task_factory_final,
    'factory_decomp': _task_factory_decomp, 'factory_nc': _task_factory_nc,
    'final_crypto4h': _task_final_crypto4h,
    'battery_load': _task_battery_load, 'battery': _task_battery, 'battery_final': _task_battery_final,
    'final_h2': _task_final_h2, 'vrp_probe': _task_vrp_probe, 'vrp_measure': _task_vrp_measure, 'vrp_sim': _task_vrp_sim,
    'h2_funding_check': _task_h2_funding_check, 'h2_replicate': _task_h2_replicate, 'xfund_test': _task_xfund_test, 'xfund_1h_errors': _task_xfund_1h_errors, 'h2_ext': _task_h2_ext, 'disk_usage': _task_disk_usage, 'venue_probe': _task_venue_probe, 'contract_probe': _task_contract_probe, 'h2_coins': _task_h2_coins, 'binance_bases': _task_binance_bases, 'leadlag_report': _task_leadlag_report,
    'audit': _task_audit, 'audit_import': _task_audit_import, 'signal_study': _task_signal_study, 'signal_final': _task_signal_final,
}


def _write_result(d: str, obj: dict):
    tmp = os.path.join(d, 'result.json.tmp')
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(obj, f, ensure_ascii=False)
    os.replace(tmp, os.path.join(d, 'result.json'))


def child_main(job_path: str) -> int:
    """Точка входа дочернего процесса. Лимит памяти — ПЕРВЫМ действием (до импортов numpy/ccxt)."""
    with open(job_path, encoding='utf-8') as f:
        job = json.load(f)
    _apply_limit(float(job.get('mem_mb') or DEFAULT_MEM_MB))
    d = job['dir']
    ctx = Ctx(job)
    try:
        fn = TASKS[job['task']]
        out = asyncio.run(fn(ctx))
        _write_result(d, dict(out or {}, ok=True, rss_mb=round(rss_mb(), 1)))
        return 0
    except MemoryError:
        try:
            _write_result(d, {'ok': False, 'kind': 'memory', 'error': f'MemoryError: превышен лимит памяти RESEARCH_MEM_MB={float(job.get("mem_mb") or 0):g} (RSS {rss_mb():.0f} МБ)'})
        except BaseException:
            pass
        return EXIT_MEMORY
    except BaseException as e:
        tb = ''.join(traceback.format_exception(type(e), e, e.__traceback__))[-1500:]
        try:
            _write_result(d, {'ok': False, 'kind': 'error', 'error': f'{type(e).__name__}: {e}', 'traceback': tb})
        except BaseException:
            pass
        sys.stderr.write(tb)
        return EXIT_ERROR


# ═══ родительская сторона (в процессе бота) ══════════════════════════════════════════════════════════
def _read_new_progress(path: str, offset: int):
    """→ (список dict, новый offset). Неполная последняя строка не потребляется."""
    try:
        with open(path, 'rb') as f:
            f.seek(offset)
            chunk = f.read()
    except FileNotFoundError:
        return [], offset
    if not chunk:
        return [], offset
    end = chunk.rfind(b'\n')
    if end < 0:
        return [], offset
    out = []
    for ln in chunk[:end + 1].splitlines():
        try:
            out.append(json.loads(ln.decode('utf-8', 'replace')))
        except Exception:
            continue
    return out, offset + end + 1


def describe_exit(code, result, mem_mb, stderr_tail: str = '') -> str:
    """Причина падения дочернего процесса для сообщения в Telegram."""
    if result and result.get('error') and not result.get('ok'):
        r = result['error']
        return r if result.get('kind') == 'memory' else f'исключение в расчёте: {r}'
    if code is None:
        return 'дочерний процесс не завершился корректно'
    if code < 0:
        try:
            name = signal.Signals(-code).name
        except Exception:
            name = str(-code)
        extra = ' — вероятно, убит ОС за нехватку памяти (OOM-killer)' if name == 'SIGKILL' else ''
        return f'убит сигналом {name}{extra}'
    if code == EXIT_MEMORY:
        return f'MemoryError: превышен лимит памяти RESEARCH_MEM_MB={mem_mb:g}'
    low = (stderr_tail or '').lower()
    if 'openblas' in low or 'memory allocation' in low:
        thr = child_env().get('OPENBLAS_NUM_THREADS', '?')
        return f'нехватка адресного пространства для BLAS; потоки BLAS: {thr}; лимит RESEARCH_MEM_MB={mem_mb:g} (код возврата {code})'
    tail = (stderr_tail or '').strip().splitlines()[-3:]
    return f'код возврата {code}' + (f'; stderr: {" | ".join(tail)[:400]}' if tail else '')


async def run_job(task: str, params: dict = None, payload=None, progress=None, mem_mb: float = None, poll_s: float = 2.0,
                  base_dir: str = None, script: str = None, label: str = None) -> dict:
    """Запускает задачу в дочернем процессе и ждёт её. progress — async callable(text): пачка строк прогресса за интервал опроса.
    → {'ok', 'text', 'error', 'code', 'last_progress', 'rss_peak_mb', 'result'}. Никогда не бросает исключений наружу (кроме CancelledError)."""
    mem_mb = float(mem_mb if mem_mb is not None else mem_limit_mb())
    base = base_dir or workdir_base()
    out = {'ok': False, 'text': '', 'error': '', 'code': None, 'last_progress': '', 'rss_peak_mb': 0.0, 'result': None}
    d = None
    proc = None
    try:
        os.makedirs(base, exist_ok=True)
        d = tempfile.mkdtemp(prefix=f'{task}_', dir=base)
        job = {'task': task, 'params': params or {}, 'dir': d, 'mem_mb': mem_mb, 'ppid': os.getpid()}
        with open(os.path.join(d, 'job.json'), 'w', encoding='utf-8') as f:
            json.dump(job, f, ensure_ascii=False, default=str)
        if payload is not None:
            with open(os.path.join(d, 'payload.pkl'), 'wb') as f:
                pickle.dump(payload, f, protocol=pickle.HIGHEST_PROTOCOL)
        env = child_env(mem_mb)
        script = script or os.path.abspath(__file__)
        err_f = open(os.path.join(d, 'stderr.log'), 'wb')
        try:
            proc = await asyncio.create_subprocess_exec(sys.executable, script, os.path.join(d, 'job.json'),
                                                        stdout=asyncio.subprocess.DEVNULL, stderr=err_f, env=env, cwd=os.path.dirname(os.path.abspath(__file__)))
        finally:
            err_f.close()
        STATE.update(task=label or task, pid=proc.pid, started=time.time(), rss_mb=0.0, last='')
        offset = 0
        pf = os.path.join(d, 'progress.jsonl')
        waiter = asyncio.ensure_future(proc.wait())

        async def pump():
            nonlocal offset
            items, offset = _read_new_progress(pf, offset)
            if items:
                out['last_progress'] = items[-1].get('msg', '')
                STATE['last'] = out['last_progress']
                out['rss_peak_mb'] = max(out['rss_peak_mb'], max((float(i.get('rss') or 0) for i in items), default=0.0))
                if progress:
                    text = '\n'.join(str(i.get('msg', '')) for i in items)
                    try:
                        await progress(text[-3500:])
                    except Exception:
                        pass
        while not waiter.done():
            await asyncio.wait({waiter}, timeout=poll_s)
            cur = rss_mb(proc.pid)
            if cur:
                STATE['rss_mb'] = cur
                out['rss_peak_mb'] = max(out['rss_peak_mb'], cur)
            await pump()
        await pump()
        out['code'] = waiter.result()
        res = None
        rp = os.path.join(d, 'result.json')
        if os.path.exists(rp):
            try:
                with open(rp, encoding='utf-8') as f:
                    res = json.load(f)
            except Exception:
                res = None
        out['result'] = res
        if out['code'] == 0 and res and res.get('ok'):
            out['ok'], out['text'] = True, res.get('text', '')
        else:
            tail = ''
            try:
                with open(os.path.join(d, 'stderr.log'), 'rb') as f:
                    f.seek(0, 2); sz = f.tell(); f.seek(max(0, sz - 1500)); tail = f.read().decode('utf-8', 'replace')
            except Exception:
                pass
            out['error'] = describe_exit(out['code'], res, mem_mb, tail)
    except asyncio.CancelledError:
        if proc is not None and proc.returncode is None:
            proc.kill()
        raise
    except Exception as e:
        out['error'] = f'сбой запуска дочернего процесса: {type(e).__name__}: {e}'
        if proc is not None and proc.returncode is None:
            proc.kill()
    finally:
        STATE.update(task=None, pid=None)
        if d:
            shutil.rmtree(d, ignore_errors=True)
    return out


def status_line(bot_rss: float = None) -> str:
    """Строка для /status: «исследование: <команда> (N мин, RSS дочернего M МБ) / нет, RSS бота N МБ»."""
    bot = rss_mb() if bot_rss is None else bot_rss
    if STATE['task']:
        mins = (time.time() - STATE['started']) / 60
        return f'исследование: {STATE["task"]} ({mins:.0f} мин, RSS дочернего {STATE["rss_mb"]:.0f} МБ, лимит {mem_limit_mb():g} МБ), RSS бота {bot:.0f} МБ'
    return f'исследование: нет, RSS бота {bot:.0f} МБ'


if __name__ == '__main__':
    sys.exit(child_main(sys.argv[1]))
