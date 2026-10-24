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
THREAD_ENV = {'OPENBLAS_NUM_THREADS': '1', 'OMP_NUM_THREADS': '1', 'MKL_NUM_THREADS': '1', 'NUMEXPR_NUM_THREADS': '1', 'MALLOC_ARENA_MAX': '2'}
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
    import research_ma
    return {'text': await research_ma.run_probe_ma(ctx.payload(), ctx.params, progress=ctx.progress)}


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


TASKS = {
    '_selftest_mem': _task_selftest_mem, '_selftest_ok': _task_selftest_ok, '_selftest_kill': _task_selftest_kill,
    'edge_audit': _task_edge_audit, 'of_backtest': _task_of_backtest, 'htf_backtest': _task_htf_backtest, 'htf_wide': _task_htf_wide,
    'htf_xsmom': _task_htf_xsmom, 'wide_stop': _task_wide_stop, 'data_probe': _task_data_probe, 'carry_test': _task_carry_test, 'data_probe_ma': _task_data_probe_ma,
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
        env = dict(os.environ, **THREAD_ENV, RESEARCH_MEM_MB=str(mem_mb), BOT_RESEARCH_CHILD='1', PYTHONUNBUFFERED='1')
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
