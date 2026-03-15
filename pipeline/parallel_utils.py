"""
parallel_utils.py — Parallel task runners for the Hansard pipeline.

Two modes are available:

  eager_map / eager_threaded_map  (default for all pipeline scripts)
    Simple, fixed-worker pools that use every available CPU core with no
    dynamic scaling.  eager_map uses ProcessPoolExecutor (cpu_count workers);
    eager_threaded_map uses ThreadPoolExecutor (cpu_count × 4 threads, suited
    for I/O-bound work like reading many files concurrently).

  dynamic_map / threaded_map  (legacy)
    Pool that scales workers up/down based on live CPU and memory readings
    via psutil.  Retained for backward compatibility.

Public API
----------
  eager_map(func, items, ...)           — CPU-bound tasks, all CPUs, no scaling
  eager_threaded_map(func, items, ...)  — I/O-bound tasks, high thread count
  dynamic_map(func, items, ...)         — CPU-bound, dynamic scaling (legacy)
  threaded_map(func, items, ...)        — I/O-bound, dynamic scaling (legacy)
"""

import logging
import os
import threading
import time
from concurrent.futures import Future, ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from multiprocessing import cpu_count
from typing import Any, Callable, Optional

try:
    from tqdm import tqdm as _tqdm
except ImportError:
    def _tqdm(it, **_kw):           # type: ignore[misc]
        return it

try:
    import psutil
    _HAS_PSUTIL = True
except ImportError:
    _HAS_PSUTIL = False

log = logging.getLogger(__name__)




# ── Eager (non-conservative) parallel runners ─────────────────────────────────

def eager_map(
    func: Callable,
    items: list,
    *,
    workers: Optional[int] = None,
    initializer: Optional[Callable] = None,
    initargs: tuple = (),
    desc: Optional[str] = None,
    unit: str = "item",
) -> list[Any]:
    """
    Apply `func` to every item in `items` using a ProcessPoolExecutor with
    all available CPU cores.  No dynamic scaling — all cores run at full speed
    for the duration.

    Parameters
    ----------
    func          Callable taking a single positional argument.
    items         Input list; results are returned in the same order.
    workers       Number of worker processes (default: os.cpu_count()).
    initializer   Optional callable run once per worker at startup.
    initargs      Arguments forwarded to `initializer`.
    desc          tqdm progress-bar label.
    unit          tqdm progress-bar unit string.
    """
    if not items:
        return []
    n = workers or (cpu_count() or 1)
    log.info("eager_map: %d items  workers=%d  func=%s", len(items), n, func.__name__)
    kw: dict = {"max_workers": n}
    if initializer is not None:
        kw["initializer"] = initializer
        kw["initargs"] = initargs
    with ProcessPoolExecutor(**kw) as pool:
        return list(_tqdm(
            pool.map(func, items),
            total=len(items),
            desc=desc or func.__name__,
            unit=unit,
        ))


def eager_threaded_map(
    func: Callable,
    items: list,
    *,
    workers: Optional[int] = None,
    desc: Optional[str] = None,
    unit: str = "item",
) -> list[Any]:
    """
    Apply `func` to every item in `items` using a ThreadPoolExecutor with
    cpu_count × 4 threads.  Threads share process memory so there is no
    serialisation overhead — suited for I/O-bound work (disk reads, HTTP).

    Parameters
    ----------
    func     Callable taking a single positional argument.
    items    Input list; results are returned in the same order.
    workers  Thread count (default: os.cpu_count() × 4).
    desc     tqdm progress-bar label.
    unit     tqdm progress-bar unit string.
    """
    if not items:
        return []
    n = workers or ((cpu_count() or 1) * 4)
    log.info("eager_threaded_map: %d items  threads=%d  func=%s", len(items), n, func.__name__)
    with ThreadPoolExecutor(max_workers=n) as pool:
        return list(_tqdm(
            pool.map(func, items),
            total=len(items),
            desc=desc or func.__name__,
            unit=unit,
        ))


# ── Resource monitor ──────────────────────────────────────────────────────────

class ResourceMonitor(threading.Thread):
    """
    Daemon thread that samples CPU and memory every `interval` seconds.

    All public attributes are updated atomically so the main thread can
    read them safely at any time.
    """

    def __init__(self, interval: float = 30.0):
        super().__init__(daemon=True, name="ResourceMonitor")
        self.interval = interval
        self._stop_event = threading.Event()
        self._lock = threading.Lock()
        # Public, read from main thread
        self.cpu_pct: float = 0.0
        self.mem_pct: float = 0.0
        self.other_python_procs: int = 0
        # Do one synchronous sample so callers get real data immediately
        self._sample()

    def _sample(self) -> None:
        if not _HAS_PSUTIL:
            return
        try:
            cpu = psutil.cpu_percent(interval=1.0)       # blocking 1-second sample
            mem = psutil.virtual_memory().percent
            own_pid = os.getpid()
            other = sum(
                1 for p in psutil.process_iter(["pid", "name"])
                if p.info["pid"] != own_pid
                and "python" in (p.info.get("name") or "").lower()
            )
            with self._lock:
                self.cpu_pct = cpu
                self.mem_pct = mem
                self.other_python_procs = other
        except Exception as exc:
            log.debug("ResourceMonitor._sample: %s", exc)

    def stats(self) -> dict:
        with self._lock:
            return {
                "cpu_pct":             self.cpu_pct,
                "mem_pct":             self.mem_pct,
                "other_python_procs":  self.other_python_procs,
            }

    def run(self) -> None:
        while not self._stop_event.wait(timeout=self.interval):
            self._sample()

    def stop(self) -> None:
        self._stop_event.set()
        self.join(timeout=5)


# ── Dynamic semaphore ─────────────────────────────────────────────────────────

class _DynamicSemaphore:
    """
    Semaphore whose concurrency limit can be raised or lowered at runtime.

    Lowering the limit only affects future acquires — it does not interrupt
    already-running tasks.  Raising the limit immediately wakes any threads
    blocked in `acquire()`.
    """

    def __init__(self, initial: int):
        self._cond = threading.Condition(threading.Lock())
        self._available = initial
        self._limit = initial

    def acquire(self, timeout: Optional[float] = None) -> bool:
        """Block until a slot is free. Returns True when acquired."""
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._cond:
            while self._available <= 0:
                wait_for = None if deadline is None else deadline - time.monotonic()
                if wait_for is not None and wait_for <= 0:
                    return False
                self._cond.wait(timeout=wait_for)
            self._available -= 1
            return True

    def release(self) -> None:
        """Release one slot (call from a future done-callback)."""
        with self._cond:
            if self._available < self._limit:
                self._available += 1
            self._cond.notify()

    def set_limit(self, new_limit: int) -> None:
        """Adjust the concurrency limit; takes effect on the next acquire."""
        with self._cond:
            old = self._limit
            self._limit = new_limit
            if new_limit > old:
                self._available = min(self._available + (new_limit - old), new_limit)
                self._cond.notify_all()
            # Reducing: _available drains naturally as running tasks finish


# ── Worker-count adjustment logic ─────────────────────────────────────────────

def _adjust_workers(
    monitor: ResourceMonitor,
    current: int,
    min_w: int,
    max_w: int,
    cpu_hi: float,
    cpu_lo: float,
    mem_hi: float,
) -> int:
    """Return an updated worker count based on current resource stats."""
    if not _HAS_PSUTIL:
        return current

    s = monitor.stats()
    cpu   = s["cpu_pct"]
    mem   = s["mem_pct"]
    other = s["other_python_procs"]
    new   = current

    if mem > mem_hi:
        new = max(min_w, current - 2)
        log.info(
            "workers %2d → %2d  [↓ mem %.0f%% > %.0f%%  other_py=%d]",
            current, new, mem, mem_hi, other,
        )
    elif cpu > cpu_hi:
        new = max(min_w, current - 1)
        log.info(
            "workers %2d → %2d  [↓ cpu %.0f%% > %.0f%%  other_py=%d]",
            current, new, cpu, cpu_hi, other,
        )
    elif cpu < cpu_lo and mem < mem_hi - 15:
        new = min(max_w, current + 1)
        log.info(
            "workers %2d → %2d  [↑ cpu %.0f%% < %.0f%%  mem %.0f%%  other_py=%d]",
            current, new, cpu, cpu_lo, mem, other,
        )
    else:
        log.debug(
            "workers %2d →  =  [cpu %.0f%%  mem %.0f%%  other_py=%d]",
            current, cpu, mem, other,
        )

    return new


# ── Core parallel runner ──────────────────────────────────────────────────────

def _run_executor(
    Executor,          # ProcessPoolExecutor or ThreadPoolExecutor
    func: Callable,
    items: list,
    *,
    min_workers: int,
    max_workers: int,
    initial_workers: int,
    check_interval: float,
    cpu_hi: float,
    cpu_lo: float,
    mem_hi: float,
    initializer: Optional[Callable],
    initargs: tuple,
) -> list:
    """
    Submit all items to `Executor`, throttled by a _DynamicSemaphore that the
    ResourceMonitor adjusts every `check_interval` seconds.

    Returns results in original item order.
    """
    monitor = ResourceMonitor(interval=check_interval)
    monitor.start()

    current_workers = initial_workers
    sem = _DynamicSemaphore(current_workers)
    results: dict[int, Any] = {}
    t_last_check = time.monotonic()

    def on_done(future: Future, idx: int) -> None:
        try:
            results[idx] = future.result()
        except Exception as exc:
            log.error("Worker error on item %d: %s", idx, exc)
            results[idx] = None
        finally:
            sem.release()

    executor_kwargs: dict = {"max_workers": max_workers}
    if Executor is ProcessPoolExecutor and initializer is not None:
        executor_kwargs["initializer"] = initializer
        executor_kwargs["initargs"]    = initargs

    try:
        with Executor(**executor_kwargs) as executor:
            for idx, item in enumerate(items):
                sem.acquire()  # blocks until a slot is available

                now = time.monotonic()
                if now - t_last_check >= check_interval:
                    new_w = _adjust_workers(
                        monitor, current_workers,
                        min_workers, max_workers,
                        cpu_hi, cpu_lo, mem_hi,
                    )
                    if new_w != current_workers:
                        sem.set_limit(new_w)
                        current_workers = new_w
                    t_last_check = now

                f = executor.submit(func, item)
                f.add_done_callback(lambda fut, i=idx: on_done(fut, i))
            # Executor.__exit__ waits until all submitted futures complete
    finally:
        monitor.stop()

    log.info(
        "parallel run complete: %d items processed  final_workers=%d",
        len(results), current_workers,
    )
    return [results[i] for i in range(len(items))]


# ── Public API ────────────────────────────────────────────────────────────────

def dynamic_map(
    func: Callable,
    items: list,
    *,
    min_workers: int = 2,
    max_workers: Optional[int] = None,
    initial_workers: Optional[int] = None,
    check_interval: float = 30.0,
    cpu_hi: float = 80.0,
    cpu_lo: float = 50.0,
    mem_hi: float = 85.0,
    initializer: Optional[Callable] = None,
    initargs: tuple = (),
    fallback_sequential: bool = True,
) -> list[Any]:
    """
    Apply `func` to every item in `items` using a process pool that scales
    dynamically based on CPU and memory load.

    Results are returned in the same order as `items`.

    Parameters
    ----------
    func              Callable that takes a single item and returns a result.
    items             List of inputs.
    min_workers       Minimum concurrent workers (floor, default 2).
    max_workers       Upper bound; defaults to cpu_count() - 1.
    initial_workers   Starting worker count; defaults to min(4, max_workers).
    check_interval    Seconds between resource re-checks (default 30).
    cpu_hi            Scale down when cpu% exceeds this (default 80).
    cpu_lo            Scale up when cpu% is below this (default 50).
    mem_hi            Scale down when memory% exceeds this (default 85).
    initializer       Optional function called once per worker process at
                      startup (e.g. to load shared lookup data).
    initargs          Arguments for `initializer`.
    fallback_sequential
                      If True, fall back to sequential processing if the
                      parallel run raises an unhandled exception.
    """
    if not items:
        return []

    n_cores     = cpu_count()
    max_workers = min(max_workers or (n_cores - 1), max(1, n_cores - 1))
    min_workers = max(1, min(min_workers, max_workers))
    initial_workers = max(min_workers, min(initial_workers or min(4, max_workers), max_workers))

    if not _HAS_PSUTIL:
        log.warning(
            "psutil not available — worker count fixed at %d. "
            "Install: pip install psutil",
            initial_workers,
        )

    log.info(
        "dynamic_map: %d items  cores=%d  workers=[%d..%d]  initial=%d  "
        "cpu_hi=%.0f%%  mem_hi=%.0f%%",
        len(items), n_cores, min_workers, max_workers, initial_workers,
        cpu_hi, mem_hi,
    )

    try:
        return _run_executor(
            ProcessPoolExecutor, func, items,
            min_workers=min_workers, max_workers=max_workers,
            initial_workers=initial_workers, check_interval=check_interval,
            cpu_hi=cpu_hi, cpu_lo=cpu_lo, mem_hi=mem_hi,
            initializer=initializer, initargs=initargs,
        )
    except Exception as exc:
        if fallback_sequential:
            log.error("Process pool failed (%s) — falling back to sequential", exc)
            if initializer is not None:
                initializer(*initargs)
            return [func(item) for item in items]
        raise


def threaded_map(
    func: Callable,
    items: list,
    *,
    max_workers: Optional[int] = None,
    check_interval: float = 30.0,
    cpu_hi: float = 80.0,
    cpu_lo: float = 50.0,
    mem_hi: float = 85.0,
    fallback_sequential: bool = True,
) -> list[Any]:
    """
    Apply `func` to every item in `items` using a thread pool that scales
    dynamically based on CPU and memory load.

    Threads share memory and avoid serialisation overhead — suited for
    I/O-bound work such as reading many parquet files in parallel.

    Parameters mirror `dynamic_map` except there is no `initializer`
    (threads already share the process's global state).
    """
    if not items:
        return []

    n_cores     = cpu_count()
    # For I/O-bound work, allow more threads than CPU cores (disk can serve
    # multiple readers concurrently); cap at 2× cores to avoid thrashing.
    max_workers = min(max_workers or (n_cores * 2), n_cores * 2)
    min_workers = 2
    initial_workers = min(8, max_workers)

    log.info(
        "threaded_map: %d items  max_threads=%d  initial=%d",
        len(items), max_workers, initial_workers,
    )

    try:
        return _run_executor(
            ThreadPoolExecutor, func, items,
            min_workers=min_workers, max_workers=max_workers,
            initial_workers=initial_workers, check_interval=check_interval,
            cpu_hi=cpu_hi, cpu_lo=cpu_lo, mem_hi=mem_hi,
            initializer=None, initargs=(),
        )
    except Exception as exc:
        if fallback_sequential:
            log.error("Thread pool failed (%s) — falling back to sequential", exc)
            return [func(item) for item in items]
        raise
