"""Prioritised background fetching for ``load(telemetry="lazy")`` (Elis E10).

A lazy load fetches a driver's laps on the first access of any of them. One
wave per driver pays its own ramp-up and tail, so a script that touches
every lap of a cold session used to pay ~6% more than the eager batch. The
:class:`PriorityFetchStream` keeps one event loop on a daemon thread that
runs every lazy request under one concurrency cap: the laps a caller waits
for go first, and the free slots (the tail of a foreground wave, the time
the script spends on the frames) fetch the drivers that are likely next.
A key is fetched once whatever its priority: a foreground request for a key
already in flight joins it, one for a queued background key promotes it.

The module is pandas- and polars-free and imports asyncio on the stream
thread only.
"""

from __future__ import annotations

import concurrent.futures
import logging
import threading
import weakref
from collections import deque
from collections.abc import Awaitable, Callable, Hashable, Iterable
from typing import Any

logger = logging.getLogger(__name__)

_FOREGROUND = 0
_BACKGROUND = 1
_IN_FLIGHT = 2
_DONE = 3

_live_streams: weakref.WeakSet[PriorityFetchStream] = weakref.WeakSet()
_exit_hook_lock = threading.Lock()
_exit_hook_registered = False


def _close_live_streams() -> None:
    """Interpreter exit: drop the queues and cancel the requests in flight.

    A request left running would find the thread pools shut down a moment
    later and walk the CDN retry loop (warnings, backoff sleeps, a tripped
    circuit breaker) on a dying interpreter.
    """
    for stream in list(_live_streams):
        stream.close(cancel=True)


def _register_exit_hook() -> None:
    """Close every stream before the thread pools are joined at interpreter exit.

    ``threading._register_atexit`` hooks run before ``concurrent.futures``
    joins its workers (``atexit`` hooks run after), so a stream stops
    queueing requests before the joins start.
    """
    global _exit_hook_registered
    with _exit_hook_lock:
        if _exit_hook_registered:
            return
        _exit_hook_registered = True
    register = getattr(threading, "_register_atexit", None)
    try:
        if callable(register):
            register(_close_live_streams)
            return
    except RuntimeError:  # interpreter already shutting down
        return
    import atexit

    atexit.register(_close_live_streams)


class PriorityFetchStream:
    """Run keyed requests on one background event loop, foreground keys first.

    Args:
        fetch: Coroutine function fetching one request; its result (``None``
            for a failure) becomes the key's future result. An exception is
            logged and also resolves to ``None``.
        max_concurrent: Requests in flight at most (foreground and background).
        background_concurrent: Background requests in flight at most; they
            are only started while no foreground key is queued.
        on_idle: Called on the stream thread whenever the stream runs out of
            work (nothing queued that may start, nothing in flight).
    """

    def __init__(
        self,
        fetch: Callable[[Any], Awaitable[Any]],
        *,
        max_concurrent: int,
        background_concurrent: int,
        on_idle: Callable[[], None] | None = None,
    ) -> None:
        self._fetch = fetch
        self._max = max(1, int(max_concurrent))
        self._bg_max = max(0, min(int(background_concurrent), self._max))
        self._on_idle = on_idle
        self._lock = threading.Lock()
        self._requests: dict[Hashable, Any] = {}
        self._futures: dict[Hashable, concurrent.futures.Future[Any]] = {}
        self._state: dict[Hashable, int] = {}
        self._foreground: deque[Hashable] = deque()
        self._background: deque[Hashable] = deque()
        self._in_flight = 0
        self._bg_in_flight = 0
        self._running = False
        self._closed = False
        self._loop: Any = None
        self._wake: Any = None
        self._tasks: set[Any] = set()
        self._idle = threading.Event()
        self._idle.set()
        _live_streams.add(self)
        _register_exit_hook()

    # -- caller side -------------------------------------------------------------

    def request(
        self, items: Iterable[tuple[Hashable, Any]]
    ) -> list[concurrent.futures.Future[Any]]:
        """Futures of ``items`` (``(key, request)``), fetched before any background key.

        A key already done, in flight or queued in the foreground keeps its
        future; a queued background key is promoted. After :meth:`close`
        an unknown key resolves to ``None`` at once.
        """
        futures: list[concurrent.futures.Future[Any]] = []
        with self._lock:
            for key, request in items:
                future = self._futures.get(key)
                state = self._state.get(key)
                if future is None:
                    future = concurrent.futures.Future()
                    if self._closed:
                        future.set_result(None)
                    else:
                        self._futures[key] = future
                        self._requests[key] = request
                        self._state[key] = _FOREGROUND
                        self._foreground.append(key)
                elif state == _BACKGROUND:
                    self._state[key] = _FOREGROUND
                    self._foreground.append(key)
                futures.append(future)
            self._kick_locked()
        return futures

    def prefetch(self, items: Iterable[tuple[Hashable, Any]]) -> None:
        """Make ``items`` the background queue, in order.

        Background keys queued earlier and not in ``items`` are dropped (a
        later :meth:`request` or :meth:`prefetch` can queue them again);
        keys already requested, in flight or done are skipped.
        """
        with self._lock:
            if self._closed:
                return
            queue: deque[Hashable] = deque()
            wanted: set[Hashable] = set()
            for key, request in items:
                state = self._state.get(key)
                if state is None:
                    self._futures[key] = concurrent.futures.Future()
                    self._requests[key] = request
                    self._state[key] = _BACKGROUND
                elif state != _BACKGROUND:
                    continue
                if key not in wanted:
                    wanted.add(key)
                    queue.append(key)
            for key in self._background:
                if key not in wanted and self._state.get(key) == _BACKGROUND:
                    self._forget_locked(key)
            self._background = queue
            self._kick_locked()

    def holds(self, key: Hashable) -> bool:
        """Whether ``key`` is queued (either priority), in flight or fetched."""
        with self._lock:
            return key in self._state

    def results(self) -> dict[Hashable, Any]:
        """Results of the keys fetched so far (failures included, as ``None``)."""
        with self._lock:
            done = [key for key, state in self._state.items() if state == _DONE]
            return {key: self._futures[key].result() for key in done}

    def close(self, *, cancel: bool = False) -> list[concurrent.futures.Future[Any]]:
        """Stop starting requests; returns the futures of the requests still in flight.

        Queued keys (either priority) are dropped; their futures resolve to
        ``None`` so no waiter hangs. Results fetched so far stay readable
        through :meth:`results`. ``cancel`` also cancels the requests in
        flight (interpreter exit); their futures resolve to ``None``, and
        the call waits up to half a second for the stream thread to do it.
        """
        cancelled = threading.Event()
        with self._lock:
            if cancel and self._loop is not None and self._tasks:
                self._loop.call_soon_threadsafe(self._cancel_tasks, cancelled)
            else:
                cancelled.set()
            self._closed = True
            dropped = [
                key for key, state in self._state.items() if state in (_FOREGROUND, _BACKGROUND)
            ]
            for key in dropped:
                self._forget_locked(key)
            self._foreground.clear()
            self._background.clear()
            in_flight = [
                self._futures[key] for key, state in self._state.items() if state == _IN_FLIGHT
            ]
        cancelled.wait(0.5)
        return in_flight

    def _cancel_tasks(self, cancelled: threading.Event) -> None:
        """Stream thread: cancel every request task (see :meth:`close`)."""
        for task in list(self._tasks):
            task.cancel()
        cancelled.set()

    def wait_idle(self, timeout: float | None = None) -> bool:
        """Block until nothing is in flight or startable; ``False`` on timeout."""
        return self._idle.wait(timeout)

    @property
    def closed(self) -> bool:
        """Whether :meth:`close` ran."""
        return self._closed

    # -- internals (lock held where the name says so) ----------------------------

    def _forget_locked(self, key: Hashable) -> None:
        """Drop a queued key; its future resolves to ``None`` (no waiter hangs)."""
        self._state.pop(key, None)
        self._requests.pop(key, None)
        future = self._futures.pop(key, None)
        if future is not None and not future.done():
            future.set_result(None)

    def _kick_locked(self) -> None:
        """Wake the stream thread, or start one when there is work and none runs."""
        if self._running:
            if self._loop is not None:
                self._loop.call_soon_threadsafe(self._wake.set)
            # else: the thread is starting and dispatches before it waits.
            return
        if not self._foreground and not (self._background and self._bg_max):
            return
        self._running = True
        self._idle.clear()
        thread = threading.Thread(target=self._thread_main, name="tif1-lazy-telemetry", daemon=True)
        thread.start()

    def _next_locked(self) -> tuple[Hashable, bool] | None:
        while self._foreground:
            key = self._foreground.popleft()
            if self._state.get(key) == _FOREGROUND:
                return key, False
        if self._bg_in_flight >= self._bg_max:
            return None
        while self._background:
            key = self._background.popleft()
            if self._state.get(key) == _BACKGROUND:
                return key, True
        return None

    def _dispatch_locked(self, loop: Any) -> None:
        while self._in_flight < self._max:
            picked = self._next_locked()
            if picked is None:
                return
            key, background = picked
            self._state[key] = _IN_FLIGHT
            self._in_flight += 1
            if background:
                self._bg_in_flight += 1
            task = loop.create_task(self._run(key, self._requests[key], background))
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)

    async def _run(self, key: Hashable, request: Any, background: bool) -> None:
        result: Any = None
        try:
            result = await self._fetch(request)
        except Exception as e:
            logger.warning("Lazy telemetry fetch failed for %s: %s: %s", key, type(e).__name__, e)
        finally:
            with self._lock:
                if self._state.get(key) == _IN_FLIGHT:
                    self._state[key] = _DONE
                self._in_flight -= 1
                if background:
                    self._bg_in_flight -= 1
                future = self._futures.get(key)
            if future is not None and not future.done():
                future.set_result(result)
            if self._wake is not None:
                self._wake.set()

    async def _main(self) -> None:
        import asyncio  # lazy: only the stream thread needs it

        loop = asyncio.get_running_loop()
        wake = asyncio.Event()
        with self._lock:
            self._loop, self._wake = loop, wake
        while True:
            with self._lock:
                self._dispatch_locked(loop)
                if self._in_flight == 0:
                    self._running = False
                    self._loop = self._wake = None
                    break
            await wake.wait()
            wake.clear()

    def _thread_main(self) -> None:
        import asyncio  # lazy: only the stream thread needs it

        try:
            asyncio.run(self._main())
        except BaseException as e:  # never leave a waiter hanging
            logger.warning("Lazy telemetry stream stopped: %s: %s", type(e).__name__, e)
            with self._lock:
                self._running = False
                self._loop = self._wake = None
                pending = [
                    future
                    for key, future in self._futures.items()
                    if self._state.get(key) != _DONE and not future.done()
                ]
                for key in list(self._state):
                    if self._state[key] != _DONE:
                        self._state[key] = _DONE
                self._foreground.clear()
                self._background.clear()
                self._in_flight = self._bg_in_flight = 0
            for future in pending:
                if not future.done():
                    future.set_result(None)
        else:
            if self._on_idle is not None:
                try:
                    self._on_idle()
                except Exception as e:
                    logger.debug("Lazy telemetry stream idle hook failed: %s", e)
        with self._lock:
            if not self._running:  # no newer thread took over meanwhile
                self._idle.set()
