import itertools
import threading
from collections import deque
from collections.abc import Callable, Iterable, Iterator
from concurrent.futures import Future, ThreadPoolExecutor
from queue import PriorityQueue
from time import sleep
from typing import Any

import attrs

_SENTINEL = object()


def _chain_future(source: Future, dest: Future) -> None:
    """Propagate the outcome (result, exception, or cancellation) from source to dest."""
    if dest.cancelled():
        return
    try:
        if source.cancelled():
            dest.cancel()
        elif exc := source.exception():
            dest.set_exception(exc)
        else:
            dest.set_result(source.result())
    except Exception as exc:
        try:
            dest.set_exception(exc)
        except Exception:
            pass


class PriorityThreadPoolExecutor:
    """Thread pool that executes tasks in priority order.

    Uses a dispatcher thread to pull work from a PriorityQueue and
    submit it to a standard ThreadPoolExecutor. No CPython internals.
    """

    def __init__(self, max_workers: int | None = None) -> None:
        self._inner = ThreadPoolExecutor(max_workers=max_workers)
        self._queue: PriorityQueue = PriorityQueue()
        self._counter = itertools.count().__next__
        self._shutdown = False
        self._dispatcher = threading.Thread(target=self._dispatch, daemon=True)
        self._dispatcher.start()

    def _dispatch(self) -> None:
        while True:
            try:
                _priority, _count, item = self._queue.get()
                if item is _SENTINEL:
                    break
                fn, args, kwargs, proxy = item
                inner_future = self._inner.submit(fn, *args, **kwargs)
                inner_future.add_done_callback(lambda f, p=proxy: _chain_future(f, p))
            except Exception as exc:
                if "proxy" in dir() and not proxy.done():
                    try:
                        proxy.set_exception(exc)
                    except Exception:
                        pass

    def submit(self, fn, /, *args, priority: int = 0, **kwargs) -> Future:
        if self._shutdown:
            raise RuntimeError("cannot submit after shutdown")
        proxy = Future()
        self._queue.put((-priority, self._counter(), (fn, args, kwargs, proxy)))
        return proxy

    def shutdown(self, wait: bool = True) -> None:
        self._shutdown = True
        self._queue.put((float("inf"), self._counter(), _SENTINEL))
        self._dispatcher.join(timeout=30)
        if self._dispatcher.is_alive():
            raise RuntimeError("PriorityThreadPoolExecutor dispatcher did not shut down within 30s")
        self._inner.shutdown(wait=wait)


@attrs.define(frozen=False, init=False)
class ThreadedYielder(Iterable):
    """Yields results from multiple threads into a single iterator, ordered by priority.

    To add a source iterator, call ``submit()`` with a function that returns an iterator.
    Priority for the iterator can be provided via the keyword argument 'priority'. (higher runs first)
    """

    _pool: PriorityThreadPoolExecutor
    _futures: deque
    _yield: deque = attrs.field(alias="_yield")  # Python keyword!
    _exception: None = None
    yield_list: bool = False

    def __init__(self, max_workers: int | None = None, yield_list: bool = False) -> None:
        super().__init__()
        self._pool = PriorityThreadPoolExecutor(max_workers)
        self._futures = deque()
        self._yield = deque()
        self._exception = None
        self.yield_list = yield_list

    def _worker(self, fn, *args, **kwargs) -> None:
        try:
            res = fn(*args, **kwargs)
            if res is not None:
                if self.yield_list:
                    self._yield.append(res)
                else:
                    self._yield += res
        except Exception as e:
            self._exception = e

    def submit(self, fn: Callable, *args, priority: int = 0, **kwargs) -> None:
        self._futures.append(self._pool.submit(self._worker, fn, *args, priority=priority, **kwargs))

    def __iter__(self) -> Iterator[Any]:
        while True:
            if self._exception:
                raise self._exception

            while self._yield:
                yield self._yield.popleft()

            if not self._futures:
                # No more tasks
                return

            if self._futures[0].done():
                self._futures.popleft()
            else:
                sleep(0.001)
