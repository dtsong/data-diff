import threading

import pytest

from data_diff.thread_utils import PriorityThreadPoolExecutor, ThreadedYielder


class TestPriorityThreadPoolExecutor:
    def test_priority_ordering(self):
        """Higher-priority tasks execute before lower-priority ones."""
        gate = threading.Event()
        results = []

        pool = PriorityThreadPoolExecutor(max_workers=1)

        # Block the single worker so tasks queue up
        pool.submit(lambda: gate.wait(), priority=0)

        # Submit tasks with different priorities while worker is blocked
        for p in [1, 3, 2]:
            pool.submit(lambda p=p: results.append(p), priority=p)

        # Release the gate — queued tasks run in priority order
        gate.set()
        pool.shutdown(wait=True)

        assert results == [3, 2, 1]

    def test_fifo_within_same_priority(self):
        """Equal-priority tasks run in submission order (FIFO)."""
        gate = threading.Event()
        results = []

        pool = PriorityThreadPoolExecutor(max_workers=1)
        pool.submit(lambda: gate.wait(), priority=0)

        for i in range(5):
            pool.submit(lambda i=i: results.append(i), priority=1)

        gate.set()
        pool.shutdown(wait=True)

        assert results == [0, 1, 2, 3, 4]

    def test_submit_returns_future_with_result(self):
        """submit() returns a Future that resolves to the function's return value."""
        pool = PriorityThreadPoolExecutor(max_workers=2)
        future = pool.submit(lambda: 42)
        assert future.result(timeout=5) == 42
        pool.shutdown()

    def test_submit_returns_future_with_exception(self):
        """Exceptions in submitted functions propagate through the Future."""
        pool = PriorityThreadPoolExecutor(max_workers=2)
        future = pool.submit(lambda: 1 / 0)
        with pytest.raises(ZeroDivisionError):
            future.result(timeout=5)
        pool.shutdown()

    def test_concurrent_submit(self):
        """Submitting from multiple threads is safe."""
        pool = PriorityThreadPoolExecutor(max_workers=4)
        results = []
        lock = threading.Lock()

        def task(n):
            with lock:
                results.append(n)

        threads = []
        for i in range(20):
            t = threading.Thread(target=lambda i=i: pool.submit(task, i, priority=0))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        pool.shutdown(wait=True)
        assert sorted(results) == list(range(20))

    def test_shutdown_with_pending_work(self):
        """Shutdown completes all pending work before returning."""
        results = []
        pool = PriorityThreadPoolExecutor(max_workers=1)

        for i in range(10):
            pool.submit(lambda i=i: results.append(i), priority=0)

        pool.shutdown(wait=True)
        assert sorted(results) == list(range(10))

    def test_no_cpython_internals_imported(self):
        """Verify _WorkItem is not imported."""
        import data_diff.thread_utils as mod

        assert not hasattr(mod, "_WorkItem")


class TestThreadedYielder:
    def test_basic_yield(self):
        """ThreadedYielder collects results from submitted functions."""
        ty = ThreadedYielder(max_workers=2)
        ty.submit(lambda: [1, 2, 3])
        ty.submit(lambda: [4, 5, 6])

        result = list(ty)
        assert sorted(result) == [1, 2, 3, 4, 5, 6]

    def test_priority_behavior(self):
        """Higher-priority iterators get scheduled first."""
        gate = threading.Event()
        ty = ThreadedYielder(max_workers=1)

        # Block the worker
        def wait_gate():
            gate.wait()
            return []

        ty.submit(wait_gate, priority=0)

        # Queue tasks with different priorities
        ty.submit(lambda: ["low"], priority=1)
        ty.submit(lambda: ["high"], priority=3)
        ty.submit(lambda: ["mid"], priority=2)

        gate.set()
        result = list(ty)
        # High-priority tasks should execute first
        assert result == ["high", "mid", "low"]

    def test_yield_list_mode(self):
        """yield_list=True appends entire results rather than extending."""
        ty = ThreadedYielder(max_workers=1, yield_list=True)
        ty.submit(lambda: [1, 2, 3])

        result = list(ty)
        assert result == [[1, 2, 3]]

    def test_exception_propagation(self):
        """Exceptions in submitted functions propagate through iteration."""
        ty = ThreadedYielder(max_workers=1)
        ty.submit(lambda: (_ for _ in ()).throw(ValueError("boom")))

        with pytest.raises(ValueError, match="boom"):
            list(ty)
