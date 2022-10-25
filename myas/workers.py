from __future__ import annotations

import asyncio
from asyncio import Task
from functools import cached_property
from typing import (
    Generic,
    TypeVar,
    Any,
    Callable,
    Coroutine,
    Iterable,
    AsyncIterable,
    Literal,
    AsyncIterator,
)

from loguru import logger

from myas import CloseableQueue, q2a, QueueExhausted, CloseableQueueLike

_OutputT = TypeVar("_OutputT")
_InputT = TypeVar("_InputT")


class WorkerPool(
    Generic[_InputT, _OutputT],
    AsyncIterable[_OutputT],
    CloseableQueueLike[_InputT, _OutputT],
):
    _fn: Callable[[_InputT], Coroutine[Any, Any, _OutputT]]

    def __init__(
        self,
        fn: Callable[[_InputT], Coroutine[Any, Any, _OutputT]],
        num_workers: int = 5,
        input_queue_size: int = 0,
        output_queue_size: int = -2,
    ) -> None:
        """Create a worker pool.

        Todo - think through the idea of having the output queue size be a multiple of the number
          of workers when the output queue size is negative, to enable backpressure while still
          having a buffer.
        """

        # todo - consider pool which flattens output - i.e., fn returns an iterable/async iterable,
        #  and the pool flattens it into the output queue.
        self._fn = fn

        if output_queue_size < 0:
            output_queue_size = num_workers * -output_queue_size
        self._output_queue = CloseableQueue[_OutputT](maxsize=output_queue_size)
        self._input_queue = CloseableQueue[_InputT](maxsize=input_queue_size)

        self._exc_queue = CloseableQueue[tuple[_InputT, Exception]]()  # todo - use this

        self._workers: list[Task[None]] = [
            asyncio.create_task(self._worker(i)) for i in range(num_workers)
        ]
        self._idle_workers: set[Task[None]] = set(self._workers)
        self._workers_idle = asyncio.Event()

    async def _worker(self, worker_id: int) -> None:
        logger.debug(f"Worker {worker_id} started")

        crt_task = asyncio.current_task()
        if crt_task is None:
            raise RuntimeError("No current task")

        while True:
            try:
                if self._input_queue.empty():
                    self._idle_workers.add(crt_task)
                    if len(self._idle_workers) == len(self._workers):
                        self._workers_idle.set()

                item = await self._input_queue.get()
                self._idle_workers.discard(crt_task)
                self._workers_idle.clear()

            except QueueExhausted:
                logger.debug(f"Worker {worker_id} closing (input queue exhausted)")
                return

            logger.debug(f"Worker {worker_id} got item {item}")
            try:
                result = await self._fn(item)
            except Exception as e:
                logger.error(f"Worker {worker_id} failed with exception {e}")
                await self._exc_queue.put((item, e.with_traceback(e.__traceback__)))
            else:
                await self._output_queue.put(result)
            finally:
                self._input_queue.task_done()

    async def add_source(
        self,
        *sources: Iterable[_InputT] | AsyncIterable[_InputT],
        close_when_done: bool = False,
    ) -> None:
        await self._input_queue.add_source(*sources, close_when_done=close_when_done)
        if close_when_done:
            await self.close()

    async def aclose(self) -> None:
        self._input_queue.close()
        await asyncio.gather(*self._workers)
        self._output_queue.close()

    @property
    def exhausted(self) -> Coroutine[Any, Any, Literal[True]]:
        return self._output_queue.exhausted

    @property
    def closed(self) -> Coroutine[Any, Any, Literal[True]]:
        # return self._input_queue.closed
        return 4

    async def put(self, item: _InputT) -> None:
        await self._input_queue.put(item)

    async def get(self) -> _OutputT:
        return await self._output_queue.get()

    # async def __call__(self, item: _InputT) -> _OutputT:
    #     todo - this is interesting, but it's not clear how it'd work exactly;
    #       when putting an item via calling, do we return the result? (possible with
    #       a proxy obj). If we do, do we still include it in the async iterator?
    #       is it possible to have a worker pool which is both callable and
    #       iterable/queue, or should we have two classes?
    #     return await self.put(item)

    async def join(self) -> None:
        while True:
            await asyncio.wait(
                (
                    self._input_queue.join(),  # No more tasks in the input queue
                    self._workers_idle.wait(),  # All workers are idle
                    self._output_queue.join(),  # No more results in the output queue
                ),
            )

            # Make sure all conditions for the join are met *simultaneously*; asyncio.wait may
            # return after a condition has been met, but subsequently unmet.
            if (
                self._input_queue.empty()
                and self._output_queue.empty()
                and self._workers_idle.is_set()
            ):
                break

    def __aiter__(self) -> AsyncIterator[_OutputT]:
        return q2a(self._output_queue)

    @cached_property
    def exceptions(self) -> AsyncIterator[tuple[_InputT, Exception]]:
        return q2a(self._exc_queue)


if __name__ == "__main__":

    async def main() -> None:
        async def fn(it: int) -> int:
            await asyncio.sleep(0.1)
            return it**2 // it - 8

        async def ait() -> AsyncIterator[int]:
            for i in range(10):
                await asyncio.sleep(0.1)
                yield i

        async def on_finished(p: WorkerPool[Any, Any]) -> None:
            await p.exhausted
            print("Whew. That was exhausting.")

        pool = WorkerPool(fn, num_workers=3)
        asyncio.create_task(on_finished(pool))

        asyncio.create_task(pool.add_source(ait(), ait(), close_when_done=False))
        await asyncio.sleep(0.2)

        asyncio.create_task(
            pool.add_source(
                range(10),
                range(1337, 1348),
                ait(),
                close_when_done=True,
            )
        )
        async for x in pool:
            print(x)
            # reveal_type(x)  # Revealed type is 'builtins.int'

        async for inp, ex in pool.exceptions:
            print(f"Exception {ex} for input {inp}\n")
            logger.opt(exception=True).exception(
                "Exception in worker pool", exc_info=ex, stack_info=True
            )

    asyncio.run(main())
