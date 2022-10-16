from __future__ import annotations

import asyncio
import statistics
import textwrap
from asyncio import Queue, Future, Task
from collections import Counter
from dataclasses import dataclass, field
from datetime import timedelta, datetime
from enum import Enum
from typing import (
    Generic,
    TypeVar,
    Callable,
    Coroutine,
    Any,
    AsyncIterable,
    Protocol,
    AsyncIterator,
    NewType,
    NamedTuple,
    NoReturn,
)

from loguru import logger as default_logger

from myas import queue_to_async_iterator
from myas.utils.misc import wait_all_simultaneously, NegativeAwaitableEvent

_InputT = TypeVar("_InputT")
_OutputT = TypeVar("_OutputT")

_T_contra = TypeVar("_T_contra", contravariant=True)
_T_co = TypeVar("_T_co", covariant=True)

_SentinelT = NewType("_SentinelT", object)
_NoMoreItems = _SentinelT(object())

logger = default_logger.bind(name=__name__)
# logger.remove()


class SentinelEnum(Enum):
    NoMoreItems = object()


class AcceptsInput(Protocol[_T_contra]):
    async def put(self, item: _T_contra) -> None:
        ...


class WorkerStats(NamedTuple):
    name: str
    num_items_processed: int
    created_at: datetime
    last_idle_at: datetime
    time_idle: timedelta
    is_idle: bool
    time_spent_processing: list[timedelta] = field(default_factory=list)

    @property
    def time_busy(self) -> timedelta:
        return datetime.now() - self.created_at - self.time_idle

    @property
    def mean_processing_time(self) -> timedelta:
        if not self.time_spent_processing:
            return timedelta()
        return sum(self.time_spent_processing, timedelta()) / len(self.time_spent_processing) or 1

    @property
    def processing_time_stdev(self) -> timedelta:

        if len(self.time_spent_processing) < 2:
            return timedelta()

        secs_stdev = statistics.stdev((t.total_seconds() for t in self.time_spent_processing))
        return timedelta(seconds=secs_stdev)

    @property
    def processing_rate(self) -> float:
        return self.num_items_processed / self.time_busy.total_seconds()


@dataclass
class Worker(Generic[_InputT, _OutputT]):

    name: str
    _func: Callable[[_InputT], Coroutine[Any, Any, _OutputT]]
    _on_result: Callable[[_OutputT], Coroutine[Any, Any, None]]
    _input_queue: Queue[_InputT]

    num_items_processed: int = 0
    created_at: datetime = field(default_factory=datetime.now)
    last_idle_at: datetime = field(default_factory=datetime.now)
    time_idle: timedelta = timedelta(0)
    idle_event: NegativeAwaitableEvent = field(default_factory=NegativeAwaitableEvent)
    task: Task[NoReturn] = field(init=False)

    time_spent_processing: list[timedelta] = field(default_factory=list)

    async def run(self) -> NoReturn:
        task = asyncio.current_task()
        if task is None:
            raise RuntimeError("No current task")
        self.task = task

        logger.debug(f"Starting worker - {self.name}")
        asyncio.create_task(self._track_idle_time())

        while True:
            if self._input_queue.empty():
                self.idle_event.set()
                logger.debug(f"Input queue empty - worker {self.name} waiting for input")
                item = await self._input_queue.get()
            else:
                item = self._input_queue.get_nowait()

            logger.debug(f"{self.name} got item from input queue - {item}")
            self.idle_event.clear()
            try:
                start = datetime.now()
                result = await self._func(item)
                self.time_spent_processing.append(datetime.now() - start)
            except Exception as exc:
                logger.error(f"Error in worker - {exc!r}")
                # todo - handle errors
            else:
                await self._on_result(result)
            finally:
                self.num_items_processed += 1
                self._input_queue.task_done()

    async def _track_idle_time(self) -> NoReturn:
        while True:
            await self.idle_event.wait()
            self.last_idle_at = datetime.now()
            await self.idle_event.wait_clear()
            self.time_idle += datetime.now() - self.last_idle_at

    @property
    def time_busy(self) -> timedelta:
        return datetime.now() - self.created_at - self.time_idle

    def get_stats(self) -> WorkerStats:
        return WorkerStats(
            name=self.name,
            num_items_processed=self.num_items_processed,
            created_at=self.created_at,
            last_idle_at=self.last_idle_at,
            time_idle=self.time_idle,
            is_idle=self.idle_event.is_set(),
            time_spent_processing=self.time_spent_processing,
        )


class WorkerManagerStatus(NamedTuple):
    worker_stats: tuple[WorkerStats, ...]
    num_items_in_queue: int
    queue_capacity: int

    @property
    def num_workers(self) -> int:
        return len(self.worker_stats)

    @property
    def num_idle_workers(self) -> int:
        return sum(1 for stat in self.worker_stats if stat.is_idle)

    @property
    def num_active_workers(self) -> int:
        return len(self.worker_stats) - self.num_idle_workers

    @property
    def idle_time_percent(self) -> float:
        total_idle = sum([stat.time_idle for stat in self.worker_stats], timedelta(0))
        total_time = sum([stat.time_busy for stat in self.worker_stats], timedelta(0)) + total_idle
        return 100 * total_idle / total_time

    @property
    def num_items_processed(self) -> int:
        return sum(stat.num_items_processed for stat in self.worker_stats)


class WorkerManagerBase(Generic[_InputT, _OutputT], AcceptsInput[_InputT]):
    def __init__(
        self,
        function: Callable[[_InputT], Coroutine[Any, Any, _OutputT]],
        n_workers: int = 5,
        start_immediately: bool = True,
    ) -> None:
        self._function = function
        self._n_workers = n_workers

        self._source_available_event = NegativeAwaitableEvent()

        self._input_queue = Queue[_InputT](maxsize=50)  # todo - revert maxsize
        # todo - measure task wait time in queue, time taken to process

        self._sinks: list[Callable[[_OutputT], Coroutine[Any, Any, Any]]] = []
        self._ongoing_sink_tasks = set[asyncio.Task[Any]]()

        self._workers: list[Worker[_InputT, _OutputT]] = []

        # self._idle_worker_event: dict[asyncio.Task[None], asyncio.Event] = {}
        # self._worker_items_processed = Counter[asyncio.Task[None]]()
        # self._worker_time_idle = dict[]

        self._source_aiter_futures: dict[Future[_InputT], AsyncIterator[_InputT]] = {}

        if start_immediately:
            self.start()
            # todo - catch exception from when create_task is called before the event loop is
            #  running and display helpful message

    def add_source(self, source: Queue[_InputT] | AsyncIterable[_InputT]) -> None:
        logger.debug(f"Adding source - {source}")
        if isinstance(source, Queue):
            source = queue_to_async_iterator(source)
        if isinstance(source, AsyncIterable):
            source = aiter(source)

        if not isinstance(source, AsyncIterator):
            raise TypeError(f"Source must be an AsyncIterator (got {type(source)})")

        fut = asyncio.ensure_future(anext(source))
        self._source_aiter_futures[fut] = source
        logger.debug(f"Added source - {source}")

        self._source_available_event.set()

    def add_sink(self, sink: Callable[[_OutputT], Coroutine[Any, Any, Any]]) -> None:
        self._sinks.append(sink)

    def start(self) -> None:
        logger.debug(f"Starting up {self!r}")
        asyncio.create_task(self._queue_filler())
        self._start_workers()

    async def on_result(self, result: _OutputT) -> None:
        logger.debug(f"Got result - {result}")
        for sink in self._sinks:
            task = asyncio.create_task(sink(result))
            self._ongoing_sink_tasks.add(task)
            task.add_done_callback(self._ongoing_sink_tasks.remove)

    async def put(self, item: _InputT) -> None:
        await self._input_queue.put(item)

    async def _queue_filler(self) -> None:
        async for item in self._sources_iterator():
            logger.debug(f"Inserting item into input queue - {item}")
            await self._input_queue.put(item)

    def get_status(self) -> WorkerManagerStatus:
        return WorkerManagerStatus(
            worker_stats=tuple(worker.get_stats() for worker in self._workers),
            num_items_in_queue=self._input_queue.qsize(),
            queue_capacity=self._input_queue.maxsize,
        )

    async def _sources_iterator(self) -> AsyncIterator[_InputT]:

        while True:

            if not self._source_aiter_futures:
                logger.debug("No _aiter_futures - waiting for new source")
                self._source_available_event.clear()
                await self._source_available_event.wait()
                logger.debug("New source added, proceeding")

            done, pending = await asyncio.wait(
                self._source_aiter_futures.keys(),
                return_when=asyncio.FIRST_COMPLETED,
            )

            for done_future in done:

                if exc := done_future.exception():
                    if isinstance(exc, StopAsyncIteration):
                        self._source_aiter_futures.pop(done_future)
                        logger.debug(f"Source finished - {done_future}")
                        continue
                    else:
                        logger.error(f"Source error - {exc!r}")
                        raise exc

                future_aiter = self._source_aiter_futures.pop(done_future)
                new_future = asyncio.ensure_future(anext(future_aiter))
                self._source_aiter_futures[new_future] = future_aiter
                yield done_future.result()

    def _start_workers(self) -> None:
        if self._workers:
            raise RuntimeError("Workers already started")

        self._workers = [
            Worker(
                name=f"{self._function.__name__} worker {i}",
                _func=self._function,
                _input_queue=self._input_queue,
                _on_result=self.on_result,
            )
            for i in range(self._n_workers)
        ]
        for worker in self._workers:
            asyncio.create_task(worker.run())

    async def join(self) -> None:
        """Wait until the input queue is empty, all sources have been exhausted, and there are
        no workers currently processing items.
        """
        while True:

            input_queue_empty = self._input_queue.empty()
            sources_exhausted = not self._source_aiter_futures
            workers_idle = all(worker.idle_event.is_set() for worker in self._workers)

            if sources_exhausted and workers_idle and input_queue_empty:
                break

            futures = [
                asyncio.ensure_future(coro)
                for coro in (
                    self._input_queue.join(),
                    self._source_available_event.wait_clear(),
                    wait_all_simultaneously(*[w.idle_event for w in self._workers]),
                )
            ]

            await asyncio.wait(futures)

    async def wait_outputs(self) -> None:
        """Wait until all sinks have completed."""
        await asyncio.wait(self._ongoing_sink_tasks)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self._function.__name__})"


class WorkerManager(WorkerManagerBase[_InputT, _OutputT], AsyncIterable[_OutputT]):
    def __init__(
        self,
        function: Callable[[_InputT], Coroutine[Any, Any, _OutputT]],
        n_workers: int = 5,
        start_immediately: bool = True,
    ) -> None:
        super().__init__(function, n_workers, start_immediately)
        self._output_queue = Queue[_OutputT](maxsize=self)

        self._closed_future = asyncio.Future[None]()
        self.add_sink(self._output_queue.put)
        # todo - there's not actually any backpressure going on, as outputs are just being
        #  run in separate tasks, and not waiting for the queue.

    def __aiter__(self) -> AsyncIterator[_OutputT]:
        return queue_to_async_iterator(self._output_queue, stop_future=self._closed_future)

    async def close(self, stop_immediately: bool = False) -> None:

        logger.debug(f"Closing {self!r}")
        if not stop_immediately:
            logger.debug("Waiting for inputs to be processed")
            await self.join()
            logger.debug("Waiting for outputs queue to be processed")
            await self._output_queue.join()

        self._closed_future.set_result(None)
        for worker in self._workers:
            worker.task.cancel()
