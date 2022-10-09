from __future__ import annotations

import asyncio
from asyncio import Future, Task
from collections import defaultdict
from typing import (
    Generic,
    TypeVar,
    AsyncIterable,
    Iterable,
    AsyncIterator,
    Callable,
    Hashable,
    Coroutine,
    Any,
    TypeAlias,
    Sequence,
)

from loguru import logger

from myas import merge_async_iterators, iter_to_aiter, ensure_coroutine

"""

API ideas:

stream = ait(a, b, range(10)) | filter(it > 5) | map(it * 2) | wpool(5, get)
stream = ait(a, b, range(10)) | (filter, it > 5) | (map, it * 2) | wpool(get, workers=5)

a << arange(10)

async for i in stream:
    ...

async for e in stream.exceptions:
    ...

res = await stream.gather()
exc = await stream.exceptions.gather()

@wpool(pool_size=5, queue_capacity=30)
async def get(url):
    ...

wpool(to_thread=True, to_process=True)
wpool_to_thread()
wpool_to_process()

@wpool(on_exception=do_on_exc)
async def get(url):
    ...
    
async def do_on_exc(item, exc, context: PoolExecutionContext):
    if context.attempts == 1:
        context.retry()
    else:
        logger.error(f"Failed to get {item} after {context.attempts} attempts")

stream2 = stream | wpool((aget, aparse, it * 5, adoit(2, X, 6), lambda: adothat), workers=5)
"""

_InputT = TypeVar("_InputT")
_OutputT = TypeVar("_OutputT")
_T_co = TypeVar("_T_co", covariant=True)
_KeyT = TypeVar("_KeyT", bound=Hashable)

_T = TypeVar("_T")


def merge_iter_aiter(*its_aits: AsyncIterable[_T] | Iterable[_T]) -> AsyncIterator[_T]:
    """Merge multiple (optionally async) iterables into a single async iterable."""
    if not its_aits:
        return iter_to_aiter(())
    aits = [iter_to_aiter(other) if isinstance(other, Iterable) else other for other in its_aits]
    return merge_async_iterators(*aits)


class AsyncStream(Generic[_InputT], AsyncIterable[_InputT]):

    _aiterable: AsyncIterable[_InputT]

    def __init__(self, *aiterables: AsyncIterable[_InputT] | Iterable[_InputT]) -> None:
        self._aiterable = merge_iter_aiter(*aiterables)

    def __aiter__(self) -> AsyncIterator[_InputT]:
        return aiter(self._aiterable)

    def merge(self, *others: AsyncIterable[_InputT] | Iterable[_InputT]) -> AsyncStream[_InputT]:
        self._aiterable = merge_iter_aiter(self._aiterable, *others)
        return self

    async def gather(self) -> list[_InputT]:
        return [item async for item in self._aiterable]

    async def gather_set(self) -> set[_InputT]:
        return {item async for item in self._aiterable}

    async def gather_grouped(self, key: Callable[[_InputT], _KeyT]) -> dict[_KeyT, list[_InputT]]:
        grouped = defaultdict(list)
        async for item in self._aiterable:
            grouped[key(item)].append(item)
        return grouped

    #####

    def amap(
        self,
        func: Callable[[_InputT], Coroutine[Any, Any, _OutputT]] | Callable[[_InputT], _OutputT],
    ) -> AsyncStream[_OutputT]:
        _func = ensure_coroutine(func)

        async def _amap() -> AsyncIterator[_OutputT]:
            async for item in self:
                yield await _func(item)

        return AsyncStream(_amap())

    def afilter(
        self, func: Callable[[_InputT], Coroutine[Any, Any, bool]] | Callable[[_InputT], bool]
    ) -> AsyncStream[_InputT]:
        _func = ensure_coroutine(func)

        async def _afilter() -> AsyncIterator[_InputT]:
            async for item in self:
                if await _func(item):
                    yield item

        return AsyncStream(_afilter())

    def flat_map(
        self, func: Callable[[_InputT], AsyncIterable[_OutputT] | Iterable[_OutputT]]
    ) -> AsyncStream[_OutputT]:
        async def _flat_map() -> AsyncIterator[_OutputT]:
            async for item in self.amap(func):
                if isinstance(item, AsyncIterable):
                    async for subitem in item:
                        yield subitem
                elif isinstance(item, Iterable):
                    for subitem in item:
                        yield subitem
                else:
                    raise TypeError(
                        f"flat_map expected AsyncIterable or Iterable, got {type(item)}"
                    )

        return AsyncStream(_flat_map())

    async def group_streams(
        self,
        *keys: _KeyT,
        key_func: Callable[[_InputT], Coroutine[Any, Any, _KeyT]] | Callable[[_InputT], _KeyT],
    ) -> dict[_KeyT, AsyncStream[_InputT]]:
        _key_func = ensure_coroutine(key_func)

        key_streams: dict[_KeyT, AsyncStream[_InputT]] = {key: AsyncStream() for key in keys}

        async def _grouper() -> None:
            async for item in self:
                key = await _key_func(item)
                if key not in key_streams:
                    raise ValueError(f"Unexpected key {key}")
                key_streams[key].merge((item,))

        asyncio.create_task(_grouper())
        return key_streams

    # async def split_stream(self, predicate):


# FutureAndResultB: TypeAlias = tuple[asyncio.Future["FutureAndResultB[_T]"], _T]
# FutureAndResult: TypeAlias = FutureAndResultB[FutureAndResultB[_T]]

FutB = Future[tuple[_T, "FutB[_T]"]]
Nested = Sequence[_T | "Nested[_T]"]


class M(Generic[_T], AsyncIterator[_T]):
    def __init__(self, initial_future: FutureAndResult[_T]) -> None:
        self._future = initial_future

    async def __anext__(self) -> _T:
        next_future, item = self._future

        if not next_future.done():
            await next_future

        self._future = next_future
        return item


class N(Generic[_T], AsyncIterator[_T]):
    def __init__(self, copier_task: Task[None]) -> None:
        self._future = asyncio.Future[_T]()
        self.copier_task = copier_task

    def asend(self, value: _T) -> None:
        self._future.set_result(value)

    async def __anext__(self) -> _T:
        await self._future

        if self._future.exception():
            # could be a StopAsyncIteration
            raise self._future.exception()

        return self._future.result()


async def _copied_aiter(future: FutB[_InputT]) -> AsyncIterator[_InputT]:
    while True:
        try:
            await future
            if future.exception():
                raise future.exception()
            item, future = future.result()
            yield item
        except StopAsyncIteration:
            break


async def copy_async_iterable(
    ait: AsyncIterable[_InputT], n_copies: int
) -> tuple[AsyncIterator[_InputT], ...]:
    """Copy an async iterable into a new async iterator."""

    copy_task = asyncio.current_task()
    if copy_task is None:
        raise RuntimeError("copy_async_iterable must be called from an async context")

    aits = [N(copy_task) for _ in range(n_copies)]

    async def _copier() -> None:
        async for item in ait:
            for ait in aits:
                ait.asend(item)

    _copier.task = asyncio.create_task(_copier())
    task = asyncio.create_task(_copier())

    return _copied_aiter(future)


class MultiConsumerAsyncStream(AsyncStream[_InputT]):
    def __init__(self, *aiterables: AsyncIterable[_InputT] | Iterable[_InputT]):
        super().__init__(*aiterables)
        self._pending_watcher_notifications = []

        self._self_aiterable = aiter(self._aiterable)

    async def _copied_stream(self, future: FutureAndResult[_InputT]) -> AsyncIterator[_InputT]:
        while True:
            try:
                next_future, item = future
                yield item
                future = await next_future
            except StopAsyncIteration:
                break

    async def _copy_stream(self, stream: AsyncStream[_InputT]) -> None:
        async for item in stream:
            yield item

    async def _notify_watchers(self, item):
        for watcher in self._pending_watcher_notifications:
            watcher.set_result(item)
        self._pending_watcher_notifications = []

    def __aiter__(self):
        if not self._self_aiterable:
            self._self_aiterable = self._aiterable
        return self

    # def __anext__(self):
