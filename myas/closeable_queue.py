from __future__ import annotations

import asyncio
import collections
from asyncio import (
    PriorityQueue as AsyncioPriorityQueue,
    LifoQueue as AsyncioLifoQueue,
    Queue as AsyncioQueue,
    QueueEmpty,
    Task,
)
from collections.abc import Sized
from typing import (
    TypeVar,
    AsyncIterable,
    Any,
    Collection,
    TypeAlias,
    Iterable,
    Generic,
    NamedTuple,
    runtime_checkable,
    Protocol,
    Coroutine,
    Literal, Callable, overload,
)

from myas import QueueLike, CloseableQueueLike, merge_async_iterables, iter_to_aiter

_T = TypeVar("_T")

_IterableOrAsyncIterable: TypeAlias = Iterable[_T] | AsyncIterable[_T]


class QueueClosedException(Exception):
    """Raised when a queue is closed and a new item is added to it."""


class QueueExhausted(QueueEmpty):
    """Raised when a queue is closed and no more items can be retrieved."""


async def empty_async_iterable() -> AsyncIterable[_T]:
    """An empty async iterable."""
    for i in tuple[_T]():
        yield i


class Queue(AsyncioQueue[_T], Sized, Generic[_T], QueueLike[_T, _T]):
    def __init__(
        self,
        maxsize: int = 0,
        **kwargs: Any,
    ) -> None:
        super().__init__(maxsize=maxsize, **kwargs)
        self._sources: AsyncIterable[_T] = empty_async_iterable()

    async def add_source(self, *sources: _IterableOrAsyncIterable[_T], **kwargs: Any) -> None:
        """Add a source of items to the queue."""
        async for item in merge_async_iterables(
                *(
                        iter_to_aiter(source)
                        if not isinstance(source, AsyncIterable)
                        else source
                        for source in sources
                ),
        ):
            await self.put(item)

    def __len__(self) -> int:
        return self.qsize()


class PriorityQueue(AsyncioPriorityQueue[_T], Queue[_T]):
    pass


class LifoQueue(AsyncioLifoQueue[_T], Queue[_T]):
    pass


class CloseableQueue(Queue[_T], CloseableQueueLike[_T, _T]):
    """A queue that can be closed, as opposed to one that runs indefinitely.

    Calling `close` will cause subsequent calls to `put` and `put_nowait` to raise
    `QueueClosedException`.

    After the queue is closed, `get` and `get_nowait` continue to operate normally until the queue
    becomes empty. At that point, any subsequent or ongoing (i.e. currently being waited on) calls
    to `get` or `get_nowait` will raise `QueueExhausted`.

    One can wait for the queue to become exhausted by calling `wait_exhausted`, or closed by calling
    `wait_closed`.
    """

    def __init__(self, maxsize: int = 0) -> None:
        super().__init__(maxsize)
        self._ev_closed = asyncio.Event()
        self._ev_exhausted = asyncio.Event()

    def close(self) -> None:
        self._ev_closed.set()
        if self.empty():
            self._notify_exhausted()

    async def aclose(self) -> None:
        self.close()

    @property
    def is_closed(self) -> bool:
        return self._ev_closed.is_set()

    @property
    def is_exhausted(self) -> bool:
        return self._ev_exhausted.is_set()

    def is_exh_bool(self) -> bool:
        return self.is_exhausted

    iis_exhausted = property(is_exh_bool)

    async def wait_closed(self) -> None:
        await self._ev_closed.wait()

    @property
    def exhausted(self) -> Coroutine[Any, Any, Literal[True]]:
        return self._ev_exhausted.wait()

    @property
    def closed(self) -> Coroutine[Any, Any, Literal[True]]:
        return self._ev_closed.wait()

    def put_nowait(self, item: _T) -> None:
        """Put an item into the queue immediately, raising an exception if the queue is full or
        closed.

        Raises:
            QueueClosedException: If the queue is closed.
            QueueFull: If the queue is full.
        """

        if self.is_closed:
            raise QueueClosedException()
        return super().put_nowait(item)

    async def put(self, item: _T) -> None:
        """Put an item into the queue, raising an exception if the queue is closed.

        Raises:
            QueueClosedException: If the queue is closed.
        """

        if self.is_closed:
            raise QueueClosedException()
        return await super().put(item)

    def _notify_exhausted(self) -> None:
        self._ev_exhausted.set()

    async def get(self) -> _T:
        """Get an item from the queue.

        If the queue is closed and empty, or becomes so while waiting, `QueueExhausted` is raised.

        Returns:
            The item.

        Raises:
            QueueExhausted: If the queue is closed and empty, or becomes so while waiting.
        """

        if self.is_exhausted:
            raise QueueExhausted()

        exhausted_fut = asyncio.ensure_future(self._ev_exhausted.wait())
        get_fut = asyncio.ensure_future(super().get())
        done, pending = await asyncio.wait(
            (exhausted_fut, get_fut),
            return_when=asyncio.FIRST_COMPLETED,
        )

        if exhausted_fut not in done:
            exhausted_fut.cancel()

        if get_fut not in done:
            get_fut.cancel()

        elif self.is_closed and self.empty():
            asyncio.get_running_loop().call_soon(self._notify_exhausted)

        if get_fut in done:
            return get_fut.result()
        else:
            raise QueueExhausted()

    def get_nowait(self) -> _T:
        """Get an item from the queue immediately, raising an exception if no items are available.

        Returns:
            The item.

        Raises:
            QueueEmpty: If the queue is empty, but not exhausted (closed + empty).
            QueueExhausted: If the queue is closed and empty.
        """

        if self.is_exhausted:
            raise QueueExhausted()
        result = super().get_nowait()
        if self.is_closed and self.empty():
            asyncio.get_running_loop().call_soon(self._notify_exhausted)
        return result

    async def add_source(
        self,
        *sources: _IterableOrAsyncIterable[_T],
        close_when_done: bool = False,
        **kwargs: Any,
    ) -> None:
        await super().add_source(*sources, **kwargs)
        if close_when_done:
            self.close()


class CloseablePriorityQueue(CloseableQueue[_T], PriorityQueue[_T]):
    """A priority queue that can be closed, as opposed to one that runs indefinitely.

    See `CloseableQueue` for details.
    """


class CloseableLifoQueue(CloseableQueue[_T], asyncio.LifoQueue[_T]):
    """A LIFO queue that can be closed, as opposed to one that runs indefinitely.

    See `CloseableQueue` for details.
    """



async def queue_to_async_iterator(queue: Queue[T]) -> AsyncIterator[T]:
    """Iterate over the items in a queue.

    For ordinary asyncio.Queue objects, iteration will continue indefinitely. For CloseableQueue
    objects, iteration will stop when the queue is closed.

    Args:
        queue: The queue to iterate over.

    Yields:
        The items in the queue.
    """
    while True:
        try:
            res = await queue.get()
            queue.task_done()
            yield res
        except QueueExhausted:
            break

_U = TypeVar("_U")

@overload
async def async_iterable_to_queue(  # type: ignore
    async_iterable: AsyncIterable[_T],
    queue: CloseableQueueLike[_T, _U],
    close_on_finished: Literal[True] = ...,
) -> None:
    ...


@overload
async def async_iterable_to_queue(
    async_iterable: AsyncIterable[_T],
    queue: Queue[T],
    close_on_finished: Literal[True] = ...,
) -> NoReturn:
    ...


@overload
async def async_iterable_to_queue(
    async_iterable: AsyncIterable[T],
    queue: QueueLike[_T, _U],
    close_on_finished: bool = ...,
) -> None:
    ...


async def async_iterable_to_queue(
    async_iterable: AsyncIterable[_T],
    queue: QueueLike[_T, _U],
    close_on_finished: bool = False,
) -> None:
    """Iterate over an async iterable and put the items in a queue.

    If `close_on_finished` is True and the queue is a CloseableQueue (or a subclass of it), the
    queue will be closed when the iteration is finished.

    Args:
        async_iterable: The async iterable to iterate over.
        queue: The queue to put the items in.
        close_on_finished: Whether to close the queue when the iteration is finished.

    Raises:
        TypeError: If `close_on_finished` is True and the queue is not a CloseableQueue.
    """
    if close_on_finished and not isinstance(queue, CloseableQueue):
        raise TypeError("Cannot close a non-CloseableQueue")
    if isinstance(queue, CloseableQueue) and queue.is_closed:
        raise QueueClosedException("Cannot put items in a closed queue")

    async for item in async_iterable:
        await queue.put(item)
    print("Finished iteration")
    if close_on_finished and isinstance(queue, CloseableQueue):
        queue.close()


__all__: tuple[str, ...] = (
    "Queue",
    "PriorityQueue",
    "LifoQueue",
    "CloseableQueue",
    "CloseablePriorityQueue",
    "CloseableLifoQueue",
    "QueueClosedException",
    "QueueExhausted",
)
