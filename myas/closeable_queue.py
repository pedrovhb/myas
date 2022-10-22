import asyncio
from asyncio import Queue, Task, PriorityQueue, Future, mixins, AbstractEventLoop
from typing import TypeVar

import myas

_T = TypeVar("_T")


class QueueClosedException(Exception):
    """Raised when a queue is closed and a new item is added to it."""


class QueueExhausted(Exception):
    """Raised when a queue is closed and no more items can be retrieved."""


class CloseableQueue(Queue[_T]):
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

    @property
    def is_closed(self) -> bool:
        return self._ev_closed.is_set()

    @property
    def is_exhausted(self) -> bool:
        return self._ev_exhausted.is_set()

    async def wait_closed(self) -> None:
        await self._ev_closed.wait()

    async def wait_exhausted(self) -> None:
        await self._ev_exhausted.wait()

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


class CloseablePriorityQueue(CloseableQueue[_T], PriorityQueue[_T]):
    """A priority queue that can be closed, as opposed to one that runs indefinitely.

    See `CloseableQueue` for details.
    """


class CloseableLifoQueue(CloseableQueue[_T], asyncio.LifoQueue[_T]):
    """A LIFO queue that can be closed, as opposed to one that runs indefinitely.

    See `CloseableQueue` for details.
    """


__all__ = (
    "CloseableQueue",
    "CloseablePriorityQueue",
    "CloseableLifoQueue",
    "QueueClosedException",
    "QueueExhausted",
)
