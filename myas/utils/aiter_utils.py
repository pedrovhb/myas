from __future__ import annotations

import asyncio
import functools
from asyncio import Queue, Future
from typing import (
    Callable,
    Coroutine,
    Any,
    ParamSpec,
    TypeVar,
    Iterable,
    AsyncIterator,
    AsyncIterable,
    NewType,
)

T = TypeVar("T")
P = ParamSpec("P")

_SentinelType = NewType("_SentinelType", object)
_NoStopSentinel = _SentinelType(object())
StopQueueIteration = _SentinelType(object())


def run_sync(f: Callable[P, Coroutine[Any, Any, T]]) -> Callable[P, T]:
    """Given a function, return a new function that runs the original one with asyncio.

    This can be used to transparently wrap asynchronous functions. It can be used for example to
    use an asynchronous function as an entry point to a `Typer` CLI.

    Args:
        f: The function to run synchronously.

    Returns:
        A new function that runs the original one with `asyncio.run`.
    """

    @functools.wraps(f)
    def decorated(*args: P.args, **kwargs: P.kwargs) -> T:
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(f(*args, **kwargs))

    return decorated


def run_on_thread(
    func: Callable[P, T],
) -> Callable[P, Coroutine[Any, Any, T]]:
    """Run a function on a separate thread using the default executor.

    Args:
        func: The function to run on a thread.

    Returns:
        The return value of the function.
    """

    @functools.wraps(func)
    async def _inner(*args: P.args, **kwargs: P.kwargs) -> T:
        return await asyncio.to_thread(func, *args, **kwargs)

    return _inner


async def iter_to_aiter(iterable: Iterable[T]) -> AsyncIterator[T]:
    """Convert an iterable to an async iterable.

    Note that no work is delegated to threads, so blocking operations will still block the event
    loop. This merely allows for the use of `async for` on an iterable and inserts an async sleep
    between iterations to allow other tasks to run. To run the iterable on a thread, use
    `run_on_thread` to decorate a function, or `asyncio.to_thread` to submit it to the default
    executor.

    Args:
        iterable: The iterable to convert.

    Returns:
        An async iterable that yields the items from the iterable.
    """
    for item in iterable:
        yield item
        await asyncio.sleep(0)


async def merge_async_iterators(*async_iterables: AsyncIterable[T]) -> AsyncIterator[T]:
    """Merge multiple async iterables into a single iterator.

    We do this by creating a dict of `anext` futures to their iterators, and updating the map
    to remove the future from the keys and replace it with the next `anext` future for each
    result as it comes in. When the future carries a `StopAsyncIteration` exception, we remove
    the iterator from the map and continue. When the map is empty, we are done.

    Args:
        async_iterables: The async iterables to merge.

    Yields:
        The next item from the async iterables.
    """
    fut_to_aiter = dict[Future[T], AsyncIterator[T]]()

    for async_iterable in async_iterables:
        async_iterator = aiter(async_iterable)
        fut = asyncio.ensure_future(anext(async_iterator))
        fut_to_aiter[fut] = async_iterator

    while fut_to_aiter:
        done, pending = await asyncio.wait(
            fut_to_aiter.keys(),
            return_when=asyncio.FIRST_COMPLETED,
        )

        for done_future in done:

            if exc := done_future.exception():
                if isinstance(exc, StopAsyncIteration):
                    fut_to_aiter.pop(done_future)
                    continue
                else:
                    raise exc

            future_aiter = fut_to_aiter.pop(done_future)
            new_future = asyncio.ensure_future(anext(future_aiter))
            fut_to_aiter[new_future] = future_aiter
            yield done_future.result()


async def queue_to_aiter(
    queue: Queue[T],
    stop_sentinel: T | _SentinelType = StopQueueIteration,
) -> AsyncIterator[T]:
    """Iterate over the items in a queue.

    Optionally, a stop sentinel can be provided. If the sentinel is encountered, the iteration
    will stop. If the sentinel is not provided, the iteration will continue indefinitely.

    Args:
        queue: The queue to iterate over.
        stop_sentinel: If this object is received from the queue, the iteration will stop. The
            _NoStopSentinel object is used to indicate that the iteration should continue
            indefinitely.

    Yields:
        The items in the queue.
    """
    while True:
        item = await queue.get()
        queue.task_done()
        if item is stop_sentinel and stop_sentinel is not _NoStopSentinel:
            return
        yield item


async def aiter_to_queue(
    async_iterable: AsyncIterable[T],
    queue: Queue[T | _SentinelType],
    stop_sentinel: T | _SentinelType = _NoStopSentinel,
) -> None:
    """Iterate over an async iterable and put the items in a queue.

    Optionally, a stop sentinel can be provided. If the sentinel is encountered, the iteration
    will stop. If the sentinel is not provided, the iteration will continue indefinitely.

    Args:
        async_iterable: The async iterable to iterate over.
        queue: The queue to put the items in.
        stop_sentinel: If this object is received from the queue, the iteration will stop. The
            _NoStopSentinel object is used to indicate that the iteration should continue
            indefinitely.
    """
    async for item in async_iterable:
        await queue.put(item)
    if stop_sentinel is not _NoStopSentinel:
        await queue.put(stop_sentinel)


async def gather_async_iterators(*async_iterables: AsyncIterator[T]) -> list[T]:
    """Gather the results from multiple async iterators.

    This is useful for when you want to gather the results from multiple async iterables
    without having to iterate over them.

    Args:
        async_iterables: The async iterables to gather the results from.

    Returns:
        A list of the results from the async iterables in the order in which they were completed.
    """
    return [item async for item in merge_async_iterators(*async_iterables)]
