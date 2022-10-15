from __future__ import annotations

import asyncio
import functools
from asyncio import Future, Queue, Task
from typing import (
    Any,
    AsyncIterable,
    AsyncIterator,
    Callable,
    Coroutine,
    Iterable,
    ParamSpec,
    TypeVar,
    TypeAlias,
    cast,
    TypeGuard,
    NamedTuple,
    Generic,
    Iterator, Type, overload, Literal, Annotated,
)

from mypy.types import LiteralType

from .compose import ensure_coroutine

T = TypeVar("T")
U = TypeVar("U")

_TrueT = TypeVar("_TrueT")
_FalseT = TypeVar("_FalseT")

P = ParamSpec("P")


class _SentinelType:
    """An object representing a sentinel command."""


_NoStopSentinel = _SentinelType()
StopQueueIteration = _SentinelType()


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


async def merge_async_iterables(*async_iterables: AsyncIterable[T]) -> AsyncIterator[T]:
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


async def queue_to_async_iterator(
    queue: Queue[T],
    stop_future: Future[None] | None = None,
) -> AsyncIterator[T]:
    """Iterate over the items in a queue.

    Optionally, a stop future can be provided. If the future is cancelled, the iteration will
    stop. If the future is not provided, the iteration will continue indefinitely.

    Args:
        queue: The queue to iterate over.
        stop_future: If this future is done, the iteration will stop.

    Yields:
        The items in the queue.
    """
    while True:

        get_item_task: Future[T] = asyncio.ensure_future(queue.get())

        futs: tuple[Future[T]] | tuple[Future[T], Future[None]]
        futs = (get_item_task,) if stop_future is None else (get_item_task, stop_future)

        done, pending = await asyncio.wait(futs, return_when=asyncio.FIRST_COMPLETED)
        if stop_future in done:
            break

        item = get_item_task.result()
        queue.task_done()
        yield item


async def async_iterable_to_queue(
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


async def gather_async_iterables(*async_iterables: AsyncIterable[T]) -> list[T]:
    """Gather the results from multiple async iterators.

    This is useful for when you want to gather the results from multiple async iterables
    without having to iterate over them.

    Args:
        async_iterables: The async iterables to gather the results from.

    Returns:
        A list of the results from the async iterables in the order in which they were completed.
    """
    return [item async for item in merge_async_iterables(*async_iterables)]


ItemAndNextFuture: TypeAlias = Future[tuple[T, "ItemAndNextFuture[T]"]]


async def _cloned_aiter(future: ItemAndNextFuture[T]) -> AsyncIterator[T]:
    """Helper function for `clone_async_iterator`."""
    while True:
        try:
            item, future = await future
            yield item
        except asyncio.CancelledError:
            return


def clone_async_iterable(source: AsyncIterable[T], n_clones: int) -> tuple[AsyncIterator[T], ...]:
    """Create n clones of an async iterable, each receiving every item generated by the source.

    Note that this is not a true copy, as the source iterable is not duplicated. Instead, each
    copy is a separate iterator that receives the same items from the source iterable. The elements
    are not copied, so if a list is yielded from the source iterable, each copy will receive the
    same list object.

    Objects yielded from the source iterable will also stay in memory until all copies have
    finished iterating over them. This can cause memory issues in a long-running program if the
    copied async iterators are not consumed at the same rate as the source iterable.
      Todo - Perhaps use weak references to allow the source iterable to be garbage collected

    Args:
        source: The source iterable to copy.
        n_clones: The number of copies to create.

    Returns:
        A tuple of async iterators, each receiving the same items from the source iterable.
    """
    futures = [ItemAndNextFuture[T]() for _ in range(n_clones)]
    stop_fut = Future[None]()

    async def _copier() -> None:
        futs = futures

        async for item in source:

            new_futs: list[ItemAndNextFuture[T]] = []
            for fut in futs:
                next_fut = ItemAndNextFuture[T]()
                new_futs.append(next_fut)
                fut.set_result((item, next_fut))
            futs = new_futs

        for fut in futs:
            fut.cancel(StopAsyncIteration)

    asyncio.create_task(_copier())
    copied = tuple(_cloned_aiter(fut) for fut in futures)
    return copied


async def map_async_iterable(
    iterable: AsyncIterable[T],
    func: Callable[[T], Coroutine[Any, Any, U]] | Callable[[T], U],
) -> AsyncIterator[U]:
    """Map a function over an async iterable.

    Args:
        iterable: The async iterable to map the function over.
        func: The function to map over the async iterable.

    Yields:
        The results of the function applied to each item in the async iterable.
    """
    _fn = ensure_coroutine(func)
    async for item in iterable:
        result = await _fn(item)
        yield cast(U, result)


async def filter_async_iterable(
    iterable: AsyncIterable[T],
    func: Callable[[T], Coroutine[Any, Any, bool]] | Callable[[T], bool],
) -> AsyncIterator[T]:
    """Filter an async iterable using a predicate.

    Args:
        iterable: The async iterable to filter.
        func: The predicate to filter the async iterable with.

    Yields:
        The items in the async iterable for which the predicate returned True.
    """
    _fn = ensure_coroutine(func)
    async for item in iterable:
        if await _fn(item):
            yield item


def split_async_iterable(
    iterable: AsyncIterable[T],
    predicate: Callable[[T], Coroutine[Any, Any, bool]] | Callable[[T], bool],
) -> tuple[AsyncIterator[Annotated[T, _TrueT]], AsyncIterator[Annotated[T, _FalseT]]]:
    """Split an async iterable into two based on a predicate.

    Args:
        iterable: The async iterable to split.
        predicate: The predicate to split the async iterable with.

    Returns:
        A tuple of two async iterators. The first iterator contains the items for which the
        predicate returned True, and the second iterator contains the items for which the
        predicate returned False.
    """
    future_true = ItemAndNextFuture[T]()
    future_false = ItemAndNextFuture[T]()

    async def _splitter() -> None:
        nonlocal future_true, future_false

        _fn = cast(Callable[[T], Coroutine[Any, Any, bool]], ensure_coroutine(predicate))
        async for item in iterable:
            if await _fn(item):
                next_future_true = ItemAndNextFuture[T]()
                future_true.set_result((item, next_future_true))
                future_true = next_future_true
            else:
                next_future_false = ItemAndNextFuture[T]()
                future_false.set_result((item, next_future_false))
                future_false = next_future_false

        future_true.cancel(StopAsyncIteration)
        future_false.cancel(StopAsyncIteration)

    asyncio.create_task(_splitter())
    _cloned_true = _cloned_aiter(future_true)
    _cloned_false = _cloned_aiter(future_false)
    # todo - would love to have this work as a type guard, not sure if possible
    return _cloned_true, _cloned_false

__all__ = (
    "_SentinelType",
    "_NoStopSentinel",
    "StopQueueIteration",
    "run_sync",
    "run_on_thread",
    "iter_to_aiter",
    "merge_async_iterables",
    "gather_async_iterables",
    "clone_async_iterable",
    "map_async_iterable",
    "filter_async_iterable",
    "split_async_iterable",
    "queue_to_async_iterator",
    "async_iterable_to_queue",
)
