from __future__ import annotations

import asyncio
import inspect
from typing import Callable, Coroutine, Any, cast, overload, TypeVar, ParamSpec

T = TypeVar("T")
P = ParamSpec("P")


async def wait_all_simultaneously(*evs: asyncio.Event) -> None:
    """Wait for all events to be set simultaneously.

    This is different from `asyncio.wait(evs)` in that it will wait for all events to be set
    simultaneously. In contrast, `asyncio.wait(evs)` will wait for each event to be have been set
    once, but not necessarily simultaneously - it will still return if the final event is set
    but others have been cleared meanwhile.

    Args:
        evs: The events to wait for.
    """
    while True:
        await asyncio.wait([ev.wait() for ev in evs])
        if all(ev.is_set() for ev in evs):
            break


def crt_task_or_raise() -> asyncio.Task[T]:
    """Return the current task or raise an exception if it is not a task.

    Returns:
        The current task.
    """
    task = asyncio.current_task()
    if task is None:
        raise RuntimeError("Not running in an asyncio task")
    return task


class NegativeAwaitableEvent(asyncio.Event):
    """An event that can be waited on for either set or clear states."""

    def __init__(self) -> None:
        super().__init__()
        self._negative_event = asyncio.Event()
        self._negative_event.set()

    def clear(self) -> None:
        super().clear()
        self._negative_event.set()

    async def wait_clear(self) -> None:
        await self._negative_event.wait()

    def set(self) -> None:
        super().set()
        self._negative_event.clear()


__all__ = (
    "wait_all_simultaneously",
    "crt_task_or_raise",
    "NegativeAwaitableEvent",
)


@overload
def ensure_coroutine(
    fn: Callable[P, Coroutine[Any, Any, T]],
) -> Callable[P, Coroutine[Any, Any, T]]:
    ...


@overload
def ensure_coroutine(
    fn: Callable[P, T],
) -> Callable[P, Coroutine[Any, Any, T]]:
    ...


def ensure_coroutine(
    fn: Callable[P, T] | Callable[P, Coroutine[Any, Any, T]],
) -> Callable[P, Coroutine[Any, Any, T]]:
    """Ensure that a function is a coroutine (i.e. async).

    If the given callable is not a coroutine, it will be wrapped in a coroutine that calls the
    function and returns the result. Otherwise, the function is returned as-is.

    Args:
        fn: The function to ensure is a coroutine.

    Returns:
        The function as a coroutine.
    """

    if inspect.iscoroutinefunction(fn):
        _fn = cast(Callable[P, Coroutine[Any, Any, T]], fn)
        return _fn

    _fn_sync = cast(Callable[P, T], fn)

    async def _as_async(*args: P.args, **kwargs: P.kwargs) -> T:
        result = _fn_sync(*args, **kwargs)
        return result

    # return cast(Callable[P, Coroutine[Any, Any, _OutputT]], _as_async)
    return _as_async
