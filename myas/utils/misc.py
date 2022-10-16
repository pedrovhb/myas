import asyncio
from typing import Literal


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


def crt_task_or_raise() -> asyncio.Task:
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
