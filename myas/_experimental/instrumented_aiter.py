from __future__ import annotations

import asyncio
import time
from typing import AsyncIterable, AsyncIterator, Generic, TypeVar

_T = TypeVar("_T")


class InstrumentedAsyncIterator(Generic[_T], AsyncIterator[_T]):
    """An async iterator that can be instrumented to track the number of items it has yielded.

    This class is intended to be used as a base class for other async iterators that need to
    track the number of items they have yielded. It is not intended to be used directly.
    """

    def __init__(self, async_iterable: AsyncIterable[_T]) -> None:
        if isinstance(async_iterable, AsyncIterable):
            self._async_iterator = async_iterable.__aiter__()
        elif isinstance(async_iterable, AsyncIterator):
            self._async_iterator = async_iterable
        else:
            raise TypeError(
                f"async_iterable must be an AsyncIterable "
                f"or AsyncIterator, not {type(async_iterable)}"
            )

        self._num_yielded = 0
        self._yielded_timestamps = []

    @classmethod
    def instrument(cls, async_iterable: AsyncIterable[_T]) -> InstrumentedAsyncIterator[_T]:
        """Return an instrumented version of the given async iterable."""
        return cls(async_iterable)

    @property
    def num_yielded(self) -> int:
        """Return the number of items that have been yielded."""
        return self._num_yielded

    @property
    def rate(self) -> float:
        """Return the average rate of items yielded per second."""
        if not self._yielded_timestamps:
            return 0.0
        return self._num_yielded / (time.monotonic() - self._yielded_timestamps[0])

    def rate_of_last(self, num_items: int) -> float:
        """Return the rate of the given number of items per second."""
        relevant_timestamps = self._yielded_timestamps[-num_items:]
        if len(relevant_timestamps) < 2:
            return 0.0
        return len(relevant_timestamps) / (relevant_timestamps[-1] - relevant_timestamps[0])

    def rate_in(self, seconds: float) -> float:
        """Return the rate of items yielded in the given number of seconds."""
        if not self._yielded_timestamps:
            return 0.0
        t = time.monotonic()
        relevant_timestamps = [ts for ts in self._yielded_timestamps if ts >= t - seconds]
        return len(relevant_timestamps) / seconds

    async def __anext__(self) -> _T:
        """Return the next item."""
        result = await self._async_iterator.__anext__()
        self._num_yielded += 1
        self._yielded_timestamps.append(time.monotonic())
        return result


class InstrumentedAsyncIterable(AsyncIterable[_T]):
    """An async iterable that can be instrumented to track the number of items it has yielded.

    This class is intended to be used as a base class for other async iterables that need to
    track the number of items they have yielded. It is not intended to be used directly.
    """

    def __aiter__(self) -> InstrumentedAsyncIterator[_T]:
        """Return an async iterator."""
        return InstrumentedAsyncIterator(aiter(self))


if __name__ == "__main__":
    from asyncio import run
    from itertools import count

    async def acount() -> AsyncIterator[int]:
        for i in count():
            yield i
            await asyncio.sleep(0.1 * i)

    async def main():
        async for i in (it := InstrumentedAsyncIterator.instrument(acount())):
            print(i)
            print(f"{it.rate=}")
            print(f"{it.rate_of_last(min(i, 10))=}")
            print(f"{it.rate_in(0.5)=}")
            if i > 10:
                break

    run(main())
