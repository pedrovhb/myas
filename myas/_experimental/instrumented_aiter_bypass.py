from __future__ import annotations

import asyncio
import math
import statistics
import time
from datetime import datetime, timedelta
from typing import AsyncIterator, TypeVar, AsyncIterable, ClassVar
from array import array

from rich.console import Console
from rich.table import Table
from rich.live import Live
from rich.panel import Panel
from humanize import naturaldelta as nat_delta, naturalsize as nat_size, naturaltime as nat_time

_T = TypeVar("_T")


class SentinelT:

    _names: ClassVar[dict[str, SentinelT]] = {}

    def __new__(cls, sentinel_name: str):
        if sentinel_name in cls._names:
            return cls._names[sentinel_name]
        self = super().__new__(cls)
        self._name = sentinel_name
        cls._names[sentinel_name] = self
        return self

    def __repr__(self):
        return f"<SentinelT({self._name})>"

    __str__ = __repr__


NoItemsSentinel = SentinelT("NoItemsSentinel")


class TimeKeeper:
    def __init__(self):
        self._start_time = time.time()
        self._timestamps = array("d")

    def add_timestamp(self, timestamp: float | datetime | None = None) -> None:
        """Add a timestamp to the list of timestamps.

        Args:
            timestamp: The timestamp to add. If `None`, the current time will be used.
        """
        if timestamp is None:
            timestamp = time.time()
        elif isinstance(timestamp, datetime):
            timestamp = timestamp.timestamp()
        self._timestamps.append(timestamp)

    @property
    def timestamps_datetime(self) -> list[datetime]:
        """Return `datetime` objects representing the time of added records.

        Returns:
            A list of `datetime` objects representing the time of added records.
        """
        return [datetime.fromtimestamp(ts) for ts in self._timestamps]

    @property
    def timestamps_since_start(self) -> list[float]:
        return [ts - self._start_time for ts in self._timestamps]

    def mean_rate(self) -> float | None:
        """Return the mean rate of records since creation of the object.

        Returns:
            The mean rate of records since creation of the object, or `None` if there are not enough
            records to calculate the mean rate.
        """
        if not self._timestamps:
            return None
        if self._timestamps[-1] == self._start_time:
            return math.inf
        return len(self._timestamps) / (self._timestamps[-1] - self._start_time)

    def rate_of_last(self, num_items: int) -> float | None:
        """Return the rate of records within the timespace of the last `num_items` iterations.

        Args:
            num_items: The number of items to consider.

        Returns:
            The rate of iteration within the timespace of the last `num_items` iterations.
        """
        try:
            relevant_timestamps = self._timestamps[-num_items:]
        except IndexError:
            return None
        if len(relevant_timestamps) < 2:
            return None
        return len(relevant_timestamps) / (relevant_timestamps[-1] - relevant_timestamps[0])

    def rate_in_last(self, seconds: float | timedelta) -> float | None:
        """Return the rate of iteration within the last `seconds` seconds.

        Args:
            seconds: The number of seconds to consider. If a `timedelta` is given, it will be
                converted to seconds.

        Returns:
            The rate of iteration within the last `seconds` seconds.
        """
        if isinstance(seconds, timedelta):
            seconds = seconds.total_seconds()
        t = time.time()
        relevant_timestamps = [ts for ts in self._timestamps if ts >= t - seconds]
        if len(relevant_timestamps) < 2:
            return None
        return len(relevant_timestamps) / (relevant_timestamps[-1] - relevant_timestamps[0])

    def rate_since(self, timestamp: float | datetime) -> float | None:
        if isinstance(timestamp, datetime):
            timestamp = timestamp.timestamp()
        relevant_timestamps = [ts for ts in self._timestamps if ts >= timestamp]
        if len(relevant_timestamps) < 2:
            return None
        return len(relevant_timestamps) / (relevant_timestamps[-1] - relevant_timestamps[0])

    def jitter(self) -> float | None:
        if len(self._timestamps) < 2:
            return None
        return max(self._timestamps) - min(self._timestamps)

    @property
    def most_recent(self) -> datetime | None:
        if not self._timestamps:
            return None
        return datetime.fromtimestamp(self._timestamps[-1])

    @property
    def time_since_most_recent(self) -> timedelta | None:
        if not self._timestamps:
            return None
        return datetime.now() - self.most_recent

    @property
    def num_timestamps(self):
        return len(self._timestamps)


class InstrumentedAsyncIterator(AsyncIterator[_T], TimeKeeper):
    def __init__(self, async_iterable: AsyncIterable[_T]) -> None:
        super().__init__()
        self._async_iterator = aiter(async_iterable)
        self._most_recent_item = NoItemsSentinel

    def __aiter__(self) -> AsyncIterator[_T]:
        return self

    async def __anext__(self) -> _T:
        result = await self._async_iterator.__anext__()
        self.add_timestamp()
        self._most_recent_item = result
        return result

    @property
    def most_recent_item(self) -> _T | SentinelT:
        return self._most_recent_item


class RichDisplayedAsyncIterator(InstrumentedAsyncIterator[_T]):
    def __init__(
        self, async_iterable: AsyncIterable[_T], *, name: str = None, console: Console | None = None
    ) -> None:
        super().__init__(async_iterable)
        self._name = name

    def __rich__(self) -> str:
        if self._name is not None:
            name = self._name
        else:
            name = self.__class__.__name__

        most_recent_str = nat_time(self.time_since_most_recent, minimum_unit="milliseconds")
        mean_rate = self.mean_rate()
        mean_rate_str = f"{mean_rate:.2f} items/s" if mean_rate is not None else "N/A"
        return (
            f"{name}({self.num_timestamps} items, {mean_rate_str}\n"
            f"Last seen item: {self.most_recent_item} ({most_recent_str})"
        )


if __name__ == "__main__":

    async def arange(start: int, stop: int, step: int = 1) -> AsyncIterator[int]:
        i = start
        while i < stop:
            yield i
            i += step
            await asyncio.sleep(0.1)

    async def main():
        console = Console()
        rit = RichDisplayedAsyncIterator(arange(0, 100), console=console)
        with Live(console=console, refresh_per_second=10, renderable=rit):
            async for i in rit:
                console.log(i)

    asyncio.run(main())
