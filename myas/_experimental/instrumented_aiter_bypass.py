from __future__ import annotations

import asyncio
import inspect
import math
import random
import statistics
import time
from abc import ABC, abstractmethod
from asyncio import iscoroutinefunction, iscoroutine
from datetime import datetime, timedelta
from functools import cached_property, lru_cache
from typing import (
    AsyncIterator,
    TypeVar,
    AsyncIterable,
    ClassVar,
    Generic,
    Callable,
    Coroutine,
    Any,
    cast,
    Protocol,
    ParamSpec,
    TypeGuard,
    Awaitable,
)
from array import array

import rich

from myas import apipe, compose

from rich.console import Console, Group
from rich.table import Table
from rich.live import Live
from rich.panel import Panel
from humanize import naturaldelta as nat_delta, naturalsize as nat_size, naturaltime as nat_time

_T = TypeVar("_T")


class SentinelT:

    _names: ClassVar[dict[str, SentinelT]] = {}
    _name: str
    __slots__ = ("_name",)

    def __new__(cls, sentinel_name: str) -> SentinelT:
        if sentinel_name in cls._names:
            return cls._names[sentinel_name]
        self = super().__new__(cls)
        self._name = sentinel_name
        cls._names[sentinel_name] = self
        return self

    def __repr__(self) -> str:
        return f"<SentinelT({self._name})>"

    __str__ = __repr__


NoItemsSentinel = SentinelT("NoItemsSentinel")


class TimeKeeper:
    def __init__(self) -> None:
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
        if not self.most_recent:
            return None
        return datetime.now() - self.most_recent

    @property
    def num_timestamps(self) -> int:
        return len(self._timestamps)


class InstrumentedAsyncIterator(AsyncIterator[_T], TimeKeeper):
    def __init__(self, async_iterable: AsyncIterable[_T]) -> None:
        super().__init__()
        self._async_iterator = aiter(async_iterable)
        self._most_recent_item: SentinelT | _T = NoItemsSentinel

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
        self,
        async_iterable: AsyncIterable[_T],
        *,
        name: str | None = None,
        console: Console | None = None,
    ) -> None:
        super().__init__(async_iterable)
        self._name = name if name is not None else f"{async_iterable.__qualname__}"  # type: ignore

    def __rich__(self) -> str:

        most_recent_str = (
            nat_time(self.time_since_most_recent, minimum_unit="milliseconds")
            if self.time_since_most_recent is not None
            else "<no items>"
        )

        mean_rate = self.mean_rate()
        mean_rate_str = f"{mean_rate:.2f} items/s" if mean_rate is not None else "N/A"
        return (
            f"{self._name} {self.num_timestamps} items, {mean_rate_str}\n"
            f"Last seen item: {self.most_recent_item} ({most_recent_str})"
        )


# def transformer(
#     async_iterable: AsyncIterable[_T],
#     *,
#     name: str | None = None,
#     console: Console | None = None,
# ) -> RichDisplayedAsyncIterator[_T]:
#     return RichDisplayedAsyncIterator(async_iterable, name=name, console=console)


_InputT = TypeVar("_InputT", contravariant=True)
_OutputT = TypeVar("_OutputT", covariant=True)

_T_co = TypeVar("_T_co", covariant=True)
_T_co2 = TypeVar("_T_co2", covariant=True)
_T_contra = TypeVar("_T_contra", contravariant=True)
# _T_Transformer = TypeVar("_T_Transformer", bound="AsyncIteratorTransformer[_InputT, _OutputT]")


class TransformerMethod(Protocol[_T_contra, _T_co]):
    def __call__(self: Any, item: _T_contra) -> _T_co:
        ...


class AsyncTransformerMethod(Protocol[_T_contra, _T_co]):
    async def __call__(self: Any, item: _T_contra) -> _T_co:
        ...


# class AsyncTransformerMethod(Protocol[_T_contra, _T_co]):
#     def __call__(self, item: _T_contra) -> Coroutine[Any, Any, _T_co]:
#         ...
P = ParamSpec("P")
P2 = ParamSpec("P2")
_A = TypeVar("_A")
_B = TypeVar("_B")

_GenericTransformerMethod = (
    TransformerMethod[_InputT, _OutputT] | AsyncTransformerMethod[_InputT, _OutputT]
)


# def myiscoroutinefunction(
#     func: _GenericTransformerMethod,
# ) -> TypeGuard[AsyncTransformerMethod[_InputT, _OutputT]]:
#     """Return True if func is a decorated coroutine function."""
#     if inspect.iscoroutinefunction(func):
#         return True
#     return False


def is_async_transformer_method(
    func: Callable[[_InputT], _OutputT | Coroutine[Any, Any, _OutputT]],
) -> TypeGuard[Callable[[_InputT], Coroutine[Any, Any, _OutputT]]]:
    """Return True if func is a decorated coroutine function."""
    if inspect.iscoroutinefunction(func):
        return True
    return False


def is_transformer_method(
    func: Callable[[_InputT], _OutputT | Coroutine[Any, Any, _OutputT]],
) -> TypeGuard[Callable[[_InputT], _OutputT]]:
    """Return True if func is a decorated coroutine function."""
    if not inspect.iscoroutinefunction(func) and callable(func):
        return True
    return False


class AsyncIteratorTransformerBase(Generic[_InputT, _OutputT], AsyncIterator[_OutputT], ABC):
    fun: Callable[[_InputT], _OutputT | Coroutine[Any, Any, _OutputT]]

    def __init__(
        self,
        async_iterable: AsyncIterable[_InputT],
        # fun: Callable[[_InputT], Coroutine[Any, Any, _OutputT]] | Callable[[_InputT], _OutputT],
    ) -> None:
        self._async_iterator = aiter(async_iterable)
        # self._fun = fun

    async def __anext__(self) -> _OutputT:

        item = await self._async_iterator.__anext__()

        if is_async_transformer_method(async_fun := self.fun):
            return await async_fun(item)
        elif is_transformer_method(sync_fun := self.fun):
            return sync_fun(item)
        else:
            raise TypeError(f"Unknown type of function {self.fun}")


class AsyncIteratorTransformer(AsyncIteratorTransformerBase[_InputT, _OutputT]):
    def __init__(
        self,
        async_iterable: AsyncIterable[_InputT],
        fun: Callable[[_InputT], _OutputT | Coroutine[Any, Any, _OutputT]],
    ) -> None:
        super().__init__(async_iterable)
        self._fun = fun

    async def fun(self, item: _InputT) -> _OutputT:
        if is_async_transformer_method(async_fun := self._fun):
            return await async_fun(item)
        elif is_transformer_method(sync_fun := self._fun):
            return sync_fun(item)
        else:
            raise TypeError(f"Unknown type of function {self.fun}")


class DoublerTransformer(AsyncIteratorTransformerBase[int, int]):
    def fun(self, item: int) -> int:
        return item * 2


class RichPrintTransformer(AsyncIteratorTransformerBase[_T, _T]):
    def __init__(
        self,
        async_iterable: AsyncIterable[_T],
        *,
        name: str | None = None,
        console: Console | None = None,
    ) -> None:
        super().__init__(async_iterable)

        if not name:
            name = next(
                (
                    getattr(async_iterable, attr)
                    for attr in (
                        "name",
                        "title",
                        "description",
                        "__qualname__",
                        "__name__",
                    )
                    if hasattr(async_iterable, attr)
                ),
                None,
            )
        if not name:
            name = next(
                (
                    getattr(async_iterable_cls, attr)
                    for attr in (
                        "__name__",
                        "__qualname__",
                    )
                    if hasattr(async_iterable_cls := async_iterable.__class__, attr)
                ),
                repr(async_iterable),
            )

        self._name = name if name is not None else f"{async_iterable.__qualname__}"  # type: ignore
        self.console = console or rich.get_console()

    def fun(self, item: _T) -> _T:
        self.console.log(f"[green]aiter [bold]{self._name}[/][green]:[/]", item)
        return item


if __name__ == "__main__":

    async def arange(start: int, stop: int, step: int = 1) -> AsyncIterator[int]:
        i = start
        while i < stop:
            yield i
            i += step
            await asyncio.sleep(0.1)

    async def main() -> None:
        console = Console()
        rit = RichDisplayedAsyncIterator(arange(0, 100), console=console)

        async def double(x: int) -> int:
            return x * 2

        # doubler = RichPrintTransformer(DoublerTransformer(RichPrintTransformer(arange(0, 100))))

        async def spell_out(x: int) -> str:
            nums = "zero one two three four five six seven eight nine".split()
            return " ".join(nums[int(x)] for x in str(x))

        cn = compose(double, double)
        f = await cn(5)
        reveal_type(f)
        reveal_type(cn)

        cn2 = compose(double, double, spell_out)
        f2 = await cn2(5)
        reveal_type(f2)
        reveal_type(cn2)

        quadrupler = AsyncIteratorTransformer[int, str](
            arange(0, 100), compose(double, double, spell_out)
        )
        qu2 = apipe(arange(0, 100), double, double, spell_out)

        # async for item in doubler:
        # console.print(item)
        # pass

        async for item in qu2:
            console.print(item)

        exit(0)

        # async def double(a: int) -> int:
        #     await asyncio.sleep(0.1)
        #     return a * 2

        doubled = RichDisplayedAsyncIterator(apipe(rit, double))
        quadrupled = RichDisplayedAsyncIterator(apipe(doubled, double))

        group = Group(rit, doubled, quadrupled)

        with Live(console=console, refresh_per_second=10, renderable=group):
            async for i in quadrupled:
                console.log(i)

    asyncio.run(main())
