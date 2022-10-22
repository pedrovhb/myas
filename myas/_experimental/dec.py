from __future__ import annotations

from asyncio import iscoroutinefunction
from functools import wraps
from types import NotImplementedType
from typing import (
    Callable,
    ParamSpec,
    TypeVar,
    Protocol,
    Any,
    cast,
    Coroutine,
    Generic,
    overload,
    AsyncGenerator,
)
from collections.abc import AsyncIterable, Iterable, Awaitable, AsyncIterator, Generator

from myas import iter_to_aiter, ensure_coroutine
from myas.stream import merge_iter_aiter

T = TypeVar("T")
U = TypeVar("U")
P = ParamSpec("P")


def amap(fn: Callable[[T], Awaitable[U]], async_iter: AsyncIterable[T]) -> AsyncIterable[U]:
    @wraps(fn)
    async def _inner() -> AsyncIterator[U]:
        async for i in async_iter:
            yield await fn(i)

    return _inner()


def make_transformer(
    fn: Callable[[T], Awaitable[U]]
) -> Callable[[AsyncIterable[T]], AsyncIterable[U]]:
    @wraps(fn)
    async def _inner(async_iter: AsyncIterable[T]) -> AsyncIterable[U]:
        async for i in async_iter:
            yield await fn(i)

    return _inner


class ExtendedAsyncIterable(Generic[T], AsyncIterable[T]):
    _aiter: AsyncIterable[T]

    def __init__(self, *aiterables: AsyncIterable[T] | Iterable[T]) -> None:
        its = [iter_to_aiter(it) if isinstance(it, Iterable) else it for it in aiterables]
        self._aiter = merge_iter_aiter(*its)

    @overload
    def __or__(self, other: Callable[[T], Coroutine[Any, Any, U]]) -> ExtendedAsyncIterable[U]:
        ...

    @overload
    def __or__(self, other: Callable[[T], U]) -> ExtendedAsyncIterable[U]:
        ...

    def __or__(self, other: Any) -> Any:
        if iscoroutinefunction(other):
            _fn = cast(Callable[[T], Coroutine[Any, Any, U]], other)
            return ExtendedAsyncIterable(amap(_fn, self._aiter))
        elif callable(other):
            _fn = ensure_coroutine(other)
            return ExtendedAsyncIterable(amap(_fn, self._aiter))
        return NotImplemented

    def __aiter__(self) -> AsyncIterator[T]:
        return aiter(self._aiter)


class ExtendedCoroFunc(Generic[T]):
    def __init__(self, fn: Callable[[T], Coroutine[Any, Any, U]]) -> None:
        self._fn = fn

    def __call__(self, arg: T) -> Coroutine[Any, Any, U]:
        return self._fn(arg)

    @overload
    def __ror__(self, other: AsyncIterable[T]) -> ExtendedAsyncIterable[U]:
        ...

    @overload
    def __ror__(self, other: Iterable[T]) -> ExtendedAsyncIterable[U]:
        ...

    def __ror__(
        self, other: AsyncIterable[T] | Iterable[T]
    ) -> ExtendedAsyncIterable[U] | NotImplementedType:
        if isinstance(other, AsyncIterable):
            return ExtendedAsyncIterable(other) | self._fn
        return NotImplemented


def eas(fn: Callable[P, AsyncIterable[T]]) -> Callable[P, ExtendedAsyncIterable[T]]:
    @wraps(fn)
    def _inner(*args: P.args, **kwargs: P.kwargs) -> ExtendedAsyncIterable[T]:
        return ExtendedAsyncIterable(fn(*args, **kwargs))

    return _inner


#
# def eas(fn: Callable[P, Coroutine[Any, Any, T]]) -> Callable[P, ExtendedCoroFunc[T]]:
#     @wraps(fn)
#     def _inner(*args: P.args, **kwargs: P.kwargs) -> ExtendedAsyncIterable[T]:
#         return ExtendedAsyncIterable(fn(*args, **kwargs))
#
#     return _inner


# def pipeable(fn: Callable[P, T]) -> Callable[P, T]:
#
#     # async def _inner(*args: P.args, **kwargs: P.kwargs) -> AsyncIterableT[T]:
#
#     fn.__or__ = __or__
#     fn.__ror__ = __or__
#     new_fn = wraps(fn, assigned=("__or__",))(fn)
#     return new_fn


# @pipeable
async def transform(n: int) -> int:
    return n * 2


@eas
async def arange(stop: int, start: int = 0, step: int = 1) -> AsyncIterable[int]:
    for i in range(start, stop, step):
        yield i


async def spell_out(n: int) -> str:
    nums = "zero one two three four five six seven eight nine".split()
    return " ".join(nums[int(c)] for c in str(n))


def check(t: Generator[str, Any, None]) -> None:
    print(t)
    print(t.send(None))
    print(t.send(None))
    print(t.send(None))
    print(t.send(None))
    print(t.send(None))
    print(t.send(None))


async def agen() -> AsyncGenerator[int, None]:
    yield 12
    yield 23
    yield 34


async def checkasync(t: AsyncGenerator[int, Any]) -> None:
    print(t)
    async for val in t:
        print(val)


async def main() -> None:
    pipe = arange(5) | transform

    async for ch in arange(5) | transform | spell_out:
        print(ch)

    check(a for a in "abcdef")
    o = (a async for a in arange(5))
    await checkasync(o)
    await checkasync(agen())


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
