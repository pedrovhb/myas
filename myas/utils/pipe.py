from __future__ import annotations

from typing import (
    Any,
    AsyncIterable,
    AsyncIterator,
    Callable,
    Coroutine,
    ParamSpec,
    TypeVar,
    overload,
)

from .compose import compose

P = ParamSpec("P")
_InputT = TypeVar("_InputT")
_OutputT = TypeVar("_OutputT")
_A = TypeVar("_A")
_B = TypeVar("_B")
_C = TypeVar("_C")
_D = TypeVar("_D")
_E = TypeVar("_E")
_F = TypeVar("_F")


T = TypeVar("T")
U = TypeVar("U")


@overload
async def pipe(
    value: _InputT,
    __function_1: Callable[[_InputT], Coroutine[Any, Any, _OutputT]]
    | Callable[[_InputT], _OutputT] = ...,
) -> _A:
    ...


@overload
async def pipe(
    value: _InputT,
    __function_1: Callable[[_InputT], Coroutine[Any, Any, _A]],
    __function_2: Callable[[_A], Coroutine[Any, Any, _B]] = ...,
) -> _B:
    ...


@overload
async def pipe(
    value: _InputT,
    __function_1: Callable[[_InputT], Coroutine[Any, Any, _A]],
    __function_2: Callable[[_A], Coroutine[Any, Any, _B]] = ...,
    __function_3: Callable[[_B], Coroutine[Any, Any, _C]] = ...,
) -> _C:
    ...


@overload
async def pipe(
    value: _InputT,
    __function_1: Callable[[_InputT], Coroutine[Any, Any, _A]],
    __function_2: Callable[[_A], Coroutine[Any, Any, _B]] = ...,
    __function_3: Callable[[_B], Coroutine[Any, Any, _C]] = ...,
    __function_4: Callable[[_C], Coroutine[Any, Any, _D]] = ...,
) -> _D:
    ...


@overload
async def pipe(
    value: _InputT,
    __function_1: Callable[[_InputT], Coroutine[Any, Any, _A]],
    __function_2: Callable[[_A], Coroutine[Any, Any, _B]] = ...,
    __function_3: Callable[[_B], Coroutine[Any, Any, _C]] = ...,
    __function_4: Callable[[_C], Coroutine[Any, Any, _D]] = ...,
    __function_5: Callable[[_D], Coroutine[Any, Any, _E]] = ...,
) -> _E:
    ...


@overload
async def pipe(
    value: _InputT,
    __function_1: Callable[[_InputT], Coroutine[Any, Any, _A]],
    __function_2: Callable[[_A], Coroutine[Any, Any, _B]] = ...,
    __function_3: Callable[[_B], Coroutine[Any, Any, _C]] = ...,
    __function_4: Callable[[_C], Coroutine[Any, Any, _D]] = ...,
    __function_5: Callable[[_D], Coroutine[Any, Any, _E]] = ...,
    __function_6: Callable[[_E], Coroutine[Any, Any, _F]] = ...,
) -> _F:
    ...


async def pipe(
    value: _InputT,
    *funcs: Callable[[Any], Coroutine[Any, Any, Any]] | Callable[[Any], Any],
) -> Any:
    """Pipe a value through a series of functions.

    Args:
        value: The value to pipe through the functions.
        funcs: The functions to pipe the value through.

    Returns:
        The value after it has been piped through all the functions.
    """

    for function in funcs:
        value = await function(value)
    return value


@overload
def pipe_async_iterable(
    async_iterable: AsyncIterable[_InputT],
) -> AsyncIterator[_InputT]:
    ...


@overload
def pipe_async_iterable(
    async_iterable: AsyncIterable[_InputT],
    __function_1: Callable[[_InputT], Coroutine[Any, Any, _A]],
) -> AsyncIterator[_A]:
    ...


@overload
def pipe_async_iterable(
    async_iterable: AsyncIterable[_InputT],
    __function_1: Callable[[_InputT], Coroutine[Any, Any, _A]],
    __function_2: Callable[[_A], Coroutine[Any, Any, _B]],
) -> AsyncIterator[_B]:
    ...


@overload
def pipe_async_iterable(
    async_iterable: AsyncIterable[_InputT],
    __function_1: Callable[[_InputT], Coroutine[Any, Any, _A]],
    __function_2: Callable[[_A], Coroutine[Any, Any, _B]],
    __function_3: Callable[[_B], Coroutine[Any, Any, _C]] = ...,
) -> AsyncIterator[_C]:
    ...


@overload
def pipe_async_iterable(
    async_iterable: AsyncIterable[_InputT],
    __function_1: Callable[[_InputT], Coroutine[Any, Any, _A]],
    __function_2: Callable[[_A], Coroutine[Any, Any, _B]],
    __function_3: Callable[[_B], Coroutine[Any, Any, _C]],
    __function_4: Callable[[_C], Coroutine[Any, Any, _D]] = ...,
) -> AsyncIterator[_D]:
    ...


@overload
def pipe_async_iterable(
    async_iterable: AsyncIterable[_InputT],
    __function_1: Callable[[_InputT], Coroutine[Any, Any, _A]],
    __function_2: Callable[[_A], Coroutine[Any, Any, _B]],
    __function_3: Callable[[_B], Coroutine[Any, Any, _C]],
    __function_4: Callable[[_C], Coroutine[Any, Any, _D]],
    __function_5: Callable[[_D], Coroutine[Any, Any, _E]] = ...,
) -> AsyncIterator[_E]:
    ...


@overload
def pipe_async_iterable(
    async_iterable: AsyncIterable[_InputT],
    __function_1: Callable[[_InputT], Coroutine[Any, Any, _A]],
    __function_2: Callable[[_A], Coroutine[Any, Any, _B]],
    __function_3: Callable[[_B], Coroutine[Any, Any, _C]],
    __function_4: Callable[[_C], Coroutine[Any, Any, _D]],
    __function_5: Callable[[_D], Coroutine[Any, Any, _E]],
    __function_6: Callable[[_E], Coroutine[Any, Any, _F]] = ...,
) -> AsyncIterator[_F]:
    ...


async def pipe_async_iterable(
    async_iterable: AsyncIterable[_InputT],
    *functions: Callable[..., Coroutine[Any, Any, Any]],
) -> AsyncIterator[_OutputT]:
    fn: Callable[[_InputT], Coroutine[Any, Any, _OutputT]] = compose(*functions)

    async for value in async_iterable:
        yield await fn(value)


__all__ = (
    "pipe",
    "pipe_async_iterable",
)
