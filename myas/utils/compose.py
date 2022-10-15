from __future__ import annotations

import inspect
from typing import Any, Callable, Coroutine, ParamSpec, TypeVar, cast, overload

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
def ensure_coroutine(
    fn: Callable[P, Coroutine[Any, Any, _OutputT]],
) -> Callable[P, Coroutine[Any, Any, _OutputT]]:
    ...


@overload
def ensure_coroutine(
    fn: Callable[P, _OutputT],
) -> Callable[P, Coroutine[Any, Any, _OutputT]]:
    ...


def ensure_coroutine(
    fn: Callable[P, _OutputT] | Callable[P, Coroutine[Any, Any, _OutputT]],
) -> Callable[P, Coroutine[Any, Any, _OutputT]]:
    """Ensure that a function is a coroutine (i.e. async).

    If the given callable is not a coroutine, it will be wrapped in a coroutine that calls the
    function and returns the result. Otherwise, the function is returned as-is.

    Args:
        fn: The function to ensure is a coroutine.

    Returns:
        The function as a coroutine.
    """

    if inspect.iscoroutinefunction(fn):
        _fn = cast(Callable[P, Coroutine[Any, Any, _OutputT]], fn)
        return _fn

    _fn_sync = cast(Callable[P, _OutputT], fn)

    async def _as_async(*args: P.args, **kwargs: P.kwargs) -> _OutputT:
        result = _fn_sync(*args, **kwargs)
        return result

    # return cast(Callable[P, Coroutine[Any, Any, _OutputT]], _as_async)
    return _as_async


#
# async def a() -> int:
#     async def async_fn() -> int:
#         await asyncio.sleep(1)
#         return 1
#
#     def sync_fn() -> int:
#         return 2
#
#     a = ensure_coroutine(async_fn)
#     b = ensure_coroutine(sync_fn)
#
#     reveal_type(a)
#     reveal_type(b)


@overload
def compose(
    __function_1: Callable[P, Coroutine[Any, Any, _A]],
) -> Callable[P, Coroutine[Any, Any, _A]]:
    ...


@overload
def compose(
    __function_1: Callable[P, Coroutine[Any, Any, _A]],
    __function_2: Callable[[_A], Coroutine[Any, Any, _B]] = ...,
) -> Callable[P, Coroutine[Any, Any, _B]]:
    ...


@overload
def compose(
    __function_1: Callable[P, Coroutine[Any, Any, _A]],
    __function_2: Callable[[_A], Coroutine[Any, Any, _B]],
    __function_3: Callable[[_B], Coroutine[Any, Any, _C]] = ...,
) -> Callable[P, Coroutine[Any, Any, _C]]:
    ...


@overload
def compose(
    __function_1: Callable[P, Coroutine[Any, Any, _A]],
    __function_2: Callable[[_A], Coroutine[Any, Any, _B]],
    __function_3: Callable[[_B], Coroutine[Any, Any, _C]],
    __function_4: Callable[[_C], Coroutine[Any, Any, _D]] = ...,
) -> Callable[P, Coroutine[Any, Any, _D]]:
    ...


@overload
def compose(
    __function_1: Callable[P, Coroutine[Any, Any, _A]],
    __function_2: Callable[[_A], Coroutine[Any, Any, _B]],
    __function_3: Callable[[_B], Coroutine[Any, Any, _C]],
    __function_4: Callable[[_C], Coroutine[Any, Any, _D]],
    __function_5: Callable[[_D], Coroutine[Any, Any, _E]] = ...,
) -> Callable[P, Coroutine[Any, Any, _E]]:
    ...


@overload
def compose(
    __function_1: Callable[P, Coroutine[Any, Any, _A]],
    __function_2: Callable[[_A], Coroutine[Any, Any, _B]],
    __function_3: Callable[[_B], Coroutine[Any, Any, _C]],
    __function_4: Callable[[_C], Coroutine[Any, Any, _D]],
    __function_5: Callable[[_D], Coroutine[Any, Any, _E]],
    __function_6: Callable[[_E], Coroutine[Any, Any, _F]] = ...,
) -> Callable[P, Coroutine[Any, Any, _F]]:
    ...


def compose(
    fn: Callable[P, Coroutine[Any, Any, Any]],
    *fns: Callable[..., Coroutine[Any, Any, Any]],
) -> Callable[P, Coroutine[Any, Any, Any]]:
    """Compose functions together.

    Args:
        fn: The first function to compose.
        fns: The remaining functions to compose.

    Returns:
        A function that composes the given functions together.
    """
    if not fns:
        return ensure_coroutine(fn)

    _fn = ensure_coroutine(fn)
    _fns = [ensure_coroutine(f) for f in fns]

    async def _composed(*args: P.args, **kwargs: P.kwargs) -> Any:
        result = await _fn(*args, **kwargs)
        for f in _fns:
            result = await f(result)
        return result

    return _composed


__all__ = (
    "compose",
    "ensure_coroutine",
)
