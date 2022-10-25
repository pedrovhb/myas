from __future__ import annotations

from typing import Any, Callable, Coroutine, ParamSpec, TypeVar, overload

import myas

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
        return myas.ensure_coroutine(fn)

    _fn = myas.ensure_coroutine(fn)
    _fns = [myas.ensure_coroutine(f) for f in fns]

    async def _composed(*args: P.args, **kwargs: P.kwargs) -> Any:
        result = await _fn(*args, **kwargs)
        for f in _fns:
            result = await f(result)
        return result

    return _composed


__all__ = ("compose",)
