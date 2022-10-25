from __future__ import annotations

from typing import TypeVar, Coroutine, Any, Literal, Protocol, Callable, Awaitable

_PutT = TypeVar("_PutT", covariant=True)
_GetT = TypeVar("_GetT")
T = TypeVar("T")


class QueueLike(Protocol[_PutT, _GetT]):

    put: Callable[[_PutT], Coroutine[Any, Any, None]]
    put_nowait: Callable[[_PutT], None]

    get: Callable[[], Coroutine[Any, Any, _GetT]]
    get_nowait: Callable[[], _GetT]

    join: Callable[[], Coroutine[Any, Any, None]]


class CloseableQueueLike(Protocol[_PutT, _GetT], QueueLike[_PutT, _GetT]):

    aclose: Callable[[], Coroutine[Any, Any, None]]

    # todo - the below are properties, but mypy doesn't recognize them until 0.9xx is merged;
    #  see https://github.com/python/mypy/pull/13475
    is_closed: Callable[[], bool]
    is_exhausted: Callable[[], bool]

    exhausted: Callable[[], Awaitable[Literal[True]]]
    closed: Callable[[], Awaitable[Literal[True]]]

