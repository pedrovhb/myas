import asyncio
import heapq
import sys
from collections import defaultdict
from collections.abc import Mapping, Sequence
from itertools import tee
from typing import Any, Counter, Iterable, TypeVar

import pytest
from hypothesis import given
from hypothesis import strategies as st

from myas.closeable_queue import (
    CloseableQueue,
    QueueClosedException,
    QueueExhausted,
    CloseablePriorityQueue,
)
from myas import (
    amerge,
    q2a,
)


T = TypeVar("T")
random_t = st.integers() | st.text() | st.lists(st.integers() | st.text() | st.lists(st.integers()))
priority_tuples = st.tuples(st.floats() | st.integers(), random_t)
RandomT = TypeVar("RandomT", int, str, list[int | str | list[int | str]])


def deep_equals(a: Any, b: Any) -> bool:
    if isinstance(a, Sequence):
        return isinstance(b, type(a)) and all(deep_equals(x, y) for x, y in zip(a, b))
    elif isinstance(a, Mapping):
        return isinstance(b, type(a)) and all(
            deep_equals(x, y) for x, y in zip(a.items(), b.items())
        )
    else:
        return bool(a == b)


@pytest.mark.asyncio
@given(st.iterables(random_t, min_size=1, max_size=200))
async def test_queue(values: Iterable[RandomT]) -> None:
    asyncio.get_running_loop().set_debug(True)

    q = CloseableQueue[RandomT]()
    use, check = tee(values)
    for value in use:
        await q.put(value)
    q.close()

    with pytest.raises(QueueClosedException):
        await q.put(123)  # type: ignore

    assert not q.empty()
    assert q.is_closed
    assert not q.is_exhausted

    while not q.empty():
        assert not q.is_exhausted
        assert await q.get() == next(check)

    assert q.is_exhausted
    with pytest.raises(QueueExhausted):
        await q.get()

    assert [x async for x in q2a(q)] == [x for x in values]


# Test for PriorityQueue
@pytest.mark.asyncio
@given(
    st.iterables(st.tuples(st.floats(allow_nan=False) | st.integers()), min_size=1, max_size=200)
    | st.iterables(st.tuples(st.floats(allow_nan=False), st.text()), min_size=1, max_size=200)
    | st.iterables(st.text(), min_size=1, max_size=200)
)
async def test_priority_queue(values: Iterable[tuple[float, RandomT]]) -> None:

    q = CloseablePriorityQueue[tuple[float, RandomT]]()
    use, check = tee(values)
    for value in use:
        await q.put(value)
    q.close()

    assert not q.empty()
    assert q.is_closed
    assert not q.is_exhausted

    checks = list(check)

    prev = None
    while not q.empty():

        it = await q.get()
        if prev is None:
            prev = it
        else:
            assert prev <= it
            prev = it
        checks.remove(it)

    assert not checks

    assert q.is_exhausted
    with pytest.raises(QueueExhausted):
        await q.get()
    assert q.empty()


if __name__ == "__main__":
    pytest.main(sys.argv)
