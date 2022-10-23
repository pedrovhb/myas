import asyncio
import sys
from itertools import tee
from typing import Any, Counter, Iterable

import pytest
from hypothesis import given
from hypothesis import strategies as st

from myas import CloseableQueue, amerge, arange, iter_to_aiter, q2a


@pytest.mark.asyncio
@given(st.integers(min_value=0, max_value=100), st.integers(min_value=0, max_value=100))
async def test_arange(frm: int, to: int) -> None:
    assert [x async for x in arange(frm, to)] == [x for x in range(frm, to)]


@pytest.mark.asyncio
@given(*((st.integers(min_value=0, max_value=100),) * 3))
async def test_arange_steps(frm: int, to: int, step: int) -> None:
    assert [x async for x in arange(frm, to)] == [x for x in range(frm, to)]


@pytest.mark.asyncio
@given(st.iterables(st.integers() | st.text() | st.lists(st.integers())))
async def test_iter_to_aiter(it: Iterable[Any]) -> None:
    it1, it2 = tee(it)
    iterator = iter(it2)
    async for x in iter_to_aiter(it1):
        assert x == next(iterator)

    # Check that the iterator is exhausted
    obj = object()
    assert next(iterator, obj) is obj


# Test for merge_async_iterables
@pytest.mark.asyncio
@given(st.iterables(st.iterables(st.integers())))
async def test_merge_async_iterables(iters: Iterable[Iterable[int]]) -> None:
    check, use = [], []
    for it in iters:
        it1, it2 = tee(it)
        check.append(it1)
        use.append(iter_to_aiter(it2))

    merged = amerge(*use)

    check_set = Counter[int]()
    for it in check:
        check_set.update(it)

    async for x in merged:
        check_set[x] -= 1

    assert all(x == 0 for x in check_set.values())


# Test for queue_to_async_iterable
@pytest.mark.asyncio
@given(st.lists(st.integers()))
async def test_queue_to_async_iterable(its: list[int]) -> None:
    q = CloseableQueue[int]()

    async def fill_q() -> None:
        for item in its:
            await q.put(item)
        q.close()

    asyncio.create_task(fill_q())

    j = 0
    async for x in q2a(q):
        assert x == its[j]
        j += 1

    assert j == len(its)


if __name__ == "__main__":
    pytest.main()
