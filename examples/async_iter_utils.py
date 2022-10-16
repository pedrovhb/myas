from __future__ import annotations

import asyncio
import random
from typing import AsyncIterator

from myas import (
    pipe_async_iterable,
    merge_async_iterables,
    clone_async_iterable,
    split_async_iterable,
    aclone,
)


async def do_work() -> None:
    # Pretend to do some work for testing purposes
    await asyncio.sleep(random.uniform(0.1, 0.4))


async def arange(start: int, stop: int, step: int = 1) -> AsyncIterator[int]:
    """Iterate over the integers in the range [start, stop) with step size step."""
    while start < stop:
        await do_work()
        yield start
        start += step


async def multiply_by_two(x: int) -> int:
    await do_work()
    return x * 2


async def add_one(x: int) -> int:
    await do_work()
    return x + 1


async def spell_out(x: int) -> str:
    await do_work()
    nums = str(x)
    words = "zero one two three four five six seven eight nine".split()
    return " ".join(words[int(num)] for num in nums)


async def invert(x: str) -> str:
    await do_work()
    return x[::-1]


async def example_merge_aiters() -> None:

    # Create some async iterators to play with
    async_iterators = [
        arange(0, 10),
        arange(100, 110),
        arange(1337, 1369),
    ]

    merged_iterator = merge_async_iterables(*async_iterators)

    async for item in merged_iterator:
        print(item)


async def example_compose_pipe() -> None:
    async for item in pipe_async_iterable(arange(0, 10), multiply_by_two, add_one):
        print(item)

    async_iterators = [
        pipe_async_iterable(arange(0, 10), add_one, multiply_by_two),
        pipe_async_iterable(arange(100, 110), add_one, multiply_by_two, multiply_by_two),
    ]
    merged_iterator = merge_async_iterables(*async_iterators)

    async for inverted_string in pipe_async_iterable(merged_iterator, spell_out, invert):
        # reveal_type(inverted_string)  # mypy: Revealed type is 'builtins.str'
        print(inverted_string)


async def example_split_evens() -> None:
    async def is_even(x: int) -> bool:
        await do_work()
        return x % 2 == 0

    evens, odds = split_async_iterable(arange(0, 10), is_even)

    async for val in evens:
        print(f"even: {val}")


async def example_clone_async_iterator() -> None:
    async_0_10 = arange(0, 10)
    a, b = aclone(async_0_10, 2)

    async for item in a:
        print(item)

    async for item in b:
        print(item)

    async_80_90 = arange(80, 90)
    c, d, e = aclone(async_80_90, 3)
    async for item in merge_async_iterables(c, d, e):
        print(item)


if __name__ == "__main__":

    asyncio.run(example_merge_aiters())
    asyncio.run(example_compose_pipe())
    asyncio.run(example_clone_async_iterator())
    asyncio.run(example_split_evens())
