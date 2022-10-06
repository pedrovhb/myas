from __future__ import annotations

import asyncio
import random
from typing import AsyncIterator

from myas.utils import async_iterable_pipe, merge_async_iterators


async def do_work() -> None:
    # Pretend to do some work for testing purposes
    await asyncio.sleep(random.uniform(0.1, 0.4))


async def aiter_ints(a: int, b: int) -> AsyncIterator[int]:
    for i in range(a, b):
        await do_work()
        yield i


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
        aiter_ints(0, 10),
        aiter_ints(100, 110),
        aiter_ints(1337, 1369),
    ]

    merged_iterator = merge_async_iterators(*async_iterators)

    async for item in merged_iterator:
        print(item)


async def example_compose_pipe() -> None:
    async for item in async_iterable_pipe(aiter_ints(0, 10), multiply_by_two, add_one):
        print(item)

    async_iterators = [
        async_iterable_pipe(aiter_ints(0, 10), add_one, multiply_by_two),
        async_iterable_pipe(aiter_ints(100, 110), add_one, multiply_by_two, multiply_by_two),
    ]
    merged_iterator = merge_async_iterators(*async_iterators)

    async for inverted_string in async_iterable_pipe(merged_iterator, spell_out, invert):
        # reveal_type(inverted_string)  # mypy: Revealed type is 'builtins.str'
        print(inverted_string)


if __name__ == "__main__":

    asyncio.run(example_merge_aiters())
    asyncio.run(example_compose_pipe())
