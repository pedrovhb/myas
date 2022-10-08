import asyncio
import random
from itertools import pairwise
from typing import AsyncIterable, Any, Callable, Coroutine, AsyncIterator

from myas.processor import WorkerManager


def pipe_pooled(
    ait: AsyncIterable[Any],
    *funcs: Callable[..., Coroutine[Any, Any, Any]],
) -> tuple[list[WorkerManager[Any, Any]], AsyncIterator[Any]]:
    """Chain multiple workers together, passing the output of one as the input of the next."""
    workers = [WorkerManager(f) for f in funcs]
    for a, b in pairwise(workers):
        b.add_source(a)
    workers[0].add_source(ait)
    for w in workers:
        asyncio.create_task(w.close())
    return workers, aiter(workers[-1])


async def mul_two(x: int) -> int:
    return x * 2


async def spell_number(x: int) -> str:
    numbers = "zero one two three four five six seven eight nine ten".split()
    words = [numbers[int(i)] for i in str(x)]
    return " ".join(words)


async def some_source() -> AsyncIterator[int]:
    for i in range(10):
        yield i
        await asyncio.sleep(random.uniform(0.1, 0.4))


if __name__ == "__main__":

    async def main() -> None:
        workers, chained = pipe_pooled(some_source(), mul_two, spell_number)
        async for result in chained:
            print(result)

        for w in workers:
            s = w.get_status()
            print(w, s.idle_time_percent, s.num_items_in_queue, s.num_items_processed)

        print("Done")

    asyncio.run(main())
