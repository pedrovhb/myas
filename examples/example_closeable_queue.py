"""Examples of using the CloseableQueue class."""

import asyncio
import random
from typing import Any, AsyncIterable, AsyncIterator, Callable, Coroutine

from myas import a2q, amap, apipe, populate_queue, q2a
from myas.closeable_queue import CloseableQueue, QueueExhausted


async def some_source() -> AsyncIterator[int]:
    for i in range(10):
        yield i
        await asyncio.sleep(random.uniform(0.1, 0.4))


async def mul_two(x: int) -> int:
    return x * 2


async def spell_number(x: int) -> str:
    numbers = "zero one two three four five six seven eight nine ten".split()
    words = [numbers[int(i)] for i in str(x)]
    return " ".join(words)


async def main() -> None:
    q = CloseableQueue()
    for i in range(10):
        await q.put(i)

    populate_queue(q, some_source(), amap(some_source(), mul_two), close_on_finished=True)
    async for result in q2a(q):  # Iterate over queue with q2a (queue to async iterator)
        print(result)
    print("Iterator done")

    # Use a new one for the next example, since the previous one was closed
    q2 = CloseableQueue()

    async def queue_fill(queue: CloseableQueue) -> None:
        async for item in apipe(some_source(), mul_two, mul_two, spell_number):
            await queue.put(item)
        print("Queue fill done")
        queue.close()

    asyncio.create_task(queue_fill(q2))

    while True:
        try:
            print(await q2.get())
        except QueueExhausted:
            print("Queue exhausted")
            break


if __name__ == "__main__":
    asyncio.run(main())
