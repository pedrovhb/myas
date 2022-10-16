from __future__ import annotations
from __future__ import annotations

from dataclasses import dataclass
import asyncio
import random
from itertools import pairwise
from typing import AsyncIterable, Any, Callable, Coroutine, AsyncIterator, TypeVar, Type

from rich.columns import Columns
from rich.console import ConsoleOptions, RenderResult, RenderableType, NewLine
from rich.progress_bar import ProgressBar
from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.text import Text
from rich.progress import (
    Progress,
    ProgressColumn,
    Task as RichProgressTask,
)

from myas import merge_async_iterables
from myas.processor import WorkerManager, WorkerManagerBase, WorkerManagerStatus

c = Console()

_InputT = TypeVar("_InputT")
_OutputT = TypeVar("_OutputT")


class DashManager:
    def __init__(self, manager: WorkerManagerBase[Any, Any], task: RichProgressTask) -> None:
        self._manager = manager
        self._task = task
        # self._progress.add_task(


class Dashboard:
    def __init__(self, console: Console | None) -> None:
        self.console = console or Console()

        self._managers: list[WorkerManagerBase[Any, Any]] = []
        self.live = Live(console=self.console)

        self._ctx_manager_live = asyncio.Event()

        self._progress = Progress(console=self.console, auto_refresh=False)

    async def _update_manager_status(self, manager: WorkerManagerBase[Any, Any]) -> None:
        self.console.print(f"{manager} updated - {manager._input_queue.qsize()} items in queue")
        # todo - add get_status method to WorkerManagerBase

    async def _manager_watcher(self) -> None:
        while True:
            await asyncio.gather(
                *[self._update_manager_status(manager) for manager in self._managers]
            )
            asyncio.sleep(self._update_rate)

    def register_manager(self, manager: WorkerManager[_InputT, _OutputT]) -> None:
        self._managers.append(manager)

        async def _update_dash(result: _OutputT) -> None:
            await self._update_manager_status(manager)
            self.console.print(f"Update dash - got result {result}")

        manager.add_sink(_update_dash)

    def _progress_panel(self) -> Panel:

        Group(
            Progress(console=self.console, auto_refresh=False),
        )

        return Panel(
            Text(
                "Progress",
                style="bold",
            ),
            height=5 * len(self._managers),
        )

    async def __aenter__(self) -> Dashboard:
        self._ctx_manager_live.set()
        self.live.start()
        return self

    async def __aexit__(
        self,
        exc_type: Type[BaseException],
        exc_val: BaseException,
        exc_tb: Any,
    ) -> None:
        self._ctx_manager_live.clear()
        self.live.stop()


# class RichManager(WorkerManager[_InputT, _OutputT]):
#     def __init__(self):
#         super().__init__(self._function)
#         self._sinks: list[Callable[[Any], Coroutine[Any, Any, Any]]] = []


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
    ait = aiter(workers[-1])
    return workers, ait


async def mul_two(x: int) -> int:
    await asyncio.sleep(random.uniform(0.8, 1.6))
    return x * 2


async def spell_number(x: int) -> str:
    numbers = "zero one two three four five six seven eight nine ten".split()
    await asyncio.sleep(random.uniform(0.6, 1))
    words = [numbers[int(i)] for i in str(x)]
    return " ".join(words)


async def some_source() -> AsyncIterator[int]:
    for i in range(100):
        yield i
        # await asyncio.sleep(random.uniform(0.1, 0.2))
        await asyncio.sleep(random.uniform(0.1, 0.2))


c = Console()


@dataclass
class ManagerTrackingTask(RichProgressTask):
    manager: WorkerManagerBase[Any, Any] | None = None

    def queue_size(self) -> int:
        if self.manager is None:
            return 0
        return self.manager._input_queue.qsize()

    def n_workers(self) -> int:
        if self.manager is None:
            return 0
        return self.manager._n_workers

    def n_idle_workers(self) -> int:
        if self.manager is None:
            return 0
        return sum(1 for idle_ev in self.manager._idle_worker_event.values() if idle_ev.is_set())


class MultiProgressBar:
    def __init__(self, console: Console, manager: WorkerManager[Any, Any]) -> None:
        total_width = console.width
        self.n_bars = 4
        colors = ["#ff0000", "#00ff00", "#0000ff", "#ffff00"]

        # workers comprise a part of the bar.
        #   - green is an idle worker
        #   - red is a busy worker
        #   - blue is an item on queue
        #   - gray is spare queue space
        # if all workers are idle, the bar is green for each worker, then gray for the capacity of
        # the queue.
        # If all workers are busy, the bar is red for each worker, blue for the items
        # on the queue, and gray for the spare queue space.
        # If some workers are idle, the bar is green for each idle worker, red for each busy worker,
        # blue for the items on the queue, and gray for the spare queue space.

        # total_width = console.width / 2
        status = manager.get_status()
        bar_total = status.num_workers + status.queue_capacity
        red_bar = status.num_active_workers
        green_bar = status.num_idle_workers
        blue_bar = status.num_items_in_queue
        gray_bar = bar_total - red_bar - green_bar - blue_bar

        # todo - figure out relative widths of the bars

        console.log(status)
        console.rule(f"Manager {manager}")
        console.print(f"{red_bar=} {green_bar=} {blue_bar=} {gray_bar=}")

        self._bars = []
        if red_bar:
            self._bars.append(ProgressBar(total=100, completed=100, width=red_bar, style="red"))
        if green_bar:
            self._bars.append(ProgressBar(total=100, completed=100, width=green_bar, style="green"))
        if blue_bar:
            self._bars.append(ProgressBar(total=100, completed=100, width=blue_bar, style="blue"))
        if gray_bar:
            self._bars.append(
                ProgressBar(total=100, completed=100, width=gray_bar, style="#cccccc")
            )

        # self._bars = [
        #     ProgressBar(total=100, completed=100, width=red_bar, style="red"),
        #     ProgressBar(total=100, completed=100, width=green_bar, style="green"),
        #     ProgressBar(total=100, completed=100, width=blue_bar, style="blue"),
        #     ProgressBar(total=100, completed=100, width=gray_bar, style="#cccccc"),
        # ]

    def __rich_console__(self, console: Console, options: ConsoleOptions) -> RenderResult:
        for bar in self._bars:
            yield from bar.__rich_console__(console, options)


class MyProgressColumn(ProgressColumn):
    def render(self, task: RichProgressTask) -> RenderableType:
        p = ProgressBar
        # return MultiProgressBar(console=c, manager=task.fields["manager"])
        status: WorkerManagerStatus = task.fields["manager"].get_status()
        bar_total = status.num_workers + status.queue_capacity
        red_bar = status.num_active_workers
        green_bar = status.num_idle_workers
        blue_bar = status.num_items_in_queue
        gray_bar = bar_total - red_bar - green_bar - blue_bar

        # todo - figure out relative widths of the bars

        # console.log(status)
        # console.rule(f"Manager {manager}")
        # console.print(f"{red_bar=} {green_bar=} {blue_bar=} {gray_bar=}")

        # uncertainty character (plus/minus): ±

        self._bars = [
            ProgressBar(total=bar_total, completed=red_bar, complete_style="red"),
            NewLine(),
            ProgressBar(total=bar_total, completed=green_bar, complete_style="green"),
            NewLine(),
            ProgressBar(total=bar_total, completed=blue_bar, complete_style="blue"),
            NewLine(),
            ProgressBar(total=bar_total, completed=gray_bar, complete_style="#cccccc"),
            NewLine(),
            Text(
                f"Queue - {status.num_active_workers} active, {status.num_idle_workers} idle, "
                f"{status.num_items_in_queue} in queue ({status.queue_capacity} max)\n"
                f"{status.num_items_processed} processed, {status.idle_time_percent:.2f}% idle"
            ),
            *[
                Text(
                    f"{w.name} {w.processing_rate:.2f} it/s\t"
                    f"mean {w.mean_processing_time.total_seconds():.2f}s"
                    f" ± {w.processing_time_stdev.total_seconds():.2f}s"
                )
                for w in status.worker_stats
            ],
        ]
        return Panel(Group(*self._bars), title="Workers", height=16)


# have -
#  - number of items in queue
#  - number of workers
#    - number of idle workers
#    - number of busy workers
#  - number of sources
#  - number of sinks
#  - number of items processed
#  - number of items processed per second
#  - number of items processed per second per worker
#  - idle worker time ratio


async def main() -> None:

    default_cols = Progress.get_default_columns()
    used_cols = (default_cols[0], MyProgressColumn(), *default_cols[2:])

    p = Progress(*used_cols, console=c)

    mega_source = merge_async_iterables(some_source(), some_source(), some_source())
    managers, chained_aiter = pipe_pooled(mega_source, mul_two, spell_number)

    with p:
        for i, man in enumerate(managers):
            p.add_task(f"man {i}", manager=man)

        async for x in chained_aiter:
            c.print(x)

    # d = Dashboard(c)
    # async with d as dash:
    #
    #     for manager in managers:
    #         dash.register_manager(manager)
    #
    #     async for result in chained_aiter:
    #         pass


if __name__ == "__main__":

    asyncio.run(main())
