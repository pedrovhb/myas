from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

from rich.progress import Progress, ProgressColumn, Task as RichProgressTask
from rich.console import Console, ConsoleOptions, RenderResult
from rich.progress_bar import ProgressBar

from myas.processor import WorkerManagerBase

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
    def __init__(self, console: Console) -> None:
        total_width = console.width
        self.n_bars = 4
        colors = ["#ff0000", "#00ff00", "#0000ff", "#ffff00"]
        self._bars = [
            ProgressBar(width=total_width // self.n_bars, total=100, style=color)
            for color in colors
        ]

    def __rich_console__(self, console: Console, options: ConsoleOptions) -> RenderResult:
        for bar in self._bars:
            yield from bar.__rich_console__(console, options)


class MyProgressColumn(ProgressColumn):
    def render(self, task: RichProgressTask) -> MultiProgressBar:
        return MultiProgressBar(console=c)


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


default_cols = Progress.get_default_columns()
used_cols = (default_cols[0], MyProgressColumn(), *default_cols[2:])

p = Progress(*used_cols, console=c)

with p:
    p.add_task()
