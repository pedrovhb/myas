"""The utils module contains various utility functions.

`compose`, `pipe`, and friends have their own files because the typing is verbose and unsightly.
It's not pretty, but it works, and it's also how the standard library does it ¯\\_(ツ)_/¯
"""

from .aiter_utils import (
    aiter_to_queue,
    gather_async_iterators,
    iter_to_aiter,
    merge_async_iterators,
    queue_to_aiter,
    run_on_thread,
    run_sync,
)
from .compose import compose, ensure_coroutine
from .pipe import async_iterable_pipe, pipe

__all__ = [
    "compose",
    "pipe",
    "async_iterable_pipe",
    "ensure_coroutine",
    "run_sync",
    "run_on_thread",
    "iter_to_aiter",
    "merge_async_iterators",
    "queue_to_aiter",
    "aiter_to_queue",
    "gather_async_iterators",
]
