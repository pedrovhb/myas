from __future__ import annotations

import asyncio.subprocess
import functools
from asyncio import subprocess, streams, IncompleteReadError
from asyncio.subprocess import Process
from os import PathLike
from pathlib import Path
from typing import (
    Any,
    Protocol,
    TypeVar,
    ParamSpec,
    Callable,
    Literal,
    TypeGuard,
    Coroutine,
    cast,
    AsyncIterable,
    Iterator,
    Tuple,
    Concatenate,
    AsyncIterator,
    NamedTuple,
    Type,
)

from mypy_extensions import Arg

from myas import amerge

P = ParamSpec("P")
R = TypeVar("R")
T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)

PIPE: Literal[-1] = -1
STDOUT: Literal[-2] = -2
DEVNULL: Literal[-3] = -3

_ProcT = TypeVar("_ProcT", bound="Proc")


def _process_verifier_wrapper(
    cls: Type[_ProcT],
    fn: Callable[P, Coroutine[Any, Any, _ProcT]],
) -> Callable[P, Coroutine[Any, Any, _ProcT]]:
    @functools.wraps(fn)
    async def _verify_output(*args: P.args, **kwargs: P.kwargs) -> _ProcT:
        proc = await fn(*args, **kwargs)
        _proc = cast(_ProcT, cls.from_process(proc))
        return _proc

    return _verify_output


# def _run_exec(cls: Type[_ProcT])


async def line_iterator(
    stream: streams.StreamReader,
    sep: bytes,
    strip_sep: bool = True,
) -> AsyncIterator[bytes]:
    while True:
        try:
            line = await stream.readuntil(sep)
        except IncompleteReadError as e:
            if e.partial:
                yield e.partial
            break
        if strip_sep:
            line = line[: -len(sep)]
        yield line


class Proc(Process, AsyncIterable[bytes]):
    """Simplified version of asyncio.subprocess.Process.

    - Asserts that stdout, stderr, and stdin are streams.
    - Adds a few convenience methods:
        - write                     (write to stdin)
        - write_line                (writes to stdin, separated by line_separator)
        - drain                     (waits until the stdin buffer is drained)
        - reversed_lines            (reverses each line of stdout)
        - __iter__                  (returns stdout, stderr)
        - __aiter__                 (returns combined output)


    - Adds a few convenience properties:
        - combined_output           (returns a merged stream of stdout and stderr, correctly
                                    split by line_separator)
        - stdout_lines              (async iterator over stdout, separated by line_separator)
        - stderr_lines              (async iterator over stderr, separated by line_separator)


    """

    stdin: asyncio.StreamWriter
    stdout: asyncio.StreamReader
    stderr: asyncio.StreamReader

    # line_separator: bytes

    def _initialize(self, line_separator: bytes = b"\n") -> None:
        self.line_separator = line_separator

    @classmethod
    def from_process(cls, process: Process, line_separator: bytes = b"\n") -> Proc:
        process.__class__ = cls
        if process.stdout is None or process.stderr is None or process.stdin is None:
            raise ValueError("Process is not connected")
        proc = cast(Proc, process)

        proc._initialize(line_separator)
        return proc

    def write(self, data: bytes) -> None:
        """Write data to stdin."""
        self.stdin.write(data)

    def write_line(self, data: bytes) -> None:
        """Write a line to stdin."""
        self.write(data + self.line_separator)

    async def drain(self) -> None:
        return await self.stdin.drain()

    async def reversed_lines(self) -> AsyncIterable[bytes]:
        async for line in self.stdout:
            yield line[::-1]

    def __iter__(self) -> Iterator[asyncio.StreamReader]:
        return iter((self.stdout, self.stderr))

    @property
    def combined_output(self) -> AsyncIterator[bytes]:
        return amerge(
            line_iterator(self.stdout, self.line_separator),
            line_iterator(self.stderr, self.line_separator),
        )

    def __aiter__(self) -> AsyncIterator[bytes]:
        return self.combined_output

    run_exec = classmethod(_process_verifier_wrapper(subprocess.create_subprocess_exec))
    run_shell = classmethod(_process_verifier_wrapper(subprocess.create_subprocess_shell))


class DuOutput(NamedTuple):
    size: int
    path: Path

    @classmethod
    def from_line(cls, line: bytes) -> DuOutput:
        size, path = line.split(b"\t")
        return cls(int(size), Path(path.decode()))


class DuProc(Proc):
    @classmethod
    async def over_files(cls, files: AsyncIterable[PathLike[Any]], **kwargs: Any) -> DuProc:
        return await cls.run_exec("du", "-b")


async def main() -> None:
    loop = asyncio.get_running_loop()

    # noinspection PyArgumentList
    proc_fd = await Proc.run_exec(
        "fd",
        # "--print0",
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        bufsize=0,
    )
    # noinspection PyArgumentList

    proc = await Proc.run_exec(
        "du",
        "--files0-from=-",
        # "--time",
        # "--time-style=full-iso",
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        bufsize=0,
    )
    sout, serr = proc

    async def write_to_du() -> None:
        # for file in Path(".").iterdir():
        #     await asyncio.sleep(0.3)
        #     proc.stdin.write(file.name.encode("utf-8") + b"\0")
        async for ln in proc_fd.reversed_lines():
            proc.stdin.write(ln.strip() + b"\0")
        proc.stdin.write_eof()

    print(f"{isinstance(proc, Proc)=}")
    print(f"{isinstance(proc, Process)=}")

    asyncio.create_task(write_to_du())
    async for line in proc.stdout:
        print("du", line)

    async for line in proc:
        print("duerr", line)

    await proc.wait()
    await proc_fd.wait()


if __name__ == "__main__":
    asyncio.run(main())
