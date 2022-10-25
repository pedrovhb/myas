from __future__ import annotations

import asyncio.subprocess
import functools
import os.path
import time
from asyncio import (
    IncompleteReadError,
    LimitOverrunError,
    events,
    protocols,
    streams,
    subprocess,
    transports,
)
from asyncio.subprocess import Process, SubprocessStreamProtocol
from concurrent.futures import ProcessPoolExecutor
from hashlib import md5
from os import PathLike
from pathlib import Path
from typing import (
    Any,
    AsyncIterable,
    AsyncIterator,
    Callable,
    Concatenate,
    Coroutine,
    Iterable,
    Iterator,
    Literal,
    NamedTuple,
    ParamSpec,
    Protocol,
    Tuple,
    Type,
    TypeGuard,
    TypeVar,
    cast,
)

import rich
from mypy_extensions import Arg

from myas import amap, amerge, iter_to_aiter, merge_async_iterables

P = ParamSpec("P")
R = TypeVar("R")
T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)

PIPE: Literal[-1] = -1
STDOUT: Literal[-2] = -2
DEVNULL: Literal[-3] = -3


_DEFAULT_PIPE_BUFFER = 2**16  # 64 KiB (same as streams.FlowControlMixin._DEFAULT_LIMIT)

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

    def __init__(
        self,
        transport: transports.BaseTransport,
        protocol: protocols.BaseProtocol,
        loop: events.AbstractEventLoop,
        line_separator: bytes | None = b"\n",
        extra_info: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(transport, protocol, loop)
        self.line_separator = line_separator
        self._aiter = aiter(self.combined_output)
        self._extra_info = extra_info

    async def line_iterator(
        self,
        stream: streams.StreamReader,
        sep: bytes | None,
        strip_sep: bool = True,
    ) -> AsyncIterator[bytes]:
        if sep is None:
            while True:
                ln = await stream.read(1)
                if ln == b"":
                    return
                yield ln
        else:
            while True:
                try:
                    line = await stream.readuntil(sep)
                except IncompleteReadError as e:
                    if e.partial:
                        yield e.partial
                    return
                except LimitOverrunError as e:
                    return
                if strip_sep:
                    line = line[: -len(sep)]
                yield line

    @property
    def stdout_lines(self) -> AsyncIterator[bytes]:
        return self.line_iterator(self.stdout, self.line_separator)

    @property
    def stderr_lines(self) -> AsyncIterable[bytes]:
        return self.line_iterator(self.stderr, self.line_separator)

    async def feed_from_async_iterable(
        self,
        ait: AsyncIterable[bytes],
        close_when_done: bool = False,
    ) -> None:
        async for data in ait:
            self.write(data)
            await self.drain()

        if close_when_done:
            self.stdin.write_eof()

    def write(self, data: bytes) -> None:
        """Write data to stdin."""
        self.stdin.write(data)

    def write_line(self, data: bytes) -> None:
        """Write a line to stdin."""
        self.write(data + self.line_separator if self.line_separator else data)

    async def drain(self) -> None:
        """Wait until the stdin buffer is drained."""
        return await self.stdin.drain()

    async def reversed_lines(self) -> AsyncIterable[bytes]:
        async for line in self.stdout:
            yield line[::-1]

    def __iter__(self) -> Iterator[asyncio.StreamReader]:
        """Make it possible to do `stdout, stderr = proc`."""
        return iter((self.stdout, self.stderr))

    @property
    def combined_output(self) -> AsyncIterator[bytes]:
        return amerge(
            self.line_iterator(self.stdout, self.line_separator),
            self.line_iterator(self.stderr, self.line_separator),
        )

    def __aiter__(self) -> AsyncIterator[bytes]:
        return self

    async def __anext__(self) -> bytes:
        return await anext(self._aiter)

    def __repr__(self) -> str:
        return f"<Proc {self.pid} {self._extra_info}>"

    @classmethod
    async def run_shell(
        cls: Type[_ProcT],
        cmd: str | bytes,
        limit: int = _DEFAULT_PIPE_BUFFER,
        line_separator: bytes | None = b"\n",
        **kwds: Any,  # todo - type kwds
    ) -> _ProcT:
        loop = events.get_running_loop()
        transport, protocol = await loop.subprocess_shell(
            lambda: SubprocessStreamProtocol(limit=limit, loop=loop),
            cmd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            **kwds,
        )
        return cls(
            transport, protocol, loop, line_separator=line_separator, extra_info={"cmd": cmd}
        )

    @classmethod
    async def run_exec(
        cls: Type[_ProcT],
        program: str | bytes | PathLike[str] | PathLike[bytes],
        *args: str | bytes | PathLike[str] | PathLike[bytes],
        limit: int = _DEFAULT_PIPE_BUFFER,
        line_separator: bytes | None = b"\n",
        **kwds: Any,  # todo - type kwds
    ) -> _ProcT:
        # mostly copied over from asyncio.create_subprocess_exec()
        loop = events.get_running_loop()
        transport, protocol = await loop.subprocess_exec(
            lambda: SubprocessStreamProtocol(limit=limit, loop=loop),
            program,
            *args,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            **kwds,
        )
        return cls(
            transport,
            protocol,
            loop,
            line_separator=line_separator,
            extra_info={"program": program, "args": args},
        )


class DuOutput(NamedTuple):
    size: int
    path: Path

    @classmethod
    def from_line(cls, line: bytes) -> DuOutput:
        size, path = line.split(b"\t")
        return cls(int(size), Path(path.decode()))


class DuProc(Proc):
    @classmethod
    async def over_files(
        cls,
        files: AsyncIterable[str | bytes | PathLike[str] | PathLike[bytes]]
        | Iterable[str | bytes | PathLike[str] | PathLike[bytes]],
        **kwargs: Any,
    ) -> DuProc:
        proc = await cls.run_exec("du", "--files0-from=-", **kwargs)

        if not isinstance(files, AsyncIterable):
            _files = iter_to_aiter(files)
        else:
            _files = files

        async def feed_data() -> None:
            async for path in _files:
                if isinstance(path, bytes):
                    proc.write(path + b"\0")
                else:
                    proc.write(str(path).encode() + b"\0")

            proc.stdin.write_eof()
            proc.stdin.close()

        asyncio.create_task(feed_data(), name="feed_data")
        proc.ait = aiter(amap(DuOutput.from_line, proc.stdout_lines))
        return proc

    # type ignore because we're kind of breaking the lsp here
    async def __anext__(self) -> DuOutput:  # type: ignore
        result = await super().__anext__()
        return DuOutput.from_line(result)

    def __aiter__(self) -> AsyncIterator[DuOutput]:  # type: ignore
        return self


async def main() -> None:
    # loop = asyncio.get_running_loop()

    # async def print_lines() -> None:
    #     await asyncio.sleep(1)
    #     print(asyncio.all_tasks())
    #
    # asyncio.create_task(print_lines(), name="print_lines")

    # noinspection PyArgumentList
    # proc_fd = await Proc.run_exec(
    #     "fd",
    #     # "--print0",
    # )
    # # noinspection PyArgumentList
    #
    # proc = await Proc.run_exec(
    #     "du",
    #     "--files0-from=-",
    #     # "--time",
    #     # "--time-style=full-iso",
    # )
    # sout, serr = proc
    #
    # async def write_to_du() -> None:
    #     # for file in Path(".").iterdir():
    #     #     await asyncio.sleep(0.3)
    #     #     proc.stdin.write(file.name.encode("utf-8") + b"\0")
    #     f_out, f_err = proc_fd
    #     async for ln in f_out:
    #         proc.stdin.write(ln.strip() + b"\0")
    #     proc.stdin.write_eof()
    #
    # print(f"{isinstance(proc, Proc)=}")
    # print(f"{isinstance(proc, Process)=}")
    #
    # asyncio.create_task(write_to_du(), name="write_to_du")
    # async for line in proc.stdout:
    #     print("du", line)
    #
    # async for line in proc:
    #     print("duerr", line)
    #
    # await proc.wait()
    # await proc_fd.wait()
    #
    # async def afiles(p: Path) -> AsyncIterable[Path]:
    #     for file in p.iterdir():
    #         yield file

    # dp = await DuProc.over_files(afiles(Path("/home/pedro/Downloads")))
    # proc_fd = await Proc.run_exec(
    #     "fd",
    #     "--print0",
    #     ".",
    #     "/home/pedro",
    #     "-e",
    #     "mp4",
    #     "-e",
    #     "jpg",
    #     line_separator=b"\0",
    # )
    # # async for line in DuProc.over_files | amap(lambda l: l + b"\0", proc_fd.stdout_lines):
    # # async for line in (DuProc.over_files | amap(lambda l: l + b"\0", proc_fd.stdout_lines)):
    # du_out = await DuProc.over_files(proc_fd.stdout_lines)
    # async for o in du_out:
    #     print(o)
    # exit()

    t = time.perf_counter()
    # fd = await Proc.run_exec(
    #     "fd",
    #     "--print0",
    #     ".",
    #     "/home/pedro",
    #     "-e",
    #     "mp4",
    #     # line_separator=None,
    #     line_separator=b"\0",
    # )
    # fd2 = await Proc.run_exec(
    #     "fd",
    #     "--print0",
    #     ".",
    #     "/home/pedro/Pictures",
    #     "-e",
    #     "jpg",
    #     # line_separator=None,
    #     line_separator=b"\0",
    # )

    fd = await Proc.run_exec(
        "fd",
        "--print0",
        ".",
        "/home/pedro",
        "-e",
        "mp4",
        "-e",
        "jpg",
        "-S",
        "+5M",
        # line_separator=None,
        line_separator=b"\0",
    )

    dp2 = await DuProc.over_files(fd)
    async for du_file in dp2:
        print(du_file)

    # async for line in fd_serr:
    #     print(line)
    rich.print(f"{time.perf_counter() - t=}")


big_file = "/dev/shm/random1"
cmds = [
    ["fd", "--print0", ".", "/home/pedro", "-uu"],
    ["du", "-h", "--files0-from=-"],
]

cmds = [
    ["cat", big_file],
    ["zstd", "-"],
]

cmds = [
    ["cat", big_file],
    ["md5sum", "-"],
]

OUT_READ_SIZE = 1024 * 1024 * 10


async def direct_md5() -> bytes:
    # f = os.open(big_file, os.O_RDONLY)
    process_1 = await asyncio.create_subprocess_exec(
        "md5sum",
        "-",
        stdin=Path(big_file).open("rb"),
        stdout=Path(big_file + ".md5").open("wb"),
    )
    await process_1.wait()
    return Path(big_file + ".md5").read_bytes()
    # os.close(f)
    # async for line in process_1.stdout:
    #     yield line
    # process_2 = await asyncio.create_subprocess_exec(
    #     *cmds[1],
    #     stdin=read,
    #     stdout=PIPE,
    # )
    # os.close(read)
    # assert process_2.stdout is not None
    #
    # while True:
    #     line = await process_2.stdout.read(OUT_READ_SIZE)
    #     if not line:
    #         break
    #     yield line


async def direct_pipe_example() -> AsyncIterator[bytes]:
    read, write = os.pipe()
    process_1 = await asyncio.create_subprocess_exec(
        *cmds[0],
        stdout=write,
    )
    os.close(write)
    process_2 = await asyncio.create_subprocess_exec(
        *cmds[1],
        stdin=read,
        stdout=PIPE,
    )
    os.close(read)
    assert process_2.stdout is not None

    while True:
        line = await process_2.stdout.read(OUT_READ_SIZE)
        if not line:
            break
        yield line


async def aio_reader_writer_example() -> AsyncIterator[bytes]:
    process_1 = await asyncio.create_subprocess_exec(
        *cmds[0],
        stdout=asyncio.subprocess.PIPE,
    )
    process_2 = await asyncio.create_subprocess_exec(
        *cmds[1],
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
    )

    assert process_1.stdout is not None
    assert process_2.stdin is not None

    async def fillit() -> None:
        while True:
            line = await process_1.stdout.read(OUT_READ_SIZE)
            if not line:
                break
            process_2.stdin.write(line)
        process_2.stdin.write_eof()

    asyncio.create_task(fillit())

    while True:
        line = await process_2.stdout.read(OUT_READ_SIZE)
        if not line:
            break
        yield line


async def compare_pipes() -> None:

    con = rich.get_console()
    con.rule()
    con.print("direct_pipe_example")
    t1 = time.perf_counter()
    out_direct_pipe = [ln async for ln in direct_pipe_example()]
    t2 = time.perf_counter()
    con.print(f"Got result in {t2 - t1:.5f} seconds: {len(out_direct_pipe)} lines")
    con.print(f"Output: {out_direct_pipe!r}")

    con.rule()
    con.print("direct_md5")
    t1 = time.perf_counter()
    out_direct_md5 = await direct_md5()
    t2 = time.perf_counter()
    con.print(f"Got result in {t2 - t1:.5f} seconds: {len(out_direct_md5)} lines")
    con.print(f"Output: {out_direct_md5!r}")

    con.rule()
    con.print("aio_reader_writer_example")
    t1 = time.perf_counter()
    out_aio_reader_writer = [ln async for ln in aio_reader_writer_example()]
    t2 = time.perf_counter()
    con.print(f"Got result in {t2 - t1:.5f} seconds: {len(out_aio_reader_writer)} lines")
    con.print(f"Hash output: {out_aio_reader_writer}")

    con.rule()
    con.print("python md5 off hashlib")
    t1 = time.perf_counter()
    py_md5 = md5(Path(big_file).read_bytes()).hexdigest().encode()
    t2 = time.perf_counter()
    con.print(f"Got result in {t2 - t1:.5f} seconds: {len(py_md5)} lines")
    con.print(f"Output: {py_md5!r}")

    # print("direct_pipe_example")
    # t = time.perf_counter()
    # dp = direct_pipe_example()
    # first = await anext(dp)
    # t_first = time.perf_counter() - t
    # async for line in dp:
    #     pass
    # t_remaining = time.perf_counter() - t - t_first
    # print(f"{t_first=}")
    # print(f"{t_remaining=}")
    #
    # print("aio_reader_writer_example")
    # t = time.perf_counter()
    # ar = aio_reader_writer_example()
    # first = await anext(ar)
    # t_first = time.perf_counter() - t
    # async for line in ar:
    #     pass
    # t_remaining = time.perf_counter() - t - t_first
    # print(f"{t_first=}")
    # print(f"{t_remaining=}")

    output = Path(big_file + ".md5")
    Path(big_file + ".md5").unlink(missing_ok=True)
    print("direct_md5")
    t1 = time.perf_counter()
    await direct_md5()
    t2 = time.perf_counter() - t1
    print(f"Time taken was {t2=:0.5f} seconds")
    print(output.read_text())


if __name__ == "__main__":
    # asyncio.run(main())
    t = time.perf_counter()
    out = asyncio.run(compare_pipes())
    # print(out)
    print(f"{time.perf_counter() - t=}")
