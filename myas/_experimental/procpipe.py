from __future__ import annotations

import asyncio
import os
import time
from asyncio import StreamWriter, StreamReader
from asyncio.subprocess import Process
import io
from os import memfd_create
from pathlib import Path
from typing import AsyncIterable, Iterable, AsyncIterator, Iterator, IO, Any

from loguru import logger

from myas import amerge


class ProcessPipe(AsyncIterable[bytes], Iterable[AsyncIterator[bytes]]):
    def __init__(
        self,
        cmd: str,
        *args: str,
        stdin: int | IO[Any] | None = None,
        stdout: int | IO[Any] | None = None,
        stderr: int | IO[Any] | None = None,
    ) -> None:
        self.cmd = cmd
        self.args = args
        self._process: Process | None = None
        self._stdout = stdout
        self._stderr = stderr
        self._stdin = stdin
        # self._combined_output: AsyncIterator[bytes] | None = None
        self._started = asyncio.Event()

    async def start(self) -> None:

        # if self._stdin is None:
        #     self._stdin = asyncio.subprocess.PIPE

        self._process = await asyncio.create_subprocess_exec(
            self.cmd,
            *self.args,
            stdout=self._stdout,
            stderr=self._stderr,
            stdin=self._stdin,
        )
        self._stdout = self._process.stdout
        self._stderr = self._process.stderr

        # self._stdin = self._process.stdin
        self._combined_output = amerge(self._stdout, self._stderr)
        self._started.set()

    @property
    def stdout(self):
        return self._stdout

    @property
    def stderr(self):
        return self._stdout

    async def _procfd_iterator(self, fd: str) -> AsyncIterator[bytes]:
        if not self._started.is_set():
            await self.start()

        if fd == "stdout":
            async_iterator = self._stdout
        elif fd == "stderr":
            async_iterator = self._stderr
        else:
            raise ValueError("Invalid file descriptor")

        async for line in async_iterator:
            yield line

    def __aiter__(self) -> AsyncIterator[bytes]:
        return self

    def __iter__(self) -> Iterator[AsyncIterator[bytes]]:
        # todo - create stand-ins for stdout and stderr that are filled when the process is created,
        #  and then return them here (to avoid having to start the process immediately in a possibly
        #  synchronous context)
        # if self._stdout is None or self._stderr is None:
        #     raise RuntimeError("Process not started")
        return iter((self._procfd_iterator("stdout"), self._procfd_iterator("stderr")))

    async def __anext__(self) -> bytes:
        if not self._started.is_set():
            await self.start()

        if self._combined_output is None:
            raise RuntimeError("Process not started")

        return await self._combined_output.__anext__()

    async def __aenter__(self) -> ProcessPipe:
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._process is None:
            raise RuntimeError("Process not started")
        # self._process.terminate()
        await self._process.wait()


class LineSource(asyncio.Protocol):
    def connection_made(self, transport: asyncio.Transport) -> None:
        self.transport = transport

    def data_received(self, data: bytes) -> None:
        self._buf += data
        *msgs, remaining = self._buf.split(b"\n")
        self._buf = remaining
        for msg in msgs:
            self.transport.write(msg)

    def eof_received(self) -> None:
        if self._buf:
            self.transport.write(self._buf)
        self.transport.write_eof()


class LineSink(asyncio.Protocol):
    def __init__(self, transport):
        self.transport = transport
        self._buf = b""

    def data_received(self, data: bytes) -> None:
        self._buf += data
        *msgs, remaining = self._buf.split(b"\n")
        self._buf = remaining
        for msg in msgs:
            self.transport.write(msg)

    def eof_received(self) -> None:
        if self._buf:
            self.transport.write(self._buf)
        self.transport.write_eof()


async def do_writing(writer: StreamWriter) -> None:
    for i in range(1, 4):
        writer.write(("stuff " + str(i)).encode())
        await asyncio.sleep(1)
    writer.close()


async def do_reading(reader: StreamReader) -> None:
    while not reader.at_eof():
        some_bytes = await reader.read(2**16)
        print("here's what we got:", some_bytes)


async def main() -> None:
    # proc = ProcessPipe("ls", "-l")
    # async for line in proc:
    #     logger.info(line)

    t = time.perf_counter()
    p = Path("t")
    p.touch()
    # fd = p.open("r+b")
    # fd = BytesIO()

    loop = asyncio.get_event_loop()

    read_fd, write_fd = os.pipe()
    read_io = os.fdopen(read_fd, "rb")
    write_io = os.fdopen(write_fd, "wb")

    write_io.write(b"hello world")
    write_io.close()
    r = read_io.read()
    print(r)

    exit()
    reader = asyncio.StreamReader()

    proc = await asyncio.create_subprocess_exec(
        "ls",
        stdout=asyncio.subprocess.PIPE,
    )

    read_protocol = asyncio.StreamReaderProtocol(reader)
    read_transport, _ = await loop.connect_read_pipe(lambda: read_protocol, os.fdopen(read_fd))
    write_protocol = asyncio.StreamReaderProtocol(asyncio.StreamReader())
    write_transport, _ = await loop.connect_write_pipe(
        lambda: write_protocol, os.fdopen(write_fd, "w")
    )
    writer = asyncio.StreamWriter(write_transport, write_protocol, None, loop)

    # asyncio.create_task(do_writing(writer))

    # proc_echo = await asyncio.create_subprocess_exec(
    #     "echo", stdin=os.fdopen(read_fd), stdout=asyncio.subprocess.PIPE
    # )

    await do_reading(reader)


if __name__ == "__main__":
    asyncio.run(main())
