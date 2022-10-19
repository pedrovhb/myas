import asyncio
import io
import os
from asyncio import (
    BaseProtocol,
    BaseTransport,
    StreamWriter,
    StreamReader,
    SubprocessProtocol,
    AbstractEventLoop,
    streams,
)
from asyncio.streams import FlowControlMixin, StreamReaderProtocol
from asyncio.subprocess import SubprocessStreamProtocol
from asyncio.unix_events import _UnixReadPipeTransport, _UnixWritePipeTransport
from pathlib import Path
from typing import AsyncIterable, AsyncIterator, IO

from myas import merge_async_iterables


class MyWriterProtocol(BaseProtocol):
    def __init__(self) -> None:
        self._transport = None

    def connection_made(self, transport: BaseTransport) -> None:
        self._transport = transport

    def data_received(self, data: bytes) -> None:
        print(data)


class SubStreamProt(SubprocessStreamProtocol):
    # def __init__(
    #     self,
    #     limit: int = 2**16,
    #     loop: AbstractEventLoop | None = None,
    #     astream: AsyncIterable[bytes] | None = None,
    #     stdin_transport: BaseTransport | None = None,
    # ) -> None:
    #     if loop is None:
    #         loop = asyncio.get_running_loop()
    #     super().__init__(limit=limit, loop=loop)
    #     self._stdin_transport = stdin_transport
    #     self._limit = limit
    #     self.stdin = self.stdout = self.stderr = None
    #     self._transport = None
    #     self._process_exited = False
    #     self._pipe_fds = []
    #     self._stdin_closed = self._loop.create_future()
    #     self._astream = astream

    # async def _feed_stdin(self) -> None:
    #     async for data in self._astream:
    #         self.stdin.write(data)
    #         await self.stdin.drain()

    def connection_made(self, transport: BaseTransport) -> None:
        super().connection_made(transport)
        # self.stdin.write(b"/home/pedro/webrepl.html\n")

    #     if isinstance(transport, _UnixReadPipeTransport):
    #         self._stdin_transport = transport
    #         print("stdin transport", transport)
    #         return
    #     elif isinstance(transport, _UnixWritePipeTransport):
    #         self._stdin_write_transport = transport
    #         print("stdin write transport", transport)
    #         return
    #
    #     stdout_transport = transport.get_pipe_transport(1)
    #     if stdout_transport is not None:
    #         self.stdout = streams.StreamReader(limit=self._limit, loop=self._loop)
    #         self.stdout.set_transport(stdout_transport)
    #         self._pipe_fds.append(1)
    #
    #     stderr_transport = transport.get_pipe_transport(2)
    #     if stderr_transport is not None:
    #         self.stderr = streams.StreamReader(limit=self._limit, loop=self._loop)
    #         self.stderr.set_transport(stderr_transport)
    #         self._pipe_fds.append(2)
    #
    #     self.stdin = streams.StreamWriter(
    #         self._stdin_transport, protocol=self, reader=None, loop=self._loop
    #     )
    #
    #     # self.stdin = StreamWriter(self._stdin_transport, self, None, asyncio.get_running_loop())
    #     asyncio.create_task(self._feed_stdin())

    def connection_lost(self, exc: Exception | None) -> None:
        print("cl", exc)
        super().connection_lost(exc)

    def process_exited(self) -> None:
        print("pe")
        super().process_exited()

    def pipe_data_received(self, fd: int, data: bytes) -> None:
        print("pdr", data, fd)
        super().pipe_data_received(fd, data)


async def print_output(stderr, stdout) -> None:
    async for line in merge_async_iterables(stderr, stdout):
        print(line)


class MySubpProtocol(SubprocessProtocol):
    def __init__(self):
        super().__init__()
        self._stdin_transport = None
        self._stdin_write_transport = None

    def pipe_data_received(self, fd: int, data: bytes) -> None:
        print("pdr", data, fd)
        super().pipe_data_received(fd, data)

    def connection_made(self, transport: BaseTransport) -> None:
        if isinstance(transport, _UnixReadPipeTransport):
            self._stdin_transport = transport
            print("stdin transport", transport)
            return
        elif isinstance(transport, _UnixWritePipeTransport):
            self._stdin_write_transport = transport
            self._stdin_write_transport.write(b"hi")
            print("stdin write transport", transport)
            return

        super().connection_made(transport)
        print("cm", transport)
        # print("stdin closing- {}".format(transport.get_pipe_transport(0).is_closing()))


class StdoutReaderProtocol(SubprocessProtocol):
    def __init__(self, reader, loop=None):
        self.reader_prot = StreamReaderProtocol(reader, loop=loop)

    def pipe_data_received(self, fd, data):
        if fd == 1:
            self.reader_prot.data_received(data)
        else:
            print("fd", fd, data)


class MyProtStream(SubprocessStreamProtocol):
    def __init__(self, reader, loop=None):
        super().__init__(reader, loop)
        self.finished = asyncio.get_running_loop().create_future()

    def process_exited(self) -> None:
        self.finished.set_result(None)


async def main() -> None:
    loop = asyncio.get_running_loop()
    # loop.subprocess_exec()

    # read, write = os.pipe()
    # read_fd = os.fdopen(read, "rb")
    # write_fd = os.fdopen(write, "wb")
    # w_transport, w_protocol = await loop.connect_write_pipe(lambda: MyWriterProtocol(), write_fd)
    # # sr = StreamReader()
    # # sw = StreamWriter(transport, protocol, sr, loop)
    #
    # r_transport, r_protocol = await loop.connect_read_pipe(lambda: MyReaderProtocol(), read_fd)
    # r_sr = StreamReader()
    # r_sw = StreamWriter(r_transport, r_protocol, r_sr, loop)
    #
    # p_transport, p_protocol = await loop.subprocess_exec(
    #     lambda: MyReaderProtocol(),
    #     "du",
    #     "-h",
    #     stdout=asyncio.subprocess.PIPE,
    #     stderr=asyncio.subprocess.PIPE,
    #     stdin=read_fd,
    # )

    async def feeder():
        for p in Path(".").iterdir():
            await asyncio.sleep(0.3)
            print(f"Sending {p}")
            yield p.name.encode()

    (p := Path("t.txt")).write_bytes(b"/home/pedro/webrepl.html")

    read, write = os.pipe()
    #
    fd_reader = os.fdopen(read, "rb")
    fd_writer = os.fdopen(write, "wb")

    p_fd = p.open("rb")
    os.set_inheritable(p_fd.fileno(), True)
    os.set_inheritable(fd_reader.fileno(), True)
    os.set_inheritable(fd_writer.fileno(), True)
    os.set_inheritable(read, True)
    os.set_inheritable(write, True)

    prot = MySubpProtocol()
    # transp, _ = await loop.connect_write_pipe(lambda: prot, fd_writer)

    sr_prot = StreamReaderProtocol(StreamReader(2**16))
    transp, _ = await loop.connect_read_pipe(lambda: sr_prot, fd_reader)
    transpw, _ = await loop.connect_write_pipe(lambda: prot, fd_writer)

    async def stdout_reader(stream):
        async for line in stream:
            print("stdout", line)

    transport, protocol = await loop.subprocess_exec(
        lambda: MyProtStream(2**16, loop),
        "cat",
        # fd_reader,
        # "du",
        # pass_fds=[p_fd.fileno()],
        stdin=asyncio.subprocess.PIPE,
        # stdin=p_fd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    asyncio.create_task(stdout_reader(protocol.stdout))

    async def writer():
        i = 0
        while True:
            i += 1
            protocol.stdin.write(f"hi {i}\n".encode())
            await protocol.stdin.drain()
            await asyncio.sleep(0.5)
            if i == 10:
                protocol.stdin.write_eof()
                break

    # protocol.stdin.write_eof()
    # print(await protocol.stdout.read())
    asyncio.create_task(writer())

    await protocol.finished


if __name__ == "__main__":

    asyncio.run(main())
