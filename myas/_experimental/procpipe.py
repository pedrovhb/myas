from __future__ import annotations

import asyncio
from asyncio.subprocess import Process
from typing import AsyncIterable, Iterable, AsyncIterator, Iterator
from myas import amerge

from loguru import logger


class ProcessPipe(AsyncIterable[bytes], Iterable[AsyncIterator[bytes]]):
    def __init__(self, cmd: str, *args: str) -> None:
        self.cmd = cmd
        self.args = args
        self._process: Process | None = None
        self._stdout: AsyncIterator[bytes] | None = None
        self._stderr: AsyncIterator[bytes] | None = None
        # self._stdin: AsyncIterator[bytes] | None = None
        self._combined_output: AsyncIterator[bytes] | None = None
        self._started = asyncio.Event()

    async def _start(self) -> None:
        self._process = await asyncio.create_subprocess_exec(
            self.cmd,
            *self.args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            stdin=asyncio.subprocess.PIPE,
        )
        self._stdout = self._process.stdout
        self._stderr = self._process.stderr
        # self._stdin = self._process.stdin
        self._combined_output = amerge(self._stdout, self._stderr)
        self._started.set()

    async def _procfd_iterator(self, fd: str) -> AsyncIterator[bytes]:
        if not self._started.is_set():
            await self._start()

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
            await self._start()

        if self._combined_output is None:
            raise RuntimeError("Process not started")

        return await self._combined_output.__anext__()

    async def __aenter__(self) -> ProcessPipe:
        await self._start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._process is None:
            raise RuntimeError("Process not started")
        # self._process.terminate()
        await self._process.wait()


async def main() -> None:
    proc = ProcessPipe("ls", "-l")
    async for line in proc:
        logger.info(line)

    stdout, stderr = ProcessPipe("fd", ".", "/home/pedro")
    async for line in stdout:
        logger.info(line)

    async for line in stderr:
        logger.error(line)


if __name__ == "__main__":
    asyncio.run(main())
