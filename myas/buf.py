from __future__ import annotations

import asyncio
import operator
from abc import ABC
from asyncio import Event, Queue, Task, Future, SubprocessProtocol, transports, StreamReader
from collections import deque, defaultdict
from typing import (
    Generic,
    TypeVar,
    Iterable,
    Sequence,
    Container,
    MutableSequence,
    Protocol,
    Iterator,
    AsyncIterable,
    AsyncIterator,
    Awaitable,
    Literal,
    ClassVar,
    Callable,
    ParamSpec,
    NewType,
    TypeAlias,
    Any,
)

import rich
from loguru import logger

from myas import pipe_async_iterable

_InputT = TypeVar("_InputT")
_OutputT = TypeVar("_OutputT", covariant=True)


class _BufferProcessorBase(Protocol[_InputT, _OutputT]):

    _buffer: MutableSequence[_InputT]

    def __next__(self) -> _OutputT:
        ...

    def __iter__(self) -> Iterator[_OutputT]:
        ...

    def update_buffer(self, new_data: Iterable[_InputT]) -> None:
        ...

    async def process_buffer(self) -> None:
        ...


class BufferProcessor(Generic[_InputT, _OutputT], AsyncIterable[_OutputT], ABC):
    """Buffer processor."""

    _input_buffer: MutableSequence[_InputT]

    def __init__(self) -> None:
        """Initialize the buffer processor."""
        self._input_buffer_updated_event = Event()
        self._output_buffer_updated_event = Event()
        self._output_buffer: deque[_OutputT] = deque()
        self._finished: Future[None] = Future()

        in_update_fut = asyncio.ensure_future(self._input_buffer_updated_event.wait())
        in_update_fut.add_done_callback(self._process_buffer)

    def set_finished(self) -> None:
        """Set the finished future."""
        self._finished.set_result(None)

    def update_buffer(self, new_data: Iterable[_InputT]) -> None:
        """Update the buffer."""
        self._input_buffer.extend(new_data)
        self._input_buffer_updated_event.set()

    def _process_buffer(self, _: Task[Literal[True]]) -> None:
        """Process the buffer."""
        self._input_buffer_updated_event.clear()
        asyncio.create_task(self.process_buffer())

    async def process_buffer(self) -> None:
        """Process the buffer."""
        raise NotImplementedError

    def __aiter__(self) -> AsyncIterator[_OutputT]:
        """Return the async iterator."""
        return self

    async def __anext__(self) -> _OutputT:
        """Return the next item."""

        while True:
            if self._output_buffer:
                return self._output_buffer.popleft()
            elif self._finished.done():
                raise StopAsyncIteration
            else:
                self._output_buffer_updated_event.clear()
                output_buf_fut = asyncio.ensure_future(self._output_buffer_updated_event.wait())
                done, pending = await asyncio.wait(
                    (output_buf_fut, self._finished),
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if output_buf_fut in done:
                    return self._output_buffer.popleft()


class ProcessLineProcessor(BufferProcessor[bytes, bytes]):
    """Process line processor."""

    _line_separator: ClassVar[bytes] = b"\n"

    def __init__(self) -> None:
        """Initialize the process line processor."""
        super().__init__()
        self._input_buffer = list[bytes]()

    async def process_buffer(self) -> None:
        """Process the buffer."""
        logger.debug("Processing buffer")
        input_str = b"".join(self._input_buffer)
        if input_str.endswith(self._line_separator):
            last_sep = input_str.rfind(self._line_separator)
            input_str, remaining = input_str[:last_sep], input_str[last_sep + 1 :]
            self._input_buffer = [remaining]

        lines = input_str.split(self._line_separator)
        self._output_buffer.extend(lines)
        self._output_buffer_updated_event.set()


class LineDecoder(BufferProcessor[bytes, str]):
    """Line decoder."""

    async def process_buffer(self) -> None:
        """Process the buffer."""
        for line in self._input_buffer:
            self._output_buffer.append(line.decode())
        self._output_buffer_updated_event.set()


_InputT_contra = TypeVar("_InputT_contra", contravariant=True)
P = ParamSpec("P")


class StreamTransformer(Protocol[P, _InputT_contra, _OutputT]):
    """Stream transformer."""

    def __call__(
        self,
        stream: AsyncIterable[_InputT_contra],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> AsyncIterable[_OutputT]:
        ...


BytesLine = NewType("BytesLine", bytes)


async def aiter_partition(
    stream: AsyncIterable[_InputT],
    separator: _InputT,
    include_sep: Literal["before", "after", "no"] = "no",
    partition_identifier: Callable[[_InputT, _InputT], bool] = operator.eq,
    base_buffer: MutableSequence[_InputT] | None = None,
) -> AsyncIterator[_InputT]:
    """Aggregate lines from the stream."""

    buffer: MutableSequence[_InputT] = list[_InputT]() if base_buffer is None else base_buffer
    async for data in stream:

        if not partition_identifier(data, separator):
            buffer.append(data)
            continue

        if include_sep == "after":
            buffer.append(data)

        for item in buffer:
            yield item

        buffer.clear()
        if include_sep == "before":
            buffer.append(data)

    if buffer:
        for item in buffer:
            yield item


async def buffer_line_partition(
    stream: AsyncIterable[bytes],
    line_separator: bytes = b"\n",
) -> AsyncIterator[BytesLine]:
    """Aggregate lines from the stream."""
    logger.debug("Starting buffer line aggregator")
    buffer = bytearray()

    async for data in stream:

        buffer.extend(data)
        if line_separator not in data:
            continue

        last_sep = buffer.rfind(line_separator)
        buffer, remaining = buffer[:last_sep], buffer[last_sep + 1 :]
        for line in buffer.split(line_separator):
            yield BytesLine(bytes(line))

        buffer = bytearray(remaining)

    if buffer:
        yield BytesLine(bytes(buffer))


async def process_liner(
    stream: StreamReader,
    line_separator: bytes = b"\n",
) -> AsyncIterable[bytes]:
    """Process lines."""


_StderrOutputT = TypeVar("_StderrOutputT", covariant=True)
_StdoutOutputT = TypeVar("_StdoutOutputT", covariant=True)


class BaseSubprocessor(Generic[_StdoutOutputT, _StderrOutputT], SubprocessProtocol):
    """Subprocess buffer processor."""

    _stdout_buffer: BufferProcessor[bytes, _StdoutOutputT]
    _stderr_buffer: BufferProcessor[bytes, _StderrOutputT]


class EncodedLineYielderProcessor(BaseSubprocessor[str, str]):
    def __init__(self) -> None:
        """Initialize the subprocess buffer processor."""
        self._stdout_buffer_processor = ProcessLineProcessor()
        self._stderr_buffer_processor = ProcessLineProcessor()
        self._transport: transports.SubprocessTransport | None = None

    def connection_made(self, transport: transports.SubprocessTransport) -> None:  # type: ignore
        """Process the connection made."""
        self._transport = transport

    def pipe_data_received(self, fd: int, data: bytes) -> None:
        """Process the pipe data."""
        if fd == 1:
            self._stdout_buffer_processor.update_buffer(data)
        elif fd == 2:
            self._stderr_buffer_processor.update_buffer(data)
        else:
            raise ValueError(f"Unknown fd: {fd}")

    def pipe_connection_lost(self, fd: int, exc: Exception | None) -> None:
        """Process the pipe connection lost."""

        if exc is not None:
            raise exc

        if fd == 0:
            if self._transport is None:
                raise RuntimeError("Transport is None")
            self._transport.close()
        elif fd == 1:
            self._stdout_buffer_processor.set_finished()
        elif fd == 2:
            self._stderr_buffer_processor.set_finished()
        else:
            raise ValueError(f"Unknown fd: {fd}")

    async def on_stdout_line(self, line: bytes) -> None:
        """Process the stdout line."""
        raise NotImplementedError

    async def on_stderr_line(self, line: bytes) -> None:
        """Process the stderr line."""
        raise NotImplementedError

    def stdout(self) -> AsyncIterable[_StdoutOutputT]:
        """Return the stdout."""
        return pipe_async_iterable(self._stdout_buffer_processor, self.on_stdout_line)

    def stderr(self) -> AsyncIterable[_StderrOutputT]:
        """Return the stderr."""
        return aiter(self._stderr_buffer_processor)

    # def process_exited(self) -> None:
    #     """Process the process exited."""
    #     self._transport.close()


class LinePrinterSubprocessor(BaseSubprocessor[bytes, bytes]):
    """Subprocess buffer processor."""

    def __init__(self, proc: asyncio.subprocess.Process) -> None:
        """Initialize the subprocess buffer processor."""
        super().__init__()
        self._stdout_buffer_processor = ProcessLineProcessor()
        self._stderr_buffer_processor = ProcessLineProcessor()


T = TypeVar("T")


class _LeafSentinelT:
    pass


LeafSentinel = _LeafSentinelT()
TrieNode: TypeAlias = defaultdict[T, dict[T, "TrieNode[T]"]]


async def main() -> None:
    # transport, proc = await asyncio.get_event_loop().subprocess_exec(
    #     BaseSubprocessor,
    #     "fd",
    #     "/home/pedro/projs",
    #     stdout=asyncio.subprocess.PIPE,
    #     stderr=asyncio.subprocess.PIPE,
    # )
    # async for line in proc.stdout():
    #     print(f"stdout: {line!r}")

    proc = await asyncio.create_subprocess_exec(
        "fd",
        ".",
        "/home/pedro/projs",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    assert proc.stdout is not None

    trie: TrieNode[BytesLine] = defaultdict(dict)

    crt_node = trie
    path: list[BytesLine] = []
    async for line in aiter_partition(proc.stdout, separator=b"/"):
        rich.print(f"stdout: {line!r}")

    await proc.wait()

    if proc.stderr is not None:
        async for line in proc.stderr:
            print(f"stderr: {line!r}")

    if proc.returncode != 0:
        raise RuntimeError(f"Process exited with code {proc.returncode}")


if __name__ == "__main__":
    asyncio.run(main())
