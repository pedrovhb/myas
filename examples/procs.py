from __future__ import annotations
import asyncio
import json
import re
import textwrap
from abc import ABC
from dataclasses import dataclass
from itertools import pairwise
from os import PathLike
from pathlib import Path
from typing import (
    AsyncIterable,
    Any,
    Callable,
    Coroutine,
    AsyncIterator,
    Generic,
    TypeVar,
    ClassVar,
    Type,
    NamedTuple,
    Literal,
)

import parse

from myas.processor import WorkerManager


_ProcessOutputT = TypeVar("_ProcessOutputT", bound="ProcessOutput")


@dataclass
class ProcessOutput(ABC):
    @classmethod
    def from_results(
        cls: Type[_ProcessOutputT],
        return_code: int,
        stdout: bytes,
        stderr: bytes,
    ) -> _ProcessOutputT:
        ...


@dataclass
class Process(Generic[_ProcessOutputT], ABC):

    cmd: ClassVar[str]
    _output_class: Type[_ProcessOutputT]

    def get_args(self) -> list[str]:
        ...

    async def run(self) -> _ProcessOutputT:
        proc = await asyncio.create_subprocess_exec(
            self.cmd,
            *self.get_args(),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()
        await proc.wait()
        assert proc.returncode is not None
        return self._output_class.from_results(proc.returncode, stdout, stderr)


class JpegRecompressOutput(NamedTuple):
    """Parse from output of jpeg-recompress.

    jpeg-recompress help text:

        usage: jpeg-recompress [options] input.jpg output.jpg
        options:

          -V, --version                output program version
          -h, --help                   output program help
          -t, --target [arg]           set target quality [0.9999]
          -q, --quality [arg]          set a quality preset: low, medium, high, veryhigh [medium]
          -n, --min [arg]              minimum JPEG quality [40]
          -x, --max [arg]              maximum JPEG quality [95]
          -l, --loops [arg]            set the number of runs to attempt [6]
          -a, --accurate               favor accuracy over speed
          -m, --method [arg]           set comparison method to one of 'mpe', 'ssim', 'ms-ssim', 'smallfry' [ssim]
          -s, --strip                  strip metadata
          -d, --defish [arg]           set defish strength [0.0]
          -z, --zoom [arg]             set defish zoom [1.0]
          -r, --ppm                    parse input as PPM
          -c, --no-copy                disable copying files that will not be compressed
          -p, --no-progressive         disable progressive encoding
          -S, --subsample [arg]        set subsampling method to one of 'default', 'disable' [default]
          -T, --input-filetype [arg]   set input file type to one of 'auto', 'jpeg', 'ppm' [auto]
          -Q, --quiet                  only print out errors


    Example output:

        Metadata size is 15kb
        ssim at q=52 (10 - 95): 0.998627
        ssim at q=30 (10 - 51): 0.996449
        ssim at q=19 (10 - 29): 0.991761
        ssim at q=14 (10 - 18): 0.984787
        ssim at q=11 (10 - 13): 0.974284
        Final optimized ssim at q=10: 0.968901
        New size is 20% of original (saved 538 kb)
    """

    metadata_size: int
    optimized_quality: int
    optimized_ssim: float
    optimized_size: int
    original_size: int
    original_path: Path
    optimized_path: Path

    @classmethod
    def from_results(
        cls,
        stdout: bytes,
        stderr: bytes,
        original_path: Path,
        dst_path: Path,
    ) -> JpegRecompressOutput:

        templates = (
            "Metadata size is {metadata_size}kb",
            "Final optimized ssim at q={optimized_quality}: {optimized_ssim}",
            "New size is {optimized_size}% of original (saved {saved_kb} kb)",
        )

        output = (stdout + stderr).decode("utf-8")
        parsed_data = [parse.search(t, output).named for t in templates]
        data = {
            **parsed_data[0],
            **parsed_data[1],
            **parsed_data[2],
        }

        metadata_size = int(data["metadata_size"])
        optimized_ssim = float(data["optimized_ssim"])
        saved_kb = int(data["saved_kb"])
        saved_b = saved_kb * 1024
        optimized_size = dst_path.stat().st_size
        original_size = optimized_size + saved_b
        optimized_quality = int(data["optimized_quality"])

        result = cls(
            metadata_size=metadata_size,
            optimized_ssim=optimized_ssim,
            optimized_size=optimized_size,
            original_size=original_size,
            optimized_quality=optimized_quality,
            original_path=original_path,
            optimized_path=dst_path,
        )
        return result

    @classmethod
    async def optimize(
        cls,
        original_path: Path | PathLike[str],
        dst_path: Path | PathLike[str] | None = None,
        *,
        quality: Literal["low", "medium", "high", "veryhigh"] = "medium",
        min_quality: int = 40,
        max_quality: int = 95,
        loops: int = 6,
        accurate: bool = False,
        method: Literal["mpe", "ssim", "ms-ssim", "smallfry"] = "ssim",
        strip: bool = False,
        defish: float = 0.0,
        zoom: float = 1.0,
        ppm: bool = False,
        no_copy: bool = False,
        no_progressive: bool = False,
        subsample: Literal["default", "disable"] = "default",
    ) -> JpegRecompressOutput:

        if not isinstance(original_path, Path):
            original_path = Path(original_path)

        if dst_path is None:
            dst_path = original_path
        elif not isinstance(dst_path, Path):
            dst_path = Path(dst_path)

        flags = []
        if accurate:
            flags.append("--accurate")
        if strip:
            flags.append("--strip")
        if ppm:
            flags.append("--ppm")
        if no_copy:
            flags.append("--no-copy")
        if no_progressive:
            flags.append("--no-progressive")

        cmd = [
            "jpeg-recompress",
            f"--quality",
            str(quality),
            f"--min",
            str(min_quality),
            f"--max",
            str(max_quality),
            f"--loops",
            str(loops),
            f"--method",
            str(method),
            f"--defish",
            str(defish),
            f"--zoom",
            str(zoom),
            f"--subsample",
            str(subsample),
            *flags,
            str(original_path.absolute()),
            str(dst_path.absolute()),
        ]
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()
        await proc.wait()
        assert proc.returncode is not None
        return cls.from_results(stdout, stderr, original_path, dst_path)


def pipe_pooled(
    ait: AsyncIterable[Any],
    *funcs: Callable[..., Coroutine[Any, Any, Any]],
) -> AsyncIterator[Any]:
    """Chain multiple workers together, passing the output of one as the input of the next."""
    workers = [WorkerManager(f) for f in funcs]
    for a, b in pairwise(workers):
        b.add_source(a)
    workers[0].add_source(ait)
    for w in workers:
        asyncio.create_task(w.close())
    return aiter(workers[-1])


async def mul_two(x: int) -> int:
    return x * 2


async def spell_number(x: int) -> str:
    numbers = "zero one two three four five six seven eight nine ten".split()
    words = [numbers[int(i)] for i in str(x)]
    return " ".join(words)


async def some_source() -> AsyncIterator[int]:
    for i in range(10):
        yield i
        await asyncio.sleep(0.1)


if __name__ == "__main__":

    async def main() -> None:
        chained = pipe_pooled(some_source(), mul_two, spell_number)
        async for result in chained:
            print(result)

        path = Path(           "/home/pedro/in.jpg"        )
        dst_path = Path("/home/pedro/out.jpg")
        result = await JpegRecompressOutput.optimize(path, dst_path)

        ps = [
            p
            for p in Path("/home/pedro/Pictures").iterdir()
            if p.is_file() and p.suffix == ".jpg"
        ]

        async def async_jpgs() -> AsyncIterator[Path]:
            for p in ps:
                yield p

        man = WorkerManager(JpegRecompressOutput.optimize)
        man.add_source(async_jpgs())

        with open("out.jsonl", "w") as f:
            total_saved = 0
            async for output in man:
                print(output)
                json.dump(output._asdict(), f, default=str)
                f.write("\n")
                total_saved += output.original_size - output.optimized_size
                print(f"Total saved MB: {total_saved / 1024 / 1024:.2f}")

    asyncio.run(main())
