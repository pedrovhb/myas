from asyncio import create_subprocess_exec
from asyncio.subprocess import PIPE
from functools import partial

run_exec = partial(create_subprocess_exec, stdin=PIPE, stdout=PIPE, stderr=PIPE)


async def main() -> None:
    proc = await run_exec(
        "echo",
        "hello",
    )
    stdout, stderr = await proc.communicate()
    print(stdout.decode())
