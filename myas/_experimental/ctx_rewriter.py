from __future__ import annotations

from types import CoroutineType, FrameType
import asyncio
import textwrap
from inspect import (
    Traceback,
)
from typing import (
    AsyncIterator,
    Any,
    Generic,
    ParamSpec,
    TypeVar,
    Callable,
    Coroutine,
    Protocol,
    cast,
    Iterable,
)
import inspect

from loguru import logger


def extract_indented_contents(x: Traceback) -> str:
    line_no = x.lineno
    ls = []
    with open(x.filename) as fd:
        indent = None
        for i, line in enumerate(fd):
            if i < line_no:
                continue

            print(line)
            stripped = line.lstrip()

            ls.append(stripped)
            if indent is None:
                indent = len(line) - len(stripped)
            elif len(line) - len(stripped) <= indent:
                break

    code = textwrap.dedent("".join(ls))
    return code


P = ParamSpec("P")
PP = ParamSpec("PP")
R = TypeVar("R", covariant=True)

T = TypeVar("T")
TT = TypeVar("TT")


Coroutine


_CoroReturn = TypeVar("_CoroReturn", bound=Coroutine[Any, Any, Any], covariant=True)


class CR(Protocol[_CoroReturn]):
    def __call__(self, *args: Any, **kwargs: Any) -> _CoroReturn:
        ...


class CoroReturn(Protocol[P, R]):
    def __call__(self, *args, **kwargs) -> Coroutine[Any, Any, R]:
        ...

    pass


class CoroCallable(Protocol[P, R]):
    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> CoroutineType[Any, Any, R]:
        ...


class ComposableFunction(Generic[P, R]):
    def __init__(self, f: Callable[P, Coroutine[Any, Any, R]]) -> None:
        self.f = f

    def compose(
        self,
        *others: CoroCallable[..., Any],
        last: Callable[..., Coroutine[Any, Any, T]],
    ) -> ComposableFunction[P, T]:
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            result = await self.f(*args, **kwargs)
            for other in others:
                result = await other(result)
            _result = await last(result)
            return _result

        return ComposableFunction(wrapper)

    def __add__(self, other: Any) -> ComposableFunction[P, T]:

        if isinstance(other, ComposableFunction) or inspect.iscoroutinefunction(other):
            _o = cast(Callable[..., Coroutine[Any, Any, T]], other)
            return self.compose(last=_o)
        # todo - add support for other types of functions
        return NotImplemented

    def __radd__(self, other: Any) -> ComposableFunction[PP, R]:
        if inspect.iscoroutinefunction(other):
            _o = cast(Callable[PP, Coroutine[Any, Any, T]], other)
            _oo = ComposableFunction[PP, T](_o)
            return _oo.compose(last=self.f)
        elif isinstance(other, ComposableFunction):
            _o = cast(Callable[PP, Coroutine[Any, Any, T]], other.f)
            _oo = ComposableFunction[PP, T](_o)
            return _oo.compose(last=self.f)
        return NotImplemented

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> Coroutine[Any, Any, R]:
        return self.f(*args, **kwargs)


class PipelineEnchanter:
    def __init__(self) -> None:
        self._substituted_vars: dict[str, dict[str, Callable[..., Any]]]
        self._frame: FrameType

    def __enter__(self) -> None:
        crt_frame = inspect.currentframe()
        assert isinstance(crt_frame, FrameType) and isinstance(crt_frame.f_back, FrameType)
        stack_frame = crt_frame.f_back
        self._frame = stack_frame

        replaced_funs: dict[str, dict[str, Callable[..., Any]]] = {
            "f_locals": {},
            "f_globals": {},
            "f_builtins": {},
        }

        for var_scope_name in "f_locals", "f_globals", "f_builtins":
            logger.info(f"Looking through {var_scope_name}")
            var_scope = getattr(stack_frame, var_scope_name)

            for name in stack_frame.f_code.co_names:
                if name in var_scope:

                    to_replace = var_scope[name]
                    if not inspect.isfunction(to_replace):
                        continue

                    print(f"Found function {name}")
                    replaced_funs[var_scope_name][name] = to_replace

                    to_replace.__dict__["__original_call"] = getattr(to_replace, "__call__", None)
                    to_replace.__dict__["__call__"] = lambda *args, **kwargs: print("honk")
                    to_replace.__dict__["__add__"] = lambda *args, **kwargs: print("honk")
                    var_scope[name] = to_replace

        self._substituted_vars = replaced_funs

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        print("exit")
        for var_dict_name, var_dict in self._substituted_vars.items():
            for name, fun in var_dict.items():
                var_scope = getattr(self._frame, var_dict_name)
                var_scope[name] = fun
        return False


async def add(x: int, y: int) -> AsyncIterator[int]:
    for i in range(10):
        await asyncio.sleep(0.1)
        yield x + y + i


T_double = TypeVar("T_double", int, str)


async def double(x: T_double) -> T_double:
    return x * 2


async def spell_out(x: int) -> str:
    nums = "zero one two three four five six seven eight nine".split()
    return " ".join(nums[int(xx)] for xx in str(x))


async def reverse(x: str) -> str:
    return x[::-1]


c = ComposableFunction(double)
d = c.compose(last=spell_out)
# e = d.compose(last=double)

f = d.compose(last=reverse)


# reveal_type(f)

print(f"{inspect.getsource(d.f)=}")
print(f"{inspect.signature(d)=}")

global_scope = 5


async def fuooo():
    nonlocal_scop = 1

    async def some():
        with A() as a:
            # f = 2 - "foo"
            # print(f)
            zz = 2 + 10
            compd = double + spell_out + (double + reverse)  # type: ignore
            # add(1, 2) >> compd >> (spell_out + reverse)

            """
            neat possibilities:

            - composing functions
                composed = spell_out & double
                multi_composed = double & spell_out & double

            - injecting variables into the scope
                function_definition = (a, b) > double(a) & spell_out(b)  # (partial application)
                pipe = aiter() | function_definition(1, 2) | function_definition(3, 4)
                pipe = aiter() >> (double & spell_out)(1, 2) >> (double & spell_out)(3, 4)
                pipe = aiter() >> (split_0 := double @ split_1 := spell_out)
                ...  split_0 >> double
                ...  split_1 >> spell_out
                pipe = aiter() >> (split_0 := double @ split_1 := spell_out) >> double >> spell_out
                pipe = aiter() >> (double / split_1, spell_out / split_2)
                ...  split_1 + split_2 >> output  # (merged items)

            - combining multiple inputs from multiple streams
                pipe = aiter() & aiter() >> double >> spell_out
                pipe = aiter() & aiter() >> (double & spell_out)
                pipe = aiter() & aiter() >> (double & spell_out) >> double >> spell_out
                
            - operators: +, -, *, /, %, ~, &, |, @, >, <, >>, <<, **, //
            - infix: +=, -=, *=, /=, %=, &=, |=, ^=, >>=, <<=, **=, //=, @=, ==, !=, <=, >=
            - keyword: is, is not, in, not in, and, or, not, del
            
            - notation:
                - map:  aiter() @ double > spell_out
                - flat_map:  aiter() @ double >> spell_out
                - filter:  aiter() @ double % spell_out
                - merge/zip:  aiter() & aiter() @ double
                - concat:  aiter() + aiter() @ double
                - filter:  aiter() < double
                
                - aiter  %  func  ---> aiter  (filter)
                - aiter  >  func  ---> aiter  (map)
                - aiter >>  func  ---> aiter  (flat_map)
                - aiter  &  aiter ---> aiter  (zip)
                - aiter  |  aiter ---> aiter  (union)
                - aiter  +  aiter ---> aiter  (concat)
                - aiter >>  aiter ---> None   (merge)
                - aiter  @  func  ---> aiter  (apply)
                - func   @  aiter ---> aiter  (apply)
                - func   +  func  ---> func   (compose)
                - func   *  func  ---> func   (pipe?)
                - aiter //  func  ---> func   (split?)
                    (AsyncIterable[T] // Callable[[T], R])
                     ⤷ AsyncIterable[T] -> defaultdict[R, AsyncIterable[T]]
                
                
                ASCII diagram with examples:
                
                - sequence_aiter() % filter_odd() > map_double()
                
                    0 -> 1 -> 2 -> 3 -> [filter_odd] -> 1 -> 3 -> [map_double] -> 2 -> 6
                
                - get_alphabet_ords =
                - alphabet_aiter() % filter_vowels() > map_double() > title_case() >> flat_map_ord()
                
                    a -> b -> c -> d -> [filter_vowels] -> a -> e -> i -> o -> u -> [map_double]
                      -> aa -> ee -> ii -> oo -> uu -> [title_case] -> Aa -> Ee -> Ii -> Oo -> Uu
                      -> [flat_map_ord] -> 65 -> 97 -> 69 -> 101 -> 73 -> 105 -> 79 -> 111 ...
            """

            async def get_alphabet_ords() -> AsyncIterator[int]:
                async def alphabet_aiter() -> AsyncIterator[str]:
                    for letter in "abcdefghijklmnopqrstuvwxyz":
                        yield letter

                async def filter_vowels(letter: str) -> bool:
                    return letter not in "aeiou"

                async def map_double(letter: str) -> str:
                    return letter * 2

                async def title_case(letter: str) -> str:
                    return letter.title()

                async def flatten(seq: Iterable[T]) -> AsyncIterator[T]:
                    for item in seq:
                        yield item

                async def do_ord(letter: str) -> int:
                    return ord(letter)

                # return (
                #     alphabet_aiter() \
                #     % filter_vowels() \
                #     > map_double() \
                #     > title_case() \
                #     >> flat_map_ord()
                # )

                async for ch in alphabet_aiter():
                    if not (await filter_vowels(ch)):
                        continue
                    s = await map_double(ch)
                    st = await title_case(s)
                    async for sc in flatten(st):
                        yield await do_ord(sc)

                # alphabet_aiter() % filter_vowels > map_double > title_case >> do_ord

                # Aiter X func -> aiter:
                # >  - map (for each item, apply function)
                # %  - filter (for each item, apply function, if returns false, skip)
                # >> - flat_map (for each item, apply function, for each item in result, yield item)

                # Aiter X aiter -> aiter:
                # |  - merge (create one iterable from multiple, yielding as items arrive)
                # &  - concat (finish one iterable, then start the next)
                # +  - zip (create one iterable from multiple, yielding when all have yielded)

                # func X func -> func:
                # *  - compose (apply the second function to the result of the first)

                # other:
                # // - split (create multiple iterables from one, yielding to each as items arrive;
                #       return dict of iterables, keyed by the result of the function -
                #       aiter[T] // func[T, R] -> dict[R, aiter[T]]
                # @  - apply (apply function to iterable, producing a single item as result)
                #       aiter[T] @ func[T, R] -> R
                #       func[T, R] @ aiter[T] -> R

                # aiter[T] % func[T, bool] -> aiter[T]
                # aiter[T] > func[T, R] -> aiter[R]
                # aiter[T] >> func[T, Iterable[R]] -> aiter[R]
                # aiter[T] & aiter[T] -> aiter[T]
                # aiter[T] | aiter[T] -> aiter[T]
                # aiter[T] + aiter[U] -> aiter[tuple[T, U]]
                # aiter[T] >> aiter[T] -> None
                # aiter[T] @ func[T, R] -> aiter[R]
                # func[T, R] @ aiter[T] -> aiter[R]
                # func[T, R] * func[R, S] -> func[T, S]
                # aiter[T] // func[T, R] -> dict[R, aiter[T]]

                # To make it compatible with the existing async for syntax, we need to
                # implement __aiter__ and __anext__ on the aiter class.
                # We can also implement __iter__ and __next__ to make it compatible with
                # the existing for syntax, but that's not necessary for async for.
                # We can also implement __await__ to make it compatible with the existing
                # await syntax; this could be related to the @ operator.
                # CoPilot is my coding buddy, and he's going to help me with this.
                # Let's go, CoPilot! (cheerful emoji by CoPilot: 🚀)

        async for x in get_alphabet_ords():
            print(x)

        async for num in add(zz, 2):
            print(num, await compd(num), await f(num))

    await some()

    # but now they are back to normal
    compd2 = double + spell_out
    async for num in add(2, 2):
        print(num, await compd2(num))


asyncio.run(fuooo())


T_co = TypeVar("T_co", covariant=True)

# class FilterFunc(Generic[T_co]):
#     def __init__(self, func: Callable[[T_co], bool]):
#         self.func = func
#
#     def __call__(self, item: T_co) -> bool:
#         return self.func(item)
#
#     def __mod__(self, other: Callable[[T_co], bool]) -> Callable[[T_co], bool]:
#         def func(item: T_co) -> bool:
#             return self(item) and other(item)
#
#         return func
#
#     def __invert__(self) -> Callable[[T_co], bool]:
#         def func(item: T_co) -> bool:
#             return not self(item)
#
#         return func
#
#     def __and__(self, other: Callable[[T_co], bool]) -> Callable[[T_co], bool]:
#         def func(item: T_co) -> bool:
#             return self(item) and other(item)
#
#         return func
#
#     def __or__(self, other: Callable[[T_co], bool]) -> Callable[[T_co], bool]:
#         def func(item: T_co) -> bool:
#             return self(item) or other(item)
#
#         return func
#
#     def __xor__(self, other: Callable[[T_co], bool]) -> Callable[[T_co], bool]:
#         def func(item: T_co) -> bool:
#             return self(item) ^ other(item)
#
#         return func
#
#     def __rshift__(self, other: Callable[[T_co], bool]) -> Callable[[T_co], bool]:
#         def func(item: T_co) -> bool:
#             return self(item) >> other(item)
#
#         return func
#
#     def __lshift__(self, other: Callable[[T_co], bool]) -> Callable[[T_co], bool]:
#         def func(item: T_co) -> bool:
#             return self(item) << other(item)
#
#         return func
#
#     def __eq__(self, other: Callable[[T_co], bool]) -> Callable[[T_co], bool]:
#         def func(item: T_co) -> bool:
#             return self(item) == other(item)
#
#         return func
#
#     def __ne__(self, other: Callable[[T_co], bool]) -> Callable[[T_co], bool]:
#         def func(item: T_co) -> bool:
#             return self(item) != other(item)
#
#         return func
#
#     def __le__(self, other: Callable[[T_co], bool]) -> Callable[[T_co], bool]:
#         def func(item:
#
# class FilterableAsyncIterable(AsyncIterable[T_co]):
#     def __init__(self, aiter: AsyncIterable[T_co]):
#         self.aiter = aiter
#
#     def __aiter__(self) -> AsyncIterator[T_co]:
#         return self.aiter.__aiter__()
#
#     def __mod__(self, filter_func: Callable[[T_co], bool]) -> "FilterableAsyncIterable[T_co]":
#         return FilterableAsyncIterable((item async for item in self if filter_func(item)))
#
#     def __gt__(self, map_func: Callable[[T_co], R]) -> "FilterableAsyncIterable[R]":
#         return FilterableAsyncIterable((map_func(item) async for item in self))
#
#     def __rshift__(
#         self, flat_map_func: Callable[[T_co], AsyncIterable[R]]
#     ) -> "FilterableAsyncIterable[R]":
#         return FilterableAsyncIterable(
#             (item async for subitem in flat_map_func(item) for item in self)
#         )
