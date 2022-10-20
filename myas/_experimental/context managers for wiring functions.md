

## Context managers for wiring function pipelines

I have a dream. I dream of a world where we can write code like this:


```python
def double(x: int) -> int:
    return x * 2

def spell_out(x: int) -> str:
    nums = "zero one two three four five six seven eight nine".split()
    return " ".join(nums[int(xx)] for xx in str(x))

cool_numbers = range(4, 12) > double > spell_out > str.title @ list
# [Eight, One Zero, One Two, One Four, One Six, One Eight, Two Zero, Two Two]
```

Instead, we have to write code like this:

```python
cool_numbers = []
for num in range(4, 12):
    new_num = double(num)
    spelled_out = spell_out(new_num)
    title_cased = spelled_out.title()
    cool_numbers.append(title_cased)
# ['Eight', 'One Zero', 'One Two', 'One Four', 'One Six', 'One Eight', 'Two Zero', 'Two Two']
```

By the time I'm done typing all that, those numbers are no longer cool. They're slightly warm, and I'm slightly annoyed.
We could define a new class which subclasses iterators and callables and correctly implements the `__gt__` and `__matmul__` operators, but that's lacking in _magic_.
It's no fun to not corrupt the language every chance you get.
Here's what we really want:

```python

class FunWrapper:

    def __init__(self, func):
        self.func = func

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)

    def __gt__(self, other):
        print("I'm doing the thing!")
        return FunWrapper(lambda x: other(self(x)))

    def __matmul__(self, other):
        print("I'm doing the other thing!")
        return FunWrapper(lambda x: other(self(x)))


class PipelineEnchanter:
    def __init__(self) -> None:
        self._substituted_vars: dict[str, dict[str, Callable[..., Any]]]
        self._frame: FrameType

    def __enter__(self) -> None:

        # self._frame = sys._getframe(1)  - ??

        # Get a window into our surroundings
        crt_frame = inspect.currentframe()

        # Appease the type checker; it's important
        # for our safety in the journey ahead
        assert isinstance(crt_frame, FrameType)
        assert isinstance(crt_frame.f_back, FrameType)

        stack_frame = crt_frame.f_back
        self._frame = stack_frame

        # We wouldn't want to do anything irreversible, would we?
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

        # No one will ever know! Shh
        for var_dict_name, var_dict in self._substituted_vars.items():
            for name, fun in var_dict.items():
                var_scope = getattr(self._frame, var_dict_name)
                var_scope[name] = fun
        return False

with PipelineEnchanter():
    cool_numbers = range(4, 12) > double > spell_out > str.title @ list
    # [Eight, One Zero, One Two, One Four, One Six, One Eight, Two Zero, Two Two]

# We're outside the magic circle, so no use in trying to be cool anymore
problematic_numbers = range(1, 23) > double > spell_out > str.title @ list

# TypeError: '>' not supported between instances of 'range' and 'function'



# todo finish
```

