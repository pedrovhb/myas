"""The utils module contains various utility functions.

`compose`, `pipe`, and friends have their own files because the typing is verbose and unsightly.
It's not pretty, but it works, and it's also how the standard library does it ¯\\_(ツ)_/¯
"""

from . import aiter_utils as _aiter_utils_file
from . import compose as _compose_file
from . import pipe as _pipe_file
from . import misc as _misc_file

from .aiter_utils import *
from .compose import *
from .pipe import *
from .misc import *


__all__ = (
    *_compose_file.__all__,
    *_pipe_file.__all__,
    *_aiter_utils_file.__all__,
    *_misc_file.__all__,
)
