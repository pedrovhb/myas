# """The utils module contains various utility functions.
#
# `compose`, `pipe`, and friends have their own files because the typing is verbose and unsightly.
# It's not pretty, but it works, and it's also how the standard library does it ¯\\_(ツ)_/¯
# """
#
# import myas.utils.aiter_utils
# import myas.utils.compose
# import myas.utils.pipe
# import myas.utils.misc
#
#
# __all__: tuple[str, ...] = (
#     *myas.utils.aiter_utils.__all__,
#     *myas.utils.compose.__all__,
#     *myas.utils.pipe.__all__,
#     *myas.utils.misc.__all__,
# )
