import myas.utils.aiter_utils
import myas.closeable_queue
import myas.utils.misc
import myas.protocols

# import myas.closeable_queue
#
#
# # apipe = myas.utils.pipe_async_iterable
amerge = myas.utils.aiter_utils.merge_async_iterables
amap = myas.utils.aiter_utils.map_async_iterable
atee = myas.utils.aiter_utils.tee_async_iterable
q2a = myas.utils.aiter_utils.queue_to_async_iterator
a2q = myas.utils.aiter_utils.async_iterable_to_queue


CloseableQueue = myas.closeable_queue.CloseableQueue
PriorityQueue = myas.closeable_queue.PriorityQueue
LifoQueue = myas.closeable_queue.LifoQueue
QueueClosedException = myas.closeable_queue.QueueClosedException
QueueExhausted = myas.closeable_queue.QueueExhausted

ensure_coroutine = myas.utils.misc.ensure_coroutine


QueueLike = myas.protocols.QueueLike
CloseableQueueLike = myas.protocols.CloseableQueueLike

#
#
# __all__: tuple[str, ...] = (
#     # "apipe",
#     "amerge",
#     "amap",
#     "atee",
#     "q2a",
#     "a2q",
#     *myas.closeable_queue.__all__,
#     *myas.utils.__all__,
# )
