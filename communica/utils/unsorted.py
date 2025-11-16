import logging
import itertools
import threading
from typing import Any, Generic, TypeVar
from asyncio import (
    Task,
    Future,
    current_task,
    get_running_loop,
)
from inspect import isclass, ismethod, isfunction
from operator import attrgetter
from traceback import format_exception
from collections import deque, defaultdict

from typing_extensions import TypeAlias


_TV = TypeVar('_TV')
ByteSeq: TypeAlias = 'bytes | memoryview | bytearray'

logger = logging.getLogger('communica')
_task_counters = defaultdict(lambda: itertools.count())

ETX_CHAR = b'\x04'
NULL_CHAR = b'\x00'

UINT32MAX = 2**32 - 1
UINT16MAX = 2**16 - 1
INT32RANGE = (-2**31, 2**31 - 1)


_HasLoopMixin_lock = threading.Lock()


class read_accessor(Generic[_TV]):
    """
    ```
    class Foo:
        url: read_accessor[str] = read_accessor('_url')
    ```
    is equivalent for
    ```
    class Foo:
        @property
        def url(self) -> str:
            return self._url
    ```
    """
    __slots__ = ('_getter',)

    def __init__(self, path: str) -> None:
        self._getter = attrgetter(path)

    def __get__(self, instance, owner=None) -> _TV:
        return self._getter(instance)


# can be replaced with asyncio.mixins._LoopBoundMixin,
# but it's not in public API
class HasLoopMixin:
    _bound_loop = None

    def _get_loop(self):
        loop = get_running_loop()

        if self._bound_loop is None:
            with _HasLoopMixin_lock:
                if self._bound_loop is None:
                    self._bound_loop = loop
        if loop is not self._bound_loop:
            raise RuntimeError(
                f'{self!r} is bound to a different event loop. '
                'Consider using one event loop per process, '
                'or at least do not use one asyncio object '
                'in different loops and/or threads.'
            )
        return loop


class TaskSet(HasLoopMixin):
    tasks: 'set[Task]'

    def __init__(self) -> None:
        self.tasks = set()

    def create_task(self, coro, *, name=None) -> Task:
        task = self._get_loop().create_task(coro, name=name)
        task.add_done_callback(self.tasks.discard)
        self.tasks.add(task)
        return task

    def create_task_with_exc_log(self, coro, *, name=None):
        task = self.create_task(coro, name=name)
        task.add_done_callback(exc_log_callback)
        return task

    def cancel(self):
        "Cancel all tasks except current"
        cur_task = current_task()
        for task in self.tasks:
            if task is not cur_task:
                task.cancel()


class MessageQueue(HasLoopMixin, Generic[_TV]):
    # XXX: does slots make sense when __dict__ slot defined?
    __slots__ = ('_queue', '_max_items', '_get_waiter', '_put_waiters')

    def __init__(self, max_items: int) -> None:
        self._queue = deque()
        self._max_items = max_items
        self._get_waiter = None
        self._put_waiters = deque()

    def empty(self):
        return (len(self._queue) == 0)

    def full(self):
        return (len(self._queue) >= self._max_items)

    def set_max_items(self, max_items: int):
        self._max_items = max_items

    def put_back(self, item: _TV):
        self._queue.appendleft(item)

    def put_nowait(self, item: _TV):
        """Put item in queue, ignoring length"""
        self._queue.append(item)

    async def put(self, item: _TV):
        if len(self._queue) >= self._max_items:
            waiter = self._get_loop().create_future()
            self._put_waiters.append(waiter)
            await waiter

        if self._get_waiter:
            if not self._get_waiter.done():
                self._get_waiter.set_result(True)
            self._get_waiter = None

        self._queue.append(item)

    async def get(self) -> _TV:
        if not self._queue:
            if self._get_waiter and not self._get_waiter.done():
                raise RuntimeError('Duplicate get from queue')

            waiter = self._get_loop().create_future()
            self._get_waiter = waiter
            await self._get_waiter

        while self._put_waiters:
            if not (waiter := self._put_waiters.popleft()).done():
                waiter.set_result(True)
                break

        return self._queue.popleft()


class LateBoundFuture(Future[_TV], HasLoopMixin):
    """
    This future sets loop to itself only on first access
    (default future sets it in __init__).

    It also contains incorrect source_traceback.
    """

    def __init__(self, *, loop=None) -> None:
        pass

    def get_loop(self):
        is_initialized = bool(self._bound_loop)
        loop = self._get_loop()
        if not is_initialized:
            super().__init__(loop=loop)
        return loop

    def __getattribute__(self, name: str) -> Any:
        passthrough = ('get_loop', '_get_loop', '_bound_loop')
        if name in passthrough or name.startswith('__'):
            return super().__getattribute__(name)
        self.get_loop()
        return super().__getattribute__(name)


def exc_log_callback(task: Task):
    if not task.cancelled() and (exc := task.exception()):
        tb = format_exception(type(exc), exc, exc.__traceback__)
        logger.warning('Uncaught exception in %r:\n%s', task, '\n'.join(tb))


def iscallable(obj, _prev=None):
    if isfunction(obj):
        return True
    if isclass(obj):
        return False
    if ismethod(obj):
        return True
    if obj is _prev:
        return False
    return iscallable(getattr(obj, '__call__', None), obj)


def cycle_range(start: int, stop: int):
    if stop <= start:
        ValueError('stop should be greater than start')

    i = start
    while True:
        yield i
        if i >= stop:
            i = start
        else:
            i += 1


def fmt_task_name(name: str) -> str:
    return f'communica-{name}-{next(_task_counters[name])}'
