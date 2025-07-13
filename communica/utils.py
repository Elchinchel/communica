import logging
import itertools
import threading
from random import gauss
from typing import Generic, TypeVar
from asyncio import Task, sleep, current_task, get_running_loop
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


class BackoffDelayer:
    __slots__ = ('start_delay', 'max_delay', 'factor', 'jitter', '_delay')

    def __init__(
            self,
            start_delay: float,
            max_delay: float,
            factor: float,
            jitter: float
    ) -> None:
        assert factor > 1, 'factor less or equal 1'
        assert max_delay > start_delay, 'max delay less or equal to start delay'
        self.factor = factor
        self.jitter = jitter

        self._delay = start_delay
        self.max_delay = max_delay
        self.start_delay = start_delay

    def reset(self):
        self._delay = self.start_delay

    async def wait(self):
        await sleep(self._delay)
        new_delay = gauss(
            min(self.max_delay, (self._delay * self.factor)),
            self.jitter
        )
        if new_delay > self._delay:
            self._delay = new_delay


def exc_log_callback(task: Task):
    if not task.cancelled() and (exc := task.exception()):
        tb = format_exception(type(exc), exc, exc.__traceback__)
        logger.warning('Uncaught exception in %r:\n%s', task, '\n'.join(tb))


def iscallable(obj):
    if isfunction(obj):
        return True
    if isclass(obj):
        return False
    return ismethod(obj)


def cycle_range(start: int, stop: int):
    if stop <= start:
        ValueError('stop should be greater than start')

    i = start
    while True:
        yield i
        if i >= stop:
            i = 0
        else:
            i += 1


def fmt_task_name(name: str) -> str:
    return f'communica-{name}-{next(_task_counters[name])}'
