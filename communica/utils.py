from os import getenv
from random import gauss
from typing import Any, TypeVar, Generic
from logging import getLogger
from inspect import isclass, isfunction, ismethod
from asyncio import get_running_loop, current_task, sleep, CancelledError, Task
from traceback import format_exception
from collections import deque


_TV = TypeVar('_TV')

logger = getLogger('communica')

if getenv('COMMUNICA_DEBUG'):
    logger.setLevel(10)

ETX_CHAR = b'\x04'
NULL_CHAR = b'\x00'

INT32MAX = 2**32 - 1
UINT16MAX = 2**16 - 1
INT32RANGE = (-2**31, 2**31 - 1)


try:
    import orjson

    _orjson_option = (
        orjson.OPT_NON_STR_KEYS
    )

    def json_dumpb(obj: Any) -> bytes:
        return orjson.dumps(obj, option=_orjson_option)

    json_loadb = orjson.loads

except ImportError:
    import json

    def json_dumpb(obj: Any) -> bytes:
        return json.dumps(obj, ensure_ascii=False).encode('utf-8')

    json_loadb = json.loads


class HasLoopMixin:
    __slots__ = ('_loop',)

    def _get_loop(self):
        try:
            return self._loop
        except AttributeError:
            self._loop = get_running_loop()
            return self._loop


class TaskSet(HasLoopMixin):
    tasks: 'set[Task]'

    def __init__(self) -> None:
        self.tasks = set()

    def create_task(self, coro) -> Task:
        task = self._get_loop().create_task(coro)
        task.add_done_callback(self.tasks.discard)
        self.tasks.add(task)
        return task

    def create_task_with_exc_log(self, coro):
        task = self.create_task(coro)
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
            try:
                await waiter
            except CancelledError:
                waiter.cancel()
                raise

        if self._get_waiter is not None:
            self._get_waiter.set_result(True)
            self._get_waiter = None

        self._queue.append(item)

    async def get(self) -> _TV:
        if not self._queue:
            if self._get_waiter is not None:
                raise RuntimeError('Duplicate get from queue')

            waiter = self._get_loop().create_future()
            try:
                self._get_waiter = waiter
                await self._get_waiter
            finally:
                if self._get_waiter is waiter:
                    self._get_waiter = None

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
        self._delay = gauss(
            min(self.max_delay, (self._delay * self.factor)),
            self.jitter
        )


def exc_log_callback(task: Task):
    if not task.cancelled() and (exc := task.exception()):
        tb = format_exception(type(exc), exc, exc.__traceback__)
        logger.warning('Uncaught exception in %r:\n%s', task, '\n'.join(tb))


def _time_print(*args):  # pragma: no cover
    import time
    print(round(time.monotonic(), 3), *args)


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
