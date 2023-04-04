from os import getenv
from typing import Any, TypeVar, Generic
from logging import getLogger
from asyncio import get_running_loop, CancelledError
from inspect import isclass, isfunction, ismethod
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


# XXX: оно того стоит?
try:
    import orjson

    json_dumpb = orjson.dumps  # type: ignore
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

        self._queue.append(item)

    async def get(self) -> _TV:
        if not self._queue:
            if self._get_waiter is not None:
                raise RuntimeError('Duplicate get from queue')

            try:
                self._get_waiter = self._get_loop().create_future()
                await self._get_waiter
            finally:
                self._get_waiter = None

        while self._put_waiters:
            if not (waiter := self._put_waiters.popleft()).done():
                waiter.set_result(True)
                break

        return self._queue.popleft()


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
