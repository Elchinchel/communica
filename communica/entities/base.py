import asyncio
from abc import ABC, abstractmethod
from typing import Any, Callable, Protocol
from inspect import iscoroutinefunction
from functools import wraps, partial
from concurrent.futures import ThreadPoolExecutor

from typing_extensions import Self, TypeAlias

from communica.utils import HasLoopMixin, BackoffDelayer, iscallable
from communica.utils.delayer import default_backoff_delayer
from communica.connectors.base import BaseConnector


class BaseEntity(ABC, HasLoopMixin):
    __slots__ = ('connector', '_closed', '_stop_event')

    connector: BaseConnector

    def __init__(self, connector: BaseConnector) -> None:
        self.connector = connector

    def __repr__(self) -> str:
        return f'{type(self).__name__}()'

    async def run(self):
        """Run until .stop() is called."""
        if hasattr(self, '_stop_event'):
            raise RuntimeError('.run() must be called only once on same object')

        self._closed = asyncio.get_event_loop().create_future()
        self._stop_event = asyncio.Event()
        try:
            async with self:
                print(f'Running {self!r} on {self.connector.repr_address()}')
                await self._stop_event.wait()
        finally:
            del(self._stop_event)
            self._closed.set_result(None)

    def stop(self):
        """If .run() was called, it returns, otherwise this method does nothing"""
        if hasattr(self, '_stop_event'):
            self._stop_event.set()

    async def wait_stop(self):
        """Call .stop() and wait until closed"""
        if hasattr(self, '_stop_event'):
            self.stop()
            await self._closed

    @abstractmethod
    async def init(self) -> Self:
        raise NotImplementedError

    @abstractmethod
    async def close(self) -> None:
        raise NotImplementedError

    async def __aenter__(self) -> Self:
        return await self.init()

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()


class BaseClient(BaseEntity):
    __slots__ = ()

    @abstractmethod
    async def init(
        self,
        timeout: 'int | None' = 0,
        delayer_factory: Callable[[], BackoffDelayer] = default_backoff_delayer,
    ) -> Self:
        """
        Establish connection with server.

        On timeout cancels connect task.

        Args:
            timeout: Time in seconds.
              If omitted or zero, this method will block until connect succeed.
              If None, connection initiation will start and any
              .request() or .throw() calls will block until connect succeed.
            delayer_factory: function which returns
              communica.utils.delayer.BackoffDelayer instance.
              By default communica.utils.delayer.default_backoff_delayer is used.

        Raises:
            asyncio.TimeoutError
        """
        raise NotImplementedError

    @abstractmethod
    async def close(self) -> None:
        """Close connection with server."""
        raise NotImplementedError


class BaseServer(BaseEntity):
    __slots__ = ()

    @abstractmethod
    async def init(self) -> Self:
        """Create server and start accepting connections"""
        raise NotImplementedError

    @abstractmethod
    async def close(self) -> None:
        """Stop serving and cancel all handler tasks"""
        raise NotImplementedError


HandlerType: TypeAlias = 'SyncHandlerType | AsyncHandlerType | RequestHandler'


class SyncHandlerType(Protocol):
    def __call__(self, data: Any) -> Any: ...


class AsyncHandlerType(Protocol):
    async def __call__(self, data: Any) -> Any: ...


class RequestHandler:
    __slots__ = ('_repr', 'is_async', 'endpoint')

    is_async: bool
    endpoint: 'SyncHandlerType | AsyncHandlerType'

    def __repr__(self) -> str:
        try:
            return self._repr
        except AttributeError:
            name = getattr(self.endpoint, '__qualname__',
                           getattr(self.endpoint, '__name__', 'UNKNOWN'))
            htype = 'async' if self.is_async else 'sync'
            self._repr = f'<RequestHandler {htype} endpoint={name}>'
            return self._repr

    def __init__(self, endpoint: 'SyncHandlerType | AsyncHandlerType') -> None:
        if not iscallable(endpoint):
            raise TypeError('Request handler must be function, '
                            'coroutine function or method')
        self.is_async = iscoroutinefunction(endpoint)
        self.endpoint = endpoint


def threaded_handler(executor: 'ThreadPoolExecutor | None' = None):
    """
    Returns decorator, which returns RequestHandler, which runs
    decorated function in separate thread.
    """
    def decorator(handler: SyncHandlerType):
        # TODO: test with auto serializer
        @wraps(handler)
        async def wrapper(*args, **kwargs):
            loop = asyncio.get_running_loop()
            func = partial(handler, *args, **kwargs)
            return await loop.run_in_executor(executor, func)
        return RequestHandler(handler)
    return decorator
