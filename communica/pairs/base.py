import asyncio

from abc import abstractmethod, ABC
from typing import Protocol, Any

from typing_extensions import Self

from communica.utils import HasLoopMixin, logger
from communica.connectors.base import BaseConnector


class BaseEntity(ABC, HasLoopMixin):
    __slots__ = ('connector', '_stop_event')

    connector: BaseConnector

    def __init__(self, connector: BaseConnector) -> None:
        self.connector = connector

    def __repr__(self) -> str:
        return f'{type(self).__name__}()'

    async def run(self):
        """Run until .stop() is called."""
        if hasattr(self, '_stop_event'):
            RuntimeError('.run() must be called only once on same object')

        self._stop_event = asyncio.Event()
        try:
            async with self:
                print(f'Running {self!r} on {self.connector.repr_address()}')
                await self._stop_event.wait()
        finally:
            del(self._stop_event)

    def stop(self):
        """If .run() was called, it returns, otherwise this method does nothing"""
        if hasattr(self, '_stop_event'):
            self._stop_event.set()

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
    async def init(self, timeout: 'int | None' = None) -> Self:
        """
        Establish connection with server.

        On connect timeout cancels connect task.

        Args:
            timeout: Time in seconds,
              if omitted, method will block until connect succeed.

        Raises:
            TimeoutError
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


class SyncHandlerType(Protocol):
    def __call__(self, data: Any) -> Any: ...


class AsyncHandlerType(Protocol):
    async def __call__(self, data: Any) -> Any: ...
