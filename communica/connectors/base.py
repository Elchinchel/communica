import asyncio

from abc import abstractmethod, ABC
from typing import Any, Protocol, Callable, Awaitable, AsyncGenerator
from typing_extensions import Self

from communica.utils import logger, json_dumpb, json_loadb


HandshakeGen = AsyncGenerator['dict | HandshakeOk', dict]


class Handshaker(Protocol):
    async def __call__(self, connection: 'BaseConnection') -> HandshakeGen: ...


class ClientConnectedCB(Protocol):
    def __call__(self, connection: 'BaseConnection'): ...


class RequestReceivedCB(Protocol):
    def __call__(self, metadata: Any, raw_data: memoryview): ...


class HandshakeOk: ...


class HandshakeFail(Exception):
    def __init__(self, reason: str) -> None:
        self.reason = reason

    def dumpb(self):
        obj = {'reason': self.reason}
        return json_dumpb(obj)

    @classmethod
    def loadb(cls, data: bytes):
        obj = json_loadb(data)
        return cls(obj['reason'])


class BaseConnection(ABC):
    __slots__ = ('_alive', '_handshake_result')

    @property
    def is_alive(self) -> bool:
        try:
            return self._alive
        except AttributeError:
            return False

    def get_handshake_result(self) -> HandshakeOk:
        """
        Get result of recent handshake.

        Handshake should be performed
        by connection only once right after connect.

        Raises:
            RuntimeError: if called more than once
        """
        try:
            result = self._handshake_result
        except AttributeError:
            raise RuntimeError('Handshake not completed yet') from None
        if result is None:
            raise RuntimeError('Handshake result already has been received')
        return result

    async def _run_handshaker(
            self,
            handshaker: Handshaker,
            send_message: Callable[[bytes], Awaitable],
            recv_message: Callable[[], Awaitable[bytes]]
    ) -> 'HandshakeOk':
        handshake_gen = handshaker(self)

        async def send_to_gen(data):
            if data is not None:
                data = json_loadb(data)
            gen_return = await handshake_gen.asend(data)
            if isinstance(gen_return, dict):
                return json_dumpb(gen_return)
            elif isinstance(gen_return, HandshakeOk):
                if hasattr(self, '_handshake_result'):
                    raise RuntimeError('Handshake repeated on same connection')
                self._handshake_result = gen_return
                return gen_return
            else:
                raise StopAsyncIteration

        try:
            # Первый asend должен быть с None
            data_or_result = await send_to_gen(None)
            if not isinstance(data_or_result, bytes):
                return data_or_result

            while True:
                await send_message(data_or_result)

                received_data = await recv_message()

                data_or_result = await send_to_gen(received_data)
                if not isinstance(data_or_result, bytes):
                    return data_or_result
        except HandshakeFail:
            raise
        except StopAsyncIteration:
            raise RuntimeError('Handshake generator must return bytes, '
                               'HandshakeOk or HandshakeFail') from None
        except Exception as error:
            logger.error('Handshake failed: %r raised %r', handshaker, error)
            raise
        finally:
            await handshake_gen.aclose()

    @abstractmethod
    def update(self, connection: Self) -> None:
        """Must be called before run_until_fail"""
        raise NotImplementedError

    @abstractmethod
    async def send(self, metadata: Any, raw_data: bytes):
        raise NotImplementedError

    @abstractmethod
    async def run_until_fail(
            self,
            request_received_cb: RequestReceivedCB
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    async def close(self) -> None:
        raise NotImplementedError


class BaseConnector(ABC):
    """Making connections"""
    _TYPE: str

    __slots__ = ()

    @abstractmethod
    def repr_address(self) -> str:
        raise NotImplementedError

    @classmethod
    def _check_dump(cls, dump: 'dict[str, Any]'):
        if cls._TYPE != dump['type']:
            raise ValueError('Wrong connector type: passed string was '
                            f'constructed with "{dump["type"]}", '
                            f'current is "{cls._TYPE}"')

    @abstractmethod
    def dump_state(self) -> str:
        """
        Get a string which can be used to
        create new connector with .from_state() method
        """
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def from_state(cls, state: str) -> Self:
        """Make new connector from string, obtained with .dump_state() method"""
        raise NotImplementedError

    @abstractmethod
    async def server_start(
            self,
            handshaker: Handshaker,
            client_connected_cb: ClientConnectedCB,
    ) -> asyncio.AbstractServer:
        raise NotImplementedError

    @abstractmethod
    async def client_connect(self, handshaker: Handshaker) -> BaseConnection:
        raise NotImplementedError
