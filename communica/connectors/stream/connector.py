import json
import asyncio
import logging
from abc import ABC, abstractmethod
from ssl import SSLContext
from struct import Struct
from typing import Any, Dict, Type, ClassVar, Optional
from contextlib import suppress
from dataclasses import dataclass

from typing_extensions import Self

from communica.utils import (
    ETX_CHAR,
    NULL_CHAR,
    UINT32MAX,
    MessageQueue,
    json_dumpb,
    json_loadb,
    read_accessor,
)
from communica.exceptions import FeatureNotAvailable
from communica.connectors.base import (
    Handshaker,
    HandshakeOk,
    BaseConnector,
    HandshakeFail,
    BaseConnection,
    ClientConnectedCB,
    RequestReceivedCB,
)
from communica.connectors.stream import local_connections


__all__ = (
    'TcpConnector',
    'LocalConnector'
)

logger = logging.getLogger('communica.connectors.stream')
DEFAULT_STREAM_HWM = 32


class Frame(ABC):
    CODE: ClassVar[int]
    code_byte: ClassVar[bytes]
    LENGTH_PACKER = Struct(
        '!I'  # big-endian uint32
    )

    length_size = LENGTH_PACKER.size
    length_pack = LENGTH_PACKER.pack
    length_unpack_from = LENGTH_PACKER.unpack_from

    _frames: ClassVar[Dict[int, Type[Self]]] = {}

    def __init_subclass__(cls) -> None:
        cls._frames[cls.CODE] = cls
        cls.code_byte = bytes([cls.CODE])

    @abstractmethod
    def to_bytes(self) -> bytes:
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def _load(cls, data: memoryview):
        raise NotImplementedError

    @classmethod
    def from_bytes(cls, data: memoryview) -> Self:
        try:
            frame = cls._frames[data[0]]
        except ValueError:
            raise ValueError('Unknown protocol') from None

        return frame._load(data[1:])


@dataclass
class MessageFrame(Frame):
    CODE = 10

    metadata: Any
    raw_data: bytes

    @classmethod
    def _load(cls, data: memoryview):
        metadata_end = cls.length_unpack_from(data)[0]
        return cls(
            json_loadb(data[cls.length_size:metadata_end]),
            data[metadata_end:]
        )

    def to_bytes(self) -> bytes:
        return (
            self.code_byte +
            self.length_pack(len(self.metadata) + self.length_size) +
            self.metadata +
            self.raw_data
        )


@dataclass
class CloseNotifyFrame(Frame):
    CODE = 20

    @classmethod
    def to_bytes_with_header(cls) -> bytes:
        return cls.length_pack(len(cls.code_byte)) + cls.code_byte

    @classmethod
    def _load(cls, data: memoryview):
        return cls()

    def to_bytes(self) -> bytes:
        return self.code_byte


class StreamConnection(BaseConnection):
    __slots__ = ('reader', 'writer', 'connector', '_send_queue')

    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    connector: 'BaseStreamConnector'

    _send_queue: 'MessageQueue[Frame]'
    _handshake_result: 'HandshakeOk | None'

    _MAX_CHUNK_SIZE = UINT32MAX + 1 - Frame.length_size

    max_chunk_size: read_accessor[int] = read_accessor('_MAX_CHUNK_SIZE')
    """Max size of one message in bytes"""

    def __init__(
            self,
            reader: asyncio.StreamReader,
            writer: asyncio.StreamWriter,
            connector: 'BaseStreamConnector'
    ) -> None:
        self.reader, self.writer = reader, writer
        self.connector = connector
        self._send_queue = MessageQueue(DEFAULT_STREAM_HWM)

    def update(self, connection: Self):
        if not connection._send_queue.empty():
            raise ValueError('New connection\'s queue must be empty')
        self.reader, self.writer = connection.reader, connection.writer

    async def run_until_fail(
            self,
            request_received_cb: RequestReceivedCB,
    ):
        self._alive = True
        try:
            await self._run_connection(request_received_cb)
        except asyncio.IncompleteReadError:
            pass
        finally:
            self._alive = False
        return None

    async def send(self, metadata: Any, raw_data: bytes):
        # XXX: split big messages instead of error?
        if len(raw_data) > self._MAX_CHUNK_SIZE:
            raise ValueError('Data exceeds max chunk size (%d bytes)' %
                             self._MAX_CHUNK_SIZE)

        await self._send_queue.put(
            MessageFrame(json_dumpb(metadata), raw_data)
        )

    async def close(self):
        close_chunk = CloseNotifyFrame.to_bytes_with_header()
        self.writer.write(close_chunk)
        self.writer.close()
        with suppress(AssertionError):
            self.reader.feed_data(close_chunk)

        await self.writer.wait_closed()

    async def _run_connection(self, request_received_cb: RequestReceivedCB):
        is_closing = self.writer.is_closing
        readexactly = self.reader.readexactly
        header_size = Frame.length_size
        header_unpack = Frame.length_unpack_from

        write_task = asyncio.create_task(self._send_runner())
        try:
            while not is_closing():
                header = await readexactly(header_size)
                chunk_len = header_unpack(header)[0]
                chunk = memoryview(await readexactly(chunk_len))

                frame = Frame.from_bytes(chunk)
                logger.debug('Received %r', frame)
                if isinstance(frame, MessageFrame):
                    request_received_cb(frame.metadata, frame.raw_data)
                elif isinstance(frame, CloseNotifyFrame):
                    self.writer.close()
                    return
                else:
                    logger.warning('wtf')
        except Exception as e:
            logger.info('Connection broken: %r', e)
            raise
        finally:
            write_task.cancel()
            await asyncio.wait([write_task])

    async def _send_runner(self):
        write = self.writer.write
        drain = self.writer.drain
        header_pack = Frame.length_pack
        send_queue_get = self._send_queue.get

        while True:
            frame = await send_queue_get()
            data = frame.to_bytes()

            write(
                header_pack(len(data)) +
                data
            )
            logger.debug('Sent %r', frame)

            # yield to loop, giving chance to pause protocol
            await asyncio.sleep(0)

            await drain()

    async def _do_handshake(
            self,
            handshaker: Handshaker,
    ):
        async def send_message(data):
            if NULL_CHAR in data or ETX_CHAR in data:
                raise ValueError('Handshake messages must NOT contain '
                                f'{NULL_CHAR!r} and {ETX_CHAR!r} characters')
            self.writer.write(data + NULL_CHAR)
            await self.writer.drain()

        async def recv_message() -> bytes:
            try:
                return (await self.reader.readuntil(NULL_CHAR))[:-1]
            except asyncio.IncompleteReadError as e:
                if (pos := e.partial.find(ETX_CHAR)) == -1:
                    raise
                raise HandshakeFail.loadb(e.partial[pos+1:])

        try:
            await self._run_handshaker(handshaker, send_message, recv_message)
            return True
        except HandshakeFail as fail:
            self.writer.write(ETX_CHAR + fail.dumpb())
            self.writer.close()
            await self.writer.wait_closed()
            return False


class BaseStreamConnector(BaseConnector):
    __slots__ = ()

    def _create_server_cb(
            self,
            handshaker: Handshaker,
            client_connected_cb: ClientConnectedCB
    ):
        async def callback(r: asyncio.StreamReader, w: asyncio.StreamWriter) -> None:
            connection = StreamConnection(r, w, self)
            if not await connection._do_handshake(handshaker):
                return
            client_connected_cb(connection)

        return callback

    @abstractmethod
    async def _open_connection(self) -> 'tuple[asyncio.StreamReader, asyncio.StreamWriter]':
        raise NotImplementedError

    async def client_connect(self, handshaker: Handshaker):
        r, w = await self._open_connection()
        connection = StreamConnection(r, w, self)
        try:
            await connection._do_handshake(handshaker)
        except HandshakeFail as fail:
            w.write(ETX_CHAR + fail.dumpb())
            w.close()
            await w.wait_closed()
            raise
        return connection


class TcpConnector(BaseStreamConnector):
    _TYPE = 'TcpConnector'

    __slots__ = ('host', 'port', 'ssl')

    def __init__(self, host: str, port: int, ssl: Optional[SSLContext] = None):
        self.host = host
        self.port = port
        self.ssl = ssl

    def repr_address(self) -> str:
        return f'{self.host}:{self.port}'

    async def server_start(
            self,
            handshaker: Handshaker,
            client_connected_cb: ClientConnectedCB,
    ):
        return await asyncio.start_server(
            self._create_server_cb(handshaker, client_connected_cb),
            host=self.host,
            port=self.port,
            ssl=self.ssl
        )

    async def _open_connection(self):
        return await asyncio.open_connection(
            host=self.host,
            port=self.port,
            ssl=self.ssl
        )

    def dump_state(self) -> str:
        return json.dumps({
            'type': self._TYPE,
            'host': self.host,
            'port': self.port,
            'ssl': bool(self.ssl)
        })

    @classmethod
    def from_state(cls, state: str, ssl: Optional[SSLContext] = None):
        data = json.loads(state)
        cls._check_dump(data)
        if data['ssl'] and not ssl:
            raise ValueError('SSL context was not provided, but connector state '
                             'demands it\'s presence')
        return cls(data['host'], data['port'], ssl)


class LocalConnector(BaseStreamConnector):
    """
    Connector for processes on same machine.

    Uses Named Pipes on Windows and Unix Domain Sockets on POSIX OS.
    """

    _TYPE = 'LocalConnector'

    __slots__ = ('_name',)

    @property
    def name(self):
        return self._name

    def __init__(self, name: str):
        """
        Args:
            name: Address of named pipe, or name of socket in tempdir.
              There is limit for name length, depending on running OS.
              Case insensitive (will be lowered).
        """
        if not local_connections.IS_AVAILABLE:
            raise FeatureNotAvailable(
                'Current platform does not provide named pipes or unix '
                'sockets. If there is some similar way of communication '
                'for local processes, please file an issue to communica\'s'
                'GitHub repository ("Bug Tracker" on PyPI page).'
            )
        self._name = name.lower()

    def repr_address(self) -> str:
        return local_connections.format_address(self._name)

    async def server_start(
            self,
            handshaker: Handshaker,
            client_connected_cb: ClientConnectedCB,
    ):
        return await local_connections.start_server(
            self._create_server_cb(handshaker, client_connected_cb),
            self._name
        )

    async def _open_connection(self):
        return await local_connections.open_connection(self._name)

    def dump_state(self) -> str:
        return json.dumps({'type': self._TYPE, 'name': self._name})

    @classmethod
    def from_state(cls, state: str):
        data = json.loads(state)
        cls._check_dump(data)
        return cls(data['name'])
