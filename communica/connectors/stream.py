import json
import asyncio

from ssl import SSLContext
from abc import abstractmethod
from struct import Struct
from typing import Optional, Any

from typing_extensions import Self

from communica.utils import (
    INT32MAX, NULL_CHAR, ETX_CHAR,
    MessageQueue, logger, json_dumpb, json_loadb
)

from communica.connectors import _stream_local
from communica.connectors.base import (
    BaseConnection, BaseConnector,
    RequestReceivedCB, ClientConnectedCB,
    Handshaker, HandshakeOk, HandshakeFail
)


__all__ = (
    'TcpConnector',
    'LocalConnector'
)

DEFAULT_STREAM_HWM = 32


class StreamConnection(BaseConnection):
    __slots__ = ('reader', 'writer', 'connector', '_send_queue')

    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    connector: 'BaseStreamConnector'

    _send_queue: 'MessageQueue[tuple[bytes, bytes]]'
    _handshake_result: 'HandshakeOk | None'

    _header_packer = Struct(
        '!I'  # message length
         'H'  # data start index
    )
    _MAX_CHUNK_SIZE = INT32MAX + 1 - _header_packer.size

    @property
    def max_chunk_size(self):
        """Максимально допустимый для одного сообщения размер, в байтах"""
        return self._MAX_CHUNK_SIZE

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
        if len(raw_data) > self._MAX_CHUNK_SIZE:
            raise ValueError('Data exceeds max chunk size (%d bytes)' %
                             self._MAX_CHUNK_SIZE)

        await self._send_queue.put((json_dumpb(metadata), raw_data))

    async def close(self):
        self.writer.close()
        await self.writer.wait_closed()

    async def _run_connection(self, request_received_cb: RequestReceivedCB):
        is_closing = self.writer.is_closing
        readexactly = self.reader.readexactly
        header_size = self._header_packer.size
        header_unpack = self._header_packer.unpack

        write_task = asyncio.create_task(self._send_runner())

        try:
            while not is_closing():
                header = await readexactly(header_size)
                chunk_len, data_start = header_unpack(header)
                chunk = await readexactly(chunk_len)

                # XXX: попробовать с memoryview
                request_received_cb(json_loadb(chunk[:data_start]), chunk[data_start:])
        except Exception as e:
            logger.debug(f'Connection broken: {e!r}')
            raise
        finally:
            write_task.cancel()

        return None

    async def _send_runner(self):
        header_pack = self._header_packer.pack

        while True:
            metadata, data = await self._send_queue.get()

            meta_len = len(metadata)
            self.writer.write(
                header_pack(meta_len + len(data), meta_len) +
                metadata +
                data
            )

            # yield to loop, giving chance to pause protocol
            await asyncio.sleep(0)

            await self.writer.drain()

    async def _do_handshake(
            self,
            handshaker: Handshaker,
    ):
        async def send_message(data):
            if NULL_CHAR in data:
                raise ValueError('Handshake messages must NOT contain '
                                 'NULL and ETX characters')

            self.writer.write(data + NULL_CHAR)
            await self.writer.drain()

        async def recv_message() -> bytes:
            try:
                return (await self.reader.readuntil(NULL_CHAR))[:-1]
            except asyncio.IncompleteReadError as e:
                if (pos := e.partial.find(ETX_CHAR)) == -1:
                    raise
                err_obj = json_loadb(e.partial[pos+1:])
                raise HandshakeFail(err_obj['reason'])

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
        import warnings
        warnings.warn(
            'This connector unstable and may lead to message loss'
        )
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
        self._name = name.lower()

    def repr_address(self) -> str:
        return _stream_local.format_address(self._name)

    async def server_start(
            self,
            handshaker: Handshaker,
            client_connected_cb: ClientConnectedCB,
    ):
        return await _stream_local.start_server(
            self._create_server_cb(handshaker, client_connected_cb),
            self._name
        )

    async def _open_connection(self):
        return await _stream_local.open_connection(self._name)

    def dump_state(self) -> str:
        return json.dumps({'type': self._TYPE, 'name': self._name})

    @classmethod
    def from_state(cls, state: str):
        data = json.loads(state)
        cls._check_dump(data)
        return cls(data['name'])
