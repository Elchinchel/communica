import json
import asyncio
import logging
from abc import ABC, abstractmethod
from ssl import SSLContext
from math import ceil
from struct import Struct
from typing import Any, Dict, Type, ClassVar, Optional
from contextlib import suppress
from dataclasses import dataclass

from typing_extensions import Self

from communica.utils import (
    ETX_CHAR,
    NULL_CHAR,
    UINT32MAX,
    ByteSeq,
    MessageQueue,
    fmt_task_name,
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
    BaseConnectorServer,
)
from communica.serializers.json import json_dumpb, json_loadb
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


# TODO: keepalives maybe? is they needed?
@dataclass
class MessageFrame(Frame):
    CODE = 10

    header_size = Frame.length_size

    metadata: ByteSeq
    raw_data: ByteSeq

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
class ChunkedMessageFrame(Frame):
    CODE = 11

    chunk_index: int
    total_chunks: int
    metadata: ByteSeq
    raw_data: ByteSeq

    CHUNK_HEADER_PACKER = Struct(
        '!I'  # chunk index
         'I'  # total chunks
         'I'  # metadata length
    )
    chunk_header_size = CHUNK_HEADER_PACKER.size
    chunk_header_pack = CHUNK_HEADER_PACKER.pack
    chunk_header_unpack_from = CHUNK_HEADER_PACKER.unpack_from

    @classmethod
    def _load(cls, data: memoryview):
        chunk_index, total_chunks, metadata_length = \
            cls.chunk_header_unpack_from(data, offset=0)

        metadata_end = cls.chunk_header_size + metadata_length
        metadata = data[cls.chunk_header_size:metadata_end]
        raw_data = data[metadata_end:]

        return cls(
            chunk_index,
            total_chunks,
            metadata,
            raw_data
        )

    def to_bytes(self) -> bytes:
        return (
            self.code_byte +
            self.chunk_header_pack(self.chunk_index, self.total_chunks, len(self.metadata)) +
            self.metadata +  # might be empty
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
    __slots__ = (
        'reader', 'writer', 'connector', '_send_queue'
    )

    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    connector: 'BaseStreamConnector'

    _send_queue: 'MessageQueue[Frame]'
    _handshake_result: 'HandshakeOk | None'

    _MAX_CHUNK_SIZE = (
        UINT32MAX -
        len(MessageFrame.code_byte) -
        MessageFrame.header_size
    )
    """Includes length of message header and code byte"""

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
        metadata_bytes = json_dumpb(metadata)

        if len(metadata_bytes) + len(raw_data) <= self._MAX_CHUNK_SIZE:
            await self._send_queue.put(
                MessageFrame(metadata_bytes, raw_data)
            )
            return

        # There is actually one byte more than this but whatever
        other_max_size = (
            self._MAX_CHUNK_SIZE -
            ChunkedMessageFrame.chunk_header_size
        )
        first_max_size = other_max_size - len(metadata)
        total_chunks = 1 + ceil(
            (len(raw_data) - first_max_size) / other_max_size
        )

        offset = 0
        chunks = [
            ChunkedMessageFrame(
                chunk_index=0,
                total_chunks=total_chunks,
                metadata=metadata_bytes,
                raw_data=raw_data[:first_max_size]
            )
        ]
        offset += first_max_size

        for chunk_index in range(1, total_chunks):
            chunks.append(ChunkedMessageFrame(
                chunk_index=chunk_index,
                total_chunks=total_chunks,
                metadata=b'',
                raw_data=raw_data[offset:offset+other_max_size]
            ))
            offset += other_max_size

        await self._send_queue.put(chunks[0])
        for chunk in chunks[1:]:
            self._send_queue.put_nowait(chunk)

    async def close(self):
        close_chunk = CloseNotifyFrame.to_bytes_with_header()
        with suppress(BrokenPipeError):
            self.writer.write(close_chunk)
        self.writer.close()
        with suppress(AssertionError):
            self.reader.feed_data(close_chunk)
        with suppress(BrokenPipeError):
            await self.writer.wait_closed()

    async def _run_connection(self, request_received_cb: RequestReceivedCB):
        is_closing = self.writer.is_closing
        readexactly = self.reader.readexactly
        header_size = Frame.length_size
        header_unpack = Frame.length_unpack_from
        chunked_frames_buf = []

        write_task = asyncio.create_task(
            self._send_runner(),
            name=fmt_task_name('stream-connector-writer')
        )
        try:
            while not is_closing():
                header = await readexactly(header_size)
                chunk_len = header_unpack(header)[0]
                chunk = memoryview(await readexactly(chunk_len))

                frame = Frame.from_bytes(chunk)
                logger.debug('Received %r',
                             frame if len(chunk) < 1024 else frame.__class__)
                if isinstance(frame, MessageFrame):
                    request_received_cb(frame.metadata, frame.raw_data)
                elif isinstance(frame, ChunkedMessageFrame):
                    self._handle_chunked_frame(
                        frame, chunked_frames_buf, request_received_cb
                    )
                elif isinstance(frame, CloseNotifyFrame):
                    self.writer.close()
                    return
                else:
                    logger.warning('wtf %r', frame)
        except Exception as e:
            logger.info('Connection broken: %r', e)
            raise
        finally:
            write_task.cancel()
            await asyncio.wait([write_task])

    def _handle_chunked_frame(
            self,
            frame: ChunkedMessageFrame,
            chunks: list[ChunkedMessageFrame],
            request_received_cb: RequestReceivedCB
    ):
        if frame.chunk_index != 0 and not chunks:
            logger.warning(
                'Received chunk %d/%d without first chunk, discarding',
                frame.chunk_index, frame.total_chunks
            )
            return

        chunks.append(frame)
        if len(chunks) != frame.total_chunks:
            return

        reassembled_data = b''.join(chunk.raw_data for chunk in chunks)
        metadata = json_loadb(chunks[0].metadata)
        chunks.clear()

        request_received_cb(metadata, reassembled_data)

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
            logger.debug('Sent %r',
                         frame if len(data) < 1024 else frame.__class__)

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
        server = await asyncio.start_server(
            self._create_server_cb(handshaker, client_connected_cb),
            host=self.host,
            port=self.port,
            ssl=self.ssl
        )
        return BaseConnectorServer(server)

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
        self._name = name.lower()  # XXX: any reason to do this?

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
