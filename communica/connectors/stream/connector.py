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


@dataclass
class MessageFrame(Frame):
    CODE = 10

    metadata: Any
    raw_data: 'bytes | memoryview'

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
    metadata: 'bytes | None'
    raw_data: 'bytes | memoryview'

    # Packer for chunk_index, total_chunks, metadata_length (3 x uint32)
    CHUNK_HEADER_PACKER = Struct('!III')
    chunk_header_size = CHUNK_HEADER_PACKER.size
    chunk_header_pack = CHUNK_HEADER_PACKER.pack
    chunk_header_unpack_from = CHUNK_HEADER_PACKER.unpack_from

    @classmethod
    def _load(cls, data: memoryview):
        chunk_index, total_chunks, metadata_length = cls.chunk_header_unpack_from(data, offset=0)

        # Load metadata if present (first chunk only)
        if metadata_length > 0:
            metadata_start = cls.chunk_header_size
            metadata_end = metadata_start + metadata_length
            metadata = data[metadata_start:metadata_end]
            raw_data = data[metadata_end:]
        else:
            # Non-first chunks have no metadata
            metadata = None
            raw_data = data[cls.chunk_header_size:]

        return cls(
            chunk_index,
            total_chunks,
            metadata,
            raw_data
        )

    def to_bytes(self) -> bytes:
        # Calculate metadata length (0 for non-first chunks)
        metadata_length = len(self.metadata) if self.metadata is not None else 0

        result = (
            self.code_byte +
            self.chunk_header_pack(self.chunk_index, self.total_chunks, metadata_length)
        )

        # Only first chunk includes metadata
        if metadata_length > 0:
            result += self.metadata

        result += self.raw_data
        return result


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
        'reader', 'writer', 'connector', '_send_queue',
        '_incomplete_message'
    )

    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    connector: 'BaseStreamConnector'

    _send_queue: 'MessageQueue[Frame]'
    _handshake_result: 'HandshakeOk | None'
    _incomplete_message: 'list[ChunkedMessageFrame] | None'

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
        self._incomplete_message = None

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

        # Calculate total size for MessageFrame (without outer header)
        message_frame_size = (
            1 +  # code byte
            Frame.length_size +  # metadata length field
            len(metadata_bytes) +
            len(raw_data)
        )

        # If message fits in single frame, send as regular MessageFrame
        if message_frame_size <= self._MAX_CHUNK_SIZE:
            await self._send_queue.put(
                MessageFrame(metadata_bytes, raw_data)
            )
            return

        # Message needs to be chunked
        # Calculate overhead for first chunk (includes metadata)
        first_chunk_overhead = (
            1 +  # code byte
            ChunkedMessageFrame.chunk_header_size +  # chunk_index, total_chunks, metadata_length
            len(metadata_bytes)
        )

        # Calculate overhead for subsequent chunks (no metadata)
        other_chunk_overhead = (
            1 +  # code byte
            ChunkedMessageFrame.chunk_header_size  # chunk_index, total_chunks, metadata_length (=0)
        )

        # Calculate max data for first chunk and other chunks
        max_first_chunk_data = self._MAX_CHUNK_SIZE - first_chunk_overhead
        max_other_chunk_data = self._MAX_CHUNK_SIZE - other_chunk_overhead

        # Split data into chunks
        chunks = []
        offset = 0

        # First chunk
        first_chunk_size = min(max_first_chunk_data, len(raw_data))
        chunks.append(ChunkedMessageFrame(
            chunk_index=0,
            total_chunks=0,  # Will be updated later
            metadata=metadata_bytes,
            raw_data=raw_data[offset:offset + first_chunk_size]
        ))
        offset += first_chunk_size

        # Remaining chunks
        chunk_index = 1
        while offset < len(raw_data):
            chunk_size = min(max_other_chunk_data, len(raw_data) - offset)
            chunks.append(ChunkedMessageFrame(
                chunk_index=chunk_index,
                total_chunks=0,  # Will be updated later
                metadata=None,
                raw_data=raw_data[offset:offset + chunk_size]
            ))
            offset += chunk_size
            chunk_index += 1

        # Update total_chunks in all frames
        total_chunks = len(chunks)
        for chunk in chunks:
            chunk.total_chunks = total_chunks

        # Wait for queue to have space for first chunk, then send all chunks atomically
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
                logger.debug('Received %r', frame)
                if isinstance(frame, MessageFrame):
                    request_received_cb(frame.metadata, frame.raw_data)
                elif isinstance(frame, ChunkedMessageFrame):
                    self._handle_chunked_frame(frame, request_received_cb)
                elif isinstance(frame, CloseNotifyFrame):
                    self.writer.close()
                    return
                else:
                    logger.warning('wtf')
        except Exception as e:
            logger.info('Connection broken: %r', e)
            raise
        finally:
            # Discard incomplete messages on connection loss
            self._incomplete_message = None
            write_task.cancel()
            await asyncio.wait([write_task])

    def _handle_chunked_frame(
            self,
            frame: ChunkedMessageFrame,
            request_received_cb: RequestReceivedCB
    ):
        # If we receive a non-first chunk without having the first chunk,
        # discard it (can happen after connection loss)
        if frame.chunk_index != 0 and self._incomplete_message is None:
            logger.warning(
                'Received chunk %d/%d without first chunk, discarding',
                frame.chunk_index, frame.total_chunks
            )
            return

        # First chunk - initialize incomplete message
        if frame.chunk_index == 0:
            self._incomplete_message = [frame]
            return

        # Subsequent chunks - append to list
        self._incomplete_message.append(frame)

        # Check if all chunks have been received
        if len(self._incomplete_message) != frame.total_chunks:
            return

        # Reassemble the message
        reassembled_data = b''.join(
            chunk.raw_data for chunk in self._incomplete_message
        )

        # Deliver the complete message (metadata is in first chunk)
        # Deserialize metadata from bytes
        metadata = json_loadb(self._incomplete_message[0].metadata)
        request_received_cb(metadata, reassembled_data)

        # Clean up
        self._incomplete_message = None

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
