"""
In-process connector for testing purposes
"""
import os
import asyncio
import logging
import threading
from typing import Callable, Awaitable
from collections import deque
from asyncio.futures import wrap_future
from concurrent.futures import Future

from communica.connectors.base import (
    Handshaker,
    BaseConnector,
    HandshakeFail,
    BaseConnection,
    ClientConnectedCB,
    RequestReceivedCB,
    BaseConnectorServer,
)
from communica.serializers.json import json_dumpb, json_loadb


logger = logging.getLogger('communica.connectors.dummy')


class _MemoryPipe:
    # this wasn't tested for thread and coroutine safety

    def __init__(self, label: str) -> None:
        self.lock = threading.Lock()
        self.alock = asyncio.Lock()
        self.waiters: deque[Future] = deque()
        self.label = label

    def _last_pending_waiter(self):
        if self.waiters:
            waiter = self.waiters[-1]
            if not waiter.done():
                return waiter
        return self._create_waiter()

    def _create_waiter(self):
        waiter = Future()
        self.waiters.append(waiter)
        return waiter

    def send(self, data: bytes):
        # logger.debug('[%s] sending %r', self.label, data)
        with self.lock:
            waiter = self._last_pending_waiter()
            waiter.set_result(data)

    def recv_waiter(self):
        with self.lock:
            if not self.waiters:
                return self._create_waiter()
            else:
                return self.waiters[0]

    async def arecv(self):
        async with self.alock:
            # logger.debug('[%s] receiving...', self.label)
            waiter = self.recv_waiter()
            result = await wrap_future(waiter)
            # logger.debug('[%s] received %r', self.label, result)
            with self.lock:
                assert self.waiters.popleft() is waiter
            return result


class Disconnect(Exception):
    pass


class CantConnect(Exception):
    pass


class InvalidState(Exception):
    pass


class DummyConnection(BaseConnection):
    def __init__(
            self,
            read: Callable[[], Awaitable[bytes]],
            write: Callable[[bytes], None],
            server: 'DummyServer'
    ) -> None:
        self.read = read
        self.write = write
        self._alive = False
        self._server = server

    def update(self, connection: 'DummyConnection') -> None:
        self.read = connection.read
        self.write = connection.write
        assert self._server is connection._server

    async def send(self, metadata, raw_data: bytes):
        self.write(json_dumpb(metadata) + b'\0' + raw_data)

    async def run_until_fail(self, request_received_cb: RequestReceivedCB) -> None:
        self._alive = True
        try:
            while self._server.is_serving():
                message = await self.read()
                if message == b'close':
                    self.write(b'close')
                    return

                meta_raw, _, data = message.partition(b'\0')
                request_received_cb(json_loadb(meta_raw), data)
        finally:
            self._alive = False

    async def close(self) -> None:
        self.write(b'close')

    async def _do_handshake(self, handshaker: Handshaker):
        async def send_message(data):
            self.write(b'\1' + data)

        async def recv_message() -> bytes:
            data = await self.read()
            if data[0] == 1:
                return data[1:]
            else:
                raise HandshakeFail.loadb(data[1:])

        try:
            await self._run_handshaker(handshaker, send_message, recv_message)
            return True
        except HandshakeFail as fail:
            logger.warning('Handshake failed: %r', fail)
            self.write(b'\2' + fail.dumpb())
            return False


class DummyServer(BaseConnectorServer):
    def __init__(
            self,
            connector: 'DummyConnector',
            handshaker: Handshaker,
            client_connected_cb: ClientConnectedCB
    ) -> None:
        self.connector = connector
        self.handshaker = handshaker
        self.client_connected_cb = client_connected_cb

        self.serving = True
        self.connector.server = self

    def _connect(self):
        to_client, to_server = _MemoryPipe('to_client'), _MemoryPipe('to_server')

        async def accept_connect():
            server_conn = DummyConnection(to_server.arecv, to_client.send, self)
            if await server_conn._do_handshake(self.handshaker):
                self.client_connected_cb(server_conn)

        asyncio.create_task(accept_connect())

        return to_client.arecv, to_server.send

    def close(self) -> None:
        self.serving = False
        self.connector.server = None

    def is_serving(self) -> bool:
        return self.serving

    async def wait_closed(self):
        pass


class DummyConnector(BaseConnector):
    # This connector differs from the others.
    # All connectors are meant to be stateless,
    # however this connector stores server state.
    # Not cool, but I don't see any problems with this (yet).

    def __init__(self) -> None:
        self.server: DummyServer | None = None

    def repr_address(self) -> str:
        return f'[pid={os.getpid()}, id={hex(id(self))}]'

    async def server_start(
            self,
            handshaker: Handshaker,
            client_connected_cb: ClientConnectedCB
    ) -> BaseConnectorServer:
        if self.server is not None:
            raise InvalidState('Server already started, close it first.')
        return DummyServer(self, handshaker, client_connected_cb)

    async def client_connect(self, handshaker: Handshaker) -> BaseConnection:
        if self.server is None:
            raise CantConnect('Server is closed/wasn\'t started')
        read, write = self.server._connect()
        conn = DummyConnection(read, write, self.server)
        if not await conn._do_handshake(handshaker):
            raise CantConnect('Handshake fail')
        return conn
