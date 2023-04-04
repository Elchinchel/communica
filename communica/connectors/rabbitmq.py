# cursed.

import asyncio

try:
    import aiormq
    _HAS_AIORMQ = True
except ImportError:
    _HAS_AIORMQ = False
else:
    from yarl import URL

from enum import Enum
from typing import Any
from datetime import datetime, timedelta
from collections import deque

from typing_extensions import Self

from communica.utils import (
    NULL_CHAR, json_dumpb, json_loadb, HasLoopMixin
)

from communica.connectors.base import (
    BaseConnection, BaseConnector,
    RequestReceivedCB, ClientConnectedCB,
    Handshaker, HandshakeFail
)


_DEFAULT_EXCHANGE = 'communica'


class _MessageType(str, Enum):
    CONNECT_REQUEST = 'communica_client_connect'
    CONNECT_RESPONSE = 'communica_client_connect_ok'
    HS_NEXT = 'communica_handshake_next'
    HS_FAIL = 'communica_handshake_fail'
    HS_DONE = 'communica_handshake_done'
    MESSAGE = 'message'
    CLOSE = 'close'


_connections: \
    'dict[URL, aiormq.Connection | asyncio.Future[aiormq.Connection]]' = {}


def _closed_exceptions():
    return (aiormq.exceptions.ChannelClosed,
            aiormq.exceptions.ConnectionClosed)


class MessageWaiter(HasLoopMixin):
    __slots__ = ('_waiter', '_messages')

    @classmethod
    async def new(
            cls,
            chan: 'aiormq.abc.AbstractChannel',
            queue: str,
            no_ack: bool
    ):
        inst = cls()
        inst._waiter = None
        inst._messages = deque()

        await chan.basic_consume(queue, inst._callback, no_ack=no_ack)

        return inst

    async def _callback(self, message: 'aiormq.abc.DeliveredMessage'):
        if not self._waiter or self._waiter.done():
            self._messages.append(message)
        else:
            self._waiter.set_result(message)
            self._waiter = None

    async def wait(self) -> 'aiormq.abc.DeliveredMessage':
        if self._messages:
            return self._messages.popleft()

        self._waiter = self._get_loop().create_future()
        return await self._waiter


class RmqConnection(BaseConnection):
    __slots__ = ('_connector', '_rmq_chan', '_ready',
                 '_recv_queue', '_send_queue')

    _ready: asyncio.Event
    _rmq_chan: 'aiormq.abc.AbstractChannel'
    _connector: 'RmqConnector'

    @classmethod
    async def _do_handshake_and_create_connection(
            cls,
            chan: 'aiormq.abc.AbstractChannel',
            connector: 'RmqConnector',
            handshaker: Handshaker,
            resp_waiter: MessageWaiter,
            send_queue: str
    ) -> Self:
        inst = cls()

        async def send_message(data: bytes):
            await chan.basic_publish(
                data,
                exchange=connector.exchange,
                routing_key=send_queue,
                properties=aiormq.spec.Basic.Properties(
                    message_type=_MessageType.HS_NEXT
                )
            )

        async def recv_message():
            message = await resp_waiter.wait()
            if message.header.properties.message_type == _MessageType.HS_FAIL:
                raise HandshakeFail.loadb(message.body)
            elif message.header.properties.message_type != _MessageType.HS_NEXT:
                raise ValueError('Unknown message in handshake queue')
            return message.body

        try:
            await inst._run_handshaker(handshaker, send_message, recv_message)
        except HandshakeFail as fail:
            await chan.basic_publish(
                body=fail.dumpb(),
                exchange=connector.exchange,
                routing_key=send_queue,
                properties=aiormq.spec.Basic.Properties(
                    message_type=_MessageType.HS_FAIL
                )
            )
            raise

        inst._ready = asyncio.Event()
        inst._ready.set()
        inst._connector = connector

        return inst

    def _set_run_data(
            self,
            chan: 'aiormq.abc.AbstractChannel',
            recv_queue: str,
            send_queue: str
    ):
        self._rmq_chan = chan
        self._recv_queue, self._send_queue = recv_queue, send_queue

    def update(self, connection: Self) -> None:
        if self._connector._connect_id != connection._connector._connect_id:
            raise ValueError('Got connection with different connect_id')
        if connection._rmq_chan.is_closed:
            raise ValueError('Got connection with closed channel')
        self._ready.set()
        self._rmq_chan = connection._rmq_chan

    async def send(self, metadata: Any, raw_data: bytes):
        body = json_dumpb(metadata) + NULL_CHAR + raw_data
        try:
            await self._send(body)
        except _closed_exceptions():
            self._ready.clear()
            await self._send(body)  # retry once after channel opened
        except aiormq.exceptions.AMQPError:
            self._ready.clear()
            raise

    async def _send(self, body: bytes):
        await self._ready.wait()
        await self._rmq_chan.basic_publish(
            body,
            exchange=self._connector.exchange,
            routing_key=self._send_queue,
            properties=aiormq.spec.Basic.Properties(
                message_type=_MessageType.MESSAGE,
            )
        )

    async def run_until_fail(
            self,
            request_received_cb: RequestReceivedCB
    ) -> None:
        try:
            await self._run_until_fail(request_received_cb)
        except aiormq.exceptions.AMQPError:
            self._ready.clear()
            raise

    async def _run_until_fail(self, request_received_cb):
        async def ack_message(message):
            await self._rmq_chan.basic_ack(message.delivery_tag)

        waiter = await MessageWaiter.new(self._rmq_chan,
                                         self._recv_queue, no_ack=False)

        while not self._rmq_chan.is_closed:
            message = await waiter.wait()

            if message.header.properties.message_type == _MessageType.MESSAGE:
                null_pos = message.body.find(NULL_CHAR)
                if null_pos == -1:
                    await ack_message(message)
                    await self._close(True)
                    raise ValueError('Got message without metadata separator')

                metadata = json_loadb(message.body[:null_pos])
                request_received_cb(metadata, message.body[null_pos+1:])
                await ack_message(message)

            elif message.header.properties.message_type == _MessageType.CLOSE:
                timestamp = message.header.properties.timestamp
                if timestamp is None:
                    await ack_message(message)
                    await self._close(True)
                    raise ValueError('Got close request without timestamp')
                await ack_message(message)
                if datetime.now() - timestamp > timedelta(seconds=5):
                    continue
                await self._close(False)
                return

            else:
                await ack_message(message)
                await self._close(True)
                message_type = message.header.properties.message_type
                raise ValueError('Unknown message in message queue, '
                                f'got type {message_type!r}')

    async def close(self) -> None:
        await self._close(True)

    async def _close(self, send_close_request: bool):
        if not self._ready.is_set():
            return
        self._ready.clear()

        if self._rmq_chan.is_closed or self._rmq_chan.connection.is_closed:
            return

        try:
            if send_close_request:
                await self._rmq_chan.basic_publish(
                    b'',
                    exchange=self._connector.exchange,
                    routing_key=self._send_queue,
                    properties=aiormq.spec.Basic.Properties(
                        timestamp=datetime.now(),
                        message_type=_MessageType.CLOSE,
                    )
                )
            await self._rmq_chan.close()
        except _closed_exceptions():
            return


class RmqServer(asyncio.AbstractServer):
    @property
    def exchange(self):
        return self._connector._exchange

    def __init__(
            self,
            connector: 'RmqConnector',
            rmq_chan: 'aiormq.abc.AbstractChannel',
            handshaker: Handshaker,
            client_connected_cb: ClientConnectedCB
    ) -> None:
        self._chan = rmq_chan
        self._connector = connector
        self._handshaker = handshaker
        self._client_connected_cb = client_connected_cb

    def is_serving(self):
        """Return True if the server is accepting connections."""
        return not self._chan.is_closed

    def close(self):
        def del_task(_):
            del(self._close_task)

        self._close_task = asyncio.create_task(self._chan.close())
        self._close_task.add_done_callback(del_task)

    async def wait_closed(self):
        if not hasattr(self, '_close_task'):
            raise RuntimeError('wait_closed() should be '
                               'called right after close() method')
        await self._close_task

    async def _on_connect(self, message: 'aiormq.abc.DeliveredMessage'):
        if message.header.properties.message_type != _MessageType.CONNECT_REQUEST:
            raise ValueError('Unknown message in connect request queue')

        if message.header.properties.reply_to is None:
            raise ValueError('Reply queue not specified in connect request')
        hs_to_cli_queue = message.header.properties.reply_to
        connect_id = json_loadb(message.body)

        hs_to_srv_queue = await self._connector._queue_declare_and_bind(
            self._chan, exclusive=True
        )

        await self._chan.basic_publish(
            b'',
            exchange=self.exchange,
            routing_key=hs_to_cli_queue,
            properties=aiormq.spec.Basic.Properties(
                message_type=_MessageType.CONNECT_RESPONSE,
                reply_to=hs_to_srv_queue
            )
        )

        await self._chan.basic_ack(message.delivery_tag)  # type: ignore

        resp_waiter = await MessageWaiter.new(self._chan,
                                              hs_to_srv_queue, no_ack=True)

        try:
            conn = await RmqConnection._do_handshake_and_create_connection(
                self._chan,
                self._connector,
                self._handshaker,
                resp_waiter,
                hs_to_cli_queue
            )
        except HandshakeFail:
            return
        finally:
            if not self._chan.is_closed:
                await self._chan.queue_delete(hs_to_srv_queue)

        to_cli_queue, to_srv_queue = \
            self._connector._get_transport_queue_names(connect_id)


        await self._connector._queue_declare_and_bind(
            self._chan, name=to_cli_queue, durable=True
        )
        await self._connector._queue_declare_and_bind(
            self._chan, name=to_srv_queue, durable=True
        )

        await self._chan.basic_publish(
            b'',
            exchange=self.exchange,
            routing_key=hs_to_cli_queue,
            properties=aiormq.spec.Basic.Properties(
                message_type=_MessageType.HS_DONE
            )
        )

        # open new to prevent server's chan to be closed by connection
        run_chan = await self._connector._open_channel()
        conn._set_run_data(run_chan, to_srv_queue, to_cli_queue)
        self._client_connected_cb(conn)

    async def start_serving(self):
        queue_name = self._connector._get_connect_queue_name()

        await self._connector._queue_declare_and_bind(
            self._chan, queue_name, durable=True
        )
        await self._chan.basic_consume(queue_name, self._on_connect)

    def get_loop(self):
        raise NotImplementedError

    async def serve_forever(self):
        raise NotImplementedError


class RmqConnector(BaseConnector):
    """
    Uses RabbitMQ for communication.

    WARNING:
        For this connector to work properly, only one client
        with same address and connect_id must be connected at same time.

        For those, who connect, connect_id must be set explicitly
        and persist between service restarts.

        Failure to comply with these rules
        leads to unclosed queues growth and message loss.
    """

    _TYPE = 'RABBITMQ'

    __slots__ = ('_url', '_address', '_exchange', '_connect_id')

    @property
    def exchange(self):
        return self._exchange

    def __init__(
            self,
            url: str,
            address: str,
            connect_id: 'str | None' = None,
            exchange_name: str = _DEFAULT_EXCHANGE,
    ) -> None:
        """
        Max summary length of address and connect_id is 224 characters

        Args:
            url (str): used to create connection with RabbitMQ server.
              Format specified at https://www.rabbitmq.com/uri-spec.html.
            address (str): unique identifier for client-server pair.
              If more than one server with same address bound to same exchange,
              behaviour undefined.
            connect_id (str): unique identifier of connecting process.
              Must be set for clients.
        """
        if not _HAS_AIORMQ:
            raise ImportError('RmqConnector requires aiormq library. '
                              'Install communica with [rabbitmq] extra.')

        if connect_id is not None and len(address + connect_id) > 224:
            raise ValueError('Max address + connect_id length is 224 characters')

        self._url = URL(url)
        self._address = address
        self._exchange = exchange_name
        self._connect_id = connect_id

    def _get_connect_queue_name(self):
        return self._fmt_queue_name('connect', 'server')

    def _get_transport_queue_names(self, connect_id: str):
        return (
            self._fmt_queue_name('toClient', connect_id),
            self._fmt_queue_name('toServer', connect_id)
        )

    def _fmt_queue_name(self, *parts):
        return f'communica.{self._address}.' + '.'.join(parts)

    def repr_address(self) -> str:
        if self._url.password is not None:
            return self._url.with_password("[PASSWORD]").human_repr()
        return self._url.human_repr()

    async def server_start(
            self,
            handshaker: Handshaker,
            client_connected_cb: ClientConnectedCB,
    ) -> asyncio.AbstractServer:
        chan = await self._open_channel()

        await chan.basic_qos(prefetch_count=1)

        await chan.exchange_declare(
            self._exchange,
            exchange_type="direct",
            durable=True,
        )

        server = RmqServer(self, chan, handshaker, client_connected_cb)
        await server.start_serving()
        return server

    async def _client_connect(
            self,
            handshaker: Handshaker,
            chan: 'aiormq.abc.AbstractChannel'
    ):
        await chan.basic_qos(prefetch_count=1)

        hs_to_cli_queue = \
            await self._queue_declare_and_bind(chan, exclusive=True)

        await chan.basic_publish(
            body=json_dumpb(self._connect_id),
            exchange=self.exchange,
            routing_key=self._get_connect_queue_name(),
            properties=aiormq.spec.Basic.Properties(
                reply_to=hs_to_cli_queue,
                message_type=_MessageType.CONNECT_REQUEST
            )
        )

        resp_waiter = \
            await MessageWaiter.new(chan, hs_to_cli_queue, no_ack=True)

        message = await resp_waiter.wait()
        if message.header.properties.message_type != _MessageType.CONNECT_RESPONSE:
            raise ValueError('Unknown message in connect response queue')

        if message.header.properties.reply_to is None:
            raise ValueError('Server queue not specified in connect response')
        hs_to_srv_queue = message.header.properties.reply_to

        try:
            conn = await RmqConnection._do_handshake_and_create_connection(
                chan,
                self,
                handshaker,
                resp_waiter,
                hs_to_srv_queue
            )
        except Exception:
            if not chan.is_closed:
                await chan.queue_delete(hs_to_cli_queue)
            raise

        message = await resp_waiter.wait()
        await chan.queue_delete(hs_to_cli_queue)

        if message.header.properties.message_type != _MessageType.HS_DONE:
            message_type = message.header.properties.message_type
            raise ValueError(f'Got message with "{message_type}" type '
                              'instead of handshake confirmation')

        to_cli_queue, to_srv_queue = \
            self._get_transport_queue_names(self._connect_id)  # type: ignore
        conn._set_run_data(chan, to_cli_queue, to_srv_queue)

        return conn

    async def client_connect(self, handshaker: Handshaker) -> BaseConnection:
        if self._connect_id is None:
            raise TypeError('Cannot connect to server. For those, who connect, '
                            'connect_id parameter must be set. '
                            'Check RmqConnector docs for details.')

        chan = await self._open_channel()
        try:
            return await self._client_connect(handshaker, chan)
        except Exception:
            await chan.close()
            raise


    async def cleanup(self):
        """
        Drop all pending messages and
        delete queues with connector's connect_id

        If connect_id is None, noop
        """
        if self._connect_id is None:
            return

        for queue in self._get_transport_queue_names(self._connect_id):
            chan = await self._open_channel()
            try:
                await chan.queue_purge(queue)
                await chan.queue_delete(queue)
            except aiormq.exceptions.ChannelNotFoundEntity:
                # channel will be closed by server if we try to delete unknown queue
                continue
            await chan.close()

    def dump_state(self) -> str:
        """Unsupported for this connector"""
        raise TypeError('This method unsupported cause user and password '
                        'are not encrypted in dump, which is insecure')

    @classmethod
    def from_state(cls, state: str) -> Self:
        """Unsupported for this connector"""
        raise TypeError('This method unsupported cause user and password '
                        'are not encrypted in dump, which is insecure')

    async def _open_channel(self):
        # there is two limits for simultaneously opened channels:
        # 65k defined by AMQP protocol (channel number range),
        # which cause 'while not (some channel closed)' loop in conn.channel()
        # and 'max-channels' limit set by AMQP server
        # (RabbitMQ 3.11 default is 2047), which cause connection termination
        # it's unlikely to happen, but i'll keep this in mind ( in comment :D )

        if (conn := _connections.get(self._url)) is None:
            fut = asyncio.get_running_loop().create_future()
            _connections[self._url] = fut
            try:
                conn = await aiormq.connect(self._url)
            except Exception as e:
                fut.set_exception(e)
                del(_connections[self._url])
                raise
            else:
                fut.set_result(conn)
                _connections[self._url] = conn  # type: ignore

        elif isinstance(conn, asyncio.Future):
            conn = await conn

        elif conn.is_closed:
            del(_connections[self._url])
            return await self._open_channel()

        return await conn.channel()

    async def _queue_declare_and_bind(
            self,
            chan: 'aiormq.abc.AbstractChannel',
            name: str = '',
            **declare_kwargs
    ):
        declare_ok = await chan.queue_declare(queue=name, **declare_kwargs)
        assert declare_ok.queue is not None, 'Incompatible AMQP server'
        queue_name = declare_ok.queue

        await chan.queue_bind(queue_name, self.exchange, routing_key=queue_name)
        return queue_name
