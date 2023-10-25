# cursed.

import asyncio
import weakref

try:
    import aiormq
    _HAVE_AIORMQ = True
except ModuleNotFoundError:
    _HAVE_AIORMQ = False
else:
    from yarl import URL

from abc import ABC, abstractmethod
from enum import Enum
from typing import Any
from datetime import datetime, timedelta
from collections import deque

from typing_extensions import Self

from communica.utils import (
    NULL_CHAR, HasLoopMixin, json_dumpb, json_loadb,
    exc_log_callback
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

    CLOSE = 'close'
    MESSAGE = 'message'
    LISTENING = 'listening'


_connections: '''dict[
    URL,
    aiormq.abc.AbstractConnection | asyncio.Future[aiormq.abc.AbstractConnection]
]''' = {}


def _closed_exceptions():
    return (aiormq.exceptions.ChannelClosed,  # pyright: ignore[reportUnboundVariable]
            aiormq.exceptions.ConnectionClosed)  # pyright: ignore[reportUnboundVariable]


async def _publish(
        chan: 'aiormq.abc.AbstractChannel',
        msg_type: str,
        body: bytes,
        exchange: str,
        routing_key: str,
        properties_kwargs: 'dict[str, Any]' = {}
):
    return await chan.basic_publish(
        body,
        exchange=exchange,
        routing_key=routing_key,
        properties=aiormq.spec.Basic.Properties(  # pyright: ignore[reportUnboundVariable]
            message_type=msg_type,
            **properties_kwargs
        )
    )


class MessageWaiter(HasLoopMixin):
    __slots__ = ('_waiter', '_messages', '_timeout')

    @classmethod
    async def new(
            cls,
            chan: 'aiormq.abc.AbstractChannel',
            queue: str,
            *,
            no_ack: bool,
            timeout: 'int | None' = None
    ):
        inst = cls()
        inst._waiter = None
        inst._timeout = timeout
        inst._messages = deque()

        await chan.basic_consume(queue, inst._callback, no_ack=no_ack)

        return inst

    async def _callback(self, message: 'aiormq.abc.DeliveredMessage'):
        if not self._waiter or self._waiter.done():
            self._messages.append(message)
        else:
            self._waiter.set_result(message)
            self._waiter = None

    def set_timeout_duration(self, timeout: int):
        if self._waiter and not self._waiter.done():
            raise RuntimeError('Waiter not completed')
        self._timeout = timeout

    def _set_timeout(self, fut: asyncio.Future):
        if not fut.done():
            fut.set_exception(asyncio.TimeoutError)

    def interrupt(self):
        if self._waiter:
            self._waiter.set_exception(asyncio.TimeoutError)

    async def wait(self) -> 'aiormq.abc.DeliveredMessage':
        if self._messages:
            return self._messages.popleft()

        if self._waiter and not self._waiter.done():
            raise RuntimeError(f'Duplicate .wait() call on {self!r}')

        self._waiter = self._get_loop().create_future()

        if self._timeout:
            self._get_loop().call_later(
                self._timeout, self._set_timeout, self._waiter)

        return await self._waiter


class ConnectionCheckPolicy(HasLoopMixin, ABC):
    period: float
    _handle: 'asyncio.TimerHandle | None'

    def message_sent(self):
        pass

    def message_received(self):
        pass

    @abstractmethod
    def _trigger(self):
        raise NotImplementedError

    @abstractmethod
    def _cancel(self):
        raise NotImplementedError

    @abstractmethod
    def replace_conn(self, conn: 'RmqConnection') -> Self:
        raise NotImplementedError

    def _set_handle(self, period: 'float | None' = None):
        if period is None:
            self._last_message = self._get_loop().time()
            period = self.period
        self._handle = self._get_loop().call_later(period, self._trigger)


class ServerCheckPolicy(ConnectionCheckPolicy):
    def __init__(self, conn: 'RmqConnection', period: float) -> None:
        self._get_loop()

        self.period = period
        self._waiter = self._get_loop().create_future()

        self._set_handle()

        self._send_task = self._get_loop().create_task(self._sender())
        self._send_task.add_done_callback(exc_log_callback)
        self._conn = weakref.ref(conn, self._conn_died)

    def replace_conn(self, conn: 'RmqConnection'):
        conn._check_policy._cancel()
        self._conn = weakref.ref(conn, self._conn_died)
        return self

    def _cancel(self):
        if self._handle is not None:
            self._handle.cancel()
        self._conn_died(None)

    def _conn_died(self, _):
        if not self._waiter.done():
            self._waiter.set_result(False)

    def _trigger(self):
        if self._waiter.done():
            return

        time_diff = self._get_loop().time() - self._last_message
        if time_diff < self.period:
            self._set_handle(self.period - time_diff)
            return

        self._handle = None
        self._waiter.set_result(True)
        self._waiter = self._get_loop().create_future()

    async def _sender(self):
        while (await self._waiter):
            if (conn := self._conn()) is None:
                return
            await conn._send(_MessageType.LISTENING, b'')
            del(conn)  # deleting reference
            if self._handle is None:
                self._set_handle()

    def message_sent(self):
        if self._handle is None:
            self._set_handle()
        else:
            self._last_message = self._get_loop().time()


class ClientCheckPolicy(ConnectionCheckPolicy):
    def __init__(self, conn: 'RmqConnection', period: float) -> None:
        self._get_loop()

        self.period = period
        self._set_handle()

        self._conn = weakref.ref(conn)
        self._close_task = None

    def replace_conn(self, conn: 'RmqConnection'):
        conn._check_policy._cancel()
        self._conn = weakref.ref(conn)
        return self

    def _cancel(self):
        if self._handle is not None:
            self._handle.cancel()

    def _trigger(self):
        if (conn := self._conn()) is None:
            return

        time_diff = self._get_loop().time() - self._last_message
        if time_diff < self.period:
            print('RESETTING', int(self._last_message), int(time_diff), self.period, id(self))
            self._set_handle(self.period - time_diff)
            return

        print('CLOSING', int(self._last_message), int(self._get_loop().time()), id(self))
        self._close_task = self._get_loop().create_task(conn.close())

    def message_received(self):
        print('MSG_RCVD', int(self._last_message))
        if self._close_task is None:
            self._last_message = self._get_loop().time()
            print('LAST_MSG_SET', int(self._last_message), id(self))


class RmqConnection(BaseConnection):
    __slots__ = ('_connector', '_rmq_chan', '_ready', '_closing',
                 '_recv_queue', '_send_queue', '_check_policy',
                 '__weakref__')

    _ready: asyncio.Event
    _closing: 'asyncio.Future | None'
    _rmq_chan: 'aiormq.abc.AbstractChannel'
    _connector: 'RmqConnector'

    @property
    def is_alive(self):
        return self._ready.is_set()

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
            await _publish(
                chan,
                msg_type=_MessageType.HS_NEXT,
                body=data,
                exchange=connector.exchange,
                routing_key=send_queue
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
            await _publish(
                chan,
                msg_type=_MessageType.HS_FAIL,
                body=fail.dumpb(),
                exchange=connector.exchange,
                routing_key=send_queue
            )
            raise

        inst._ready = asyncio.Event()
        inst._ready.set()
        inst._closing = None
        inst._connector = connector

        return inst

    def __repr__(self) -> str:
        cls_name = self.__class__.__name__
        if self._ready:
            state = f'on channel {self._rmq_chan.number}'
        else:
            state = 'not ready'
        return f'<{cls_name} {state}>'

    def _set_run_data(
            self,
            chan: 'aiormq.abc.AbstractChannel',
            policy: ConnectionCheckPolicy,
            recv_queue: str,
            send_queue: str
    ):
        self._rmq_chan = chan
        self._check_policy = policy
        self._recv_queue, self._send_queue = recv_queue, send_queue

    def update(self, connection: Self) -> None:
        if self._connector._connect_id != connection._connector._connect_id:
            raise ValueError('Got connection with different connect_id')
        if connection._rmq_chan.is_closed:
            raise ValueError('Got connection with closed channel')

        self._ready.set()
        self._closing = None
        self._rmq_chan = connection._rmq_chan
        self._check_policy = connection._check_policy.replace_conn(self)

    async def send(self, metadata: Any, raw_data: bytes):
        body = json_dumpb(metadata) + NULL_CHAR + raw_data
        try:
            await self._send(_MessageType.MESSAGE, body)
        except _closed_exceptions():
            self._ready.clear()
            await self._send(_MessageType.MESSAGE, body)  # retry once after channel opened
        except aiormq.exceptions.AMQPError:
            self._ready.clear()
            raise
        self._check_policy.message_sent()

    async def _send(self, msg_type: _MessageType, body: bytes):
        await self._ready.wait()
        await _publish(
            self._rmq_chan,
            msg_type=msg_type,
            body=body,
            exchange=self._connector.exchange,
            routing_key=self._send_queue
        )

    async def run_until_fail(
            self,
            request_received_cb: RequestReceivedCB
    ) -> None:
        try:
            await self._run_until_fail(request_received_cb)
        except asyncio.TimeoutError:
            return
        finally:
            self._ready.clear()

    async def _run_until_fail(self, request_received_cb):
        async def ack_message(message):
            await self._rmq_chan.basic_ack(message.delivery_tag)

        waiter = await MessageWaiter.new(self._rmq_chan,
                                         self._recv_queue, no_ack=False)

        self._rmq_chan.closing.add_done_callback(lambda _: waiter.interrupt())

        # TODO: even when connection between Server and Client
        # is not established, we can publish messages to queue
        # as long as we have channel open
        while not self._rmq_chan.is_closed:
            message = await waiter.wait()

            self._check_policy.message_received()

            if message.header.properties.message_type == _MessageType.MESSAGE:
                null_pos = message.body.find(NULL_CHAR)
                if null_pos == -1:
                    await ack_message(message)
                    await self._close(True)
                    raise ValueError('Got message without metadata separator')

                metadata = json_loadb(message.body[:null_pos])
                request_received_cb(metadata, message.body[null_pos+1:])
                await ack_message(message)

            elif message.header.properties.message_type == _MessageType.LISTENING:
                await ack_message(message)
                continue

            elif message.header.properties.message_type == _MessageType.CLOSE:
                await ack_message(message)
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
        if self._closing:
            return await self._closing

        if not self._ready.is_set():
            return

        self._ready.clear()

        if self._rmq_chan.is_closed or self._rmq_chan.connection.is_closed:
            return

        self._closing = asyncio.get_running_loop().create_future()
        try:
            if send_close_request:
                await _publish(
                    self._rmq_chan,
                    msg_type=_MessageType.CLOSE,
                    body=b'',
                    exchange=self._connector.exchange,
                    routing_key=self._send_queue,
                    properties_kwargs={
                        'timestamp': datetime.now()
                    }
                )
            await self._rmq_chan.close()
        except _closed_exceptions() + (asyncio.CancelledError,):
            return
        finally:
            await self._connector._check_connection_use()
            self._closing.set_result(None)


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
        self._closing = None
        self._connector = connector
        self._handshaker = handshaker
        self._client_connected_cb = client_connected_cb

    def is_serving(self):
        """Return True if the server is accepting connections."""
        return not self._chan.is_closed

    def close(self):
        async def close_channel():
            if not self._chan.is_closed:
                await self._chan.close()
            await self._connector._check_connection_use()
            if self._closing is not asyncio.current_task():
                raise RuntimeError('Server closing task changed')
            self._closing = None

        if not self._closing or self._closing.done():
            self._closing = asyncio.create_task(close_channel())

    async def wait_closed(self):
        if not self._closing or self._closing.done():
            raise RuntimeError('wait_closed() should be '
                               'called right after close() method')
        await self._closing

    async def _on_connect(self, message: 'aiormq.abc.DeliveredMessage'):
        if message.header.properties.message_type != _MessageType.CONNECT_REQUEST:
            await self._connector._ack_message(self._chan, message)
            raise ValueError('Unknown message in connect request queue')

        if message.header.properties.reply_to is None:
            await self._connector._ack_message(self._chan, message)
            raise ValueError('Reply queue not specified in connect request')

        hs_to_cli_queue = message.header.properties.reply_to
        connect_id = json_loadb(message.body)

        hs_to_srv_queue = await self._connector._queue_declare_and_bind(
            self._chan, exclusive=True
        )

        await _publish(
            self._chan,
            msg_type=_MessageType.CONNECT_RESPONSE,
            body=b'',
            exchange=self.exchange,
            routing_key=hs_to_cli_queue,
            properties_kwargs={
                'reply_to': hs_to_srv_queue
            }
        )

        await self._connector._ack_message(self._chan, message)

        resp_waiter = await MessageWaiter.new(self._chan, hs_to_srv_queue,
                                              no_ack=True, timeout=10)

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

        await _publish(
            self._chan,
            msg_type=_MessageType.HS_DONE,
            body=b'',
            exchange=self.exchange,
            routing_key=hs_to_cli_queue
        )

        # open new to prevent server's chan to be closed by connection
        run_chan = await self._connector._open_channel()
        policy = \
            ServerCheckPolicy(conn, self._connector.CONNECTION_CHECK_PERIOD)
        conn._set_run_data(run_chan, policy, to_srv_queue, to_cli_queue)

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
    Uses RabbitMQ for communication. Theoretically can be used
    with any AMQP-compatible server, but this wasn't tested.

    WARNING:
        For this connector to work properly, only one client
        with same address and connect_id must be connected at same time.

        For those, who connect, connect_id must be set explicitly
        and persist between service restarts.

        Failure to comply with these rules
        leads to unclosed queues growth and message loss.
    """

    _TYPE = 'RABBITMQ'

    CONNECTION_CHECK_PERIOD = 20
    """every X seconds server sends message to client,
    if client did not got any message in (this period * 1.5),
    it closes connection"""

    __slots__ = ('_url', '_address', '_connect_id',
                 '_exchange', '_exchange_declared')

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
        if not _HAVE_AIORMQ:
            raise ImportError('RmqConnector requires aiormq library. '
                              'Install communica with [rabbitmq] extra.')

        if connect_id is not None and len(address + connect_id) > 224:
            raise ValueError('Max address + connect_id length is 224 characters')

        self._url = URL(url)
        self._address = address
        self._exchange = exchange_name
        self._connect_id = connect_id
        self._exchange_declared = False

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

        await _publish(
            chan,
            msg_type=_MessageType.CONNECT_REQUEST,
            body=json_dumpb(self._connect_id),
            exchange=self.exchange,
            routing_key=self._get_connect_queue_name(),
            properties_kwargs={
                'reply_to': hs_to_cli_queue
            }
        )

        resp_waiter = \
            await MessageWaiter.new(chan, hs_to_cli_queue, no_ack=True)

        message = await resp_waiter.wait()
        if message.header.properties.message_type != _MessageType.CONNECT_RESPONSE:
            raise ValueError('Unknown message in connect response queue')

        if message.header.properties.reply_to is None:
            raise ValueError('Server queue not specified in connect response')
        hs_to_srv_queue = message.header.properties.reply_to

        resp_waiter.set_timeout_duration(10)

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
        policy = ClientCheckPolicy(conn, self.CONNECTION_CHECK_PERIOD * 1.5)
        conn._set_run_data(chan, policy, to_cli_queue, to_srv_queue)

        return conn

    async def client_connect(self, handshaker: Handshaker) -> BaseConnection:
        if self._connect_id is None:
            raise TypeError('Cannot connect to server. For those, who connect, '
                            'connect_id parameter must be set. '
                            'Check RmqConnector docs for details.')

        chan = await self._open_channel()
        try:
            if not self._exchange_declared:
                await chan.exchange_declare(
                    self._exchange,
                    exchange_type="direct",
                    durable=True,
                )
                self._exchange_declared = True

            return await self._client_connect(handshaker, chan)
        except Exception:
            if not chan.is_closed:
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
                # channel will be closed by server when trying to delete unknown queue
                await asyncio.wait([chan.closing])
            await chan.close()

        await self._check_connection_use()

    def dump_state(self) -> str:
        """Unsupported for this connector"""
        raise TypeError('This method unsupported cause user and password '
                        'are not encrypted in dump, which is insecure')

    @classmethod
    def from_state(cls, state: str) -> Self:
        """Unsupported for this connector"""
        raise TypeError('This method unsupported cause user and password '
                        'are not encrypted in dump, which is insecure')

    async def _ack_message(self, chan, message):
        await chan.basic_ack(message.delivery_tag)

    async def _open_channel(self):
        # there are two limits for simultaneously opened channels:
        # 65k defined by AMQP protocol (channel number range),
        # which cause 'while not (some channel closed)' loop in conn.channel()
        # and 'max-channels' limit set by AMQP server
        # (RabbitMQ 3.11 default is 2048), which cause connection termination
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
                _connections[self._url] = conn

        elif isinstance(conn, asyncio.Future):
            conn = await conn

        elif conn.is_closed:
            del(_connections[self._url])
            return await self._open_channel()

        return await conn.channel()

    async def _check_connection_use(self):
        conn = _connections.get(self._url)
        if conn is None or isinstance(conn, asyncio.Future):
            return

        for chan in conn.channels.values():
            if chan is not None and not chan.is_closed:
                return
        del(_connections[self._url])
        await conn.close()

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
