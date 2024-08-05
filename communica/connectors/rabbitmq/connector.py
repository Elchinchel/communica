import asyncio
import logging
import weakref
from abc import ABC, abstractmethod
from enum import Enum
from uuid import uuid4
from typing import Any, Literal
from collections import deque

from typing_extensions import Self

from communica.utils import (
    NULL_CHAR,
    HasLoopMixin,
    BackoffDelayer,
    json_dumpb,
    json_loadb,
    fmt_task_name,
    read_accessor,
    exc_log_callback,
)
from communica.exceptions import FeatureNotAvailable
from communica.connectors.base import (
    Handshaker,
    BaseConnector,
    HandshakeFail,
    BaseConnection,
    ClientConnectedCB,
    RequestReceivedCB,
)
from communica.connectors.rabbitmq.pool import (
    ChannelPool,
    ChannelRoute,
    PooledChannel,
    ChannelExpired,
)


try:
    import aiormq
    _HAVE_AIORMQ = True
except ModuleNotFoundError:
    _HAVE_AIORMQ = False
else:
    from yarl import URL


DEFAULT_POOL = ChannelPool()
DEFAULT_EXCHANGE = 'communica'
HANDSHAKE_TIMEOUT = 30

logger = logging.getLogger('communica.connectors.rabbitmq')


class _MessageType(str, Enum):
    CONNECT_REQUEST = 'communica_client_connect'
    CONNECT_RESPONSE = 'communica_client_connect_ok'

    HS_NEXT = 'communica_handshake_next'
    HS_FAIL = 'communica_handshake_fail'
    HS_DONE = 'communica_handshake_done'

    CLOSE = 'close'
    MESSAGE = 'message'
    LISTENING = 'listening'


class MessageWaiter(HasLoopMixin):
    __slots__ = ('_waiter', '_messages', '_timeout')

    @classmethod
    async def new(
            cls,
            chan: PooledChannel,
            queue: str,
            *,
            no_ack: bool,
            timeout: 'int | None' = None
    ):
        inst = cls()
        inst._waiter = None
        inst._timeout = timeout
        inst._messages = deque()

        await chan.consume(queue, inst._callback, no_ack=no_ack)

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
        if self._waiter and not self._waiter.done():
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
    def cancel(self):
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
        self.period = period
        self._waiter = self._get_loop().create_future()

        self._send_task = self._get_loop().create_task(
            self._sender(),
            name=fmt_task_name('rmq-server-listen-notifier')
        )
        self._send_task.add_done_callback(exc_log_callback)
        self._conn = weakref.ref(conn, self._conn_died)

        self._set_handle()

    def replace_conn(self, conn: 'RmqConnection'):
        conn._check_policy.cancel()
        self._conn = weakref.ref(conn, self._conn_died)
        return self

    def cancel(self):
        if self._handle is not None:
            self._handle.cancel()
        self._conn_died(None)
        self._send_task.cancel()

    def _conn_died(self, _):
        if not self._waiter.done():
            self._waiter.set_result(False)

    def _trigger(self):
        if self._waiter.done():
            return
        if self._send_task.done():
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
        self.period = period
        self._set_handle()

        self._conn = weakref.ref(conn)
        self._close_task = None

    def replace_conn(self, conn: 'RmqConnection'):
        conn._check_policy.cancel()
        self._conn = weakref.ref(conn)
        return self

    def cancel(self):
        if self._handle is not None:
            self._handle.cancel()

    def _trigger(self):
        if (conn := self._conn()) is None:
            return

        time_diff = self._get_loop().time() - self._last_message
        if time_diff < self.period:
            self._set_handle(self.period - time_diff)
            return

        logger.info('Closing %r cause no messages received in %f seconds',
                    conn, self.period)
        self._close_task = self._get_loop().create_task(
            conn.close(),
            name=fmt_task_name('rmq-client-check-closer')
        )

    def message_received(self):
        if self._close_task is None:
            self._last_message = self._get_loop().time()


class RmqConnection(BaseConnection):
    __slots__ = ('_connector', '_chan', '_ready', '_closing',
                 '_recv_queue', '_send_route', '_check_policy',
                 '_connect_id', '__weakref__')

    _chan: PooledChannel
    _ready: asyncio.Event
    _closing: 'asyncio.Future | None'
    _connector: 'RmqConnector'

    @property
    def is_alive(self):
        return self._ready.is_set()

    @classmethod
    async def _do_handshake_and_create_connection(
            cls,
            connector: 'RmqConnector',
            handshaker: Handshaker,
            resp_waiter: MessageWaiter,
            send_route: ChannelRoute
    ) -> Self:
        inst = cls()

        async def send_message(data: bytes):
            await send_route.publish(_MessageType.HS_NEXT, body=data)

        async def recv_message():
            try:
                message = await resp_waiter.wait()
            except asyncio.TimeoutError:
                raise HandshakeFail('Response timeout exceeded '
                                    'when connection is established') from None

            if message.header.properties.message_type == _MessageType.HS_FAIL:
                raise HandshakeFail.loadb(message.body)
            elif message.header.properties.message_type != _MessageType.HS_NEXT:
                raise ValueError('Unknown message in handshake queue')
            return message.body

        try:
            await inst._run_handshaker(handshaker, send_message, recv_message)
        except HandshakeFail as fail:
            await send_route.publish(_MessageType.HS_FAIL, fail.dumpb())
            raise

        inst._ready = asyncio.Event()
        inst._ready.set()
        inst._closing = None
        inst._connector = connector

        return inst

    def __repr__(self) -> str:
        cls_name = self.__class__.__name__
        if self._ready:
            state = f'on channel {self._chan.number}'
        else:
            state = 'not ready'
        return f'<{cls_name} {state}>'

    def _set_run_data(
            self,
            chan: PooledChannel,
            policy: ConnectionCheckPolicy,
            recv_queue: str,
            send_queue: str,
            connect_id: bytes
    ):
        self._chan = chan
        self._connect_id = connect_id
        self._check_policy = policy
        self._recv_queue = recv_queue
        self._send_route = ChannelRoute(
            chan,
            self._connector.exchange_name,
            send_queue
        )

    def update(self, connection: Self) -> None:
        if self._connector._client_id != connection._connector._client_id:
            raise ValueError('Got connection with different client_id')
        if connection._chan.expired:
            raise ValueError('Got connection with expired channel')

        self._ready.set()
        self._closing = None
        self._set_run_data(
            connection._chan,
            connection._check_policy.replace_conn(self),
            connection._recv_queue,
            connection._send_route.routing_key,
            connection._connect_id,
        )

    async def send(self, metadata: Any, raw_data: bytes):
        body = json_dumpb(metadata) + NULL_CHAR + raw_data
        try:
            await self._send(_MessageType.MESSAGE, body)
        except ChannelExpired:
            self._ready.clear()
            await self._chan.release()
            await self._send(_MessageType.MESSAGE, body)  # retry once after channel opened
        except aiormq.exceptions.AMQPError:
            self._ready.clear()
            raise
        self._check_policy.message_sent()

    async def _send(self, msg_type: _MessageType, body: bytes):
        await self._ready.wait()
        logger.debug('%r sent %r to %r',
                     self, msg_type, self._send_route.routing_key)

        # cancelling of aiormq RPC task will send Channel.Close to server,
        # which greatly increases chances of connection exception,
        # which in turn will break everything on same URL in same pool
        await asyncio.shield(
            self._send_route.publish(msg_type, body=body)
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
            await self._chan.release()

    async def _run_until_fail(self, request_received_cb):
        waiter = await MessageWaiter.new(
            self._chan,
            self._recv_queue,
            no_ack=False
        )

        self._chan.on_release(lambda _: waiter.interrupt())
        self._chan.inner.closing.add_done_callback(lambda _: waiter.interrupt())

        while self._ready.is_set():
            message = await waiter.wait()
            logger.debug(
                '%r received %r from %r',
                self, message.header.properties.message_type, self._recv_queue
            )

            self._check_policy.message_received()

            if message.header.properties.message_type == _MessageType.MESSAGE:
                null_pos = message.body.find(NULL_CHAR)
                if null_pos == -1:
                    await self._chan.ack_message(message)
                    await self._close(True)
                    raise ValueError('Got message without metadata separator')

                metadata = json_loadb(message.body[:null_pos])
                request_received_cb(metadata, message.body[null_pos+1:])
                await self._chan.ack_message(message)

            elif message.header.properties.message_type == _MessageType.LISTENING:
                await self._chan.ack_message(message)
                continue

            elif message.header.properties.message_type == _MessageType.CLOSE:
                await self._chan.ack_message(message)

                if message.body == self._connect_id:
                    await self._close(False)
                    return

            else:
                await self._chan.ack_message(message)
                await self._close(True)
                message_type = message.header.properties.message_type
                raise ValueError('Unknown message in message queue, '
                                f'got type {message_type!r}')

    async def close(self) -> None:
        logger.debug('Closing %r', self)
        await self._close(True)

    async def _close(self, send_close_request: bool):
        if self._closing:
            return await self._closing

        if not self._ready.is_set():
            return

        self._ready.clear()
        self._check_policy.cancel()

        if self._chan.expired:
            return

        self._closing = asyncio.get_running_loop().create_future()
        try:
            if send_close_request:
                await self._send_route.publish(
                    _MessageType.CLOSE,
                    body=self._connect_id
                )
            await self._chan.release()
        except (
            ChannelExpired,
            asyncio.CancelledError,
            aiormq.exceptions.ChannelClosed,
            aiormq.exceptions.ConnectionClosed
        ):
            return
        finally:
            self._closing.set_result(None)


class RmqServer(asyncio.AbstractServer):
    _closing: 'asyncio.Future | Literal[True] | None'
    _restart_task: 'asyncio.Task | None'

    @property
    def exchange(self):
        return self._connector._exchange

    def __init__(
            self,
            connector: 'RmqConnector',
            handshaker: Handshaker,
            client_connected_cb: ClientConnectedCB
    ) -> None:
        self._closing = None
        self._restart_task = None
        self._start_lock = asyncio.Lock()

        self._connector = connector
        self._handshaker = handshaker
        self._client_connected_cb = client_connected_cb

    def is_serving(self):
        """Returns True if the server accepts connections."""
        try:
            return not self._chan.expired
        except AttributeError:
            return False

    def close(self):
        if self._restart_task:
            self._restart_task.cancel()
            self._restart_task = None

        if not hasattr(self, '_chan'):
            self._closing = True
        if self._closing is True:
            return

        async def close_channel():
            if not self._chan.expired:
                await self._chan.release()
            if self._closing is not asyncio.current_task():
                raise RuntimeError('Server closing task changed')
            self._closing = None

        if not self._closing or self._closing.done():
            self._closing = asyncio.create_task(
                close_channel(),
                name=fmt_task_name('rmq-server-closer')
            )

    async def wait_closed(self):
        if self._closing is True:
            self._closing = None
            return

        if not self._closing or self._closing.done():
            raise RuntimeError('wait_closed() should be '
                               'called right after close() method')
        await self._closing

    async def _on_connect(self, message: 'aiormq.abc.DeliveredMessage'):
        if message.header.properties.message_type != _MessageType.CONNECT_REQUEST:
            await self._chan.ack_message(message)
            logger.error('Unknown message type in connect request queue (%r in %r)',
                         message.header.properties.message_type, self._queue_name)
            return

        if message.header.properties.reply_to is None:
            await self._chan.ack_message(message)
            logger.error('Reply queue is not specified in the connect request')
            return

        client_id = json_loadb(message.body)
        hs_to_cli_route = ChannelRoute(
            self._chan,
            self.exchange,
            message.header.properties.reply_to
        )

        logger.debug('got connect request from client_id=%s', client_id)

        hs_to_srv_queue = await self._chan.queue_declare_and_bind(
            self.exchange,
            exclusive=True
        )

        await hs_to_cli_route.publish(
            _MessageType.CONNECT_RESPONSE,
            body=b'',
            properties_kwargs={
                'reply_to': hs_to_srv_queue
            }
        )

        await self._chan.ack_message(message)

        run_chan = await self._connector.acquire_channel()
        resp_waiter = await MessageWaiter.new(
            run_chan,
            hs_to_srv_queue,
            no_ack=True,
            timeout=HANDSHAKE_TIMEOUT
        )
        hs_completed = False
        try:
            conn = await RmqConnection._do_handshake_and_create_connection(
                self._connector,
                self._handshaker,
                resp_waiter,
                hs_to_cli_route
            )
            hs_completed = True
        except HandshakeFail:
            # if fail happens on server's side, client will get and log it
            # so log on server only when debugging
            logger.debug('Handshake with client_id=%s failed',
                         client_id, exc_info=True)
            await run_chan.release()
            return
        finally:
            if not hs_completed:
                await run_chan.release()
            if not self._chan.expired:
                await self._chan.inner.queue_delete(hs_to_srv_queue)
        await run_chan.cancel_consume()

        to_cli_queue, to_srv_queue = \
            self._connector._get_transport_queue_names(client_id)

        await run_chan.queue_declare_and_bind(
            self.exchange,
            name=to_cli_queue,
            durable=True
        )
        await run_chan.queue_declare_and_bind(
            self.exchange,
            name=to_srv_queue,
            durable=True
        )

        connect_id = uuid4().bytes
        await hs_to_cli_route.publish(_MessageType.HS_DONE, connect_id)
        policy = ServerCheckPolicy(
            conn,
            self._connector.CONNECTION_CHECK_PERIOD
        )
        conn._set_run_data(
            run_chan,
            policy,
            to_srv_queue,
            to_cli_queue,
            connect_id
        )

        logger.info('Handshake with client_id=%s succedeed, connect_id=%s',
                     client_id, connect_id)
        self._client_connected_cb(conn)

    async def _restart_serving(self):
        logger.warning(
            'aiormq channel under %r was unexpectedly closed! '
            'Will repeatedly try to restart server on a new '
            'channel after 10 seconds',
            self
        )
        await self._chan.release()

        delayer = BackoffDelayer(10, 90, 2, 0.5)
        while True:
            await delayer.wait()
            try:
                await self.start_serving()
                break
            except Exception:
                logger.exception('%r restart failed', self)

        logger.info('%r restarted successfully', self)

    def _make_aiormq_channel_close_cb(self):
        last_alive_channel = self._chan

        def callback(_):
            if self._chan is not last_alive_channel:
                return  # server was restarted after pool closed
            if self._chan.explicitly_released:
                return  # server was closed explicitly
            if self._restart_task and not self._restart_task.done():
                return  # this should never happen but just in case
            self._restart_task = asyncio.create_task(
                self._restart_serving(),
                name=fmt_task_name('rmq-server-restart')
            )

        return callback

    async def start_serving(self):
        if self.is_serving():
            return

        async with self._start_lock:
            self._chan = await self._connector.acquire_channel()
            self._queue_name = self._connector._get_connect_queue_name()

            await self._chan.inner.basic_qos(prefetch_count=1)
            await self._chan.queue_declare_and_bind(
                self.exchange,
                self._queue_name,
                durable=True
            )
            await self._chan.consume(self._queue_name, self._on_connect)

        self._chan.inner.closing.add_done_callback(
            self._make_aiormq_channel_close_cb()
        )

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
        with same address and client_id must be connected at same time.

        For those, who connect, client_id must be set explicitly
        and persist between service restarts.

        Failure to comply with these rules
        leads to unclosed queues growth and message loss.
    """

    _TYPE = 'RABBITMQ'

    CONNECTION_CHECK_PERIOD = 20
    """every X seconds server sends message to client,
    if client did not got any message in (this period * 1.5),
    it closes connection"""

    __slots__ = ('_url', '_address', '_client_id',
                 '_exchange', '_exchange_declared', '_pool')

    url: read_accessor[str] = read_accessor('_url')
    address: read_accessor[str] = read_accessor('_address')
    client_id: read_accessor[str] = read_accessor('_client_id')
    exchange_name: read_accessor[str] = read_accessor('_exchange')

    def __init__(
            self,
            url: str,
            address: str,
            client_id: 'str | None' = None,
            exchange_name: 'str | None' = None,
            channel_pool: 'ChannelPool | None' = None
    ) -> None:
        """
        Max summary length of address and client_id is 224 characters

        Args:
            url (str): used to create connection with RabbitMQ server.
              Format specified at https://www.rabbitmq.com/uri-spec.html.
            address (str): unique identifier for client-server pair.
              If more than one server with same address bound to same exchange,
              behaviour undefined.
            client_id (str): unique identifier of connecting process.
              Must be set for clients.
        """
        if not _HAVE_AIORMQ:
            raise FeatureNotAvailable(
                'RmqConnector requires aiormq and yarl libraries. '
                'Install communica with [rabbitmq] extra.'
            )

        if client_id is not None and len(address + client_id) > 224:
            raise ValueError('Max address + client_id length is 224 characters')

        self._url = URL(url)
        self._client_id = client_id
        self._address = address
        self._exchange_declared = False

        self._pool = channel_pool or DEFAULT_POOL
        self._exchange = exchange_name or DEFAULT_EXCHANGE

    def _get_connect_queue_name(self):
        return self._fmt_queue_name('connect', 'server')

    def _get_transport_queue_names(self, client_id: str):
        return (
            self._fmt_queue_name('toClient', client_id),
            self._fmt_queue_name('toServer', client_id)
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
        chan = await self._pool.acquire_chan(self._url)

        await chan.inner.exchange_declare(
            self._exchange,
            exchange_type="direct",
            durable=True,
        )
        await chan.release()

        server = RmqServer(self, handshaker, client_connected_cb)
        await server.start_serving()
        return server

    async def _client_connect(
            self,
            handshaker: Handshaker,
            chan: PooledChannel
    ):
        await chan.inner.basic_qos(prefetch_count=1)

        hs_to_cli_queue = await chan.queue_declare_and_bind(
            self.exchange_name,
            exclusive=True
        )

        await chan.publish(
            msg_type=_MessageType.CONNECT_REQUEST,
            body=json_dumpb(self._client_id),
            exchange=self.exchange_name,
            routing_key=self._get_connect_queue_name(),
            properties_kwargs={
                'reply_to': hs_to_cli_queue
            }
        )

        resp_waiter = await MessageWaiter.new(
            chan,
            hs_to_cli_queue,
            no_ack=True
            # there is no timeout needed for first response,
            # server will reply as soon as it ready
        )

        message = await resp_waiter.wait()
        if message.header.properties.message_type != _MessageType.CONNECT_RESPONSE:
            raise ValueError('Unknown message in connect response queue')
        if message.header.properties.reply_to is None:
            raise ValueError('Server queue not specified in connect response')
        hs_to_srv_queue = message.header.properties.reply_to

        resp_waiter.set_timeout_duration(HANDSHAKE_TIMEOUT)

        try:
            conn = await RmqConnection._do_handshake_and_create_connection(
                self,
                handshaker,
                resp_waiter,
                ChannelRoute(chan, self.exchange_name, hs_to_srv_queue)
            )
        except Exception:
            if not chan.expired:
                await chan.inner.queue_delete(hs_to_cli_queue)
            raise

        message = await resp_waiter.wait()
        await chan.inner.queue_delete(hs_to_cli_queue)
        await chan.cancel_consume()

        if message.header.properties.message_type != _MessageType.HS_DONE:
            message_type = message.header.properties.message_type
            raise ValueError(f'Got message with "{message_type}" type '
                              'instead of handshake confirmation')
        connect_id = message.body

        to_cli_queue, to_srv_queue = \
            self._get_transport_queue_names(self._client_id)  # pyright: ignore[reportArgumentType]
        policy = ClientCheckPolicy(
            conn,
            self.CONNECTION_CHECK_PERIOD * 1.5
        )
        conn._set_run_data(
            chan,
            policy,
            to_cli_queue,
            to_srv_queue,
            connect_id
        )

        return conn

    async def client_connect(self, handshaker: Handshaker) -> BaseConnection:
        if self._client_id is None:
            raise TypeError('Cannot connect to server. For those, who connect, '
                            'client_id parameter must be set. '
                            'Check RmqConnector docs for details.')

        chan = await self.acquire_channel()
        try:
            if not self._exchange_declared:
                await chan.inner.exchange_declare(
                    self._exchange,
                    exchange_type="direct",
                    durable=True,
                )
                self._exchange_declared = True

            return await self._client_connect(handshaker, chan)
        except Exception:
            if not chan.expired:
                await chan.release()
            raise

    async def cleanup(self):
        """
        Drop all pending messages and
        delete queues with connector's client_id

        If client_id is None, server connect queue is deleted.
        """
        if self._client_id is None:
            queue_names = [self._get_connect_queue_name()]
        else:
            queue_names = self._get_transport_queue_names(self._client_id)

        for queue in queue_names:
            chan = await self.acquire_channel()
            try:
                await chan.inner.queue_purge(queue)
                await chan.inner.queue_delete(queue)
            except aiormq.exceptions.ChannelNotFoundEntity:
                # channel will be closed by server if queue is not found
                if not chan.expired:
                    await asyncio.wait([chan.inner.closing])
            await chan.release()

    def acquire_channel(self):
        return self._pool.acquire_chan(self._url)

    @classmethod
    def _load_pickled(cls, kwargs: dict):
        return cls(**kwargs)

    def __reduce__(self):
        return (
            RmqConnector._load_pickled,
            (
                {
                    'url': self._url,
                    'address': self._address,
                    'client_id': self._client_id,
                    'exchange_name': self._exchange,
                    'channel_pool': None,
                },
            )
        )
