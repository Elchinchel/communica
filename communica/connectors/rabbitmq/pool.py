import asyncio
import logging
import warnings
import threading
from uuid import uuid4
from typing import Any, Callable
from collections import defaultdict
from dataclasses import dataclass

from typing_extensions import Self

from communica.utils import read_accessor


try:
    import aiormq
    from yarl import URL
except ModuleNotFoundError:
    pass


logger = logging.getLogger('communica.connectors.rabbitmq')


class ChannelExpired(RuntimeError):
    pass


class ChannelMaxReached(Exception):
    pass


class PooledChannel:
    __slots__ = ('url', '_pool', '_chan', '_expired',
                 '_consumer_tag', '_release_callbacks')

    number: read_accessor[int] = read_accessor('_chan.number')
    explicitly_released: read_accessor[bool] = read_accessor('_expired')

    def __init__(
            self,
            url: 'URL',
            pool: 'ChannelPool',
            chan: 'aiormq.abc.AbstractChannel',
    ) -> None:
        self.url = url
        self._pool = pool
        self._chan = chan
        self._expired = False
        self._consumer_tag = None
        self._release_callbacks = []

    def __repr__(self) -> str:
        return (f'<{self.__class__.__name__} expired={self.expired} '
                f'ctag={self._consumer_tag} inner={self._chan!r}>')

    def __del__(self):
        if not self._expired:
            warnings.warn(f'Unreleased {self!r}', ResourceWarning, source=self)

    @property
    def expired(self) -> bool:
        return (self._expired or self._chan.is_closed)

    @property
    def inner(self) -> 'aiormq.abc.AbstractChannel':
        if self._expired:
            raise ChannelExpired(f'{self!r} is expired')
        if self._chan.is_closed:
            raise ChannelExpired(f'Underlying {self._chan!r} is closed')
        return self._chan

    def on_release(self, cb: Callable[[Self], None]):
        self._release_callbacks.append(cb)

    def publish(
            self,
            msg_type: str,
            body: bytes,
            exchange: str,
            routing_key: str,
            properties_kwargs: 'dict[str, Any]' = {}
    ):
        return self.inner.basic_publish(
            body,
            exchange=exchange,
            routing_key=routing_key,
            properties=aiormq.spec.Basic.Properties(  # pyright: ignore[reportUnboundVariable]
                message_type=msg_type,
                **properties_kwargs
            )
        )

    async def ack_message(self, message):
        await self._chan.basic_ack(message.delivery_tag)

    async def consume(self, queue: str, cb: 'aiormq.abc.ConsumerCallback', **kwargs):
        """
        Start queue consumer.
        One PooledChannel can have only one active consumer.
        """
        if self._consumer_tag:
            raise RuntimeError(f'{self!r} already consuming')
        self._consumer_tag = uuid4().hex
        return await self.inner.basic_consume(
            queue,
            cb,
            consumer_tag=self._consumer_tag,
            **kwargs
        )

    async def cancel_consume(self):
        if not self._consumer_tag:
            raise RuntimeError(f'{self!r} not consuming')
        if self._chan.is_closed:
            return
        coro = self._chan.basic_cancel(consumer_tag=self._consumer_tag)
        self._consumer_tag = None
        await coro

    async def queue_declare_and_bind(
            self,
            exchange: str,
            name: str = '',
            **declare_kwargs
    ):
        declare_ok = await self.inner.queue_declare(queue=name, **declare_kwargs)
        assert declare_ok.queue is not None, 'Incompatible AMQP server'
        queue_name = declare_ok.queue

        await self.inner.queue_bind(
            queue_name,
            exchange,
            routing_key=queue_name
        )
        return queue_name

    async def release(self):
        """
        Set .expired = true, restrict .inner access
        and return channel to pool. Can be called multiple times.
        """
        if self._expired:
            return
        self._expired = True

        for cb in self._release_callbacks:
            cb(self)

        if self._consumer_tag:
            try:
                await self.cancel_consume()
            except aiormq.exceptions.ChannelInvalidStateError:
                pass

        await self._pool._release_chan(self.url, self._chan)


class ChannelRoute:
    __slots__ = ('chan', '_exchange', '_routing_key')

    routing_key: read_accessor[str] = read_accessor('_routing_key')

    def __init__(
            self,
            chan: PooledChannel,
            exchange_name: str,
            routing_key: str
    ) -> None:
        self.chan = chan
        self._exchange = exchange_name
        self._routing_key = routing_key

    def publish(
            self,
            msg_type: str,
            body: bytes,
            properties_kwargs: 'dict[str, Any]' = {}
    ):
        return self.chan.publish(
            msg_type,
            body,
            exchange=self._exchange,
            routing_key=self._routing_key,
            properties_kwargs=properties_kwargs
        )


class ChannelPool:
    __slots__ = ('_loop_contexts', '_thread_lock')

    _thread_lock: threading.Lock
    _loop_contexts: 'dict[asyncio.AbstractEventLoop, _LoopContext]'

    @dataclass(frozen=True)
    class _LoopContext:
        locks: 'defaultdict[URL, asyncio.Lock]'
        connections: 'dict[URL, aiormq.abc.AbstractConnection]'
        free_channels: 'set[aiormq.abc.AbstractChannel]'
        release_waiters: 'set[asyncio.Future]'

    def __init__(self) -> None:
        self._thread_lock = threading.Lock()
        self._loop_contexts = {}

    def _check_loops(self):
        msg = '%r at 0x%02x is closed, but unclosed AMQP connection (%r) still '\
            'associated with %r. This may lead to weird bugs, cause broker '\
            'probably still think that connection exists. Before closing '\
            'asyncio loop you should call .close(url) method on pool.\n%s'

        for loop, ctx in self._loop_contexts.items():
            if not loop.is_closed() or not ctx.connections:
                continue

            for conn in ctx.connections.values():
                if conn.is_closed:
                    continue

            from os import environ  # noqa
            from communica.connectors.rabbitmq import DEFAULT_POOL  # noqa

            note = ''
            if environ.get('PYTEST_CURRENT_TEST'):
                note += 'If you run this code within pytest, then either '\
                    'create new connector with new ChannelPool for each test, '\
                    'or call channel_pool.purge() after each test '\
                    '(e.g. in RmqConnector fixture finalization).\n'
            else:
                note += '(actually you should use only ONE event loop per ONE '\
                    'process, but whatever)\n'
            if self is DEFAULT_POOL:
                note += 'This is default pool. You can access it on '\
                    'communica.connectors.rabbitmq.DEFAULT_POOL. Also consider '\
                    'specifying your own pool in RmqConnector arguments.\n'

            logger.warning(msg, loop, id(loop), conn, self, note.strip())

    def _get_current_context(self) -> _LoopContext:
        loop = asyncio.get_running_loop()

        context = self._loop_contexts.get(loop)
        if context is not None:
            return context

        with self._thread_lock:
            self._check_loops()

            if loop not in self._loop_contexts:
                self._loop_contexts[loop] = self._LoopContext(
                    locks=defaultdict(lambda: asyncio.Lock()),
                    connections=dict(),
                    free_channels=set(),
                    release_waiters=set()
                )
        return self._loop_contexts[loop]

    async def acquire_chan(self, url: 'URL') -> PooledChannel:
        context = self._get_current_context()

        async with context.locks[url]:
            conn = context.connections.get(url)
            if conn is None or conn.is_closed:
                conn = await aiormq.connect(url)
                context.connections[url] = conn

            if len(conn.channels) >= conn.connection_tune.channel_max:
                logger.warning(
                    'Negotiated channel limit for %r reached. Channel '
                    'will not be acquired until other channel released.',
                    url.with_password(None)
                )
                fut = asyncio.get_running_loop().create_future()
                context.release_waiters.add(fut)
                await fut

            if context.free_channels:
                rmq_chan = context.free_channels.pop()
                assert not rmq_chan.is_closed, 'Channel in pool was unexpectedly closed.'
            else:
                rmq_chan = await conn.channel()

            logger.debug('%r acquired from %r (%r)',
                         rmq_chan, self, url.with_password(None))
            return PooledChannel(url, self, rmq_chan)

    async def _release_chan(self, url: 'URL', rmq_chan: 'aiormq.abc.AbstractChannel'):
        logger.debug('%r released from %r (%r)',
                     rmq_chan, self, url.with_password(None))
        context = self._get_current_context()

        async with context.locks[url]:
            if not rmq_chan.is_closed:
                context.free_channels.add(rmq_chan)

            while context.release_waiters:
                waiter = context.release_waiters.pop()
                if not waiter.done():
                    waiter.set_result(None)
                    break

    async def close(self, url: 'URL') -> bool:
        """
        Close connection for given url.
        If there is no connection in pool, returns False.

        This does not prevent opening of the new connections.
        """
        context = self._get_current_context()
        async with context.locks[url]:
            while context.release_waiters:
                waiter = context.release_waiters.pop()
                if not waiter.done():
                    waiter.set_exception(ChannelMaxReached())

            conn = context.connections.pop(url, None)
            if conn is None:
                return False
            if conn.is_opened:
                await conn.close()
            return True

    async def purge(self):
        """
        Close all connections associated with current event loop.
        """
        context = self._get_current_context()
        connections = list(context.connections.values())

        for conn in connections:
            await self.close(conn.url)
