import random
import asyncio

from abc import abstractmethod
from enum import Enum
from uuid import uuid4, uuid1
from typing import TypedDict, Any, cast
from inspect import iscoroutinefunction
from traceback import format_exc
from dataclasses import dataclass

from communica.serializers import BaseSerializer, default_serializer
from communica.exceptions import (
    ReqError, RespError, UnknownError, SerializerError
)
from communica.connectors.base import (
    BaseConnection, BaseConnector,
    HandshakeOk, HandshakeGen
)
from communica.utils import (
    BackoffDelayer, HasLoopMixin, TaskSet, iscallable, logger
)

from communica.pairs.base import (
    BaseClient, BaseServer,
    SyncHandlerType, AsyncHandlerType
)


__all__ = (
    'SimpleClient',
    'SimpleServer'
)


class RequestType(int, Enum):
    REQ_REP = 1
    REQ_THROW = 2

    RESP_OK = 11

    RESP_ERR_UNKNOWN = 30

    RESP_ERR_REQUESTER = 41
    RESP_ERR_DATA_LOAD = 42

    RESP_ERR_RESPONDER = 51


@dataclass
class ServerHandshakeOk(HandshakeOk):
    client_id: str


class Metadata(TypedDict):
    type: RequestType
    id: str


class RequestHandler:
    __slots__ = ('_repr', 'is_async', 'endpoint', 'running_tasks')

    is_async: bool
    endpoint: 'SyncHandlerType | AsyncHandlerType'
    running_tasks: TaskSet

    def __repr__(self) -> str:
        try:
            return self._repr
        except AttributeError:
            name = getattr(self.endpoint, '__qualname__',
                           getattr(self.endpoint, '__name__', 'UNKNOWN'))
            htype = 'async' if self.is_async else 'sync'
            self._repr = f'<RequestHandler {htype} endpoint={name}>'
            return self._repr

    def __init__(self, endpoint: 'SyncHandlerType | AsyncHandlerType') -> None:
        if not iscallable(endpoint):
            raise TypeError('Request handler must be function, '
                            'coroutine function or method')
        self.is_async = iscoroutinefunction(endpoint)
        self.endpoint = endpoint
        self.running_tasks = TaskSet()


class ReqRepMessageFlow(HasLoopMixin):
    __slots__ = (
        '_connection', '_response_waiters'
    )

    _response_waiters: 'dict[str, asyncio.Future]'

    @property
    def connection(self):
        return self._connection

    def __init__(self):
        self._response_waiters = {}

    def update_connection(self, connection: BaseConnection):
        try:
            self._connection.update(connection)
        except AttributeError:
            self._connection = connection
        return self._connection

    def _create_response_waiter(self):
        request_id = uuid1().hex

        fut = self._get_loop().create_future()
        self._response_waiters[request_id] = fut
        fut.add_done_callback(lambda _: self._response_waiters.pop(request_id, None))

        return request_id, fut

    def _handle_response(
            self,
            serializer: BaseSerializer,
            metadata: Metadata,
            raw_data: bytes
    ):
        fut = self._response_waiters.pop(metadata['id'], None)
        if fut is None or fut.done():
            logger.warning('Drop response for unknown or expired request')
            return

        req_type = metadata['type']

        if req_type == RequestType.RESP_OK:
            try:
                fut.set_result(serializer.client_load(raw_data))
            except Exception as e:
                # if serializer can't load response, considering this
                # requester's error, cause responder can't do
                # anything about it after sending response
                fut.set_exception(e)
            return

        data = default_serializer.client_load(raw_data)
        if req_type == RequestType.RESP_ERR_REQUESTER:
            fut.set_exception(ReqError.from_dict(data))
        elif req_type == RequestType.RESP_ERR_RESPONDER:
            fut.set_exception(RespError.from_dict(data))
        elif req_type == RequestType.RESP_ERR_UNKNOWN:
            fut.set_exception(UnknownError.from_dict(data))
        elif req_type == RequestType.RESP_ERR_DATA_LOAD:
            fut.set_exception(SerializerError.from_dict(data))
        else:
            logger.critical(f"{metadata = }, wtf")
            fut.set_exception(
                UnknownError(f'Got unknown response type: {metadata["type"]}')
            )

    async def _handle_request(
            self,
            handler: RequestHandler,
            serializer: BaseSerializer,
            metadata: Metadata,
            raw_data: Any
    ) -> 'tuple[dict, Any]':
        resp_meta = metadata.copy()

        try:
            data = serializer.load(raw_data)
        except Exception as e:
            resp_meta['type'] = RequestType.RESP_ERR_DATA_LOAD
            resp_data = SerializerError(repr(e)).to_dict()
            return resp_meta, resp_data  # type: ignore

        try:
            if handler.is_async:
                resp_data = await handler.endpoint(data)
            else:
                resp_data = handler.endpoint(data)
            resp_meta['type'] = RequestType.RESP_OK

        except ReqError as e:
            resp_data = e.to_dict()
            resp_meta['type'] = RequestType.RESP_ERR_REQUESTER

        except RespError as e:
            resp_data = e.to_dict()
            resp_meta['type'] = RequestType.RESP_ERR_RESPONDER

        except Exception as e:
            logger.error('Unexpected exception in %r', handler, exc_info=True)
            resp_data = UnknownError(repr(e)).to_dict()
            resp_meta['type'] = RequestType.RESP_ERR_UNKNOWN

        return resp_meta, resp_data  # type: ignore

    async def _send_response(
            self,
            req_meta,
            resp_meta,
            resp_data,
            serializer: BaseSerializer
    ):
        if req_meta['type'] == RequestType.REQ_REP:
            if resp_meta['type'] > RequestType.RESP_OK:
                serializer = default_serializer

            try:
                raw_data = serializer.dump(resp_data)
            except Exception:
                logger.error('Can\'t serialize response:\n%s', format_exc())

                raw_data = default_serializer.dump(
                    RespError('Response serialize error').to_dict()
                )
                resp_meta['type'] = RequestType.RESP_ERR_RESPONDER

            await self._connection.send(resp_meta, raw_data)

        elif resp_meta['type'] > RequestType.RESP_ERR_UNKNOWN:
            logger.warning('Error occured while handling request, but '
                           'requester don\'t know about this:\n' + resp_data['msg'])


class SimpleMessageFlow(ReqRepMessageFlow):
    __slots__ = ('handler', 'serializer')

    def __init__(
            self,
            handler: 'SyncHandlerType | AsyncHandlerType',
            serializer: BaseSerializer
    ):
        super().__init__()
        self.handler = RequestHandler(handler)
        self.serializer = serializer

    def dispatch(self, metadata: Metadata, raw_data: bytes):
        if metadata['type'] < RequestType.RESP_OK:
            self.handler.running_tasks.create_task_with_exc_log(
                self.handle_request(metadata, raw_data)
            )
        else:
            self._handle_response(self.serializer, metadata, raw_data)

    async def handle_request(self, req_meta: Metadata, raw_data: bytes):
        resp_meta, resp_data = await self._handle_request(
            self.handler, self.serializer, req_meta, raw_data
        )
        await self._send_response(
            req_meta, resp_meta, resp_data, self.serializer
        )

    async def request(self, data: Any) -> bytes:
        request_id, fut = self._create_response_waiter()

        await self._connection.send(
            Metadata(id=request_id, type=RequestType.REQ_REP),
            self.serializer.client_dump(data)
        )

        return await fut

    async def throw(self, data: Any) -> None:
        await self._connection.send(
            Metadata(id='', type=RequestType.REQ_THROW),
            self.serializer.client_dump(data)
        )


class ReqRepClient(BaseClient):
    __slots__ = ('_connected_event', '_run_task', '_client_id', '_flow')

    _run_task: 'asyncio.Task | None'

    @property
    def client_id(self) -> str:
        return self._client_id

    @property
    def is_running(self):
        return not (self._run_task is None or self._run_task.done())

    @property
    def connected_event(self) -> asyncio.Event:
        try:
            return self._connected_event
        except AttributeError:
            self._connected_event = asyncio.Event()
            return self._connected_event

    async def init(self, timeout: 'int | None' = None):
        if not self._run_task or self._run_task.done():
            self._run_task = \
                self._get_loop().create_task(self._connection_keeper())
            # self._run_task.add_done_callback(self._on_conn_fail)

        try:
            await asyncio.wait_for(self.connected_event.wait(), timeout)
        except asyncio.TimeoutError:
            self._run_task.cancel()
            raise
        return self

    async def close(self) -> None:
        if self._run_task is not None:
            await self._flow._connection.close()
            self._run_task.cancel()

    async def _connection_keeper(self):
        delayer = BackoffDelayer(0.1, 5, 2, 0.5)
        while True:
            try:
                new_conn = await self.connector.client_connect(self._handshaker)
            except Exception as e:
                # TODO: log successful reconnect
                logger.warning('%r: Connect failed: %r', self.connector, e)
                await delayer.wait()
                continue

            connection = self._flow.update_connection(new_conn)
            self.connected_event.set()
            delayer.reset()

            try:
                await connection.run_until_fail(self._flow.dispatch)
            except Exception as e:
                logger.error('Unhandled exception '
                             'in connection runner: %r', e)
            self.connected_event.clear()
            await asyncio.sleep(1)

    async def _handshaker(self, connection: BaseConnection) -> HandshakeGen:
        client_hello = {
            'client_id': self._client_id
        }

        server_hello = (yield client_hello)

        yield HandshakeOk()


class SimpleClient(ReqRepClient):
    """
    Pair to SimpleServer.

    Has only one optional request handler.
    """

    __slots__ = ('serializer',)

    serializer: BaseSerializer
    _flow: SimpleMessageFlow

    def __init__(
            self,
            connector: BaseConnector,
            handler: 'SyncHandlerType | AsyncHandlerType | None' = None,
            serializer: 'BaseSerializer | None' = None,
            client_id: 'str | None' = None,
    ) -> None:
        super().__init__(connector)

        if serializer is None:
            serializer = default_serializer
        if handler is None:
            handler = self._not_defined_handler

        self._flow = SimpleMessageFlow(handler, serializer)

        self._client_id = client_id or uuid4().hex
        self._run_task = None

    def _not_defined_handler(self, data: Any):
        raise RespError('Client side not defined handler for server requests')

    async def request(self, data: Any) -> Any:
        """Send request, wait response."""
        return await self._flow.request(data)

    async def throw(self, data: Any) -> None:
        """Send request without waiting response."""
        return await self._flow.throw(data)


class ReqRepServer(BaseServer):
    __slots__ = ('_server', '_known_clients', '_client_conn_runners')

    _server: asyncio.AbstractServer
    _known_clients: 'dict[str, ReqRepMessageFlow | asyncio.Future[ReqRepMessageFlow]]'
    _client_conn_runners: 'dict[str, asyncio.Task]'

    async def init(self):
        if not hasattr(self, '_server') or not self._server.is_serving():
            self._server = await self.connector.server_start(
                self._handshaker, self._on_client_connect)
        return self

    async def close(self) -> None:
        if not hasattr(self, '_server'):
            return

        self._server.close()
        await self._server.wait_closed()

        for client_id, flow in self._known_clients.items():
            if isinstance(flow, ReqRepMessageFlow):
                self._cancel_handler_tasks(flow)
                await flow._connection.close()
            if (conn_task := self._client_conn_runners.get(client_id)):
                conn_task.cancel()

    async def _handshaker(self, connection: BaseConnection) -> HandshakeGen:
        server_hello = {
            'hello': 'hello'
        }

        client_hello = (yield server_hello)

        client_id = client_hello['client_id']
        yield ServerHandshakeOk(client_id=client_id)

    # TODO: clear _known_clients
    async def _get_client_flow(self, client_id: 'str | None') -> Any:
        if client_id is None:
            while not (connected := self._get_connected_clients()):
                await asyncio.sleep(1)
            return random.choice(connected)

        flow = self._known_clients.get(client_id)
        if not isinstance(flow, ReqRepMessageFlow):
            if flow is None:
                self._known_clients[client_id] = self._get_loop().create_future()
            flow = await self._known_clients[client_id]  # type: ignore
        return flow

    def _get_connected_clients(self):
        return [
            flow for flow in self._known_clients.values()
                if not isinstance(flow, asyncio.Future) and flow.connection.is_alive
        ]

    def _on_conn_fail(self, task: asyncio.Task):
        if task.cancelled():
            return
        if (exc := task.exception()):
            logger.warning(f'Client read failed: {exc!r}')

    @abstractmethod
    def _cancel_handler_tasks(self, flow):
        raise NotImplementedError

    @abstractmethod
    def _on_client_connect(self, connection: BaseConnection):
        raise NotImplementedError


class SimpleServer(ReqRepServer):
    """
    Pair to SimpleClient.

    Has only one request handler.
    """

    __slots__ = ('_handler', '_serializer')

    _handler: 'SyncHandlerType | AsyncHandlerType'
    _serializer: BaseSerializer

    def __init__(
            self,
            connector: BaseConnector,
            handler: 'SyncHandlerType | AsyncHandlerType',
            serializer: 'BaseSerializer | None' = None,
    ) -> None:
        if serializer is None:
            serializer = default_serializer

        self._handler = handler
        self.connector = connector
        self._serializer = serializer
        self._known_clients = {}
        self._client_conn_runners = {}

    def _cancel_handler_tasks(self, flow: SimpleMessageFlow):
        flow.handler.running_tasks.cancel()

    def _on_client_connect(self, connection: BaseConnection):
        loop = self._get_loop()
        handshake_result = cast(ServerHandshakeOk, connection.get_handshake_result())
        client_id = handshake_result.client_id

        flow = self._known_clients.get(client_id)
        if not isinstance(flow, SimpleMessageFlow):
            new_flow = SimpleMessageFlow(self._handler, self._serializer)
            if isinstance(flow, asyncio.Future):
                flow.set_result(new_flow)
            self._known_clients[client_id] = new_flow
            flow = new_flow
        flow.update_connection(connection)

        task = loop.create_task(connection.run_until_fail(flow.dispatch))
        task.add_done_callback(self._on_conn_fail)
        self._client_conn_runners[client_id] = task

    async def request(self, data: Any, client_id: 'str | None' = None) -> Any:
        """
        Send request, wait for response.

        Args:
            client_id: If omitted or None, random connected client will be chosen.
        """
        flow = await self._get_client_flow(client_id)
        return await flow.request(data)

    async def throw(self, data: Any, client_id: 'str | None' = None) -> None:
        """
        Send request without waiting for response.

        Args:
            client_id: If omitted or None, random connected client will be chosen.
        """
        flow = await self._get_client_flow(client_id)
        return await flow.throw(data)
