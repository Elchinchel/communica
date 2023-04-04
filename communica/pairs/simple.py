import json
import asyncio

from abc import abstractmethod
from enum import Enum
from uuid import uuid4
from typing import TypedDict, Any, cast
from inspect import iscoroutinefunction
from traceback import format_exc
from dataclasses import dataclass

from communica.exceptions import RequesterError, ResponderError
from communica.serializers import BaseSerializer, default_serializer
from communica.connectors.base import (
    BaseConnection, BaseConnector,
    HandshakeOk, HandshakeGen
)
from communica.utils import (
    cycle_range, json_dumpb, iscallable,
    HasLoopMixin,
    INT32RANGE,
    logger
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

    RESP_ERR_REQUESTER = 31
    RESP_ERR_RESPONDER = 41
    RESP_ERR_UNKNOWN = 42


@dataclass
class ServerHandshakeOk(HandshakeOk):
    client_id: str


class Metadata(TypedDict):
    type: RequestType
    id: int


class RequestHandler:
    __slots__ = ('is_async', 'endpoint', 'running_tasks')

    is_async: bool
    endpoint: 'SyncHandlerType | AsyncHandlerType'
    running_tasks: 'set[asyncio.Task]'

    def __init__(self, endpoint: 'SyncHandlerType | AsyncHandlerType') -> None:
        if not iscallable(endpoint):
            raise TypeError('Request handler must be function, '
                            'coroutine function or method')
        self.is_async = iscoroutinefunction(endpoint)
        self.endpoint = endpoint
        self.running_tasks = set()


class ReqRepMessageFlow(HasLoopMixin):
    __slots__ = (
        '_connection', '_id_counter', '_response_waiters'
    )

    _response_waiters: 'dict[int, asyncio.Future]'

    def __init__(self):
        self._id_counter = cycle_range(*INT32RANGE)
        self._response_waiters = {}

    def update_connection(self, connection: BaseConnection):
        try:
            self._connection.update(connection)
        except AttributeError:
            self._connection = connection
        return self._connection

    def create_response_waiter(self):
        request_id = next(self._id_counter)

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
            fut.set_result(serializer.load(raw_data))
            return

        data = default_serializer.load(raw_data)
        if req_type == RequestType.RESP_ERR_REQUESTER:
            fut.set_exception(RequesterError(data['msg']))
        elif req_type == RequestType.RESP_ERR_RESPONDER:
            fut.set_exception(ResponderError(data['msg']))
        elif req_type == RequestType.RESP_ERR_UNKNOWN:
            fut.set_exception(ResponderError(data['msg']))  # XXX (че по исключению)
        else:
            logger.critical(f"{metadata['type'] = }, чзх")
            fut.set_exception(Exception('каво нахуй'))

    async def _handle_request(
            self,
            handler: RequestHandler,
            metadata: Metadata,
            data: Any
    ) -> 'tuple[dict, Any]':
        resp_meta = metadata.copy()

        try:
            if handler.is_async:
                resp_data = await handler.endpoint(data)
            else:
                resp_data = handler.endpoint(data)
            resp_meta['type'] = RequestType.RESP_OK
        except RequesterError as e:
            resp_data = {'msg': e.message}
            resp_meta['type'] = RequestType.RESP_ERR_REQUESTER
        except ResponderError as e:
            resp_data = {'msg': e.message}
            resp_meta['type'] = RequestType.RESP_ERR_RESPONDER
        except Exception as e:
            resp_data = {'msg': f'fuck you, there is {e!r}'}
            resp_meta['type'] = RequestType.RESP_ERR_UNKNOWN

        return resp_meta, resp_data  # type: ignore


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
            data = self.serializer.load(raw_data)
            runner_task = self._get_loop().create_task(
                self.handle_request(metadata, data))
            runner_task.add_done_callback(self.handler.running_tasks.discard)
            self.handler.running_tasks.add(runner_task)
        else:
            self._handle_response(self.serializer, metadata, raw_data)

    async def handle_request(self, req_meta: Metadata, data: Any):
        resp_meta, resp_data = await self._handle_request(
            self.handler, req_meta, data
        )

        req_type = req_meta['type']
        resp_type = resp_meta['type']

        if resp_type == RequestType.RESP_OK:
            if req_type == RequestType.REQ_REP:
                await self._connection.send(
                    resp_meta, self.serializer.dump(resp_data))

        elif req_type == RequestType.REQ_THROW:
            logger.warning('Error occured while handling request, but '
                           'requester don\'t know about this:\n' + format_exc())

    async def request(self, data: Any) -> bytes:
        request_id, fut = self.create_response_waiter()

        await self._connection.send(
            Metadata(id=request_id, type=RequestType.REQ_REP),
            self.serializer.dump(data)
        )

        return await fut

    async def throw(self, data: Any) -> None:
        await self._connection.send(
            Metadata(id=0, type=RequestType.REQ_THROW),
            self.serializer.dump(data)
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
            self._run_task.cancel()
            await self._flow._connection.close()
        self.stop()

    # def _on_conn_fail(self, task: asyncio.Task):
    #     if task.cancelled():
    #         return
    #     if (exc := task.exception()):
    #         logger.error('Unhandled exception in connection runner: {exc!r}')

    async def _connection_keeper(self):
        while True:
            try:
                new_conn = await self.connector.client_connect(self._handshaker)
            except Exception as e:
                logger.warning('Connect failed: %s', repr(e))
                await asyncio.sleep(1)
                continue
            else:
                connection = self._flow.update_connection(new_conn)
                self.connected_event.set()

            try:
                await connection.run_until_fail(self._flow.dispatch)
            except Exception as e:
                logger.error('Unhandled exception '
                            f'in connection runner: {e!r}')
            self.connected_event.clear()
            await asyncio.sleep(1)

    async def _handshaker(self, connection: BaseConnection) -> HandshakeGen:
        client_hello = {
            'client_id': self._client_id
        }

        server_hello = json.loads((yield json_dumpb(client_hello)))

        yield HandshakeOk()


class SimpleClient(ReqRepClient):
    """
    Работает в паре с SimpleServer.

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
        raise ResponderError('Client side not defined handler for server requests')

    async def request(self, data: Any) -> bytes:
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
        self._server = await self.connector.server_start(
            self._handshaker, self._on_client_connect)
        return self

    async def close(self) -> None:
        for client_id, flow in self._known_clients.items():
            if (conn_task := self._client_conn_runners.get(client_id)):
                conn_task.cancel()
            if isinstance(flow, ReqRepMessageFlow):
                self._cancel_handler_tasks(flow)
                await flow._connection.close()
        self._server.close()
        await self._server.wait_closed()

    async def _handshaker(self, connection: BaseConnection) -> HandshakeGen:
        server_hello = {
            'hello': 'hello'
        }

        client_hello = json.loads((yield json_dumpb(server_hello)))

        client_id = client_hello['client_id']
        yield ServerHandshakeOk(client_id=client_id)

    async def _get_client_flow(self, client_id: 'str | None') -> Any:
        if client_id is None:
            while not self._known_clients:
                await asyncio.sleep(1)

            import random

            flow = random.choice(list(self._known_clients.values()))
            if isinstance(flow, ReqRepMessageFlow):
                return flow
            return await flow

        flow = self._known_clients.get(client_id)
        if not isinstance(flow, ReqRepMessageFlow):
            if flow is None:
                self._known_clients[client_id] = self._get_loop().create_future()
            flow = await self._known_clients[client_id]  # type: ignore
        return flow

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
    Работает в паре с SimpleClient.

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
        for task in flow.handler.running_tasks:
            if task is not asyncio.tasks.current_task():
                task.cancel()

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

    async def request(self, data: Any, client_id: 'str | None' = None) -> bytes:
        """
        Send request, wait response.

        Args:
            client_id: If omitted or None, random client will be chosen
        """
        flow = await self._get_client_flow(client_id)
        return await flow.request(data)

    async def throw(self, data: Any, client_id: 'str | None' = None) -> None:
        """
        Send request without waiting response.

        Args:
            client_id: If omitted or None, random client will be chosen
        """
        flow = await self._get_client_flow(client_id)
        return await flow.throw(data)
