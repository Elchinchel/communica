import asyncio
from uuid import uuid4
from typing import Any, Iterable, TypedDict

from typing_extensions import Self

from communica.utils import ByteSeq, TaskSet, logger, fmt_task_name
from communica.exceptions import ReqError, RouteOverrideError
from communica.serializers import BaseSerializer, default_serializer
from communica.entities.base import HandlerType
from communica.connectors.base import (
    BaseConnector,
)
from communica.entities.simple import (
    RequestType,
    ReqRepClient,
    ReqRepServer,
    RequestHandler,
    ReqRepMessageFlow,
)


__all__ = (
    'RouteClient',
    'RouteServer'
)


class Metadata(TypedDict):
    id: str
    type: RequestType
    route: str


class RouteTable:
    """
    This object allows you to register routes in global variable
    and then use them when creating RouteServer or RouteClient.
    """

    __slots__ = ('route_map', 'name')

    route_map: 'dict[str, tuple[RequestHandler, BaseSerializer]]'

    def __init__(self, name: 'str | None' = None) -> None:
        self.name = name or 'unnamed'
        self.route_map = {}

    def __repr__(self):
        clsname = self.__class__.__name__
        return f'<{clsname} name={self.name}>'

    def add_route(
            self,
            route: str,
            handler: HandlerType,
            serializer: 'BaseSerializer | None'
    ):
        if route in self.route_map:
            raise RouteOverrideError(f'Route {route!r} already defined')

        if serializer is None:
            serializer = default_serializer
        if not isinstance(handler, RequestHandler):
            handler = RequestHandler(handler)
        self.route_map[route] = (handler, serializer)

    def handler(
            self,
            route: str,
            serializer: 'BaseSerializer | None' = None
    ):
        """
        Register handler for route.

        Args:
            route: Handler identifier.
            serializer: Instance of :obj:`communica.serializers.BaseSeralizer`.
                Defaults to JsonSerializer.
        """
        def decorator(handler: HandlerType):
            self.add_route(route, handler, serializer)
            if isinstance(handler, RequestHandler):
                return handler.endpoint
            return handler

        return decorator

    def __getitem__(self, __key: str) -> 'tuple[RequestHandler, BaseSerializer]':
        return self.route_map[__key]

    @classmethod
    def _join_route_tables(
            cls,
            anothers: 'Iterable[Self | None] | Self | None'
    ) -> Self:
        """
        Create new table with routes from anothers.
        """
        result_table = cls()

        def add_table(table: 'RouteTable | None'):
            if table is None:
                return
            for route, value in table.route_map.items():
                if route in result_table.route_map:
                    break
                result_table.route_map[route] = value
            else:
                return

            for antable in anothers:  # pyright: ignore
                if route not in antable.route_map:
                    continue
                raise RouteOverrideError(
                    f'Can\'t join {antable} with {table}: '
                    f'route {route!r} already defined.'
                )

        if anothers is None or isinstance(anothers, RouteTable):
            add_table(anothers)
            if anothers is not None:
                result_table.name = anothers.name
        else:
            for table in anothers:
                add_table(table)
            result_table.name = 'joined'
        return result_table


class RouteMessageFlow(ReqRepMessageFlow):
    __slots__ = ('routes', 'fallback_task_set', '_response_serializers',)

    routes: RouteTable
    fallback_task_set: TaskSet

    _response_serializers: 'dict[str, BaseSerializer]'

    def __init__(self, route_table: RouteTable):
        super().__init__()
        self.routes = route_table
        self.fallback_task_set = TaskSet()
        self._response_serializers = {}

    def dispatch(self, metadata: Metadata, raw_data: ByteSeq):
        if metadata['type'] < RequestType.RESP_OK:
            try:
                route_handle = self.routes[metadata['route']]
            except KeyError:
                route_handle = None

            self.task_set.create_task_with_exc_log(
                self.handle_request(route_handle, metadata, raw_data),
                name=fmt_task_name('route-request-handler')
            )
        else:
            try:
                serializer = self._response_serializers[metadata['route']]
            except KeyError:  # pragma: no cover
                # this is possible after program restart.
                # just log and skip, cause we don't have a routine,
                # waiting for this response
                # XXX: should it be logged?
                logger.warning('Got response with not requested '
                              f'route ({metadata["route"]})')
                return
            self._handle_response(serializer, metadata, raw_data)

    async def handle_request(
            self,
            route_handle: 'tuple[RequestHandler, BaseSerializer] | None',
            req_meta: Metadata,
            raw_data: ByteSeq
        ):
        if route_handle is not None:
            handler, serializer = route_handle
            resp_meta, resp_data = await self._handle_request(
                handler, serializer, req_meta, raw_data
            )
        else:
            serializer = default_serializer
            resp_meta = req_meta.copy()
            resp_meta['type'] = RequestType.RESP_ERR_REQUESTER
            resp_data = \
                ReqError(f'Route "{req_meta["route"]}" is not defined').to_dict()

        await self._send_response(req_meta, resp_meta, resp_data, serializer)

    async def request(
            self,
            route: str,
            serializer: 'BaseSerializer | None',
            data: Any
    ) -> bytes:
        # TODO: timeout
        request_id, fut = self._create_response_waiter()

        serializer = serializer or default_serializer
        if (saved_serializer := self._response_serializers.get(route)) is None:
            self._response_serializers[route] = serializer
        elif saved_serializer is not serializer:
            raise TypeError('You should always use the same serializer for '
                           f'the same route ({saved_serializer!r} '
                            'has already been used earlier)')

        await self._connection.send(
            Metadata(id=request_id, type=RequestType.REQ_REP, route=route),
            serializer.client_dump(data)
        )

        return await fut

    async def throw(
            self,
            route: str,
            serializer: 'BaseSerializer | None',
            data: Any
    ) -> None:
        await self._connection.send(
            Metadata(id='', type=RequestType.REQ_THROW, route=route),
            (serializer or default_serializer).client_dump(data)
        )


class RouteClient(ReqRepClient[RouteMessageFlow]):
    """
    Pair for RouteServer.

    Handler searched by exact match of route string.
    """

    __slots__ = ()

    @property
    def routes(self):
        return self._flow.routes

    def __init__(
            self,
            connector: BaseConnector,
            client_id: 'str | None' = None,
            route_table: 'Iterable[RouteTable] | RouteTable | None' = None
    ) -> None:
        super().__init__(connector)

        self._flow = RouteMessageFlow(
            RouteTable._join_route_tables(route_table)
        )
        self._run_task = None
        self._client_id = client_id or uuid4().hex

    def route_handler(self, *args, **kwargs):
        "Deprecated"
        return self.routes.handler(*args, **kwargs)

    async def request(
            self,
            route: str,
            data: Any,
            serializer: 'BaseSerializer | None' = None
    ) -> Any:
        """
        Send request, wait response.

        Args:
            route: Handler identifier.
            client_id: If omitted or None, random client will be chosen.
            serializer: Instance of :obj:`communica.serializers.BaseSeralizer`.
                Defaults to JsonSerializer.
        """
        if not self._connected_event.is_set():
            await self._connected_event.wait()
        return await self._flow.request(route, serializer, data)

    async def throw(
            self,
            route: str,
            data: Any,
            serializer: 'BaseSerializer | None' = None
    ) -> None:
        """
        Send request without waiting response.
        However, this method will block until data is sent.

        Args:
            route: Handler identifier.
            client_id: If omitted or None, random client will be chosen.
            serializer: Instance of :obj:`communica.serializers.BaseSeralizer`.
                Defaults to JsonSerializer.
        """
        if not self._connected_event.is_set():
            await self._connected_event.wait()
        return await self._flow.throw(route, serializer, data)


class RouteServer(ReqRepServer[RouteMessageFlow]):
    """
    Pair for RouteClient.

    Handler searched by exact match of route string.
    """

    __slots__ = ('_route_table',)

    _route_table: RouteTable
    _known_clients: 'dict[str, RouteMessageFlow | asyncio.Future[RouteMessageFlow]]'

    @property
    def routes(self):
        return self._route_table

    def __init__(
            self,
            connector: BaseConnector,
            route_table: 'RouteTable | None' = None
    ) -> None:
        self.connector = connector
        self._known_clients = {}
        self._client_connected = asyncio.Event()
        self._client_conn_runners = {}

        self._route_table = RouteTable._join_route_tables(route_table)

    def _create_new_flow(self) -> RouteMessageFlow:
        return RouteMessageFlow(self._route_table)

    def route_handler(self, *args, **kwargs):
        "Deprecated"
        return self.routes.handler(*args, **kwargs)

    async def request(
            self,
            route: str,
            data: Any,
            serializer: 'BaseSerializer | None' = None,
            client_id: 'str | None' = None
    ) -> Any:
        """
        Send request, wait response.

        Args:
            route: Handler identifier.
            client_id: If omitted or None, random connected client will be chosen.
            serializer: Instance of :obj:`communica.serializers.BaseSeralizer`.
                Defaults to JsonSerializer. Once serializer was used for
                request, no other serializer can be used for same route.
        """
        flow = await self._get_client_flow(client_id)
        return await flow.request(route, serializer, data)

    async def throw(
            self,
            route: str,
            data: Any,
            serializer: 'BaseSerializer | None' = None,
            client_id: 'str | None' = None
    ) -> None:
        """
        Send request without waiting response.

        Args:
            route: Handler identifier.
            client_id: If omitted or None, random connected client will be chosen.
            serializer: Instance of :obj:`communica.serializers.BaseSeralizer`.
                Defaults to JsonSerializer. Once serializer was used for
                request, no other serializer can be used for same route.
        """
        flow = await self._get_client_flow(client_id)
        return await flow.throw(route, serializer, data)
