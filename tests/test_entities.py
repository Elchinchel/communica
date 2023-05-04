"""Test SimpleClient and SimpleServer"""

import asyncio

import pytest

from communica.pairs.base import BaseEntity
from communica.exceptions import ReqError, RespError, UnknownError
from communica.pairs import (
    SimpleClient, SimpleServer, RouteClient, RouteServer
)

from utils_misc import wait_tasks
from utils_simple_entities_for_tests import(
    MessageExistenceChecker,
    client_to_server_messages, server_to_client_messages,
    run_concurrent_send_with_simples,
    run_sequential_send_with_simples
)


class TestBaseEntities:
    class Entity(BaseEntity):
        async def init(self):
            return self

        async def close(self):
            pass

    async def test_double_run_call(self, connector):
        entity = self.Entity(connector)

        runner = asyncio.create_task(entity.run())

        await asyncio.sleep(0)

        with pytest.raises(RuntimeError):
            await entity.run()

        runner.cancel()


class TestSimpleEntities:
    async def test_error_handling(self, connector, serializer):
        async def handler(data):
            if data == 1:
                raise ValueError('hello')
            elif data == 2:
                raise RespError('im sorry', 500)
            else:
                raise ReqError('dude no such data', 404)

        client = SimpleClient(connector)
        server = SimpleServer(connector, handler)

        async with server, client:
            with pytest.raises(UnknownError, match=r'ValueError.+hello'):
                await asyncio.wait_for(client.request(1), timeout=1)

            try:
                await asyncio.wait_for(client.request(2), timeout=1)
            except RespError as e:
                assert e.code == 500
                assert e.message == 'im sorry'

            try:
                await asyncio.wait_for(client.request(3), timeout=1)
            except ReqError as e:
                assert e.code == 404
                assert e.message == 'dude no such data'

            await client.throw(1)

    async def test_sequential_send(self, connector, serializer):
        await run_sequential_send_with_simples(connector, serializer)

    async def test_concurrent_send(self, connector, serializer):
        await run_concurrent_send_with_simples(connector, serializer)


class TestRouteEntities:
    async def test_error_handling(self, connector, serializer):
        client = RouteClient(connector)
        server = RouteServer(connector)

        @server.route_handler('1')
        async def handler_1(data):
            raise ValueError('hello')

        @server.route_handler('2')
        async def handler_2(data):
            raise RespError('im sorry', 500)

        @server.route_handler('3')
        async def handler_3(data):
            raise ReqError('dude no such data', 404)

        async with server, client:
            with pytest.raises(UnknownError, match=r'ValueError.+hello'):
                await asyncio.wait_for(client.request('1', None), timeout=1)

            try:
                await asyncio.wait_for(client.request('2', None), timeout=1)
            except RespError as e:
                assert e.code == 500
                assert e.message == 'im sorry'

            try:
                await asyncio.wait_for(client.request('3', None), timeout=1)
            except ReqError as e:
                assert e.code == 404
                assert e.message == 'dude no such data'

    @pytest.mark.asyncio
    async def test_routing(self, connector, serializer):
        async def run(entity: 'RouteClient | RouteServer', messages, done_event):
            for message in messages:
                resp = await entity.request(str(message), message)
                assert resp == message, 'Response data must be equal to sent'
            await done_event.wait()

        def add_handler(entity: 'RouteClient | RouteServer', route_id: str, checker):
            @entity.route_handler(route_id)
            async def handler(data):
                a = await checker.handler(data)
                return a

        cli_checker = MessageExistenceChecker(server_to_client_messages)
        client = RouteClient(connector)
        for message in server_to_client_messages:
            add_handler(client, str(message), cli_checker)

        srv_checker = MessageExistenceChecker(client_to_server_messages)
        server = RouteServer(connector)
        for message in client_to_server_messages:
            add_handler(server, str(message), srv_checker)

        async with server, client:
            await wait_tasks(
                run(server, server_to_client_messages, srv_checker.done),
                run(client, client_to_server_messages, cli_checker.done),
                timeout=5
            )

