import asyncio

import pytest
from utils_misc import create_task
from utils_simple_entities_for_tests import (
    run_concurrent_send_with_simples,
    run_sequential_send_with_simples,
)

from communica.clients import SimpleClient
from communica.servers import SimpleServer
from communica.connectors import RmqConnector
from communica.serializers import default_serializer


class FastDyingRmqConnector(RmqConnector):
    CONNECTION_CHECK_PERIOD = 1


class TestRmqConnector:
    @pytest.mark.asyncio
    async def test_listen_checks(self):
        connector = FastDyingRmqConnector(
            url='amqp://guest:guest@localhost:5672/',
            address='test',
            client_id='test_client'
        )

        server = SimpleServer(connector, lambda data: None)
        create_task(server.run())

        async with SimpleClient(connector) as client:
            await asyncio.sleep(2)

            # no messages, but notifications sent by server,
            # so connection should be alive
            assert client._flow._connection.is_alive

            server.stop()
            await asyncio.sleep(2)

            # no messages and no notifications, connection should be closed
            assert not client._flow._connection.is_alive

            await asyncio.sleep(2)
            create_task(server.run())
            # reconnect to restarted server
            while not client._flow._connection.is_alive:
                await asyncio.sleep(0.5)

            assert client._flow._connection.is_alive

        assert not client._flow._connection.is_alive

        # reconnect to running server
        async with SimpleClient(connector) as client:
            await asyncio.sleep(2)

            assert client._flow._connection.is_alive

        await server.wait_stop()

    @pytest.mark.asyncio
    async def test_connector(self):
        connector = RmqConnector(
            url='amqp://guest:guest@localhost:5672/',
            address='test',
            client_id='test_client'
        )

        await connector.cleanup()

        await run_sequential_send_with_simples(
            connector, default_serializer
        )
        await run_concurrent_send_with_simples(
            connector, default_serializer
        )

        await connector.cleanup()
