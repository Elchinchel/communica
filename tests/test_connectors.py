import asyncio

import pytest
from utils_misc import create_task, create_string
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
    async def test_listen_checks(self, rmq_connector):
        connector = FastDyingRmqConnector(
            url=rmq_connector.url,
            address=rmq_connector.address,
            client_id=rmq_connector.client_id
        )

        server = SimpleServer(connector, lambda data: None)
        await server.init()

        async with SimpleClient(connector) as client:
            await asyncio.sleep(2)

            # no messages, but notifications sent by server,
            # so connection should be alive
            assert client._flow._connection.is_alive

            await server.close()
            await asyncio.sleep(2)

            # no messages and no notifications, connection should be closed
            assert not client._flow._connection.is_alive

            await asyncio.sleep(2)
            await server.init()
            # reconnect to restarted server
            while not client._flow._connection.is_alive:
                await asyncio.sleep(0.5)

            assert client._flow._connection.is_alive

        assert not client._flow._connection.is_alive

        # reconnect to running server
        async with SimpleClient(connector) as client:
            await asyncio.sleep(2)

            assert client._flow._connection.is_alive

        await server.close()

    @pytest.mark.asyncio
    async def test_rmq_connector(self, rmq_connector):
        await rmq_connector.cleanup()

        await run_sequential_send_with_simples(
            rmq_connector, default_serializer
        )
        await run_concurrent_send_with_simples(
            rmq_connector, default_serializer
        )

        await rmq_connector.cleanup()

    @pytest.mark.asyncio
    async def test_reconnect_after_connection_close(
            self,
            rmq_connector: RmqConnector
    ):
        messages = []

        server = SimpleServer(rmq_connector, lambda data: messages.append(data))
        client = SimpleClient(rmq_connector)

        async with server, client:
            await client.request(1)
            await client.request(2)
            assert messages == [1, 2]

            chan = await rmq_connector.acquire_channel()
            await chan.inner.connection.close()
            await client.request(3)
