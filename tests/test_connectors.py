import pytest

from communica.clients import SimpleClient
from communica.servers import SimpleServer
from communica.connectors import RmqConnector
from communica.serializers import default_serializer


from utils import wait_tasks  # type: ignore
from simple_entities_for_tests import(
    CLIENT_ID,
    MessageExistenceChecker, MessageOrderChecker,
    client_to_server_messages, server_to_client_messages,
    run_concurrent_send_with_simples, run_sequential_send_with_simples,
)


class TestRmqConnector:
    @pytest.mark.asyncio
    async def test_connector(self):
        connector = RmqConnector(
            url='amqp://guest:guest@localhost:5672/',
            address='test',
            connect_id='test_client'
        )

        await connector.cleanup()

        await run_sequential_send_with_simples(
            connector, default_serializer
        )
        await run_concurrent_send_with_simples(
            connector, default_serializer
        )

        await connector.cleanup()
