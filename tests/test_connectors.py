import asyncio

import pytest
from utils_simple_entities_for_tests import (
    run_concurrent_send_with_simples,
    run_sequential_send_with_simples,
)

from tests.utils_misc import dummy_handshaker
from communica.clients import SimpleClient
from communica.servers import SimpleServer
from communica.connectors import RmqConnector, LocalConnector
from communica.serializers import default_serializer
from communica.connectors.stream.connector import TcpConnector, StreamConnection


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


@pytest.mark.asyncio
async def test_local_server_close_with_connections(local_connector):
    await server_close_test_runner(local_connector)


@pytest.mark.asyncio
async def test_tcp_server_close_with_connections(tcp_connector):
    await server_close_test_runner(tcp_connector)


async def server_close_test_runner(
    connector: 'LocalConnector | TcpConnector'
):
    server = await connector.server_start(
        dummy_handshaker,
        lambda connection: print('client connected'),
    )
    conn = await connector.client_connect(
        dummy_handshaker
    )
    conn_runner = asyncio.create_task(
        conn.run_until_fail(lambda metadata, raw_data: print('got message'))
    )
    await asyncio.sleep(0)

    server.close()
    wait_task = asyncio.create_task(server.wait_closed())
    _, pending1 = await asyncio.wait([wait_task], timeout=1)
    await conn.close()
    conn_runner.cancel()
    if not pending1:
        return
    done2, _ = await asyncio.wait([wait_task], timeout=1)
    if done2:
        pytest.fail('Server closing only after clients disconnect')

    for task in asyncio.all_tasks():
        print(task)
    pytest.fail('Server not closing at all')


@pytest.mark.asyncio
async def test_large_message_with_tcp(tcp_connector):
    """Test that large messages are automatically chunked and reassembled"""
    await large_message_test_runner(tcp_connector)


@pytest.mark.asyncio
async def test_large_message_with_local(local_connector):
    """Test that large messages are automatically chunked and reassembled"""
    await large_message_test_runner(local_connector)


async def large_message_test_runner(
    connector: 'LocalConnector | TcpConnector'
):
    # Temporarily reduce chunk size to test chunking with reasonable data sizes
    from communica.connectors.stream.connector import StreamConnection
    original_chunk_size = StreamConnection._MAX_CHUNK_SIZE
    StreamConnection._MAX_CHUNK_SIZE = 1024 * 1024  # 1MB for testing

    try:
        # Create a large message that exceeds the chunk size
        large_data = b'x' * (10 * 1024 * 1024)  # 10 MB

        received_messages = []

        def handler(data: bytes):
            received_messages.append(data)
            return b'response'

        server = SimpleServer(connector, handler, default_serializer)
        await server.init()

        try:
            async with SimpleClient(connector, default_serializer) as client:
                response = await client.request(large_data)
                assert response == b'response'
                assert len(received_messages) == 1
                assert received_messages[0] == large_data
        finally:
            await server.close()
    finally:
        # Restore original chunk size
        StreamConnection._MAX_CHUNK_SIZE = original_chunk_size


@pytest.mark.asyncio
async def test_multiple_large_messages_with_tcp(tcp_connector):
    """Test sending multiple large messages in sequence"""
    await multiple_large_messages_test_runner(tcp_connector)


@pytest.mark.asyncio
async def test_multiple_large_messages_with_local(local_connector):
    """Test sending multiple large messages in sequence"""
    await multiple_large_messages_test_runner(local_connector)


async def multiple_large_messages_test_runner(
    connector: 'LocalConnector | TcpConnector'
):
    # Temporarily reduce chunk size to test chunking with reasonable data sizes
    from communica.connectors.stream.connector import StreamConnection
    original_chunk_size = StreamConnection._MAX_CHUNK_SIZE
    StreamConnection._MAX_CHUNK_SIZE = 1024 * 1024  # 1MB for testing

    try:
        # Create multiple different large messages
        large_data_1 = b'a' * (5 * 1024 * 1024)  # 5 MB
        large_data_2 = b'b' * (7 * 1024 * 1024)  # 7 MB
        large_data_3 = b'c' * (3 * 1024 * 1024)  # 3 MB

        received_messages = []

        def handler(data: bytes):
            received_messages.append(data)
            return len(data)

        server = SimpleServer(connector, handler, default_serializer)
        await server.init()

        try:
            async with SimpleClient(connector, default_serializer) as client:
                resp1 = await client.request(large_data_1)
                resp2 = await client.request(large_data_2)
                resp3 = await client.request(large_data_3)

                assert resp1 == len(large_data_1)
                assert resp2 == len(large_data_2)
                assert resp3 == len(large_data_3)
                assert len(received_messages) == 3
                assert received_messages[0] == large_data_1
                assert received_messages[1] == large_data_2
                assert received_messages[2] == large_data_3
        finally:
            await server.close()
    finally:
        # Restore original chunk size
        StreamConnection._MAX_CHUNK_SIZE = original_chunk_size


@pytest.mark.asyncio
async def test_chunking_boundary_conditions(tcp_connector):
    """Test messages at and around the chunk size boundary"""
    from communica.connectors.stream.connector import StreamConnection
    original_chunk_size = StreamConnection._MAX_CHUNK_SIZE
    StreamConnection._MAX_CHUNK_SIZE = 1024 * 1024  # 1MB for testing

    try:
        received_messages = []

        def handler(data: bytes):
            received_messages.append(data)
            return b'ok'

        server = SimpleServer(tcp_connector, handler, default_serializer)
        await server.init()

        try:
            async with SimpleClient(tcp_connector, default_serializer) as client:
                # Test with a message just under chunk limit
                data_small = b'x' * 1000
                await client.request(data_small)

                # Test with a message that exceeds the chunk size
                data_medium = b'y' * (2 * 1024 * 1024)  # 2MB
                await client.request(data_medium)

                assert len(received_messages) == 2
                assert received_messages[0] == data_small
                assert received_messages[1] == data_medium
        finally:
            await server.close()
    finally:
        # Restore original chunk size
        StreamConnection._MAX_CHUNK_SIZE = original_chunk_size
