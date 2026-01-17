import asyncio
from contextlib import contextmanager

import pytest
from utils_simple_entities_for_tests import (
    run_concurrent_send_with_simples,
    run_sequential_send_with_simples,
)

from tests.utils_misc import wait_second, dummy_handshaker
from communica.clients import SimpleClient
from communica.servers import SimpleServer
from communica.connectors import RmqConnector, LocalConnector
from communica.serializers import default_serializer
from communica.connectors.base import BaseConnector, BaseConnection
from communica.connectors.dummy import DummyConnector, _MemoryPipe
from communica.connectors.stream.connector import TcpConnector, StreamConnection


@contextmanager
def temporary_max_chunk_size(size: int):
    """Context manager to temporarily set StreamConnection._MAX_CHUNK_SIZE"""
    original_size = StreamConnection._MAX_CHUNK_SIZE
    StreamConnection._MAX_CHUNK_SIZE = size
    try:
        yield
    finally:
        StreamConnection._MAX_CHUNK_SIZE = original_size


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
            assert messages == [1, 2, 3]


class TestDummyConnector:
    @pytest.mark.asyncio
    async def test_memory_pipe(self):
        pipe = _MemoryPipe('')
        pipe.send(b'1')
        pipe.send(b'2')
        pipe.send(b'3')
        messages = [
            await wait_second(pipe.arecv()),
            await wait_second(pipe.arecv()),
            await wait_second(pipe.arecv()),
        ]
        assert messages == [b'1', b'2', b'3']

        task = asyncio.create_task(pipe.arecv())
        await asyncio.sleep(0)
        assert not task.done()

        pipe.send(b'4')
        await asyncio.sleep(0)
        # _chain_future adds callback on wrapped future completion,
        # so one iteration is not enough
        await asyncio.sleep(0)
        assert task.result() == b'4'

    @pytest.mark.asyncio
    async def test_dummy_connector(self):
        connector = DummyConnector()
        await run_concurrent_send_with_simples(connector, default_serializer)


@pytest.mark.asyncio
async def test_basic_communication(connector: BaseConnector):
    messages = []

    def on_connect(connection: BaseConnection):
        def on_message(metadata, raw_data):
            messages.append(raw_data)
            assert metadata == 'cts' and raw_data == b'client_to_server'

        asyncio.create_task(connection.send('stc', b'server_to_client'))
        asyncio.create_task(connection.run_until_fail(on_message))

    server = await connector.server_start(dummy_handshaker, on_connect)

    conn = await connector.client_connect(dummy_handshaker)

    async def check_is_alive():
        await asyncio.sleep(0)
        assert conn.is_alive, '`is_alive` property of new connection is falsey'

    def on_client_message(metadata, raw_data):
        async def respond():
            await conn.send('cts', b'client_to_server')
            for _ in range(100):  # bruh
                if len(messages) > 1:
                    break
                await asyncio.sleep(0.01)
            await conn.close()

        assert metadata == 'stc' and raw_data == b'server_to_client'
        messages.append(raw_data)
        asyncio.create_task(respond())

    asyncio.create_task(check_is_alive())
    await conn.run_until_fail(on_client_message)

    assert not conn.is_alive, '`is_alive` property of closed connection is truthy'
    assert messages == [b'server_to_client', b'client_to_server']

    server.close()
    await server.wait_closed()


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
async def test_stream_connection_chunking(local_connector):
    with temporary_max_chunk_size(1024 * 1024):  # 1MB for testing
        large_data = []
        # 1 MB * 4 (0x?? for each i) + json overhead (around 8MB total)
        for i in range(1024 * 1024):
            large_data.append(hex(i % 256))

        received_messages = []

        def handler(data):
            received_messages.append(data)
            return data

        async with SimpleServer(local_connector, handler):
            async with SimpleClient(local_connector) as client:
                response = await client.request(large_data)
                assert response == large_data
                assert len(received_messages) == 1
                assert received_messages[0] == large_data


@pytest.mark.asyncio
async def test_stream_connection_chunking2(local_connector):
    with temporary_max_chunk_size(1024 * 1024):  # 1 MB
        large_data_1 = 'a' * (5 * 1024 * 1024)  # 5 MB
        large_data_2 = 'b' * (7 * 1024 * 1024)  # 7 MB
        large_data_3 = 'c' * (3 * 1024 * 1024)  # 3 MB

        received_messages = []

        def handler(data: bytes):
            received_messages.append(data)
            return len(data)

        async with SimpleServer(local_connector, handler):
            async with SimpleClient(local_connector) as client:
                resp1, resp2, resp3 = await asyncio.gather(
                    client.request(large_data_1),
                    client.request(large_data_2),
                    client.request(large_data_3)
                )

                assert resp1 == len(large_data_1)
                assert resp2 == len(large_data_2)
                assert resp3 == len(large_data_3)
                assert len(received_messages) == 3
                assert received_messages[0] == large_data_1
                assert received_messages[1] == large_data_2
                assert received_messages[2] == large_data_3
