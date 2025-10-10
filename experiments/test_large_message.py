"""
Experiment script to validate large message chunking functionality.

This script demonstrates that the StreamConnector can now handle messages
larger than _MAX_CHUNK_SIZE by automatically splitting them into chunks
and reassembling them on the receiving end.

Run with: python -m experiments.test_large_message
"""
import asyncio

from communica import SimpleServer, SimpleClient, TcpConnector
from communica.serializers import JsonSerializer


async def main():
    # Temporarily reduce chunk size to test chunking with smaller messages
    from communica.connectors.stream.connector import StreamConnection
    original_chunk_size = StreamConnection._MAX_CHUNK_SIZE
    StreamConnection._MAX_CHUNK_SIZE = 1024 * 1024  # 1MB for testing

    connector = TcpConnector('localhost', 16162)
    serializer = JsonSerializer()

    print(f'Testing large message chunking (chunk size: {StreamConnection._MAX_CHUNK_SIZE:,} bytes)...\n')

    # Test 1: Send a 10MB message (will be split into ~10 chunks)
    print('Test 1: Sending 10MB message')
    large_data = b'x' * (10 * 1024 * 1024)
    print(f'  Data size: {len(large_data):,} bytes')
    print(f'  Expected chunks: ~{len(large_data) // StreamConnection._MAX_CHUNK_SIZE}')

    received_data = []

    def handler(data):
        print(f'  Server received: {len(data):,} bytes')
        received_data.append(data)
        return f'Received {len(data)} bytes'

    server = SimpleServer(connector, handler, serializer)
    await server.init()

    try:
        async with SimpleClient(connector, serializer) as client:
            response = await client.request(large_data)
            print(f'  Client received response: {response}')

        assert len(received_data) == 1
        assert received_data[0] == large_data
        print('  ✓ Test 1 passed: Data transmitted correctly\n')

        # Test 2: Send multiple large messages
        print('Test 2: Sending multiple large messages (5MB, 7MB, 3MB)')
        received_data.clear()

        data_1 = b'a' * (5 * 1024 * 1024)
        data_2 = b'b' * (7 * 1024 * 1024)
        data_3 = b'c' * (3 * 1024 * 1024)

        async with SimpleClient(connector, serializer) as client:
            resp1 = await client.request(data_1)
            print(f'  Response 1: {resp1}')
            resp2 = await client.request(data_2)
            print(f'  Response 2: {resp2}')
            resp3 = await client.request(data_3)
            print(f'  Response 3: {resp3}')

        assert len(received_data) == 3
        assert received_data[0] == data_1
        assert received_data[1] == data_2
        assert received_data[2] == data_3
        print('  ✓ Test 2 passed: Multiple messages transmitted correctly\n')

        # Test 3: Send a very small message to verify normal path still works
        print('Test 3: Sending small message (1KB)')
        received_data.clear()

        small_data = b'small' * 200  # 1KB

        async with SimpleClient(connector, serializer) as client:
            response = await client.request(small_data)
            print(f'  Response: {response}')

        assert len(received_data) == 1
        assert received_data[0] == small_data
        print('  ✓ Test 3 passed: Small messages still work correctly\n')

        print('All tests passed! ✓')
    finally:
        await server.close()
        # Restore original chunk size
        StreamConnection._MAX_CHUNK_SIZE = original_chunk_size


if __name__ == '__main__':
    asyncio.run(main())
