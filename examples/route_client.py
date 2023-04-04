import asyncio

from communica.clients import RouteClient
from communica.connectors import TcpConnector


connector = TcpConnector('localhost', 16161)

client = RouteClient(connector=connector)


@client.route_handler('server_hello')
def handle_hello_from_server(data: str):
    print('Handling hello from server, data:', repr(data))


@client.route_handler('close_connection')
def close_client(data: str):
    print('Received "close_connection" request')
    asyncio.create_task(client.close())


asyncio.run(client.run())
