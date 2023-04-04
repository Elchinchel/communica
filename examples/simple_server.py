"Server with one handler"

import asyncio

from communica.servers import SimpleServer
from communica.connectors import TcpConnector


connector = TcpConnector('localhost', 16161)


def handler(data: str):
    print(f'Received {data!r} from client')

    if data == 'bye':
        server.stop()

    return f'Thanks for your {data!r}!'


server = SimpleServer(
    connector=connector,
    handler=handler
)

asyncio.run(server.run())
