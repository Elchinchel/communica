import asyncio

from communica import SimpleServer, TcpConnector


connector = TcpConnector('localhost', 16161)

def handler(data: str):
    print(f'Received {data!r} from client')
    return f'Thanks for your {data!r}!'

server = SimpleServer(
    connector=connector,
    handler=handler
)

asyncio.run(server.run())
