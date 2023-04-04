import asyncio

from communica.clients import SimpleClient
from communica.connectors import TcpConnector


connector = TcpConnector('localhost', 16161)


async def main():
    async with SimpleClient(connector=connector) as client:
        resp = await client.request('hello')
        print(f'Server responded with {resp!r}')

        await client.throw('bye')


asyncio.run(main())
