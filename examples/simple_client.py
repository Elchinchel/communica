import asyncio

from communica import SimpleClient, TcpConnector


connector = TcpConnector('localhost', 16161)

async def main():
    async with SimpleClient(connector=connector) as client:
        resp = await client.request('hello')
        print(f'Server responded with {resp!r}')

asyncio.run(main())
