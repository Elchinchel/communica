import asyncio


ADDR = {'host': 'localhost', 'port': 5050}


async def on_connect(r: asyncio.StreamReader, w: asyncio.StreamWriter):
    await r.read(5)
    w.write(b'ok!')
    await w.drain()
    print('SRV: Response done, closing...')

    w.close()
    await w.wait_closed()
    print('SRV: Connection closed.')

    server.close()


async def write():
    r, w = await asyncio.open_connection(**ADDR)

    w.write(b'hello')
    await w.drain()

    print('CLI: hello sent')

    await r.read(3)

    print('CLI: response read done')

    w.write(b'how are you?')
    await w.drain()

    print('CLI: next request sent')

    w.write(b'okay?')
    await w.drain()

    print('CLI: last request sent')


async def main():
    global server

    server = await asyncio.start_server(on_connect, **ADDR)

    client = asyncio.create_task(write())

    await server.wait_closed()
    await client


asyncio.run(main())
