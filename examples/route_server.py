"""
See description for examples/route_client.py
"""

import asyncio
from typing import Any

from communica import RouteTable, RouteServer, JsonSerializer, LocalConnector


routes = RouteTable()

@routes.handler(
    route='get_some_info_about',
    serializer=JsonSerializer()  # can be omitted
)
def foo_handler(data: Any):
    return 'message from server'


async def main():
    server = RouteServer(
        connector=LocalConnector('foobar'),
        route_table=routes
    )

    async with server:
        print('waiting for client')
        # this will block until the client connected
        await server.request(
            'process_something',
            {'some very dating data': 1}
        )

        print('throwing "close_client"')
        await server.throw('close_client', None)

        # there is no way to acknowledge receive of
        # "throwed" request, so we will just assume
        # that connection will be fast enough
        await asyncio.sleep(0.5)

    print('server is closed')


if __name__ == '__main__':
    asyncio.run(main())
