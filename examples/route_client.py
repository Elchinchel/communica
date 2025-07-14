"""
This is an example of worker client, which gets
requests from server, processing them
(fetching some additional info from server in process)
and returns back to server.

Server requests processing with "process_something".
After everything is done, server shuts client down,
requesting "close_connection" route.
"""

import asyncio
from typing import Any

from communica import RouteClient, LocalConnector


connector = LocalConnector('foobar')
client = RouteClient(connector=connector)


@client.routes.handler('process_something')
async def handle_hello_from_server(data: Any):
    print('Processing data from server:', data)
    await asyncio.sleep(1)
    info = await client.request(
        'get_some_info_about',
        {'subject': 'goose'}
    )
    print('Got info from server:', info)
    return data


@client.routes.handler('close_client')
async def close_client(data: None):
    print('Received "close_client" request')
    client.stop()
    # at this moment connection likely have no chance
    # to send response back, that's why "throw" used
    # on requesting side.


if __name__ == '__main__':
    asyncio.run(client.run())
    print('client is closed')
