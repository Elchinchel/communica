# pyright: reportAttributeAccessIssue=none

import sys
import asyncio


IS_AVAILABLE = True


if sys.platform == 'win32':
    class PipeServerWrapper(asyncio.AbstractServer):
        def __init__(self, server: asyncio.windows_events.PipeServer) -> None:
            self.server = server

        def close(self):
            self.server.close()

        def is_serving(self):
            return not self.server.closed()

        async def start_serving(self):
            return

        async def wait_closed(self):
            return

        def get_loop(self):
            raise NotImplementedError

        async def serve_forever(self):
            raise NotImplementedError


    async def open_connection(
            address: str
    ) -> 'tuple[asyncio.StreamReader, asyncio.StreamWriter]':
        loop = asyncio.get_event_loop()
        if not hasattr(loop, 'create_pipe_connection'):
            raise TypeError('Named pipes are not supported on current platform')

        reader = asyncio.StreamReader(loop=loop)
        protocol = asyncio.StreamReaderProtocol(reader, loop=loop)
        transport, _ = await loop.create_pipe_connection(
            lambda: protocol, format_address(address))
        writer = asyncio.StreamWriter(transport, protocol, reader, loop)
        return reader, writer

    async def start_server(
            client_connected_cb, address: str
    ) -> asyncio.AbstractServer:
        loop = asyncio.get_event_loop()
        if not hasattr(loop, 'start_serving_pipe'):
            raise TypeError('Named pipes are not supported on current platform')

        def factory():
            reader = asyncio.StreamReader(loop=loop)
            protocol = asyncio.StreamReaderProtocol(
                reader, client_connected_cb, loop=loop)
            return protocol

        servers = await loop.start_serving_pipe(factory, format_address(address))
        return PipeServerWrapper(servers[0])

    def format_address(address: str) -> str:
        return '\\\\.\\pipe\\' + 'communica.' + address

elif hasattr(asyncio, 'open_unix_connection'):
    import os
    import os.path
    import tempfile

    SOCK_DIR = os.path.join(tempfile.gettempdir(), 'communica')
    if not os.path.exists(SOCK_DIR):
        os.makedirs(SOCK_DIR)

    async def open_connection(
            address: str
    ) -> 'tuple[asyncio.StreamReader, asyncio.StreamWriter]':
        sock_path = format_address(address)
        return await asyncio.open_unix_connection(sock_path)

    async def start_server(
            client_connected_cb, address: str
    ) -> asyncio.AbstractServer:
        sock_path = format_address(address)
        return await asyncio.start_unix_server(client_connected_cb, sock_path)

    def format_address(address: str) -> str:
        return os.path.join(SOCK_DIR, address + '.sock')

else:
    IS_AVAILABLE = False
