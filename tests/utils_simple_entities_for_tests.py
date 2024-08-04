import asyncio
from typing import Iterable

import pytest
from utils_misc import wait_tasks

from communica.entities import SimpleClient, SimpleServer


class MessageOrderChecker:
    def __init__(self, messages: 'list | Iterable') -> None:
        if not isinstance(messages, list):
            messages = list(messages)
        self.messages = messages
        self.done = asyncio.Event()

    async def handler(self, data):
        if not self.messages:
            pytest.fail(f'Got "{data}", no messages expected')
        if data != self.messages[0]:
            pytest.fail('Message out of order:\n'
                       f'expect "{self.messages[0]}", got "{data}"')
        self.messages.pop(0)
        if not self.messages:
            self.done.set()
        return data


class MessageExistenceChecker:
    def __init__(self, messages: 'set | Iterable') -> None:
        if not isinstance(messages, set):
            messages = set(messages)
        self.received = set()
        self.expected = messages
        self.done = asyncio.Event()

    async def handler(self, data):
        if not self.expected:
            pytest.fail(f'Got "{data}", no messages expected')

        if data not in self.expected:
            if data in self.received:
                pytest.fail(f'Got same message twice: "{data}"')
            else:
                pytest.fail(f'Unexpected message: "{data}"')
        else:
            self.expected.remove(data)
            self.received.add(data)

        if not self.expected:
            self.done.set()

        return data


CLIENT_ID = 'test_client'

server_to_client_messages = range(50)
client_to_server_messages = range(50, 100)


async def run_sequential_send_with_simples(connector, serializer):
    async def run(entity, messages, done_event):
        for message in messages:
            resp = await entity.request(message)
            assert resp == message, 'Response data must be equal to sent'
        await done_event.wait()

    cli_checker = MessageOrderChecker(server_to_client_messages)
    client = SimpleClient(
        connector, cli_checker.handler, serializer, CLIENT_ID)

    srv_checker = MessageOrderChecker(client_to_server_messages)
    server = SimpleServer(connector, srv_checker.handler, serializer)

    async with server, client:
        await wait_tasks(
            run(server, server_to_client_messages, srv_checker.done),
            run(client, client_to_server_messages, cli_checker.done),
            timeout=15
        )


async def run_concurrent_send_with_simples(connector, serializer):
    async def run(entity, messages, done_event):
        calls = [entity.throw(msg) for msg in messages]
        await asyncio.gather(*calls)
        await done_event.wait()

    cli_checker = MessageExistenceChecker(server_to_client_messages)
    client = SimpleClient(
        connector, cli_checker.handler, serializer, CLIENT_ID)

    srv_checker = MessageExistenceChecker(client_to_server_messages)
    server = SimpleServer(connector, srv_checker.handler, serializer)

    async with server, client:
        await wait_tasks(
            run(server, server_to_client_messages, srv_checker.done),
            run(client, client_to_server_messages, cli_checker.done),
            timeout=15
        )
