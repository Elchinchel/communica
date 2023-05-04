# type: ignore

import gc
import time
import asyncio

import pytest

from communica.connectors.rabbitmq import (
    ConnectionCheckPolicy, ServerCheckPolicy, ClientCheckPolicy
)


class Connection:
    policy: ConnectionCheckPolicy
    closed: bool

    def __init__(self) -> None:
        self.closed = False
        self.messages_by_policy = []

    def send(self):
        self.policy.message_sent()

    async def _send(self, msg_type, body):
        self.messages_by_policy.append(time.time())
        await asyncio.sleep(0.5)

    async def close(self):
        self.closed = True


@pytest.mark.asyncio
async def test_server_policy():
    conn = Connection()
    policy = ServerCheckPolicy(conn, 1)
    conn.policy = policy

    await asyncio.sleep(1.1)
    # policy should send message, 1 second without messages

    assert len(conn.messages_by_policy) == 1

    # policy should not send messages
    await asyncio.sleep(0.5)
    conn.send()
    await asyncio.sleep(0.5)
    conn.send()
    await asyncio.sleep(0.5)
    conn.send()
    await asyncio.sleep(0.5)
    conn.send()
    assert len(conn.messages_by_policy) == 1

    await asyncio.sleep(0.8)
    assert len(conn.messages_by_policy) == 1

    await asyncio.sleep(0.3)

    # last message 0.8 + 0.3 > 1 seconds, should send
    assert len(conn.messages_by_policy) == 2

    # this is call of .message_sent() when policy in process of sending
    conn.send()

    await asyncio.sleep(1.1)

    assert len(conn.messages_by_policy) == 3

    assert not policy._send_task.done()
    del(conn)
    gc.collect()  # destroying conn (hopefully)
    await asyncio.sleep(2)
    assert policy._send_task.done()


@pytest.mark.asyncio
async def test_client_policy():
    conn = Connection()
    policy = ClientCheckPolicy(conn, 1)
    conn.policy = policy

    await asyncio.sleep(0.5)

    assert not conn.closed

    policy.message_received()
    await asyncio.sleep(0.5)
    policy.message_received()
    await asyncio.sleep(0.5)
    policy.message_received()
    await asyncio.sleep(0.5)
    policy.message_received()

    assert not conn.closed

    await asyncio.sleep(0.8)
    assert not conn.closed

    await asyncio.sleep(0.3)
    assert conn.closed

    policy = ClientCheckPolicy(conn, 1)

    del(conn)
    gc.collect()
    await asyncio.sleep(1.1)
    assert policy._handle.when() < policy._loop.time()
