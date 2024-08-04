import os
import logging

import pytest
import pytest_asyncio

from communica.utils import logger
from communica.connectors import RmqConnector, TcpConnector, LocalConnector
from communica.serializers import JsonSerializer
from communica.connectors.rabbitmq import DEFAULT_POOL


logger.setLevel(logging.DEBUG)


RMQ_URL = os.getenv('RABBITMQ_URL', 'amqp://guest:guest@localhost:5672/')


@pytest.fixture
def serializer():
    return JsonSerializer()


async def tcp_connector_factory():
    yield TcpConnector(host='localhost', port=10624)


async def local_connector_factory():
    yield LocalConnector(name='test')


async def rmq_connector_factory():
    yield RmqConnector(
        url=RMQ_URL,
        address='test',
        client_id='test_client'
    )
    await DEFAULT_POOL.purge()


tcp_connector = pytest_asyncio.fixture(name='tcp_connector')(tcp_connector_factory)
local_connector = pytest_asyncio.fixture(name='local_connector')(local_connector_factory)
rmq_connector = pytest_asyncio.fixture(name='rmq_connector')(rmq_connector_factory)


@pytest_asyncio.fixture(params=[
    tcp_connector_factory,
    local_connector_factory,
    rmq_connector_factory
])
async def connector(request: pytest.FixtureRequest):
    factory = request.param
    async for val in factory():
        yield val
