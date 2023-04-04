import asyncio

import pytest

from communica.connectors import *
from communica.serializers import JsonSerializer


@pytest.fixture
def serializer():
    return JsonSerializer()


@pytest.fixture(params=[
    (TcpConnector, lambda: {
        'host': 'localhost',
        'port': 10624
    }),
    (LocalConnector, lambda: {
        'name': 'test',
    }),
    (RmqConnector, lambda: {
        'url': 'amqp://guest:guest@localhost:5672/',
        'address': 'test',
        'connect_id': 'test_client'
    }),
])
def connector(request: pytest.FixtureRequest):
    cls, get_args = request.param
    return cls(**get_args())
