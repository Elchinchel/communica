import os
import logging

import pytest

from communica.utils import logger
from communica.connectors import RmqConnector, TcpConnector, LocalConnector
from communica.serializers import JsonSerializer


logger.setLevel(logging.DEBUG)


RMQ_URL = os.getenv('RABBITMQ_URL', 'amqp://guest:guest@localhost:5672/')


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
        'url': RMQ_URL,
        'address': 'test',
        'client_id': 'test_client'
    }),
])
def connector(request: pytest.FixtureRequest):
    cls, get_args = request.param
    return cls(**get_args())
