"""Test SimpleClient and SimpleServer"""

import asyncio

from typing import Iterable

import pytest

from communica.pairs import *

from utils import wait_tasks  # type: ignore
from simple_entities_for_tests import(
    MessageExistenceChecker, MessageOrderChecker,
    client_to_server_messages, server_to_client_messages,
    CLIENT_ID
)


# class TestSimpleEntities:
    # @pytest.mark.asyncio
    # async def test_sequential_send(connector, serializer):
    #     async def run(entity, messages, done_event):
    #         async with entity:
    #             for message in messages:
    #                 resp = await entity.request(message)
    #                 assert resp == message, 'Response data must be equal to sent'
    #             await done_event.wait()

    #     cli_checker = MessageOrderChecker(server_to_client_messages)
    #     client = SimpleClient(
    #         connector, cli_checker.handler, serializer, CLIENT_ID)

    #     srv_checker = MessageOrderChecker(client_to_server_messages)
    #     server = SimpleServer(connector, srv_checker.handler, serializer)

    #     await wait_tasks(
    #         run(server, server_to_client_messages, srv_checker.done),
    #         run(client, client_to_server_messages, cli_checker.done),
    #         timeout=10
    #     )


    # @pytest.mark.asyncio
    # async def test_concurrent_send(connector, serializer):
    #     async def run(entity, messages, done_event):
    #         async with entity:
    #             calls = [entity.throw(msg) for msg in messages]
    #             await asyncio.gather(*calls)
    #             await done_event.wait()

    #     cli_checker = MessageExistenceChecker(server_to_client_messages)
    #     client = SimpleClient(
    #         connector, cli_checker.handler, serializer, CLIENT_ID)

    #     srv_checker = MessageExistenceChecker(client_to_server_messages)
    #     server = SimpleServer(connector, srv_checker.handler, serializer)

    #     await wait_tasks(
    #         run(server, server_to_client_messages, srv_checker.done),
    #         run(client, client_to_server_messages, cli_checker.done),
    #         timeout=10
    #     )


# class RouteEntitiesTestCase(EntitiesTestCase):
#     # @pytest.mark.asyncio
#     # async def test_connect(connector, serializer):

#     #     with pytest.raises(asyncio.TimeoutError):
#     #         await client.init(timeout=1)

#     #     await client.init(timeout=3)


#     @pytest.mark.asyncio
#     async def test_sequential_send(self, connector, serializer):
#         async def run(entity, messages, done_event):
#             async with entity:
#                 for message in messages:
#                     resp = await entity.request(message)
#                     assert resp == message, 'Response data must be equal to sent'
#                 await done_event.wait()

#         def add_handler(entity, data: str):
#             @entity.route(str(data))
#             async def handler():

#         cli_checker = MessageOrderChecker(self.server_to_client_messages)
#         client = RouteClient(connector, self.CLIENT_ID)

#         srv_checker = MessageOrderChecker(self.client_to_server_messages)
#         server = RouteServer(connector)

#         await asyncio.gather(
#             run(server, self.server_to_client_messages, srv_checker.done),
#             run(client, self.client_to_server_messages, cli_checker.done),
#         )

#     @pytest.mark.asyncio
#     async def test_concurrent_send(self, connector, serializer):
#         async def run(entity, messages, done_event):
#             async with entity:
#                 calls = [entity.throw(msg) for msg in messages]
#                 await asyncio.gather(*calls)
#                 await done_event.wait()

#         cli_checker = MessageExistenceChecker(self.server_to_client_messages)
#         client = RouteClient(
#             connector, cli_checker.handler, serializer, self.CLIENT_ID)

#         srv_checker = MessageExistenceChecker(self.client_to_server_messages)
#         server = RouteServer(connector, srv_checker.handler, serializer)

#         await asyncio.gather(
#             run(server, self.server_to_client_messages, srv_checker.done),
#             run(client, self.client_to_server_messages, cli_checker.done),
#         )
