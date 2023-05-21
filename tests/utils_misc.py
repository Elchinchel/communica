import asyncio
import logging

from traceback import format_exception

import pytest


tasks = set()
logger = logging.getLogger()


def create_task(coro):
    task = asyncio.create_task(coro)
    tasks.add(task)
    task.add_done_callback(tasks.discard)
    return task


def dummy_handler(data):
    return data


async def wait_second(coro):
    try:
        return await asyncio.wait_for(coro, timeout=1)
    except asyncio.TimeoutError:
        pass
    pytest.fail('Wait timeout exceeded', pytrace=False)


async def wait_tasks(*tasks, timeout: int):
    done, pending = await asyncio.wait(tasks, timeout=timeout)
    if not pending:
        return

    print('Timeout details:')
    for fut in done:
        print(f'>>> Done {fut!r}')
    for fut in pending:
        print(f'>>> Pending {fut!r}')
        fut.cancel()

    pytest.fail('Wait timeout exceeded', pytrace=False)