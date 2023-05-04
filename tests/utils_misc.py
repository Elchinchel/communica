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