import asyncio
import logging

from traceback import format_exception

import pytest


logger = logging.getLogger()


class GatheredContexts:
    def __init__(self, *context_managers) -> None:
        self._context_managers = context_managers

    async def __aenter__(self):
        await asyncio.gather(*(
            ctx_manager.__aenter__()
                for ctx_manager in self._context_managers
        ))

    async def __aexit__(self, __exc_type, __exc_value, __traceback):
        result = await asyncio.gather(*(
            ctx_manager.__aexit__(__exc_type, __exc_value, __traceback)
                for ctx_manager in self._context_managers
        ), return_exceptions=True)

        for exc in filter(lambda x: isinstance(x, Exception), result):
            exc_str = format_exception(type(exc), exc, exc.__traceback__)
            logger.error('>>> Context leave exception\n%s', '\n'.join(exc_str))


async def wait_tasks(*tasks, timeout: int):
    done, pending = await asyncio.wait(tasks, timeout=timeout)
    if not pending:
        return

    print('Timeout details:')
    for fut in done:
        print(f'>>> Done {fut!r}')
    for fut in pending:
        print(f'>>> Pending {fut!r}')

    pytest.fail('Wait timeout exceeded', pytrace=False)