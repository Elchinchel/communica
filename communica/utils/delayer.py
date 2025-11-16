import random
import asyncio


class BackoffDelayer:
    __slots__ = ('start_delay', 'max_delay', 'factor', 'jitter', '_delay')

    def __init__(
            self,
            start_delay: float,
            max_delay: float,
            factor: float,
            jitter: float
    ) -> None:
        assert factor > 1, 'factor less or equal 1'
        assert max_delay > start_delay, 'max delay less or equal to start delay'
        self.factor = factor
        self.jitter = jitter

        self._delay = start_delay
        self.max_delay = max_delay
        self.start_delay = start_delay

    def reset(self):
        self._delay = self.start_delay

    async def wait(self):
        await asyncio.sleep(self._delay)
        new_delay = random.gauss(
            min(self.max_delay, (self._delay * self.factor)),
            self.jitter
        )
        if new_delay > self._delay:
            self._delay = new_delay


def default_backoff_delayer():
    return BackoffDelayer(
        start_delay=1,
        max_delay=30,
        factor=2,
        jitter=0.5
    )
