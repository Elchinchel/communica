import pytest

from communica.utils import LateBoundFuture, iscallable


class CallableObject:
    def __call__(self):
        return True

    def callable_method(self):
        return True


def callable_function():
    return True


callable_function2 = lambda: True  # noqa


def test_iscallable():
    callable_obj = CallableObject()

    assert callable_obj()
    assert callable_obj.callable_method()
    assert callable_function()
    assert callable_function2()

    assert iscallable(callable_obj)
    assert iscallable(callable_obj.callable_method)
    assert iscallable(callable_function)
    assert iscallable(callable_function2)

    assert not iscallable(None)
    assert not iscallable(int)
    assert not iscallable('?')


@pytest.mark.asyncio
async def test_late_bound_future():
    fut = LateBoundFuture()
    assert fut.get_loop()

    fut = LateBoundFuture()
    fut.set_result(1)
    assert await fut == 1

    fut = LateBoundFuture()
    fut.set_exception(KeyError)
    with pytest.raises(KeyError):
        fut.result()

    LateBoundFuture().add_done_callback(lambda _: ...)
