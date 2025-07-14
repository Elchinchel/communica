from communica.utils import iscallable


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
