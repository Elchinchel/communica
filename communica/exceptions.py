class CommunicaError(Exception):
    """Base exception for all library errors."""


class SerializerError(CommunicaError):
    """Error on loading or dumping data"""

    def __init__(self, message: str) -> None:
        super().__init__(message)

    @property
    def message(self):
        return self.args[0]

    def to_dict(self):
        return {
            'message': self.message
        }

    @classmethod
    def from_dict(cls, data: dict):
        return cls(data['message'])


class _RequestsErr(CommunicaError):
    def __init__(self, message: str, code: 'int | None' = None) -> None:
        super().__init__(message, code)

    @property
    def message(self):
        return self.args[0]

    @property
    def code(self):
        return self.args[1]

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}({self.to_dict()!r})'

    def to_dict(self):
        return {
            'code': self.code,
            'message': self.message
        }

    @classmethod
    def from_dict(cls, data: dict):
        return cls(data['message'], data['code'])


class ReqError(_RequestsErr):
    """
    Errors, caused by requesting side
    (don't forget, Server can make request to Client too)

    Can be raised by user code in request handler, in this case
    requesting side get same exception with specified
    message and code properties.
    """


class RespError(_RequestsErr):
    """
    Errors, caused by responding side.

    Can be raised by user code in request handler, in this case
    requesting side get same exception with specified
    message and code properties.
    """


class UnknownError(_RequestsErr):
    """Unhandled exception on responding side."""

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}({self.message})'

    @property
    def code(self) -> None:
        return None
