class CommunicaError(Exception):
    """Base exception for all library errors"""


class RequesterError(CommunicaError):
    """
    Errors, caused by requesting side
    (not forget, Server can make request to Client too)
    """

    @property
    def message(self):
        return self.args[0]


class ResponderError(CommunicaError):
    """Errors, caused by responding side"""

    @property
    def message(self):
        return self.args[0]
