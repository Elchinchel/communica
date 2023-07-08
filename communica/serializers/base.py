from abc import ABC, abstractmethod
from typing import Any


class BaseSerializer(ABC):
    """
    Responsible for message serialization and data validation.

    Different serializers may be compatible, but better
    have serializers of the same type on both sides.
    """

    __slots__ = ()

    @abstractmethod
    def dump(self, data: Any) -> bytes:
        raise NotImplementedError

    @abstractmethod
    def load(self, raw_data: bytes) -> Any:
        raise NotImplementedError

    # distinct methods if serialization on client side differs

    def client_dump(self, data: Any) -> bytes:
        return self.dump(data)

    def client_load(self, raw_data: bytes) -> Any:
        return self.load(raw_data)
