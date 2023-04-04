from abc import ABC, abstractmethod
from typing import Any


class BaseSerializer(ABC):
    """
    Отвечает за сериализацию и десериализацию сообщений.

    Подразумевается, что на другом конце соединения будет такой же
    Serializer и разные их типы не совместимы между собой.
    """

    __slots__ = ()

    @abstractmethod
    def dump(self, data: Any) -> bytes:
        raise NotImplementedError

    @abstractmethod
    def load(self, raw_data: bytes) -> Any:
        raise NotImplementedError
