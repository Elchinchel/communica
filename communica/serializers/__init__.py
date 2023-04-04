from .base import BaseSerializer
from .json import JsonSerializer


default_serializer = JsonSerializer()


__all__ = (
    'BaseSerializer',
    'JsonSerializer'
)
