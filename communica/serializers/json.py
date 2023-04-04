from typing import Any

from communica.utils import json_dumpb, json_loadb

from .base import BaseSerializer


class JsonSerializer(BaseSerializer):
    def dump(self, data: Any) -> bytes:
        return json_dumpb(data)

    def load(self, raw_data: bytes) -> Any:
        return json_loadb(raw_data)
