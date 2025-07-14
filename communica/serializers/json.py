import json
from typing import Any

from communica.utils import ByteSeq
from communica.exceptions import FeatureNotAvailable

from .base import BaseSerializer


class JsonSerializer(BaseSerializer):
    """
    This serializer just passes data to json dump function.
    """
    __slots__ = ()

    def json_dumpb(self, obj: Any) -> bytes:
        return json.dumps(obj, ensure_ascii=False).encode('utf-8')

    def json_loadb(self, __obj: ByteSeq) -> Any:
        if not isinstance(__obj, (bytes, str)):
            __obj = bytes(__obj)
        return json.loads(__obj)

    def dump(self, data: Any) -> bytes:
        return self.json_dumpb(data)

    def load(self, raw_data: ByteSeq) -> Any:
        return self.json_loadb(raw_data)


try:
    import orjson

    class OrjsonSerializer(JsonSerializer):  # pyright: ignore[reportRedeclaration]
        __slots__ = ('_option',)

        def __init__(
                self,
                orjson_dump_option: int = orjson.OPT_NON_STR_KEYS
        ) -> None:
            self._option = orjson_dump_option

        def orjson_dumpb(self, obj: Any) -> bytes:
            return orjson.dumps(obj, option=self._option)

        orjson_loadb = orjson.loads

    json_dumpb = OrjsonSerializer().json_dumpb
    json_loadb = OrjsonSerializer().json_loadb
except ModuleNotFoundError:
    class OrjsonSerializer(JsonSerializer):
        def __init__(
                self,
                orjson_dump_option: int = 0
        ) -> None:
            raise FeatureNotAvailable(
                'OrjsonSerializer requires orjson library. '
                'Install communica with [orjson] extra.'
            )

    json_dumpb = JsonSerializer().json_dumpb
    json_loadb = JsonSerializer().json_loadb
