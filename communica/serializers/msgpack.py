try:
    import msgpack

    _HAVE_MSGPACK = True
except ModuleNotFoundError:
    _HAVE_MSGPACK = False

from typing import Any

from communica.utils import ByteSeq
from communica.exceptions import FeatureNotAvailable

from .base import BaseSerializer


class MsgpackSerializer(BaseSerializer):
    """
    This serializer just passes data to json dump function.
    """
    __slots__ = ()

    def __init__(self) -> None:
        if not _HAVE_MSGPACK:
            raise FeatureNotAvailable(
                'MsgpackSerializer requires msgpack library. '
                'Install communica with [msgpack] extra.'
            )

    def dump(self, data: Any) -> bytes:
        dumped = msgpack.packb(data)
        assert isinstance(dumped, bytes)
        return dumped

    def load(self, raw_data: ByteSeq) -> Any:
        return msgpack.unpackb(raw_data)
