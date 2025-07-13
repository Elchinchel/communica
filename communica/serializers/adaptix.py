try:
    import adaptix

    _HAVE_ADAPTIX = True
except ModuleNotFoundError:
    _HAVE_ADAPTIX = False

from typing import Any, Type, Generic, TypeVar

from communica.utils import ByteSeq
from communica.exceptions import FeatureNotAvailable
from communica.serializers import BaseSerializer
from communica.serializers.json import JsonSerializer


TReq = TypeVar('TReq')
TResp = TypeVar('TResp')

DEFAULT_JSON_SERIALIZER = JsonSerializer()


class AdaptixSerializer(BaseSerializer, Generic[TReq, TResp]):
    """
    This serializer passes data to `adaptix.Retort`

    If you specify `request_model` as A and `response_model` as B:
    - On request side, it dumps A (out) and loads B (in)
    - On response side, it loads A (in) and dumps B (out)

    So, validation ensured on both sides.
    If the same serializer defined on both sides, of course.
    """

    __slots__ = ('_retort', '_req_model', '_resp_model', '_json')

    # XXX: один из вариантов
    # FROM_HANDLER = специальное значение
    # если регистратор ручки видит это значение вместо сериалайзера,
    # он берет ручку и через какой-нибудь метод собирает сериалайзер
    # по сигнатуре ручки

    def __init__(
            self,
            request_model: 'Type[TReq] | None',
            response_model: 'Type[TResp] | None' = None,
            retort: 'adaptix.Retort | None' = None,
            json_serializer: 'JsonSerializer | None' = None,
    ) -> None:
        """
            Args:
                request_model: Type of object, which will be sent
                    from requester to responder.
                    None means no validation/conversion performed.
                response_model: Type of object, which will be sent
                    from responder to requester.
                    None means no validation/conversion performed.
        """

        if not _HAVE_ADAPTIX:
            raise FeatureNotAvailable(
                'AdaptixSerializer requires adaptix library. '
                'Install communica with [adaptix] extra.'
            )

        self._json = json_serializer or DEFAULT_JSON_SERIALIZER
        self._retort = retort or adaptix.Retort()  # pyright: ignore[reportPossiblyUnboundVariable]
        self._req_model = request_model or Any
        self._resp_model = response_model or Any

    def load(self, raw_data: ByteSeq) -> TReq:
        'Load data as request_model'
        return self._retort.load(
            self._json.load(raw_data),
            self._req_model
        )  # pyright: ignore[reportReturnType]  # until typevar defaults available

    def dump(self, data: TResp) -> bytes:
        'Dump data as response_model'
        return self._json.dump(
            self._retort.dump(data, self._resp_model)
        )

    def client_load(self, raw_data: ByteSeq) -> TResp:
        'Load data as response_model'
        return self._retort.load(
            self._json.load(raw_data),
            self._resp_model
        )  # pyright: ignore[reportReturnType]

    def client_dump(self, data: TReq) -> bytes:
        'Dump data as request_model'
        return self._json.dump(
            self._retort.dump(data, self._req_model)
        )
