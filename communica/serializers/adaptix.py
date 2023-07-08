try:
    import adaptix

    _HAVE_ADAPTIX = True
except ModuleNotFoundError:
    _HAVE_ADAPTIX = False

from typing import Any, Generic, Type, TypeVar

from communica.utils import json_dumpb, json_loadb
from communica.serializers import BaseSerializer


TReq = TypeVar('TReq')
TResp = TypeVar('TResp')


class AdaptixSerializer(BaseSerializer, Generic[TReq, TResp]):
    __slots__ = ('_retort', '_req_model', '_resp_model')

    # XXX: один из вариантов
    # FROM_HANDLER = специальное значение
    # если регистратор ручки видит это значение вместо сериалайзера,
    # он берет ручку и через какой-нибудь метод собирает сериалайзер
    # по сигнатуре ручки

    def __init__(
            self,
            request_model: Type[TReq],
            response_model: 'Type[TResp] | None' = None,
            retort: 'adaptix.Retort | None' = None
    ) -> None:
        if not _HAVE_ADAPTIX:
            raise ImportError('AdaptixSerializer requires adaptix library. '
                              'Install communica with [adaptix] extra.')

        self._retort = retort or adaptix.Retort()
        self._req_model = request_model
        self._resp_model = response_model or Any

    def load(self, raw_data: bytes) -> TReq:
        return self._retort.load(
            json_loadb(raw_data),
            self._req_model
        )

    def dump(self, data: TResp) -> bytes:
        return json_dumpb(
            self._retort.dump(data, self._resp_model)
        )

    def client_load(self, raw_data: bytes) -> 'TResp | Any':
        return self._retort.load(
            json_loadb(raw_data),
            self._resp_model
        )

    def client_dump(self, data: TReq) -> bytes:
        return json_dumpb(
            self._retort.dump(data, self._req_model)
        )
