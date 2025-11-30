import json
from typing import Dict, Type, ClassVar
from decimal import Decimal
from dataclasses import dataclass

import pytest
from adaptix.load_error import LoadError

from communica.serializers import (
    JsonSerializer,
    AdaptixSerializer,
    MsgpackSerializer,
)
from communica.serializers.base import BaseSerializer
from communica.serializers.json import OrjsonSerializer, json_loadb


@dataclass
class Model:
    number: int
    num_to_strings: Dict[Decimal, str]


raw_Model_json = b'''
{
    "number": 1,
    "num_to_strings": {
        "1": "1",
        "2": "2"
    }
}
'''
parsed_Model = json.loads(raw_Model_json)
Model_inst = Model(
    number=1,
    num_to_strings={
        Decimal(1): '1',
        Decimal(2): '2'
    }
)


class TestAdaptixSerializer:
    def test_load(self):
        serializer = AdaptixSerializer(Model)
        data = serializer.load(raw_Model_json)

        assert data.number == Model_inst.number
        assert data.num_to_strings == Model_inst.num_to_strings

    def test_dump(self):
        serializer = AdaptixSerializer(Model)
        raw = serializer.client_dump(Model_inst)

        assert json_loadb(raw) == json_loadb(raw_Model_json)

    def test_bad_request(self):
        serializer = AdaptixSerializer(Model)
        with pytest.raises(LoadError):
            serializer.load(b'{}')


class _TrivialSerializerTest:
    SerializerCls: ClassVar[Type[BaseSerializer]]
    raw_Model: ClassVar[bytes]

    def test_load(self):
        serializer = self.SerializerCls()
        data = serializer.load(self.raw_Model)

        assert data == parsed_Model

    def test_dump(self):
        serializer = self.SerializerCls()
        raw = serializer.dump(parsed_Model)

        assert serializer.load(raw) == json_loadb(raw_Model_json)


class TestJsonSerializer(_TrivialSerializerTest):
    SerializerCls = JsonSerializer
    raw_Model = raw_Model_json


class TestOrjsonSerializer(TestJsonSerializer):
    SerializerCls = OrjsonSerializer


class TestMsgpackSerializer(_TrivialSerializerTest):
    SerializerCls = MsgpackSerializer
    raw_Model = b'\x82\xA6number\x01\xAEnum_to_strings\x82\xA11\xA11\xA12\xA12'

    def test_bytes(self):
        serializer = self.SerializerCls()
        some_bytes = b'pivo'
        dumped = serializer.dump(some_bytes)
        assert serializer.load(dumped) == some_bytes
