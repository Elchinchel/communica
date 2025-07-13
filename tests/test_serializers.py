from typing import Dict
from decimal import Decimal
from dataclasses import dataclass

import pytest
from adaptix.load_error import LoadError

from communica.serializers import AdaptixSerializer
from communica.serializers.json import json_loadb


@dataclass
class Model:
    number: int
    num_to_strings: Dict[Decimal, str]


raw_Model = b'''
{
    "number": 1,
    "num_to_strings": {
        "1": "1",
        "2": "2"
    }
}
'''
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
        data = serializer.load(raw_Model)

        assert data.number == Model_inst.number
        assert data.num_to_strings == Model_inst.num_to_strings

    def test_dump(self):
        serializer = AdaptixSerializer(Model)
        raw = serializer.client_dump(Model_inst)

        assert json_loadb(raw) == json_loadb(raw_Model)

    def test_bad_request(self):
        serializer = AdaptixSerializer(Model)
        with pytest.raises(LoadError):
            serializer.load(b'{}')
