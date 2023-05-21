from typing import Dict
from decimal import Decimal
from dataclasses import dataclass

import pytest

from adaptix.load_error import LoadError

from communica.utils import json_loadb
from communica.serializers import AdaptixSerializer


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


class TestAdaptixSerializer:
    def test_load(self):
        serializer = AdaptixSerializer(Model)
        data = serializer.load(raw_Model)

        assert data.number == 1
        assert data.num_to_strings == {1: '1', 2: '2'}

    def test_dump(self):
        serializer = AdaptixSerializer(Model)
        data = Model(1, {1: '1', 2: '2'})  # type: ignore
        raw = serializer.dump(data)

        assert json_loadb(raw) == json_loadb(raw_Model)

    def test_bad_request(self):
        serializer = AdaptixSerializer(Model)
        with pytest.raises(LoadError):
            serializer.load(b'{}')
