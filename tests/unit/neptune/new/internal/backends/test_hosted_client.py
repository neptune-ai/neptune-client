#
# Copyright (c) 2019, Neptune Labs Sp. z o.o.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from dataclasses import dataclass

import pytest

from neptune.api.models import FieldType
from neptune.internal.backends.hosted_neptune_backend import _get_column_type_from_entries


def test__get_column_type_from_entries():
    @dataclass
    class DTO:
        type: str
        name: str = "test_column"

    # when
    test_cases = [
        {"entries": [], "exc": ValueError},
        {"entries": [DTO(type="float")], "result": FieldType.FLOAT.value},
        {"entries": [DTO(type="string")], "result": FieldType.STRING.value},
        {"entries": [DTO(type="float"), DTO(type="floatSeries")], "exc": ValueError},
        {"entries": [DTO(type="float"), DTO(type="int")], "result": FieldType.FLOAT.value},
        {"entries": [DTO(type="float"), DTO(type="int"), DTO(type="datetime")], "result": FieldType.STRING.value},
        {"entries": [DTO(type="float"), DTO(type="int"), DTO(type="string")], "result": FieldType.STRING.value},
        {
            "entries": [DTO(type="float"), DTO(type="int"), DTO(type="string", name="test_column_different")],
            "result": FieldType.FLOAT.value,
        },
    ]

    # then
    for tc in test_cases:
        exc = tc.get("exc", None)
        if exc is not None:
            with pytest.raises(exc):
                _get_column_type_from_entries(tc["entries"], column="test_column")
        else:
            result = _get_column_type_from_entries(tc["entries"], column="test_column")
            assert result == tc["result"]
