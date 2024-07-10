#
# Copyright (c) 2024, Neptune Labs Sp. z o.o.
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

import datetime
import random
import string

import pytest

from neptune.core.operations import (
    AssignBool,
    AssignDatetime,
    AssignFloat,
    AssignInt,
    AssignString,
    LogFloats,
)
from neptune.core.operations.operation import FieldOperation
from neptune.exceptions import MalformedOperation


class TestAssignInt:
    def test__assign_int_operation__to_dict(self):
        # given
        value = random.randint(int(-1e8), int(1e8))
        assign_int = AssignInt(["test", "path"], value)

        # when
        data = assign_int.to_dict()

        # then
        assert data == {"type": "AssignInt", "path": ["test", "path"], "value": value}

    def test__assign_int_operation__from_dict(self):
        # given
        value = random.randint(int(-1e8), int(1e8))
        data = {"type": "AssignInt", "path": ["test", "path"], "value": value}

        # when
        assign_int = AssignInt.from_dict(data)

        # then
        assert assign_int == AssignInt(["test", "path"], value)


class TestAssignString:
    def test__assign_string_operation__to_dict(self):
        # given
        value = "".join(random.choices(string.ascii_lowercase, k=20))
        assign_string = AssignString(["test", "path"], value)

        # when
        data = assign_string.to_dict()

        # then
        assert data == {"type": "AssignString", "path": ["test", "path"], "value": value}

    def test__assign_string_operation__from_dict(self):
        # given
        value = "".join(random.choices(string.ascii_lowercase, k=20))
        data = {"type": "AssignString", "path": ["test", "path"], "value": value}
        assign_string = AssignString.from_dict(data)

        # then
        assert assign_string == AssignString(["test", "path"], value)


class TestAssignBool:
    def test__assign_bool_operation__to_dict(self):
        # given
        value = random.choice([True, False])
        assign_bool = AssignBool(["test", "path"], value)

        # when
        data = assign_bool.to_dict()

        # then
        assert data == {"type": "AssignBool", "path": ["test", "path"], "value": value}

    def test__assign_bool_operation__from_dict(self):
        # given
        value = random.choice([True, False])
        data = {"type": "AssignBool", "path": ["test", "path"], "value": value}

        # when
        assign_bool = AssignBool.from_dict(data)

        # then
        assert assign_bool == AssignBool(["test", "path"], value)


class TestAssignFloat:
    def test__assign_float_operation__to_dict(self):
        # given
        value = random.uniform(-1e8, 1e8)
        assign_float = AssignFloat(["test", "path"], value)

        # when
        data = assign_float.to_dict()

        # then
        assert data == {"type": "AssignFloat", "path": ["test", "path"], "value": value}

    def test__assign_float_operation__from_dict(self):
        # given
        value = random.uniform(-1e8, 1e8)
        data = {"type": "AssignFloat", "path": ["test", "path"], "value": value}
        assign_float = AssignFloat.from_dict(data)

        # then
        assert assign_float == AssignFloat(["test", "path"], value)


class TestAssignDatetime:
    def test__assign_datetime_operation__to_dict(self):
        # given
        value = datetime.datetime.utcnow()
        assign_datetime = AssignDatetime(["test", "path"], value)

        # when
        data = assign_datetime.to_dict()

        # then
        assert data == {"type": "AssignDatetime", "path": ["test", "path"], "value": int(value.timestamp() * 1000)}

    def test__assign_datetime_operation__from_dict(self):
        # given
        value = datetime.datetime.now().replace(microsecond=0)
        value_as_int = int(value.timestamp() * 1000)
        data = {
            "type": "AssignDatetime",
            "path": ["test", "path"],
            "value": value_as_int,
        }
        assign_datetime = AssignDatetime.from_dict(data)

        # then
        assert assign_datetime == AssignDatetime(["test", "path"], value)


class TestLogFloats:
    def test__log_floats_operation__to_dict(self):
        # given
        values = [
            LogFloats.ValueType(
                value=random.uniform(-1e8, 1e8),
                step=random.uniform(-1e8, 1e8),
                ts=random.uniform(-1e8, 1e8),
            )
            for _ in range(5)
        ]

        expected_values = [value.to_dict() for value in values]
        log_floats = LogFloats(["test", "path"], values)

        # when
        data = log_floats.to_dict()

        # then
        assert data == {"type": "LogFloats", "path": ["test", "path"], "values": expected_values}

    def test__log_floats_operation__from_dict(self):
        # given
        values = [
            LogFloats.ValueType(
                value=random.uniform(-1e8, 1e8),
                step=random.uniform(-1e8, 1e8),
                ts=random.uniform(-1e8, 1e8),
            )
            for _ in range(5)
        ]

        dict_values = [value.to_dict() for value in values]
        data = {"type": "LogFloats", "path": ["test", "path"], "values": dict_values}

        # when
        log_floats = LogFloats.from_dict(data)

        # then
        assert log_floats == LogFloats(["test", "path"], values)


@pytest.mark.parametrize(
    "operation",
    [
        AssignInt(["test", "path"], 1),
        AssignString(["test", "path"], "test"),
        AssignBool(["test", "path"], True),
        AssignDatetime(["test", "path"], datetime.datetime.now().replace(microsecond=0)),
        AssignFloat(["test", "path"], 1.0),
        LogFloats(["test", "path"], [LogFloats.ValueType(1.0, 1.0, 1.0)]),
    ],
)
def test_is_serialisation_consistent(operation):
    assert operation.from_dict(operation.to_dict()) == operation


def test__operation__from_dict():
    with pytest.raises(MalformedOperation):
        FieldOperation.from_dict({"path": ["test", "path"], "value": 1})

    with pytest.raises(MalformedOperation):
        FieldOperation.from_dict({"type": "IncorrectType", "path": ["test", "path"], "value": 1})

    assert FieldOperation.from_dict({"type": "AssignInt", "path": ["test", "path"], "value": 1}) == AssignInt(
        ["test", "path"], 1
    )

    assert FieldOperation.from_dict(
        {"type": "LogFloats", "path": ["test", "path"], "values": [{"value": 1.0, "step": 1.0, "ts": 1.0}]}
    ) == LogFloats(["test", "path"], [LogFloats.ValueType(1.0, 1.0, 1.0)])
