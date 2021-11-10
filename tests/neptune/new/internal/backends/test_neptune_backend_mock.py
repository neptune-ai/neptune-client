#
# Copyright (c) 2020, Neptune Labs Sp. z o.o.
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
import unittest
import uuid
from time import time

from neptune.new.exceptions import MetadataInconsistency

from neptune.new.internal.backends.api_model import (
    DatetimeAttribute,
    FloatAttribute,
    StringAttribute,
    FloatSeriesAttribute,
    StringSeriesAttribute,
    StringSetAttribute,
    StringSeriesValues,
    FloatSeriesValues,
    StringPointValue,
    FloatPointValue,
)

from neptune.new.internal.operation import (
    AssignFloat,
    AssignString,
    AssignDatetime,
    LogFloats,
    LogStrings,
    AddStrings,
)

from neptune.new.internal.backends.neptune_backend_mock import NeptuneBackendMock


class TestNeptuneBackendMock(unittest.TestCase):
    # pylint:disable=protected-access

    project_uuid = uuid.uuid4()

    def test_get_float_attribute(self):
        # given
        backend = NeptuneBackendMock()
        exp = backend.create_run(self.project_uuid)
        backend.execute_operations(exp.id, [AssignFloat(["x"], 5)])

        # when
        ret = backend.get_float_attribute(exp.id, ["x"])

        # then
        self.assertEqual(FloatAttribute(5), ret)

    def test_get_string_attribute(self):
        # given
        backend = NeptuneBackendMock()
        exp = backend.create_run(self.project_uuid)
        backend.execute_operations(exp.id, [AssignString(["x"], "abcx")])

        # when
        ret = backend.get_string_attribute(exp.id, ["x"])

        # then
        self.assertEqual(StringAttribute("abcx"), ret)

    def test_get_datetime_attribute(self):
        # given
        backend = NeptuneBackendMock()
        exp = backend.create_run(self.project_uuid)
        now = datetime.datetime.now()
        now = now.replace(microsecond=1000 * int(now.microsecond / 1000))
        backend.execute_operations(exp.id, [AssignDatetime(["x"], now)])

        # when
        ret = backend.get_datetime_attribute(exp.id, ["x"])

        # then
        self.assertEqual(DatetimeAttribute(now), ret)

    def test_get_float_series_attribute(self):
        # given
        backend = NeptuneBackendMock()
        exp = backend.create_run(self.project_uuid)
        backend.execute_operations(
            exp.id,
            [
                LogFloats(
                    ["x"],
                    [
                        LogFloats.ValueType(5, None, time()),
                        LogFloats.ValueType(3, None, time()),
                    ],
                )
            ],
        )
        backend.execute_operations(
            exp.id,
            [
                LogFloats(
                    ["x"],
                    [
                        LogFloats.ValueType(2, None, time()),
                        LogFloats.ValueType(9, None, time()),
                    ],
                )
            ],
        )

        # when
        ret = backend.get_float_series_attribute(exp.id, ["x"])

        # then
        self.assertEqual(FloatSeriesAttribute(9), ret)

    def test_get_string_series_attribute(self):
        # given
        backend = NeptuneBackendMock()
        exp = backend.create_run(self.project_uuid)
        backend.execute_operations(
            exp.id,
            [
                LogStrings(
                    ["x"],
                    [
                        LogStrings.ValueType("adf", None, time()),
                        LogStrings.ValueType("sdg", None, time()),
                    ],
                )
            ],
        )
        backend.execute_operations(
            exp.id,
            [
                LogStrings(
                    ["x"],
                    [
                        LogStrings.ValueType("dfh", None, time()),
                        LogStrings.ValueType("qwe", None, time()),
                    ],
                )
            ],
        )

        # when
        ret = backend.get_string_series_attribute(exp.id, ["x"])

        # then
        self.assertEqual(StringSeriesAttribute("qwe"), ret)

    def test_get_string_set_attribute(self):
        # given
        backend = NeptuneBackendMock()
        exp = backend.create_run(self.project_uuid)
        backend.execute_operations(exp.id, [AddStrings(["x"], {"abcx", "qwe"})])

        # when
        ret = backend.get_string_set_attribute(exp.id, ["x"])

        # then
        self.assertEqual(StringSetAttribute({"abcx", "qwe"}), ret)

    def test_get_string_series_values(self):
        # given
        backend = NeptuneBackendMock()
        exp = backend.create_run(self.project_uuid)
        backend.execute_operations(
            exp.id,
            [
                LogStrings(
                    ["x"],
                    [
                        LogStrings.ValueType("adf", None, time()),
                        LogStrings.ValueType("sdg", None, time()),
                    ],
                )
            ],
        )
        backend.execute_operations(
            exp.id,
            [
                LogStrings(
                    ["x"],
                    [
                        LogStrings.ValueType("dfh", None, time()),
                        LogStrings.ValueType("qwe", None, time()),
                    ],
                )
            ],
        )

        # when
        ret = backend.get_string_series_values(exp.id, ["x"], limit=100, offset=0)

        # then
        self.assertEqual(
            StringSeriesValues(
                4,
                [
                    StringPointValue(timestampMillis=42342, step=0, value="adf"),
                    StringPointValue(timestampMillis=42342, step=1, value="sdg"),
                    StringPointValue(timestampMillis=42342, step=2, value="dfh"),
                    StringPointValue(timestampMillis=42342, step=3, value="qwe"),
                ],
            ),
            ret,
        )

    def test_get_float_series_values(self):
        # given
        backend = NeptuneBackendMock()
        exp = backend.create_run(self.project_uuid)
        backend.execute_operations(
            exp.id,
            [
                LogFloats(
                    ["x"],
                    [
                        LogFloats.ValueType(5, None, time()),
                        LogFloats.ValueType(3, None, time()),
                    ],
                )
            ],
        )
        backend.execute_operations(
            exp.id,
            [
                LogFloats(
                    ["x"],
                    [
                        LogFloats.ValueType(2, None, time()),
                        LogFloats.ValueType(9, None, time()),
                    ],
                )
            ],
        )

        # when
        ret = backend.get_float_series_values(exp.id, ["x"], limit=100, offset=0)

        # then
        self.assertEqual(
            FloatSeriesValues(
                4,
                [
                    FloatPointValue(timestampMillis=42342, step=0, value=5),
                    FloatPointValue(timestampMillis=42342, step=1, value=3),
                    FloatPointValue(timestampMillis=42342, step=2, value=2),
                    FloatPointValue(timestampMillis=42342, step=3, value=9),
                ],
            ),
            ret,
        )

    def test_get_float_attribute_wrong_type(self):
        # given
        backend = NeptuneBackendMock()
        exp = backend.create_run(self.project_uuid)
        backend.execute_operations(exp.id, [AssignString(["x"], "abc")])

        # then
        with self.assertRaises(MetadataInconsistency):
            backend.get_float_series_attribute(exp.id, ["x"])

    def test_get_string_attribute_wrong_type(self):
        # given
        backend = NeptuneBackendMock()
        exp = backend.create_run(self.project_uuid)
        backend.execute_operations(exp.id, [AssignFloat(["x"], 5)])

        # then
        with self.assertRaises(MetadataInconsistency):
            backend.get_string_attribute(exp.id, ["x"])

    def test_get_datetime_attribute_wrong_type(self):
        # given
        backend = NeptuneBackendMock()
        exp = backend.create_run(self.project_uuid)
        backend.execute_operations(exp.id, [AssignString(["x"], "abc")])

        # then
        with self.assertRaises(MetadataInconsistency):
            backend.get_datetime_attribute(exp.id, ["x"])

    def test_get_float_series_attribute_wrong_type(self):
        # given
        backend = NeptuneBackendMock()
        exp = backend.create_run(self.project_uuid)
        backend.execute_operations(exp.id, [AssignString(["x"], "abc")])

        # then
        with self.assertRaises(MetadataInconsistency):
            backend.get_float_series_attribute(exp.id, ["x"])

    def test_get_string_series_attribute_wrong_type(self):
        # given
        backend = NeptuneBackendMock()
        exp = backend.create_run(self.project_uuid)
        backend.execute_operations(exp.id, [AssignString(["x"], "abc")])

        # then
        with self.assertRaises(MetadataInconsistency):
            backend.get_string_series_attribute(exp.id, ["x"])

    def test_get_string_set_attribute_wrong_type(self):
        # given
        backend = NeptuneBackendMock()
        exp = backend.create_run(self.project_uuid)
        backend.execute_operations(exp.id, [AssignString(["x"], "abc")])

        # then
        with self.assertRaises(MetadataInconsistency):
            backend.get_string_set_attribute(exp.id, ["x"])
