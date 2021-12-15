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
from random import randint
from time import time

from neptune.new.exceptions import (
    MetadataInconsistency,
    ProjectUUIDNotFound,
    RunUUIDNotFound,
)
from neptune.new.internal.backends.api_model import (
    DatetimeAttribute,
    FloatAttribute,
    FloatPointValue,
    FloatSeriesAttribute,
    FloatSeriesValues,
    StringAttribute,
    StringPointValue,
    StringSeriesAttribute,
    StringSeriesValues,
    StringSetAttribute,
)
from neptune.new.internal.backends.neptune_backend_mock import NeptuneBackendMock
from neptune.new.internal.container_type import ContainerType
from neptune.new.internal.operation import (
    AddStrings,
    AssignDatetime,
    AssignFloat,
    AssignString,
    LogFloats,
    LogStrings,
)
from tests.neptune.random_utils import a_string


class TestNeptuneBackendMock(unittest.TestCase):
    # pylint:disable=protected-access

    def setUp(self) -> None:
        self.backend = NeptuneBackendMock()
        self.exp = self.backend.create_run(self.backend._project_id)
        self.ids_with_types = [
            (self.exp.id, ContainerType.RUN),
            (self.backend._project_id, ContainerType.PROJECT),
        ]

    def test_get_float_attribute(self):
        for container_id, container_type in self.ids_with_types:
            with self.subTest(f"For containerType: {container_type}"):
                # given
                digit = randint(1, 10 ** 4)
                self.backend.execute_operations(
                    container_id, container_type, operations=[AssignFloat(["x"], digit)]
                )

                # when
                ret = self.backend.get_float_attribute(
                    container_id, container_type, path=["x"]
                )

                # then
                self.assertEqual(FloatAttribute(digit), ret)

    def test_get_string_attribute(self):
        for container_id, container_type in self.ids_with_types:
            with self.subTest(f"For containerType: {container_type}"):
                # given
                text = a_string()
                self.backend.execute_operations(
                    container_id, container_type, operations=[AssignString(["x"], text)]
                )

                # when
                ret = self.backend.get_string_attribute(
                    container_id, container_type, path=["x"]
                )

                # then
                self.assertEqual(StringAttribute(text), ret)

    def test_get_datetime_attribute(self):
        for container_id, container_type in self.ids_with_types:
            with self.subTest(f"For containerType: {container_type}"):
                # given
                now = datetime.datetime.now()
                now = now.replace(microsecond=1000 * int(now.microsecond / 1000))
                self.backend.execute_operations(
                    container_id, container_type, [AssignDatetime(["x"], now)]
                )

                # when
                ret = self.backend.get_datetime_attribute(
                    container_id, container_type, ["x"]
                )

                # then
                self.assertEqual(DatetimeAttribute(now), ret)

    def test_get_float_series_attribute(self):
        # given
        for container_id, container_type in self.ids_with_types:
            with self.subTest(f"For containerType: {container_type}"):
                self.backend.execute_operations(
                    container_id,
                    container_type,
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
                self.backend.execute_operations(
                    container_id,
                    container_type,
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
                ret = self.backend.get_float_series_attribute(
                    container_id, container_type, ["x"]
                )

                # then
                self.assertEqual(FloatSeriesAttribute(9), ret)

    def test_get_string_series_attribute(self):
        # given
        for container_id, container_type in self.ids_with_types:
            with self.subTest(f"For containerType: {container_type}"):
                self.backend.execute_operations(
                    container_id,
                    container_type,
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
                self.backend.execute_operations(
                    container_id,
                    container_type,
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
                ret = self.backend.get_string_series_attribute(
                    container_id, container_type, ["x"]
                )

                # then
                self.assertEqual(StringSeriesAttribute("qwe"), ret)

    def test_get_string_set_attribute(self):
        # given
        for container_id, container_type in self.ids_with_types:
            with self.subTest(f"For containerType: {container_type}"):
                self.backend.execute_operations(
                    container_id, container_type, [AddStrings(["x"], {"abcx", "qwe"})]
                )

                # when
                ret = self.backend.get_string_set_attribute(
                    container_id, container_type, ["x"]
                )

                # then
        self.assertEqual(StringSetAttribute({"abcx", "qwe"}), ret)

    def test_get_string_series_values(self):
        # given
        for container_id, container_type in self.ids_with_types:
            with self.subTest(f"For containerType: {container_type}"):
                self.backend.execute_operations(
                    container_id,
                    container_type,
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
                self.backend.execute_operations(
                    container_id,
                    container_type,
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
                ret = self.backend.get_string_series_values(
                    container_id, container_type, path=["x"], limit=100, offset=0
                )

                # then
                self.assertEqual(
                    StringSeriesValues(
                        4,
                        [
                            StringPointValue(
                                timestampMillis=42342, step=0, value="adf"
                            ),
                            StringPointValue(
                                timestampMillis=42342, step=1, value="sdg"
                            ),
                            StringPointValue(
                                timestampMillis=42342, step=2, value="dfh"
                            ),
                            StringPointValue(
                                timestampMillis=42342, step=3, value="qwe"
                            ),
                        ],
                    ),
                    ret,
                )

    def test_get_float_series_values(self):
        # given
        for container_id, container_type in self.ids_with_types:
            with self.subTest(f"For containerType: {container_type}"):
                self.backend.execute_operations(
                    container_id,
                    container_type,
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
                self.backend.execute_operations(
                    container_id,
                    container_type,
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
                ret = self.backend.get_float_series_values(
                    container_id, container_type, path=["x"], limit=100, offset=0
                )

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
        for container_id, container_type in self.ids_with_types:
            with self.subTest(f"For containerType: {container_type}"):
                self.backend.execute_operations(
                    container_id, container_type, [AssignString(["x"], "abc")]
                )

                # then
                with self.assertRaises(MetadataInconsistency):
                    self.backend.get_float_series_attribute(
                        container_id, container_type, ["x"]
                    )

    def test_get_string_attribute_wrong_type(self):
        # given
        for container_id, container_type in self.ids_with_types:
            with self.subTest(f"For containerType: {container_type}"):
                self.backend.execute_operations(
                    container_id, container_type, [AssignFloat(["x"], 5)]
                )

                # then
                with self.assertRaises(MetadataInconsistency):
                    self.backend.get_string_attribute(
                        container_id, container_type, ["x"]
                    )

    def test_get_datetime_attribute_wrong_type(self):
        # given
        for container_id, container_type in self.ids_with_types:
            with self.subTest(f"For containerType: {container_type}"):
                self.backend.execute_operations(
                    container_id, container_type, [AssignString(["x"], "abc")]
                )

                # then
                with self.assertRaises(MetadataInconsistency):
                    self.backend.get_datetime_attribute(
                        container_id, container_type, ["x"]
                    )

    def test_get_string_series_attribute_wrong_type(self):
        # given
        for container_id, container_type in self.ids_with_types:
            with self.subTest(f"For containerType: {container_type}"):
                self.backend.execute_operations(
                    container_id, container_type, [AssignString(["x"], "abc")]
                )

                # then
                with self.assertRaises(MetadataInconsistency):
                    self.backend.get_string_series_attribute(
                        container_id, container_type, ["x"]
                    )

    def test_get_string_set_attribute_wrong_type(self):
        # given
        for container_id, container_type in self.ids_with_types:
            with self.subTest(f"For containerType: {container_type}"):
                self.backend.execute_operations(
                    container_id, container_type, [AssignString(["x"], "abc")]
                )

                # then
                with self.assertRaises(MetadataInconsistency):
                    self.backend.get_string_set_attribute(
                        container_id, container_type, ["x"]
                    )

    def test_container_not_found(self):
        # given
        ids_with_types_and_exceptions = [
            (container_id, container_type, exception_type)
            for (container_id, container_type), exception_type in zip(
                self.ids_with_types, [RunUUIDNotFound, ProjectUUIDNotFound]
            )
        ]
        for (
            container_id,
            container_type,
            exception_type,
        ) in ids_with_types_and_exceptions:
            with self.subTest(f"For containerType: {container_type}"):
                self.backend.execute_operations(
                    container_id, container_type, [AssignString(["x"], "abc")]
                )

                # then
                with self.assertRaises(exception_type):
                    self.backend.get_float_series_attribute(
                        str(uuid.uuid4()), container_type, ["x"]
                    )
