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
import random
import string
import unittest
import uuid
from pathlib import Path
from time import time

from freezegun import freeze_time

from neptune.api.models import (
    DateTimeField,
    FloatField,
    FloatPointValue,
    FloatSeriesField,
    FloatSeriesValues,
    StringField,
    StringPointValue,
    StringSeriesField,
    StringSeriesValues,
    StringSetField,
)
from neptune.core.components.operation_storage import OperationStorage
from neptune.exceptions import (
    ContainerUUIDNotFound,
    MetadataInconsistency,
)
from neptune.internal.backends.neptune_backend_mock import NeptuneBackendMock
from neptune.internal.container_type import ContainerType
from neptune.internal.operation import (
    AddStrings,
    AssignDatetime,
    AssignFloat,
    AssignString,
    LogFloats,
    LogStrings,
)


def a_string() -> str:
    char_set = string.ascii_letters
    return "".join(random.sample(char_set * 10, 10))


class TestNeptuneBackendMock(unittest.TestCase):
    def setUp(self) -> None:
        self.backend = NeptuneBackendMock()
        project_id = self.backend._project_id
        exp = self.backend.create_run(project_id=project_id)
        model = self.backend.create_model(
            project_id=project_id,
            key="MOD",
        )
        model_version = self.backend.create_model_version(project_id=project_id, model_id=model.id)
        self.ids_with_types = [
            (self.backend._project_id, ContainerType.PROJECT),
            (exp.id, ContainerType.RUN),
            (model.id, ContainerType.MODEL),
            (model_version.id, ContainerType.MODEL_VERSION),
        ]
        self.dummy_operation_storage = OperationStorage(Path("./tests/dummy_storage"))

    def test_get_float_attribute(self):
        for container_id, container_type in self.ids_with_types:
            with self.subTest(f"For containerType: {container_type}"):
                # given
                digit = random.randint(1, 10**4)
                path = ["x"]
                self.backend.execute_operations(
                    container_id,
                    container_type,
                    operations=[AssignFloat(path, digit)],
                    operation_storage=self.dummy_operation_storage,
                )

                # when
                ret = self.backend.get_float_attribute(container_id, container_type, path=path)

                # then
                self.assertEqual(FloatField(path="x", value=digit), ret)

    def test_get_string_attribute(self):
        for container_id, container_type in self.ids_with_types:
            with self.subTest(f"For containerType: {container_type}"):
                # given
                text = a_string()
                path = ["x"]
                self.backend.execute_operations(
                    container_id,
                    container_type,
                    operations=[AssignString(path, text)],
                    operation_storage=self.dummy_operation_storage,
                )

                # when
                ret = self.backend.get_string_attribute(container_id, container_type, path=path)

                # then
                self.assertEqual(StringField(path="x", value=text), ret)

    def test_get_datetime_attribute(self):
        for container_id, container_type in self.ids_with_types:
            with self.subTest(f"For containerType: {container_type}"):
                # given
                now = datetime.datetime.now()
                now = now.replace(microsecond=1000 * int(now.microsecond / 1000))
                path = ["x"]

                # and
                self.backend.execute_operations(
                    container_id,
                    container_type,
                    [AssignDatetime(path, now)],
                    operation_storage=self.dummy_operation_storage,
                )

                # when
                ret = self.backend.get_datetime_attribute(container_id, container_type, path)

                # then
                self.assertEqual(DateTimeField(path="x", value=now), ret)

    def test_get_float_series_attribute(self):
        # given
        path = ["x"]

        # and
        for container_id, container_type in self.ids_with_types:
            with self.subTest(f"For containerType: {container_type}"):
                self.backend.execute_operations(
                    container_id,
                    container_type,
                    [
                        LogFloats(
                            path,
                            [
                                LogFloats.ValueType(5, None, time()),
                                LogFloats.ValueType(3, None, time()),
                            ],
                        )
                    ],
                    operation_storage=self.dummy_operation_storage,
                )
                self.backend.execute_operations(
                    container_id,
                    container_type,
                    [
                        LogFloats(
                            path,
                            [
                                LogFloats.ValueType(2, None, time()),
                                LogFloats.ValueType(9, None, time()),
                            ],
                        )
                    ],
                    operation_storage=self.dummy_operation_storage,
                )

                # when
                ret = self.backend.get_float_series_attribute(container_id, container_type, path)

                # then
                self.assertEqual(FloatSeriesField(last=9, path="x"), ret)

    def test_get_string_series_attribute(self):
        # given
        path = ["x"]

        # and
        for container_id, container_type in self.ids_with_types:
            with self.subTest(f"For containerType: {container_type}"):
                self.backend.execute_operations(
                    container_id,
                    container_type,
                    [
                        LogStrings(
                            path,
                            [
                                LogStrings.ValueType("adf", None, time()),
                                LogStrings.ValueType("sdg", None, time()),
                            ],
                        )
                    ],
                    operation_storage=self.dummy_operation_storage,
                )
                self.backend.execute_operations(
                    container_id,
                    container_type,
                    [
                        LogStrings(
                            path,
                            [
                                LogStrings.ValueType("dfh", None, time()),
                                LogStrings.ValueType("qwe", None, time()),
                            ],
                        )
                    ],
                    operation_storage=self.dummy_operation_storage,
                )

                # when
                ret = self.backend.get_string_series_attribute(container_id, container_type, path)

                # then
                self.assertEqual(StringSeriesField(last="qwe", path="x"), ret)

    def test_get_string_set_attribute(self):
        # given
        path = ["x"]

        # and
        for container_id, container_type in self.ids_with_types:
            with self.subTest(f"For containerType: {container_type}"):
                self.backend.execute_operations(
                    container_id,
                    container_type,
                    [AddStrings(path, {"abcx", "qwe"})],
                    operation_storage=self.dummy_operation_storage,
                )

                # when
                ret = self.backend.get_string_set_attribute(container_id, container_type, path)

                # then
        self.assertEqual(StringSetField(values={"abcx", "qwe"}, path="x"), ret)

    @freeze_time("2024-01-01T12:34:56.123456Z")
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
                    operation_storage=self.dummy_operation_storage,
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
                    operation_storage=self.dummy_operation_storage,
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
                                timestamp=datetime.datetime(2024, 1, 1, 12, 34, 56, 123456), step=0, value="adf"
                            ),
                            StringPointValue(
                                timestamp=datetime.datetime(2024, 1, 1, 12, 34, 56, 123456), step=1, value="sdg"
                            ),
                            StringPointValue(
                                timestamp=datetime.datetime(2024, 1, 1, 12, 34, 56, 123456), step=2, value="dfh"
                            ),
                            StringPointValue(
                                timestamp=datetime.datetime(2024, 1, 1, 12, 34, 56, 123456), step=3, value="qwe"
                            ),
                        ],
                    ),
                    ret,
                )

    @freeze_time("2024-01-01T12:34:56.123456Z")
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
                    operation_storage=self.dummy_operation_storage,
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
                    operation_storage=self.dummy_operation_storage,
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
                            FloatPointValue(
                                timestamp=datetime.datetime(2024, 1, 1, 12, 34, 56, 123456), step=0, value=5
                            ),
                            FloatPointValue(
                                timestamp=datetime.datetime(2024, 1, 1, 12, 34, 56, 123456), step=1, value=3
                            ),
                            FloatPointValue(
                                timestamp=datetime.datetime(2024, 1, 1, 12, 34, 56, 123456), step=2, value=2
                            ),
                            FloatPointValue(
                                timestamp=datetime.datetime(2024, 1, 1, 12, 34, 56, 123456), step=3, value=9
                            ),
                        ],
                    ),
                    ret,
                )

    def test_get_float_attribute_wrong_type(self):
        # given
        for container_id, container_type in self.ids_with_types:
            with self.subTest(f"For containerType: {container_type}"):
                self.backend.execute_operations(
                    container_id,
                    container_type,
                    [AssignString(["x"], "abc")],
                    operation_storage=self.dummy_operation_storage,
                )

                # then
                with self.assertRaises(MetadataInconsistency):
                    self.backend.get_float_series_attribute(container_id, container_type, ["x"])

    def test_get_string_attribute_wrong_type(self):
        # given
        for container_id, container_type in self.ids_with_types:
            with self.subTest(f"For containerType: {container_type}"):
                self.backend.execute_operations(
                    container_id,
                    container_type,
                    [AssignFloat(["x"], 5)],
                    operation_storage=self.dummy_operation_storage,
                )

                # then
                with self.assertRaises(MetadataInconsistency):
                    self.backend.get_string_attribute(container_id, container_type, ["x"])

    def test_get_datetime_attribute_wrong_type(self):
        # given
        for container_id, container_type in self.ids_with_types:
            with self.subTest(f"For containerType: {container_type}"):
                self.backend.execute_operations(
                    container_id,
                    container_type,
                    [AssignString(["x"], "abc")],
                    operation_storage=self.dummy_operation_storage,
                )

                # then
                with self.assertRaises(MetadataInconsistency):
                    self.backend.get_datetime_attribute(container_id, container_type, ["x"])

    def test_get_string_series_attribute_wrong_type(self):
        # given
        for container_id, container_type in self.ids_with_types:
            with self.subTest(f"For containerType: {container_type}"):
                self.backend.execute_operations(
                    container_id,
                    container_type,
                    [AssignString(["x"], "abc")],
                    operation_storage=self.dummy_operation_storage,
                )

                # then
                with self.assertRaises(MetadataInconsistency):
                    self.backend.get_string_series_attribute(container_id, container_type, ["x"])

    def test_get_string_set_attribute_wrong_type(self):
        # given
        for container_id, container_type in self.ids_with_types:
            with self.subTest(f"For containerType: {container_type}"):
                self.backend.execute_operations(
                    container_id,
                    container_type,
                    [AssignString(["x"], "abc")],
                    operation_storage=self.dummy_operation_storage,
                )

                # then
                with self.assertRaises(MetadataInconsistency):
                    self.backend.get_string_set_attribute(container_id, container_type, ["x"])

    def test_container_not_found(self):
        # given
        for container_id, container_type in self.ids_with_types:
            with self.subTest(f"For containerType: {container_type}"):
                self.backend.execute_operations(
                    container_id,
                    container_type,
                    [AssignString(["x"], "abc")],
                    operation_storage=self.dummy_operation_storage,
                )

                # then
                with self.assertRaises(ContainerUUIDNotFound):
                    self.backend.get_float_series_attribute(str(uuid.uuid4()), container_type, ["x"])
