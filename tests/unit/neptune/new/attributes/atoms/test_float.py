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
import pytest
from mock import (
    MagicMock,
    patch,
)

from neptune import init_run
from neptune.attributes.atoms.float import (
    Float,
    FloatVal,
)
from neptune.common.warnings import NeptuneUnsupportedValue
from neptune.exceptions import MetadataInconsistency
from neptune.internal.operation import AssignFloat
from tests.unit.neptune.new.attributes.test_attribute_base import TestAttributeBase


class TestFloat(TestAttributeBase):
    @patch("neptune.metadata_containers.metadata_container.get_operation_processor")
    def test_assign(self, get_operation_processor):
        processor = MagicMock()
        get_operation_processor.return_value = processor

        value_and_expected = [
            (13, 13),
            (15.3, 15.3),
            (FloatVal(17), 17),
            (FloatVal(17.5), 17.5),
        ]

        for value, expected in value_and_expected:
            path, wait = (
                self._random_path(),
                self._random_wait(),
            )
            with self._exp() as run:
                var = Float(run, path)
                var.assign(value, wait=wait)
                processor.enqueue_operation.assert_called_with(AssignFloat(path, expected), wait=wait)

    def test_assign_type_error(self):
        values = ["string", None]
        for value in values:
            with self.assertRaises(Exception):
                Float(MagicMock(), MagicMock()).assign(value)

    def test_get(self):
        with self._exp() as run:
            var = Float(run, self._random_path())
            var.assign(5)
            self.assertEqual(5, var.fetch())

    def test_float_warnings(self):
        run = init_run(mode="debug")
        with pytest.warns(NeptuneUnsupportedValue):
            run["infinity"] = float("inf")
            run["neg-infinity"] = float("-inf")
            run["nan"] = float("nan")

        with pytest.raises(MetadataInconsistency):
            run["infinity"].fetch()

        with pytest.raises(MetadataInconsistency):
            run["neg-infinity"].fetch()

        with pytest.raises(MetadataInconsistency):
            run["nan"].fetch()

        run.stop()
