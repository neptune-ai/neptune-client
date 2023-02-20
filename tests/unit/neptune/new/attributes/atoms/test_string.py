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
from mock import (
    MagicMock,
    patch,
)

from neptune.attributes.atoms.string import (
    String,
    StringVal,
)
from neptune.internal.operation import AssignString
from tests.unit.neptune.new.attributes.test_attribute_base import TestAttributeBase


class TestString(TestAttributeBase):
    @patch("neptune.metadata_containers.metadata_container.get_operation_processor")
    def test_assign(self, get_operation_processor):
        processor = MagicMock()
        get_operation_processor.return_value = processor

        value_and_expected = [
            ("qwertyu", "qwertyu"),
            (StringVal("Some string"), "Some string"),
        ]

        for value, expected in value_and_expected:
            path, wait = (
                self._random_path(),
                self._random_wait(),
            )
            with self._exp() as exp:
                var = String(exp, path)
                var.assign(value, wait=wait)
                processor.enqueue_operation.assert_called_with(AssignString(path, expected), wait=wait)

    def test_get(self):
        with self._exp() as exp:
            var = String(exp, self._random_path())
            var.assign("adfh")
            self.assertEqual("adfh", var.fetch())
