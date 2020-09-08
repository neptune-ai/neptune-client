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

# pylint: disable=protected-access

from mock import MagicMock
from neptune.exceptions import MetadataInconsistency

from neptune.internal.operation import AssignString

from neptune.variables.atoms.string import String, StringVal
from tests.neptune.variables.test_variable_base import TestVariableBase


class TestString(TestVariableBase):

    def test_assign(self):
        value_and_expected = [
            ("qwertyu", "qwertyu"),
            (StringVal("Some string"), "Some string")
        ]

        for value, expected in value_and_expected:
            backend, processor = MagicMock(), MagicMock()
            exp, path, wait = self._create_experiment(backend, processor), self._random_path(), self._random_wait()
            var = String(exp, path)
            var.assign(value, wait=wait)
            processor.enqueue_operation.assert_called_once_with(AssignString(exp._uuid, path, expected), wait)

    def test_assign_type_error(self):
        values = [55, None]
        for value in values:
            with self.assertRaises(TypeError):
                String(MagicMock(), MagicMock()).assign(value)

    def test_get(self):
        backend, processor = MagicMock(), MagicMock()
        exp, path = self._create_experiment(backend, processor), self._random_path()
        var = String(exp, path)
        backend.get_attribute.return_value = StringVal("text")
        self.assertEqual("text", var.get())

    def test_get_wrong_type(self):
        backend, processor = MagicMock(), MagicMock()
        exp, path = self._create_experiment(backend, processor), self._random_path()
        var = String(exp, path)
        backend.get_attribute.return_value = 5
        with self.assertRaises(MetadataInconsistency):
            var.get()
