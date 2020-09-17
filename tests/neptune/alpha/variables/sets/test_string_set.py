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
from mock import MagicMock, call

from neptune.alpha.exceptions import MetadataInconsistency
from neptune.alpha.internal.operation import ClearStringSet, AddStrings, RemoveStrings
from neptune.alpha.variables.sets.string_set import StringSet, StringSetVal

from tests.neptune.alpha.variables.test_variable_base import TestVariableBase


class TestStringSet(TestVariableBase):

    def test_assign(self):
        value = StringSetVal(["ert", "qwe"])
        expected = {"ert", "qwe"}

        backend, processor = MagicMock(), MagicMock()
        exp, path, wait = self._create_experiment(backend, processor), self._random_path(), self._random_wait()
        var = StringSet(exp, path)
        var.assign(value, wait=wait)
        self.assertEqual(2, processor.enqueue_operation.call_count)
        processor.enqueue_operation.assert_has_calls([
            call(ClearStringSet(path), False),
            call(AddStrings(path, expected), wait)
        ])

    def test_assign_empty(self):
        backend, processor = MagicMock(), MagicMock()
        exp, path, wait = self._create_experiment(backend, processor), self._random_path(), self._random_wait()
        var = StringSet(exp, path)
        var.assign(StringSetVal([]), wait=wait)
        processor.enqueue_operation.assert_called_once_with(ClearStringSet(path), wait)

    def test_assign_type_error(self):
        values = [{5.}, {"text"}, {}, [5.], ["text"], [], 55, "string", None]
        for value in values:
            with self.assertRaises(TypeError):
                StringSet(MagicMock(), MagicMock()).assign(value)

    def test_add(self):
        backend, processor = MagicMock(), MagicMock()
        exp, path, wait = self._create_experiment(backend, processor), self._random_path(), self._random_wait()
        var = StringSet(exp, path)
        var.add(["a", "bb", "ccc"], wait=wait)
        processor.enqueue_operation.assert_called_once_with(AddStrings(path, {"a", "bb", "ccc"}), wait)

    def test_remove(self):
        backend, processor = MagicMock(), MagicMock()
        exp, path, wait = self._create_experiment(backend, processor), self._random_path(), self._random_wait()
        var = StringSet(exp, path)
        var.remove(["a", "bb", "ccc"], wait=wait)
        processor.enqueue_operation.assert_called_once_with(RemoveStrings(path, {"a", "bb", "ccc"}), wait)

    def test_clear(self):
        backend, processor = MagicMock(), MagicMock()
        exp, path, wait = self._create_experiment(backend, processor), self._random_path(), self._random_wait()
        var = StringSet(exp, path)
        var.clear(wait=wait)
        processor.enqueue_operation.assert_called_once_with(ClearStringSet(path), wait)

    def test_get(self):
        backend, processor = MagicMock(), MagicMock()
        exp, path = self._create_experiment(backend, processor), self._random_path()
        var = StringSet(exp, path)
        backend.get_attribute.return_value = StringSetVal(["text", "str"])
        self.assertEqual({"text", "str"}, var.get())

    def test_get_wrong_type(self):
        backend, processor = MagicMock(), MagicMock()
        exp, path = self._create_experiment(backend, processor), self._random_path()
        var = StringSet(exp, path)
        backend.get_attribute.return_value = {"x"}
        with self.assertRaises(MetadataInconsistency):
            var.get()
