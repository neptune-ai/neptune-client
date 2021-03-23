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

from neptune.new.internal.operation import ClearStringSet, AddStrings, RemoveStrings
from neptune.new.attributes.sets.string_set import StringSet, StringSetVal

from tests.neptune.new.attributes.test_attribute_base import TestAttributeBase


class TestStringSet(TestAttributeBase):

    def test_assign(self):
        value = StringSetVal(["ert", "qwe"])
        expected = {"ert", "qwe"}

        processor = MagicMock()
        exp, path, wait = self._create_run(processor), self._random_path(), self._random_wait()
        var = StringSet(exp, path)
        var.assign(value, wait=wait)
        self.assertEqual(2, processor.enqueue_operation.call_count)
        processor.enqueue_operation.assert_has_calls([
            call(ClearStringSet(path), False),
            call(AddStrings(path, expected), wait)
        ])

    def test_assign_empty(self):
        processor = MagicMock()
        exp, path, wait = self._create_run(processor), self._random_path(), self._random_wait()
        var = StringSet(exp, path)
        var.assign(StringSetVal([]), wait=wait)
        processor.enqueue_operation.assert_called_once_with(ClearStringSet(path), wait)

    def test_assign_type_error(self):
        values = [{5.}, {"text"}, {}, [5.], ["text"], [], 55, "string", None]
        for value in values:
            with self.assertRaises(TypeError):
                StringSet(MagicMock(), MagicMock()).assign(value)

    def test_add(self):
        processor = MagicMock()
        exp, path, wait = self._create_run(processor), self._random_path(), self._random_wait()
        var = StringSet(exp, path)
        var.add(["a", "bb", "ccc"], wait=wait)
        processor.enqueue_operation.assert_called_once_with(AddStrings(path, {"a", "bb", "ccc"}), wait)

    def test_add_single_value(self):
        processor = MagicMock()
        exp, path, wait = self._create_run(processor), self._random_path(), self._random_wait()
        var = StringSet(exp, path)
        var.add("ccc", wait=wait)
        processor.enqueue_operation.assert_called_once_with(AddStrings(path, {"ccc"}), wait)

    def test_remove(self):
        processor = MagicMock()
        exp, path, wait = self._create_run(processor), self._random_path(), self._random_wait()
        var = StringSet(exp, path)
        var.remove(["a", "bb", "ccc"], wait=wait)
        processor.enqueue_operation.assert_called_once_with(RemoveStrings(path, {"a", "bb", "ccc"}), wait)

    def test_remove_single_value(self):
        processor = MagicMock()
        exp, path, wait = self._create_run(processor), self._random_path(), self._random_wait()
        var = StringSet(exp, path)
        var.remove("bb", wait=wait)
        processor.enqueue_operation.assert_called_once_with(RemoveStrings(path, {"bb"}), wait)

    def test_clear(self):
        processor = MagicMock()
        exp, path, wait = self._create_run(processor), self._random_path(), self._random_wait()
        var = StringSet(exp, path)
        var.clear(wait=wait)
        processor.enqueue_operation.assert_called_once_with(ClearStringSet(path), wait)

    def test_get(self):
        exp, path = self._create_run(), self._random_path()
        var = StringSet(exp, path)
        var.add(["abc", "xyz"])
        var.remove(["abc"])
        var.add(["hej", "lol"])
        self.assertEqual({"xyz", "hej", "lol"}, var.fetch())
