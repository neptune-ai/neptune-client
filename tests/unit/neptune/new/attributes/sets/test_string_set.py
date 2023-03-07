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
    call,
    patch,
)

from neptune.attributes.sets.string_set import (
    StringSet,
    StringSetVal,
)
from neptune.internal.operation import (
    AddStrings,
    ClearStringSet,
    RemoveStrings,
)
from tests.unit.neptune.new.attributes.test_attribute_base import TestAttributeBase


class TestStringSet(TestAttributeBase):
    @patch("neptune.metadata_containers.metadata_container.get_operation_processor")
    def test_assign(self, get_operation_processor):
        value = StringSetVal(["ert", "qwe"])
        expected = {"ert", "qwe"}

        processor = MagicMock()
        get_operation_processor.return_value = processor

        with self._exp() as exp:
            path, wait = (
                self._random_path(),
                self._random_wait(),
            )
            var = StringSet(exp, path)
            var.assign(value, wait=wait)
            processor.enqueue_operation.assert_has_calls(
                [call(ClearStringSet(path), wait=False), call(AddStrings(path, expected), wait=wait)]
            )

    @patch("neptune.metadata_containers.metadata_container.get_operation_processor")
    def test_assign_empty(self, get_operation_processor):
        processor = MagicMock()
        get_operation_processor.return_value = processor

        with self._exp() as exp:
            path, wait = (
                self._random_path(),
                self._random_wait(),
            )
            var = StringSet(exp, path)
            var.assign(StringSetVal([]), wait=wait)
            processor.enqueue_operation.assert_called_with(ClearStringSet(path), wait=wait)

    def test_assign_type_error(self):
        values = [{5.0}, {"text"}, {}, [5.0], ["text"], [], 55, "string", None]
        for value in values:
            with self.assertRaises(TypeError):
                StringSet(MagicMock(), MagicMock()).assign(value)

    @patch("neptune.metadata_containers.metadata_container.get_operation_processor")
    def test_add(self, get_operation_processor):
        processor = MagicMock()
        get_operation_processor.return_value = processor

        with self._exp() as exp:
            path, wait = (
                self._random_path(),
                self._random_wait(),
            )
            var = StringSet(exp, path)
            var.add(["a", "bb", "ccc"], wait=wait)
            processor.enqueue_operation.assert_called_with(AddStrings(path, {"a", "bb", "ccc"}), wait=wait)

    @patch("neptune.metadata_containers.metadata_container.get_operation_processor")
    def test_add_single_value(self, get_operation_processor):
        processor = MagicMock()
        get_operation_processor.return_value = processor

        with self._exp() as exp:
            path, wait = (
                self._random_path(),
                self._random_wait(),
            )
            var = StringSet(exp, path)
            var.add("ccc", wait=wait)
            processor.enqueue_operation.assert_called_with(AddStrings(path, {"ccc"}), wait=wait)

    @patch("neptune.metadata_containers.metadata_container.get_operation_processor")
    def test_remove(self, get_operation_processor):
        processor = MagicMock()
        get_operation_processor.return_value = processor

        with self._exp() as exp:
            path, wait = (
                self._random_path(),
                self._random_wait(),
            )
            var = StringSet(exp, path)
            var.remove(["a", "bb", "ccc"], wait=wait)
            processor.enqueue_operation.assert_called_with(RemoveStrings(path, {"a", "bb", "ccc"}), wait=wait)

    @patch("neptune.metadata_containers.metadata_container.get_operation_processor")
    def test_remove_single_value(self, get_operation_processor):
        processor = MagicMock()
        get_operation_processor.return_value = processor

        with self._exp() as exp:
            path, wait = (
                self._random_path(),
                self._random_wait(),
            )
            var = StringSet(exp, path)
            var.remove("bb", wait=wait)
            processor.enqueue_operation.assert_called_with(RemoveStrings(path, {"bb"}), wait=wait)

    @patch("neptune.metadata_containers.metadata_container.get_operation_processor")
    def test_clear(self, get_operation_processor):
        processor = MagicMock()
        get_operation_processor.return_value = processor

        with self._exp() as exp:
            path, wait = (
                self._random_path(),
                self._random_wait(),
            )
            var = StringSet(exp, path)
            var.clear(wait=wait)
            processor.enqueue_operation.assert_called_with(ClearStringSet(path), wait=wait)

    def test_get(self):
        with self._exp() as exp:
            var = StringSet(exp, self._random_path())
            var.add(["abc", "xyz"])
            var.remove(["abc"])
            var.add(["hej", "lol"])
            self.assertEqual({"xyz", "hej", "lol"}, var.fetch())
