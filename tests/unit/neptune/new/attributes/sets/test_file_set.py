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
import os

from mock import (
    MagicMock,
    patch,
)

from neptune.attributes.file_set import FileSet
from neptune.internal.operation import (
    DeleteFiles,
    UploadFileSet,
)
from tests.unit.neptune.new.attributes.test_attribute_base import TestAttributeBase


class TestFileSet(TestAttributeBase):
    @patch("neptune.metadata_containers.metadata_container.get_operation_processor")
    def test_assign(self, get_operation_processor):
        globs = ["path1", "dir/", "glob/*.py"]
        expected = [os.path.abspath(glob) for glob in globs]

        processor = MagicMock()
        get_operation_processor.return_value = processor

        with self._exp() as exp:
            path, wait = (
                self._random_path(),
                self._random_wait(),
            )
            var = FileSet(exp, path)
            var.assign(globs, wait=wait)
            processor.enqueue_operation.assert_called_with(UploadFileSet(path, expected, reset=True), wait=wait)

    @patch("neptune.metadata_containers.metadata_container.get_operation_processor")
    def test_upload_files(self, get_operation_processor):
        globs = ["path1", "dir/", "glob/*.py"]
        processor = MagicMock()
        get_operation_processor.return_value = processor

        with self._exp() as exp:
            path, wait = (
                self._random_path(),
                self._random_wait(),
            )
            var = FileSet(exp, path)
            var.upload_files(globs, wait=wait)
            processor.enqueue_operation.assert_called_with(
                UploadFileSet(path, [os.path.abspath(glob) for glob in globs], reset=False),
                wait=wait,
            )

    @patch("neptune.metadata_containers.metadata_container.get_operation_processor")
    def test_delete_files(self, get_operation_processor):
        processor = MagicMock()
        get_operation_processor.return_value = processor

        with self._exp() as exp:
            path, wait = (
                self._random_path(),
                self._random_wait(),
            )
            var = FileSet(exp, path)
            var.delete_files(["path1", "dir/"], wait=wait)
            processor.enqueue_operation.assert_called_with(DeleteFiles(path, {"path1", "dir/"}), wait=wait)

    @patch("neptune.metadata_containers.metadata_container.get_operation_processor")
    def test_list_fileset_files(self, get_operation_processor):
        processor = MagicMock()
        get_operation_processor.return_value = processor

        with self._exp() as exp:
            path = self._random_path()
            var = FileSet(exp, path)
            file_entries = var.list_fileset_files()

            assert isinstance(file_entries, list)
            assert len(file_entries) == 1
