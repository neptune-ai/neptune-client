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
import os

from mock import MagicMock

from neptune.new.internal.operation import UploadFileSet, DeleteFiles
from neptune.new.attributes.file_set import FileSet

from tests.neptune.new.attributes.test_attribute_base import TestAttributeBase


class TestFileSet(TestAttributeBase):

    def test_assign(self):
        globs = ["path1", "dir/", "glob/*.py"]
        expected = [os.path.abspath(glob) for glob in globs]

        processor = MagicMock()
        exp, path, wait = self._create_run(processor), self._random_path(), self._random_wait()
        var = FileSet(exp, path)
        var.assign(globs, wait=wait)
        processor.enqueue_operation.assert_called_once_with(UploadFileSet(path, expected, reset=True), wait)

    def test_upload_files(self):
        globs = ["path1", "dir/", "glob/*.py"]
        processor = MagicMock()
        exp, path, wait = self._create_run(processor), self._random_path(), self._random_wait()
        var = FileSet(exp, path)
        var.upload_files(globs, wait=wait)
        processor.enqueue_operation.assert_called_once_with(
            UploadFileSet(path, [os.path.abspath(glob) for glob in globs], reset=False), wait)

    def test_delete_files(self):
        processor = MagicMock()
        exp, path, wait = self._create_run(processor), self._random_path(), self._random_wait()
        var = FileSet(exp, path)
        var.delete_files(["path1", "dir/"], wait=wait)
        processor.enqueue_operation.assert_called_once_with(DeleteFiles(path, {"path1", "dir/"}), wait)
