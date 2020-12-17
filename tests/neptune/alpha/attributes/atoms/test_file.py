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
import unittest
from io import StringIO, BytesIO

from mock import MagicMock

from neptune.alpha.internal.operation import UploadFile, UploadFileSet
from neptune.alpha.attributes.atoms.file import File, FileVal
from neptune.alpha.attributes.file_set import FileSet, FileSetVal

from tests.neptune.alpha.attributes.test_attribute_base import TestAttributeBase
from neptune.utils import IS_WINDOWS


class TestFile(TestAttributeBase):

    @unittest.skipIf(IS_WINDOWS, "Windows behaves strangely")
    def test_assign(self):
        text_stream = StringIO("Some text stream")
        binary_stream = BytesIO(b"Some binary stream")
        value_and_expected_params = [
            ("some/path", "path", os.getcwd() + "/some/path", None),
            (FileVal("other/../other/file.txt"), "file.txt", os.getcwd() + "/other/file.txt", None),
            (text_stream, "stream.txt", None, text_stream),
            (binary_stream, "stream.bin", None, binary_stream),
        ]

        for value, expected_file_name, expected_file_path, expected_stream in value_and_expected_params:
            processor = MagicMock()
            exp, path, wait = self._create_experiment(processor), self._random_path(), self._random_wait()
            var = File(exp, path)
            var.assign(value, wait=wait)
            processor.enqueue_operation.assert_called_once_with(
                UploadFile(path, expected_file_name, expected_file_path, expected_stream),
                wait)

    def test_assign_type_error(self):
        values = [55, None, []]
        for value in values:
            with self.assertRaises(TypeError):
                File(MagicMock(), MagicMock()).assign(value)

    @unittest.skipIf(IS_WINDOWS, "Windows behaves strangely")
    def test_save(self):
        value_and_expected = [
            ("some/path", os.getcwd() + "/some/path")
        ]

        for value, expected in value_and_expected:
            processor = MagicMock()
            exp, path, wait = self._create_experiment(processor), self._random_path(), self._random_wait()
            var = File(exp, path)
            var.save(value, wait=wait)
            processor.enqueue_operation.assert_called_once_with(UploadFile(path, os.path.basename(value), expected),
                                                                wait)

    @unittest.skipIf(IS_WINDOWS, "Windows behaves strangely")
    def test_save_files(self):
        value_and_expected = [
            ("some/path/*", [os.getcwd() + "/some/path/*"])
        ]

        for value, expected in value_and_expected:
            processor = MagicMock()
            exp, path, wait = self._create_experiment(processor), self._random_path(), self._random_wait()
            var = FileSet(exp, path)
            var.save_files(value, wait=wait)
            processor.enqueue_operation.assert_called_once_with(UploadFileSet(path, expected, False), wait)

    def test_save_type_error(self):
        values = [55, None, [], FileVal]
        for value in values:
            with self.assertRaises(TypeError):
                File(MagicMock(), MagicMock()).save(value)

    def test_save__files_type_error(self):
        values = [55, None, [55], FileSetVal]
        for value in values:
            with self.assertRaises(TypeError):
                FileSet(MagicMock(), MagicMock()).save_files(value)
