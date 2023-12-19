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
import unittest
from io import (
    BytesIO,
    StringIO,
)
from pathlib import Path
from unittest.mock import PropertyMock

from mock import (
    MagicMock,
    patch,
)

from neptune.attributes.atoms.file import (
    File,
    FileVal,
)
from neptune.attributes.file_set import (
    FileSet,
    FileSetVal,
)
from neptune.common.utils import IS_WINDOWS
from neptune.internal.operation import (
    UploadFile,
    UploadFileSet,
)
from neptune.internal.types.file_types import FileType
from tests.e2e.utils import tmp_context
from tests.unit.neptune.new.attributes.test_attribute_base import TestAttributeBase


class TestFile(TestAttributeBase):
    @unittest.skipIf(IS_WINDOWS, "Windows behaves strangely")
    @patch("neptune.metadata_containers.metadata_container.get_operation_processor")
    def test_assign(self, get_operation_processor):
        def get_tmp_uploaded_file_name(tmp_upload_dir):
            """Get tmp file to uploaded from `upload_path`
            - here's assumption that we upload only one file per one path in test"""
            uploaded_files = os.listdir(tmp_upload_dir)
            assert len(uploaded_files) == 1
            return uploaded_files[0]

        a_text = "Some text stream"
        a_binary = b"Some binary stream"
        value_and_operation_factory = [
            (
                FileVal("other/../other/file.txt"),
                lambda attribute_path, _: UploadFile(
                    attribute_path, ext="txt", file_path=os.getcwd() + "/other/file.txt"
                ),
            ),
            (
                FileVal.from_stream(StringIO(a_text)),
                lambda attribute_path, tmp_uploaded_file: UploadFile(
                    attribute_path, ext="txt", tmp_file_name=tmp_uploaded_file
                ),
            ),
            (
                FileVal.from_stream(BytesIO(a_binary)),
                lambda attribute_path, tmp_uploaded_file: UploadFile(
                    attribute_path, ext="bin", tmp_file_name=tmp_uploaded_file
                ),
            ),
        ]

        for value, operation_factory in value_and_operation_factory:
            with tmp_context() as tmp_upload_dir:
                processor = MagicMock()
                processor.operation_storage = PropertyMock(upload_path=Path(tmp_upload_dir))
                get_operation_processor.return_value = processor

                with self._exp() as exp:
                    path, wait = (
                        self._random_path(),
                        self._random_wait(),
                    )
                    var = File(exp, path)
                    var.assign(value, wait=wait)

                    if value.file_type is not FileType.LOCAL_FILE:
                        tmp_uploaded_file = get_tmp_uploaded_file_name(tmp_upload_dir)
                        self.assertTrue(os.path.exists(tmp_uploaded_file))
                    else:
                        tmp_uploaded_file = None

                    processor.enqueue_operation.assert_called_with(
                        operation_factory(path, tmp_uploaded_file), wait=wait
                    )

    def test_assign_type_error(self):
        values = [55, None, []]
        for value in values:
            with self.assertRaises(TypeError):
                File(MagicMock(), MagicMock()).assign(value)

    @unittest.skipIf(IS_WINDOWS, "Windows behaves strangely")
    @patch("neptune.metadata_containers.metadata_container.get_operation_processor")
    def test_save(self, get_operation_processor):
        value_and_expected = [("some/path", os.getcwd() + "/some/path")]

        for value, expected in value_and_expected:
            processor = MagicMock()
            get_operation_processor.return_value = processor

            with self._exp() as exp:
                path, wait = (
                    self._random_path(),
                    self._random_wait(),
                )
                var = File(exp, path)
                var.upload(value, wait=wait)
                processor.enqueue_operation.assert_called_with(
                    UploadFile(path=path, ext="", file_path=expected), wait=wait
                )

    @unittest.skipIf(IS_WINDOWS, "Windows behaves strangely")
    @patch("neptune.metadata_containers.metadata_container.get_operation_processor")
    def test_save_files(self, get_operation_processor):
        value_and_expected = [("some/path/*", [os.getcwd() + "/some/path/*"])]

        for value, expected in value_and_expected:
            processor = MagicMock()
            get_operation_processor.return_value = processor

            with self._exp() as exp:
                path, wait = (
                    self._random_path(),
                    self._random_wait(),
                )
                var = FileSet(exp, path)
                var.upload_files(value, wait=wait)
                processor.enqueue_operation.assert_called_with(UploadFileSet(path, expected, False), wait=wait)

    def test_save_type_error(self):
        values = [55, None, [], FileVal]
        for value in values:
            with self.assertRaises(TypeError):
                File(MagicMock(), MagicMock()).upload(value)

    def test_save__files_type_error(self):
        values = [55, None, [55], FileSetVal]
        for value in values:
            with self.assertRaises(TypeError):
                FileSet(MagicMock(), MagicMock()).upload_files(value)

    def test_fetch_extension(self):
        value_and_expected_ext = [
            (FileVal("some/file.txt"), "txt"),
            (FileVal("some/file"), ""),
            (FileVal.from_content("Some text stream"), "txt"),
            (FileVal.from_content(b"Some binary stream"), "bin"),
            (FileVal.from_content(b"Some binary stream", extension="png"), "png"),
        ]

        for value, expected_ext in value_and_expected_ext:
            with self._exp() as exp:
                path, wait = (
                    self._random_path(),
                    self._random_wait(),
                )
                var = File(exp, path)
                var.assign(value, wait=wait)
                self.assertEqual(expected_ext, var.fetch_extension())

    def test_clean_files_on_close(self):
        with self._exp() as run:
            data_path = run._op_processor.data_path

            assert os.path.exists(data_path)

            run.stop()

            assert not os.path.exists(data_path)  # exec folder
