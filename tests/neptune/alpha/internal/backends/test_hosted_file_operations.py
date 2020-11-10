#
# Copyright (c) 2019, Neptune Labs Sp. z o.o.
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
import uuid
from tempfile import NamedTemporaryFile, TemporaryDirectory

import mock
from mock import MagicMock, patch

from neptune.alpha.exceptions import FileUploadError
from neptune.alpha.internal.backends.hosted_file_operations import upload_file_attribute, upload_file_attributes, \
    download_file_attribute, _get_content_disposition_filename
from neptune.internal.storage.storage_utils import UploadEntry
from neptune.utils import IS_WINDOWS


class TestHostedFileOperations(unittest.TestCase):
    # pylint:disable=protected-access

    @unittest.skipIf(IS_WINDOWS, "Windows behaves strangely")
    @patch('neptune.alpha.internal.backends.hosted_file_operations._upload_loop')
    def test_upload_attribute(self, upload_loop):
        # given
        exp_uuid = uuid.uuid4()
        swagger_mock = self._get_swagger_mock()

        # when
        with NamedTemporaryFile("w") as f:
            result = upload_file_attribute(
                swagger_client=swagger_mock,
                experiment_uuid=exp_uuid,
                attribute="target/path.txt",
                file_path=f.name
            )

        # then
        self.assertEqual(None, result)
        upload_loop.assert_called_once_with(
            http_client=swagger_mock.swagger_spec.http_client,
            url="ui.neptune.ai/attributes/upload",
            file_chunk_stream=mock.ANY,
            query_params={
                "experimentId": str(exp_uuid),
                "attribute": "target/path.txt",
                "filename": os.path.basename(f.name)
            }
        )

    @unittest.skipIf(IS_WINDOWS, "Windows behaves strangely")
    @patch('neptune.alpha.internal.backends.hosted_file_operations._upload_loop')
    def test_single_file(self, upload_loop):
        # given
        exp_uuid = uuid.uuid4()
        swagger_mock = self._get_swagger_mock()

        # when
        with NamedTemporaryFile("w") as f:
            result = upload_file_attributes(
                experiment_uuid=exp_uuid,
                upload_entries=[UploadEntry(f.name, "target/path.txt")],
                swagger_client=swagger_mock
            )

        # then
        self.assertEqual([], result)
        upload_loop.assert_called_once_with(
            http_client=swagger_mock.swagger_spec.http_client,
            url="ui.neptune.ai/uploadPath",
            file_chunk_stream=mock.ANY,
            query_params={
                "experimentIdentifier": str(exp_uuid),
                "resource": "attributes",
                "pathParam": "target/path.txt"
            }
        )

    @patch('neptune.alpha.internal.backends.hosted_file_operations.upload_raw_data')
    @unittest.skipIf(IS_WINDOWS, "Windows behaves strangely")
    def test_multiple_files(self, raw_upload):
        # given
        exp_uuid = uuid.uuid4()
        swagger_mock = self._get_swagger_mock()

        # when
        with NamedTemporaryFile("w") as file1:
            with NamedTemporaryFile("w") as file2:
                result = upload_file_attributes(
                    experiment_uuid=exp_uuid,
                    upload_entries=[
                        UploadEntry(file1.name, "target/path1.txt"),
                        UploadEntry(file2.name, "target/path3.txt"),
                    ],
                    swagger_client=swagger_mock
                )

        # then
        self.assertEqual([], result)

        raw_upload.assert_called_once_with(
            http_client=swagger_mock.swagger_spec.http_client,
            url="ui.neptune.ai/uploadTarStream",
            data=mock.ANY,
            headers={"Content-Type": "application/octet-stream"},
            query_params={
                "experimentIdentifier": str(exp_uuid),
                "resource": "attributes",
            }
        )

    @patch('neptune.alpha.internal.backends.hosted_file_operations.upload_raw_data')
    @unittest.skipIf(IS_WINDOWS, "Windows behaves strangely")
    def test_missing_files_or_directory(self, raw_upload):
        # given
        exp_uuid = uuid.uuid4()
        swagger_mock = self._get_swagger_mock()

        # when
        with NamedTemporaryFile("w") as file1:
            with NamedTemporaryFile("w") as file2:
                with TemporaryDirectory() as dirpath:
                    result = upload_file_attributes(
                        experiment_uuid=exp_uuid,
                        upload_entries=[
                            UploadEntry(file1.name, "target/path1.txt"),
                            UploadEntry("missing1", "target/path2.txt"),
                            UploadEntry(file2.name, "target/path3.txt"),
                            UploadEntry("missing2", "target/path4.txt"),
                            UploadEntry(dirpath, "target/path5.txt"),
                        ],
                        swagger_client=swagger_mock
                    )

        # then
        self.assertEqual([
            FileUploadError("missing1", "Path not found or is a not a file."),
            FileUploadError("missing2", "Path not found or is a not a file."),
            FileUploadError(dirpath, "Path not found or is a not a file.")
        ], result)

        raw_upload.assert_called_once_with(
            http_client=swagger_mock.swagger_spec.http_client,
            url="ui.neptune.ai/uploadTarStream",
            data=mock.ANY,
            headers={"Content-Type": "application/octet-stream"},
            query_params={
                "experimentIdentifier": str(exp_uuid),
                "resource": "attributes",
            }
        )

    def test_get_content_disposition_filename(self):
        # given
        response_mock = MagicMock()
        response_mock.headers = {'Content-Disposition': 'attachment; filename=sample.file'}

        # when
        filename = _get_content_disposition_filename(response_mock)

        # then
        self.assertEqual(filename, "sample.file")

    @patch('neptune.alpha.internal.backends.hosted_file_operations._download_raw_data')
    def test_download_file_attribute(self, download_raw):
        # given
        swagger_mock = self._get_swagger_mock()
        exp_uuid = uuid.uuid4()

        # when
        download_file_attribute(swagger_client=swagger_mock,
                                experiment_uuid=exp_uuid,
                                attribute="some/attribute",
                                destination=None)

        # then
        download_raw.assert_called_once_with(
            http_client=swagger_mock.swagger_spec.http_client,
            url="ui.neptune.ai/attributes/download",
            headers={"Accept": "application/octet-stream"},
            query_params={
                "experimentId": str(exp_uuid),
                "attribute": "some/attribute"
            }
        )

    @staticmethod
    def _get_swagger_mock():
        swagger_mock = MagicMock()
        swagger_mock.swagger_spec.http_client = MagicMock()
        swagger_mock.swagger_spec.api_url = "ui.neptune.ai"
        swagger_mock.api.uploadTarStream.operation.path_name = "/uploadTarStream"
        swagger_mock.api.uploadPath.operation.path_name = "/uploadPath"
        swagger_mock.api.uploadAttribute.operation.path_name = "/attributes/upload"
        swagger_mock.api.downloadAttribute.operation.path_name = "/attributes/download"
        return swagger_mock


if __name__ == '__main__':
    unittest.main()
