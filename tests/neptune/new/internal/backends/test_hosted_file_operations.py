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

from neptune.new.internal.backends.hosted_file_operations import upload_file_attribute, upload_file_set_attribute, \
    download_file_attribute, _get_content_disposition_filename, _attribute_upload_response_handler, \
    download_file_set_attribute
from neptune.utils import IS_WINDOWS


class TestHostedFileOperations(unittest.TestCase):
    # pylint:disable=protected-access

    @unittest.skipIf(IS_WINDOWS, "Windows behaves strangely")
    @patch('neptune.new.internal.backends.hosted_file_operations._upload_loop')
    def test_upload_file_attribute(self, upload_loop_mock):
        # given
        exp_uuid = uuid.uuid4()
        swagger_mock = self._get_swagger_mock()
        upload_loop_mock.return_value = b'null'

        # when
        with NamedTemporaryFile("w") as f:
            upload_file_attribute(
                swagger_client=swagger_mock,
                run_uuid=exp_uuid,
                attribute="target/path.txt",
                source=f.name,
                ext="txt")

        # then
        upload_loop_mock.assert_called_once_with(
            file_chunk_stream=mock.ANY,
            response_handler=_attribute_upload_response_handler,
            http_client=swagger_mock.swagger_spec.http_client,
            url="https://ui.neptune.ai/attributes/upload",
            query_params={
                "experimentId": str(exp_uuid),
                "attribute": "target/path.txt",
                "ext": "txt"
            })

    @unittest.skipIf(IS_WINDOWS, "Windows behaves strangely")
    @patch('neptune.new.internal.backends.hosted_file_operations._upload_loop')
    def test_upload_file_attribute_from_stream(self, upload_loop_mock):
        # given
        exp_uuid = uuid.uuid4()
        swagger_mock = self._get_swagger_mock()
        upload_loop_mock.return_value = b'null'

        # when
        upload_file_attribute(
            swagger_client=swagger_mock,
            run_uuid=exp_uuid,
            attribute="target/path.txt",
            source=b"Some content of test stream",
            ext="txt")

        # then
        upload_loop_mock.assert_called_once_with(
            file_chunk_stream=mock.ANY,
            response_handler=_attribute_upload_response_handler,
            http_client=swagger_mock.swagger_spec.http_client,
            url="https://ui.neptune.ai/attributes/upload",
            query_params={
                "experimentId": str(exp_uuid),
                "attribute": "target/path.txt",
                "ext": "txt"
            })

    @unittest.skipIf(IS_WINDOWS, "Windows behaves strangely")
    @patch('neptune.new.internal.backends.hosted_file_operations._upload_loop')
    @patch('neptune.new.internal.utils.glob', new=lambda path, recursive=False: [path.replace('*', 'file.txt')])
    def test_upload_single_file_in_file_set_attribute(self, upload_loop_mock):
        # given
        exp_uuid = uuid.uuid4()
        swagger_mock = self._get_swagger_mock()
        upload_loop_mock.return_value = b'null'

        # when
        with NamedTemporaryFile("w") as temp_file:
            upload_file_set_attribute(
                swagger_client=swagger_mock,
                run_uuid=exp_uuid,
                attribute="some/attribute",
                file_globs=[temp_file.name],
                reset=True)

        # then
        upload_loop_mock.assert_called_once_with(
            file_chunk_stream=mock.ANY,
            response_handler=_attribute_upload_response_handler,
            http_client=swagger_mock.swagger_spec.http_client,
            url="https://ui.neptune.ai/uploadFileSetChunk",
            query_params={
                "experimentId": str(exp_uuid),
                "attribute": "some/attribute",
                "reset": "True",
                "path": os.path.basename(temp_file.name)
            })

    @unittest.skipIf(IS_WINDOWS, "Windows behaves strangely")
    @patch('neptune.new.internal.backends.hosted_file_operations.upload_raw_data')
    @patch('neptune.new.internal.utils.glob', new=lambda path, recursive=False: [path.replace('*', 'file.txt')])
    def test_upload_multiple_files_in_file_set_attribute(self, upload_raw_data_mock):
        # given
        exp_uuid = uuid.uuid4()
        swagger_mock = self._get_swagger_mock()
        upload_raw_data_mock.return_value = b'null'

        # when
        with NamedTemporaryFile("w") as temp_file_1:
            with NamedTemporaryFile("w") as temp_file_2:
                upload_file_set_attribute(
                    swagger_client=swagger_mock,
                    run_uuid=exp_uuid,
                    attribute="some/attribute",
                    file_globs=[temp_file_1.name, temp_file_2.name],
                    reset=True)

        # then
        upload_raw_data_mock.assert_called_once_with(
            http_client=swagger_mock.swagger_spec.http_client,
            url="https://ui.neptune.ai/uploadFileSetTar",
            data=mock.ANY,
            headers={"Content-Type": "application/octet-stream"},
            query_params={
                "experimentId": str(exp_uuid),
                "attribute": "some/attribute",
                "reset": "True"
            })

    @unittest.skipIf(IS_WINDOWS, "Windows behaves strangely")
    @patch('neptune.new.internal.backends.hosted_file_operations.upload_raw_data')
    def test_missing_files_or_directory(self, upload_raw_data_mock):
        # given
        exp_uuid = uuid.uuid4()
        swagger_mock = self._get_swagger_mock()
        upload_raw_data_mock.return_value = b'null'

        # when
        with NamedTemporaryFile("w") as temp_file_1:
            with NamedTemporaryFile("w") as temp_file_2:
                with TemporaryDirectory() as temp_dir:
                    upload_file_set_attribute(
                        swagger_client=swagger_mock,
                        run_uuid=exp_uuid,
                        attribute="some/attribute",
                        file_globs=[temp_file_1.name, temp_file_2.name, os.path.abspath("missing_file"), temp_dir],
                        reset=True)

        # then
        upload_raw_data_mock.assert_called_once_with(
            http_client=swagger_mock.swagger_spec.http_client,
            url="https://ui.neptune.ai/uploadFileSetTar",
            data=mock.ANY,
            headers={"Content-Type": "application/octet-stream"},
            query_params={
                "experimentId": str(exp_uuid),
                "attribute": "some/attribute",
                "reset": "True"
            })

    def test_get_content_disposition_filename(self):
        # given
        response_mock = MagicMock()
        response_mock.headers = {'Content-Disposition': 'attachment; filename="sample.file"'}

        # when
        filename = _get_content_disposition_filename(response_mock)

        # then
        self.assertEqual(filename, "sample.file")

    @patch('neptune.new.internal.backends.hosted_file_operations._store_response_as_file')
    @patch('neptune.new.internal.backends.hosted_file_operations._download_raw_data')
    def test_download_file_attribute(self, download_raw, store_response_mock):
        # given
        swagger_mock = self._get_swagger_mock()
        exp_uuid = uuid.uuid4()

        # when
        download_file_attribute(swagger_client=swagger_mock,
                                run_uuid=exp_uuid,
                                attribute="some/attribute",
                                destination=None)

        # then
        download_raw.assert_called_once_with(
            http_client=swagger_mock.swagger_spec.http_client,
            url="https://ui.neptune.ai/attributes/download",
            headers={"Accept": "application/octet-stream"},
            query_params={
                "experimentId": str(exp_uuid),
                "attribute": "some/attribute"
            },
        )
        store_response_mock.assert_called_once_with(download_raw.return_value, None)

    @patch('neptune.new.internal.backends.hosted_file_operations._store_response_as_file')
    @patch('neptune.new.internal.backends.hosted_file_operations._download_raw_data')
    @patch('neptune.new.internal.backends.hosted_file_operations._get_download_url', new=lambda _, _id: "some_url")
    def test_download_file_set_attribute(self, download_raw, store_response_mock):
        # given
        swagger_mock = self._get_swagger_mock()
        download_id = uuid.uuid4()

        # when
        download_file_set_attribute(swagger_client=swagger_mock,
                                    download_id=download_id,
                                    destination=None)

        # then
        download_raw.assert_called_once_with(
            http_client=swagger_mock.swagger_spec.http_client,
            url="some_url",
            headers={"Accept": "application/zip"},
        )
        store_response_mock.assert_called_once_with(download_raw.return_value, None)

    @staticmethod
    def _get_swagger_mock():
        swagger_mock = MagicMock()
        swagger_mock.swagger_spec.http_client = MagicMock()
        swagger_mock.swagger_spec.api_url = "ui.neptune.ai"
        swagger_mock.api.uploadFileSetAttributeChunk.operation.path_name = "/uploadFileSetChunk"
        swagger_mock.api.uploadFileSetAttributeTar.operation.path_name = "/uploadFileSetTar"
        swagger_mock.api.uploadPath.operation.path_name = "/uploadPath"
        swagger_mock.api.uploadAttribute.operation.path_name = "/attributes/upload"
        swagger_mock.api.downloadAttribute.operation.path_name = "/attributes/download"
        swagger_mock.api.downloadFileSetAttributeZip.operation.path_name = "/attributes/downloadFileSetZip"
        swagger_mock.api.downloadFileSetAttributeZip.operation.path_name = "/attributes/downloadFileSetZip"
        swagger_mock.api.download.operation.path_name = "/download"
        return swagger_mock


if __name__ == '__main__':
    unittest.main()
