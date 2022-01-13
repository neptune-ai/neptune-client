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
import json
import os
import random
import unittest
import uuid
from collections import namedtuple
from tempfile import NamedTemporaryFile, TemporaryDirectory

import mock
from mock import MagicMock, patch, call

from neptune.new.internal.backends.api_model import ClientConfig
from neptune.new.internal.backends.hosted_file_operations import (
    upload_file_attribute,
    upload_file_set_attribute,
    download_file_attribute,
    _get_content_disposition_filename,
    download_file_set_attribute,
)
from neptune.utils import IS_WINDOWS
from tests.neptune.new.backend_test_mixin import BackendTestMixin
from tests.neptune.new.helpers import create_file


def set_expected_result(endpoint: MagicMock, value: dict):
    endpoint.return_value.response.return_value.result = namedtuple(
        endpoint.__class__.__name__, value.keys()
    )(**value)


class HostedFileOperationsHelper(unittest.TestCase):
    @staticmethod
    def get_random_bytes(count):
        return bytes(random.randint(0, 255) for _ in range(count))

    @staticmethod
    def _get_swagger_mock():
        swagger_mock = MagicMock()
        swagger_mock.swagger_spec.http_client = MagicMock()
        swagger_mock.swagger_spec.api_url = "ui.neptune.ai"
        swagger_mock.api.uploadFileSetAttributeChunk.operation.path_name = (
            "/uploadFileSetChunk"
        )
        swagger_mock.api.uploadFileSetAttributeTar.operation.path_name = (
            "/uploadFileSetTar"
        )
        swagger_mock.api.uploadPath.operation.path_name = "/uploadPath"
        swagger_mock.api.uploadAttribute.operation.path_name = "/attributes/upload"
        swagger_mock.api.downloadAttribute.operation.path_name = "/attributes/download"
        swagger_mock.api.downloadFileSetAttributeZip.operation.path_name = (
            "/attributes/downloadFileSetZip"
        )
        swagger_mock.api.downloadFileSetAttributeZip.operation.path_name = (
            "/attributes/downloadFileSetZip"
        )
        swagger_mock.api.download.operation.path_name = "/download"

        swagger_mock.api.fileAtomMultipartUploadStart.operation.path_name = (
            "/attributes/storage/file/upload/start"
        )
        swagger_mock.api.fileAtomMultipartUploadFinish.operation.path_name = (
            "/attributes/storage/file/upload/finish"
        )
        swagger_mock.api.fileAtomMultipartUploadPart.operation.path_name = (
            "/attributes/storage/file/upload/part"
        )
        swagger_mock.api.fileAtomUpload.operation.path_name = (
            "/attributes/storage/file/upload"
        )

        swagger_mock.api.fileSetFileMultipartUploadStart.operation.path_name = (
            "/attributes/storage/fileset/upload/start"
        )
        swagger_mock.api.fileSetFileMultipartUploadFinish.operation.path_name = (
            "/attributes/storage/fileset/upload/finish"
        )
        swagger_mock.api.fileSetFileMultipartUploadPart.operation.path_name = (
            "/attributes/storage/fileset/upload/part"
        )
        swagger_mock.api.fileSetFileUpload.operation.path_name = (
            "/attributes/storage/fileset/upload"
        )
        return swagger_mock


class TestCommonHostedFileOperations(HostedFileOperationsHelper):
    # pylint:disable=protected-access
    def test_get_content_disposition_filename(self):
        # given
        response_mock = MagicMock()
        response_mock.headers = {
            "Content-Disposition": 'attachment; filename="sample.file"'
        }

        # when
        filename = _get_content_disposition_filename(response_mock)

        # then
        self.assertEqual(filename, "sample.file")

    @patch(
        "neptune.new.internal.backends.hosted_file_operations._store_response_as_file"
    )
    @patch("neptune.new.internal.backends.hosted_file_operations._download_raw_data")
    def test_download_file_attribute(self, download_raw, store_response_mock):
        # given
        swagger_mock = self._get_swagger_mock()
        exp_uuid = str(uuid.uuid4())

        # when
        download_file_attribute(
            swagger_client=swagger_mock,
            container_id=exp_uuid,
            attribute="some/attribute",
            destination=None,
        )

        # then
        download_raw.assert_called_once_with(
            http_client=swagger_mock.swagger_spec.http_client,
            url="https://ui.neptune.ai/attributes/download",
            headers={"Accept": "application/octet-stream"},
            query_params={"experimentId": str(exp_uuid), "attribute": "some/attribute"},
        )
        store_response_mock.assert_called_once_with(download_raw.return_value, None)

    @patch(
        "neptune.new.internal.backends.hosted_file_operations._store_response_as_file"
    )
    @patch("neptune.new.internal.backends.hosted_file_operations._download_raw_data")
    @patch(
        "neptune.new.internal.backends.hosted_file_operations._get_download_url",
        new=lambda _, _id: "some_url",
    )
    def test_download_file_set_attribute(self, download_raw, store_response_mock):
        # given
        swagger_mock = self._get_swagger_mock()
        download_id = str(uuid.uuid4())

        # when
        download_file_set_attribute(
            swagger_client=swagger_mock, download_id=download_id, destination=None
        )

        # then
        download_raw.assert_called_once_with(
            http_client=swagger_mock.swagger_spec.http_client,
            url="some_url",
            headers={"Accept": "application/zip"},
        )
        store_response_mock.assert_called_once_with(download_raw.return_value, None)


class TestOldUploadFileOperations(HostedFileOperationsHelper):
    multipart_config = None

    @unittest.skipIf(IS_WINDOWS, "Windows behaves strangely")
    @patch("neptune.new.internal.backends.hosted_file_operations.upload_raw_data")
    def test_missing_files_or_directory(self, upload_raw_data_mock):
        # given
        exp_uuid = str(uuid.uuid4())
        swagger_mock = self._get_swagger_mock()
        upload_raw_data_mock.return_value = b"null"
        swagger_mock.api.getUploadConfig.return_value.response.return_value.result.chunkSize = (
            10
        )

        # when
        with NamedTemporaryFile("w") as temp_file_1:
            with NamedTemporaryFile("w") as temp_file_2:
                with TemporaryDirectory() as temp_dir:
                    upload_file_set_attribute(
                        swagger_client=swagger_mock,
                        container_id=exp_uuid,
                        attribute="some/attribute",
                        file_globs=[
                            temp_file_1.name,
                            temp_file_2.name,
                            os.path.abspath("missing_file"),
                            temp_dir,
                        ],
                        reset=True,
                        multipart_config=self.multipart_config,
                    )

        # then
        upload_raw_data_mock.assert_called_once_with(
            http_client=swagger_mock.swagger_spec.http_client,
            url="https://ui.neptune.ai/uploadFileSetTar",
            data=mock.ANY,
            headers={"Content-Type": "application/octet-stream"},
            query_params={
                "experimentId": str(exp_uuid),
                "attribute": "some/attribute",
                "reset": "True",
            },
        )

    @unittest.skipIf(IS_WINDOWS, "Windows behaves strangely")
    @patch("neptune.new.internal.backends.hosted_file_operations._upload_loop")
    def test_upload_file_attribute(self, upload_loop_mock):
        # given
        exp_uuid = str(uuid.uuid4())
        swagger_mock = self._get_swagger_mock()
        upload_loop_mock.return_value = b"null"

        # when
        with NamedTemporaryFile("w") as f:
            upload_file_attribute(
                swagger_client=swagger_mock,
                container_id=exp_uuid,
                attribute="target/path.txt",
                source=f.name,
                ext="txt",
                multipart_config=self.multipart_config,
            )

        # then
        upload_loop_mock.assert_called_once_with(
            file_chunk_stream=mock.ANY,
            http_client=swagger_mock.swagger_spec.http_client,
            url="https://ui.neptune.ai/attributes/upload",
            query_params={
                "experimentId": str(exp_uuid),
                "attribute": "target/path.txt",
                "ext": "txt",
            },
        )

    @unittest.skipIf(IS_WINDOWS, "Windows behaves strangely")
    @patch("neptune.new.internal.backends.hosted_file_operations._upload_loop")
    def test_upload_file_attribute_from_stream(self, upload_loop_mock):
        # given
        exp_uuid = str(uuid.uuid4())
        swagger_mock = self._get_swagger_mock()
        upload_loop_mock.return_value = b"null"

        # when
        upload_file_attribute(
            swagger_client=swagger_mock,
            container_id=exp_uuid,
            attribute="target/path.txt",
            source=b"Some content of test stream",
            ext="txt",
            multipart_config=self.multipart_config,
        )

        # then
        upload_loop_mock.assert_called_once_with(
            file_chunk_stream=mock.ANY,
            http_client=swagger_mock.swagger_spec.http_client,
            url="https://ui.neptune.ai/attributes/upload",
            query_params={
                "experimentId": str(exp_uuid),
                "attribute": "target/path.txt",
                "ext": "txt",
            },
        )

    @unittest.skipIf(IS_WINDOWS, "Windows behaves strangely")
    @patch("neptune.new.internal.backends.hosted_file_operations._upload_loop_chunk")
    @patch(
        "neptune.new.internal.utils.glob",
        new=lambda path, recursive=False: [path.replace("*", "file.txt")],
    )
    def test_upload_single_file_in_file_set_attribute(self, upload_loop_chunk_mock):
        # given
        exp_uuid = uuid.uuid4()
        swagger_mock = self._get_swagger_mock()
        upload_loop_chunk_mock.return_value = b"null"
        chunk_size = 5 * 1024 * 1024
        swagger_mock.api.getUploadConfig.return_value.response.return_value.result.chunkSize = (
            chunk_size
        )

        # when
        with NamedTemporaryFile("w") as temp_file:
            with open(temp_file.name, "wb") as handler:
                handler.write(self.get_random_bytes(2 * chunk_size))

            upload_file_set_attribute(
                swagger_client=swagger_mock,
                container_id=str(exp_uuid),
                attribute="some/attribute",
                file_globs=[temp_file.name],
                reset=True,
                multipart_config=self.multipart_config,
            )

        # then
        upload_loop_chunk_mock.assert_has_calls(
            [
                call(
                    mock.ANY,
                    mock.ANY,
                    http_client=swagger_mock.swagger_spec.http_client,
                    query_params={
                        "experimentId": str(exp_uuid),
                        "attribute": "some/attribute",
                        "reset": "True",
                        "path": os.path.basename(temp_file.name),
                    },
                    url="https://ui.neptune.ai/uploadFileSetChunk",
                ),
                call(
                    mock.ANY,
                    mock.ANY,
                    http_client=swagger_mock.swagger_spec.http_client,
                    query_params={
                        "experimentId": str(exp_uuid),
                        "attribute": "some/attribute",
                        "reset": "False",
                        "path": os.path.basename(temp_file.name),
                    },
                    url="https://ui.neptune.ai/uploadFileSetChunk",
                ),
            ]
        )

    @unittest.skipIf(IS_WINDOWS, "Windows behaves strangely")
    @patch("neptune.new.internal.backends.hosted_file_operations.upload_raw_data")
    @patch(
        "neptune.new.internal.utils.glob",
        new=lambda path, recursive=False: [path.replace("*", "file.txt")],
    )
    def test_upload_multiple_files_in_file_set_attribute(self, upload_raw_data_mock):
        # given
        exp_uuid = str(uuid.uuid4())
        swagger_mock = self._get_swagger_mock()
        upload_raw_data_mock.return_value = b"null"
        swagger_mock.api.getUploadConfig.return_value.response.return_value.result.chunkSize = (
            10
        )

        # when
        with NamedTemporaryFile("w") as temp_file_1:
            with NamedTemporaryFile("w") as temp_file_2:
                upload_file_set_attribute(
                    swagger_client=swagger_mock,
                    container_id=exp_uuid,
                    attribute="some/attribute",
                    file_globs=[temp_file_1.name, temp_file_2.name],
                    reset=True,
                    multipart_config=self.multipart_config,
                )

        # then
        upload_raw_data_mock.assert_called_once_with(
            http_client=swagger_mock.swagger_spec.http_client,
            url="https://ui.neptune.ai/uploadFileSetTar",
            data=mock.ANY,
            headers={"Content-Type": "application/octet-stream"},
            query_params={
                "experimentId": str(exp_uuid),
                "attribute": "some/attribute",
                "reset": "True",
            },
        )


class TestNewUploadFileOperations(HostedFileOperationsHelper, BackendTestMixin):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        config_swagger_client = self._get_swagger_client_mock(MagicMock())
        client_config = ClientConfig.from_api_response(
            config_swagger_client.api.getClientConfig().response().result
        )
        self.multipart_config = client_config.multipart_config

    @unittest.skipIf(IS_WINDOWS, "Windows behaves strangely")
    @patch("neptune.new.internal.backends.hosted_file_operations.upload_raw_data")
    def test_missing_files_or_directory(self, upload_raw_data_mock):
        # given
        exp_uuid = str(uuid.uuid4())
        swagger_mock = self._get_swagger_mock()
        upload_raw_data_mock.return_value = b"null"

        # when
        with NamedTemporaryFile("w") as temp_file_1:
            with NamedTemporaryFile("w") as temp_file_2:
                with TemporaryDirectory() as temp_dir:
                    upload_file_set_attribute(
                        swagger_client=swagger_mock,
                        container_id=exp_uuid,
                        attribute="some/attribute",
                        file_globs=[
                            temp_file_1.name,
                            temp_file_2.name,
                            os.path.abspath("missing_file"),
                            temp_dir,
                        ],
                        reset=True,
                        multipart_config=self.multipart_config,
                    )

        # then
        upload_raw_data_mock.assert_called_once_with(
            http_client=swagger_mock.swagger_spec.http_client,
            url="https://ui.neptune.ai/uploadFileSetTar",
            data=mock.ANY,
            headers={"Content-Type": "application/octet-stream"},
            query_params={
                "experimentId": str(exp_uuid),
                "attribute": "some/attribute",
                "reset": "True",
            },
        )

    @unittest.skipIf(IS_WINDOWS, "Windows behaves strangely")
    @patch("neptune.new.internal.backends.hosted_file_operations.upload_raw_data")
    def test_upload_small_file_attribute(self, upload_raw_data):
        # given
        exp_uuid = str(uuid.uuid4())
        swagger_mock = self._get_swagger_mock()
        upload_raw_data.return_value = json.dumps(
            {
                "uploadId": "placeholder",
                "errors": [],
            }
        )
        data = b"testdata"

        # when
        with create_file(content=data, binary_mode=True) as filename:
            upload_file_attribute(
                swagger_client=swagger_mock,
                container_id=exp_uuid,
                attribute="target/path.txt",
                source=filename,
                ext="txt",
                multipart_config=self.multipart_config,
            )

        # then
        swagger_mock.api.fileSetFileMultipartUploadStart.assert_not_called()
        swagger_mock.api.fileSetFileMultipartUploadFinish.assert_not_called()
        swagger_mock.api.fileSetFileMultipartUploadPart.assert_not_called()
        swagger_mock.api.fileSetFileUpload.assert_not_called()
        swagger_mock.api.fileAtomMultipartUploadStart.assert_not_called()
        swagger_mock.api.fileAtomMultipartUploadFinish.assert_not_called()
        swagger_mock.api.fileAtomMultipartUploadPart.assert_not_called()
        swagger_mock.api.fileAtomUpload.assert_not_called()
        upload_raw_data.assert_called_once_with(
            data=data,
            http_client=swagger_mock.swagger_spec.http_client,
            url="https://ui.neptune.ai/attributes/storage/file/upload",
            query_params={
                "experimentIdentifier": str(exp_uuid),
                "attribute": "target/path.txt",
                "ext": "txt",
            },
        )

    @unittest.skipIf(IS_WINDOWS, "Windows behaves strangely")
    @patch("neptune.new.internal.backends.hosted_file_operations.upload_raw_data")
    def test_upload_big_file_attribute(self, upload_raw_data):
        # given
        exp_uuid = str(uuid.uuid4())
        swagger_mock = self._get_swagger_mock()
        upload_id = "placeholder"
        set_expected_result(
            swagger_mock.api.fileAtomMultipartUploadStart,
            {
                "uploadId": upload_id,
                "errors": [],
            },
        )
        upload_raw_data.return_value = json.dumps(
            {
                "errors": [],
            }
        )
        data = self.get_random_bytes(8 * 2 ** 20)  # 8 MB
        chunk_size = self.multipart_config.min_chunk_size

        # when
        with create_file(content=data, binary_mode=True) as filename:
            upload_file_attribute(
                swagger_client=swagger_mock,
                container_id=exp_uuid,
                attribute="target/path.txt",
                source=filename,
                ext="txt",
                multipart_config=self.multipart_config,
            )

        # then
        swagger_mock.api.fileSetFileMultipartUploadStart.assert_not_called()
        swagger_mock.api.fileSetFileMultipartUploadFinish.assert_not_called()
        swagger_mock.api.fileSetFileMultipartUploadPart.assert_not_called()
        swagger_mock.api.fileSetFileUpload.assert_not_called()
        swagger_mock.api.fileAtomUpload.assert_not_called()
        swagger_mock.api.fileAtomMultipartUploadStart.assert_called_once_with(
            attribute="target/path.txt",
            experimentIdentifier=str(exp_uuid),
            ext="txt",
            totalLength=len(data),
        )
        swagger_mock.api.fileAtomMultipartUploadFinish.assert_called_once_with(
            attribute="target/path.txt",
            experimentIdentifier=str(exp_uuid),
            uploadId=upload_id,
        )
        upload_raw_data.assert_has_calls(
            [
                call(
                    data=data[:chunk_size],
                    http_client=swagger_mock.swagger_spec.http_client,
                    url="https://ui.neptune.ai/attributes/storage/file/upload/part",
                    headers={"X-Range": f"bytes=0-{chunk_size - 1}/{len(data)}"},
                    query_params={
                        "uploadPartIdx": 0,
                        "uploadId": upload_id,
                        "experimentIdentifier": str(exp_uuid),
                        "attribute": "target/path.txt",
                    },
                ),
                call(
                    data=data[chunk_size:],
                    http_client=swagger_mock.swagger_spec.http_client,
                    url="https://ui.neptune.ai/attributes/storage/file/upload/part",
                    headers={
                        "X-Range": f"bytes={chunk_size}-{len(data) - 1}/{len(data)}"
                    },
                    query_params={
                        "uploadPartIdx": 1,
                        "uploadId": upload_id,
                        "experimentIdentifier": str(exp_uuid),
                        "attribute": "target/path.txt",
                    },
                ),
            ]
        )

    @unittest.skipIf(IS_WINDOWS, "Windows behaves strangely")
    @patch("neptune.new.internal.backends.hosted_file_operations.upload_raw_data")
    @patch(
        "neptune.new.internal.utils.glob",
        new=lambda path, recursive=False: [path.replace("*", "file.txt")],
    )
    def test_upload_single_small_file_in_file_set_attribute(self, upload_raw_data):
        # given
        exp_uuid = uuid.uuid4()
        swagger_mock = self._get_swagger_mock()
        upload_raw_data.return_value = json.dumps(
            {
                "errors": [],
            }
        )
        data = b"testdata"

        # when
        with create_file(content=data, binary_mode=True) as filename:
            upload_file_set_attribute(
                swagger_client=swagger_mock,
                container_id=str(exp_uuid),
                attribute="some/attribute",
                file_globs=[filename],
                reset=True,
                multipart_config=self.multipart_config,
            )

        # then
        swagger_mock.api.fileSetFileMultipartUploadStart.assert_not_called()
        swagger_mock.api.fileSetFileMultipartUploadFinish.assert_not_called()
        swagger_mock.api.fileSetFileMultipartUploadPart.assert_not_called()
        swagger_mock.api.fileSetFileUpload.assert_not_called()
        swagger_mock.api.fileAtomMultipartUploadStart.assert_not_called()
        swagger_mock.api.fileAtomMultipartUploadFinish.assert_not_called()
        swagger_mock.api.fileAtomMultipartUploadPart.assert_not_called()
        swagger_mock.api.fileAtomUpload.assert_not_called()
        upload_raw_data.assert_called_once_with(
            data=data,
            http_client=swagger_mock.swagger_spec.http_client,
            url="https://ui.neptune.ai/attributes/storage/fileset/upload",
            query_params={
                "subPath": os.path.basename(filename),
                "experimentIdentifier": str(exp_uuid),
                "attribute": "some/attribute",
            },
        )

    @unittest.skipIf(IS_WINDOWS, "Windows behaves strangely")
    @patch("neptune.new.internal.backends.hosted_file_operations.upload_raw_data")
    @patch(
        "neptune.new.internal.utils.glob",
        new=lambda path, recursive=False: [path.replace("*", "file.txt")],
    )
    def test_upload_single_big_file_in_file_set_attribute(self, upload_raw_data):
        # given
        exp_uuid = uuid.uuid4()
        swagger_mock = self._get_swagger_mock()
        upload_id = "placeholder"
        set_expected_result(
            swagger_mock.api.fileSetFileMultipartUploadStart,
            {
                "uploadId": upload_id,
                "errors": [],
            },
        )
        upload_raw_data.return_value = json.dumps(
            {
                "errors": [],
            }
        )
        data = self.get_random_bytes(8 * 2 ** 20)  # 8 MB
        chunk_size = self.multipart_config.min_chunk_size

        # when
        with create_file(content=data, binary_mode=True) as filename:
            upload_file_set_attribute(
                swagger_client=swagger_mock,
                container_id=str(exp_uuid),
                attribute="some/attribute",
                file_globs=[filename],
                reset=True,
                multipart_config=self.multipart_config,
            )

        # then
        swagger_mock.api.fileSetFileMultipartUploadPart.assert_not_called()
        swagger_mock.api.fileSetFileUpload.assert_not_called()
        swagger_mock.api.fileAtomMultipartUploadStart.assert_not_called()
        swagger_mock.api.fileAtomMultipartUploadFinish.assert_not_called()
        swagger_mock.api.fileAtomMultipartUploadPart.assert_not_called()
        swagger_mock.api.fileAtomUpload.assert_not_called()
        swagger_mock.api.fileSetFileMultipartUploadStart.assert_called_once_with(
            attribute="some/attribute",
            experimentIdentifier=str(exp_uuid),
            totalLength=len(data),
            subPath=os.path.basename(filename),
        )
        swagger_mock.api.fileSetFileMultipartUploadFinish.assert_called_once_with(
            attribute="some/attribute",
            experimentIdentifier=str(exp_uuid),
            subPath=os.path.basename(filename),
            uploadId=upload_id,
        )
        upload_raw_data.assert_has_calls(
            [
                call(
                    data=data[:chunk_size],
                    http_client=swagger_mock.swagger_spec.http_client,
                    url="https://ui.neptune.ai/attributes/storage/fileset/upload/part",
                    headers={"X-Range": f"bytes=0-{chunk_size - 1}/{len(data)}"},
                    query_params={
                        "uploadPartIdx": 0,
                        "uploadId": upload_id,
                        "subPath": os.path.basename(filename),
                        "experimentIdentifier": str(exp_uuid),
                        "attribute": "some/attribute",
                    },
                ),
                call(
                    data=data[chunk_size:],
                    http_client=swagger_mock.swagger_spec.http_client,
                    url="https://ui.neptune.ai/attributes/storage/fileset/upload/part",
                    headers={
                        "X-Range": f"bytes={chunk_size}-{len(data) - 1}/{len(data)}"
                    },
                    query_params={
                        "uploadPartIdx": 1,
                        "uploadId": upload_id,
                        "subPath": os.path.basename(filename),
                        "experimentIdentifier": str(exp_uuid),
                        "attribute": "some/attribute",
                    },
                ),
            ]
        )

    @unittest.skipIf(IS_WINDOWS, "Windows behaves strangely")
    @patch("neptune.new.internal.backends.hosted_file_operations.upload_raw_data")
    @patch(
        "neptune.new.internal.utils.glob",
        new=lambda path, recursive=False: [path.replace("*", "file.txt")],
    )
    def test_upload_multiple_files_in_file_set_attribute(self, upload_raw_data_mock):
        # given
        exp_uuid = str(uuid.uuid4())
        swagger_mock = self._get_swagger_mock()
        upload_raw_data_mock.return_value = b"null"
        swagger_mock.api.getUploadConfig.return_value.response.return_value.result.chunkSize = (
            10
        )

        # when
        with NamedTemporaryFile("w") as temp_file_1:
            with NamedTemporaryFile("w") as temp_file_2:
                upload_file_set_attribute(
                    swagger_client=swagger_mock,
                    container_id=exp_uuid,
                    attribute="some/attribute",
                    file_globs=[temp_file_1.name, temp_file_2.name],
                    reset=True,
                    multipart_config=self.multipart_config,
                )

        # then
        swagger_mock.api.fileSetFileMultipartUploadStart.assert_not_called()
        swagger_mock.api.fileSetFileMultipartUploadFinish.assert_not_called()
        swagger_mock.api.fileSetFileMultipartUploadPart.assert_not_called()
        swagger_mock.api.fileSetFileUpload.assert_not_called()
        swagger_mock.api.fileAtomMultipartUploadStart.assert_not_called()
        swagger_mock.api.fileAtomMultipartUploadFinish.assert_not_called()
        swagger_mock.api.fileAtomMultipartUploadPart.assert_not_called()
        swagger_mock.api.fileAtomUpload.assert_not_called()
        upload_raw_data_mock.assert_called_once_with(
            http_client=swagger_mock.swagger_spec.http_client,
            url="https://ui.neptune.ai/uploadFileSetTar",
            data=mock.ANY,
            headers={"Content-Type": "application/octet-stream"},
            query_params={
                "experimentId": str(exp_uuid),
                "attribute": "some/attribute",
                "reset": "True",
            },
        )


if __name__ == "__main__":
    unittest.main()
