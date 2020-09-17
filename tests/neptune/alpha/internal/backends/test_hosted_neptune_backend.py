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
import io
import socket
import unittest
import uuid

from bravado.exception import HTTPNotFound
from mock import MagicMock, patch
from packaging.version import Version

from neptune.internal.storage.storage_utils import UploadEntry

from neptune.alpha.exceptions import CannotResolveHostname, UnsupportedClientVersion, ClientHttpError
from neptune.alpha.internal.backends.hosted_neptune_backend import HostedNeptuneBackend
from neptune.alpha.internal.credentials import Credentials
from neptune.alpha.internal.operation import UploadFile

API_TOKEN = 'eyJhcGlfYWRkcmVzcyI6Imh0dHBzOi8vYXBwLnN0YWdlLm5lcHR1bmUubWwiLCJ' \
            'hcGlfa2V5IjoiOTJhNzhiOWQtZTc3Ni00ODlhLWI5YzEtNzRkYmI1ZGVkMzAyIn0='

credentials = Credentials(API_TOKEN)


@patch('neptune.alpha.internal.backends.hosted_neptune_backend.RequestsClient', new=MagicMock())
@patch('neptune.alpha.internal.backends.hosted_neptune_backend.NeptuneAuthenticator', new=MagicMock())
@patch('bravado.client.SwaggerClient.from_url')
@patch('platform.platform', new=lambda: 'testPlatform')
@patch('platform.python_version', new=lambda: '3.9.test')
class TestHostedNeptuneBackend(unittest.TestCase):
    # pylint:disable=protected-access

    @patch('io.open')
    @patch('os.path.getsize', new=lambda _: 1024)
    def test_upload_files(self, open_mock, swagger_client_factory):
        # given
        self._get_swagger_client_mock(swagger_client_factory)

        open_file_mock = io.BytesIO(bytearray(1024))
        open_mock.return_value = open_file_mock

        backend = HostedNeptuneBackend(credentials)

        result_mock = MagicMock()
        result_mock.status_code = 200
        send_mock = MagicMock()
        send_mock.return_value = result_mock
        backend._http_client.session.send = send_mock

        # when
        backend.execute_operations(
            experiment_uuid=uuid.uuid4(),
            operations=[
                UploadFile(
                    path=['some', 'files', 'some_file'],
                    file_path='path_to_file'
                )
            ]
        )

    @patch('neptune.alpha.internal.backends.hosted_neptune_backend.upload_to_storage')
    def test_upload_files_destination_path(self, upload_mock, swagger_client_factory):
        # given
        self._get_swagger_client_mock(swagger_client_factory)
        backend = HostedNeptuneBackend(credentials)
        exp_uuid = uuid.uuid4()

        # when
        backend.execute_operations(
            experiment_uuid=exp_uuid,
            operations=[
                UploadFile(
                    path=['some', 'path', '1', "var"],
                    file_path='/path/to/file'
                ),
                UploadFile(
                    path=['some', 'path', '2', "var"],
                    file_path='/some.file/with.dots.txt'
                ),
                UploadFile(
                    path=['some', 'path', '3', "var"],
                    file_path='/path/to/some_image.jpeg'
                )
            ]
        )

        # then
        upload_mock.assert_called_once_with(
            upload_entries=[
                UploadEntry("/path/to/file", "some/path/1/var"),
                UploadEntry("/some.file/with.dots.txt", "some/path/2/var.txt"),
                UploadEntry("/path/to/some_image.jpeg", "some/path/3/var.jpeg"),
            ],
            http_client=backend._http_client,
            file_url="ui.neptune.ai/api/leaderboard/v1/storage/legacy/uploadOutput/{experimentId}",
            tar_files_url="ui.neptune.ai/api/leaderboard/v1/storage/legacy/uploadOutputAsTarStream/{experimentId}",
            experiment_uuid=exp_uuid
        )

    @patch('io.open')
    @patch('os.path.getsize', new=lambda _: 8)
    def test_upload_files_not_found(self, open_mock, swagger_client_factory):
        # given
        self._get_swagger_client_mock(swagger_client_factory)

        open_file_mock = io.BytesIO(bytearray(8))
        open_mock.return_value = open_file_mock

        backend = HostedNeptuneBackend(credentials)

        result_mock = MagicMock()
        result_mock.status_code = 404
        result_mock.raise_for_status.side_effect = HTTPNotFound(MagicMock())
        send_mock = MagicMock()
        send_mock.return_value = result_mock
        backend._http_client.session.send = send_mock

        # expect
        with self.assertRaises(ClientHttpError):
            backend.execute_operations(
                experiment_uuid=uuid.uuid4(),
                operations=[
                    UploadFile(
                        path=['some', 'files', 'some_file'],
                        file_path='path_to_file'
                    )
                ]
            )
        send_mock.assert_called_once()

    @patch('neptune.alpha.internal.backends.hosted_neptune_backend.neptune_client_version', Version('0.5.13'))
    def test_min_compatible_version_ok(self, swagger_client_factory):
        # given
        self._get_swagger_client_mock(swagger_client_factory, min_compatible='0.5.13')

        # expect
        HostedNeptuneBackend(credentials)

    @patch('neptune.alpha.internal.backends.hosted_neptune_backend.neptune_client_version', Version('0.5.13'))
    def test_min_compatible_version_fail(self, swagger_client_factory):
        # given
        self._get_swagger_client_mock(swagger_client_factory, min_compatible='0.5.14')

        # expect
        with self.assertRaises(UnsupportedClientVersion) as ex:
            HostedNeptuneBackend(credentials)

        self.assertTrue("Please install neptune-client>=0.5.14" in str(ex.exception))

    @patch('neptune.alpha.internal.backends.hosted_neptune_backend.neptune_client_version', Version('0.5.13'))
    def test_max_compatible_version_ok(self, swagger_client_factory):
        # given
        self._get_swagger_client_mock(swagger_client_factory, max_compatible='0.5.12')

        # expect
        HostedNeptuneBackend(credentials)

    @patch('neptune.alpha.internal.backends.hosted_neptune_backend.neptune_client_version', Version('0.5.13'))
    def test_max_compatible_version_fail(self, swagger_client_factory):
        # given
        self._get_swagger_client_mock(swagger_client_factory, max_compatible='0.4.999')

        # expect
        with self.assertRaises(UnsupportedClientVersion) as ex:
            HostedNeptuneBackend(credentials)

        self.assertTrue("Please install neptune-client==0.4.0" in str(ex.exception))

    @patch('socket.gethostbyname')
    def test_cannot_resolve_host(self, gethostname_mock, _):
        # given
        gethostname_mock.side_effect = socket.gaierror

        # expect
        with self.assertRaises(CannotResolveHostname):
            HostedNeptuneBackend(credentials)

    @staticmethod
    def _get_swagger_client_mock(
            swagger_client_factory,
            min_recommended=None,
            min_compatible=None,
            max_compatible=None):
        py_lib_versions = type('py_lib_versions', (object,), {})()
        setattr(py_lib_versions, "minRecommendedVersion", min_recommended)
        setattr(py_lib_versions, "minCompatibleVersion", min_compatible)
        setattr(py_lib_versions, "maxCompatibleVersion", max_compatible)

        client_config = type('client_config_response_result', (object,), {})()
        setattr(client_config, "pyLibVersions", py_lib_versions)
        setattr(client_config, "apiUrl", "ui.neptune.ai")
        setattr(client_config, "applicationUrl", "ui.neptune.ai")

        swagger_client = MagicMock()
        swagger_client.api.getClientConfig.return_value.response.return_value.result = client_config
        swagger_client.api.uploadOutputUsingPOST.operation.path_name = \
            "/api/leaderboard/v1/storage/legacy/uploadOutput/{experimentId}"
        swagger_client.api.uploadOutputAsTarStreamUsingPOST.operation.path_name = \
            "/api/leaderboard/v1/storage/legacy/uploadOutputAsTarStream/{experimentId}"
        swagger_client_factory.return_value = swagger_client

        return swagger_client
