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
import socket
import unittest
import uuid

from unittest.mock import call
from mock import MagicMock, patch
from packaging.version import Version

from neptune.alpha.exceptions import CannotResolveHostname, UnsupportedClientVersion, FileUploadError, \
    MetadataInconsistency
from neptune.alpha.internal.api_clients.hosted_neptune_api_client import HostedNeptuneApiClient
from neptune.alpha.internal.credentials import Credentials
from neptune.alpha.internal.operation import UploadFile, AssignString, LogFloats, UploadFileContent
from neptune.alpha.internal.utils import base64_encode
from tests.neptune.alpha.api_client_test_mixin import ApiClientTestMixin

API_TOKEN = 'eyJhcGlfYWRkcmVzcyI6Imh0dHBzOi8vYXBwLnN0YWdlLm5lcHR1bmUubWwiLCJ' \
            'hcGlfa2V5IjoiOTJhNzhiOWQtZTc3Ni00ODlhLWI5YzEtNzRkYmI1ZGVkMzAyIn0='

credentials = Credentials(API_TOKEN)


@patch('neptune.alpha.internal.api_clients.hosted_neptune_api_client.RequestsClient', new=MagicMock())
@patch('neptune.alpha.internal.api_clients.hosted_neptune_api_client.NeptuneAuthenticator', new=MagicMock())
@patch('bravado.client.SwaggerClient.from_url')
@patch('platform.platform', new=lambda: 'testPlatform')
@patch('platform.python_version', new=lambda: '3.9.test')
class TestHostedNeptuneBackend(unittest.TestCase, ApiClientTestMixin):
    # pylint:disable=protected-access

    @patch('neptune.alpha.internal.api_clients.hosted_neptune_api_client.upload_file_attribute')
    def test_execute_operations(self, upload_mock, swagger_client_factory):
        # given
        swagger_client = self._get_swagger_client_mock(swagger_client_factory)
        api_client = HostedNeptuneApiClient(credentials)
        exp_uuid = uuid.uuid4()

        response_error = MagicMock()
        response_error.errorDescription = "error1"
        swagger_client.api.executeOperations().response().result = [response_error]
        swagger_client.api.executeOperations.reset_mock()
        upload_mock.side_effect = FileUploadError("file1", "error2")
        some_text = "Some streamed text"
        some_binary = b"Some streamed binary"

        # when
        result = api_client.execute_operations(
            experiment_uuid=exp_uuid,
            operations=[
                UploadFile(
                    path=['some', 'files', 'some_file'],
                    file_name='path_to_file',
                    file_path='path_to_file'
                ),
                UploadFileContent(
                    path=['some', 'files', 'some_text_stream'],
                    file_name="stream.txt",
                    file_content=base64_encode(some_text.encode('utf-8'))
                ),
                UploadFileContent(
                    path=['some', 'files', 'some_binary_stream'],
                    file_name="stream.bin",
                    file_content=base64_encode(some_binary)
                ),
                LogFloats(["images", "img1"], [LogFloats.ValueType(1, 2, 3)]),
                AssignString(["properties", "name"], "some text"),
                UploadFile(
                    path=['some', 'other', 'file.txt'],
                    file_name="path.txt",
                    file_path='other/file/path.txt'
                )
            ]
        )

        # than
        swagger_client.api.executeOperations.assert_called_once_with(
            **{
                'experimentId': str(exp_uuid),
                'operations': [{
                    'path': "images/img1",
                    'logFloats': {
                        'entries': [{
                            'value': 1,
                            'step': 2,
                            'timestampMilliseconds': 3000
                        }]
                    }
                }, {
                    'path': "properties/name",
                    'assignString': {
                        'value': "some text"
                    }
                }]
            }
        )

        upload_mock.assert_has_calls([
            call(swagger_client=api_client.leaderboard_client,
                 experiment_uuid=exp_uuid,
                 attribute="some/other/file.txt",
                 source="other/file/path.txt",
                 target="path.txt"),
            call(swagger_client=api_client.leaderboard_client,
                 experiment_uuid=exp_uuid,
                 attribute="some/files/some_file",
                 source="path_to_file",
                 target="path_to_file"),
            call(swagger_client=api_client.leaderboard_client,
                 experiment_uuid=exp_uuid,
                 attribute="some/files/some_text_stream",
                 source=some_text.encode('utf-8'),
                 target="stream.txt"),
            call(swagger_client=api_client.leaderboard_client,
                 experiment_uuid=exp_uuid,
                 attribute="some/files/some_binary_stream",
                 source=some_binary,
                 target="stream.bin")
        ], any_order=True)

        self.assertEqual([
            FileUploadError("file1", "error2"),
            FileUploadError("file1", "error2"),
            FileUploadError("file1", "error2"),
            FileUploadError("file1", "error2"),
            MetadataInconsistency("error1")
        ], result)

    @patch('neptune.alpha.internal.api_clients.hosted_neptune_api_client.upload_file_attribute')
    def test_upload_files_destination_path(self, upload_mock, swagger_client_factory):
        # given
        self._get_swagger_client_mock(swagger_client_factory)
        api_client = HostedNeptuneApiClient(credentials)
        exp_uuid = uuid.uuid4()

        # when
        api_client.execute_operations(
            experiment_uuid=exp_uuid,
            operations=[
                UploadFile(
                    path=['some', 'path', '1', "var"],
                    file_name="file",
                    file_path='/path/to/file'
                ),
                UploadFile(
                    path=['some', 'path', '2', "var"],
                    file_name="with.dots.txt",
                    file_path='/some.file/with.dots.txt'
                ),
                UploadFile(
                    path=['some', 'path', '3', "var"],
                    file_name="some_image.jpeg",
                    file_path='/path/to/some_image.jpeg'
                )
            ]
        )

        upload_mock.assert_has_calls([
            call(swagger_client=api_client.leaderboard_client,
                 experiment_uuid=exp_uuid,
                 attribute="some/path/1/var",
                 source="/path/to/file",
                 target="file"),
            call(swagger_client=api_client.leaderboard_client,
                 experiment_uuid=exp_uuid,
                 attribute="some/path/2/var",
                 source="/some.file/with.dots.txt",
                 target="with.dots.txt"),
            call(swagger_client=api_client.leaderboard_client,
                 experiment_uuid=exp_uuid,
                 attribute="some/path/3/var",
                 source="/path/to/some_image.jpeg",
                 target="some_image.jpeg")
        ], any_order=True)

    @patch('neptune.alpha.internal.api_clients.hosted_neptune_api_client.neptune_client_version', Version('0.5.13'))
    def test_min_compatible_version_ok(self, swagger_client_factory):
        # given
        self._get_swagger_client_mock(swagger_client_factory, min_compatible='0.5.13')

        # expect
        HostedNeptuneApiClient(credentials)

    @patch('neptune.alpha.internal.api_clients.hosted_neptune_api_client.neptune_client_version', Version('0.5.13'))
    def test_min_compatible_version_fail(self, swagger_client_factory):
        # given
        self._get_swagger_client_mock(swagger_client_factory, min_compatible='0.5.14')

        # expect
        with self.assertRaises(UnsupportedClientVersion) as ex:
            HostedNeptuneApiClient(credentials)

        self.assertTrue("Please install neptune-client>=0.5.14" in str(ex.exception))

    @patch('neptune.alpha.internal.api_clients.hosted_neptune_api_client.neptune_client_version', Version('0.5.13'))
    def test_max_compatible_version_ok(self, swagger_client_factory):
        # given
        self._get_swagger_client_mock(swagger_client_factory, max_compatible='0.5.12')

        # expect
        HostedNeptuneApiClient(credentials)

    @patch('neptune.alpha.internal.api_clients.hosted_neptune_api_client.neptune_client_version', Version('0.5.13'))
    def test_max_compatible_version_fail(self, swagger_client_factory):
        # given
        self._get_swagger_client_mock(swagger_client_factory, max_compatible='0.4.999')

        # expect
        with self.assertRaises(UnsupportedClientVersion) as ex:
            HostedNeptuneApiClient(credentials)

        self.assertTrue("Please install neptune-client==0.4.0" in str(ex.exception))

    @patch('socket.gethostbyname')
    def test_cannot_resolve_host(self, gethostname_mock, _):
        # given
        gethostname_mock.side_effect = socket.gaierror

        # expect
        with self.assertRaises(CannotResolveHostname):
            HostedNeptuneApiClient(credentials)
