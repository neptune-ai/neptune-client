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
import time
import unittest
import uuid
from pathlib import Path
from unittest.mock import (
    Mock,
    call,
)

import pytest
from bravado.exception import (
    HTTPNotFound,
    HTTPPaymentRequired,
    HTTPTooManyRequests,
    HTTPUnprocessableEntity,
)
from mock import (
    MagicMock,
    patch,
)
from packaging.version import Version

from neptune.core.components.operation_storage import OperationStorage
from neptune.exceptions import (
    CannotResolveHostname,
    FileSetNotFound,
    FileUploadError,
    MetadataInconsistency,
    NeptuneClientUpgradeRequiredError,
    NeptuneLimitExceedException,
)
from neptune.internal.backends.hosted_client import (
    DEFAULT_REQUEST_KWARGS,
    _get_token_client,
    create_artifacts_client,
    create_backend_client,
    create_http_client_with_auth,
    create_leaderboard_client,
    get_client_config,
)
from neptune.internal.backends.hosted_neptune_backend import HostedNeptuneBackend
from neptune.internal.backends.swagger_client_wrapper import SwaggerClientWrapper
from neptune.internal.backends.utils import verify_host_resolution
from neptune.internal.container_type import ContainerType
from neptune.internal.credentials import Credentials
from neptune.internal.operation import (
    AssignString,
    LogFloats,
    TrackFilesToArtifact,
    UploadFile,
    UploadFileContent,
)
from neptune.internal.utils import base64_encode
from tests.unit.neptune.backend_test_mixin import BackendTestMixin
from tests.unit.neptune.new.utils import response_mock

API_TOKEN = (
    "eyJhcGlfYWRkcmVzcyI6Imh0dHBzOi8vYXBwLnN0YWdlLm5lcHR1bmUuYWkiLCJ"
    "hcGlfa2V5IjoiOTJhNzhiOWQtZTc3Ni00ODlhLWI5YzEtNzRkYmI1ZGVkMzAyIn0="
)

credentials = Credentials.from_token(API_TOKEN)


@patch("neptune.internal.backends.hosted_client.RequestsClient", new=MagicMock())
@patch("neptune.internal.backends.hosted_client.NeptuneAuthenticator", new=MagicMock())
@patch("bravado.client.SwaggerClient.from_url")
@patch("platform.platform", new=lambda: "testPlatform")
@patch("platform.python_version", new=lambda: "3.9.test")
class TestHostedNeptuneBackend(unittest.TestCase, BackendTestMixin):
    def setUp(self) -> None:
        # Clear all LRU storage
        verify_host_resolution.cache_clear()
        _get_token_client.cache_clear()
        get_client_config.cache_clear()
        create_http_client_with_auth.cache_clear()
        create_backend_client.cache_clear()
        create_leaderboard_client.cache_clear()
        create_artifacts_client.cache_clear()

        self.container_types = [ContainerType.RUN, ContainerType.PROJECT]
        self.dummy_operation_storage = OperationStorage(Path("./tests/dummy_storage"))

    @patch("neptune.internal.backends.hosted_neptune_backend.upload_file_attribute")
    @patch("socket.gethostbyname", MagicMock(return_value="1.1.1.1"))
    def test_execute_operations(self, upload_mock, swagger_client_factory):
        # given
        swagger_client = self._get_swagger_client_mock(swagger_client_factory)
        backend = HostedNeptuneBackend(credentials)
        container_uuid = str(uuid.uuid4())

        response_error = MagicMock()
        response_error.errorDescription = "error1"
        swagger_client.api.executeOperations().response().result = [response_error]
        swagger_client.api.executeOperations.reset_mock()
        upload_mock.return_value = [FileUploadError("file1", "error2")]
        some_text = "Some streamed text"
        some_binary = b"Some streamed binary"

        for container_type in self.container_types:
            with self.subTest(msg=f"For type {container_type.value}"):
                upload_mock.reset_mock()
                swagger_client_factory.reset_mock()

                # when
                result = backend.execute_operations(
                    container_id=container_uuid,
                    container_type=container_type,
                    operations=[
                        UploadFile(
                            path=["some", "files", "some_file"],
                            ext="",
                            file_path="path_to_file",
                        ),
                        UploadFileContent(
                            path=["some", "files", "some_text_stream"],
                            ext="txt",
                            file_content=base64_encode(some_text.encode("utf-8")),
                        ),
                        UploadFileContent(
                            path=["some", "files", "some_binary_stream"],
                            ext="bin",
                            file_content=base64_encode(some_binary),
                        ),
                        LogFloats(["images", "img1"], [LogFloats.ValueType(1, 2, 3)]),
                        AssignString(["properties", "name"], "some text"),
                        UploadFile(
                            path=["some", "other", "file.txt"],
                            ext="txt",
                            file_path="other/file/path.txt",
                        ),
                    ],
                    operation_storage=self.dummy_operation_storage,
                )

                # then
                swagger_client.api.executeOperations.assert_called_once_with(
                    **{
                        "experimentId": str(container_uuid),
                        "operations": [
                            {
                                "path": "images/img1",
                                "logFloats": {
                                    "entries": [
                                        {
                                            "value": 1,
                                            "step": 2,
                                            "timestampMilliseconds": 3000,
                                        }
                                    ]
                                },
                            },
                            {
                                "path": "properties/name",
                                "assignString": {"value": "some text"},
                            },
                        ],
                        **DEFAULT_REQUEST_KWARGS,
                    }
                )

                upload_mock.assert_has_calls(
                    [
                        call(
                            swagger_client=backend.leaderboard_client,
                            container_id=container_uuid,
                            attribute="some/other/file.txt",
                            source="other/file/path.txt",
                            ext="txt",
                            multipart_config=backend._client_config.multipart_config,
                        ),
                        call(
                            swagger_client=backend.leaderboard_client,
                            container_id=container_uuid,
                            attribute="some/files/some_file",
                            source="path_to_file",
                            ext="",
                            multipart_config=backend._client_config.multipart_config,
                        ),
                        call(
                            swagger_client=backend.leaderboard_client,
                            container_id=container_uuid,
                            attribute="some/files/some_text_stream",
                            source=some_text.encode("utf-8"),
                            ext="txt",
                            multipart_config=backend._client_config.multipart_config,
                        ),
                        call(
                            swagger_client=backend.leaderboard_client,
                            container_id=container_uuid,
                            attribute="some/files/some_binary_stream",
                            source=some_binary,
                            ext="bin",
                            multipart_config=backend._client_config.multipart_config,
                        ),
                    ],
                    any_order=True,
                )

                self.assertEqual(
                    (
                        6,
                        [
                            FileUploadError("file1", "error2"),
                            FileUploadError("file1", "error2"),
                            FileUploadError("file1", "error2"),
                            FileUploadError("file1", "error2"),
                            MetadataInconsistency("error1"),
                        ],
                    ),
                    result,
                )

    @pytest.mark.asyncio
    @patch("socket.gethostbyname", MagicMock(return_value="1.1.1.1"))
    async def test_too_many_requests(self, swagger_client_factory):
        # given
        swagger_client = self._get_swagger_client_mock(swagger_client_factory)
        backend = HostedNeptuneBackend(credentials)
        container_uuid = str(uuid.uuid4())

        container_type = ContainerType.RUN

        response = MagicMock()
        response.response().return_value = []

        retry_after_seconds = 5  # Przykładowy czas oczekiwania
        too_many_requests_response = HTTPTooManyRequests(MagicMock())
        too_many_requests_response.headers = {"retry-after": str(retry_after_seconds)}

        swagger_client.api.executeOperations.side_effect = Mock(
            side_effect=[too_many_requests_response, response_mock()]
        )

        # when
        result_start_time = time.time()
        result = await backend.execute_async(  # Użyj await, aby poczekać na wykonanie coroutine
            container_id=container_uuid,
            container_type=container_type,
            operations=[
                LogFloats(["images", "img1"], [LogFloats.ValueType(1, 2, 3)]),
            ],
            operation_storage=self.dummy_operation_storage,
        )
        result_end_time = time.time()

        # then
        self.assertEqual(result, (1, []))
        assert retry_after_seconds <= (result_end_time - result_start_time) <= (retry_after_seconds * 2)

    @patch("socket.gethostbyname", MagicMock(return_value="1.1.1.1"))
    def test_execute_operations_retry_request(self, swagger_client_factory):
        # given
        swagger_client = self._get_swagger_client_mock(swagger_client_factory)
        backend = HostedNeptuneBackend(credentials)
        container_uuid = str(uuid.uuid4())

        container_type = ContainerType.RUN

        response = MagicMock()
        response.response().return_value = []
        swagger_client.api.executeOperations.side_effect = Mock(
            side_effect=[HTTPTooManyRequests(MagicMock()), response_mock()]
        )

        # when
        result = backend.execute_operations(
            container_id=container_uuid,
            container_type=container_type,
            operations=[
                LogFloats(["images", "img1"], [LogFloats.ValueType(1, 2, 3)]),
            ],
            operation_storage=self.dummy_operation_storage,
        )

        # then
        self.assertEqual(result, (1, []))
        execution_operation_call = call(
            **{
                "experimentId": str(container_uuid),
                "operations": [
                    {
                        "path": "images/img1",
                        "logFloats": {
                            "entries": [
                                {
                                    "value": 1,
                                    "step": 2,
                                    "timestampMilliseconds": 3000,
                                }
                            ]
                        },
                    }
                ],
                **DEFAULT_REQUEST_KWARGS,
            }
        )
        swagger_client.api.executeOperations.assert_has_calls([execution_operation_call, execution_operation_call])

    @patch("neptune.internal.backends.hosted_neptune_backend.upload_file_attribute")
    @patch("socket.gethostbyname", MagicMock(return_value="1.1.1.1"))
    def test_upload_files_destination_path(self, upload_mock, swagger_client_factory):
        # given
        self._get_swagger_client_mock(swagger_client_factory)
        backend = HostedNeptuneBackend(credentials)
        container_uuid = str(uuid.uuid4())

        for container_type in self.container_types:
            with self.subTest(msg=f"For type {container_type.value}"):
                upload_mock.reset_mock()
                swagger_client_factory.reset_mock()

                # when
                backend.execute_operations(
                    container_id=container_uuid,
                    container_type=container_type,
                    operations=[
                        UploadFile(
                            path=["some", "path", "1", "var"],
                            ext="",
                            file_path="/path/to/file",
                        ),
                        UploadFile(
                            path=["some", "path", "2", "var"],
                            ext="txt",
                            file_path="/some.file/with.dots.txt",
                        ),
                        UploadFile(
                            path=["some", "path", "3", "var"],
                            ext="jpeg",
                            file_path="/path/to/some_image.jpeg",
                        ),
                    ],
                    operation_storage=self.dummy_operation_storage,
                )

                # then
                upload_mock.assert_has_calls(
                    [
                        call(
                            swagger_client=backend.leaderboard_client,
                            container_id=container_uuid,
                            attribute="some/path/1/var",
                            source="/path/to/file",
                            ext="",
                            multipart_config=backend._client_config.multipart_config,
                        ),
                        call(
                            swagger_client=backend.leaderboard_client,
                            container_id=container_uuid,
                            attribute="some/path/2/var",
                            source="/some.file/with.dots.txt",
                            ext="txt",
                            multipart_config=backend._client_config.multipart_config,
                        ),
                        call(
                            swagger_client=backend.leaderboard_client,
                            container_id=container_uuid,
                            attribute="some/path/3/var",
                            source="/path/to/some_image.jpeg",
                            ext="jpeg",
                            multipart_config=backend._client_config.multipart_config,
                        ),
                    ],
                    any_order=True,
                )

    @patch("neptune.internal.backends.hosted_neptune_backend.track_to_new_artifact")
    @patch("socket.gethostbyname", MagicMock(return_value="1.1.1.1"))
    def test_track_to_new_artifact(self, track_to_new_artifact_mock, swagger_client_factory):
        # given
        swagger_client = self._get_swagger_client_mock(swagger_client_factory)
        backend = HostedNeptuneBackend(credentials)
        container_id = str(uuid.uuid4())
        project_id = str(uuid.uuid4())

        response_error = MagicMock()
        response_error.errorDescription = "error1"
        swagger_client.api.executeOperations.return_value.response.return_value.result = [response_error]
        swagger_client.api.getArtifactAttribute.side_effect = HTTPNotFound(response=response_mock())
        swagger_client_wrapper = SwaggerClientWrapper(swagger_client)

        for container_type in self.container_types:
            with self.subTest(msg=f"For type {container_type.value}"):
                track_to_new_artifact_mock.reset_mock()
                swagger_client_factory.reset_mock()

                # when
                backend.execute_operations(
                    container_id=container_id,
                    container_type=container_type,
                    operations=[
                        TrackFilesToArtifact(
                            path=["sub", "one"],
                            project_id=project_id,
                            entries=[("/path/to/file", "/path/to")],
                        ),
                        TrackFilesToArtifact(
                            path=["sub", "two"],
                            project_id=project_id,
                            entries=[
                                ("/path/to/file1", None),
                                ("/path/to/file2", None),
                            ],
                        ),
                        TrackFilesToArtifact(
                            path=["sub", "three"],
                            project_id=project_id,
                            entries=[("/path/to/file1", None)],
                        ),
                        TrackFilesToArtifact(
                            path=["sub", "three"],
                            project_id=project_id,
                            entries=[("/path/to/file2", None)],
                        ),
                    ],
                    operation_storage=self.dummy_operation_storage,
                )

                # then
                track_to_new_artifact_mock.assert_has_calls(
                    [
                        call(
                            swagger_client=swagger_client_wrapper,
                            project_id=project_id,
                            path=["sub", "one"],
                            parent_identifier=str(container_id),
                            entries=[("/path/to/file", "/path/to")],
                            default_request_params=DEFAULT_REQUEST_KWARGS,
                            exclude_directory_files=True,
                            exclude_metadata_from_hash=True,
                        ),
                        call(
                            swagger_client=swagger_client_wrapper,
                            project_id=project_id,
                            path=["sub", "two"],
                            parent_identifier=str(container_id),
                            entries=[
                                ("/path/to/file1", None),
                                ("/path/to/file2", None),
                            ],
                            default_request_params=DEFAULT_REQUEST_KWARGS,
                            exclude_directory_files=True,
                            exclude_metadata_from_hash=True,
                        ),
                        call(
                            swagger_client=swagger_client_wrapper,
                            project_id=project_id,
                            path=["sub", "three"],
                            parent_identifier=str(container_id),
                            entries=[
                                ("/path/to/file1", None),
                                ("/path/to/file2", None),
                            ],
                            default_request_params=DEFAULT_REQUEST_KWARGS,
                            exclude_directory_files=True,
                            exclude_metadata_from_hash=True,
                        ),
                    ],
                    any_order=True,
                )

    @patch("neptune.internal.backends.hosted_neptune_backend.track_to_existing_artifact")
    @patch("socket.gethostbyname", MagicMock(return_value="1.1.1.1"))
    def test_track_to_existing_artifact(self, track_to_existing_artifact_mock, swagger_client_factory):
        # given
        swagger_client = self._get_swagger_client_mock(swagger_client_factory)
        backend = HostedNeptuneBackend(credentials)
        container_id = str(uuid.uuid4())
        project_id = str(uuid.uuid4())

        response_error = MagicMock()
        response_error.errorDescription = "error1"
        swagger_client.api.executeOperations.return_value.response.return_value.result = [response_error]
        swagger_client.api.getArtifactAttribute.return_value.response.return_value.result.hash = "dummyHash"
        swagger_client_wrapper = SwaggerClientWrapper(swagger_client)

        for container_type in self.container_types:
            track_to_existing_artifact_mock.reset_mock()
            swagger_client_factory.reset_mock()

            with self.subTest(msg=f"For type {container_type.value}"):
                track_to_existing_artifact_mock.reset_mock()
                swagger_client_factory.reset_mock()

                # when
                backend.execute_operations(
                    container_id=container_id,
                    container_type=container_type,
                    operations=[
                        TrackFilesToArtifact(
                            path=["sub", "one"],
                            project_id=project_id,
                            entries=[("/path/to/file", "/path/to")],
                        ),
                        TrackFilesToArtifact(
                            path=["sub", "two"],
                            project_id=project_id,
                            entries=[
                                ("/path/to/file1", None),
                                ("/path/to/file2", None),
                            ],
                        ),
                        TrackFilesToArtifact(
                            path=["sub", "three"],
                            project_id=project_id,
                            entries=[("/path/to/file1", None)],
                        ),
                        TrackFilesToArtifact(
                            path=["sub", "three"],
                            project_id=project_id,
                            entries=[("/path/to/file2", None)],
                        ),
                    ],
                    operation_storage=self.dummy_operation_storage,
                )

                # then
                track_to_existing_artifact_mock.assert_has_calls(
                    [
                        call(
                            swagger_client=swagger_client_wrapper,
                            project_id=project_id,
                            path=["sub", "one"],
                            artifact_hash="dummyHash",
                            parent_identifier=str(container_id),
                            entries=[("/path/to/file", "/path/to")],
                            default_request_params=DEFAULT_REQUEST_KWARGS,
                            exclude_directory_files=True,
                        ),
                        call(
                            swagger_client=swagger_client_wrapper,
                            project_id=project_id,
                            path=["sub", "two"],
                            artifact_hash="dummyHash",
                            parent_identifier=str(container_id),
                            entries=[
                                ("/path/to/file1", None),
                                ("/path/to/file2", None),
                            ],
                            default_request_params=DEFAULT_REQUEST_KWARGS,
                            exclude_directory_files=True,
                        ),
                        call(
                            swagger_client=swagger_client_wrapper,
                            project_id=project_id,
                            path=["sub", "three"],
                            artifact_hash="dummyHash",
                            parent_identifier=str(container_id),
                            entries=[
                                ("/path/to/file1", None),
                                ("/path/to/file2", None),
                            ],
                            default_request_params=DEFAULT_REQUEST_KWARGS,
                            exclude_directory_files=True,
                        ),
                    ],
                    any_order=True,
                )

    @patch(
        "neptune.internal.backends.hosted_client.neptune_client_version",
        Version("0.5.13"),
    )
    @patch("socket.gethostbyname", MagicMock(return_value="1.1.1.1"))
    def test_min_compatible_version_ok(self, swagger_client_factory):
        # given
        self._get_swagger_client_mock(swagger_client_factory, min_compatible="0.5.13")

        # expect
        HostedNeptuneBackend(credentials)

    @patch(
        "neptune.internal.backends.hosted_client.neptune_client_version",
        Version("0.5.13"),
    )
    @patch("socket.gethostbyname", MagicMock(return_value="1.1.1.1"))
    def test_min_compatible_version_fail(self, swagger_client_factory):
        # given
        self._get_swagger_client_mock(swagger_client_factory, min_compatible="0.5.14")

        # expect
        with self.assertRaises(NeptuneClientUpgradeRequiredError) as ex:
            HostedNeptuneBackend(credentials)

        self.assertTrue("minimum required version is >=0.5.14" in str(ex.exception))

    @patch(
        "neptune.internal.backends.hosted_client.neptune_client_version",
        Version("0.5.13"),
    )
    @patch("socket.gethostbyname", MagicMock(return_value="1.1.1.1"))
    def test_max_compatible_version_ok(self, swagger_client_factory):
        # given
        self._get_swagger_client_mock(swagger_client_factory, max_compatible="0.5.12")

        # expect
        HostedNeptuneBackend(credentials)

    @patch(
        "neptune.internal.backends.hosted_client.neptune_client_version",
        Version("0.5.13"),
    )
    @patch("socket.gethostbyname", MagicMock(return_value="1.1.1.1"))
    def test_max_compatible_version_fail(self, swagger_client_factory):
        # given
        self._get_swagger_client_mock(swagger_client_factory, max_compatible="0.4.999")

        # expect
        with self.assertRaises(NeptuneClientUpgradeRequiredError) as ex:
            HostedNeptuneBackend(credentials)

        self.assertTrue("minimum required version is ==0.4.0" in str(ex.exception))

    @patch("socket.gethostbyname")
    def test_cannot_resolve_host(self, gethostname_mock, _):
        # given
        gethostname_mock.side_effect = socket.gaierror

        # expect
        with self.assertRaises(CannotResolveHostname):
            HostedNeptuneBackend(credentials)

    @patch("socket.gethostbyname", MagicMock(return_value="1.1.1.1"))
    def test_limit_exceed(self, swagger_client_factory):
        # given
        swagger_client = self._get_swagger_client_mock(swagger_client_factory)
        backend = HostedNeptuneBackend(credentials)
        container_uuid = str(uuid.uuid4())

        # when:
        error = response_mock()
        error.json.return_value = {"title": "Maximum storage limit reached"}
        swagger_client.api.executeOperations.side_effect = HTTPPaymentRequired(response=error)

        # then:
        for container_type in self.container_types:
            with self.subTest(msg=f"For type {container_type.value}"):
                with self.assertRaises(NeptuneLimitExceedException):
                    backend.execute_operations(
                        container_id=container_uuid,
                        container_type=container_type,
                        operations=[
                            LogFloats(["float1"], [LogFloats.ValueType(1, 2, 3)]),
                        ],
                        operation_storage=self.dummy_operation_storage,
                    )

    @patch("socket.gethostbyname", MagicMock(return_value="1.1.1.1"))
    def test_limit_exceed_legacy(self, swagger_client_factory):
        # given
        swagger_client = self._get_swagger_client_mock(swagger_client_factory)
        backend = HostedNeptuneBackend(credentials)
        container_uuid = str(uuid.uuid4())

        # when:
        error = response_mock()
        error.json.return_value = {"title": "Monitoring hours not left"}
        swagger_client.api.executeOperations.side_effect = HTTPUnprocessableEntity(response=error)

        # then:
        for container_type in self.container_types:
            with self.subTest(msg=f"For type {container_type.value}"):
                with self.assertRaises(NeptuneLimitExceedException):
                    backend.execute_operations(
                        container_id=container_uuid,
                        container_type=container_type,
                        operations=[
                            LogFloats(["float1"], [LogFloats.ValueType(1, 2, 3)]),
                        ],
                        operation_storage=self.dummy_operation_storage,
                    )

    @patch("socket.gethostbyname", MagicMock(return_value="1.1.1.1"))
    def test_list_fileset_files_exception(self, swagger_client_factory):
        # given
        self._get_swagger_client_mock(swagger_client_factory)
        backend = HostedNeptuneBackend(credentials)
        mock_leaderboard_client = MagicMock()
        mock_leaderboard_client.api.lsFileSetAttribute.side_effect = HTTPNotFound(response_mock())

        backend.leaderboard_client = mock_leaderboard_client

        # then
        with pytest.raises(FileSetNotFound):
            backend.list_fileset_files(["mock"], "mock", ".")
