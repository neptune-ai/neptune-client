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

from bravado.exception import HTTPNotFound, HTTPPaymentRequired, HTTPUnprocessableEntity
from mock import MagicMock, patch
from packaging.version import Version

from neptune.new.exceptions import (
    CannotResolveHostname,
    FileUploadError,
    MetadataInconsistency,
    UnsupportedClientVersion,
    NeptuneLimitExceedException,
)
from neptune.new.internal.backends.hosted_neptune_backend import HostedNeptuneBackend
from neptune.new.internal.backends.hosted_client import (
    DEFAULT_REQUEST_KWARGS,
    _get_token_client,  # pylint:disable=protected-access
    get_client_config,
    create_http_client_with_auth,
    create_backend_client,
    create_leaderboard_client,
    create_artifacts_client,
)
from neptune.new.internal.credentials import Credentials
from neptune.new.internal.operation import (
    AssignString,
    LogFloats,
    TrackFilesToArtifact,
    UploadFile,
    UploadFileContent,
)
from neptune.new.internal.backends.utils import verify_host_resolution
from neptune.new.internal.utils import base64_encode
from tests.neptune.new.backend_test_mixin import BackendTestMixin

API_TOKEN = (
    "eyJhcGlfYWRkcmVzcyI6Imh0dHBzOi8vYXBwLnN0YWdlLm5lcHR1bmUubWwiLCJ"
    "hcGlfa2V5IjoiOTJhNzhiOWQtZTc3Ni00ODlhLWI5YzEtNzRkYmI1ZGVkMzAyIn0="
)

credentials = Credentials.from_token(API_TOKEN)


@patch("neptune.new.internal.backends.hosted_client.RequestsClient", new=MagicMock())
@patch(
    "neptune.new.internal.backends.hosted_client.NeptuneAuthenticator", new=MagicMock()
)
@patch("bravado.client.SwaggerClient.from_url")
@patch("platform.platform", new=lambda: "testPlatform")
@patch("platform.python_version", new=lambda: "3.9.test")
class TestHostedNeptuneBackend(unittest.TestCase, BackendTestMixin):
    # pylint:disable=protected-access

    def setUp(self) -> None:
        # Clear all LRU storage
        verify_host_resolution.cache_clear()
        _get_token_client.cache_clear()
        get_client_config.cache_clear()
        create_http_client_with_auth.cache_clear()
        create_backend_client.cache_clear()
        create_leaderboard_client.cache_clear()
        create_artifacts_client.cache_clear()

    @patch("neptune.new.internal.backends.hosted_neptune_backend.upload_file_attribute")
    def test_execute_operations(self, upload_mock, swagger_client_factory):
        # given
        swagger_client = self._get_swagger_client_mock(swagger_client_factory)
        backend = HostedNeptuneBackend(credentials)
        exp_uuid = str(uuid.uuid4())

        response_error = MagicMock()
        response_error.errorDescription = "error1"
        swagger_client.api.executeOperations().response().result = [response_error]
        swagger_client.api.executeOperations.reset_mock()
        upload_mock.return_value = FileUploadError("file1", "error2")
        some_text = "Some streamed text"
        some_binary = b"Some streamed binary"

        # when
        result = backend.execute_operations(
            run_id=exp_uuid,
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
        )

        # than
        swagger_client.api.executeOperations.assert_called_once_with(
            **{
                "experimentId": str(exp_uuid),
                "operations": [
                    {
                        "path": "images/img1",
                        "logFloats": {
                            "entries": [
                                {"value": 1, "step": 2, "timestampMilliseconds": 3000}
                            ]
                        },
                    },
                    {"path": "properties/name", "assignString": {"value": "some text"}},
                ],
                **DEFAULT_REQUEST_KWARGS,
            }
        )

        upload_mock.assert_has_calls(
            [
                call(
                    swagger_client=backend.leaderboard_client,
                    run_id=exp_uuid,
                    attribute="some/other/file.txt",
                    source="other/file/path.txt",
                    ext="txt",
                ),
                call(
                    swagger_client=backend.leaderboard_client,
                    run_id=exp_uuid,
                    attribute="some/files/some_file",
                    source="path_to_file",
                    ext="",
                ),
                call(
                    swagger_client=backend.leaderboard_client,
                    run_id=exp_uuid,
                    attribute="some/files/some_text_stream",
                    source=some_text.encode("utf-8"),
                    ext="txt",
                ),
                call(
                    swagger_client=backend.leaderboard_client,
                    run_id=exp_uuid,
                    attribute="some/files/some_binary_stream",
                    source=some_binary,
                    ext="bin",
                ),
            ],
            any_order=True,
        )

        self.assertEqual(
            [
                FileUploadError("file1", "error2"),
                FileUploadError("file1", "error2"),
                FileUploadError("file1", "error2"),
                FileUploadError("file1", "error2"),
                MetadataInconsistency("error1"),
            ],
            result,
        )

    @patch("neptune.new.internal.backends.hosted_neptune_backend.upload_file_attribute")
    def test_upload_files_destination_path(self, upload_mock, swagger_client_factory):
        # given
        self._get_swagger_client_mock(swagger_client_factory)
        backend = HostedNeptuneBackend(credentials)
        exp_uuid = str(uuid.uuid4())

        # when
        backend.execute_operations(
            run_id=exp_uuid,
            operations=[
                UploadFile(
                    path=["some", "path", "1", "var"], ext="", file_path="/path/to/file"
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
        )

        upload_mock.assert_has_calls(
            [
                call(
                    swagger_client=backend.leaderboard_client,
                    run_id=exp_uuid,
                    attribute="some/path/1/var",
                    source="/path/to/file",
                    ext="",
                ),
                call(
                    swagger_client=backend.leaderboard_client,
                    run_id=exp_uuid,
                    attribute="some/path/2/var",
                    source="/some.file/with.dots.txt",
                    ext="txt",
                ),
                call(
                    swagger_client=backend.leaderboard_client,
                    run_id=exp_uuid,
                    attribute="some/path/3/var",
                    source="/path/to/some_image.jpeg",
                    ext="jpeg",
                ),
            ],
            any_order=True,
        )

    @patch("neptune.new.internal.backends.hosted_neptune_backend.track_to_new_artifact")
    def test_track_to_new_artifact(
        self, track_to_new_artifact_mock, swagger_client_factory
    ):
        # given
        swagger_client = self._get_swagger_client_mock(swagger_client_factory)
        backend = HostedNeptuneBackend(credentials)
        exp_id = str(uuid.uuid4())
        project_id = str(uuid.uuid4())

        response_error = MagicMock()
        response_error.errorDescription = "error1"
        swagger_client.api.executeOperations.return_value.response.return_value.result = [
            response_error
        ]
        swagger_client.api.getArtifactAttribute.side_effect = HTTPNotFound(
            response=MagicMock()
        )

        # when
        backend.execute_operations(
            run_id=exp_id,
            operations=[
                TrackFilesToArtifact(
                    path=["sub", "one"],
                    project_id=project_id,
                    entries=[("/path/to/file", "/path/to")],
                ),
                TrackFilesToArtifact(
                    path=["sub", "two"],
                    project_id=project_id,
                    entries=[("/path/to/file1", None), ("/path/to/file2", None)],
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
        )

        # then
        track_to_new_artifact_mock.assert_has_calls(
            [
                call(
                    swagger_client=swagger_client,
                    project_id=project_id,
                    path=["sub", "one"],
                    parent_identifier=str(exp_id),
                    entries=[("/path/to/file", "/path/to")],
                    default_request_params=DEFAULT_REQUEST_KWARGS,
                ),
                call(
                    swagger_client=swagger_client,
                    project_id=project_id,
                    path=["sub", "two"],
                    parent_identifier=str(exp_id),
                    entries=[("/path/to/file1", None), ("/path/to/file2", None)],
                    default_request_params=DEFAULT_REQUEST_KWARGS,
                ),
                call(
                    swagger_client=swagger_client,
                    project_id=project_id,
                    path=["sub", "three"],
                    parent_identifier=str(exp_id),
                    entries=[("/path/to/file1", None), ("/path/to/file2", None)],
                    default_request_params=DEFAULT_REQUEST_KWARGS,
                ),
            ],
            any_order=True,
        )

    @patch(
        "neptune.new.internal.backends.hosted_neptune_backend.track_to_existing_artifact"
    )
    def test_track_to_existing_artifact(
        self, track_to_existing_artifact_mock, swagger_client_factory
    ):
        # given
        swagger_client = self._get_swagger_client_mock(swagger_client_factory)
        backend = HostedNeptuneBackend(credentials)
        exp_id = str(uuid.uuid4())
        project_id = str(uuid.uuid4())

        response_error = MagicMock()
        response_error.errorDescription = "error1"
        swagger_client.api.executeOperations.return_value.response.return_value.result = [
            response_error
        ]
        swagger_client.api.getArtifactAttribute.return_value.response.return_value.result.hash = (
            "dummyHash"
        )

        # when
        backend.execute_operations(
            run_id=exp_id,
            operations=[
                TrackFilesToArtifact(
                    path=["sub", "one"],
                    project_id=project_id,
                    entries=[("/path/to/file", "/path/to")],
                ),
                TrackFilesToArtifact(
                    path=["sub", "two"],
                    project_id=project_id,
                    entries=[("/path/to/file1", None), ("/path/to/file2", None)],
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
        )

        # then
        track_to_existing_artifact_mock.assert_has_calls(
            [
                call(
                    swagger_client=swagger_client,
                    project_id=project_id,
                    path=["sub", "one"],
                    artifact_hash="dummyHash",
                    parent_identifier=str(exp_id),
                    entries=[("/path/to/file", "/path/to")],
                    default_request_params=DEFAULT_REQUEST_KWARGS,
                ),
                call(
                    swagger_client=swagger_client,
                    project_id=project_id,
                    path=["sub", "two"],
                    artifact_hash="dummyHash",
                    parent_identifier=str(exp_id),
                    entries=[("/path/to/file1", None), ("/path/to/file2", None)],
                    default_request_params=DEFAULT_REQUEST_KWARGS,
                ),
                call(
                    swagger_client=swagger_client,
                    project_id=project_id,
                    path=["sub", "three"],
                    artifact_hash="dummyHash",
                    parent_identifier=str(exp_id),
                    entries=[("/path/to/file1", None), ("/path/to/file2", None)],
                    default_request_params=DEFAULT_REQUEST_KWARGS,
                ),
            ],
            any_order=True,
        )

    @patch(
        "neptune.new.internal.backends.hosted_client.neptune_client_version",
        Version("0.5.13"),
    )
    def test_min_compatible_version_ok(self, swagger_client_factory):
        # given
        self._get_swagger_client_mock(swagger_client_factory, min_compatible="0.5.13")

        # expect
        HostedNeptuneBackend(credentials)

    @patch(
        "neptune.new.internal.backends.hosted_client.neptune_client_version",
        Version("0.5.13"),
    )
    def test_min_compatible_version_fail(self, swagger_client_factory):
        # given
        self._get_swagger_client_mock(swagger_client_factory, min_compatible="0.5.14")

        # expect
        with self.assertRaises(UnsupportedClientVersion) as ex:
            HostedNeptuneBackend(credentials)

        self.assertTrue("Please install neptune-client>=0.5.14" in str(ex.exception))

    @patch(
        "neptune.new.internal.backends.hosted_client.neptune_client_version",
        Version("0.5.13"),
    )
    def test_max_compatible_version_ok(self, swagger_client_factory):
        # given
        self._get_swagger_client_mock(swagger_client_factory, max_compatible="0.5.12")

        # expect
        HostedNeptuneBackend(credentials)

    @patch(
        "neptune.new.internal.backends.hosted_client.neptune_client_version",
        Version("0.5.13"),
    )
    def test_max_compatible_version_fail(self, swagger_client_factory):
        # given
        self._get_swagger_client_mock(swagger_client_factory, max_compatible="0.4.999")

        # expect
        with self.assertRaises(UnsupportedClientVersion) as ex:
            HostedNeptuneBackend(credentials)

        self.assertTrue("Please install neptune-client==0.4.0" in str(ex.exception))

    @patch("socket.gethostbyname")
    def test_cannot_resolve_host(self, gethostname_mock, _):
        # given
        gethostname_mock.side_effect = socket.gaierror

        # expect
        with self.assertRaises(CannotResolveHostname):
            HostedNeptuneBackend(credentials)

    def test_limit_exceed(self, swagger_client_factory):
        # given
        swagger_client = self._get_swagger_client_mock(swagger_client_factory)
        backend = HostedNeptuneBackend(credentials)
        exp_uuid = str(uuid.uuid4())

        # when:
        error = MagicMock()
        error.json.return_value = {"title": "Maximum storage limit reached"}
        swagger_client.api.executeOperations.side_effect = HTTPPaymentRequired(
            response=error
        )

        # then:
        with self.assertRaises(NeptuneLimitExceedException):
            backend.execute_operations(
                run_id=exp_uuid,
                operations=[
                    LogFloats(["float1"], [LogFloats.ValueType(1, 2, 3)]),
                ],
            )

    def test_limit_exceed_legacy(self, swagger_client_factory):
        # given
        swagger_client = self._get_swagger_client_mock(swagger_client_factory)
        backend = HostedNeptuneBackend(credentials)
        exp_uuid = str(uuid.uuid4())

        # when:
        error = MagicMock()
        error.json.return_value = {"title": "Monitoring hours not left"}
        swagger_client.api.executeOperations.side_effect = HTTPUnprocessableEntity(
            response=error
        )

        # then:
        with self.assertRaises(NeptuneLimitExceedException):
            backend.execute_operations(
                run_id=exp_uuid,
                operations=[
                    LogFloats(["float1"], [LogFloats.ValueType(1, 2, 3)]),
                ],
            )
