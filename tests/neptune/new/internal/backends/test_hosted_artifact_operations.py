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
import unittest
import uuid

from mock import MagicMock, patch

from neptune.new.exceptions import ArtifactUploadingError
from neptune.new.internal.backends.api_model import ArtifactModel
from neptune.new.internal.artifacts.types import ArtifactFileData
from neptune.new.internal.backends.hosted_artifact_operations import (
    track_to_new_artifact,
    track_to_existing_artifact,
)


class TestHostedArtifactOperations(unittest.TestCase):
    def setUp(self) -> None:
        self.artifact_hash = (
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        )
        self.files = [
            ArtifactFileData(
                "fname.txt",
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                "test",
                {},
            ),
        ]
        self.project_id = str(uuid.uuid4())
        self.parent_identifier = str(uuid.uuid4())

    @patch(
        "neptune.new.internal.backends.hosted_artifact_operations._compute_artifact_hash"
    )
    @patch(
        "neptune.new.internal.backends.hosted_artifact_operations._extract_file_list"
    )
    @patch(
        "neptune.new.internal.backends.hosted_artifact_operations.create_new_artifact"
    )
    def test_track_to_new_artifact_calls_creation(
        self, create_new_artifact, _extract_file_list, _compute_artifact_hash
    ):
        # given
        swagger_mock = self._get_swagger_mock()
        _compute_artifact_hash.return_value = self.artifact_hash
        _extract_file_list.return_value = self.files

        # when
        track_to_new_artifact(
            swagger_client=swagger_mock,
            project_id=self.project_id,
            path=["sub", "one"],
            parent_identifier=self.parent_identifier,
            entries=[("/path/to/file", "/path/to")],
            default_request_params={},
        )

        # then
        create_new_artifact.assert_called_once_with(
            swagger_client=swagger_mock,
            project_id=self.project_id,
            artifact_hash=self.artifact_hash,
            parent_identifier=self.parent_identifier,
            size=None,
            default_request_params={},
        )

    @patch(
        "neptune.new.internal.backends.hosted_artifact_operations._compute_artifact_hash"
    )
    @patch(
        "neptune.new.internal.backends.hosted_artifact_operations._extract_file_list"
    )
    @patch(
        "neptune.new.internal.backends.hosted_artifact_operations.create_new_artifact"
    )
    @patch(
        "neptune.new.internal.backends.hosted_artifact_operations.upload_artifact_files_metadata"
    )
    def test_track_to_new_artifact_calls_upload(
        self,
        upload_artifact_files_metadata,
        create_new_artifact,
        _extract_file_list,
        _compute_artifact_hash,
    ):
        # given
        swagger_mock = self._get_swagger_mock()
        _compute_artifact_hash.return_value = self.artifact_hash
        _extract_file_list.return_value = self.files
        create_new_artifact.return_value = ArtifactModel(
            received_metadata=False, hash=self.artifact_hash, size=len(self.files)
        )

        # when
        track_to_new_artifact(
            swagger_client=swagger_mock,
            project_id=self.project_id,
            path=["sub", "one"],
            parent_identifier=self.parent_identifier,
            entries=[("/path/to/file", "/path/to")],
            default_request_params={},
        )

        # then
        upload_artifact_files_metadata.assert_called_once_with(
            swagger_client=swagger_mock,
            project_id=self.project_id,
            artifact_hash=self.artifact_hash,
            files=self.files,
            default_request_params={},
        )

    @patch(
        "neptune.new.internal.backends.hosted_artifact_operations._compute_artifact_hash"
    )
    @patch(
        "neptune.new.internal.backends.hosted_artifact_operations._extract_file_list"
    )
    def test_track_to_new_artifact_raises_exception(
        self, _extract_file_list, _compute_artifact_hash
    ):
        # given
        swagger_mock = self._get_swagger_mock()
        _compute_artifact_hash.return_value = self.artifact_hash
        _extract_file_list.return_value = []

        # when
        with self.assertRaises(ArtifactUploadingError):
            track_to_new_artifact(
                swagger_client=swagger_mock,
                project_id=self.project_id,
                path=["sub", "one"],
                parent_identifier=self.parent_identifier,
                entries=[("/path/to/file", "/path/to")],
                default_request_params={},
            )

    @patch(
        "neptune.new.internal.backends.hosted_artifact_operations._compute_artifact_hash"
    )
    @patch(
        "neptune.new.internal.backends.hosted_artifact_operations._extract_file_list"
    )
    @patch(
        "neptune.new.internal.backends.hosted_artifact_operations.create_artifact_version"
    )
    def test_track_to_existing_artifact_calls_version(
        self, create_artifact_version, _extract_file_list, _compute_artifact_hash
    ):
        # given
        swagger_mock = self._get_swagger_mock()
        _extract_file_list.return_value = self.files

        # when
        track_to_existing_artifact(
            swagger_client=swagger_mock,
            project_id=self.project_id,
            path=["sub", "one"],
            artifact_hash=self.artifact_hash,
            parent_identifier=self.parent_identifier,
            entries=[("/path/to/file", "/path/to")],
            default_request_params={},
        )

        # then
        create_artifact_version.assert_called_once_with(
            swagger_client=swagger_mock,
            project_id=self.project_id,
            artifact_hash=self.artifact_hash,
            parent_identifier=self.parent_identifier,
            files=self.files,
            default_request_params={},
        )

    @patch(
        "neptune.new.internal.backends.hosted_artifact_operations._compute_artifact_hash"
    )
    @patch(
        "neptune.new.internal.backends.hosted_artifact_operations._extract_file_list"
    )
    def test_track_to_existing_artifact_raises_exception(
        self, _extract_file_list, _compute_artifact_hash
    ):
        # given
        swagger_mock = self._get_swagger_mock()
        _compute_artifact_hash.return_value = self.artifact_hash
        _extract_file_list.return_value = []

        # when
        with self.assertRaises(ArtifactUploadingError):
            track_to_existing_artifact(
                swagger_client=swagger_mock,
                project_id=self.project_id,
                path=["sub", "one"],
                artifact_hash="abcdef",
                parent_identifier=self.parent_identifier,
                entries=[("/path/to/file", "/path/to")],
                default_request_params={},
            )

    @staticmethod
    def _get_swagger_mock():
        swagger_mock = MagicMock()
        swagger_mock.swagger_spec.http_client = MagicMock()
        swagger_mock.swagger_spec.api_url = "ui.neptune.ai"
        swagger_mock.api.createNewArtifact.operation.path_name = "/createNewArtifact"
        swagger_mock.api.uploadArtifactFilesMetadata.operation.path_name = (
            "/uploadArtifactFilesMetadata"
        )
        return swagger_mock
