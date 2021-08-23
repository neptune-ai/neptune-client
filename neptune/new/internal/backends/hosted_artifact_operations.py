#
# Copyright (c) 2021, Neptune Labs Sp. z o.o.
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
import uuid
from typing import Type, List, Optional, Dict, Tuple

from bravado.client import SwaggerClient
from bravado.exception import HTTPNotFound

from neptune.new.internal.artifacts.types import ArtifactDriversMap, ArtifactDriver, ArtifactFileData
from neptune.new.internal.artifacts.file_hasher import FileHasher
from neptune.new.exceptions import ArtifactUploadingError, ArtifactNotFoundException
from neptune.new.internal.backends.api_model import ArtifactModel
from neptune.new.internal.backends.utils import with_api_exceptions_handler
from neptune.new.internal.operation import Operation, AssignArtifact


def track_to_new_artifact(
        swagger_client: SwaggerClient,
        project_uuid: uuid.UUID,
        path: List[str],
        entries: List[Tuple[str, Optional[str]]],
        default_request_params: Dict
) -> Optional[Operation]:
    files: List[ArtifactFileData] = _extract_file_list(entries)

    if not files:
        raise ArtifactUploadingError("Uploading an empty Artifact")

    artifact_hash = _compute_artifact_hash(files)
    artifact = create_new_artifact(
        swagger_client=swagger_client,
        project_uuid=project_uuid,
        artifact_hash=artifact_hash,
        size=len(files),
        default_request_params=default_request_params
    )

    if not artifact.received_metadata:
        upload_artifact_files_metadata(
            swagger_client=swagger_client,
            project_uuid=project_uuid,
            artifact_hash=artifact_hash,
            files=files,
            default_request_params=default_request_params
        )

    return AssignArtifact(path=path, hash=artifact_hash)


def track_to_existing_artifact(
        swagger_client: SwaggerClient,
        project_uuid: uuid.UUID,
        path: List[str],
        artifact_hash: str,
        entries: List[Tuple[str, Optional[str]]],
        default_request_params: Dict
) -> Optional[Operation]:
    files: List[ArtifactFileData] = _extract_file_list(entries)

    if not files:
        raise ArtifactUploadingError("Uploading an empty Artifact")

    artifact = create_artifact_version(
        swagger_client=swagger_client,
        project_uuid=project_uuid,
        artifact_hash=artifact_hash,
        files=files,
        default_request_params=default_request_params
    )

    return AssignArtifact(path=path, hash=artifact.hash)


def _compute_artifact_hash(files: List[ArtifactFileData]) -> str:
    return FileHasher.get_artifact_hash(files)


def _extract_file_list(entries: List[Tuple[str, Optional[str]]]) -> List[ArtifactFileData]:
    files: List[ArtifactFileData] = list()

    for entry_path, entry_destination in entries:
        driver: Type[ArtifactDriver] = ArtifactDriversMap.match_path(entry_path)
        files.extend(
            driver.get_tracked_files(path=entry_path, destination=entry_destination)
        )

    return files


@with_api_exceptions_handler
def create_new_artifact(
        swagger_client: SwaggerClient,
        project_uuid: uuid.UUID,
        artifact_hash: str,
        size: int,
        default_request_params: Dict
) -> ArtifactModel:
    params = {
        'projectIdentifier': project_uuid,
        'hash': artifact_hash,
        'size': size,
        **default_request_params
    }
    try:
        result = swagger_client.api.createNewArtifact(**params).response().result
        return ArtifactModel(
            hash=result.artifactHash,
            received_metadata=result.receivedMetadata,
            size=result.size
        )
    except HTTPNotFound:
        raise ArtifactNotFoundException(artifact_hash)


@with_api_exceptions_handler
def upload_artifact_files_metadata(
        swagger_client: SwaggerClient,
        project_uuid: uuid.UUID,
        artifact_hash: str,
        files: List[ArtifactFileData],
        default_request_params: Dict
) -> ArtifactModel:
    params = {
        'projectIdentifier': project_uuid,
        'hash': artifact_hash,
        'artifactFilesDTO': {
            'files': [
                ArtifactFileData.to_dto(a) for a in files
            ]
        },
        **default_request_params
    }
    try:
        result = swagger_client.api.uploadArtifactFilesMetadata(**params).response().result
        return ArtifactModel(
            hash=result.artifactHash,
            size=result.size,
            received_metadata=result.receivedMetadata
        )
    except HTTPNotFound:
        raise ArtifactNotFoundException(artifact_hash)


@with_api_exceptions_handler
def create_artifact_version(
        swagger_client: SwaggerClient,
        project_uuid: uuid.UUID,
        artifact_hash: str,
        files: List[ArtifactFileData],
        default_request_params: Dict
) -> ArtifactModel:
    params = {
        'projectIdentifier': project_uuid,
        'hash': artifact_hash,
        'artifactFilesDTO': {
            'files': [
                ArtifactFileData.to_dto(a) for a in files
            ]
        },
        **default_request_params
    }
    try:
        result = swagger_client.api.createArtifactVersion(**params).response().result
        return ArtifactModel(
            hash=result.artifactHash,
            size=result.size,
            received_metadata=result.receivedMetadata
        )
    except HTTPNotFound:
        raise ArtifactNotFoundException(artifact_hash)
