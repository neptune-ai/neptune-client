#
# Copyright (c) 2022, Neptune Labs Sp. z o.o.
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
__all__ = [
    "track_to_new_artifact",
    "track_to_existing_artifact",
    "get_artifact_attribute",
    "list_artifact_files",
]

from typing import (
    Dict,
    List,
    Optional,
    Tuple,
    Type,
)

from bravado.exception import HTTPNotFound

from neptune.common.backends.utils import with_api_exceptions_handler
from neptune.exceptions import (
    ArtifactNotFoundException,
    ArtifactUploadingError,
    FetchAttributeNotFoundException,
    NeptuneEmptyLocationException,
)
from neptune.internal.artifacts.file_hasher import FileHasher
from neptune.internal.artifacts.types import (
    ArtifactDriver,
    ArtifactDriversMap,
    ArtifactFileData,
)
from neptune.internal.backends.api_model import (
    ArtifactAttribute,
    ArtifactModel,
)
from neptune.internal.backends.swagger_client_wrapper import SwaggerClientWrapper
from neptune.internal.operation import (
    AssignArtifact,
    Operation,
)
from neptune.internal.utils.paths import path_to_str


def _compute_artifact_size(artifact_file_list: List[ArtifactFileData]):
    artifact_size = 0
    for artifact_file in artifact_file_list:
        if artifact_file.size is None:
            # whole artifact's size is undefined in this case
            return None
        artifact_size += artifact_file.size
    return artifact_size


def _filter_empty_directory_files(files: List[ArtifactFileData]) -> List[ArtifactFileData]:
    return list(filter(lambda x: not _is_s3_empty_directory_file(x), files))


def _is_s3_empty_directory_file(file: ArtifactFileData) -> bool:
    return file.type == "S3" and file.size == 0


def track_to_new_artifact(
    swagger_client: SwaggerClientWrapper,
    project_id: str,
    path: List[str],
    parent_identifier: str,
    entries: List[Tuple[str, Optional[str]]],
    default_request_params: Dict,
    exclude_directory_files: bool,
    exclude_metadata_from_hash: bool,
) -> Optional[Operation]:
    files: List[ArtifactFileData] = _extract_file_list(path, entries)

    if exclude_directory_files:
        files = _filter_empty_directory_files(files)

    if not files:
        raise ArtifactUploadingError("Uploading an empty Artifact")

    artifact_hash = (
        _compute_artifact_hash_without_metadata(files) if exclude_metadata_from_hash else _compute_artifact_hash(files)
    )

    artifact = create_new_artifact(
        swagger_client=swagger_client,
        project_id=project_id,
        artifact_hash=artifact_hash,
        parent_identifier=parent_identifier,
        size=_compute_artifact_size(files),
        default_request_params=default_request_params,
    )

    if not artifact.received_metadata:
        upload_artifact_files_metadata(
            swagger_client=swagger_client,
            project_id=project_id,
            artifact_hash=artifact_hash,
            files=files,
            default_request_params=default_request_params,
        )

    return AssignArtifact(path=path, hash=artifact_hash)


def track_to_existing_artifact(
    swagger_client: SwaggerClientWrapper,
    project_id: str,
    path: List[str],
    artifact_hash: str,
    parent_identifier: str,
    entries: List[Tuple[str, Optional[str]]],
    default_request_params: Dict,
    exclude_directory_files: bool,
) -> Optional[Operation]:
    files: List[ArtifactFileData] = _extract_file_list(path, entries)

    if exclude_directory_files:
        files = _filter_empty_directory_files(files)

    if not files:
        raise ArtifactUploadingError("Uploading an empty Artifact")

    artifact = create_artifact_version(
        swagger_client=swagger_client,
        project_id=project_id,
        artifact_hash=artifact_hash,
        parent_identifier=parent_identifier,
        files=files,
        default_request_params=default_request_params,
    )

    return AssignArtifact(path=path, hash=artifact.hash)


def _compute_artifact_hash_without_metadata(files: List[ArtifactFileData]) -> str:
    return FileHasher.get_artifact_hash_without_metadata(files)


def _compute_artifact_hash(files: List[ArtifactFileData]) -> str:
    return FileHasher.get_artifact_hash(files)


def _extract_file_list(path: List[str], entries: List[Tuple[str, Optional[str]]]) -> List[ArtifactFileData]:
    files: List[ArtifactFileData] = list()

    for entry_path, entry_destination in entries:
        driver: Type[ArtifactDriver] = ArtifactDriversMap.match_path(entry_path)
        artifact_files = driver.get_tracked_files(path=entry_path, destination=entry_destination)

        if len(artifact_files) == 0:
            raise NeptuneEmptyLocationException(location=entry_path, namespace="/".join(path))

        files.extend(artifact_files)

    return files


@with_api_exceptions_handler
def create_new_artifact(
    swagger_client: SwaggerClientWrapper,
    project_id: str,
    artifact_hash: str,
    parent_identifier: str,
    size: int,
    default_request_params: Dict,
) -> ArtifactModel:
    params = {
        "projectIdentifier": project_id,
        "hash": artifact_hash,
        "size": size,
        "parentIdentifier": parent_identifier,
        **add_artifact_version_to_request_params(default_request_params),
    }
    try:
        result = swagger_client.api.createNewArtifact(**params).response().result
        return ArtifactModel(
            hash=result.artifactHash,
            received_metadata=result.receivedMetadata,
            size=result.size,
        )
    except HTTPNotFound:
        raise ArtifactNotFoundException(artifact_hash)


@with_api_exceptions_handler
def upload_artifact_files_metadata(
    swagger_client: SwaggerClientWrapper,
    project_id: str,
    artifact_hash: str,
    files: List[ArtifactFileData],
    default_request_params: Dict,
) -> ArtifactModel:
    params = {
        "projectIdentifier": project_id,
        "hash": artifact_hash,
        "artifactFilesDTO": {"files": [ArtifactFileData.to_dto(a) for a in files]},
        **add_artifact_version_to_request_params(default_request_params),
    }
    try:
        result = swagger_client.api.uploadArtifactFilesMetadata(**params).response().result
        return ArtifactModel(
            hash=result.artifactHash,
            size=result.size,
            received_metadata=result.receivedMetadata,
        )
    except HTTPNotFound:
        raise ArtifactNotFoundException(artifact_hash)


@with_api_exceptions_handler
def create_artifact_version(
    swagger_client: SwaggerClientWrapper,
    project_id: str,
    artifact_hash: str,
    parent_identifier: str,
    files: List[ArtifactFileData],
    default_request_params: Dict,
) -> ArtifactModel:
    params = {
        "projectIdentifier": project_id,
        "hash": artifact_hash,
        "parentIdentifier": parent_identifier,
        "artifactFilesDTO": {"files": [ArtifactFileData.to_dto(a) for a in files]},
        **add_artifact_version_to_request_params(default_request_params),
    }
    try:
        result = swagger_client.api.createArtifactVersion(**params).response().result
        return ArtifactModel(
            hash=result.artifactHash,
            size=result.size,
            received_metadata=result.receivedMetadata,
        )
    except HTTPNotFound:
        raise ArtifactNotFoundException(artifact_hash)


@with_api_exceptions_handler
def get_artifact_attribute(
    swagger_client: SwaggerClientWrapper,
    parent_identifier: str,
    path: List[str],
    default_request_params: Dict,
) -> ArtifactAttribute:
    requests_params = add_artifact_version_to_request_params(default_request_params)
    params = {
        "experimentId": parent_identifier,
        "attribute": path_to_str(path),
        **requests_params,
    }
    try:
        result = swagger_client.api.getArtifactAttribute(**params).response().result
        return ArtifactAttribute(hash=result.hash)
    except HTTPNotFound:
        raise FetchAttributeNotFoundException(path_to_str(path))


@with_api_exceptions_handler
def list_artifact_files(
    swagger_client: SwaggerClientWrapper,
    project_id: str,
    artifact_hash: str,
    default_request_params: Dict,
) -> List[ArtifactFileData]:
    requests_params = add_artifact_version_to_request_params(default_request_params)
    params = {
        "projectIdentifier": project_id,
        "hash": artifact_hash,
        **requests_params,
    }
    try:
        result = swagger_client.api.listArtifactFiles(**params).response().result
        return [ArtifactFileData.from_dto(a) for a in result.files]
    except HTTPNotFound:
        raise ArtifactNotFoundException(artifact_hash)


def add_artifact_version_to_request_params(default_request_params: Dict) -> Dict:
    current_artifact_version = "2"

    return {
        "_request_options": {
            **default_request_params["_request_options"],
            "headers": {
                **default_request_params["_request_options"]["headers"],
                "X-Neptune-Artifact-Api-Version": current_artifact_version,
            },
        }
    }
