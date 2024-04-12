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
    "Project",
    "Workspace",
    "ApiExperiment",
    "OptionalFeatures",
    "VersionInfo",
    "ClientConfig",
    "ArtifactModel",
    "MultipartConfig",
]

from dataclasses import dataclass
from typing import (
    FrozenSet,
    Optional,
)

from packaging import version

from neptune.internal.container_type import ContainerType
from neptune.internal.id_formats import (
    SysId,
    UniqueId,
)


@dataclass(frozen=True)
class MultipartConfig:
    min_chunk_size: int
    max_chunk_size: int
    max_chunk_count: int
    max_single_part_size: int

    @staticmethod
    def get_default() -> "MultipartConfig":
        return MultipartConfig(
            min_chunk_size=5242880,
            max_chunk_size=1073741824,
            max_chunk_count=1000,
            max_single_part_size=5242880,
        )


@dataclass
class Project:
    id: UniqueId
    name: str
    workspace: str
    sys_id: SysId


@dataclass
class Workspace:
    id: UniqueId
    name: str


@dataclass
class ApiExperiment:
    id: UniqueId
    type: ContainerType
    sys_id: SysId
    workspace: str
    project_name: str
    trashed: bool = False

    @classmethod
    def from_experiment(cls, response_exp):
        return cls(
            id=response_exp.id,
            type=ContainerType.from_api(response_exp.type),
            sys_id=response_exp.shortId,
            workspace=response_exp.organizationName,
            project_name=response_exp.projectName,
            trashed=response_exp.trashed,
        )


class OptionalFeatures:
    VERSION_INFO = "version_info"
    ARTIFACTS = "artifacts"
    ARTIFACTS_HASH_EXCLUDE_METADATA = "artifacts_hash_exclude_metadata"
    ARTIFACTS_EXCLUDE_DIRECTORY_FILES = "artifact_exclude_directory_files"
    MULTIPART_UPLOAD = "multipart_upload"


@dataclass(frozen=True)
class VersionInfo:
    min_recommended: Optional[version.Version]
    min_compatible: Optional[version.Version]
    max_compatible: Optional[version.Version]

    @staticmethod
    def build(
        min_recommended: Optional[str],
        min_compatible: Optional[str],
        max_compatible: Optional[str],
    ) -> "VersionInfo":
        return VersionInfo(
            min_recommended=version.parse(min_recommended) if min_recommended else None,
            min_compatible=version.parse(min_compatible) if min_compatible else None,
            max_compatible=version.parse(max_compatible) if max_compatible else None,
        )


@dataclass(frozen=True)
class ClientConfig:
    api_url: str
    display_url: str
    _missing_features: FrozenSet[str]
    version_info: VersionInfo
    multipart_config: MultipartConfig

    def has_feature(self, feature_name: str) -> bool:
        return feature_name not in self._missing_features

    @staticmethod
    def from_api_response(config) -> "ClientConfig":
        missing_features = []

        version_info_obj = getattr(config, "pyLibVersions", None)
        if version_info_obj is None:
            missing_features.append(OptionalFeatures.VERSION_INFO)
            min_recommended = min_compatible = max_compatible = None
        else:
            min_recommended = getattr(version_info_obj, "minRecommendedVersion", None)
            min_compatible = getattr(version_info_obj, "minCompatibleVersion", None)
            max_compatible = getattr(version_info_obj, "maxCompatibleVersion", None)

        multipart_upload_config_obj = getattr(config, "multiPartUpload", None)
        has_multipart_upload = getattr(multipart_upload_config_obj, "enabled", False)
        if not has_multipart_upload:
            missing_features.append(OptionalFeatures.MULTIPART_UPLOAD)
            multipart_upload_config = None
        else:
            min_chunk_size = getattr(multipart_upload_config_obj, "minChunkSize")
            max_chunk_size = getattr(multipart_upload_config_obj, "maxChunkSize")
            max_chunk_count = getattr(multipart_upload_config_obj, "maxChunkCount")
            max_single_part_size = getattr(multipart_upload_config_obj, "maxSinglePartSize")
            multipart_upload_config = MultipartConfig(
                min_chunk_size, max_chunk_size, max_chunk_count, max_single_part_size
            )

        artifacts_config_obj = getattr(config, "artifacts", None)
        has_artifacts = getattr(artifacts_config_obj, "enabled", False)
        if not has_artifacts:
            missing_features.append(OptionalFeatures.ARTIFACTS)

        artifacts_api_version = getattr(artifacts_config_obj, "apiVersion", 1)
        if artifacts_api_version == 1:
            missing_features.append(OptionalFeatures.ARTIFACTS_HASH_EXCLUDE_METADATA)
            missing_features.append(OptionalFeatures.ARTIFACTS_EXCLUDE_DIRECTORY_FILES)

        return ClientConfig(
            api_url=config.apiUrl,
            display_url=config.applicationUrl,
            _missing_features=frozenset(missing_features),
            version_info=VersionInfo.build(min_recommended, min_compatible, max_compatible),
            multipart_config=multipart_upload_config,
        )


@dataclass
class ArtifactModel:
    received_metadata: bool
    hash: str
    size: int
