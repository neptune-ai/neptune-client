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
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional, List, Set, Any, FrozenSet

from packaging import version


class Project:
    def __init__(self, _id: str, name: str, workspace: str):
        self.id = _id
        self.name = name
        self.workspace = workspace


class Workspace:
    def __init__(self, _id: str, name: str):
        self.id = _id
        self.name = name


@dataclass
class ApiRun:
    id: str
    short_id: str
    workspace: str
    project_name: str
    trashed: bool


class OptionalFeatures:
    VERSION_INFO = "version_info"
    ARTIFACTS = "artifacts"
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
class MultipartConfig:
    min_chunk_size: int
    max_chunk_size: int
    max_chunk_count: int
    max_single_part_size: int


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
            max_single_part_size = getattr(
                multipart_upload_config_obj, "maxSinglePartSize"
            )
            multipart_upload_config = MultipartConfig(
                min_chunk_size, max_chunk_size, max_chunk_count, max_single_part_size
            )

        artifacts_config_obj = getattr(config, "artifacts", None)
        has_artifacts = getattr(artifacts_config_obj, "enabled", False)
        if not has_artifacts:
            missing_features.append(OptionalFeatures.ARTIFACTS)

        return ClientConfig(
            api_url=config.apiUrl,
            display_url=config.applicationUrl,
            _missing_features=frozenset(missing_features),
            version_info=VersionInfo.build(
                min_recommended, min_compatible, max_compatible
            ),
            multipart_config=multipart_upload_config,
        )


class AttributeType(Enum):
    FLOAT = "float"
    INT = "int"
    BOOL = "bool"
    STRING = "string"
    DATETIME = "datetime"
    FILE = "file"
    FILE_SET = "fileSet"
    FLOAT_SERIES = "floatSeries"
    STRING_SERIES = "stringSeries"
    IMAGE_SERIES = "imageSeries"
    STRING_SET = "stringSet"
    GIT_REF = "gitRef"
    RUN_STATE = "experimentState"
    NOTEBOOK_REF = "notebookRef"
    ARTIFACT = "artifact"


@dataclass
class Attribute:
    path: str
    type: AttributeType


@dataclass
class AttributeWithProperties:
    path: str
    type: AttributeType
    properties: Any


@dataclass
class LeaderboardEntry:
    id: str
    attributes: List[AttributeWithProperties]


@dataclass
class StringPointValue:
    timestampMillis: int
    step: float
    value: str


@dataclass
class ImageSeriesValues:
    totalItemCount: int


@dataclass
class StringSeriesValues:
    totalItemCount: int
    values: List[StringPointValue]


@dataclass
class FloatPointValue:
    timestampMillis: int
    step: float
    value: float


@dataclass
class FloatSeriesValues:
    totalItemCount: int
    values: List[FloatPointValue]


@dataclass
class FloatAttribute:
    value: float


@dataclass
class IntAttribute:
    value: int


@dataclass
class BoolAttribute:
    value: bool


@dataclass
class FileAttribute:
    name: str
    ext: str
    size: int


@dataclass
class StringAttribute:
    value: str


@dataclass
class DatetimeAttribute:
    value: datetime


@dataclass
class ArtifactAttribute:
    hash: str


@dataclass
class ArtifactModel:
    received_metadata: bool
    hash: str
    size: int


@dataclass
class FloatSeriesAttribute:
    last: Optional[float]


@dataclass
class StringSeriesAttribute:
    last: Optional[str]


@dataclass
class StringSetAttribute:
    values: Set[str]
