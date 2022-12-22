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
__all__ = ["OfflineNeptuneBackend"]

from typing import List

from neptune.new.exceptions import NeptuneOfflineModeFetchException
from neptune.new.internal.artifacts.types import ArtifactFileData
from neptune.new.internal.backends.api_model import (
    ArtifactAttribute,
    Attribute,
    BoolAttribute,
    DatetimeAttribute,
    FileAttribute,
    FloatAttribute,
    FloatSeriesAttribute,
    FloatSeriesValues,
    ImageSeriesValues,
    IntAttribute,
    StringAttribute,
    StringSeriesAttribute,
    StringSeriesValues,
    StringSetAttribute,
)
from neptune.new.internal.backends.neptune_backend_mock import NeptuneBackendMock
from neptune.new.internal.container_type import ContainerType


class OfflineNeptuneBackend(NeptuneBackendMock):
    WORKSPACE_NAME = "offline"

    def get_attributes(self, container_id: str, container_type: ContainerType) -> List[Attribute]:
        raise NeptuneOfflineModeFetchException

    def get_float_attribute(self, container_id: str, container_type: ContainerType, path: List[str]) -> FloatAttribute:
        raise NeptuneOfflineModeFetchException

    def get_int_attribute(self, container_id: str, container_type: ContainerType, path: List[str]) -> IntAttribute:
        raise NeptuneOfflineModeFetchException

    def get_bool_attribute(self, container_id: str, container_type: ContainerType, path: List[str]) -> BoolAttribute:
        raise NeptuneOfflineModeFetchException

    def get_file_attribute(self, container_id: str, container_type: ContainerType, path: List[str]) -> FileAttribute:
        raise NeptuneOfflineModeFetchException

    def get_string_attribute(
        self, container_id: str, container_type: ContainerType, path: List[str]
    ) -> StringAttribute:
        raise NeptuneOfflineModeFetchException

    def get_datetime_attribute(
        self, container_id: str, container_type: ContainerType, path: List[str]
    ) -> DatetimeAttribute:
        raise NeptuneOfflineModeFetchException

    def get_artifact_attribute(
        self, container_id: str, container_type: ContainerType, path: List[str]
    ) -> ArtifactAttribute:
        raise NeptuneOfflineModeFetchException

    def list_artifact_files(self, project_id: str, artifact_hash: str) -> List[ArtifactFileData]:
        raise NeptuneOfflineModeFetchException

    def get_float_series_attribute(
        self, container_id: str, container_type: ContainerType, path: List[str]
    ) -> FloatSeriesAttribute:
        raise NeptuneOfflineModeFetchException

    def get_string_series_attribute(
        self, container_id: str, container_type: ContainerType, path: List[str]
    ) -> StringSeriesAttribute:
        raise NeptuneOfflineModeFetchException

    def get_string_set_attribute(
        self, container_id: str, container_type: ContainerType, path: List[str]
    ) -> StringSetAttribute:
        raise NeptuneOfflineModeFetchException

    def get_string_series_values(
        self,
        container_id: str,
        container_type: ContainerType,
        path: List[str],
        offset: int,
        limit: int,
    ) -> StringSeriesValues:
        raise NeptuneOfflineModeFetchException

    def get_float_series_values(
        self,
        container_id: str,
        container_type: ContainerType,
        path: List[str],
        offset: int,
        limit: int,
    ) -> FloatSeriesValues:
        raise NeptuneOfflineModeFetchException

    def get_image_series_values(
        self,
        container_id: str,
        container_type: ContainerType,
        path: List[str],
        offset: int,
        limit: int,
    ) -> ImageSeriesValues:
        raise NeptuneOfflineModeFetchException

    def download_file_series_by_index(
        self,
        container_id: str,
        container_type: ContainerType,
        path: List[str],
        index: int,
        destination: str,
    ):
        raise NeptuneOfflineModeFetchException
