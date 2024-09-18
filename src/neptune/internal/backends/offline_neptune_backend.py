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

from typing import (
    Generator,
    Iterable,
    List,
    Optional,
)

from neptune.api.models import (
    ArtifactField,
    BoolField,
    DateTimeField,
    Field,
    FieldDefinition,
    FileEntry,
    FileField,
    FloatField,
    FloatSeriesField,
    FloatSeriesValues,
    ImageSeriesValues,
    IntField,
    LeaderboardEntry,
    NextPage,
    QueryFieldDefinitionsResult,
    QueryFieldsResult,
    StringField,
    StringSeriesField,
    StringSeriesValues,
    StringSetField,
)
from neptune.exceptions import NeptuneOfflineModeFetchException
from neptune.internal.artifacts.types import ArtifactFileData
from neptune.internal.backends.neptune_backend_mock import NeptuneBackendMock
from neptune.internal.backends.nql import NQLQuery
from neptune.internal.container_type import ContainerType
from neptune.internal.id_formats import (
    QualifiedName,
    UniqueId,
)
from neptune.typing import ProgressBarType


class OfflineNeptuneBackend(NeptuneBackendMock):
    WORKSPACE_NAME = "offline"

    def get_attributes(self, container_id: str, container_type: ContainerType) -> List[FieldDefinition]:
        raise NeptuneOfflineModeFetchException

    def get_float_attribute(self, container_id: str, container_type: ContainerType, path: List[str]) -> FloatField:
        raise NeptuneOfflineModeFetchException

    def get_int_attribute(self, container_id: str, container_type: ContainerType, path: List[str]) -> IntField:
        raise NeptuneOfflineModeFetchException

    def get_bool_attribute(self, container_id: str, container_type: ContainerType, path: List[str]) -> BoolField:
        raise NeptuneOfflineModeFetchException

    def get_file_attribute(self, container_id: str, container_type: ContainerType, path: List[str]) -> FileField:
        raise NeptuneOfflineModeFetchException

    def get_string_attribute(self, container_id: str, container_type: ContainerType, path: List[str]) -> StringField:
        raise NeptuneOfflineModeFetchException

    def get_datetime_attribute(
        self, container_id: str, container_type: ContainerType, path: List[str]
    ) -> DateTimeField:
        raise NeptuneOfflineModeFetchException

    def get_artifact_attribute(
        self, container_id: str, container_type: ContainerType, path: List[str]
    ) -> ArtifactField:
        raise NeptuneOfflineModeFetchException

    def list_artifact_files(self, project_id: str, artifact_hash: str) -> List[ArtifactFileData]:
        raise NeptuneOfflineModeFetchException

    def get_float_series_attribute(
        self, container_id: str, container_type: ContainerType, path: List[str]
    ) -> FloatSeriesField:
        raise NeptuneOfflineModeFetchException

    def get_string_series_attribute(
        self, container_id: str, container_type: ContainerType, path: List[str]
    ) -> StringSeriesField:
        raise NeptuneOfflineModeFetchException

    def get_string_set_attribute(
        self, container_id: str, container_type: ContainerType, path: List[str]
    ) -> StringSetField:
        raise NeptuneOfflineModeFetchException

    def get_string_series_values(
        self,
        container_id: str,
        container_type: ContainerType,
        path: List[str],
        limit: int,
        from_step: Optional[float] = None,
    ) -> StringSeriesValues:
        raise NeptuneOfflineModeFetchException

    def get_float_series_values(
        self,
        container_id: str,
        container_type: ContainerType,
        path: List[str],
        limit: int,
        from_step: Optional[float] = None,
        use_proto: Optional[bool] = None,
        include_inherited: bool = True,
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
        progress_bar: Optional[ProgressBarType],
    ):
        raise NeptuneOfflineModeFetchException

    def list_fileset_files(self, attribute: List[str], container_id: str, path: str) -> List[FileEntry]:
        raise NeptuneOfflineModeFetchException

    def get_fields_with_paths_filter(
        self, container_id: str, container_type: ContainerType, paths: List[str], use_proto: Optional[bool] = None
    ) -> List[Field]:
        raise NeptuneOfflineModeFetchException

    def get_fields_definitions(
        self,
        container_id: str,
        container_type: ContainerType,
        use_proto: Optional[bool] = None,
    ) -> List[FieldDefinition]:
        raise NeptuneOfflineModeFetchException

    def search_leaderboard_entries(
        self,
        project_id: UniqueId,
        types: Optional[Iterable[ContainerType]] = None,
        query: Optional[NQLQuery] = None,
        columns: Optional[Iterable[str]] = None,
        limit: Optional[int] = None,
        sort_by: str = "sys/creation_time",
        ascending: bool = False,
        progress_bar: Optional[ProgressBarType] = None,
        use_proto: Optional[bool] = None,
    ) -> Generator[LeaderboardEntry, None, None]:
        raise NeptuneOfflineModeFetchException

    def query_fields_definitions_within_project(
        self,
        project_id: QualifiedName,
        field_name_regex: Optional[str] = None,
        experiment_ids_filter: Optional[List[str]] = None,
        next_page: Optional[NextPage] = None,
    ) -> QueryFieldDefinitionsResult:
        raise NeptuneOfflineModeFetchException

    def query_fields_within_project(
        self,
        project_id: QualifiedName,
        field_name_regex: Optional[str] = None,
        field_names_filter: Optional[List[str]] = None,
        experiment_ids_filter: Optional[List[str]] = None,
        experiment_names_filter: Optional[List[str]] = None,
        next_page: Optional[NextPage] = None,
        use_proto: Optional[bool] = None,
    ) -> QueryFieldsResult:
        raise NeptuneOfflineModeFetchException
