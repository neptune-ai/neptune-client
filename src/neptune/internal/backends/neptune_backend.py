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
__all__ = ["NeptuneBackend"]

import abc
from typing import (
    Any,
    Generator,
    List,
    Optional,
    Tuple,
    Union,
)

from neptune.api.dtos import FileEntry
from neptune.common.exceptions import NeptuneException
from neptune.core.components.operation_storage import OperationStorage
from neptune.internal.artifacts.types import ArtifactFileData
from neptune.internal.backends.api_model import (
    ApiExperiment,
    ArtifactAttribute,
    Attribute,
    AttributeType,
    BoolAttribute,
    DatetimeAttribute,
    FileAttribute,
    FloatAttribute,
    FloatSeriesAttribute,
    FloatSeriesValues,
    ImageSeriesValues,
    IntAttribute,
    LeaderboardEntry,
    Project,
    StringAttribute,
    StringSeriesAttribute,
    StringSeriesValues,
    StringSetAttribute,
    Workspace,
)
from neptune.internal.backends.nql import NQLQuery
from neptune.internal.container_type import ContainerType
from neptune.internal.id_formats import (
    QualifiedName,
    UniqueId,
)
from neptune.internal.operation import Operation
from neptune.internal.utils.git import GitInfo
from neptune.internal.websockets.websockets_factory import WebsocketsFactory
from neptune.typing import ProgressBarType


class NeptuneBackend:
    def close(self) -> None:
        """No need for closing implementation"""

    @abc.abstractmethod
    def get_display_address(self) -> str:
        pass

    def verify_feature_available(self, _: str) -> None:
        """
        this method makes sense only for backends interacting with server;
        it makes sure that a feature is supported in the backend version client interacts with
        """

    def websockets_factory(self, project_id: str, run_id: str) -> Optional[WebsocketsFactory]:
        return None

    @abc.abstractmethod
    def get_project(self, project_id: QualifiedName) -> Project:
        pass

    @abc.abstractmethod
    def get_available_projects(
        self, workspace_id: Optional[str] = None, search_term: Optional[str] = None
    ) -> List[Project]:
        pass

    @abc.abstractmethod
    def get_available_workspaces(self) -> List[Workspace]:
        pass

    @abc.abstractmethod
    def create_run(
        self,
        project_id: UniqueId,
        git_info: Optional[GitInfo] = None,
        custom_run_id: Optional[str] = None,
        notebook_id: Optional[str] = None,
        checkpoint_id: Optional[str] = None,
    ) -> ApiExperiment:
        pass

    @abc.abstractmethod
    def create_model(
        self,
        project_id: UniqueId,
        key: str,
    ) -> ApiExperiment:
        pass

    @abc.abstractmethod
    def create_model_version(
        self,
        project_id: UniqueId,
        model_id: UniqueId,
    ) -> ApiExperiment:
        pass

    @abc.abstractmethod
    def get_metadata_container(
        self,
        container_id: Union[UniqueId, QualifiedName],
        expected_container_type: Optional[ContainerType],
    ) -> ApiExperiment:
        pass

    @abc.abstractmethod
    def create_checkpoint(self, notebook_id: str, jupyter_path: str) -> Optional[str]:
        pass

    def ping(self, container_id: str, container_type: ContainerType):
        """Do nothing by default"""

    @abc.abstractmethod
    def execute_operations(
        self,
        container_id: UniqueId,
        container_type: ContainerType,
        operations: List[Operation],
        operation_storage: OperationStorage,
    ) -> Tuple[int, List[NeptuneException]]:
        pass

    @abc.abstractmethod
    def get_attributes(self, container_id: str, container_type: ContainerType) -> List[Attribute]:
        pass

    @abc.abstractmethod
    def download_file(
        self,
        container_id: str,
        container_type: ContainerType,
        path: List[str],
        destination: Optional[str] = None,
        progress_bar: Optional[ProgressBarType] = None,
    ):
        pass

    @abc.abstractmethod
    def download_file_set(
        self,
        container_id: str,
        container_type: ContainerType,
        path: List[str],
        destination: Optional[str] = None,
        progress_bar: Optional[ProgressBarType] = None,
    ):
        pass

    @abc.abstractmethod
    def get_float_attribute(self, container_id: str, container_type: ContainerType, path: List[str]) -> FloatAttribute:
        pass

    @abc.abstractmethod
    def get_int_attribute(self, container_id: str, container_type: ContainerType, path: List[str]) -> IntAttribute:
        pass

    @abc.abstractmethod
    def get_bool_attribute(self, container_id: str, container_type: ContainerType, path: List[str]) -> BoolAttribute:
        pass

    @abc.abstractmethod
    def get_file_attribute(self, container_id: str, container_type: ContainerType, path: List[str]) -> FileAttribute:
        pass

    @abc.abstractmethod
    def get_string_attribute(
        self, container_id: str, container_type: ContainerType, path: List[str]
    ) -> StringAttribute:
        pass

    @abc.abstractmethod
    def get_datetime_attribute(
        self, container_id: str, container_type: ContainerType, path: List[str]
    ) -> DatetimeAttribute:
        pass

    @abc.abstractmethod
    def get_artifact_attribute(
        self, container_id: str, container_type: ContainerType, path: List[str]
    ) -> ArtifactAttribute:
        pass

    @abc.abstractmethod
    def list_artifact_files(self, project_id: str, artifact_hash: str) -> List[ArtifactFileData]:
        pass

    @abc.abstractmethod
    def get_float_series_attribute(
        self, container_id: str, container_type: ContainerType, path: List[str]
    ) -> FloatSeriesAttribute:
        pass

    @abc.abstractmethod
    def get_string_series_attribute(
        self, container_id: str, container_type: ContainerType, path: List[str]
    ) -> StringSeriesAttribute:
        pass

    @abc.abstractmethod
    def get_string_set_attribute(
        self, container_id: str, container_type: ContainerType, path: List[str]
    ) -> StringSetAttribute:
        pass

    @abc.abstractmethod
    def download_file_series_by_index(
        self,
        container_id: str,
        container_type: ContainerType,
        path: List[str],
        index: int,
        destination: str,
        progress_bar: Optional[ProgressBarType],
    ):
        pass

    @abc.abstractmethod
    def get_image_series_values(
        self,
        container_id: str,
        container_type: ContainerType,
        path: List[str],
        offset: int,
        limit: int,
    ) -> ImageSeriesValues:
        pass

    @abc.abstractmethod
    def get_string_series_values(
        self,
        container_id: str,
        container_type: ContainerType,
        path: List[str],
        offset: int,
        limit: int,
    ) -> StringSeriesValues:
        pass

    @abc.abstractmethod
    def get_float_series_values(
        self,
        container_id: str,
        container_type: ContainerType,
        path: List[str],
        offset: int,
        limit: int,
    ) -> FloatSeriesValues:
        pass

    @abc.abstractmethod
    def get_run_url(self, run_id: str, workspace: str, project_name: str, sys_id: str) -> str:
        pass

    @abc.abstractmethod
    def get_project_url(self, project_id: str, workspace: str, project_name: str) -> str:
        pass

    @abc.abstractmethod
    def get_model_url(self, model_id: str, workspace: str, project_name: str, sys_id: str) -> str:
        pass

    @abc.abstractmethod
    def get_model_version_url(
        self,
        model_version_id: str,
        model_id: str,
        workspace: str,
        project_name: str,
        sys_id: str,
    ) -> str:
        pass

    @abc.abstractmethod
    def fetch_atom_attribute_values(
        self, container_id: str, container_type: ContainerType, path: List[str]
    ) -> List[Tuple[str, AttributeType, Any]]:
        pass

    @abc.abstractmethod
    def search_leaderboard_entries(
        self,
        project_id: UniqueId,
        types: Optional[List[ContainerType]] = None,
        query: Optional[NQLQuery] = None,
        columns: Optional[List[str]] = None,
        limit: Optional[int] = None,
        sort_by: str = "sys/creation_time",
        ascending: bool = False,
        progress_bar: Optional[ProgressBarType] = None,
    ) -> Generator[LeaderboardEntry, None, None]:
        pass

    @abc.abstractmethod
    def list_fileset_files(self, attribute: List[str], container_id: str, path: str) -> List[FileEntry]:
        pass
