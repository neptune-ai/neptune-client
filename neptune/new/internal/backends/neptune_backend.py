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
import abc
from typing import Any, List, Tuple, Optional

from neptune.new.exceptions import NeptuneException
from neptune.new.internal.artifacts.types import ArtifactFileData
from neptune.new.internal.backends.api_model import (
    ApiRun,
    Attribute,
    AttributeType,
    BoolAttribute,
    DatetimeAttribute,
    FileAttribute,
    FloatAttribute,
    FloatSeriesAttribute,
    IntAttribute,
    Project,
    Workspace,
    StringAttribute,
    StringSeriesAttribute,
    StringSetAttribute,
    StringSeriesValues,
    FloatSeriesValues,
    ImageSeriesValues,
    ArtifactAttribute,
)
from neptune.new.internal.operation import Operation
from neptune.new.internal.websockets.websockets_factory import WebsocketsFactory
from neptune.new.types.atoms import GitRef


class NeptuneBackend:
    @abc.abstractmethod
    def close(self) -> None:
        pass

    @abc.abstractmethod
    def get_display_address(self) -> str:
        pass

    def verify_feature_available(self, _: str) -> None:
        """
        this method makes sense only for backends interacting with server;
        it makes sure that a feature is supported in the backend version client interacts with
        """
        return

    # pylint: disable=unused-argument
    def websockets_factory(
        self, project_id: str, run_id: str
    ) -> Optional[WebsocketsFactory]:
        return None

    @abc.abstractmethod
    def get_project(self, project_id: str) -> Project:
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
    def get_run(self, run_id: str) -> ApiRun:
        pass

    @abc.abstractmethod
    def create_run(
        self,
        project_id: str,
        git_ref: Optional[GitRef] = None,
        custom_run_id: Optional[str] = None,
        notebook_id: Optional[str] = None,
        checkpoint_id: Optional[str] = None,
    ) -> ApiRun:
        pass

    @abc.abstractmethod
    def create_checkpoint(self, notebook_id: str, jupyter_path: str) -> Optional[str]:
        pass

    def ping_run(self, run_id: str):
        pass

    @abc.abstractmethod
    def execute_operations(
        self, run_id: str, operations: List[Operation]
    ) -> List[NeptuneException]:
        pass

    @abc.abstractmethod
    def get_attributes(self, run_id: str) -> List[Attribute]:
        pass

    @abc.abstractmethod
    def download_file(
        self, run_id: str, path: List[str], destination: Optional[str] = None
    ):
        pass

    @abc.abstractmethod
    def download_file_set(
        self, run_id: str, path: List[str], destination: Optional[str] = None
    ):
        pass

    @abc.abstractmethod
    def get_float_attribute(self, run_id: str, path: List[str]) -> FloatAttribute:
        pass

    @abc.abstractmethod
    def get_int_attribute(self, run_id: str, path: List[str]) -> IntAttribute:
        pass

    @abc.abstractmethod
    def get_bool_attribute(self, run_id: str, path: List[str]) -> BoolAttribute:
        pass

    @abc.abstractmethod
    def get_file_attribute(self, run_id: str, path: List[str]) -> FileAttribute:
        pass

    @abc.abstractmethod
    def get_string_attribute(self, run_id: str, path: List[str]) -> StringAttribute:
        pass

    @abc.abstractmethod
    def get_datetime_attribute(self, run_id: str, path: List[str]) -> DatetimeAttribute:
        pass

    @abc.abstractmethod
    def get_artifact_attribute(self, run_id: str, path: List[str]) -> ArtifactAttribute:
        pass

    @abc.abstractmethod
    def list_artifact_files(
        self, project_id: str, artifact_hash: str
    ) -> List[ArtifactFileData]:
        pass

    @abc.abstractmethod
    def get_float_series_attribute(
        self, run_id: str, path: List[str]
    ) -> FloatSeriesAttribute:
        pass

    @abc.abstractmethod
    def get_string_series_attribute(
        self, run_id: str, path: List[str]
    ) -> StringSeriesAttribute:
        pass

    @abc.abstractmethod
    def get_string_set_attribute(
        self, run_id: str, path: List[str]
    ) -> StringSetAttribute:
        pass

    @abc.abstractmethod
    def download_file_series_by_index(
        self, run_id: str, path: List[str], index: int, destination: str
    ):
        pass

    @abc.abstractmethod
    def get_image_series_values(
        self, run_id: str, path: List[str], offset: int, limit: int
    ) -> ImageSeriesValues:
        pass

    @abc.abstractmethod
    def get_string_series_values(
        self, run_id: str, path: List[str], offset: int, limit: int
    ) -> StringSeriesValues:
        pass

    @abc.abstractmethod
    def get_float_series_values(
        self, run_id: str, path: List[str], offset: int, limit: int
    ) -> FloatSeriesValues:
        pass

    @abc.abstractmethod
    def get_run_url(
        self, run_id: str, workspace: str, project_name: str, short_id: str
    ) -> str:
        pass

    @abc.abstractmethod
    def fetch_atom_attribute_values(
        self, run_id: str, path: List[str]
    ) -> List[Tuple[str, AttributeType, Any]]:
        pass
