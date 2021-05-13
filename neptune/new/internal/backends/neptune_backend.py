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
import uuid
from typing import List, Optional

from neptune.new.exceptions import NeptuneException
from neptune.new.internal.backends.api_model import (
    ApiRun,
    Attribute,
    BoolAttribute,
    DatetimeAttribute,
    FileAttribute,
    FloatAttribute,
    FloatSeriesAttribute,
    IntAttribute,
    Project,
    StringAttribute,
    StringSeriesAttribute,
    StringSetAttribute,
    StringSeriesValues,
    FloatSeriesValues,
    ImageSeriesValues,
)
from neptune.new.internal.operation import Operation
from neptune.new.internal.websockets.websockets_factory import WebsocketsFactory
from neptune.new.types.atoms import GitRef


class NeptuneBackend:

    @abc.abstractmethod
    def get_display_address(self) -> str:
        pass

    # pylint: disable=unused-argument
    def websockets_factory(self, project_uuid: uuid.UUID, run_uuid: uuid.UUID) -> Optional[WebsocketsFactory]:
        return None

    @abc.abstractmethod
    def get_project(self, project_id: str) -> Project:
        pass

    @abc.abstractmethod
    def get_run(self, run_id: str) -> ApiRun:
        pass

    @abc.abstractmethod
    def create_run(self,
                   project_uuid: uuid.UUID,
                   git_ref: Optional[GitRef] = None,
                   custom_run_id: Optional[str] = None,
                   notebook_id: Optional[uuid.UUID] = None,
                   checkpoint_id: Optional[uuid.UUID] = None
                   ) -> ApiRun:
        pass

    @abc.abstractmethod
    def create_checkpoint(self, notebook_id: uuid.UUID, jupyter_path: str) -> Optional[uuid.UUID]:
        pass

    def ping_run(self, run_uuid: uuid.UUID):
        pass

    @abc.abstractmethod
    def execute_operations(self, run_uuid: uuid.UUID, operations: List[Operation]) -> List[NeptuneException]:
        pass

    @abc.abstractmethod
    def get_attributes(self, run_uuid: uuid.UUID) -> List[Attribute]:
        pass

    @abc.abstractmethod
    def download_file(self, run_uuid: uuid.UUID, path: List[str], destination: Optional[str] = None):
        pass

    @abc.abstractmethod
    def download_file_set(self, run_uuid: uuid.UUID, path: List[str], destination: Optional[str] = None):
        pass

    @abc.abstractmethod
    def get_float_attribute(self, run_uuid: uuid.UUID, path: List[str]) -> FloatAttribute:
        pass

    @abc.abstractmethod
    def get_int_attribute(self, run_uuid: uuid.UUID, path: List[str]) -> IntAttribute:
        pass

    @abc.abstractmethod
    def get_bool_attribute(self, run_uuid: uuid.UUID, path: List[str]) -> BoolAttribute:
        pass

    @abc.abstractmethod
    def get_file_attribute(self, run_uuid: uuid.UUID, path: List[str]) -> FileAttribute:
        pass

    @abc.abstractmethod
    def get_string_attribute(self, run_uuid: uuid.UUID, path: List[str]) -> StringAttribute:
        pass

    @abc.abstractmethod
    def get_datetime_attribute(self, run_uuid: uuid.UUID, path: List[str]) -> DatetimeAttribute:
        pass

    @abc.abstractmethod
    def get_float_series_attribute(self, run_uuid: uuid.UUID, path: List[str]) -> FloatSeriesAttribute:
        pass

    @abc.abstractmethod
    def get_string_series_attribute(self, run_uuid: uuid.UUID, path: List[str]) -> StringSeriesAttribute:
        pass

    @abc.abstractmethod
    def get_string_set_attribute(self, run_uuid: uuid.UUID, path: List[str]) -> StringSetAttribute:
        pass

    @abc.abstractmethod
    def download_file_series_by_index(self, run_uuid: uuid.UUID, path: List[str],
                                      index: int, destination: str):
        pass

    @abc.abstractmethod
    def get_image_series_values(self, run_uuid: uuid.UUID, path: List[str],
                                offset: int, limit: int) -> ImageSeriesValues:
        pass

    @abc.abstractmethod
    def get_string_series_values(self, run_uuid: uuid.UUID, path: List[str],
                                 offset: int, limit: int) -> StringSeriesValues:
        pass

    @abc.abstractmethod
    def get_float_series_values(self, run_uuid: uuid.UUID, path: List[str],
                                offset: int, limit: int) -> FloatSeriesValues:
        pass

    @abc.abstractmethod
    def get_run_url(self, run_uuid: uuid, workspace: str, project_name: str, short_id: str) -> str:
        pass
