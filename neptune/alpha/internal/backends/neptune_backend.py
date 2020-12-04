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

from neptune.alpha.exceptions import NeptuneException

from neptune.alpha.internal.backends.api_model import Project, Experiment, Attribute, FloatAttribute, StringAttribute, \
    DatetimeAttribute, FloatSeriesAttribute, StringSeriesAttribute, StringSetAttribute
from neptune.alpha.internal.operation import Operation
from neptune.alpha.types.atoms import GitRef


class NeptuneBackend:

    def get_display_address(self) -> str:
        pass

    @abc.abstractmethod
    def get_project(self, project_id: str) -> Project:
        pass

    @abc.abstractmethod
    def get_experiment(self, experiment_id: str) -> Experiment:
        pass

    @abc.abstractmethod
    def create_experiment(self,
                          project_uuid: uuid.UUID,
                          git_ref: Optional[GitRef] = None,
                          custom_experiment_id: Optional[str] = None
                          ) -> Experiment:
        pass

    @abc.abstractmethod
    def execute_operations(self, experiment_uuid: uuid.UUID, operations: List[Operation]) -> List[NeptuneException]:
        pass

    @abc.abstractmethod
    def get_attributes(self, experiment_uuid: uuid.UUID) -> List[Attribute]:
        pass

    @abc.abstractmethod
    def download_file(self, experiment_uuid: uuid.UUID, path: List[str], destination: Optional[str] = None):
        pass

    @abc.abstractmethod
    def download_file_set(self, experiment_uuid: uuid.UUID, path: List[str], destination: Optional[str] = None):
        pass

    @abc.abstractmethod
    def get_float_attribute(self, experiment_uuid: uuid.UUID, path: List[str]) -> FloatAttribute:
        pass

    @abc.abstractmethod
    def get_string_attribute(self, experiment_uuid: uuid.UUID, path: List[str]) -> StringAttribute:
        pass

    @abc.abstractmethod
    def get_datetime_attribute(self, experiment_uuid: uuid.UUID, path: List[str]) -> DatetimeAttribute:
        pass

    @abc.abstractmethod
    def get_float_series_attribute(self, experiment_uuid: uuid.UUID, path: List[str]) -> FloatSeriesAttribute:
        pass

    @abc.abstractmethod
    def get_string_series_attribute(self, experiment_uuid: uuid.UUID, path: List[str]) -> StringSeriesAttribute:
        pass

    @abc.abstractmethod
    def get_string_set_attribute(self, experiment_uuid: uuid.UUID, path: List[str]) -> StringSetAttribute:
        pass
