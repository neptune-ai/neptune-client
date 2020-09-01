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

from typing import List
from uuid import UUID

from neptune_old.internal.hardware.metrics.reports.metric_report import MetricReport

from neptune_old.internal.hardware.metrics.metric import Metric

from neptune.internal.backends.api_model import Project, Experiment
from neptune.internal.operation import Operation


class NeptuneBackend:

    def get_display_address(self) -> str:
        pass

    @abc.abstractmethod
    def get_project(self, project_id: str) -> Project:
        pass

    @abc.abstractmethod
    def create_experiment(self, project_uuid: uuid.UUID) -> Experiment:
        pass

    @abc.abstractmethod
    def execute_operations(self, operations: List[Operation]) -> None:
        pass

    @abc.abstractmethod
    def send_hardware_metric_reports(
            self,
            experiment_uuid: UUID,
            metrics: List[Metric],
            metric_reports: List[MetricReport]) -> None:
        pass

    @abc.abstractmethod
    def create_hardware_metric(self, experiment_uuid: UUID, exec_id: str, metric: Metric) -> UUID:
        pass
