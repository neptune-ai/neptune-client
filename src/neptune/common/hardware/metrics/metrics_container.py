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
from typing import (
    TYPE_CHECKING,
    List,
)

if TYPE_CHECKING:
    from neptune.common.hardware.metrics.metric import Metric


class MetricsContainer:
    def __init__(
        self,
        cpu_usage_metric: "Metric",
        memory_metric: "Metric",
        gpu_usage_metric: "Metric",
        gpu_memory_metric: "Metric",
    ) -> None:
        self.cpu_usage_metric = cpu_usage_metric
        self.memory_metric = memory_metric
        self.gpu_usage_metric = gpu_usage_metric
        self.gpu_memory_metric = gpu_memory_metric

    def metrics(self) -> List["Metric"]:
        return [
            metric
            for metric in [
                self.cpu_usage_metric,
                self.memory_metric,
                self.gpu_usage_metric,
                self.gpu_memory_metric,
            ]
            if metric is not None
        ]
