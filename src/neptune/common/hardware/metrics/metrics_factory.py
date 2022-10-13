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
from neptune.common.hardware.constants import BYTES_IN_ONE_GB
from neptune.common.hardware.metrics.metric import (
    Metric,
    MetricResourceType,
)
from neptune.common.hardware.metrics.metrics_container import MetricsContainer


class MetricsFactory(object):
    def __init__(self, gauge_factory, system_resource_info):
        self.__gauge_factory = gauge_factory
        self.__system_resource_info = system_resource_info

    def create_metrics_container(self):
        cpu_usage_metric = self.__create_cpu_usage_metric()
        memory_metric = self.__create_memory_metric()

        has_gpu = self.__system_resource_info.has_gpu()
        gpu_usage_metric = self.__create_gpu_usage_metric() if has_gpu else None
        gpu_memory_metric = self.__create_gpu_memory_metric() if has_gpu else None

        return MetricsContainer(
            cpu_usage_metric=cpu_usage_metric,
            memory_metric=memory_metric,
            gpu_usage_metric=gpu_usage_metric,
            gpu_memory_metric=gpu_memory_metric,
        )

    def __create_cpu_usage_metric(self):
        return Metric(
            name="CPU - usage",
            description="average of all cores",
            resource_type=MetricResourceType.CPU,
            unit="%",
            min_value=0.0,
            max_value=100.0,
            gauges=[self.__gauge_factory.create_cpu_usage_gauge()],
        )

    def __create_memory_metric(self):
        return Metric(
            name="RAM",
            description="",
            resource_type=MetricResourceType.RAM,
            unit="GB",
            min_value=0.0,
            max_value=self.__system_resource_info.memory_amount_bytes / float(BYTES_IN_ONE_GB),
            gauges=[self.__gauge_factory.create_memory_usage_gauge()],
        )

    def __create_gpu_usage_metric(self):
        return Metric(
            name="GPU - usage",
            description="{} cards".format(self.__system_resource_info.gpu_card_count),
            resource_type=MetricResourceType.GPU,
            unit="%",
            min_value=0.0,
            max_value=100.0,
            gauges=[
                self.__gauge_factory.create_gpu_usage_gauge(card_index=card_index)
                for card_index in self.__system_resource_info.gpu_card_indices
            ],
        )

    def __create_gpu_memory_metric(self):
        return Metric(
            name="GPU - memory",
            description="{} cards".format(self.__system_resource_info.gpu_card_count),
            resource_type=MetricResourceType.GPU_RAM,
            unit="GB",
            min_value=0.0,
            max_value=self.__system_resource_info.gpu_memory_amount_bytes / float(BYTES_IN_ONE_GB),
            gauges=[
                self.__gauge_factory.create_gpu_memory_gauge(card_index=card_index)
                for card_index in self.__system_resource_info.gpu_card_indices
            ],
        )
