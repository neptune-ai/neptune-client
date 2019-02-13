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
from neptune.internal.hardware.gauges.gauge_factory import GaugeFactory
from neptune.internal.hardware.gpu.gpu_monitor import GPUMonitor
from neptune.internal.hardware.metrics.metrics_factory import MetricsFactory
from neptune.internal.hardware.metrics.reports.metric_reporter_factory import MetricReporterFactory
from neptune.internal.hardware.metrics.service.metric_service import MetricService
from neptune.internal.hardware.resources.system_resource_info_factory import SystemResourceInfoFactory


class MetricServiceFactory(object):
    def __init__(self, client, os_environ):
        self.__client = client
        self.__os_environ = os_environ

    def create(self, gauge_mode, experiment_id, reference_timestamp):
        system_resource_info = SystemResourceInfoFactory(
            gpu_monitor=GPUMonitor(), os_environ=self.__os_environ
        ).create(gauge_mode=gauge_mode)

        gauge_factory = GaugeFactory(gauge_mode=gauge_mode)
        metrics_factory = MetricsFactory(gauge_factory=gauge_factory, system_resource_info=system_resource_info)
        metrics_container = metrics_factory.create_metrics_container()

        for metric in metrics_container.metrics():
            metric.internal_id = self.__client.create_hardware_metric(experiment_id, metric)

        metric_reporter = MetricReporterFactory(reference_timestamp).create(metrics=metrics_container.metrics())

        return MetricService(
            client=self.__client,
            metric_reporter=metric_reporter,
            experiment_id=experiment_id,
            metrics_container=metrics_container
        )
