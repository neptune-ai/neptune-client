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

import logging
import os
import time

from itertools import groupby
from typing import TYPE_CHECKING, Optional, Dict

from neptune.new.types.series import FloatSeries

from neptune.internal.hardware.metrics.reports.metric_reporter import MetricReporter
from neptune.internal.hardware.metrics.reports.metric_reporter_factory import MetricReporterFactory
from neptune.internal.hardware.metrics.metrics_factory import MetricsFactory
from neptune.internal.hardware.gauges.gauge_factory import GaugeFactory
from neptune.internal.hardware.gpu.gpu_monitor import GPUMonitor
from neptune.internal.hardware.resources.system_resource_info_factory import SystemResourceInfoFactory
from neptune.internal.hardware.gauges.gauge_mode import GaugeMode
from neptune.internal.hardware.system.system_monitor import SystemMonitor
from neptune.utils import in_docker

from neptune.new.internal.background_job import BackgroundJob
from neptune.new.internal.threading.daemon import Daemon

if TYPE_CHECKING:
    from neptune.new.run import Run

_logger = logging.getLogger(__name__)


class HardwareMetricReportingJob(BackgroundJob):

    def __init__(self, period: float = 10, attribute_namespace: str = "monitoring"):
        self._period = period
        self._thread = None
        self._started = False
        self._gauges_in_resource: Dict[str, int] = dict()
        self._attribute_namespace = attribute_namespace

    @staticmethod
    def _requirements_installed() -> bool:
        return SystemMonitor.requirements_installed()

    def start(self, run: 'Run'):
        if not self._requirements_installed():
            _logger.warning('psutil is not installed. Hardware metrics will not be collected.')
            return

        gauge_mode = GaugeMode.CGROUP if in_docker() else GaugeMode.SYSTEM
        system_resource_info = SystemResourceInfoFactory(
            system_monitor=SystemMonitor(), gpu_monitor=GPUMonitor(), os_environ=os.environ
        ).create(gauge_mode=gauge_mode)
        gauge_factory = GaugeFactory(gauge_mode=gauge_mode)
        metrics_factory = MetricsFactory(gauge_factory=gauge_factory, system_resource_info=system_resource_info)
        metrics_container = metrics_factory.create_metrics_container()
        metric_reporter = MetricReporterFactory(time.time()).create(metrics=metrics_container.metrics())

        for metric in metrics_container.metrics():
            self._gauges_in_resource[metric.resource_type] = len(metric.gauges)

        for metric in metrics_container.metrics():
            for gauge in metric.gauges:
                path = self.get_attribute_name(metric.resource_type, gauge.name())
                if not run.get_attribute(path):
                    run[path] = FloatSeries([], min=metric.min_value, max=metric.max_value, unit=metric.unit)

        self._thread = self.ReportingThread(
            self,
            self._period,
            run,
            metric_reporter)
        self._thread.start()
        self._started = True

    def stop(self):
        if not self._started:
            return
        self._thread.interrupt()

    def join(self, seconds: Optional[float] = None):
        if not self._started:
            return
        self._thread.join(seconds)

    def get_attribute_name(self, resource_type, gauge_name) -> str:
        gauges_count = self._gauges_in_resource.get(resource_type, None)
        if gauges_count is None or gauges_count != 1:
            return "{}/{}_{}".format(self._attribute_namespace, resource_type, gauge_name).lower()
        return "{}/{}".format(self._attribute_namespace, resource_type).lower()

    class ReportingThread(Daemon):

        def __init__(
                self,
                outer: 'HardwareMetricReportingJob',
                period: float,
                run: 'Run',
                metric_reporter: MetricReporter):
            super().__init__(sleep_time=period)
            self._outer = outer
            self._run = run
            self._metric_reporter = metric_reporter

        def work(self) -> None:
            metric_reports = self._metric_reporter.report(time.time())
            for report in metric_reports:
                for gauge_name, metric_values in groupby(report.values, lambda value: value.gauge_name):
                    attr = self._run[self._outer.get_attribute_name(report.metric.resource_type, gauge_name)]
                    # TODO: Avoid loop
                    for metric_value in metric_values:
                        attr.log(value=metric_value.value, timestamp=metric_value.timestamp)
