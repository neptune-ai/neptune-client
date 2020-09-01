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
import uuid
import socket

from uuid import UUID

from neptune_old.internal.hardware.metrics.metrics_container import MetricsContainer
from neptune_old.internal.hardware.metrics.reports.metric_reporter import MetricReporter
from neptune_old.internal.hardware.metrics.reports.metric_reporter_factory import MetricReporterFactory
from neptune_old.internal.hardware.metrics.metrics_factory import MetricsFactory
from neptune_old.internal.hardware.gauges.gauge_factory import GaugeFactory
from neptune_old.internal.hardware.gpu.gpu_monitor import GPUMonitor
from neptune_old.internal.hardware.resources.system_resource_info_factory import SystemResourceInfoFactory
from neptune_old.internal.hardware.gauges.gauge_mode import GaugeMode
from neptune_old.internal.hardware.system.system_monitor import SystemMonitor
from neptune_old.utils import in_docker

from neptune.internal.backends.neptune_backend import NeptuneBackend
from neptune.internal.background_job import BackgroundJob
from neptune.internal.threading.daemon import Daemon


_logger = logging.getLogger(__name__)


class HardwareMetricReportingJob(BackgroundJob):

    def __init__(self, exp_uuid: UUID, backend: NeptuneBackend, period: float = 10):
        self._exp_uuid = exp_uuid
        self._backend = backend
        self._period = period
        self._random_execution_id = "{}-{}".format(socket.gethostname(), str(uuid.uuid4()))
        self._thread = None
        self._started = False

    def start(self):
        if not SystemMonitor.requirements_installed():
            _logger.warning('psutil is not installed. Hardware metrics will not be collected.')
            return

        gauge_mode = GaugeMode.CGROUP if in_docker() else GaugeMode.SYSTEM
        system_resource_info = SystemResourceInfoFactory(
            system_monitor=SystemMonitor(), gpu_monitor=GPUMonitor(), os_environ=os.environ
        ).create(gauge_mode=gauge_mode)
        gauge_factory = GaugeFactory(gauge_mode=gauge_mode)
        metrics_factory = MetricsFactory(gauge_factory=gauge_factory, system_resource_info=system_resource_info)
        metrics_container = metrics_factory.create_metrics_container()
        for metric in metrics_container.metrics():
            metric.internal_id = self._backend.create_hardware_metric(self._exp_uuid, self._random_execution_id, metric)
        metric_reporter = MetricReporterFactory(time.time()).create(metrics=metrics_container.metrics())

        self._thread = self.ReportingThread(
            self._period,
            self._exp_uuid,
            self._backend,
            metric_reporter,
            metrics_container)
        self._thread.start()
        self._started = True

    def stop(self):
        if not self._started:
            return
        self._thread.interrupt()

    def join(self):
        self._thread.join()

    class ReportingThread(Daemon):

        def __init__(
                self,
                period: float,
                exp_uuid: UUID,
                backend: NeptuneBackend,
                metric_reporter: MetricReporter,
                metrics_container: MetricsContainer):
            super().__init__(sleep_time=period)
            self._exp_uuid = exp_uuid
            self._backend = backend
            self._metric_reporter = metric_reporter
            self._metrics_container = metrics_container

        def work(self) -> None:
            metric_report = self._metric_reporter.report(time.time())
            self._backend.send_hardware_metric_reports(self._exp_uuid, self._metrics_container.metrics(), metric_report)
