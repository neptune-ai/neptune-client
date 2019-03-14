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
import os
import sys
import time
import traceback

from neptune.internal.abort import DefaultAbortImpl, CustomAbortImpl
from neptune.internal.hardware.gauges.gauge_mode import GaugeMode
from neptune.internal.hardware.metrics.service.metric_service_factory import MetricServiceFactory
from neptune.internal.hardware.system.system_monitor import SystemMonitor
from neptune.internal.streams.stdstream_uploader import StdOutWithUpload, StdErrWithUpload
from neptune.internal.threads.aborting_thread import AbortingThread
from neptune.internal.threads.hardware_metric_reporting_thread import HardwareMetricReportingThread
from neptune.internal.threads.ping_thread import PingThread
from neptune.internal.websockets.reconnecting_websocket_factory import ReconnectingWebsocketFactory
from neptune.utils import is_notebook, in_docker


class ExecutionContext(object):

    def __init__(self, client, experiment):
        self._client = client
        self._experiment = experiment
        self._ping_thread = None
        self._hardware_metric_thread = None
        self._aborting_thread = None
        self._stdout_uploader = None
        self._stderr_uploader = None
        self._uncaught_exception_handler = sys.__excepthook__

        self._previous_uncaught_exception_handler = None

    def start(self,
              abort_callback=None,
              upload_stdout=True,
              upload_stderr=True,
              send_hardware_metrics=True,
              run_monitoring_thread=True,
              handle_uncaught_exceptions=True):

        if handle_uncaught_exceptions:
            self._set_uncaught_exception_handler()

        abortable = abort_callback is not None or DefaultAbortImpl.requirements_installed()

        if abortable:
            self._run_aborting_thread(abort_callback)

        if upload_stdout and not is_notebook():
            self._stdout_uploader = StdOutWithUpload(self._experiment)

        if upload_stderr and not is_notebook():
            self._stderr_uploader = StdErrWithUpload(self._experiment)

        if run_monitoring_thread:
            self._run_monitoring_thread()

        if send_hardware_metrics and SystemMonitor.requirements_installed():
            self._run_hardware_metrics_reporting_thread()

    def stop(self):
        if self._ping_thread:
            self._ping_thread.interrupt()
            self._ping_thread = None

        if self._hardware_metric_thread:
            self._hardware_metric_thread.interrupt()
            self._hardware_metric_thread = None

        if self._aborting_thread:
            self._aborting_thread.interrupt()
            self._aborting_thread = None

        if self._stdout_uploader:
            self._stdout_uploader.close()

        if self._stderr_uploader:
            self._stderr_uploader.close()

        sys.excepthook = self._previous_uncaught_exception_handler

    def _set_uncaught_exception_handler(self):

        def exception_handler(exc_type, exc_val, exc_tb):
            self._experiment.stop("\n".join(traceback.format_tb(exc_tb)) + "\n" + repr(exc_val))

            sys.__excepthook__(exc_type, exc_val, exc_tb)

        self._uncaught_exception_handler = exception_handler

        self._previous_uncaught_exception_handler = sys.excepthook
        sys.excepthook = exception_handler

    def _run_aborting_thread(self, abort_callback):
        if abort_callback is not None:
            abort_impl = CustomAbortImpl(abort_callback)
        else:
            abort_impl = DefaultAbortImpl(pid=os.getpid())

        websocket_factory = ReconnectingWebsocketFactory(
            client=self._client,
            experiment_id=self._experiment.internal_id
        )
        self._aborting_thread = AbortingThread(
            websocket_factory=websocket_factory,
            abort_impl=abort_impl,
            experiment_id=self._experiment.internal_id
        )
        self._aborting_thread.start()

    def _run_monitoring_thread(self):
        self._ping_thread = PingThread(client=self._client, experiment=self._experiment)
        self._ping_thread.start()

    def _run_hardware_metrics_reporting_thread(self):
        gauge_mode = GaugeMode.CGROUP if in_docker() else GaugeMode.SYSTEM
        metric_service = MetricServiceFactory(self._client, os.environ).create(
            gauge_mode=gauge_mode,
            experiment=self._experiment,
            reference_timestamp=time.time()
        )

        self._hardware_metric_thread = HardwareMetricReportingThread(
            metric_service=metric_service,
            metric_sending_interval_seconds=3
        )
        self._hardware_metric_thread.start()
