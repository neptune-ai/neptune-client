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
import logging
from logging import StreamHandler

from neptune.internal.abort import DefaultAbortImpl, CustomAbortImpl
from neptune.internal.channels.channels import ChannelNamespace
from neptune.internal.hardware.gauges.gauge_mode import GaugeMode
from neptune.internal.hardware.metrics.service.metric_service_factory import MetricServiceFactory
from neptune.internal.hardware.system.system_monitor import SystemMonitor
from neptune.internal.streams.channel_writer import ChannelWriter
from neptune.internal.streams.stdstream_uploader import StdOutWithUpload, StdErrWithUpload
from neptune.internal.threads.aborting_thread import AbortingThread
from neptune.internal.threads.hardware_metric_reporting_thread import HardwareMetricReportingThread
from neptune.internal.threads.ping_thread import PingThread
from neptune.utils import is_notebook, in_docker, is_ipython

_logger = logging.getLogger(__name__)


class ExecutionContext(object):

    def __init__(self, backend, experiment):
        self._backend = backend
        self._experiment = experiment
        self._ping_thread = None
        self._hardware_metric_thread = None
        self._aborting_thread = None
        self._logger = None
        self._logger_handler = None
        self._stdout_uploader = None
        self._stderr_uploader = None
        self._uncaught_exception_handler = sys.__excepthook__

        self._previous_uncaught_exception_handler = None

    def start(self,
              abort_callback=None,
              logger=None,
              upload_stdout=True,
              upload_stderr=True,
              send_hardware_metrics=True,
              run_monitoring_thread=True,
              handle_uncaught_exceptions=True):

        if handle_uncaught_exceptions:
            self._set_uncaught_exception_handler()

        if logger:
            # pylint: disable=protected-access
            channel = self._experiment._get_channel('logger', 'text', ChannelNamespace.SYSTEM)
            channel_writer = ChannelWriter(self._experiment, channel.name, ChannelNamespace.SYSTEM)
            self._logger_handler = StreamHandler(channel_writer)
            self._logger = logger
            logger.addHandler(self._logger_handler)

        if upload_stdout and not is_notebook():
            self._stdout_uploader = StdOutWithUpload(self._experiment)

        if upload_stderr and not is_notebook():
            self._stderr_uploader = StdErrWithUpload(self._experiment)

        abortable = abort_callback is not None or DefaultAbortImpl.requirements_installed()
        if abortable:
            self._run_aborting_thread(abort_callback)
        else:
            _logger.warning('psutil is not installed. You will not be able to abort this experiment from the UI.')

        if run_monitoring_thread:
            self._run_monitoring_thread()

        if send_hardware_metrics:
            if SystemMonitor.requirements_installed():
                self._run_hardware_metrics_reporting_thread()
            else:
                _logger.warning('psutil is not installed. Hardware metrics will not be collected.')

    def stop(self):
        if self._ping_thread:
            self._ping_thread.interrupt()
            self._ping_thread = None

        if self._hardware_metric_thread:
            self._hardware_metric_thread.interrupt()
            self._hardware_metric_thread = None

        if self._aborting_thread:
            self._aborting_thread.shutdown()
            self._aborting_thread = None

        if self._stdout_uploader:
            self._stdout_uploader.close()

        if self._stderr_uploader:
            self._stderr_uploader.close()

        if self._logger and self._logger_handler:
            self._logger.removeHandler(self._logger_handler)

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
        elif not is_ipython():
            abort_impl = DefaultAbortImpl(pid=os.getpid())
        else:
            return

        websocket_factory = self._backend.websockets_factory(
            # pylint: disable=protected-access
            project_uuid=self._experiment._project.internal_id,
            experiment_id=self._experiment.internal_id
        )
        if not websocket_factory:
            return

        self._aborting_thread = AbortingThread(
            websocket_factory=websocket_factory,
            abort_impl=abort_impl,
            experiment=self._experiment
        )
        self._aborting_thread.start()

    def _run_monitoring_thread(self):
        self._ping_thread = PingThread(backend=self._backend, experiment=self._experiment)
        self._ping_thread.start()

    def _run_hardware_metrics_reporting_thread(self):
        gauge_mode = GaugeMode.CGROUP if in_docker() else GaugeMode.SYSTEM
        metric_service = MetricServiceFactory(self._backend, os.environ).create(
            gauge_mode=gauge_mode,
            experiment=self._experiment,
            reference_timestamp=time.time()
        )

        self._hardware_metric_thread = HardwareMetricReportingThread(
            metric_service=metric_service,
            metric_sending_interval_seconds=3
        )
        self._hardware_metric_thread.start()
