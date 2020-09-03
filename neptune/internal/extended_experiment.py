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

import uuid

from neptune.experiment import Experiment
from neptune.internal.backends.neptune_backend import NeptuneBackend
from neptune.internal.backgroud_job_list import BackgroundJobList
from neptune.internal.hardware.hardware_metric_reporting_job import HardwareMetricReportingJob
from neptune.internal.operation_processors.operation_processor import OperationProcessor


class ExtendedExperiment(Experiment):

    def __init__(
            self,
            _uuid: uuid.UUID,
            backend: NeptuneBackend,
            op_processor: OperationProcessor,
            capture_hardware_metrics: bool = True):
        super().__init__(_uuid, backend, op_processor)

        background_jobs = []
        if capture_hardware_metrics:
            background_jobs.append(HardwareMetricReportingJob(self))

        self._bg_job = BackgroundJobList(background_jobs)
        self._bg_job.start()

    def close(self):
        with self._lock:
            self._bg_job.stop()
            self._bg_job.join()
            super().close()
