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

import os

from pathlib import Path
from typing import Optional

import click

from neptune.alpha.constants import NEPTUNE_EXPERIMENT_DIRECTORY, OPERATIONS_DISK_QUEUE_PREFIX, OFFLINE_DIRECTORY
from neptune.alpha.envs import PROJECT_ENV_NAME
from neptune.alpha.exceptions import MissingProject
from neptune.alpha.internal.backgroud_job_list import BackgroundJobList
from neptune.alpha.internal.hardware.hardware_metric_reporting_job import HardwareMetricReportingJob
from neptune.alpha.internal.operation_processors.async_operation_processor import AsyncOperationProcessor
from neptune.alpha.internal.backends.hosted_neptune_backend import HostedNeptuneBackend
from neptune.alpha.internal.backends.neptune_backend_mock import NeptuneBackendMock
from neptune.alpha.internal.containers.disk_queue import DiskQueue
from neptune.alpha.internal.credentials import Credentials
from neptune.alpha.internal.operation import VersionedOperation
from neptune.alpha.internal.operation_processors.sync_operation_processor import SyncOperationProcessor
from neptune.alpha.internal.operation_processors.offline_operation_processor import OfflineOperationProcessor
from neptune.alpha.internal.streams.std_capture_background_job import StdoutCaptureBackgroundJob, \
    StderrCaptureBackgroundJob
from neptune.alpha.internal.utils.sync_offset_file import SyncOffsetFile
from neptune.alpha.version import version as parsed_version
from neptune.alpha.experiment import Experiment


__version__ = str(parsed_version)


def init(
        project: Optional[str] = None,
        experiment: Optional[str] = None,
        connection_mode: str = "async",
        capture_stdout: bool = True,
        capture_stderr: bool = True,
        capture_hardware_metrics: bool = True,
        flush_period: float = 5) -> Experiment:

    if not project:
        project = os.getenv(PROJECT_ENV_NAME)
    if connection_mode == 'offline':
        project = 'offline-project-placeholder'
    if not project:
        raise MissingProject()

    if connection_mode == "async":
        # TODO Initialize backend in async thread
        backend = HostedNeptuneBackend(Credentials())
    elif connection_mode == "sync":
        backend = HostedNeptuneBackend(Credentials())
    elif connection_mode == "debug":
        backend = NeptuneBackendMock()
    elif connection_mode == "offline":
        backend = NeptuneBackendMock()
    else:
        raise ValueError('connection_mode should be one of ["async", "sync", "offline", "debug"]')

    project_obj = backend.get_project(project)
    if experiment:
        exp = backend.get_experiment(project + '/' + experiment)
    else:
        exp = backend.create_experiment(project_obj.uuid)

    if connection_mode == "async":
        experiment_path = "{}/{}".format(NEPTUNE_EXPERIMENT_DIRECTORY, exp.uuid)
        operation_processor = AsyncOperationProcessor(
            exp.uuid,
            DiskQueue(experiment_path,
                      OPERATIONS_DISK_QUEUE_PREFIX,
                      VersionedOperation.to_dict,
                      VersionedOperation.from_dict),
            backend,
            SyncOffsetFile(Path(experiment_path)),
            sleep_time=flush_period)
    elif connection_mode == "sync":
        operation_processor = SyncOperationProcessor(exp.uuid, backend)
    elif connection_mode == "debug":
        operation_processor = SyncOperationProcessor(exp.uuid, backend)
    elif connection_mode == "offline":
        experiment_path = "{}/{}/{}".format(NEPTUNE_EXPERIMENT_DIRECTORY, OFFLINE_DIRECTORY, exp.uuid)
        storage_queue = DiskQueue(experiment_path,
                                  OPERATIONS_DISK_QUEUE_PREFIX,
                                  VersionedOperation.to_dict,
                                  VersionedOperation.from_dict)
        operation_processor = OfflineOperationProcessor(storage_queue)
    else:
        raise ValueError('connection_mode should be on of ["async", "sync", "offline", "debug"]')

    background_jobs = []
    if capture_hardware_metrics:
        background_jobs.append(HardwareMetricReportingJob())
    if capture_stdout:
        background_jobs.append(StdoutCaptureBackgroundJob())
    if capture_stderr:
        background_jobs.append(StderrCaptureBackgroundJob())

    click.echo("{base_url}/{workspace}/{project}/e/{exp_id}".format(
        base_url=backend.get_display_address(),
        workspace=project_obj.workspace,
        project=project_obj.name,
        exp_id=exp.id
    ))

    return Experiment(
        exp.uuid,
        backend,
        operation_processor,
        BackgroundJobList(background_jobs),
        resume=experiment is not None
    )
