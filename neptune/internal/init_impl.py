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

from typing import Optional

import click

from neptune.envs import PROJECT_ENV_NAME
from neptune.exceptions import MissingProject
from neptune.internal.backgroud_job_list import BackgroundJobList
from neptune.internal.hardware.hardware_metric_reporting_job import HardwareMetricReportingJob
from neptune.internal.operation_processors.async_operation_processor import AsyncOperationProcessor
from neptune.internal.backends.hosted_neptune_backend import HostedNeptuneBackend
from neptune.internal.backends.neptune_backend_mock import NeptuneBackendMock
from neptune.internal.containers.disk_queue import DiskQueue
from neptune.internal.credentials import Credentials
from neptune.internal.operation import VersionedOperation
from neptune.internal.operation_processors.sync_operation_processor import SyncOperationProcessor
from neptune.version import version as parsed_version
from neptune.experiment import Experiment


__version__ = str(parsed_version)


def init(
        project: Optional[str] = None,
        connection_mode: str = "async",
        capture_hardware_metrics=True,
        flush_period: float = 5) -> Experiment:

    if not project:
        project = os.getenv(PROJECT_ENV_NAME)
    if not project:
        raise MissingProject()

    credentials = Credentials()

    if connection_mode == "async":
        backend = HostedNeptuneBackend(credentials)
    elif connection_mode == "sync":
        backend = HostedNeptuneBackend(credentials)
    elif connection_mode == "offline":
        backend = NeptuneBackendMock(credentials)
    else:
        raise ValueError('connection_mode should be on of ["async", "sync", "offline"]')

    project_obj = backend.get_project(project)
    exp = backend.create_experiment(project_obj.uuid)

    if connection_mode == "async":
        operation_processor = AsyncOperationProcessor(
            DiskQueue(".neptune/{}".format(exp.uuid),
                      "operations",
                      VersionedOperation.to_dict,
                      VersionedOperation.from_dict),
            backend,
            sleep_time=flush_period)
    elif connection_mode == "sync":
        operation_processor = SyncOperationProcessor(backend)
    elif connection_mode == "offline":
        operation_processor = SyncOperationProcessor(backend)
    else:
        raise ValueError('connection_mode should be on of ["async", "sync", "offline"]')

    background_jobs = []
    if capture_hardware_metrics:
        background_jobs.append(HardwareMetricReportingJob())

    click.echo("{base_url}/{workspace}/{project}/e/{exp_id}".format(
        base_url=backend.get_display_address(),
        workspace=project_obj.workspace,
        project=project_obj.name,
        exp_id=exp.id
    ))

    return Experiment(exp.uuid, backend, operation_processor, BackgroundJobList(background_jobs))
