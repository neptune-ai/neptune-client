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
import re
from datetime import datetime
from pathlib import Path
from platform import node as get_hostname
from typing import Optional, List, Union

import click

from neptune.alpha.attributes import constants as attr_consts
from neptune.alpha.constants import (
    ASYNC_DIRECTORY,
    NEPTUNE_EXPERIMENT_DIRECTORY,
    OFFLINE_DIRECTORY,
)
from neptune.alpha.envs import PROJECT_ENV_NAME, CUSTOM_EXP_ID_ENV_NAME
from neptune.alpha.exceptions import (
    NeptuneExperimentResumeAndCustomIdCollision,
    NeptuneIncorrectProjectQualifiedNameException,
    NeptuneMissingProjectNameException,
)
from neptune.alpha.experiment import Experiment
from neptune.alpha.internal.backends.hosted_neptune_backend import HostedNeptuneBackend
from neptune.alpha.internal.backends.neptune_backend_mock import NeptuneBackendMock
from neptune.alpha.internal.backends.offline_neptune_backend import OfflineNeptuneBackend
from neptune.alpha.internal.backgroud_job_list import BackgroundJobList
from neptune.alpha.internal.containers.disk_queue import DiskQueue
from neptune.alpha.internal.credentials import Credentials
from neptune.alpha.internal.hardware.hardware_metric_reporting_job import HardwareMetricReportingJob
from neptune.alpha.internal.operation import Operation
from neptune.alpha.internal.operation_processors.async_operation_processor import AsyncOperationProcessor
from neptune.alpha.internal.operation_processors.offline_operation_processor import OfflineOperationProcessor
from neptune.alpha.internal.operation_processors.sync_operation_processor import SyncOperationProcessor
from neptune.alpha.internal.streams.std_capture_background_job import (
    StderrCaptureBackgroundJob,
    StdoutCaptureBackgroundJob,
)
from neptune.alpha.internal.utils import verify_type, verify_collection_type
from neptune.alpha.internal.utils.git import get_git_info, discover_git_repo_location
from neptune.alpha.internal.utils.source_code import upload_source_code
from neptune.alpha.internal.utils.ping_background_job import PingBackgroundJob
from neptune.alpha.types.series.string_series import StringSeries
from neptune.alpha.version import version as parsed_version
from neptune.patterns import PROJECT_QUALIFIED_NAME_PATTERN

__version__ = str(parsed_version)

_logger = logging.getLogger(__name__)

OFFLINE = "offline"
DEBUG = "debug"
ASYNC = "async"
SYNC = "sync"


def init(project: Optional[str] = None,
         api_token: Optional[str] = None,
         experiment: Optional[str] = None,
         custom_experiment_id: Optional[str] = None,
         connection_mode: str = ASYNC,
         name: Optional[str] = None,
         description: Optional[str] = None,
         tags: Optional[Union[List[str], str]] = None,
         source_files: Optional[Union[List[str], str]] = None,
         capture_stdout: bool = True,
         capture_stderr: bool = True,
         capture_hardware_metrics: bool = True,
         monitoring_namespace: str = "monitoring",
         flush_period: float = 5) -> Experiment:
    verify_type("project", project, (str, type(None)))
    verify_type("api_token", api_token, (str, type(None)))
    verify_type("experiment", experiment, (str, type(None)))
    verify_type("custom_experiment_id", custom_experiment_id, (str, type(None)))
    verify_type("connection_mode", connection_mode, str)
    verify_type("name", name, (str, type(None)))
    verify_type("description", description, (str, type(None)))
    verify_type("capture_stdout", capture_stdout, bool)
    verify_type("capture_stderr", capture_stderr, bool)
    verify_type("capture_hardware_metrics", capture_hardware_metrics, bool)
    verify_type("monitoring_namespace", monitoring_namespace, str)
    verify_type("flush_period", flush_period, (int, float))
    if tags is not None:
        if isinstance(tags, str):
            tags = [tags]
        else:
            verify_collection_type("tags", tags, str)
    if source_files is not None:
        if isinstance(source_files, str):
            source_files = [source_files]
        else:
            verify_collection_type("source_files", source_files, str)

    name = "Untitled" if experiment is None and name is None else name
    description = "" if experiment is None and description is None else description
    hostname = get_hostname() if experiment is None else None
    custom_experiment_id = custom_experiment_id or os.getenv(CUSTOM_EXP_ID_ENV_NAME)

    if experiment and custom_experiment_id:
        raise NeptuneExperimentResumeAndCustomIdCollision()

    if connection_mode == ASYNC:
        # TODO Initialize backend in async thread
        backend = HostedNeptuneBackend(Credentials(api_token=api_token))
    elif connection_mode == SYNC:
        backend = HostedNeptuneBackend(Credentials(api_token=api_token))
    elif connection_mode == DEBUG:
        backend = NeptuneBackendMock()
    elif connection_mode == OFFLINE:
        backend = OfflineNeptuneBackend()
    else:
        raise ValueError('connection_mode should be one of ["async", "sync", "offline", "debug"]')

    if not project:
        project = os.getenv(PROJECT_ENV_NAME)
    elif connection_mode == 'offline':
        project = 'offline/project-placeholder'

    if not project:
        raise NeptuneMissingProjectNameException()
    if not re.match(PROJECT_QUALIFIED_NAME_PATTERN, project):
        raise NeptuneIncorrectProjectQualifiedNameException(project)

    project_obj = backend.get_project(project)
    if experiment:
        exp = backend.get_experiment(project + '/' + experiment)
    else:
        git_ref = get_git_info(discover_git_repo_location())
        if custom_experiment_id and len(custom_experiment_id) > 32:
            _logger.warning('Given custom_experiment_id exceeds 32 characters and it will be ignored.')
            custom_experiment_id = None
        exp = backend.create_experiment(project_obj.uuid, git_ref, custom_experiment_id)

    if connection_mode == ASYNC:
        experiment_path = "{}/{}/{}".format(NEPTUNE_EXPERIMENT_DIRECTORY, ASYNC_DIRECTORY, exp.uuid)
        try:
            execution_id = len(os.listdir(experiment_path))
        except FileNotFoundError:
            execution_id = 0
        execution_path = "{}/exec-{}-{}".format(experiment_path, execution_id, datetime.now())
        execution_path = execution_path.replace(" ", "_").replace(":", ".")
        operation_processor = AsyncOperationProcessor(
            exp.uuid,
            DiskQueue(Path(execution_path), lambda x: x.to_dict(), Operation.from_dict),
            backend,
            sleep_time=flush_period)
    elif connection_mode == SYNC:
        operation_processor = SyncOperationProcessor(exp.uuid, backend)
    elif connection_mode == DEBUG:
        operation_processor = SyncOperationProcessor(exp.uuid, backend)
    elif connection_mode == OFFLINE:
        # Experiment was returned by mocked backend and has some random UUID.
        experiment_path = "{}/{}/{}".format(NEPTUNE_EXPERIMENT_DIRECTORY, OFFLINE_DIRECTORY, exp.uuid)
        storage_queue = DiskQueue(Path(experiment_path),
                                  lambda x: x.to_dict(),
                                  Operation.from_dict)
        operation_processor = OfflineOperationProcessor(storage_queue)
    else:
        raise ValueError('connection_mode should be on of ["async", "sync", "offline", "debug"]')

    stdout_path = "{}/stdout".format(monitoring_namespace)
    stderr_path = "{}/stderr".format(monitoring_namespace)

    background_jobs = []
    if capture_stdout:
        background_jobs.append(StdoutCaptureBackgroundJob(attribute_name=stdout_path))
    if capture_stderr:
        background_jobs.append(StderrCaptureBackgroundJob(attribute_name=stderr_path))
    if capture_hardware_metrics:
        background_jobs.append(HardwareMetricReportingJob(attribute_namespace=monitoring_namespace))
    background_jobs.append(PingBackgroundJob())

    _experiment = Experiment(exp.uuid, backend, operation_processor, BackgroundJobList(background_jobs))
    if connection_mode != OFFLINE:
        _experiment.sync(wait=False)

    if name is not None:
        _experiment[attr_consts.SYSTEM_NAME_ATTRIBUTE_PATH] = name
    if description is not None:
        _experiment[attr_consts.SYSTEM_DESCRIPTION_ATTRIBUTE_PATH] = description
    if hostname is not None:
        _experiment[attr_consts.SYSTEM_HOSTNAME_ATTRIBUTE_PATH] = hostname
    if tags is not None:
        _experiment[attr_consts.SYSTEM_TAGS_ATTRIBUTE_PATH].add(tags)

    if capture_stdout and not _experiment.exists(stdout_path):
        _experiment.define(stdout_path, StringSeries([]))
    if capture_stderr and not _experiment.exists(stderr_path):
        _experiment.define(stderr_path, StringSeries([]))

    upload_source_code(source_files=source_files, experiment=_experiment)

    _experiment.start()

    if connection_mode == OFFLINE:
        click.echo("offline/{}.".format(exp.uuid))
    else:
        click.echo("{base_url}/{workspace}/{project}/e/{exp_id}".format(
            base_url=backend.get_display_address(),
            workspace=exp.workspace,
            project=exp.project_name,
            exp_id=exp.short_id
        ))

    return _experiment
