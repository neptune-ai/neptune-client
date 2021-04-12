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
import uuid
from datetime import datetime
from pathlib import Path
from platform import node as get_hostname
from typing import List, Optional, Union

import click

from neptune.new.attributes import constants as attr_consts
from neptune.new.constants import (
    ASYNC_DIRECTORY,
    NEPTUNE_RUNS_DIRECTORY,
    OFFLINE_DIRECTORY,
)
from neptune.new.envs import CUSTOM_RUN_ID_ENV_NAME, NEPTUNE_NOTEBOOK_ID, NEPTUNE_NOTEBOOK_PATH, PROJECT_ENV_NAME
from neptune.new.exceptions import (NeptuneIncorrectProjectNameException, NeptuneMissingProjectNameException,
                                    NeptuneRunResumeAndCustomIdCollision)
from neptune.new.internal.backends.hosted_neptune_backend import HostedNeptuneBackend
from neptune.new.internal.backends.neptune_backend import NeptuneBackend
from neptune.new.internal.backends.neptune_backend_mock import NeptuneBackendMock
from neptune.new.internal.backends.offline_neptune_backend import OfflineNeptuneBackend
from neptune.new.internal.backgroud_job_list import BackgroundJobList
from neptune.new.internal.containers.disk_queue import DiskQueue
from neptune.new.internal.credentials import Credentials
from neptune.new.internal.hardware.hardware_metric_reporting_job import HardwareMetricReportingJob
from neptune.new.internal.notebooks.notebooks import create_checkpoint
from neptune.new.internal.operation import Operation
from neptune.new.internal.operation_processors.async_operation_processor import AsyncOperationProcessor
from neptune.new.internal.operation_processors.offline_operation_processor import OfflineOperationProcessor
from neptune.new.internal.operation_processors.sync_operation_processor import SyncOperationProcessor
from neptune.new.internal.streams.std_capture_background_job import (
    StderrCaptureBackgroundJob,
    StdoutCaptureBackgroundJob,
)
from neptune.new.internal.utils import verify_collection_type, verify_type
from neptune.new.internal.utils.git import discover_git_repo_location, get_git_info
from neptune.new.internal.utils.ping_background_job import PingBackgroundJob
from neptune.new.internal.utils.source_code import upload_source_code
from neptune.new.run import Run
from neptune.new.types.series.string_series import StringSeries
from neptune.new.version import version as parsed_version
from neptune.patterns import PROJECT_QUALIFIED_NAME_PATTERN

__version__ = str(parsed_version)

_logger = logging.getLogger(__name__)

OFFLINE = "offline"
DEBUG = "debug"
ASYNC = "async"
SYNC = "sync"


def init(project: Optional[str] = None,
         api_token: Optional[str] = None,
         run: Optional[str] = None,
         custom_run_id: Optional[str] = None,
         mode: str = ASYNC,
         name: Optional[str] = None,
         description: Optional[str] = None,
         tags: Optional[Union[List[str], str]] = None,
         source_files: Optional[Union[List[str], str]] = None,
         capture_stdout: bool = True,
         capture_stderr: bool = True,
         capture_hardware_metrics: bool = True,
         monitoring_namespace: str = "monitoring",
         flush_period: float = 5,
         proxies: dict = None) -> Run:
    verify_type("project", project, (str, type(None)))
    verify_type("api_token", api_token, (str, type(None)))
    verify_type("run", run, (str, type(None)))
    verify_type("custom_run_id", custom_run_id, (str, type(None)))
    verify_type("mode", mode, str)
    verify_type("name", name, (str, type(None)))
    verify_type("description", description, (str, type(None)))
    verify_type("capture_stdout", capture_stdout, bool)
    verify_type("capture_stderr", capture_stderr, bool)
    verify_type("capture_hardware_metrics", capture_hardware_metrics, bool)
    verify_type("monitoring_namespace", monitoring_namespace, str)
    verify_type("flush_period", flush_period, (int, float))
    verify_type("proxies", proxies, (dict, type(None)))
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

    name = "Untitled" if run is None and name is None else name
    description = "" if run is None and description is None else description
    hostname = get_hostname() if run is None else None
    custom_run_id = custom_run_id or os.getenv(CUSTOM_RUN_ID_ENV_NAME)

    if run and custom_run_id:
        raise NeptuneRunResumeAndCustomIdCollision()

    if mode == ASYNC:
        # TODO Initialize backend in async thread
        backend = HostedNeptuneBackend(
            credentials=Credentials(api_token=api_token),
            proxies=proxies)
    elif mode == SYNC:
        backend = HostedNeptuneBackend(
            credentials=Credentials(api_token=api_token),
            proxies=proxies)
    elif mode == DEBUG:
        backend = NeptuneBackendMock()
    elif mode == OFFLINE:
        backend = OfflineNeptuneBackend()
    else:
        raise ValueError('mode should be one of ["async", "sync", "offline", "debug"]')

    if mode == OFFLINE or mode == DEBUG:
        project = 'offline/project-placeholder'
    elif not project:
        project = os.getenv(PROJECT_ENV_NAME)
        if not project:
            raise NeptuneMissingProjectNameException()
    if not re.match(PROJECT_QUALIFIED_NAME_PATTERN, project):
        raise NeptuneIncorrectProjectNameException(project)

    project_obj = backend.get_project(project)
    if run:
        api_run = backend.get_run(project + '/' + run)
    else:
        git_ref = get_git_info(discover_git_repo_location())
        if custom_run_id and len(custom_run_id) > 32:
            _logger.warning('Given custom_run_id exceeds 32 characters and it will be ignored.')
            custom_run_id = None

        notebook_id, checkpoint_id = _create_notebook_checkpoint(backend)

        api_run = backend.create_run(project_obj.uuid, git_ref, custom_run_id, notebook_id, checkpoint_id)

    if mode == ASYNC:
        run_path = "{}/{}/{}".format(NEPTUNE_RUNS_DIRECTORY, ASYNC_DIRECTORY, api_run.uuid)
        try:
            execution_id = len(os.listdir(run_path))
        except FileNotFoundError:
            execution_id = 0
        execution_path = "{}/exec-{}-{}".format(run_path, execution_id, datetime.now())
        execution_path = execution_path.replace(" ", "_").replace(":", ".")
        operation_processor = AsyncOperationProcessor(
            api_run.uuid,
            DiskQueue(Path(execution_path), lambda x: x.to_dict(), Operation.from_dict),
            backend,
            sleep_time=flush_period)
    elif mode == SYNC:
        operation_processor = SyncOperationProcessor(api_run.uuid, backend)
    elif mode == DEBUG:
        operation_processor = SyncOperationProcessor(api_run.uuid, backend)
    elif mode == OFFLINE:
        # Run was returned by mocked backend and has some random UUID.
        run_path = "{}/{}/{}".format(NEPTUNE_RUNS_DIRECTORY, OFFLINE_DIRECTORY, api_run.uuid)
        storage_queue = DiskQueue(Path(run_path),
                                  lambda x: x.to_dict(),
                                  Operation.from_dict)
        operation_processor = OfflineOperationProcessor(storage_queue)
    else:
        raise ValueError('mode should be on of ["async", "sync", "offline", "debug"]')

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

    _run = Run(api_run.uuid, backend, operation_processor, BackgroundJobList(background_jobs))
    if mode != OFFLINE:
        _run.sync(wait=False)

    if name is not None:
        _run[attr_consts.SYSTEM_NAME_ATTRIBUTE_PATH] = name
    if description is not None:
        _run[attr_consts.SYSTEM_DESCRIPTION_ATTRIBUTE_PATH] = description
    if hostname is not None:
        _run[attr_consts.SYSTEM_HOSTNAME_ATTRIBUTE_PATH] = hostname
    if tags is not None:
        _run[attr_consts.SYSTEM_TAGS_ATTRIBUTE_PATH].add(tags)

    if capture_stdout and not _run.exists(stdout_path):
        _run.define(stdout_path, StringSeries([]))
    if capture_stderr and not _run.exists(stderr_path):
        _run.define(stderr_path, StringSeries([]))

    upload_source_code(source_files=source_files, run=_run)

    _run.start()

    if mode == OFFLINE:
        click.echo("offline/{}".format(api_run.uuid))
    elif mode != DEBUG:
        click.echo("{base_url}/{workspace}/{project}/e/{run_id}".format(
            base_url=backend.get_display_address(),
            workspace=api_run.workspace,
            project=api_run.project_name,
            run_id=api_run.short_id
        ))

    return _run


def _create_notebook_checkpoint(backend: NeptuneBackend) -> (uuid.UUID, uuid.UUID):
    notebook_id = None
    if os.getenv(NEPTUNE_NOTEBOOK_ID, None) is not None:
        try:
            notebook_id = uuid.UUID(os.environ[NEPTUNE_NOTEBOOK_ID])
        except ValueError:
            _logger.warning("Invalid notebook ID, must be an UUID")

    notebook_path = None
    if os.getenv(NEPTUNE_NOTEBOOK_PATH, None) is not None:
        notebook_path = os.environ[NEPTUNE_NOTEBOOK_PATH]

    checkpoint_id = None
    if notebook_id is not None and notebook_path is not None:
        checkpoint_id = create_checkpoint(backend=backend,
                                          notebook_id=notebook_id,
                                          notebook_path=notebook_path)
    return notebook_id, checkpoint_id
