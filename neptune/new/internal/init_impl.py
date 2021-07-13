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
import uuid
from datetime import datetime
from enum import Enum
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
from neptune.new.envs import (CUSTOM_RUN_ID_ENV_NAME, NEPTUNE_NOTEBOOK_ID, NEPTUNE_NOTEBOOK_PATH,
                              MONITORING_NAMESPACE)
from neptune.new.exceptions import (NeedExistingRunForReadOnlyMode, NeptuneRunResumeAndCustomIdCollision,
                                    NeptunePossibleLegacyUsageException)
from neptune.new.internal.backends.hosted_neptune_backend import HostedNeptuneBackend
from neptune.new.internal.backends.neptune_backend import NeptuneBackend
from neptune.new.internal.backends.project_name_lookup import project_name_lookup
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
from neptune.new.internal.operation_processors.read_only_operation_processor import ReadOnlyOperationProcessor
from neptune.new.internal.operation_processors.sync_operation_processor import SyncOperationProcessor
from neptune.new.internal.streams.std_capture_background_job import (
    StderrCaptureBackgroundJob,
    StdoutCaptureBackgroundJob,
)
from neptune.new.internal.utils import verify_collection_type, verify_type
from neptune.new.internal.utils.git import discover_git_repo_location, get_git_info
from neptune.new.internal.utils.ping_background_job import PingBackgroundJob
from neptune.new.internal.utils.runningmode import in_interactive, in_notebook
from neptune.new.internal.utils.source_code import upload_source_code
from neptune.new.internal.utils.traceback_job import TracebackJob
from neptune.new.internal.utils.uncaught_exception_handler import instance as uncaught_exception_handler
from neptune.new.internal.websockets.websocket_signals_background_job import WebsocketSignalsBackgroundJob
from neptune.new.run import Run
from neptune.new.types.series.string_series import StringSeries
from neptune.new.version import version as parsed_version

__version__ = str(parsed_version)

_logger = logging.getLogger(__name__)


LEGACY_KWARGS = ('project_qualified_name', 'backend')


class RunMode(str, Enum):
    OFFLINE = "offline"
    DEBUG = "debug"
    ASYNC = "async"
    SYNC = "sync"
    READ_ONLY = "read-only"

    def __repr__(self):
        return f'"{self.value}"'


def _check_for_extra_kwargs(caller_name, kwargs: dict):
    for name in LEGACY_KWARGS:
        if name in kwargs:
            raise NeptunePossibleLegacyUsageException()
    if kwargs:
        first_key = next(iter(kwargs.keys()))
        raise TypeError(f"{caller_name}() got an unexpected keyword argument '{first_key}'")


def init(project: Optional[str] = None,
         api_token: Optional[str] = None,
         run: Optional[str] = None,
         custom_run_id: Optional[str] = None,
         mode: str = RunMode.ASYNC,
         name: Optional[str] = None,
         description: Optional[str] = None,
         tags: Optional[Union[List[str], str]] = None,
         source_files: Optional[Union[List[str], str]] = None,
         capture_stdout: bool = True,
         capture_stderr: bool = True,
         capture_hardware_metrics: bool = True,
         fail_on_exception: bool = True,
         monitoring_namespace: Optional[str] = None,
         flush_period: float = 5,
         proxies: Optional[dict] = None,
         **kwargs) -> Run:
    """Starts a new tracked run, and append it to the top of the Runs table view.

    Args:
        project(str, optional): Name of a project in a form of `namespace/project_name`. Defaults to `None`.
            If `None`, the value of `NEPTUNE_PROJECT` environment variable will be taken.
        api_token(str, optional): User’s API token. Defaults to `None`.
            If `None`, the value of `NEPTUNE_API_TOKEN` environment variable will be taken.
            .. note::
                It is strongly recommended to use `NEPTUNE_API_TOKEN` environment variable rather than placing your
                API token in plain text in your source code.
        run (str, optional): An existing run's identifier like 'SAN-1' in case of resuming a tracked run.
            Defaults to `None`.
            A run with such identifier must exist. If `None` is passed, starts a new tracked run.
        custom_run_id (str, optional): A unique identifier to be used when running Neptune in pipelines.
            Defaults to `None`.
            Make sure you are using the same identifier throughout the whole pipeline execution.
        mode (str, optional): Connection mode in which the tracking will work. Defaults to `'async'`.
            Possible values 'async', 'sync', 'offline', 'read-only' and 'debug'.
        name (str, optional): Editable name of the run. Defaults to `'Untitled'`.
            Name is displayed in the run's Details and in Runs table as a column.
        description (str, optional): Editable description of the run. Defaults to `''`.
            Description is displayed in the run's Details and can be displayed in the runs view as a column.
        tags (list of str or str, optional): Tags of the run. Defaults to `[]`.
            They are editable after run is created.
            Tags are displayed in the run's Details and can be viewed in Runs table view as a column.
        source_files (list of str or str, optional): List of source files to be uploaded.
            Uploaded sources are displayed in the run’s Source code tab.
            Unix style pathname pattern expansion is supported. For example, you can pass '*.py' to upload all python
            source files from the current directory.
            If `None` is passed, Python file from which run was created will be uploaded.
        capture_stdout (bool, optional): Whether to send run's stdout. Defaults to `True`.
            Tracked metadata will be stored inside `monitoring_namespace`.
        capture_stderr (bool, optional):  Whether to send run’s stderr. Defaults to `True`.
            Tracked metadata will be stored inside `monitoring_namespace`.
        capture_hardware_metrics (bool, optional): Whether to send hardware monitoring logs
            (CPU, GPU, Memory utilization). Defaults to `True`.
            Tracked metadata will be stored inside `monitoring_namespace`.
        fail_on_exception (bool, optional): Whether to register an uncaught exception handler to this process and,
            in case of an exception, set run's sys/failed to True. Exception is always logged
        monitoring_namespace (str, optional): Namespace inside which all monitoring logs be stored.
            Defaults to 'monitoring'.
        flush_period (float, optional): In an asynchronous (default) connection mode how often asynchronous thread
            should synchronize data with Neptune servers. Defaults to 5.
        proxies (dict of str, optional): Argument passed to HTTP calls made via the Requests library.
            For more information see
            `their proxies section <https://2.python-requests.org/en/master/user/advanced/#proxies>`_.

    Returns:
        ``Run``: object that is used to manage the tracked run and log metadata to it.

    Examples:

        >>> import neptune.new as neptune

        >>> # minimal invoke
        ... run = neptune.init()

        >>> # create a tracked run with a name
        ... run = neptune.init(name='first-pytorch-ever')

        >>> # create a tracked run with a name and a description, and no sources files uploaded
        >>> run = neptune.init(name='neural-net-mnist',
        ...                    description='neural net trained on MNIST',
        ...                    source_files=[])

        >>> # Send all py files in cwd (excluding hidden files with names beginning with a dot)
        ... run = neptune.init(source_files='*.py')

        >>> # Send all py files from all subdirectories (excluding hidden files with names beginning with a dot)
        ... # Supported on Python 3.5 and later.
        ... run = neptune.init(source_files='**/*.py')

        >>> # Send all files and directories in cwd (excluding hidden files with names beginning with a dot)
        ... run = neptune.init(source_files='*')

        >>> # Send all files and directories in cwd including hidden files
        ... run = neptune.init(source_files=['*', '.*'])

        >>> # Send files with names being a single character followed by '.py' extension.
        ... run = neptune.init(source_files='?.py')

        >>> # larger example
        ... run = neptune.init(name='first-pytorch-ever',
        ...               description='write longer description here',
        ...               tags=['list-of', 'tags', 'goes-here', 'as-list-of-strings'],
        ...               source_files=['training_with_pytorch.py', 'net.py'])

    You may also want to check `init docs page`_.

    .. _init docs page:
       https://docs.neptune.ai/api-reference/neptune#init
    """
    _check_for_extra_kwargs(init.__name__, kwargs)
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
    verify_type("monitoring_namespace", monitoring_namespace, (str, type(None)))
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
    monitoring_namespace = monitoring_namespace or os.getenv(MONITORING_NAMESPACE) or 'monitoring'

    if run and custom_run_id:
        raise NeptuneRunResumeAndCustomIdCollision()

    if mode == RunMode.ASYNC:
        # TODO Initialize backend in async thread
        backend = HostedNeptuneBackend(
            credentials=Credentials(api_token=api_token),
            proxies=proxies)
    elif mode == RunMode.SYNC:
        backend = HostedNeptuneBackend(
            credentials=Credentials(api_token=api_token),
            proxies=proxies)
    elif mode == RunMode.DEBUG:
        backend = NeptuneBackendMock()
    elif mode == RunMode.OFFLINE:
        backend = OfflineNeptuneBackend()
    elif mode == RunMode.READ_ONLY:
        backend = HostedNeptuneBackend(
            credentials=Credentials(api_token=api_token),
            proxies=proxies)
    else:
        raise ValueError(f'mode should be one of {[m for m in RunMode]}')

    if mode == RunMode.OFFLINE or mode == RunMode.DEBUG:
        project = 'offline/project-placeholder'

    project_obj = project_name_lookup(backend, project)
    project = f'{project_obj.workspace}/{project_obj.name}'

    if run:
        api_run = backend.get_run(project + '/' + run)
    else:
        if mode == RunMode.READ_ONLY:
            raise NeedExistingRunForReadOnlyMode()
        git_ref = get_git_info(discover_git_repo_location())
        if custom_run_id and len(custom_run_id) > 32:
            _logger.warning('Given custom_run_id exceeds 32 characters and it will be ignored.')
            custom_run_id = None

        notebook_id, checkpoint_id = _create_notebook_checkpoint(backend)

        api_run = backend.create_run(project_obj.uuid, git_ref, custom_run_id, notebook_id, checkpoint_id)

    if mode == RunMode.ASYNC:
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
    elif mode == RunMode.SYNC:
        operation_processor = SyncOperationProcessor(api_run.uuid, backend)
    elif mode == RunMode.DEBUG:
        operation_processor = SyncOperationProcessor(api_run.uuid, backend)
    elif mode == RunMode.OFFLINE:
        # Run was returned by mocked backend and has some random UUID.
        run_path = "{}/{}/{}".format(NEPTUNE_RUNS_DIRECTORY, OFFLINE_DIRECTORY, api_run.uuid)
        storage_queue = DiskQueue(Path(run_path),
                                  lambda x: x.to_dict(),
                                  Operation.from_dict)
        operation_processor = OfflineOperationProcessor(storage_queue)
    elif mode == RunMode.READ_ONLY:
        operation_processor = ReadOnlyOperationProcessor(api_run.uuid, backend)
    else:
        raise ValueError(f'mode should be one of {[m for m in RunMode]}')

    stdout_path = "{}/stdout".format(monitoring_namespace)
    stderr_path = "{}/stderr".format(monitoring_namespace)
    traceback_path = "{}/traceback".format(monitoring_namespace)

    background_jobs = []
    if mode != RunMode.READ_ONLY:
        if capture_stdout:
            background_jobs.append(StdoutCaptureBackgroundJob(attribute_name=stdout_path))
        if capture_stderr:
            background_jobs.append(StderrCaptureBackgroundJob(attribute_name=stderr_path))
        if capture_hardware_metrics:
            background_jobs.append(HardwareMetricReportingJob(attribute_namespace=monitoring_namespace))
        websockets_factory = backend.websockets_factory(project_obj.uuid, api_run.uuid)
        if websockets_factory:
            background_jobs.append(WebsocketSignalsBackgroundJob(websockets_factory))
        background_jobs.append(TracebackJob(traceback_path, fail_on_exception))
        background_jobs.append(PingBackgroundJob())

    _run = Run(api_run.uuid, backend, operation_processor, BackgroundJobList(background_jobs),
               api_run.workspace, api_run.project_name, api_run.short_id, monitoring_namespace)
    if mode != RunMode.OFFLINE:
        _run.sync(wait=False)

    if mode != RunMode.READ_ONLY:
        if name is not None:
            _run[attr_consts.SYSTEM_NAME_ATTRIBUTE_PATH] = name
        if description is not None:
            _run[attr_consts.SYSTEM_DESCRIPTION_ATTRIBUTE_PATH] = description
        if hostname is not None:
            _run[attr_consts.SYSTEM_HOSTNAME_ATTRIBUTE_PATH] = hostname
        if tags is not None:
            _run[attr_consts.SYSTEM_TAGS_ATTRIBUTE_PATH].add(tags)
        if run is None:
            _run[attr_consts.SYSTEM_FAILED_ATTRIBUTE_PATH] = False

        if capture_stdout and not _run.exists(stdout_path):
            _run.define(stdout_path, StringSeries([]))
        if capture_stderr and not _run.exists(stderr_path):
            _run.define(stderr_path, StringSeries([]))

        if run is None or source_files is not None:
            # upload default sources ONLY if creating a new run
            upload_source_code(source_files=source_files, run=_run)

    _run.start()

    if mode != RunMode.DEBUG:
        click.echo(_run.get_run_url())

        if in_interactive() or in_notebook():
            click.echo(
                "Remember to stop your run once you’ve finished logging your metadata"
                " (https://docs.neptune.ai/api-reference/run#stop)."
                " It will be stopped automatically only when the notebook"
                " kernel/interactive console is terminated.")

    uncaught_exception_handler.activate()

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
