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
import threading
import typing
from platform import node as get_hostname
from typing import List, Optional, Union

from neptune.new.attributes import constants as attr_consts
from neptune.new.envs import (
    CUSTOM_RUN_ID_ENV_NAME,
    MONITORING_NAMESPACE,
    NEPTUNE_NOTEBOOK_ID,
    NEPTUNE_NOTEBOOK_PATH,
)
from neptune.new.exceptions import (
    NeedExistingRunForReadOnlyMode,
    NeptunePossibleLegacyUsageException,
    NeptuneRunResumeAndCustomIdCollision,
)
from neptune.new.internal import id_formats
from neptune.new.internal.backends.factory import get_backend
from neptune.new.internal.backends.neptune_backend import NeptuneBackend
from neptune.new.internal.backends.project_name_lookup import project_name_lookup
from neptune.new.internal.backgroud_job_list import BackgroundJobList
from neptune.new.internal.hardware.hardware_metric_reporting_job import (
    HardwareMetricReportingJob,
)
from neptune.new.internal.id_formats import QualifiedName
from neptune.new.internal.init.parameters import (
    DEFAULT_FLUSH_PERIOD,
    DEFAULT_NAME,
    OFFLINE_PROJECT_QUALIFIED_NAME,
)
from neptune.new.internal.notebooks.notebooks import create_checkpoint
from neptune.new.internal.operation_processors.factory import get_operation_processor
from neptune.new.internal.streams.std_capture_background_job import (
    StderrCaptureBackgroundJob,
    StdoutCaptureBackgroundJob,
)
from neptune.new.internal.utils import verify_collection_type, verify_type
from neptune.new.internal.utils.git import discover_git_repo_location, get_git_info
from neptune.new.internal.utils.limits import custom_run_id_exceeds_length
from neptune.new.internal.utils.ping_background_job import PingBackgroundJob
from neptune.new.internal.utils.source_code import upload_source_code
from neptune.new.internal.utils.traceback_job import TracebackJob
from neptune.new.internal.websockets.websocket_signals_background_job import (
    WebsocketSignalsBackgroundJob,
)
from neptune.new.metadata_containers import Run
from neptune.new.types.mode import Mode
from neptune.new.types.series.string_series import StringSeries

LEGACY_KWARGS = ("project_qualified_name", "backend")


def _check_for_extra_kwargs(caller_name, kwargs: dict):
    for name in LEGACY_KWARGS:
        if name in kwargs:
            raise NeptunePossibleLegacyUsageException()
    if kwargs:
        first_key = next(iter(kwargs.keys()))
        raise TypeError(f"{caller_name}() got an unexpected keyword argument '{first_key}'")


def init_run(
    project: Optional[str] = None,
    api_token: Optional[str] = None,
    run: Optional[str] = None,
    custom_run_id: Optional[str] = None,
    mode: str = Mode.ASYNC.value,
    name: Optional[str] = None,
    description: Optional[str] = None,
    tags: Optional[Union[List[str], str]] = None,
    source_files: Optional[Union[List[str], str]] = None,
    capture_stdout: bool = True,
    capture_stderr: bool = True,
    capture_hardware_metrics: bool = True,
    fail_on_exception: bool = True,
    monitoring_namespace: Optional[str] = None,
    flush_period: float = DEFAULT_FLUSH_PERIOD,
    proxies: Optional[dict] = None,
    capture_traceback: bool = True,
    **kwargs,
) -> Run:
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
        capture_traceback (bool, optional):  Whether to send run’s traceback in case of an exception.
            Defaults to `True`.
            Tracked metadata will be stored inside `monitoring/traceback`.

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
       https://docs.neptune.ai/api-reference/neptune#.init
    """
    _check_for_extra_kwargs(init_run.__name__, kwargs)
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
    verify_type("capture_traceback", capture_hardware_metrics, bool)
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

    # for backward compatibility imports
    mode = Mode(mode)

    name = DEFAULT_NAME if run is None and name is None else name
    description = "" if run is None and description is None else description
    hostname = get_hostname() if run is None else None
    custom_run_id = custom_run_id or os.getenv(CUSTOM_RUN_ID_ENV_NAME)
    monitoring_namespace = monitoring_namespace or os.getenv(MONITORING_NAMESPACE) or "monitoring"

    if run and custom_run_id:
        raise NeptuneRunResumeAndCustomIdCollision()

    backend = get_backend(mode=mode, api_token=api_token, proxies=proxies)

    if mode == Mode.OFFLINE or mode == Mode.DEBUG:
        project = OFFLINE_PROJECT_QUALIFIED_NAME

    project = id_formats.conform_optional(project, QualifiedName)
    project_obj = project_name_lookup(backend, project)
    project = f"{project_obj.workspace}/{project_obj.name}"

    if run:
        api_run = backend.get_metadata_container(
            container_id=QualifiedName(project + "/" + run),
            expected_container_type=Run.container_type,
        )
    else:
        if mode == Mode.READ_ONLY:
            raise NeedExistingRunForReadOnlyMode()
        git_ref = get_git_info(discover_git_repo_location())
        if custom_run_id_exceeds_length(custom_run_id):
            custom_run_id = None

        notebook_id, checkpoint_id = _create_notebook_checkpoint(backend)

        api_run = backend.create_run(
            project_id=project_obj.id,
            git_ref=git_ref,
            custom_run_id=custom_run_id,
            notebook_id=notebook_id,
            checkpoint_id=checkpoint_id,
        )

    run_lock = threading.RLock()

    operation_processor = get_operation_processor(
        mode=mode,
        container_id=api_run.id,
        container_type=Run.container_type,
        backend=backend,
        lock=run_lock,
        flush_period=flush_period,
    )

    stdout_path = "{}/stdout".format(monitoring_namespace)
    stderr_path = "{}/stderr".format(monitoring_namespace)
    traceback_path = "{}/traceback".format(monitoring_namespace)

    background_jobs = []
    if mode != Mode.READ_ONLY:
        if capture_stdout:
            background_jobs.append(StdoutCaptureBackgroundJob(attribute_name=stdout_path))
        if capture_stderr:
            background_jobs.append(StderrCaptureBackgroundJob(attribute_name=stderr_path))
        if capture_hardware_metrics:
            background_jobs.append(
                HardwareMetricReportingJob(attribute_namespace=monitoring_namespace)
            )
        websockets_factory = backend.websockets_factory(project_obj.id, api_run.id)
        if websockets_factory:
            background_jobs.append(WebsocketSignalsBackgroundJob(websockets_factory))
        if capture_traceback:
            background_jobs.append(TracebackJob(traceback_path, fail_on_exception))
        background_jobs.append(PingBackgroundJob())

    _run = Run(
        id_=api_run.id,
        mode=mode,
        backend=backend,
        op_processor=operation_processor,
        background_job=BackgroundJobList(background_jobs),
        lock=run_lock,
        workspace=api_run.workspace,
        project_name=api_run.project_name,
        sys_id=api_run.sys_id,
        project_id=project_obj.id,
        monitoring_namespace=monitoring_namespace,
    )
    if mode != Mode.OFFLINE:
        _run.sync(wait=False)

    if mode != Mode.READ_ONLY:
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

    # pylint: disable=protected-access
    _run._startup(debug_mode=mode == Mode.DEBUG)

    return _run


def _create_notebook_checkpoint(
    backend: NeptuneBackend,
) -> typing.Tuple[typing.Optional[str], typing.Optional[str]]:
    notebook_id = None
    if os.getenv(NEPTUNE_NOTEBOOK_ID, None) is not None:
        notebook_id = os.environ[NEPTUNE_NOTEBOOK_ID]

    notebook_path = None
    if os.getenv(NEPTUNE_NOTEBOOK_PATH, None) is not None:
        notebook_path = os.environ[NEPTUNE_NOTEBOOK_PATH]

    checkpoint_id = None
    if notebook_id is not None and notebook_path is not None:
        checkpoint_id = create_checkpoint(
            backend=backend, notebook_id=notebook_id, notebook_path=notebook_path
        )
    return notebook_id, checkpoint_id
