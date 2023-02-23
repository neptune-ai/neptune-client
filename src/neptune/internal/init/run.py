#
# Copyright (c) 2022, Neptune Labs Sp. z o.o.
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
__all__ = ["init_run"]

import os
import threading
import typing
from platform import node as get_hostname
from typing import (
    List,
    Optional,
    Union,
)

from neptune.attributes import constants as attr_consts
from neptune.common.warnings import (
    NeptuneWarning,
    warn_once,
)
from neptune.envs import (
    CONNECTION_MODE,
    CUSTOM_RUN_ID_ENV_NAME,
    MONITORING_NAMESPACE,
    NEPTUNE_NOTEBOOK_ID,
    NEPTUNE_NOTEBOOK_PATH,
)
from neptune.exceptions import (
    NeedExistingRunForReadOnlyMode,
    NeptunePossibleLegacyUsageException,
    NeptuneRunResumeAndCustomIdCollision,
)
from neptune.internal import id_formats
from neptune.internal.backends.factory import get_backend
from neptune.internal.backends.neptune_backend import NeptuneBackend
from neptune.internal.backends.project_name_lookup import project_name_lookup
from neptune.internal.backgroud_job_list import BackgroundJobList
from neptune.internal.hardware.hardware_metric_reporting_job import HardwareMetricReportingJob
from neptune.internal.id_formats import QualifiedName
from neptune.internal.init.parameters import (
    DEFAULT_FLUSH_PERIOD,
    DEFAULT_NAME,
    OFFLINE_PROJECT_QUALIFIED_NAME,
)
from neptune.internal.notebooks.notebooks import create_checkpoint
from neptune.internal.operation_processors.factory import get_operation_processor
from neptune.internal.streams.std_capture_background_job import (
    StderrCaptureBackgroundJob,
    StdoutCaptureBackgroundJob,
)
from neptune.internal.utils import (
    verify_collection_type,
    verify_type,
)
from neptune.internal.utils.git import (
    discover_git_repo_location,
    get_git_info,
)
from neptune.internal.utils.hashing import generate_hash
from neptune.internal.utils.limits import custom_run_id_exceeds_length
from neptune.internal.utils.ping_background_job import PingBackgroundJob
from neptune.internal.utils.runningmode import (
    in_interactive,
    in_notebook,
)
from neptune.internal.utils.source_code import upload_source_code
from neptune.internal.utils.traceback_job import TracebackJob
from neptune.internal.websockets.websocket_signals_background_job import WebsocketSignalsBackgroundJob
from neptune.metadata_containers import Run
from neptune.types.mode import Mode
from neptune.types.series.string_series import StringSeries

LEGACY_KWARGS = ("project_qualified_name", "backend")


def _check_for_extra_kwargs(caller_name, kwargs: dict):
    for name in LEGACY_KWARGS:
        if name in kwargs:
            raise NeptunePossibleLegacyUsageException()
    if kwargs:
        first_key = next(iter(kwargs.keys()))
        raise TypeError(f"{caller_name}() got an unexpected keyword argument '{first_key}'")


def init_run(
    with_id: Optional[str] = None,
    *,
    project: Optional[str] = None,
    api_token: Optional[str] = None,
    custom_run_id: Optional[str] = None,
    mode: Optional[str] = None,
    name: Optional[str] = None,
    description: Optional[str] = None,
    tags: Optional[Union[List[str], str]] = None,
    source_files: Optional[Union[List[str], str]] = None,
    capture_stdout: Optional[bool] = None,
    capture_stderr: Optional[bool] = None,
    capture_hardware_metrics: Optional[bool] = None,
    fail_on_exception: bool = True,
    monitoring_namespace: Optional[str] = None,
    flush_period: float = DEFAULT_FLUSH_PERIOD,
    proxies: Optional[dict] = None,
    capture_traceback: bool = True,
    **kwargs,
) -> Run:
    """Starts a new tracked run and adds it to the top of the runs table.

    If you provide the ID of an existing run, that run is resumed and no new run is created.

    Args:
        project: Name of the project where the run should go, in the form "workspace-name/project_name".
        If None (default), the value of the NEPTUNE_PROJECT environment variable is used.
        api_token: User's API token. Defaults to None.
            If None (default), the value of the NEPTUNE_API_TOKEN environment variable is used.
            Note: To keep your API token secure, save it to the NEPTUNE_API_TOKEN environment variable rather than
            placing it in plain text in the source code.
        with_id: If you want to resume a run, the identifier of the existing run.
            For example, 'SAN-1'. A run with such an ID must exist.
            If None (default) is passed, starts a new tracked run.
        custom_run_id: A unique identifier to be used when running Neptune in pipelines.
            Make sure to use the same identifier throughout the whole pipeline execution.
        mode: Connection mode in which the tracking will work.
            If None (default), the value of the NEPTUNE_MODE environment variable is used.
            If no value was set for the environment variable, 'async' is used by default.
            Possible values are 'async', 'sync', 'offline', 'read-only', and 'debug'.
        name: Editable name of the run. Defaults to 'Untitled'.
            The name is displayed in the run details and as a column in the runs table.
        description: Editable description of the run. Defaults to `''`.
            The description is displayed in the run details and can be added to the runs table as a column.
        tags: Tags of the run as a list of strings. Defaults to `[]`.
            Tags are displayed in the run details and in the runs table as a column.
            You can edit the tags after the run is created, either through the app or the API.
        source_files: List of source files to be uploaded.
            Uploaded source files are displayed in the 'Source code' tab of the run view.
            To not upload anything, pass an empty list (`[]`).
            Unix style pathname pattern expansion is supported. For example, you can pass `*.py` to upload
            all Python files from the current directory.
            If None is passed, the Python file from which the run was created will be uploaded.
        capture_stdout: Whether to log the stdout of the run. Defaults to True.
            The data is logged under the monitoring namespace (see the 'monitoring_namespace' parameter).
        capture_stderr:  Whether to log the stderr of the run. Defaults to True.
            The data is logged under the monitoring namespace (see the 'monitoring_namespace' parameter).
        capture_hardware_metrics: Whether to send hardware monitoring logs (CPU, GPU, and memory utilization).
            Defaults to True.
            The data is logged under the monitoring namespace (see the 'monitoring_namespace' parameter).
        fail_on_exception: Whether to register an uncaught exception handler to this process and,
            in case of an exception, set the 'sys/failed' field of the run to True.
            An exception is always logged.
        monitoring_namespace: Namespace inside which all hardware monitoring logs are stored.
            Defaults to 'monitoring/<hash>', where the hash is generated based on environment information,
            to ensure that it's unique for each process.
        flush_period: In the asynchronous (default) connection mode, how often disk flushing is triggered.
            Defaults to 5 (every 5 seconds).
        proxies: Argument passed to HTTP calls made via the Requests library, as dictionary of strings.
            For more information, see the 'Proxies' section in the Requests documentation.
        capture_traceback:  Whether to log the traceback of the run in case of an exception.
            Defaults to True.
            The tracked metadata is stored in the '<monitoring_namespace>/traceback' namespace (see the
            'monitoring_namespace' parameter).

    Returns:
        Run object that is used to manage the tracked run and log metadata to it.

    Examples:

        Creating a new run:

        >>> import neptune

        >>> # Minimal invoke
        ... # (creates a run in the project specified by the NEPTUNE_PROJECT environment variable)
        ... run = neptune.init_run()

        >>> # Create a tracked run with a name and description, and no sources files uploaded
        >>> run = neptune.init_run(
        ...     name="neural-net-mnist",
        ...     description="neural net trained on MNIST",
        ...     source_files=[],
        ... )

        >>> # Log all .py files from all subdirectories, excluding hidden files
        ... run = neptune.init_run(source_files="**/*.py")

        >>> # Log all files and directories in the current working directory, excluding hidden files
        ... run = neptune.init_run(source_files="*")

        >>> # Larger example
        ... run = neptune.init_run(
        ...     project="ml-team/classification",
        ...     name="first-pytorch-ever",
        ...     description="Longer description of the run goes here",
        ...     tags=["tags", "go-here", "as-list-of-strings"],
        ...     source_files=["training_with_pytorch.py", "net.py"],
        ...     monitoring_namespace="system_metrics",
        ...     capture_stderr=False,
        ... )

        Connecting to an existing run:

        >>> # Resume logging to an existing run with the ID "SAN-3"
        ... run = neptune.init_run(with_id="SAN-3")
        ... run["parameters/lr"] = 0.1  # modify or add metadata

        >>> # Initialize an existing run in read-only mode (logging new data is not possible, only fetching)
        ... run = neptune.init_run(with_id="SAN-4", mode="read-only")
        ... learning_rate = run["parameters/lr"].fetch()

    For more, see the API reference:
    https://docs.neptune.ai/api/neptune#init_run
    """
    _check_for_extra_kwargs(init_run.__name__, kwargs)

    verify_type("project", project, (str, type(None)))
    verify_type("api_token", api_token, (str, type(None)))
    verify_type("with_id", with_id, (str, type(None)))
    verify_type("custom_run_id", custom_run_id, (str, type(None)))
    verify_type("mode", mode, (str, type(None)))
    verify_type("name", name, (str, type(None)))
    verify_type("description", description, (str, type(None)))
    verify_type("capture_stdout", capture_stdout, (bool, type(None)))
    verify_type("capture_stderr", capture_stderr, (bool, type(None)))
    verify_type("capture_hardware_metrics", capture_hardware_metrics, (bool, type(None)))
    verify_type("monitoring_namespace", monitoring_namespace, (str, type(None)))
    verify_type("flush_period", flush_period, (int, float))
    verify_type("proxies", proxies, (dict, type(None)))
    verify_type("capture_traceback", capture_traceback, bool)
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
    mode = Mode(mode or os.getenv(CONNECTION_MODE) or Mode.ASYNC.value)
    name = DEFAULT_NAME if with_id is None and name is None else name
    description = "" if with_id is None and description is None else description
    custom_run_id = custom_run_id or os.getenv(CUSTOM_RUN_ID_ENV_NAME)

    hostname = get_hostname()
    pid = os.getpid()
    tid = threading.get_ident()

    monitoring_namespace = (
        monitoring_namespace or os.getenv(MONITORING_NAMESPACE) or generate_monitoring_namespace(hostname, pid, tid)
    )

    if capture_stdout is None:
        capture_stdout = capture_only_if_non_interactive(mode=mode)

    if capture_stderr is None:
        capture_stderr = capture_only_if_non_interactive(mode=mode)

    if capture_hardware_metrics is None:
        capture_hardware_metrics = capture_only_if_non_interactive(mode=mode)

    if with_id and custom_run_id:
        raise NeptuneRunResumeAndCustomIdCollision()

    backend = get_backend(mode=mode, api_token=api_token, proxies=proxies)

    if mode == Mode.OFFLINE or mode == Mode.DEBUG:
        project = OFFLINE_PROJECT_QUALIFIED_NAME

    project = id_formats.conform_optional(project, QualifiedName)
    project_obj = project_name_lookup(backend, project)
    project = f"{project_obj.workspace}/{project_obj.name}"

    if with_id:
        api_run = backend.get_metadata_container(
            container_id=QualifiedName(project + "/" + with_id),
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
            background_jobs.append(HardwareMetricReportingJob(attribute_namespace=monitoring_namespace))

        if capture_traceback:
            background_jobs.append(TracebackJob(traceback_path, fail_on_exception))

        websockets_factory = backend.websockets_factory(project_obj.id, api_run.id)
        if websockets_factory:
            background_jobs.append(WebsocketSignalsBackgroundJob(websockets_factory))

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
            _run[f"{monitoring_namespace}/hostname"] = hostname
            if with_id is None:
                _run[attr_consts.SYSTEM_HOSTNAME_ATTRIBUTE_PATH] = hostname

        if pid is not None:
            _run[f"{monitoring_namespace}/pid"] = str(pid)

        if tid is not None:
            _run[f"{monitoring_namespace}/tid"] = str(tid)

        if tags is not None:
            _run[attr_consts.SYSTEM_TAGS_ATTRIBUTE_PATH].add(tags)

        if with_id is None:
            _run[attr_consts.SYSTEM_FAILED_ATTRIBUTE_PATH] = False

        if capture_stdout and not _run.exists(stdout_path):
            _run.define(stdout_path, StringSeries([]))
        if capture_stderr and not _run.exists(stderr_path):
            _run.define(stderr_path, StringSeries([]))

        if with_id is None or source_files is not None:
            # upload default sources ONLY if creating a new run
            upload_source_code(source_files=source_files, run=_run)

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
        checkpoint_id = create_checkpoint(backend=backend, notebook_id=notebook_id, notebook_path=notebook_path)
    return notebook_id, checkpoint_id


def capture_only_if_non_interactive(mode) -> bool:
    if in_interactive() or in_notebook():
        if mode in {Mode.OFFLINE, Mode.SYNC, Mode.ASYNC}:
            warn_once(
                "To avoid unintended consumption of logging hours during interactive sessions, the"
                " following monitoring options are disabled unless set to 'True' when initializing"
                " the run: 'capture_stdout', 'capture_stderr', and 'capture_hardware_metrics'.",
                exception=NeptuneWarning,
            )
        return False
    return True


def generate_monitoring_namespace(*descriptors):
    return f"monitoring/{generate_hash(*descriptors, length=8)}"
