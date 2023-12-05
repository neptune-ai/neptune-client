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
__all__ = ["Run"]

import os
import threading
from platform import node as get_hostname
from typing import (
    TYPE_CHECKING,
    List,
    Optional,
    Tuple,
    Union,
)

from typing_extensions import Literal

from neptune.attributes.constants import (
    SYSTEM_DESCRIPTION_ATTRIBUTE_PATH,
    SYSTEM_FAILED_ATTRIBUTE_PATH,
    SYSTEM_HOSTNAME_ATTRIBUTE_PATH,
    SYSTEM_NAME_ATTRIBUTE_PATH,
    SYSTEM_TAGS_ATTRIBUTE_PATH,
)
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
    InactiveRunException,
    NeedExistingRunForReadOnlyMode,
    NeptunePossibleLegacyUsageException,
    NeptuneRunResumeAndCustomIdCollision,
)
from neptune.internal.backends.api_model import ApiExperiment
from neptune.internal.backends.neptune_backend import NeptuneBackend
from neptune.internal.container_type import ContainerType
from neptune.internal.hardware.hardware_metric_reporting_job import HardwareMetricReportingJob
from neptune.internal.id_formats import QualifiedName
from neptune.internal.init.parameters import (
    ASYNC_LAG_THRESHOLD,
    ASYNC_NO_PROGRESS_THRESHOLD,
    DEFAULT_FLUSH_PERIOD,
    DEFAULT_NAME,
    OFFLINE_PROJECT_QUALIFIED_NAME,
)
from neptune.internal.notebooks.notebooks import create_checkpoint
from neptune.internal.state import ContainerState
from neptune.internal.streams.std_capture_background_job import (
    StderrCaptureBackgroundJob,
    StdoutCaptureBackgroundJob,
)
from neptune.internal.utils import (
    verify_collection_type,
    verify_type,
)
from neptune.internal.utils.dependency_tracking import (
    FileDependenciesStrategy,
    InferDependenciesStrategy,
)
from neptune.internal.utils.git import (
    to_git_info,
    track_uncommitted_changes,
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
from neptune.metadata_containers import MetadataContainer
from neptune.metadata_containers.abstract import NeptuneObjectCallback
from neptune.types import (
    GitRef,
    StringSeries,
)
from neptune.types.atoms.git_ref import GitRefDisabled
from neptune.types.mode import Mode

if TYPE_CHECKING:
    from neptune.internal.background_job import BackgroundJob


class Run(MetadataContainer):
    """Starts a new tracked run that logs ML model-building metadata to neptune.ai.

    You can log metadata by assigning it to the initialized Run object:

    ```
    run = neptune.init_run()
    run["your/structure"] = some_metadata
    ```

    Examples of metadata you can log: metrics, losses, scores, artifact versions, images, predictions,
    model weights, parameters, checkpoints, and interactive visualizations.

    By default, the run automatically tracks hardware consumption, stdout/stderr, source code, and Git information.
    If you're using Neptune in an interactive session, however, some background monitoring needs to be enabled
    explicitly.

    If you provide the ID of an existing run, that run is resumed and no new run is created. You may resume a run
    either to log more metadata or to fetch metadata from it.

    The run ends either when its `stop()` method is called or when the script finishes execution.

    You can also use the Run object as a context manager (see examples).

    Args:
        project: Name of the project where the run should go, in the form `workspace-name/project_name`.
            If left empty, the value of the NEPTUNE_PROJECT environment variable is used.
        api_token: User's API token.
            If left empty, the value of the NEPTUNE_API_TOKEN environment variable is used (recommended).
        with_id: If you want to resume a run, pass the identifier of an existing run. For example, "SAN-1".
            If left empty, a new run is created.
        custom_run_id: A unique identifier to be used when running Neptune in distributed training jobs.
            Make sure to use the same identifier throughout the whole pipeline execution.
        mode: Connection mode in which the tracking will work.
            If left empty, the value of the NEPTUNE_MODE environment variable is used.
            If no value was set for the environment variable, "async" is used by default.
            Possible values are `async`, `sync`, `offline`, `read-only`, and `debug`.
        name: Custom name for the run. You can add it as a column in the runs table ("sys/name").
            You can also edit the name in the app: Open the run menu and access the run information.
        description:  Custom description of the run. You can add it as a column in the runs table
            ("sys/description").
            You can also edit the description in the app: Open the run menu and access the run information.
        tags: Tags of the run as a list of strings.
            You can edit the tags through the "sys/tags" field or in the app (run menu -> information).
            You can also select multiple runs and manage their tags as a single action.
        source_files: List of source files to be uploaded.
            Uploaded source files are displayed in the "Source code" dashboard.
            To not upload anything, pass an empty list (`[]`).
            Unix style pathname pattern expansion is supported. For example, you can pass `*.py` to upload
            all Python files from the current directory.
            If None is passed, the Python file from which the run was created will be uploaded.
        capture_stdout: Whether to log the stdout of the run.
            Defaults to `False` in interactive sessions and `True` otherwise.
            The data is logged under the monitoring namespace (see the `monitoring_namespace` parameter).
        capture_stderr: Whether to log the stderr of the run.
            Defaults to `False` in interactive sessions and `True` otherwise.
            The data is logged under the monitoring namespace (see the `monitoring_namespace` parameter).
        capture_hardware_metrics: Whether to send hardware monitoring logs (CPU, GPU, and memory utilization).
            Defaults to `False` in interactive sessions and `True` otherwise.
            The data is logged under the monitoring namespace (see the `monitoring_namespace` parameter).
        fail_on_exception: Whether to register an uncaught exception handler to this process and,
            in case of an exception, set the "sys/failed" field of the run to `True`.
            An exception is always logged.
        monitoring_namespace: Namespace inside which all hardware monitoring logs are stored.
            Defaults to "monitoring/<hash>", where the hash is generated based on environment information,
            to ensure that it's unique for each process.
        flush_period: In the asynchronous (default) connection mode, how often disk flushing is triggered
            (in seconds).
        proxies: Argument passed to HTTP calls made via the Requests library, as dictionary of strings.
            For more information about proxies, see the Requests documentation.
        capture_traceback: Whether to log the traceback of the run in case of an exception.
            The tracked metadata is stored in the "<monitoring_namespace>/traceback" namespace (see the
            `monitoring_namespace` parameter).
        git_ref: GitRef object containing information about the Git repository path.
            If None, Neptune looks for a repository in the path of the script that is executed.
            To specify a different location, set to GitRef(repository_path="path/to/repo").
            To turn off Git tracking for the run, set to False or GitRef.DISABLED.
        dependencies: If you pass `"infer"`, Neptune logs dependencies installed in the current environment.
            You can also pass a path to your dependency file directly.
            If left empty, no dependencies are tracked.
        async_lag_callback: Custom callback which is called if the lag between a queued operation and its
            synchronization with the server exceeds the duration defined by `async_lag_threshold`. The callback
            should take a Run object as the argument and can contain any custom code, such as calling `stop()` on
            the object.
            Note: Instead of using this argument, you can use Neptune's default callback by setting the
            `NEPTUNE_ENABLE_DEFAULT_ASYNC_LAG_CALLBACK` environment variable to `TRUE`.
        async_lag_threshold: In seconds, duration between the queueing and synchronization of an operation.
            If a lag callback (default callback enabled via environment variable or custom callback passed to the
            `async_lag_callback` argument) is enabled, the callback is called when this duration is exceeded.
        async_no_progress_callback: Custom callback which is called if there has been no synchronization progress
            whatsoever for the duration defined by `async_no_progress_threshold`. The callback
            should take a Run object as the argument and can contain any custom code, such as calling `stop()` on
            the object.
            Note: Instead of using this argument, you can use Neptune's default callback by setting the
            `NEPTUNE_ENABLE_DEFAULT_ASYNC_NO_PROGRESS_CALLBACK` environment variable to `TRUE`.
        async_no_progress_threshold: In seconds, for how long there has been no synchronization progress since the
            object was initialized. If a no-progress callback (default callback enabled via environment variable or
            custom callback passed to the `async_no_progress_callback` argument) is enabled, the callback is called
            when this duration is exceeded.

    Returns:
        Run object that is used to manage the tracked run and log metadata to it.

    Examples:

        Creating a new run:

        >>> import neptune

        >>> # Minimal invoke
        ... # (creates a run in the project specified by the NEPTUNE_PROJECT environment variable)
        ... run = neptune.init_run()

        >>> # Or initialize with the constructor
        ... run = Run(project="ml-team/classification")

        >>> # Create a run with a name and description, with no sources files or Git info tracked:
        >>> run = neptune.init_run(
        ...     name="neural-net-mnist",
        ...     description="neural net trained on MNIST",
        ...     source_files=[],
        ...     git_ref=False,
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
        ...     dependencies="infer",
        ...     capture_stderr=False,
        ...     git_ref=GitRef(repository_path="/Users/Jackie/repos/cls_project"),
        ... )

        Connecting to an existing run:

        >>> # Resume logging to an existing run with the ID "SAN-3"
        ... run = neptune.init_run(with_id="SAN-3")
        ... run["parameters/lr"] = 0.1  # modify or add metadata

        >>> # Initialize an existing run in read-only mode (logging new data is not possible, only fetching)
        ... run = neptune.init_run(with_id="SAN-4", mode="read-only")
        ... learning_rate = run["parameters/lr"].fetch()

        Using the Run object as context manager:

        >>> with Run() as run:
        ...     run["metric"].append(value)

    For more, see the docs:
        Initializing a run:
            https://docs.neptune.ai/api/neptune#init_run
        Run class reference:
            https://docs.neptune.ai/api/run/
        Essential logging methods:
            https://docs.neptune.ai/logging/methods/
        Resuming a run:
            https://docs.neptune.ai/logging/to_existing_object/
        Setting a custom run ID:
            https://docs.neptune.ai/logging/custom_run_id/
        Logging to multiple runs at once:
            https://docs.neptune.ai/logging/to_multiple_objects/
        Accessing the run from multiple places:
            https://docs.neptune.ai/logging/from_multiple_places/
    """

    container_type = ContainerType.RUN

    LEGACY_METHODS = (
        "create_experiment",
        "send_metric",
        "log_metric",
        "send_text",
        "log_text",
        "send_image",
        "log_image",
        "send_artifact",
        "log_artifact",
        "delete_artifacts",
        "download_artifact",
        "download_sources",
        "download_artifacts",
        "reset_log",
        "get_parameters",
        "get_properties",
        "set_property",
        "remove_property",
        "get_hardware_utilization",
        "get_numeric_channels_values",
    )

    def __init__(
        self,
        with_id: Optional[str] = None,
        *,
        project: Optional[str] = None,
        api_token: Optional[str] = None,
        custom_run_id: Optional[str] = None,
        mode: Optional[Literal["async", "sync", "offline", "read-only", "debug"]] = None,
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
        git_ref: Optional[Union[GitRef, GitRefDisabled, bool]] = None,
        dependencies: Optional[Union[str, os.PathLike]] = None,
        async_lag_callback: Optional[NeptuneObjectCallback] = None,
        async_lag_threshold: float = ASYNC_LAG_THRESHOLD,
        async_no_progress_callback: Optional[NeptuneObjectCallback] = None,
        async_no_progress_threshold: float = ASYNC_NO_PROGRESS_THRESHOLD,
        **kwargs,
    ):
        check_for_extra_kwargs("Run", kwargs)

        verify_type("with_id", with_id, (str, type(None)))
        verify_type("project", project, (str, type(None)))
        verify_type("custom_run_id", custom_run_id, (str, type(None)))
        verify_type("mode", mode, (str, type(None)))
        verify_type("name", name, (str, type(None)))
        verify_type("description", description, (str, type(None)))
        verify_type("capture_stdout", capture_stdout, (bool, type(None)))
        verify_type("capture_stderr", capture_stderr, (bool, type(None)))
        verify_type("capture_hardware_metrics", capture_hardware_metrics, (bool, type(None)))
        verify_type("fail_on_exception", fail_on_exception, bool)
        verify_type("monitoring_namespace", monitoring_namespace, (str, type(None)))
        verify_type("capture_traceback", capture_traceback, bool)
        verify_type("git_ref", git_ref, (GitRef, str, bool, type(None)))
        verify_type("dependencies", dependencies, (str, os.PathLike, type(None)))

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

        self._with_id: Optional[str] = with_id
        self._name: Optional[str] = DEFAULT_NAME if with_id is None and name is None else name
        self._description: Optional[str] = "" if with_id is None and description is None else description
        self._custom_run_id: Optional[str] = custom_run_id or os.getenv(CUSTOM_RUN_ID_ENV_NAME)
        self._hostname: str = get_hostname()
        self._pid: int = os.getpid()
        self._tid: int = threading.get_ident()
        self._tags: Optional[List[str]] = tags
        self._source_files: Optional[List[str]] = source_files
        self._fail_on_exception: bool = fail_on_exception
        self._capture_traceback: bool = capture_traceback

        if type(git_ref) is bool:
            git_ref = GitRef() if git_ref else GitRef.DISABLED

        self._git_ref: Optional[GitRef, GitRefDisabled] = git_ref or GitRef()
        self._dependencies: Optional[str, os.PathLike] = dependencies

        self._monitoring_namespace: str = (
            monitoring_namespace
            or os.getenv(MONITORING_NAMESPACE)
            or generate_monitoring_namespace(self._hostname, self._pid, self._tid)
        )

        # for backward compatibility imports
        mode = Mode(mode or os.getenv(CONNECTION_MODE) or Mode.ASYNC.value)

        self._stdout_path: str = "{}/stdout".format(self._monitoring_namespace)
        self._capture_stdout: bool = capture_stdout
        if capture_stdout is None:
            self._capture_stdout = capture_only_if_non_interactive(mode=mode)

        self._stderr_path: str = "{}/stderr".format(self._monitoring_namespace)
        self._capture_stderr: bool = capture_stderr
        if capture_stderr is None:
            self._capture_stderr = capture_only_if_non_interactive(mode=mode)

        self._capture_hardware_metrics: bool = capture_hardware_metrics
        if capture_hardware_metrics is None:
            self._capture_hardware_metrics = capture_only_if_non_interactive(mode=mode)

        if with_id and custom_run_id:
            raise NeptuneRunResumeAndCustomIdCollision()

        if mode == Mode.OFFLINE or mode == Mode.DEBUG:
            project = OFFLINE_PROJECT_QUALIFIED_NAME

        super().__init__(
            project=project,
            api_token=api_token,
            mode=mode,
            flush_period=flush_period,
            proxies=proxies,
            async_lag_callback=async_lag_callback,
            async_lag_threshold=async_lag_threshold,
            async_no_progress_callback=async_no_progress_callback,
            async_no_progress_threshold=async_no_progress_threshold,
        )

    def _get_or_create_api_object(self) -> ApiExperiment:
        project_workspace = self._project_api_object.workspace
        project_name = self._project_api_object.name
        project_qualified_name = f"{project_workspace}/{project_name}"

        if self._with_id:
            return self._backend.get_metadata_container(
                container_id=QualifiedName(project_qualified_name + "/" + self._with_id),
                expected_container_type=Run.container_type,
            )
        else:
            if self._mode == Mode.READ_ONLY:
                raise NeedExistingRunForReadOnlyMode()

            git_info = to_git_info(git_ref=self._git_ref)

            custom_run_id = self._custom_run_id
            if custom_run_id_exceeds_length(self._custom_run_id):
                custom_run_id = None

            notebook_id, checkpoint_id = create_notebook_checkpoint(backend=self._backend)

            return self._backend.create_run(
                project_id=self._project_api_object.id,
                git_info=git_info,
                custom_run_id=custom_run_id,
                notebook_id=notebook_id,
                checkpoint_id=checkpoint_id,
            )

    def _get_background_jobs(self) -> List["BackgroundJob"]:
        background_jobs = [PingBackgroundJob()]

        websockets_factory = self._backend.websockets_factory(self._project_api_object.id, self._id)
        if websockets_factory:
            background_jobs.append(WebsocketSignalsBackgroundJob(websockets_factory))

        if self._capture_stdout:
            background_jobs.append(StdoutCaptureBackgroundJob(attribute_name=self._stdout_path))

        if self._capture_stderr:
            background_jobs.append(StderrCaptureBackgroundJob(attribute_name=self._stderr_path))

        if self._capture_hardware_metrics:
            background_jobs.append(HardwareMetricReportingJob(attribute_namespace=self._monitoring_namespace))

        if self._capture_traceback:
            background_jobs.append(
                TracebackJob(path=f"{self._monitoring_namespace}/traceback", fail_on_exception=self._fail_on_exception)
            )

        return background_jobs

    def _write_initial_monitoring_attributes(self) -> None:
        if self._hostname is not None:
            self[f"{self._monitoring_namespace}/hostname"] = self._hostname
            if self._with_id is None:
                self[SYSTEM_HOSTNAME_ATTRIBUTE_PATH] = self._hostname

        if self._pid is not None:
            self[f"{self._monitoring_namespace}/pid"] = str(self._pid)

        if self._tid is not None:
            self[f"{self._monitoring_namespace}/tid"] = str(self._tid)

    def _write_initial_attributes(self):
        if self._name is not None:
            self[SYSTEM_NAME_ATTRIBUTE_PATH] = self._name

        if self._description is not None:
            self[SYSTEM_DESCRIPTION_ATTRIBUTE_PATH] = self._description

        if any((self._capture_stderr, self._capture_stdout, self._capture_traceback, self._capture_hardware_metrics)):
            self._write_initial_monitoring_attributes()

        if self._tags is not None:
            self[SYSTEM_TAGS_ATTRIBUTE_PATH].add(self._tags)

        if self._with_id is None:
            self[SYSTEM_FAILED_ATTRIBUTE_PATH] = False

        if self._capture_stdout and not self.exists(self._stdout_path):
            self.define(self._stdout_path, StringSeries([]))

        if self._capture_stderr and not self.exists(self._stderr_path):
            self.define(self._stderr_path, StringSeries([]))

        if self._with_id is None or self._source_files is not None:
            # upload default sources ONLY if creating a new run
            upload_source_code(source_files=self._source_files, run=self)

        if self._dependencies:
            try:
                if self._dependencies == "infer":
                    dependency_strategy = InferDependenciesStrategy()

                else:
                    dependency_strategy = FileDependenciesStrategy(path=self._dependencies)

                dependency_strategy.log_dependencies(run=self)
            except Exception as e:
                warn_once(
                    "An exception occurred in automatic dependency tracking."
                    "Skipping upload of requirement files."
                    "Exception: " + str(e),
                    exception=NeptuneWarning,
                )

        try:
            track_uncommitted_changes(
                git_ref=self._git_ref,
                run=self,
            )
        except Exception as e:
            warn_once(
                "An exception occurred in tracking uncommitted changes."
                "Skipping upload of patch files."
                "Exception: " + str(e),
                exception=NeptuneWarning,
            )

    @property
    def monitoring_namespace(self) -> str:
        return self._monitoring_namespace

    def _raise_if_stopped(self):
        if self._state == ContainerState.STOPPED:
            raise InactiveRunException(label=self._sys_id)

    def get_url(self) -> str:
        """Returns the URL that can be accessed within the browser"""
        return self._backend.get_run_url(
            run_id=self._id,
            workspace=self._workspace,
            project_name=self._project_name,
            sys_id=self._sys_id,
        )


def capture_only_if_non_interactive(mode) -> bool:
    if in_interactive() or in_notebook():
        if mode in {Mode.OFFLINE, Mode.SYNC, Mode.ASYNC}:
            warn_once(
                "The following monitoring options are disabled by default in interactive sessions:"
                " 'capture_stdout', 'capture_stderr', 'capture_traceback', and 'capture_hardware_metrics'."
                " To enable them, set each parameter to 'True' when initializing the run. The monitoring will"
                " continue until you call run.stop() or the kernel stops."
                " Also note: Your source files can only be tracked if you pass the path(s) to the 'source_code'"
                " argument. For help, see the Neptune docs: https://docs.neptune.ai/logging/source_code/",
                exception=NeptuneWarning,
            )
        return False
    return True


def generate_monitoring_namespace(*descriptors) -> str:
    return f"monitoring/{generate_hash(*descriptors, length=8)}"


def check_for_extra_kwargs(caller_name: str, kwargs: dict):
    legacy_kwargs = ("project_qualified_name", "backend")

    for name in legacy_kwargs:
        if name in kwargs:
            raise NeptunePossibleLegacyUsageException()

    if kwargs:
        first_key = next(iter(kwargs.keys()))
        raise TypeError(f"{caller_name}() got an unexpected keyword argument '{first_key}'")


def create_notebook_checkpoint(backend: NeptuneBackend) -> Tuple[Optional[str], Optional[str]]:
    notebook_id = os.getenv(NEPTUNE_NOTEBOOK_ID, None)
    notebook_path = os.getenv(NEPTUNE_NOTEBOOK_PATH, None)

    checkpoint_id = None
    if notebook_id is not None and notebook_path is not None:
        checkpoint_id = create_checkpoint(backend=backend, notebook_id=notebook_id, notebook_path=notebook_path)

    return notebook_id, checkpoint_id
