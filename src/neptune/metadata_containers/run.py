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
    Any,
    Dict,
    List,
    Optional,
    Tuple,
    Union,
)

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
from neptune.internal.backgroud_job_list import BackgroundJobList
from neptune.internal.container_type import ContainerType
from neptune.internal.hardware.hardware_metric_reporting_job import HardwareMetricReportingJob
from neptune.internal.id_formats import QualifiedName
from neptune.internal.init.parameters import (
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
from neptune.internal.utils.git import to_git_info
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
from neptune.types import (
    GitRef,
    StringSeries,
)
from neptune.types.atoms.git_ref import GitRefDisabled
from neptune.types.mode import Mode


class Run(MetadataContainer):
    """A Run in Neptune is a representation of all metadata that you log to Neptune.

    Beginning when you start a tracked run with `neptune.init_run()` and ending when the script finishes
    or when you explicitly stop the experiment with `.stop()`.

    You can log many ML metadata types, including:
        * metrics
        * losses
        * model weights
        * images
        * interactive charts
        * predictions
        * and much more

    Examples:
        >>> import neptune

        >>> # Create new experiment
        ... run = neptune.init_run(project='my_workspace/my_project')

        >>> # Log parameters
        ... params = {'max_epochs': 10, 'optimizer': 'Adam'}
        ... run['parameters'] = params

        >>> # Log metadata
        ... run['train/metric_name'].log()
        >>> run['predictions'].log(image)
        >>> run['model'].upload(path_to_model)

        >>> # Log whatever else you want
        ... pass

        >>> # Stop tracking and clean up
        ... run.stop()

    You may also want to check `Run docs page`_.

    .. _Run docs page:
       https://docs.neptune.ai/api/run
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
        git_ref: Optional[Union[GitRef, GitRefDisabled]] = None,
        **kwargs,
    ):
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
            git_ref: GitRef object containing information about the Git repository path.
                If None, Neptune looks for a repository in the path of the script that is executed.
                To specify a different location, set to GitRef(repository_path="path/to/repo").
                To turn off Git tracking for the run, set to GitRef.DISABLED.

        Returns:
            Run object that is used to manage the tracked run and log metadata to it.

        Examples:

            Creating a new run:

            >>> import neptune

            >>> # Minimal invoke
            ... # (creates a run in the project specified by the NEPTUNE_PROJECT environment variable)
            ... run = neptune.init_run()

            >>> # Create a run with a name and description, with no sources files or Git info tracked:
            >>> run = neptune.init_run(
            ...     name="neural-net-mnist",
            ...     description="neural net trained on MNIST",
            ...     source_files=[],
            ...     git_ref=GitRef.DISABLED,
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
            ...     git_ref=GitRef(repository_path="/Users/Jackie/repos/cls_project"),
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
        verify_type("capture_traceback", capture_traceback, bool)
        verify_type("git_ref", git_ref, (GitRef, str, type(None)))
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
        self._git_ref: Optional[GitRef, GitRefDisabled] = git_ref

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

        super().__init__(project=project, api_token=api_token, mode=mode, flush_period=flush_period, proxies=proxies)

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

            git_ref = self._git_ref or GitRef()
            git_info = to_git_info(git_ref=git_ref)

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

    def _prepare_background_jobs(self) -> BackgroundJobList:
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

        return BackgroundJobList(background_jobs)

    def _write_initial_attributes(self):
        if self._name is not None:
            self[SYSTEM_NAME_ATTRIBUTE_PATH] = self._name

        if self._description is not None:
            self[SYSTEM_DESCRIPTION_ATTRIBUTE_PATH] = self._description

        if self._hostname is not None:
            self[f"{self._monitoring_namespace}/hostname"] = self._hostname
            if self._with_id is None:
                self[SYSTEM_HOSTNAME_ATTRIBUTE_PATH] = self._hostname

        if self._pid is not None:
            self[f"{self._monitoring_namespace}/pid"] = str(self._pid)

        if self._tid is not None:
            self[f"{self._monitoring_namespace}/tid"] = str(self._tid)

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

    def assign(self, value, *, wait: bool = False) -> None:
        """Assign values to multiple fields from a dictionary.
        You can use this method to quickly log all run's parameters.
        Args:
            value (dict): A dictionary with values to assign, where keys become the paths of the fields.
                The dictionary can be nested - in such case the path will be a combination of all keys.
            wait (bool, optional): If `True` the client will first wait to send all tracked metadata to the server.
                This makes the call synchronous. Defaults to `False`.
        Examples:
            >>> import neptune
            >>> run = neptune.init_run()
            >>> # Assign multiple fields from a dictionary
            ... params = {"max_epochs": 10, "optimizer": "Adam"}
            >>> run["parameters"] = params
            >>> # You can always log explicitly parameters one by one
            ... run["parameters/max_epochs"] = 10
            >>> run["parameters/optimizer"] = "Adam"
            >>> # Dictionaries can be nested
            ... params = {"train": {"max_epochs": 10}}
            >>> run["parameters"] = params
            >>> # This will log 10 under path "parameters/train/max_epochs"
        You may also want to check `assign docs page`_.
        .. _assign docs page:
            https://docs.neptune.ai/api/run#assign
        """
        return super().assign(value=value, wait=wait)

    def fetch(self) -> dict:
        """Fetch values of all non-File Atom fields as a dictionary.
        The result will preserve the hierarchical structure of the run's metadata, but will contain only non-File Atom
        fields.
        You can use this method to quickly retrieve previous run's parameters.
        Returns:
            `dict` containing all non-File Atom fields values.
        Examples:
            >>> import neptune
            >>> resumed_run = neptune.init_run(with_id="HEL-3")
            >>> params = resumed_run['model/parameters'].fetch()
            >>> run_data = resumed_run.fetch()
            >>> print(run_data)
            >>> # this will print out all Atom attributes stored in run as a dict
        You may also want to check `fetch docs page`_.
        .. _fetch docs page:
            https://docs.neptune.ai/api/run#fetch
        """
        return super().fetch()

    def stop(self, *, seconds: Optional[Union[float, int]] = None) -> None:
        """Stops the tracked run and kills the synchronization thread.
        `.stop()` will be automatically called when a script that created the run finishes or on the destruction
        of Neptune context.
        When using Neptune with Jupyter notebooks it's a good practice to stop the tracked run manually as it
        will be stopped automatically only when the Jupyter kernel stops.
        Args:
            seconds (int or float, optional): Seconds to wait for all tracking calls to finish
                before stopping the tracked run.
                If `None` will wait for all tracking calls to finish. Defaults to `True`.
        Examples:
            If you are creating tracked runs from the script you don't need to call `.stop()`:
            >>> import neptune
            >>> run = neptune.init_run()
            >>> # Your training or monitoring code
            ... pass
            ... # If you are executing Python script .stop()
            ... # is automatically called at the end for every run
            If you are performing multiple training jobs from one script one after the other it is a good practice
            to `.stop()` the finished tracked runs as every open run keeps an open connection with Neptune,
            monitors hardware usage, etc. You can also use Context Managers - Neptune will automatically call `.stop()`
            on the destruction of Run context:
            >>> import neptune
            >>> # If you are running consecutive training jobs from the same script
            ... # stop the tracked runs manually at the end of single training job
            ... for config in configs:
            ...   run = neptune.init_run()
            ...   # Your training or monitoring code
            ...   pass
            ...   run.stop()
            >>> # You can also use with statement and context manager
            ... for config in configs:
            ...   with neptune.init_run() as run:
            ...     # Your training or monitoring code
            ...     pass
            ...     # .stop() is automatically called
            ...     # when code execution exits the with statement
        .. warning::
            If you are using Jupyter notebooks for creating your runs you need to manually invoke `.stop()` once the
            training and evaluation is done.
        You may also want to check `stop docs page`_.
        .. _stop docs page:
            https://docs.neptune.ai/api/run#stop
        """
        return super().stop(seconds=seconds)

    def get_structure(self) -> Dict[str, Any]:
        """Returns a run's metadata structure in form of a dictionary.
        This method can be used to traverse the run's metadata structure programmatically
        when using Neptune in automated workflows.
        .. danger::
            The returned object is a deep copy of an internal run's structure.
        Returns:
            ``dict``: with the run's metadata structure.
        """
        return super().get_structure()

    def print_structure(self) -> None:
        """Pretty prints the structure of the run's metadata.
        Paths are ordered lexicographically and the whole structure is neatly colored.
        """
        return super().print_structure()

    def pop(self, path: str, *, wait: bool = False) -> None:
        """Removes the field stored under the path completely and all data associated with it.
        Args:
            path (str): Path of the field to be removed.
            wait (bool, optional): If `True` the client will first wait to send all tracked metadata to the server.
                This makes the call synchronous. Defaults to `True`.
        Examples:
            >>> import neptune
            >>> run = neptune.init_run()
            >>> run['parameters/learninggg_rata'] = 0.3
            >>> # Delete a field along with it's data
            ... run.pop('parameters/learninggg_rata')
            >>> run['parameters/learning_rate'] = 0.3
            >>> # Training finished
            ... run['trained_model'].upload('model.pt')
            >>> # 'model_checkpoint' is a File field
            ... run.pop('model_checkpoint')
        You may also want to check `pop docs page`_.
        .. _pop docs page:
           https://docs.neptune.ai/api/run#pop
        """
        return super().pop(path=path, wait=wait)

    def wait(self, *, disk_only=False) -> None:
        """Wait for all the tracking calls to finish.
        Args:
            disk_only (bool, optional, default is False): If `True` the process will only wait for data to be saved
                locally from memory, but will not wait for them to reach Neptune servers.
                Defaults to `False`.
        You may also want to check `wait docs page`_.
        .. _wait docs page:
            https://docs.neptune.ai/api/run#wait
        """
        return super().wait(disk_only=disk_only)

    def sync(self, *, wait: bool = True) -> None:
        """Synchronizes local representation of the run with Neptune servers.
        Args:
            wait (bool, optional, default is True): If `True` the process will only wait for data to be saved
                locally from memory, but will not wait for them to reach Neptune servers.
                Defaults to `True`.
        Examples:
            >>> import neptune
            >>> # Connect to a run from Worker #3
            ... worker_id = 3
            >>> run = neptune.init_run(with_id='DIST-43', monitoring_namespace='monitoring/{}'.format(worker_id))
            >>> # Try to access logs that were created in meantime by Worker #2
            ... worker_2_status = run['status/2'].fetch() # Error if this field was created after this script starts
            >>> run.sync() # Synchronizes local representation with Neptune servers.
            >>> worker_2_status = run['status/2'].fetch() # No error
        You may also want to check `sync docs page`_.
        .. _sync docs page:
            https://docs.neptune.ai/api/run#sync
        """
        return super().sync(wait=wait)


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
