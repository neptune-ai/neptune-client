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
__all__ = ["NeptuneObject"]

import abc
import atexit
import datetime
import itertools
import logging
import os
import threading
import time
import traceback
import uuid
from abc import ABC
from functools import partial
from queue import Queue
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Union,
)

from neptune.api.models import FieldType
from neptune.attributes import create_attribute_from_type
from neptune.attributes.attribute import Attribute
from neptune.attributes.namespace import Namespace as NamespaceAttr
from neptune.attributes.namespace import NamespaceBuilder
from neptune.core.operation_processors.factory import get_operation_processor
from neptune.core.operation_processors.lazy_operation_processor_wrapper import LazyOperationProcessorWrapper
from neptune.core.operation_processors.operation_processor import OperationProcessor
from neptune.core.operations.operation import RunCreation
from neptune.core.typing.id_formats import CustomId
from neptune.envs import (
    NEPTUNE_ENABLE_DEFAULT_ASYNC_LAG_CALLBACK,
    NEPTUNE_ENABLE_DEFAULT_ASYNC_NO_PROGRESS_CALLBACK,
)
from neptune.exceptions import (
    MetadataInconsistency,
    NeptuneException,
    NeptuneUnsupportedFunctionalityException,
)
from neptune.handler import Handler
from neptune.internal.backgroud_job_list import BackgroundJobList
from neptune.internal.background_job import BackgroundJob
from neptune.internal.container_structure import ContainerStructure
from neptune.internal.exceptions import UNIX_STYLES
from neptune.internal.operation import DeleteAttribute
from neptune.internal.parameters import (
    ASYNC_LAG_THRESHOLD,
    ASYNC_NO_PROGRESS_THRESHOLD,
    DEFAULT_FLUSH_PERIOD,
)
from neptune.internal.signals_processing.background_job import CallbacksMonitor
from neptune.internal.state import ContainerState
from neptune.internal.utils import (
    verify_optional_callable,
    verify_type,
)
from neptune.internal.utils.limits import (
    CUSTOM_RUN_ID_LENGTH,
    custom_run_id_exceeds_length,
)
from neptune.internal.utils.logger import (
    get_disabled_logger,
    get_logger,
)
from neptune.internal.utils.paths import parse_path
from neptune.internal.utils.uncaught_exception_handler import instance as uncaught_exception_handler
from neptune.internal.utils.utils import reset_internal_ssl_state
from neptune.internal.value_to_attribute_visitor import ValueToAttributeVisitor
from neptune.internal.warnings import warn_about_unsupported_type
from neptune.objects.mode import Mode
from neptune.objects.utils import (
    ensure_not_stopped,
    temporarily_disabled,
)
from neptune.objects.with_backend import WithBackend
from neptune.types.type_casting import cast_value
from neptune.utils import stop_synchronization_callback

if TYPE_CHECKING:
    from neptune.internal.signals_processing.signals import Signal


NeptuneObjectCallback = Callable[["NeptuneObject"], None]


class NeptuneObject(WithBackend, ABC):

    def __init__(
        self,
        *,
        custom_id: Optional[str] = None,
        project: Optional[str] = None,
        api_token: Optional[str] = None,
        mode: Mode = Mode.ASYNC,
        flush_period: float = DEFAULT_FLUSH_PERIOD,
        proxies: Optional[dict] = None,
        async_lag_callback: Optional[NeptuneObjectCallback] = None,
        async_lag_threshold: float = ASYNC_LAG_THRESHOLD,
        async_no_progress_callback: Optional[NeptuneObjectCallback] = None,
        async_no_progress_threshold: float = ASYNC_NO_PROGRESS_THRESHOLD,
    ):
        verify_type("custom_id", custom_id, (str, type(None)))
        verify_type("flush_period", flush_period, (int, float))
        verify_type("async_lag_threshold", async_lag_threshold, (int, float))
        verify_optional_callable("async_lag_callback", async_lag_callback)
        verify_type("async_no_progress_threshold", async_no_progress_threshold, (int, float))
        verify_optional_callable("async_no_progress_callback", async_no_progress_callback)

        if custom_run_id_exceeds_length(custom_id):
            raise NeptuneException(
                f"Custom run ID can't be longer than {CUSTOM_RUN_ID_LENGTH} characters. "
                f"Ensure that the `custom_run_id` argument doesn't exceed the limit."
            )

        super().__init__(api_token=api_token, project=project, mode=mode, proxies=proxies)

        self._flush_period = flush_period
        self._lock: threading.RLock = threading.RLock()
        self._forking_cond: threading.Condition = threading.Condition()
        self._forking_state: bool = False
        self._state = ContainerState.CREATED
        self._signals_queue: "Queue[Signal]" = Queue()
        self._logger: logging.Logger = get_logger()

        self._custom_id: str = custom_id or str(uuid.uuid4())

        self._async_lag_threshold = async_lag_threshold
        self._async_lag_callback = NeptuneObject._get_callback(
            provided=async_lag_callback,
            env_name=NEPTUNE_ENABLE_DEFAULT_ASYNC_LAG_CALLBACK,
        )
        self._async_no_progress_threshold = async_no_progress_threshold
        self._async_no_progress_callback = NeptuneObject._get_callback(
            provided=async_no_progress_callback,
            env_name=NEPTUNE_ENABLE_DEFAULT_ASYNC_NO_PROGRESS_CALLBACK,
        )

        self._op_processor: OperationProcessor = get_operation_processor(
            mode=mode,
            custom_id=CustomId(self._custom_id),
            container_type=self.container_type,
            lock=self._lock,
            flush_period=flush_period,
            queue=self._signals_queue,
        )

        self._async_create_run()

        self._bg_job: BackgroundJobList = self._prepare_background_jobs_if_non_read_only()
        self._structure: ContainerStructure[Attribute, NamespaceAttr] = ContainerStructure(NamespaceBuilder(self))

        if self._mode != Mode.READ_ONLY:
            self._write_initial_attributes()

        self._startup(debug_mode=mode == Mode.DEBUG)

        try:
            os.register_at_fork(
                before=self._before_fork,
                after_in_child=self._handle_fork_in_child,
                after_in_parent=self._handle_fork_in_parent,
            )
        except AttributeError:
            pass

    """
    OpenSSL's internal random number generator does not properly handle forked processes.
    Applications must change the PRNG state of the parent process if they use any SSL feature with os.fork().
    Any successful call of RAND_add(), RAND_bytes() or RAND_pseudo_bytes() is sufficient.
    https://docs.python.org/3/library/ssl.html#multi-processing

    On Linux it looks like it does not help much but does not break anything either.
    """

    @temporarily_disabled
    def _async_create_run(self):
        """placeholder for async run creation"""
        operation = RunCreation(created_at=datetime.datetime.now(), custom_id=self._custom_id)
        self._op_processor.enqueue_operation(operation, wait=False)

    @staticmethod
    def _get_callback(provided: Optional[NeptuneObjectCallback], env_name: str) -> Optional[NeptuneObjectCallback]:
        if provided is not None:
            return provided
        if os.getenv(env_name, "") == "TRUE":
            return stop_synchronization_callback
        return None

    def _handle_fork_in_parent(self):
        reset_internal_ssl_state()
        if self._state == ContainerState.STARTED:
            self._op_processor.resume()
            self._bg_job.resume()

        with self._forking_cond:
            self._forking_state = False
            self._forking_cond.notify_all()

    def _handle_fork_in_child(self):
        reset_internal_ssl_state()
        self._logger = get_disabled_logger()
        if self._state == ContainerState.STARTED:
            self._op_processor.close()
            self._signals_queue = Queue()
            self._op_processor = LazyOperationProcessorWrapper(
                operation_processor_getter=partial(
                    get_operation_processor,
                    mode=self._mode,
                    container_id=self._custom_id,
                    container_type=self.container_type,
                    backend=self._backend,
                    lock=self._lock,
                    flush_period=self._flush_period,
                    queue=self._signals_queue,
                ),
            )
            # TODO: Every implementation of background job should handle fork by itself.
            jobs = []
            if self._mode == Mode.ASYNC:
                jobs.append(
                    CallbacksMonitor(
                        queue=self._signals_queue,
                        async_lag_threshold=self._async_lag_threshold,
                        async_no_progress_threshold=self._async_no_progress_threshold,
                        async_lag_callback=self._async_lag_callback,
                        async_no_progress_callback=self._async_no_progress_callback,
                    )
                )
            self._bg_job = BackgroundJobList(jobs)

        with self._forking_cond:
            self._forking_state = False
            self._forking_cond.notify_all()

    def _before_fork(self):
        with self._forking_cond:
            self._forking_cond.wait_for(lambda: self._state != ContainerState.STOPPING)
            self._forking_state = True

        if self._state == ContainerState.STARTED:
            self._bg_job.pause()
            self._op_processor.pause()

    def _prepare_background_jobs_if_non_read_only(self) -> BackgroundJobList:
        jobs = []

        if self._mode != Mode.READ_ONLY:
            jobs.extend(self._get_background_jobs())

        if self._mode == Mode.ASYNC:
            jobs.append(
                CallbacksMonitor(
                    queue=self._signals_queue,
                    async_lag_threshold=self._async_lag_threshold,
                    async_no_progress_threshold=self._async_no_progress_threshold,
                    async_lag_callback=self._async_lag_callback,
                    async_no_progress_callback=self._async_no_progress_callback,
                )
            )

        return BackgroundJobList(jobs)

    def _get_background_jobs(self) -> List["BackgroundJob"]:
        return []

    def _write_initial_attributes(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_tb is not None:
            traceback.print_exception(exc_type, exc_val, exc_tb)
        self.stop()

    def __getattr__(self, item):
        raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{item}'")

    @abc.abstractmethod
    def _raise_if_stopped(self):
        raise NotImplementedError

    def _get_subpath_suggestions(self, path_prefix: str = None, limit: int = 1000) -> List[str]:
        parsed_path = parse_path(path_prefix or "")
        return list(itertools.islice(self._structure.iterate_subpaths(parsed_path), limit))

    def _ipython_key_completions_(self):
        return self._get_subpath_suggestions()

    @ensure_not_stopped
    def __getitem__(self, path: str) -> "Handler":
        return Handler(self, path)

    @ensure_not_stopped
    def __setitem__(self, key: str, value) -> None:
        self.__getitem__(key).assign(value)

    @ensure_not_stopped
    def __delitem__(self, path) -> None:
        self.pop(path)

    @ensure_not_stopped
    def assign(self, value, *, wait: bool = False) -> None:
        """Assigns values to multiple fields from a dictionary.

        You can use this method to quickly log all parameters at once.

        Args:
            value (dict): A dictionary with values to assign, where keys become paths of the fields.
                The dictionary can be nested, in which case the path will be a combination of all the keys.
            wait: If `True`, Neptune waits to send all tracked metadata to the server before executing the call.

        Examples:
            >>> import neptune
            >>> run = neptune.init_run()
            >>> # Assign a single value with the Python "=" operator
            >>> run["parameters/learning_rate"] = 0.8
            >>> # or the assign() method
            >>> run["parameters/learning_rate"].assign(0.8)
            >>> # Assign a dictionary with the Python "=" operator
            >>> run["parameters"] = {"max_epochs": 10, "optimizer": "Adam", "learning_rate": 0.8}
            >>> # or the assign() method
            >>> run.assign({"parameters": {"max_epochs": 10, "optimizer": "Adam", "learning_rate": 0.8}})

            When operating on a handler object, you can use assign() to circumvent normal Python variable assignment.
            >>> params = run["params"]
            >>> params.assign({"max_epochs": 10, "optimizer": "Adam", "learning_rate": 0.8})

        See also the API reference:
            https://docs.neptune.ai/api/universal/#assign
        """
        self._get_root_handler().assign(value, wait=wait)

    @ensure_not_stopped
    def fetch(self) -> dict:
        """Fetch values of all non-File Atom fields as a dictionary.

        You can use this method to retrieve metadata from a started or resumed run.
        The result preserves the hierarchical structure of the run's metadata, but only contains Atom fields.
        This means fields that contain single values, as opposed to series, or sets.

        Returns:
            `dict` containing the values of all non-File Atom fields.

        Examples:
            Resuming an existing run and fetching metadata from it:
            >>> import neptune
            >>> resumed_run = neptune.init_run(with_id="CLS-3")
            >>> params = resumed_run["model/parameters"].fetch()
            >>> run_data = resumed_run.fetch()
            >>> print(run_data)
            >>> # prints all Atom attributes stored in run as a dict

            Fetching metadata from an existing model version:
            >>> model_version = neptune.init_model_version(with_id="CLS-TREE-45")
            >>> optimizer = model["parameters/optimizer"].fetch()

        See also the API reference:
            https://docs.neptune.ai/api/universal#fetch
        """
        return self._get_root_handler().fetch()

    def ping(self):
        self._backend.ping(self._custom_id, self.container_type)

    def start(self):
        atexit.register(self._shutdown_hook)
        self._op_processor.start()
        self._bg_job.start(self)
        self._state = ContainerState.STARTED

    def stop(self, *, seconds: Optional[Union[float, int]] = None) -> None:
        """Stops the connection and ends the synchronization thread.

        You should stop any initialized runs or other objects when the connection to them is no longer needed.

        This method is automatically called:
        - when the script that created the run or other object finishes execution.
        - if using a context manager, on destruction of the Neptune context.

        Note: In interactive sessions, such as Jupyter Notebook, objects are stopped automatically only when
        the Python kernel stops. However, background monitoring of system metrics and standard streams is disabled
        unless explicitly enabled when initializing Neptune.

        Args:
            seconds: Seconds to wait for all metadata tracking calls to finish before stopping the object.
                If `None`, waits for all tracking calls to finish.

        Example:
            >>> import neptune
            >>> run = neptune.init_run()
            >>> # Your training or monitoring code
            >>> run.stop()

        See also the docs:
            Best practices - Stopping objects
                https://docs.neptune.ai/usage/best_practices/#stopping-runs-and-other-objects
            API reference:
                https://docs.neptune.ai/api/universal/#stop
        """
        verify_type("seconds", seconds, (float, int, type(None)))
        if self._state != ContainerState.STARTED:
            return

        with self._forking_cond:
            self._forking_cond.wait_for(lambda: not self._forking_state)
            self._state = ContainerState.STOPPING

        ts = time.time()
        self._logger.info("Shutting down background jobs, please wait a moment...")
        self._bg_job.stop()
        self._bg_job.join(seconds)
        self._logger.info("Done!")

        sec_left = None if seconds is None else seconds - (time.time() - ts)
        self._op_processor.stop(sec_left)
        self.close()

        with self._forking_cond:
            self._state = ContainerState.STOPPED
            self._forking_cond.notify_all()

    def get_state(self) -> str:
        """Returns the current state of the container as a string.

        Examples:
            >>> from neptune import init_run
            >>> run = init_run()
            >>> run.get_state()
            'started'
            >>> run.stop()
            >>> run.get_state()
            'stopped'
        """
        return self._state.value

    def get_structure(self) -> Dict[str, Any]:
        """Returns the object's metadata structure as a dictionary.

        This method can be used to programmatically traverse the metadata structure of a run, model,
        or project object when using Neptune in automated workflows.

        Note: The returned object is a deep copy of the structure of the internal object.

        See also the API reference:
            https://docs.neptune.ai/api/universal/#get_structure
        """
        return self._structure.get_structure().to_dict()

    def print_structure(self) -> None:
        """Pretty-prints the structure of the object's metadata.

        Paths are ordered lexicographically and the whole structure is neatly colored.

        See also: https://docs.neptune.ai/api/universal/#print_structure
        """
        self._print_structure_impl(self.get_structure(), indent=0)

    def _print_structure_impl(self, struct: dict, indent: int) -> None:
        for key in sorted(struct.keys()):
            print("    " * indent, end="")
            if isinstance(struct[key], dict):
                print("{blue}'{key}'{end}:".format(blue=UNIX_STYLES["blue"], key=key, end=UNIX_STYLES["end"]))
                self._print_structure_impl(struct[key], indent=indent + 1)
            else:
                print(
                    "{blue}'{key}'{end}: {type}".format(
                        blue=UNIX_STYLES["blue"],
                        key=key,
                        end=UNIX_STYLES["end"],
                        type=type(struct[key]).__name__,
                    )
                )

    def define(
        self,
        path: str,
        value: Any,
        *,
        wait: bool = False,
    ) -> Optional[Attribute]:
        with self._lock:
            old_attr = self.get_attribute(path)
            if old_attr is not None:
                raise MetadataInconsistency("Attribute or namespace {} is already defined".format(path))

            neptune_value = cast_value(value)
            if neptune_value is None:
                warn_about_unsupported_type(type_str=str(type(value)))
                return None

            attr = ValueToAttributeVisitor(self, parse_path(path)).visit(neptune_value)
            self.set_attribute(path, attr)
            attr.process_assignment(neptune_value, wait=wait)
            return attr

    def get_attribute(self, path: str) -> Optional[Attribute]:
        with self._lock:
            return self._structure.get(parse_path(path))

    def set_attribute(self, path: str, attribute: Attribute) -> Optional[Attribute]:
        with self._lock:
            return self._structure.set(parse_path(path), attribute)

    def exists(self, path: str) -> bool:
        """Checks if there is a field or namespace under the specified path."""
        verify_type("path", path, str)
        return self.get_attribute(path) is not None

    @ensure_not_stopped
    def pop(self, path: str, *, wait: bool = False) -> None:
        """Removes the field stored under the path and all data associated with it.

        Args:
            path: Path of the field to be removed.
            wait: If `True`, Neptune waits to send all tracked metadata to the server before executing the call.

        Examples:
            >>> import neptune
            >>> run = neptune.init_run()
            >>> run["parameters/learninggg_rata"] = 0.3
            >>> # Let's delete that misspelled field along with its data
            ... run.pop("parameters/learninggg_rata")
            >>> run["parameters/learning_rate"] = 0.3
            >>> # Training finished
            ... run["trained_model"].upload("model.pt")
            >>> # "model_checkpoint" is a File field
            ... run.pop("model_checkpoint")

        See also the API reference:
           https://docs.neptune.ai/api/universal/#pop
        """
        verify_type("path", path, str)
        self._get_root_handler().pop(path, wait=wait)

    def _pop_impl(self, parsed_path: List[str], *, wait: bool):
        self._structure.pop(parsed_path)
        self._op_processor.enqueue_operation(DeleteAttribute(parsed_path), wait=wait)

    def lock(self) -> threading.RLock:
        return self._lock

    def wait(self, *, disk_only=False) -> None:
        """Wait for all the queued metadata tracking calls to reach the Neptune servers.

        Args:
            disk_only: If `True`, the process will only wait for data to be saved
                locally from memory, but will not wait for them to reach Neptune servers.

        See also the API reference:
            https://docs.neptune.ai/api/universal/#wait
        """
        with self._lock:
            if disk_only:
                self._op_processor.flush()
            else:
                self._op_processor.wait()

    def sync(self, *, wait: bool = True) -> None:
        """Synchronizes the local representation of the object with the representation on the Neptune servers.

        Args:
            wait: If `True`, the process will only wait for data to be saved
                locally from memory, but will not wait for them to reach Neptune servers.

        Example:
            >>> import neptune
            >>> # Connect to a run from Worker #3
            ... worker_id = 3
            >>> run = neptune.init_run(with_id="DIST-43", monitoring_namespace=f"monitoring/{worker_id}")
            >>> # Try to access logs that were created in the meantime by Worker #2
            ... worker_2_status = run["status/2"].fetch()
            ... # Error if this field was created after this script starts
            >>> run.sync() # Synchronizes local representation with Neptune servers
            >>> worker_2_status = run["status/2"].fetch()
            ... # No error

        See also the API reference:
            https://docs.neptune.ai/api/universal/#sync
        """
        with self._lock:
            if wait:
                self._op_processor.wait()
            attributes = self._backend.get_attributes(self._custom_id, self.container_type)
            self._structure.clear()
            for attribute in attributes:
                self._define_attribute(parse_path(attribute.path), attribute.type)

    def _define_attribute(self, _path: List[str], _type: FieldType):
        attr = create_attribute_from_type(_type, self, _path)
        self._structure.set(_path, attr)

    def _get_root_handler(self):
        return Handler(self, "")

    def get_url(self) -> str:
        """Returns a link to the object in the Neptune app.

        The same link is printed in the console once the object has been initialized.

        API reference: https://docs.neptune.ai/api/universal/#get_url
        """
        raise NeptuneUnsupportedFunctionalityException

    def _startup(self, debug_mode):
        if not debug_mode:
            self._logger.info(f"Neptune initialized. Object identifier: {self._custom_id}")

        self.start()

        uncaught_exception_handler.activate()

    def _shutdown_hook(self):
        self.stop()

    def get_root_object(self) -> "NeptuneObject":
        """Returns the same Neptune object."""
        return self
