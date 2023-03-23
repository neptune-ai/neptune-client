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
__all__ = ["MetadataContainer"]

import abc
import atexit
import itertools
import threading
import time
import traceback
from contextlib import AbstractContextManager
from functools import wraps
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    Optional,
    Union,
)

from neptune.attributes import create_attribute_from_type
from neptune.attributes.attribute import Attribute
from neptune.attributes.namespace import Namespace as NamespaceAttr
from neptune.attributes.namespace import NamespaceBuilder
from neptune.common.exceptions import UNIX_STYLES
from neptune.common.warnings import warn_about_unsupported_type
from neptune.exceptions import (
    MetadataInconsistency,
    NeptunePossibleLegacyUsageException,
)
from neptune.handler import Handler
from neptune.internal.backends.api_model import (
    ApiExperiment,
    AttributeType,
    Project,
)
from neptune.internal.backends.factory import get_backend
from neptune.internal.backends.neptune_backend import NeptuneBackend
from neptune.internal.backends.nql import NQLQuery
from neptune.internal.backends.project_name_lookup import project_name_lookup
from neptune.internal.backgroud_job_list import BackgroundJobList
from neptune.internal.container_structure import ContainerStructure
from neptune.internal.container_type import ContainerType
from neptune.internal.id_formats import (
    QualifiedName,
    SysId,
    UniqueId,
    conform_optional,
)
from neptune.internal.init.parameters import DEFAULT_FLUSH_PERIOD
from neptune.internal.operation import DeleteAttribute
from neptune.internal.operation_processors.factory import get_operation_processor
from neptune.internal.operation_processors.operation_processor import OperationProcessor
from neptune.internal.state import ContainerState
from neptune.internal.utils import verify_type
from neptune.internal.utils.logger import logger
from neptune.internal.utils.paths import parse_path
from neptune.internal.utils.uncaught_exception_handler import instance as uncaught_exception_handler
from neptune.internal.value_to_attribute_visitor import ValueToAttributeVisitor
from neptune.metadata_containers.abstract import SupportsNamespaces
from neptune.metadata_containers.metadata_containers_table import Table
from neptune.types.mode import Mode
from neptune.types.type_casting import cast_value


def ensure_not_stopped(fun):
    @wraps(fun)
    def inner_fun(self: "MetadataContainer", *args, **kwargs):
        self._raise_if_stopped()
        return fun(self, *args, **kwargs)

    return inner_fun


class MetadataContainer(AbstractContextManager, SupportsNamespaces):
    container_type: ContainerType

    LEGACY_METHODS = set()

    def __init__(
        self,
        *,
        project: Optional[str] = None,
        api_token: Optional[str] = None,
        mode: Mode = Mode.ASYNC,
        flush_period: float = DEFAULT_FLUSH_PERIOD,
        proxies: Optional[dict] = None,
    ):
        verify_type("project", project, (str, type(None)))
        verify_type("api_token", api_token, (str, type(None)))
        verify_type("mode", mode, Mode)
        verify_type("flush_period", flush_period, (int, float))
        verify_type("proxies", proxies, (dict, type(None)))

        self._mode: Mode = mode
        self._lock: threading.RLock = threading.RLock()
        self._state: ContainerState = ContainerState.CREATED

        self._backend: NeptuneBackend = get_backend(mode=mode, api_token=api_token, proxies=proxies)

        self._project_qualified_name: Optional[str] = conform_optional(project, QualifiedName)
        self._project_api_object: Project = project_name_lookup(
            backend=self._backend, name=self._project_qualified_name
        )
        self._project_id: UniqueId = self._project_api_object.id

        self._api_object: ApiExperiment = self._get_or_create_api_object()
        self._id: UniqueId = self._api_object.id
        self._sys_id: SysId = self._api_object.sys_id
        self._workspace: str = self._api_object.workspace
        self._project_name: str = self._api_object.project_name

        self._op_processor: OperationProcessor = get_operation_processor(
            mode=mode,
            container_id=self._id,
            container_type=self.container_type,
            backend=self._backend,
            lock=self._lock,
            flush_period=flush_period,
        )
        self._bg_job: BackgroundJobList = self._prepare_background_jobs_if_non_read_only()
        self._structure: ContainerStructure[Attribute, NamespaceAttr] = ContainerStructure(NamespaceBuilder(self))

        if self._mode != Mode.OFFLINE:
            self.sync(wait=False)

        if self._mode != Mode.READ_ONLY:
            self._write_initial_attributes()

        self._startup(debug_mode=mode == Mode.DEBUG)

    def _prepare_background_jobs_if_non_read_only(self) -> BackgroundJobList:
        if self._mode != Mode.READ_ONLY:
            return self._prepare_background_jobs()
        return BackgroundJobList([])

    @abc.abstractmethod
    def _get_or_create_api_object(self) -> ApiExperiment:
        raise NotImplementedError

    def _prepare_background_jobs(self) -> BackgroundJobList:
        return BackgroundJobList([])

    def _write_initial_attributes(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_tb is not None:
            traceback.print_exception(exc_type, exc_val, exc_tb)
        self.stop()

    def __getattr__(self, item):
        if item in self.LEGACY_METHODS:
            raise NeptunePossibleLegacyUsageException()
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
        self._get_root_handler().assign(value, wait=wait)

    @ensure_not_stopped
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
        return self._get_root_handler().fetch()

    def ping(self):
        self._backend.ping(self._id, self.container_type)

    def start(self):
        atexit.register(self._shutdown_hook)
        self._op_processor.start()
        self._bg_job.start(self)
        self._state = ContainerState.STARTED

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
        verify_type("seconds", seconds, (float, int, type(None)))
        if self._state != ContainerState.STARTED:
            return

        self._state = ContainerState.STOPPING
        ts = time.time()
        logger.info("Shutting down background jobs, please wait a moment...")
        self._bg_job.stop()
        self._bg_job.join(seconds)
        logger.info("Done!")

        sec_left = None if seconds is None else seconds - (time.time() - ts)
        self._op_processor.stop(sec_left)

        if self._mode not in {Mode.OFFLINE, Mode.DEBUG}:
            logger.info("Explore the metadata in the Neptune app:")
            logger.info(self.get_url().rstrip("/") + "/metadata")
        self._backend.close()
        self._state = ContainerState.STOPPED

    def get_structure(self) -> Dict[str, Any]:
        """Returns a run's metadata structure in form of a dictionary.
        This method can be used to traverse the run's metadata structure programmatically
        when using Neptune in automated workflows.
        .. danger::
            The returned object is a deep copy of an internal run's structure.
        Returns:
            ``dict``: with the run's metadata structure.
        """
        return self._structure.get_structure().to_dict()

    def print_structure(self) -> None:
        """Pretty prints the structure of the run's metadata.
        Paths are ordered lexicographically and the whole structure is neatly colored.
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
        verify_type("path", path, str)
        self._get_root_handler().pop(path, wait=wait)

    def _pop_impl(self, parsed_path: List[str], *, wait: bool):
        self._structure.pop(parsed_path)
        self._op_processor.enqueue_operation(DeleteAttribute(parsed_path), wait=wait)

    def lock(self) -> threading.RLock:
        return self._lock

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
        with self._lock:
            if disk_only:
                self._op_processor.flush()
            else:
                self._op_processor.wait()

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
        with self._lock:
            if wait:
                self._op_processor.wait()
            attributes = self._backend.get_attributes(self._id, self.container_type)
            self._structure.clear()
            for attribute in attributes:
                self._define_attribute(parse_path(attribute.path), attribute.type)

    def _define_attribute(self, _path: List[str], _type: AttributeType):
        attr = create_attribute_from_type(_type, self, _path)
        self._structure.set(_path, attr)

    def _get_root_handler(self):
        return Handler(self, "")

    @abc.abstractmethod
    def get_url(self) -> str:
        """Returns the URL that can be accessed within the browser"""
        ...

    def _startup(self, debug_mode):
        if not debug_mode:
            logger.info(self.get_url())

        self.start()

        uncaught_exception_handler.activate()

    def _shutdown_hook(self):
        self.stop()

    def _fetch_entries(self, child_type: ContainerType, query: NQLQuery, columns: Optional[Iterable[str]]) -> Table:
        if columns is not None:
            # always return entries with `sys/id` column when filter applied
            columns = set(columns)
            columns.add("sys/id")

        leaderboard_entries = self._backend.search_leaderboard_entries(
            project_id=self._project_id,
            types=[child_type],
            query=query,
            columns=columns,
        )

        return Table(
            backend=self._backend,
            container_type=child_type,
            entries=leaderboard_entries,
        )

    def get_root_object(self) -> "MetadataContainer":
        """Returns the same Neptune object."""
        return self
