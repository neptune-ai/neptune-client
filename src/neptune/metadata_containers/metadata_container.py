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
from neptune.internal.backends.api_model import AttributeType
from neptune.internal.backends.neptune_backend import NeptuneBackend
from neptune.internal.backends.nql import NQLQuery
from neptune.internal.background_job import BackgroundJob
from neptune.internal.container_structure import ContainerStructure
from neptune.internal.container_type import ContainerType
from neptune.internal.id_formats import (
    SysId,
    UniqueId,
)
from neptune.internal.operation import DeleteAttribute
from neptune.internal.operation_processors.operation_processor import OperationProcessor
from neptune.internal.state import ContainerState
from neptune.internal.utils import verify_type
from neptune.internal.utils.logger import logger
from neptune.internal.utils.paths import parse_path
from neptune.internal.utils.uncaught_exception_handler import instance as uncaught_exception_handler
from neptune.internal.value_to_attribute_visitor import ValueToAttributeVisitor
from neptune.metadata_containers.metadata_containers_table import Table
from neptune.types.mode import Mode
from neptune.types.type_casting import cast_value


def ensure_not_stopped(fun):
    @wraps(fun)
    def inner_fun(self: "MetadataContainer", *args, **kwargs):
        self._raise_if_stopped()
        return fun(self, *args, **kwargs)

    return inner_fun


class MetadataContainer(AbstractContextManager):
    container_type: ContainerType

    LEGACY_METHODS = set()

    def __init__(
        self,
        *,
        id_: UniqueId,
        mode: Mode,
        backend: NeptuneBackend,
        op_processor: OperationProcessor,
        background_job: BackgroundJob,
        lock: threading.RLock,
        project_id: UniqueId,
        project_name: str,
        workspace: str,
        sys_id: SysId,
    ):
        self._id = id_
        self._mode = mode
        self._project_id = project_id
        self._project_name = project_name
        self._workspace = workspace
        self._backend = backend
        self._op_processor = op_processor
        self._bg_job = background_job
        self._structure: ContainerStructure[Attribute, NamespaceAttr] = ContainerStructure(NamespaceBuilder(self))
        self._lock = lock
        self._state = ContainerState.CREATED
        self._sys_id = sys_id

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
        self._get_root_handler().assign(value, wait=wait)

    @ensure_not_stopped
    def fetch(self) -> dict:
        return self._get_root_handler().fetch()

    def ping(self):
        self._backend.ping(self._id, self.container_type)

    def start(self):
        atexit.register(self._shutdown_hook)
        self._op_processor.start()
        self._bg_job.start(self)
        self._state = ContainerState.STARTED

    def stop(self, *, seconds: Optional[Union[float, int]] = None) -> None:
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
        return self._structure.get_structure().to_dict()

    def print_structure(self) -> None:
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
        verify_type("path", path, str)
        return self.get_attribute(path) is not None

    @ensure_not_stopped
    def pop(self, path: str, *, wait: bool = False) -> None:
        verify_type("path", path, str)
        self._get_root_handler().pop(path, wait=wait)

    def _pop_impl(self, parsed_path: List[str], *, wait: bool):
        self._structure.pop(parsed_path)
        self._op_processor.enqueue_operation(DeleteAttribute(parsed_path), wait=wait)

    def lock(self) -> threading.RLock:
        return self._lock

    def wait(self, *, disk_only=False) -> None:
        with self._lock:
            if disk_only:
                self._op_processor.flush()
            else:
                self._op_processor.wait()

    def sync(self, *, wait: bool = True) -> None:
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
        raise NotImplementedError

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
