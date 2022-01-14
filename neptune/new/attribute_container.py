#
# Copyright (c) 2021, Neptune Labs Sp. z o.o.
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
import abc
import atexit
import itertools
import threading
import time
import traceback
from contextlib import AbstractContextManager
from datetime import datetime
from functools import wraps
from typing import Any, Dict, List, Optional, Union

import click

from neptune.exceptions import UNIX_STYLES
from neptune.new.attributes import create_attribute_from_type
from neptune.new.attributes.attribute import Attribute
from neptune.new.attributes.namespace import (
    NamespaceBuilder,
    Namespace as NamespaceAttr,
)
from neptune.new.exceptions import (
    MetadataInconsistency,
    InactiveProjectException,
    InactiveRunException,
    NeptunePossibleLegacyUsageException,
)
from neptune.new.handler import Handler
from neptune.new.internal.backends.api_model import AttributeType
from neptune.new.internal.backends.neptune_backend import NeptuneBackend
from neptune.new.internal.background_job import BackgroundJob
from neptune.new.internal.container_type import ContainerType
from neptune.new.internal.operation import DeleteAttribute
from neptune.new.internal.operation_processors.operation_processor import (
    OperationProcessor,
)
from neptune.new.internal.run_structure import ContainerStructure
from neptune.new.internal.state import ContainerState
from neptune.new.internal.utils import (
    is_bool,
    is_float,
    is_float_like,
    is_int,
    is_string,
    is_string_like,
    verify_type,
    is_dict_like,
)
from neptune.new.internal.utils.paths import parse_path
from neptune.new.internal.utils.runningmode import in_interactive, in_notebook
from neptune.new.internal.utils.uncaught_exception_handler import (
    instance as uncaught_exception_handler,
)
from neptune.new.internal.value_to_attribute_visitor import ValueToAttributeVisitor
from neptune.new.types import Boolean, Integer
from neptune.new.types.atoms.datetime import Datetime
from neptune.new.types.atoms.float import Float
from neptune.new.types.atoms.string import String
from neptune.new.types.namespace import Namespace
from neptune.new.types.value import Value
from neptune.new.types.value_copy import ValueCopy


def ensure_not_stopped(fun):
    @wraps(fun)
    def inner_fun(self: "AttributeContainer", *args, **kwargs):
        # pylint: disable=protected-access
        if self._state == ContainerState.STOPPED:
            if self.container_type == ContainerType.RUN:
                raise InactiveRunException(label=self._label)
            elif self.container_type == ContainerType.PROJECT:
                raise InactiveProjectException(label=self._label)
        return fun(self, *args, **kwargs)

    return inner_fun


class AttributeContainer(AbstractContextManager):
    container_type: ContainerType

    LEGACY_METHODS = set()

    def __init__(
        self,
        _id: str,
        backend: NeptuneBackend,
        op_processor: OperationProcessor,
        background_job: BackgroundJob,
        lock: threading.RLock,
        project_id: str,
        project_name: str,
        workspace: str,
    ):
        self._id = _id
        self._project_id = project_id
        self._project_name = project_name
        self._workspace = workspace
        self._backend = backend
        self._op_processor = op_processor
        self._bg_job = background_job
        self._structure: ContainerStructure[
            Attribute, NamespaceAttr
        ] = ContainerStructure(NamespaceBuilder(self))
        self._lock = lock
        self._state = ContainerState.CREATED

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_tb is not None:
            traceback.print_exception(exc_type, exc_val, exc_tb)
        self.stop()

    def __getattr__(self, item):
        if item in self.LEGACY_METHODS:
            raise NeptunePossibleLegacyUsageException()
        raise AttributeError(
            f"'{self.__class__.__name__}' object has no attribute '{item}'"
        )

    @property
    @abc.abstractmethod
    def _label(self) -> str:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def _docs_url_stop(self) -> str:
        raise NotImplementedError

    def _get_subpath_suggestions(
        self, path_prefix: str = None, limit: int = 1000
    ) -> List[str]:
        parsed_path = parse_path(path_prefix or "")
        return list(
            itertools.islice(self._structure.iterate_subpaths(parsed_path), limit)
        )

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
    def assign(self, value, wait: bool = False) -> None:
        self._get_root_handler().assign(value, wait)

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

    def stop(self, seconds: Optional[Union[float, int]] = None) -> None:
        verify_type("seconds", seconds, (float, int, type(None)))
        if self._state != ContainerState.STARTED:
            return

        self._state = ContainerState.STOPPING
        ts = time.time()
        click.echo("Shutting down background jobs, please wait a moment...")
        self._bg_job.stop()
        self._bg_job.join(seconds)
        click.echo("Done!")
        with self._lock:
            sec_left = None if seconds is None else seconds - (time.time() - ts)
            self._op_processor.stop(sec_left)
        self._backend.close()
        self._state = ContainerState.STOPPED

    def get_structure(self) -> Dict[str, Any]:
        # This is very weird pylint false-positive.
        # pylint: disable=no-member
        return self._structure.get_structure().to_dict()

    def print_structure(self) -> None:
        self._print_structure_impl(self.get_structure(), indent=0)

    def _print_structure_impl(self, struct: dict, indent: int) -> None:
        for key in sorted(struct.keys()):
            click.echo("    " * indent, nl=False)
            if isinstance(struct[key], dict):
                click.echo(
                    "{blue}'{key}'{end}:".format(
                        blue=UNIX_STYLES["blue"], key=key, end=UNIX_STYLES["end"]
                    )
                )
                self._print_structure_impl(struct[key], indent=indent + 1)
            else:
                click.echo(
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
        value: Union[Value, int, float, str, datetime],
        wait: bool = False,
    ) -> Attribute:
        if isinstance(value, Value):
            pass
        elif isinstance(value, Handler):
            value = ValueCopy(value)
        elif is_bool(value):
            value = Boolean(value)
        elif is_int(value):
            value = Integer(value)
        elif is_float(value):
            value = Float(value)
        elif is_string(value):
            value = String(value)
        elif isinstance(value, datetime):
            value = Datetime(value)
        elif is_float_like(value):
            value = Float(float(value))
        elif is_dict_like(value):
            value = Namespace(value)
        elif is_string_like(value):
            value = String(str(value))
        else:
            raise TypeError("Value of unsupported type {}".format(type(value)))
        parsed_path = parse_path(path)

        with self._lock:
            old_attr = self._structure.get(parsed_path)
            if old_attr:
                raise MetadataInconsistency(
                    "Attribute or namespace {} is already defined".format(path)
                )
            attr = ValueToAttributeVisitor(self, parsed_path).visit(value)
            self._structure.set(parsed_path, attr)
            attr.process_assignment(value, wait)
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

    def pop(self, path: str, wait: bool = False) -> None:
        verify_type("path", path, str)
        with self._lock:
            self._pop_impl(parse_path(path), wait)

    def _pop_impl(self, parsed_path: List[str], wait: bool):
        attribute = self._structure.get(parsed_path)
        if isinstance(attribute, NamespaceAttr):
            self._pop_namespace(attribute, wait)
        else:
            self._structure.pop(parsed_path)
            self._op_processor.enqueue_operation(DeleteAttribute(parsed_path), wait)

    def _pop_namespace(self, namespace: NamespaceAttr, wait: bool):
        children = list(namespace)
        for key in children:
            sub_attr_path = namespace._path + [key]  # pylint: disable=protected-access
            self._pop_impl(sub_attr_path, wait)

    def lock(self) -> threading.RLock:
        return self._lock

    def wait(self, disk_only=False) -> None:
        with self._lock:
            if disk_only:
                self._op_processor.flush()
            else:
                self._op_processor.wait()

    def sync(self, wait: bool = True) -> None:
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

    def _startup(self, debug_mode):
        self.start()

        if not debug_mode:
            if in_interactive() or in_notebook():
                click.echo(
                    f"Remember to stop your {self.container_type.value} once you’ve finished logging your metadata"
                    f" ({self._docs_url_stop})."
                    " It will be stopped automatically only when the notebook"
                    " kernel/interactive console is terminated."
                )

        uncaught_exception_handler.activate()

    def _shutdown_hook(self):
        self.stop()
