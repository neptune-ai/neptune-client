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
import atexit
import threading
import time
import traceback
import uuid
from contextlib import AbstractContextManager
from datetime import datetime
from typing import Dict, Any, Union, List, Optional

import click

from neptune.new.attributes.atoms.datetime import Datetime as DatetimeAttr
from neptune.new.attributes.atoms.run_state import RunState as RunStateAttr
from neptune.new.attributes.atoms.file import File as FileAttr
from neptune.new.attributes.atoms.float import Float as FloatAttr
from neptune.new.attributes.atoms.git_ref import GitRef as GitRefAttr
from neptune.new.attributes.atoms.string import String as StringAttr
from neptune.new.attributes.attribute import Attribute
from neptune.new.attributes.file_set import FileSet as FileSetAttr
from neptune.new.attributes.series.float_series import FloatSeries as FloatSeriesAttr
from neptune.new.attributes.series.file_series import FileSeries as ImageSeriesAttr
from neptune.new.attributes.series.string_series import StringSeries as StringSeriesAttr
from neptune.new.attributes.sets.string_set import StringSet as StringSetAttr
from neptune.new.exceptions import MetadataInconsistency
from neptune.new.handler import Handler
from neptune.new.internal.backends.api_model import AttributeType
from neptune.new.internal.backends.neptune_backend import NeptuneBackend
from neptune.new.internal.background_job import BackgroundJob
from neptune.new.internal.run_structure import RunStructure
from neptune.new.internal.operation import DeleteAttribute
from neptune.new.internal.operation_processors.operation_processor import OperationProcessor
from neptune.new.internal.utils import verify_type, is_float, is_string, is_float_like, is_string_like
from neptune.new.internal.utils.paths import parse_path
from neptune.new.internal.value_to_attribute_visitor import ValueToAttributeVisitor
from neptune.new.types.atoms.datetime import Datetime
from neptune.new.types.atoms.float import Float
from neptune.new.types.atoms.string import String
from neptune.new.types.value import Value
from neptune.exceptions import UNIX_STYLES


class Run(AbstractContextManager):
    last_run = None  # "static" instance of recently created Run

    def __init__(
            self,
            _uuid: uuid.UUID,
            backend: NeptuneBackend,
            op_processor: OperationProcessor,
            background_job: BackgroundJob
    ):
        self._uuid = _uuid
        self._backend = backend
        self._op_processor = op_processor
        self._bg_job = background_job
        self._structure = RunStructure[Attribute]()
        self._lock = threading.RLock()
        self._started = False

        Run.last_run = self

    def __exit__(self, exc_type, exc_val, exc_tb):
        traceback.print_exception(exc_type, exc_val, exc_tb)
        self.stop()

    def __getitem__(self, path: str) -> 'Handler':
        return Handler(self, path)

    def __setitem__(self, key: str, value) -> None:
        self.__getitem__(key).assign(value)

    def __delitem__(self, path) -> None:
        self.pop(path)

    def assign(self, value, wait: bool = False) -> None:
        self[""].assign(value, wait)

    def ping(self):
        self._backend.ping_run(self._uuid)

    def start(self):
        atexit.register(self._shutdown_hook)
        self._op_processor.start()
        self._bg_job.start(self)
        self._started = True

    def stop(self, seconds: Optional[Union[float, int]] = None):
        verify_type("seconds", seconds, (float, int, type(None)))
        if not self._started:
            return
        self._started = False
        ts = time.time()
        self._bg_job.stop()
        self._bg_job.join(seconds)
        with self._lock:
            sec_left = None if seconds is None else seconds - (time.time() - ts)
            self._op_processor.stop(sec_left)

    def get_structure(self) -> Dict[str, Any]:
        return self._structure.get_structure()

    def print_structure(self) -> None:
        self._print_structure_impl(self.get_structure(), indent=0)

    def _print_structure_impl(self, struct: dict, indent: int) -> None:
        for key in sorted(struct.keys()):
            click.echo("    " * indent, nl=False)
            if isinstance(struct[key], dict):
                click.echo("{blue}'{key}'{end}:".format(
                    blue=UNIX_STYLES['blue'],
                    key=key,
                    end=UNIX_STYLES['end']))
                self._print_structure_impl(struct[key], indent=indent+1)
            else:
                click.echo("{blue}'{key}'{end}: {type}".format(
                    blue=UNIX_STYLES['blue'],
                    key=key,
                    end=UNIX_STYLES['end'],
                    type=type(struct[key]).__name__))

    def define(self,
               path: str,
               value: Union[Value, int, float, str, datetime],
               wait: bool = False
               ) -> Attribute:
        if isinstance(value, Value):
            pass
        elif is_float(value):
            value = Float(value)
        elif is_string(value):
            value = String(value)
        elif isinstance(value, datetime):
            value = Datetime(value)
        elif is_float_like(value):
            value = Float(float(value))
        elif is_string_like(value):
            value = String(str(value))
        else:
            raise TypeError("Value of unsupported type {}".format(type(value)))
        parsed_path = parse_path(path)

        with self._lock:
            old_attr = self._structure.get(parsed_path)
            if old_attr:
                raise MetadataInconsistency("Attribute {} is already defined".format(path))
            attr = ValueToAttributeVisitor(self, parsed_path).visit(value)
            attr.assign(value, wait)
            self._structure.set(parsed_path, attr)
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

    def pop(self, path: str, wait: bool = False):
        verify_type("path", path, str)
        with self._lock:
            parsed_path = parse_path(path)
            self._op_processor.enqueue_operation(DeleteAttribute(parsed_path), wait)
            self._structure.pop(parsed_path)

    def lock(self) -> threading.RLock:
        return self._lock

    def wait(self, disk_only=False):
        with self._lock:
            if disk_only:
                self._op_processor.flush()
            else:
                self._op_processor.wait()

    def sync(self, wait: bool = True):
        with self._lock:
            if wait:
                self._op_processor.wait()
            attributes = self._backend.get_attributes(self._uuid)
            self._structure.clear()
            for attribute in attributes:
                self._define_attribute(parse_path(attribute.path), attribute.type)

    def _define_attribute(self, _path: List[str], _type: AttributeType):
        if _type == AttributeType.FLOAT:
            self._structure.set(_path, FloatAttr(self, _path))
        if _type == AttributeType.STRING:
            self._structure.set(_path, StringAttr(self, _path))
        if _type == AttributeType.DATETIME:
            self._structure.set(_path, DatetimeAttr(self, _path))
        if _type == AttributeType.FILE:
            self._structure.set(_path, FileAttr(self, _path))
        if _type == AttributeType.FILE_SET:
            self._structure.set(_path, FileSetAttr(self, _path))
        if _type == AttributeType.FLOAT_SERIES:
            self._structure.set(_path, FloatSeriesAttr(self, _path))
        if _type == AttributeType.STRING_SERIES:
            self._structure.set(_path, StringSeriesAttr(self, _path))
        if _type == AttributeType.IMAGE_SERIES:
            self._structure.set(_path, ImageSeriesAttr(self, _path))
        if _type == AttributeType.STRING_SET:
            self._structure.set(_path, StringSetAttr(self, _path))
        if _type == AttributeType.GIT_REF:
            self._structure.set(_path, GitRefAttr(self, _path))
        if _type == AttributeType.RUN_STATE:
            self._structure.set(_path, RunStateAttr(self, _path))

    def _shutdown_hook(self):
        self.stop()
