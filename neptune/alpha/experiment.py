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

import threading
import time
import uuid
from datetime import datetime
from typing import Dict, Any, Union, List, Optional

import atexit

from neptune.alpha.attributes.atoms.file import File as FileAttr
from neptune.alpha.attributes.atoms.float import Float as FloatAttr
from neptune.alpha.attributes.atoms.git_ref import GitRef as GitRefAttr
from neptune.alpha.attributes.atoms.string import String as StringAttr
from neptune.alpha.attributes.atoms.datetime import Datetime as DatetimeAttr
from neptune.alpha.attributes.attribute import Attribute
from neptune.alpha.attributes.series.float_series import FloatSeries as FloatSeriesAttr
from neptune.alpha.attributes.series.image_series import ImageSeries as ImageSeriesAttr
from neptune.alpha.attributes.series.string_series import StringSeries as StringSeriesAttr
from neptune.alpha.attributes.sets.string_set import StringSet as StringSetAttr
from neptune.alpha.exceptions import MetadataInconsistency
from neptune.alpha.handler import Handler
from neptune.alpha.internal.value_to_attribute_visitor import ValueToAttributeVisitor
from neptune.alpha.internal.backends.api_model import AttributeType
from neptune.alpha.internal.backends.neptune_backend import NeptuneBackend
from neptune.alpha.internal.background_job import BackgroundJob
from neptune.alpha.internal.experiment_structure import ExperimentStructure
from neptune.alpha.internal.operation import DeleteAttribute
from neptune.alpha.internal.operation_processors.operation_processor import OperationProcessor
from neptune.alpha.internal.utils.paths import parse_path
from neptune.alpha.types.atoms.float import Float
from neptune.alpha.types.atoms.string import String
from neptune.alpha.types.atoms.datetime import Datetime
from neptune.alpha.types.value import Value


class Experiment(Handler):

    def __init__(
            self,
            _uuid: uuid.UUID,
            backend: NeptuneBackend,
            op_processor: OperationProcessor,
            background_job: BackgroundJob
    ):
        super().__init__(self, path="")
        self._uuid = _uuid
        self._backend = backend
        self._op_processor = op_processor
        self._bg_job = background_job
        self._structure = ExperimentStructure[Attribute]()
        self._lock = threading.RLock()
        self._started = False

    def start(self):
        atexit.register(self._shutdown_hook)
        self._op_processor.start()
        self._bg_job.start(self)
        self._started = True

    def stop(self, seconds: Optional[float] = None):
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

    def define(self, path: str, value: Union[Value, int, float, str, datetime], wait: bool = False) -> Attribute:
        if isinstance(value, (int, float)):
            value = Float(value)
        elif isinstance(value, str):
            value = String(value)
        elif isinstance(value, datetime):
            value = Datetime(value)
        parsed_path = parse_path(path)

        with self._lock:
            old_attr = self._structure.get(parsed_path)
            if old_attr:
                raise MetadataInconsistency("Attribute {} is already defined".format(path))
            attr = ValueToAttributeVisitor(self, parsed_path).visit(value)
            self._structure.set(parsed_path, attr)
            attr.assign(value, wait)
            return attr

    def ping(self):
        self._backend.execute_operations(self._uuid, [])

    def get_attribute(self, path: str) -> Optional[Attribute]:
        with self._lock:
            return self._structure.get(parse_path(path))

    def set_attribute(self, path: str, attribute: Attribute) -> Optional[Attribute]:
        with self._lock:
            return self._structure.set(parse_path(path), attribute)

    def pop(self, path: str, wait: bool = False):
        with self._lock:
            parsed_path = parse_path(path)
            self._structure.pop(parsed_path)
            self._op_processor.enqueue_operation(DeleteAttribute(parsed_path), wait)

    def lock(self) -> threading.RLock:
        return self._lock

    def wait(self):
        with self._lock:
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
        if _type == AttributeType.EXPERIMENT_STATE:
            self._structure.set(_path, StringAttr(self, _path))

    def _shutdown_hook(self):
        self.stop()
