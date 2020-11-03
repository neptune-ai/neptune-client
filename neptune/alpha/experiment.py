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
import uuid
from datetime import datetime
from typing import Dict, Any, Union, List

from neptune.alpha.attributes.atoms.file import File as FileAttr
from neptune.alpha.attributes.atoms.float import Float as FloatAttr
from neptune.alpha.attributes.atoms.string import String as StringAttr
from neptune.alpha.attributes.atoms.datetime import Datetime as DatetimeAttr
from neptune.alpha.attributes.attribute import Attribute
from neptune.alpha.attributes.series.float_series import FloatSeries as FloatSeriesAttr
from neptune.alpha.attributes.series.image_series import ImageSeries as ImageSeriesAttr
from neptune.alpha.attributes.series.string_series import StringSeries as StringSeriesAttr
from neptune.alpha.attributes.sets.string_set import StringSet as StringSetAttr
from neptune.alpha.exceptions import MetadataInconsistency
from neptune.alpha.handler import Handler
from neptune.alpha.internal.attribute_setter_value_visitor import AttributeSetterValueVisitor
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
            uuid: uuid.UUID,
            backend: NeptuneBackend,
            op_processor: OperationProcessor,
            background_job: BackgroundJob,
            resume: bool
    ):
        super().__init__(self, path="")
        self._uuid = uuid
        self._backend = backend
        self._op_processor = op_processor
        self._bg_job = background_job
        self._structure = ExperimentStructure[Attribute]()
        self._lock = threading.RLock()

        if resume:
            self.sync()
        else:
            self._prepare_sys_namespace()

        self._bg_job.start(self)

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
            visitor = AttributeSetterValueVisitor(self, parsed_path, wait)
            attr = visitor.visit(value)
            self._structure.set(parsed_path, attr)
            return attr

    def get_attribute(self, path: str) -> Attribute:
        with self._lock:
            return self._structure.get(parse_path(path))

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

    def close(self):
        with self._lock:
            self._bg_job.stop()
            self._op_processor.stop()

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

    def _prepare_sys_namespace(self):
        sys_id = ["sys", "id"]
        sys_owner = ["sys", "owner"]
        sys_name = ["sys", "name"]
        sys_description = ["sys", "description"]
        sys_hostname = ["sys", "hostname"]
        sys_creation_time = ["sys", "creation_time"]
        sys_modification_time = ["sys", "modification_time"]
        sys_size = ["sys", "size"]
        sys_tags = ["sys", "tags"]
        sys_notebook_id = ["sys", "notebook", "id"]
        sys_notebook_name = ["sys", "notebook", "name"]
        sys_notebook_checkpoint_id = ["sys", "notebook", "checkpoint", "id"]
        sys_notebook_checkpoint_name = ["sys", "notebook", "checkpoint", "name"]

        self._structure.set(sys_id, StringAttr(self, sys_id))
        self._structure.set(sys_owner, StringAttr(self, sys_owner))
        self._structure.set(sys_name, StringAttr(self, sys_name))
        self._structure.set(sys_description, StringAttr(self, sys_description))
        self._structure.set(sys_hostname, StringAttr(self, sys_hostname))
        self._structure.set(sys_creation_time, DatetimeAttr(self, sys_creation_time))
        self._structure.set(sys_modification_time, DatetimeAttr(self, sys_modification_time))
        self._structure.set(sys_size, FloatAttr(self, sys_size))
        self._structure.set(sys_tags, StringSetAttr(self, sys_tags))
        self._structure.set(sys_notebook_id, StringAttr(self, sys_notebook_id))
        self._structure.set(sys_notebook_name, StringAttr(self, sys_notebook_name))
        self._structure.set(sys_notebook_checkpoint_id, StringAttr(self, sys_notebook_checkpoint_id))
        self._structure.set(sys_notebook_checkpoint_name, StringAttr(self, sys_notebook_checkpoint_name))
