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
from typing import Dict, Any, Union

from neptune.alpha.handler import Handler
from neptune.alpha.internal.backends.neptune_backend import NeptuneBackend
from neptune.alpha.internal.background_job import BackgroundJob
from neptune.alpha.internal.experiment_structure import ExperimentStructure
from neptune.alpha.internal.operation import DeleteAttribute
from neptune.alpha.internal.operation_processors.operation_processor import OperationProcessor
from neptune.alpha.internal.utils.paths import parse_path
from neptune.alpha.internal.attribute_setter_value_visitor import AttributeSetterValueVisitor
from neptune.alpha.types.atoms.string import String
from neptune.alpha.types.atoms.float import Float
from neptune.alpha.types.value import Value
from neptune.alpha.attributes.atoms.float import Float as FloatAttr
from neptune.alpha.attributes.atoms.string import String as StringAttr
from neptune.alpha.attributes.sets.string_set import StringSet as StringSetAttr
from neptune.alpha.attributes.attribute import Attribute
from neptune.alpha.exceptions import MetadataInconsistency


class Experiment(Handler):

    def __init__(
            self,
            _uuid: uuid.UUID,
            backend: NeptuneBackend,
            op_processor: OperationProcessor,
            background_job: BackgroundJob):
        super().__init__(self, path="")
        self._uuid = _uuid
        self._backend = backend
        self._op_processor = op_processor
        self._bg_job = background_job
        self._structure = ExperimentStructure[Attribute]()
        self._lock = threading.RLock()
        self._bg_job.start(self)
        self._prepare_sys_namespace()

    def get_structure(self) -> Dict[str, Any]:
        return self._structure.get_structure()

    def define(self, path: str, value: Union[Value, int, float, str], wait: bool = False) -> Attribute:
        if isinstance(value, (int, float)):
            value = Float(value)
        elif isinstance(value, str):
            value = String(value)
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

    def _prepare_sys_namespace(self):
        self._structure.set(["sys", "id"], StringAttr)
        self._structure.set(["sys", "owner"], StringAttr)
        self._structure.set(["sys", "name"], StringAttr)
        self._structure.set(["sys", "description"], StringAttr)
        self._structure.set(["sys", "hostname"], StringAttr)
        self._structure.set(["sys", "creation_time"], StringAttr)
        self._structure.set(["sys", "modification_time"], StringAttr)
        self._structure.set(["sys", "size"], FloatAttr)
        self._structure.set(["sys", "tags"], StringSetAttr)
        self._structure.set(["sys", "notebook", "id"], StringAttr)
        self._structure.set(["sys", "notebook", "name"], StringAttr)
        self._structure.set(["sys", "notebook", "checkpoint", "id"], StringAttr)
        self._structure.set(["sys", "notebook", "checkpoint", "name"], StringAttr)
