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

import neptune.handler as handler

from neptune.internal.backends.neptune_backend import NeptuneBackend
from neptune.internal.background_job import BackgroundJob
from neptune.internal.experiment_structure import ExperimentStructure
from neptune.internal.operation import DeleteVariable
from neptune.internal.operation_processors.operation_processor import OperationProcessor
from neptune.internal.utils.paths import parse_path
from neptune.internal.variable_setter_value_visitor import VariableSetterValueVisitor
from neptune.types.atoms.string import String
from neptune.types.atoms.float import Float
from neptune.types.value import Value
from neptune.variables.atoms.float import Float as FloatVar
from neptune.variables.atoms.string import String as StringVar
from neptune.variables.sets.string_set import StringSet as StringSetVar
from neptune.variables.variable import Variable
from neptune.exceptions import MetadataInconsistency


class Experiment(handler.Handler):

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
        self._structure = ExperimentStructure[Variable]()
        self._lock = threading.RLock()
        self._bg_job.start(self)
        self._prepare_sys_namespace()

    def get_structure(self) -> Dict[str, Any]:
        return self._structure.get_structure()

    def define(self, path: str, value: Union[Value, int, float, str], wait: bool = False) -> Variable:
        if isinstance(value, (int, float)):
            value = Float(value)
        elif isinstance(value, str):
            value = String(value)
        parsed_path = parse_path(path)

        with self._lock:
            old_var = self._structure.get(parsed_path)
            if old_var:
                raise MetadataInconsistency("Variable {} is already defined".format(path))
            visitor = VariableSetterValueVisitor(self, parsed_path, wait)
            var = visitor.visit(value)
            self._structure.set(parsed_path, var)
            return var

    def get_attribute(self, path: str) -> Variable:
        with self._lock:
            return self._structure.get(parse_path(path))

    def pop(self, path: str, wait: bool = False):
        with self._lock:
            parsed_path = parse_path(path)
            self._structure.pop(parsed_path)
            self._op_processor.enqueue_operation(DeleteVariable(parsed_path), wait)

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
        self._structure.set(["sys", "id"], StringVar)
        self._structure.set(["sys", "owner"], StringVar)
        self._structure.set(["sys", "name"], StringVar)
        self._structure.set(["sys", "description"], StringVar)
        self._structure.set(["sys", "hostname"], StringVar)
        self._structure.set(["sys", "creation_time"], StringVar)
        self._structure.set(["sys", "modification_time"], StringVar)
        self._structure.set(["sys", "size"], FloatVar)
        self._structure.set(["sys", "tags"], StringSetVar)
        self._structure.set(["sys", "notebook", "id"], StringVar)
        self._structure.set(["sys", "notebook", "name"], StringVar)
        self._structure.set(["sys", "notebook", "checkpoint", "id"], StringVar)
        self._structure.set(["sys", "notebook", "checkpoint", "name"], StringVar)
