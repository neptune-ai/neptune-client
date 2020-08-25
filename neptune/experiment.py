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

import uuid
from typing import Dict, Any, TYPE_CHECKING

import neptune.handler as handler

from neptune.exceptions import MetadataInconsistency
from neptune.internal.experiment_structure import ExperimentStructure
from neptune.internal.operation import DeleteVariable
from neptune.internal.utils.paths import parse_path
from neptune.internal.variable_setter_value_visitor import VariableSetterValueVisitor
from neptune.types.value import Value
from neptune.variables.variable import Variable

if TYPE_CHECKING:
    from neptune.internal.backends.neptune_backend import NeptuneBackend
    from neptune.internal.operation_processor import OperationProcessor


class Experiment(handler.Handler):

    def __init__(self, _uuid: uuid.UUID, backend: 'NeptuneBackend', op_processor: 'OperationProcessor'):
        super().__init__(self, path=[])
        self._uuid = _uuid
        self._backend = backend
        self._op_processor = op_processor
        self._structure = ExperimentStructure[Variable]()

    def get_structure(self) -> Dict[str, Any]:
        return self._structure.get_structure()

    def define(self, path: str, value: Value, wait: bool = False) -> Variable:
        parsed_path = parse_path(path)
        old_var = self._structure.get(parsed_path)
        if old_var:
            raise MetadataInconsistency("Variable {} is already defined".format(path))
        visitor = VariableSetterValueVisitor(self, parsed_path, wait)
        var = visitor.visit(value)
        self._structure.set(parsed_path, var)
        return var

    def pop(self, path: str, wait: bool = False):
        parsed_path = parse_path(path)
        self._structure.pop(parsed_path)
        self._op_processor.enqueue_operation(DeleteVariable(self._uuid, parsed_path), wait)

    def wait(self):
        self._op_processor.wait()
