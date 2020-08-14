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
from typing import Optional, List

from neptune import Experiment
from neptune.exceptions import MetadataInconsistency, InternalClientError, ExperimentUUIDNotFound
from neptune.internal.experiment_structure import ExperimentStructure
from neptune.internal.neptune_server import NeptuneServer
from neptune.internal.operation import Operation, RemoveStrings, InsertStrings, LogStrings, LogFloats, \
    AssignString, AssignFloat, DeleteVariable, ClearFloatLog, ClearStringLog, ClearStringSet
from neptune.internal.operation_visitor import OperationVisitor
from neptune.types.atoms.float import Float
from neptune.types.atoms.string import String
from neptune.types.series.float_series import FloatSeries
from neptune.types.series.string_series import StringSeries
from neptune.types.sets.string_set import StringSet
from neptune.types.value import Value


class SyncNeptuneServerMock(NeptuneServer):

    def __init__(self):
        self._experiments = dict()

    def create_experiment(self) -> Experiment:
        new_uuid = uuid.uuid4()
        self._experiments[new_uuid] = ExperimentStructure[Value]()
        return Experiment(new_uuid, self)

    def queue_operation(self, op: Operation) -> None:
        if op.exp_uuid not in self._experiments:
            raise ExperimentUUIDNotFound(op.exp_uuid)
        exp = self._experiments[op.exp_uuid]
        val = exp.get(op.path)
        if val is not None and not isinstance(val, Value):
            if isinstance(val, dict):
                raise MetadataInconsistency("{} is a namespace, not a variable".format(op.path))
            else:
                raise InternalClientError("{} is a {}".format(op.path, type(val)))
        visitor = SyncNeptuneServerMock.NewValueOpVisitor(op.path, val)
        new_val = visitor.visit(op)
        if new_val:
            exp.set(op.path, new_val)
        else:
            exp.pop(op.path)

    def get(self, _uuid: uuid.UUID, path: List[str]) -> Value:
        return self._experiments[_uuid].get(path)

    class NewValueOpVisitor(OperationVisitor[Optional[Value]]):

        def __init__(self, path: List[str], current_value: Optional[Value]):
            self._path = path
            self._current_value = current_value

        def visit_assign_float(self, op: AssignFloat) -> Optional[Value]:
            if self._current_value is not None and not isinstance(self._current_value, Float):
                raise self._create_type_error("assign", Float.__name__)
            return Float(op.value)

        def visit_assign_string(self, op: AssignString) -> Optional[Value]:
            if self._current_value is not None and not isinstance(self._current_value, String):
                raise self._create_type_error("assign", String.__name__)
            return String(op.value)

        def visit_log_floats(self, op: LogFloats) -> Optional[Value]:
            if self._current_value is None:
                return FloatSeries(op.values)
            if not isinstance(self._current_value, FloatSeries):
                raise self._create_type_error("log", FloatSeries.__name__)
            return FloatSeries(self._current_value.values + op.values)

        def visit_log_strings(self, op: LogStrings) -> Optional[Value]:
            if self._current_value is None:
                return StringSeries(op.values)
            if not isinstance(self._current_value, StringSeries):
                raise self._create_type_error("log", StringSeries.__name__)
            return StringSeries(self._current_value.values + op.values)

        def visit_clear_float_log(self, op: ClearFloatLog) -> Optional[Value]:
            if self._current_value is None:
                raise MetadataInconsistency(
                    "Cannot perform clear operation on {}. Variable is undefined.".format(self._path))
            if not isinstance(self._current_value, FloatSeries):
                raise self._create_type_error("clear", FloatSeries.__name__)
            return FloatSeries([])

        def visit_clear_string_log(self, op: ClearStringLog) -> Optional[Value]:
            if self._current_value is None:
                raise MetadataInconsistency(
                    "Cannot perform clear operation on {}. Variable is undefined.".format(self._path))
            if not isinstance(self._current_value, StringSeries):
                raise self._create_type_error("clear", StringSeries.__name__)
            return StringSeries([])

        def visit_insert_strings(self, op: InsertStrings) -> Optional[Value]:
            if self._current_value is None:
                return StringSet(op.values)
            if not isinstance(self._current_value, StringSet):
                raise self._create_type_error("insert", StringSet.__name__)
            return StringSet(self._current_value.values.union(op.values))

        def visit_remove_strings(self, op: RemoveStrings) -> Optional[Value]:
            if self._current_value is None:
                raise MetadataInconsistency(
                    "Cannot perform remove operation on {}. Variable is undefined.".format(self._path))
            if not isinstance(self._current_value, StringSet):
                raise self._create_type_error("remove", StringSet.__name__)
            return StringSet(self._current_value.values.difference(op.values))

        def visit_clear_string_set(self, op: ClearStringSet) -> Optional[Value]:
            if self._current_value is None:
                raise MetadataInconsistency(
                    "Cannot perform clear operation on {}. Variable is undefined.".format(self._path))
            if not isinstance(self._current_value, StringSet):
                raise self._create_type_error("clear", StringSet.__name__)
            return StringSet(set())

        def visit_delete_variable(self, op: DeleteVariable) -> Optional[Value]:
            if self._current_value is None:
                raise MetadataInconsistency(
                    "Cannot perform delete operation on {}. Variable is undefined.".format(self._path))
            return None

        def _create_type_error(self, op_name, expected):
            return MetadataInconsistency("Cannot perform {} operation on {}. Expected {}, {} found."
                                         .format(op_name, self._path, expected, type(self._current_value)))
