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
import abc
import uuid
from typing import List, TypeVar

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from neptune.internal.operation_visitor import OperationVisitor

Ret = TypeVar('Ret')


class Operation:

    def __init__(self, exp_uuid: uuid.UUID, path: List[str]):
        self.exp_uuid = exp_uuid
        self.path = path

    @abc.abstractmethod
    def accept(self, visitor: 'OperationVisitor[Ret]') -> Ret:
        pass


class AssignFloat(Operation):

    def __init__(self, exp_uuid: uuid.UUID, path: List[str], value: float):
        super().__init__(exp_uuid, path)
        self.value = value

    def accept(self, visitor: 'OperationVisitor[Ret]') -> Ret:
        return visitor.visit_assign_float(self)


class AssignString(Operation):

    def __init__(self, exp_uuid: uuid.UUID, path: List[str], value: str):
        super().__init__(exp_uuid, path)
        self.value = value

    def accept(self, visitor: 'OperationVisitor[Ret]') -> Ret:
        return visitor.visit_assign_string(self)


class LogFloats(Operation):

    def __init__(self, exp_uuid: uuid.UUID, path: List[str], values: List[float]):
        super().__init__(exp_uuid, path)
        self.values = values

    def accept(self, visitor: 'OperationVisitor[Ret]') -> Ret:
        return visitor.visit_log_floats(self)


class LogStrings(Operation):

    def __init__(self, exp_uuid: uuid.UUID, path: List[str], values: List[str]):
        super().__init__(exp_uuid, path)
        self.values = values

    def accept(self, visitor: 'OperationVisitor[Ret]') -> Ret:
        return visitor.visit_log_strings(self)


class ClearFloatLog(Operation):

    def accept(self, visitor: 'OperationVisitor[Ret]') -> Ret:
        return visitor.visit_clear_float_log(self)


class ClearStringLog(Operation):

    def accept(self, visitor: 'OperationVisitor[Ret]') -> Ret:
        return visitor.visit_clear_string_log(self)


class InsertStrings(Operation):

    def __init__(self, exp_uuid: uuid.UUID, path: List[str], values: List[str]):
        super().__init__(exp_uuid, path)
        self.values = values

    def accept(self, visitor: 'OperationVisitor[Ret]') -> Ret:
        return visitor.visit_insert_strings(self)


class RemoveStrings(Operation):

    def __init__(self, exp_uuid: uuid.UUID, path: List[str], values: List[str]):
        super().__init__(exp_uuid, path)
        self.values = values

    def accept(self, visitor: 'OperationVisitor[Ret]') -> Ret:
        return visitor.visit_remove_strings(self)


class ClearStringSet(Operation):

    def accept(self, visitor: 'OperationVisitor[Ret]') -> Ret:
        return visitor.visit_clear_string_set(self)


class DeleteVariable(Operation):

    def accept(self, visitor: 'OperationVisitor[Ret]') -> Ret:
        return visitor.visit_delete_variable(self)
