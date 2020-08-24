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


def all_subclasses(cls):
    return set(cls.__subclasses__()).union(
        [s for c in cls.__subclasses__() for s in all_subclasses(c)])


class Operation:

    def __init__(self, exp_uuid: uuid.UUID, path: List[str]):
        self.exp_uuid = exp_uuid
        self.path = path

    @abc.abstractmethod
    def accept(self, visitor: 'OperationVisitor[Ret]') -> Ret:
        pass

    def to_dict(self) -> dict:
        return {
            "type": self.__class__.__name__,
            "exp_uuid": str(self.exp_uuid),
            "path": self.path
        }

    @staticmethod
    def from_dict(data: dict) -> 'Operation':
        if "type" not in data:
            raise ValueError("Malformed operation {} - type is missing".format(data))
        sub_classes = {cls.__name__: cls for cls in all_subclasses(Operation)}
        if not data["type"] in sub_classes:
            raise ValueError("Malformed operation {} - unknown type {}".format(data, data["type"]))
        return sub_classes[data["type"]].from_dict(data)


class VersionedOperation:

    def __init__(self, op: Operation, version: int):
        self.op = op
        self.version = version

    @staticmethod
    def to_dict(obj: 'VersionedOperation') -> dict:
        return {
            "op": obj.op.to_dict(),
            "version": obj.version
        }

    @staticmethod
    def from_dict(data: dict) -> 'VersionedOperation':
        return VersionedOperation(Operation.from_dict(data["op"]), data["version"])


class AssignFloat(Operation):

    def __init__(self, exp_uuid: uuid.UUID, path: List[str], value: float):
        super().__init__(exp_uuid, path)
        self.value = value

    def accept(self, visitor: 'OperationVisitor[Ret]') -> Ret:
        return visitor.visit_assign_float(self)

    def to_dict(self) -> dict:
        ret = super().to_dict()
        ret["value"] = self.value
        return ret

    @staticmethod
    def from_dict(data: dict) -> 'AssignFloat':
        return AssignFloat(uuid.UUID(data["exp_uuid"]), data["path"], data["value"])


class AssignString(Operation):

    def __init__(self, exp_uuid: uuid.UUID, path: List[str], value: str):
        super().__init__(exp_uuid, path)
        self.value = value

    def accept(self, visitor: 'OperationVisitor[Ret]') -> Ret:
        return visitor.visit_assign_string(self)

    def to_dict(self) -> dict:
        ret = super().to_dict()
        ret["value"] = self.value
        return ret

    @staticmethod
    def from_dict(data: dict) -> 'AssignString':
        return AssignString(uuid.UUID(data["exp_uuid"]), data["path"], data["value"])


class LogFloats(Operation):

    def __init__(self, exp_uuid: uuid.UUID, path: List[str], values: List[float]):
        super().__init__(exp_uuid, path)
        self.values = values

    def accept(self, visitor: 'OperationVisitor[Ret]') -> Ret:
        return visitor.visit_log_floats(self)

    def to_dict(self) -> dict:
        ret = super().to_dict()
        ret["values"] = self.values
        return ret

    @staticmethod
    def from_dict(data: dict) -> 'LogFloats':
        return LogFloats(uuid.UUID(data["exp_uuid"]), data["path"], data["values"])


class LogStrings(Operation):

    def __init__(self, exp_uuid: uuid.UUID, path: List[str], values: List[str]):
        super().__init__(exp_uuid, path)
        self.values = values

    def accept(self, visitor: 'OperationVisitor[Ret]') -> Ret:
        return visitor.visit_log_strings(self)

    def to_dict(self) -> dict:
        ret = super().to_dict()
        ret["values"] = self.values
        return ret

    @staticmethod
    def from_dict(data: dict) -> 'LogStrings':
        return LogStrings(uuid.UUID(data["exp_uuid"]), data["path"], data["values"])


class ClearFloatLog(Operation):

    def accept(self, visitor: 'OperationVisitor[Ret]') -> Ret:
        return visitor.visit_clear_float_log(self)

    @staticmethod
    def from_dict(data: dict) -> 'ClearFloatLog':
        return ClearFloatLog(uuid.UUID(data["exp_uuid"]), data["path"])


class ClearStringLog(Operation):

    def accept(self, visitor: 'OperationVisitor[Ret]') -> Ret:
        return visitor.visit_clear_string_log(self)

    @staticmethod
    def from_dict(data: dict) -> 'ClearStringLog':
        return ClearStringLog(uuid.UUID(data["exp_uuid"]), data["path"])


class InsertStrings(Operation):

    def __init__(self, exp_uuid: uuid.UUID, path: List[str], values: List[str]):
        super().__init__(exp_uuid, path)
        self.values = values

    def accept(self, visitor: 'OperationVisitor[Ret]') -> Ret:
        return visitor.visit_insert_strings(self)

    def to_dict(self) -> dict:
        ret = super().to_dict()
        ret["values"] = self.values
        return ret

    @staticmethod
    def from_dict(data: dict) -> 'InsertStrings':
        return InsertStrings(uuid.UUID(data["exp_uuid"]), data["path"], data["values"])


class RemoveStrings(Operation):

    def __init__(self, exp_uuid: uuid.UUID, path: List[str], values: List[str]):
        super().__init__(exp_uuid, path)
        self.values = values

    def accept(self, visitor: 'OperationVisitor[Ret]') -> Ret:
        return visitor.visit_remove_strings(self)

    def to_dict(self) -> dict:
        ret = super().to_dict()
        ret["values"] = self.values
        return ret

    @staticmethod
    def from_dict(data: dict) -> 'RemoveStrings':
        return RemoveStrings(uuid.UUID(data["exp_uuid"]), data["path"], data["values"])


class ClearStringSet(Operation):

    def accept(self, visitor: 'OperationVisitor[Ret]') -> Ret:
        return visitor.visit_clear_string_set(self)

    @staticmethod
    def from_dict(data: dict) -> 'ClearStringSet':
        return ClearStringSet(uuid.UUID(data["exp_uuid"]), data["path"])


class DeleteVariable(Operation):

    def accept(self, visitor: 'OperationVisitor[Ret]') -> Ret:
        return visitor.visit_delete_variable(self)

    @staticmethod
    def from_dict(data: dict) -> 'DeleteVariable':
        return DeleteVariable(uuid.UUID(data["exp_uuid"]), data["path"])
