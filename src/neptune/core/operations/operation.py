#
# Copyright (c) 2024, Neptune Labs Sp. z o.o.
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
__all__ = [
    "AssignInt",
    "AssignFloat",
    "AssignBool",
    "AssignString",
    "AssignDatetime",
    "LogFloats",
    "Operation",
]

import abc
from dataclasses import dataclass
from datetime import datetime
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    Dict,
    Generic,
    List,
    Optional,
    Type,
    TypeVar,
    Union,
)

from neptune.core.components.operation_storage import OperationStorage
from neptune.exceptions import MalformedOperation

if TYPE_CHECKING:
    from neptune.core.operations.operation_visitor import OperationVisitor

Ret = TypeVar("Ret")
T = TypeVar("T")


@dataclass
class Operation(abc.ABC):
    path: List[str]
    _registry: ClassVar[Dict[str, Type["Operation"]]] = {}

    @abc.abstractmethod
    def accept(self, visitor: "OperationVisitor[Ret]") -> Ret:
        pass

    def clean(self, operation_storage: OperationStorage) -> None:
        pass

    def to_dict(self) -> dict:
        return {"type": self.__class__.__name__, "path": self.path}

    @staticmethod
    def from_dict(data: dict) -> "Operation":
        if "type" not in data:
            raise MalformedOperation("Malformed operation {} - type is missing".format(data))
        operation_type = data["type"]
        if operation_type not in Operation._registry:
            raise MalformedOperation("Malformed operation {} - unknown type {}".format(data, operation_type))
        return Operation._registry[operation_type].from_dict(data)


@dataclass
class AssignFloat(Operation):
    value: float

    def accept(self, visitor: "OperationVisitor[Ret]") -> Ret:
        return visitor.visit_assign_float(self)

    def to_dict(self) -> dict:
        ret = super().to_dict()
        ret["value"] = self.value
        return ret

    @staticmethod
    def from_dict(data: dict) -> "AssignFloat":
        return AssignFloat(data["path"], data["value"])


@dataclass
class AssignInt(Operation):

    value: int

    def accept(self, visitor: "OperationVisitor[Ret]") -> Ret:
        return visitor.visit_assign_int(self)

    def to_dict(self) -> dict:
        ret = super().to_dict()
        ret["value"] = self.value
        return ret

    @staticmethod
    def from_dict(data: dict) -> "AssignInt":
        return AssignInt(data["path"], data["value"])


@dataclass
class AssignBool(Operation):

    value: bool

    def accept(self, visitor: "OperationVisitor[Ret]") -> Ret:
        return visitor.visit_assign_bool(self)

    def to_dict(self) -> dict:
        ret = super().to_dict()
        ret["value"] = self.value
        return ret

    @staticmethod
    def from_dict(data: dict) -> "AssignBool":
        return AssignBool(data["path"], data["value"])


@dataclass
class AssignString(Operation):

    value: str

    def accept(self, visitor: "OperationVisitor[Ret]") -> Ret:
        return visitor.visit_assign_string(self)

    def to_dict(self) -> dict:
        ret = super().to_dict()
        ret["value"] = self.value
        return ret

    @staticmethod
    def from_dict(data: dict) -> "AssignString":
        return AssignString(data["path"], data["value"])


@dataclass
class AssignDatetime(Operation):

    value: datetime

    def accept(self, visitor: "OperationVisitor[Ret]") -> Ret:
        return visitor.visit_assign_datetime(self)

    def to_dict(self) -> dict:
        ret = super().to_dict()
        ret["value"] = int(1000 * self.value.timestamp())
        return ret

    @staticmethod
    def from_dict(data: dict) -> "AssignDatetime":
        return AssignDatetime(data["path"], datetime.fromtimestamp(data["value"] / 1000))


class LogOperation(Operation, abc.ABC):
    pass


@dataclass
class LogSeriesValue(Generic[T]):

    value: T
    step: Optional[float]
    ts: float

    def to_dict(
        self,
        value_serializer: Callable[[T], Any] = lambda x: x,
    ) -> Dict[str, Union[T, Optional[float], float]]:
        return {"value": value_serializer(self.value), "step": self.step, "ts": self.ts}

    @staticmethod
    def from_dict(data: Dict[str, Any], value_deserializer: Callable[[T], Any] = lambda x: x) -> "LogSeriesValue[T]":
        return LogSeriesValue[T](value_deserializer(data["value"]), data.get("step", None), data["ts"])


@dataclass
class LogFloats(LogOperation):
    ValueType = LogSeriesValue[float]

    values: List[ValueType]

    def accept(self, visitor: "OperationVisitor[Ret]") -> Ret:
        return visitor.visit_log_floats(self)

    def to_dict(self) -> Dict[str, Any]:
        ret = super().to_dict()
        ret["values"] = [value.to_dict() for value in self.values]
        return ret

    @staticmethod
    def from_dict(data: dict) -> "LogFloats":
        return LogFloats(
            data["path"],
            [LogFloats.ValueType.from_dict(value) for value in data["values"]],  # type: ignore[misc]
        )
