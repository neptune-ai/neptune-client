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
    "FieldOperation",
    "RunCreation",
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
    _registry: ClassVar[Dict[str, Type["Operation"]]] = {}

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        cls._registry[cls.__name__] = cls

    @abc.abstractmethod
    def accept(self, visitor: "OperationVisitor[Ret]") -> Ret:
        pass

    def clean(self, operation_storage: OperationStorage) -> None:
        pass

    def to_dict(self) -> Dict[str, Any]:
        return {"type": self.__class__.__name__}

    @classmethod
    def from_dict(cls, data: dict) -> "Operation":
        if "type" not in data:
            raise MalformedOperation("Malformed operation {} - type is missing".format(data))
        operation_type = data["type"]
        if operation_type not in Operation._registry:
            raise MalformedOperation("Malformed operation {} - unknown type {}".format(data, operation_type))
        return Operation._registry[operation_type].from_dict(data)


@dataclass
class RunCreation(Operation):
    created_at: datetime
    custom_id: str

    def accept(self, visitor: "OperationVisitor[Ret]") -> Ret:
        return visitor.visit_run_creation(self)

    @classmethod
    def from_dict(cls, data: dict) -> "Operation":
        return cls(data["created_at"], data["custom_id"])

    def to_dict(self) -> Dict[str, Any]:
        return {**super().to_dict(), "created_at": self.created_at, "custom_id": self.custom_id}


@dataclass
class FieldOperation(Operation, abc.ABC):
    path: List[str]

    def to_dict(self) -> Dict[str, Any]:
        return {**super().to_dict(), "path": self.path}


@dataclass
class AssignFloat(FieldOperation):
    value: float

    def accept(self, visitor: "OperationVisitor[Ret]") -> Ret:
        return visitor.visit_assign_float(self)

    def to_dict(self) -> dict:
        return {**super().to_dict(), "value": self.value}

    @classmethod
    def from_dict(cls, data: dict) -> "AssignFloat":
        return cls(data["path"], data["value"])


@dataclass
class AssignInt(FieldOperation):
    value: int

    def accept(self, visitor: "OperationVisitor[Ret]") -> Ret:
        return visitor.visit_assign_int(self)

    def to_dict(self) -> dict:
        return {**super().to_dict(), "value": self.value}

    @classmethod
    def from_dict(cls, data: dict) -> "AssignInt":
        return cls(data["path"], data["value"])


@dataclass
class AssignBool(FieldOperation):
    value: bool

    def accept(self, visitor: "OperationVisitor[Ret]") -> Ret:
        return visitor.visit_assign_bool(self)

    def to_dict(self) -> dict:
        return {**super().to_dict(), "value": self.value}

    @classmethod
    def from_dict(cls, data: dict) -> "AssignBool":
        return cls(data["path"], data["value"])


@dataclass
class AssignString(FieldOperation):
    value: str

    def accept(self, visitor: "OperationVisitor[Ret]") -> Ret:
        return visitor.visit_assign_string(self)

    def to_dict(self) -> dict:
        return {**super().to_dict(), "value": self.value}

    @classmethod
    def from_dict(cls, data: dict) -> "AssignString":
        return AssignString(data["path"], data["value"])


@dataclass
class AssignDatetime(FieldOperation):
    value: datetime

    def accept(self, visitor: "OperationVisitor[Ret]") -> Ret:
        return visitor.visit_assign_datetime(self)

    def to_dict(self) -> dict:
        return {**super().to_dict(), "value": int(1000 * self.value.timestamp())}

    @classmethod
    def from_dict(cls, data: dict) -> "AssignDatetime":
        return AssignDatetime(data["path"], datetime.fromtimestamp(data["value"] / 1000))


class LogOperation(FieldOperation, abc.ABC):
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
        return LogSeriesValue[T](value_deserializer(data["value"]), data.get("step"), data["ts"])


@dataclass
class LogFloats(LogOperation):
    ValueType = LogSeriesValue[float]

    values: List[ValueType]

    def accept(self, visitor: "OperationVisitor[Ret]") -> Ret:
        return visitor.visit_log_floats(self)

    def to_dict(self) -> Dict[str, Any]:
        return {**super().to_dict(), "values": [value.to_dict() for value in self.values]}

    @classmethod
    def from_dict(cls, data: dict) -> "LogFloats":
        return cls(
            data["path"],
            [cls.ValueType.from_dict(value) for value in data["values"]],  # type: ignore[misc]
        )
