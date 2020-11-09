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
from datetime import datetime
from typing import List, TypeVar, Generic, Optional, Set

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from neptune.alpha.internal.operation_visitor import OperationVisitor

Ret = TypeVar('Ret')
T = TypeVar('T')


def all_subclasses(cls):
    return set(cls.__subclasses__()).union(
        [s for c in cls.__subclasses__() for s in all_subclasses(c)])


class Operation:

    def __init__(self, path: List[str]):
        self.path = path

    @abc.abstractmethod
    def accept(self, visitor: 'OperationVisitor[Ret]') -> Ret:
        pass

    def to_dict(self) -> dict:
        return {
            "type": self.__class__.__name__,
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

    def __eq__(self, other):
        if type(other) is type(self):
            return self.path == other.path
        else:
            return False

    def __hash__(self):
        return hash(self.path)


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

    def __init__(self, path: List[str], value: float):
        super().__init__(path)
        self.value = value

    def accept(self, visitor: 'OperationVisitor[Ret]') -> Ret:
        return visitor.visit_assign_float(self)

    def to_dict(self) -> dict:
        ret = super().to_dict()
        ret["value"] = self.value
        return ret

    @staticmethod
    def from_dict(data: dict) -> 'AssignFloat':
        return AssignFloat(data["path"], data["value"])

    def __eq__(self, other):
        if type(other) is type(self):
            return super().__eq__(other) and self.value == other.value
        else:
            return False

    def __hash__(self):
        return hash((super().__hash__(), self.value))


class AssignString(Operation):

    def __init__(self, path: List[str], value: str):
        super().__init__(path)
        self.value = value

    def accept(self, visitor: 'OperationVisitor[Ret]') -> Ret:
        return visitor.visit_assign_string(self)

    def to_dict(self) -> dict:
        ret = super().to_dict()
        ret["value"] = self.value
        return ret

    @staticmethod
    def from_dict(data: dict) -> 'AssignString':
        return AssignString(data["path"], data["value"])

    def __eq__(self, other):
        if type(other) is type(self):
            return super().__eq__(other) and self.value == other.value
        else:
            return False

    def __hash__(self):
        return hash((super().__hash__(), self.value))


class AssignDatetime(Operation):

    def __init__(self, path: List[str], value: datetime):
        super().__init__(path)
        self.value = value

    def accept(self, visitor: 'OperationVisitor[Ret]') -> Ret:
        return visitor.visit_assign_datetime(self)

    def to_dict(self) -> dict:
        ret = super().to_dict()
        ret["value"] = int(1000 * self.value.timestamp())
        return ret

    @staticmethod
    def from_dict(data: dict) -> 'AssignDatetime':
        return AssignDatetime(data["path"], datetime.fromtimestamp(data["value"] / 1000))

    def __eq__(self, other):
        if type(other) is type(self):
            return super().__eq__(other) and self.value == other.value
        else:
            return False

    def __hash__(self):
        return hash((super().__hash__(), self.value))


class UploadFile(Operation):

    def __init__(self, path: List[str], file_path: str):
        super().__init__(path)
        self.file_path = file_path

    def accept(self, visitor: 'OperationVisitor[Ret]') -> Ret:
        return visitor.visit_upload_file(self)

    def to_dict(self) -> dict:
        ret = super().to_dict()
        ret["file_path"] = self.file_path
        return ret

    @staticmethod
    def from_dict(data: dict) -> 'UploadFile':
        return UploadFile(data["path"], data["file_path"])

    def __eq__(self, other):
        if type(other) is type(self):
            return super().__eq__(other) and self.file_path == other.file_path
        else:
            return False

    def __hash__(self):
        return hash((super().__hash__(), self.file_path))


class LogSeriesValue(Generic[T]):

    def __init__(self, value: T, step: Optional[float], ts: float):
        self.value = value
        self.step = step
        self.ts = ts

    def to_dict(self) -> dict:
        return {
            "value": self.value,
            "step": self.step,
            "ts": self.ts
        }

    @staticmethod
    def from_dict(data: dict) -> 'LogSeriesValue[T]':
        return LogSeriesValue[T](data["value"], data.get("step", None), data["ts"])

    def __eq__(self, other):
        if type(other) is type(self):
            return self.value == other.value and self.step == other.step and self.ts == other.ts
        else:
            return False

    def __hash__(self):
        return hash((self.value, self.step, self.ts))


class LogFloats(Operation):

    ValueType = LogSeriesValue[float]

    def __init__(self, path: List[str], values: List[ValueType]):
        super().__init__(path)
        self.values = values

    def accept(self, visitor: 'OperationVisitor[Ret]') -> Ret:
        return visitor.visit_log_floats(self)

    def to_dict(self) -> dict:
        ret = super().to_dict()
        ret["values"] = [value.to_dict() for value in self.values]
        return ret

    @staticmethod
    def from_dict(data: dict) -> 'LogFloats':
        return LogFloats(
            data["path"],
            [LogFloats.ValueType.from_dict(value) for value in data["values"]]
        )

    def __eq__(self, other):
        if type(other) is type(self):
            return super().__eq__(other) and self.values == other.values
        else:
            return False

    def __hash__(self):
        return hash((super().__hash__(), self.values))


class LogStrings(Operation):

    ValueType = LogSeriesValue[str]

    def __init__(self, path: List[str], values: List[ValueType]):
        super().__init__(path)
        self.values = values

    def accept(self, visitor: 'OperationVisitor[Ret]') -> Ret:
        return visitor.visit_log_strings(self)

    def to_dict(self) -> dict:
        ret = super().to_dict()
        ret["values"] = [value.to_dict() for value in self.values]
        return ret

    @staticmethod
    def from_dict(data: dict) -> 'LogStrings':
        return LogStrings(
            data["path"],
            [LogStrings.ValueType.from_dict(value) for value in data["values"]]
        )

    def __eq__(self, other):
        if type(other) is type(self):
            return super().__eq__(other) and self.values == other.values
        else:
            return False

    def __hash__(self):
        return hash((super().__hash__(), self.values))


class LogImages(Operation):

    ValueType = LogSeriesValue[Optional[str]]

    def __init__(self, path: List[str], values: List[ValueType]):
        super().__init__(path)
        self.values = values

    def accept(self, visitor: 'OperationVisitor[Ret]') -> Ret:
        return visitor.visit_log_images(self)

    def to_dict(self) -> dict:
        ret = super().to_dict()
        ret["values"] = [value.to_dict() for value in self.values]
        return ret

    @staticmethod
    def from_dict(data: dict) -> 'LogImages':
        return LogImages(
            data["path"],
            [LogImages.ValueType.from_dict(value) for value in data["values"]]
        )

    def __eq__(self, other):
        if type(other) is type(self):
            return super().__eq__(other) and self.values == other.values
        else:
            return False

    def __hash__(self):
        return hash((super().__hash__(), self.values))


class ClearFloatLog(Operation):

    def accept(self, visitor: 'OperationVisitor[Ret]') -> Ret:
        return visitor.visit_clear_float_log(self)

    @staticmethod
    def from_dict(data: dict) -> 'ClearFloatLog':
        return ClearFloatLog(data["path"])


class ClearStringLog(Operation):

    def accept(self, visitor: 'OperationVisitor[Ret]') -> Ret:
        return visitor.visit_clear_string_log(self)

    @staticmethod
    def from_dict(data: dict) -> 'ClearStringLog':
        return ClearStringLog(data["path"])


class ClearImageLog(Operation):

    def accept(self, visitor: 'OperationVisitor[Ret]') -> Ret:
        return visitor.visit_clear_image_log(self)

    @staticmethod
    def from_dict(data: dict) -> 'ClearImageLog':
        return ClearImageLog(data["path"])


class ConfigFloatSeries(Operation):

    # pylint: disable=redefined-builtin
    def __init__(self, path: List[str], min: Optional[float], max: Optional[float], unit: Optional[str]):
        super().__init__(path)
        self.min = min
        self.max = max
        self.unit = unit

    def accept(self, visitor: 'OperationVisitor[Ret]') -> Ret:
        return visitor.visit_config_float_series(self)

    def to_dict(self) -> dict:
        ret = super().to_dict()
        ret["min"] = self.min
        ret["max"] = self.max
        ret["unit"] = self.unit
        return ret

    @staticmethod
    def from_dict(data: dict) -> 'ConfigFloatSeries':
        return ConfigFloatSeries(data["path"], data["min"], data["max"], data["unit"])

    def __eq__(self, other):
        if type(other) is type(self):
            return super().__eq__(other) and self.min == other.min and self.max == other.max and self.unit == other.unit
        else:
            return False

    def __hash__(self):
        return hash((super().__hash__(), self.min, self.max, self.unit))


class AddStrings(Operation):

    def __init__(self, path: List[str], values: Set[str]):
        super().__init__(path)
        self.values = values

    def accept(self, visitor: 'OperationVisitor[Ret]') -> Ret:
        return visitor.visit_add_strings(self)

    def to_dict(self) -> dict:
        ret = super().to_dict()
        ret["values"] = list(self.values)
        return ret

    @staticmethod
    def from_dict(data: dict) -> 'AddStrings':
        return AddStrings(data["path"], set(data["values"]))

    def __eq__(self, other):
        if type(other) is type(self):
            return super().__eq__(other) and self.values == other.values
        else:
            return False

    def __hash__(self):
        return hash((super().__hash__(), self.values))


class RemoveStrings(Operation):

    def __init__(self, path: List[str], values: Set[str]):
        super().__init__(path)
        self.values = values

    def accept(self, visitor: 'OperationVisitor[Ret]') -> Ret:
        return visitor.visit_remove_strings(self)

    def to_dict(self) -> dict:
        ret = super().to_dict()
        ret["values"] = list(self.values)
        return ret

    @staticmethod
    def from_dict(data: dict) -> 'RemoveStrings':
        return RemoveStrings(data["path"], set(data["values"]))

    def __eq__(self, other):
        if type(other) is type(self):
            return super().__eq__(other) and self.values == other.values
        else:
            return False

    def __hash__(self):
        return hash((super().__hash__(), self.values))


class ClearStringSet(Operation):

    def accept(self, visitor: 'OperationVisitor[Ret]') -> Ret:
        return visitor.visit_clear_string_set(self)

    @staticmethod
    def from_dict(data: dict) -> 'ClearStringSet':
        return ClearStringSet(data["path"])


class DeleteAttribute(Operation):

    def accept(self, visitor: 'OperationVisitor[Ret]') -> Ret:
        return visitor.visit_delete_attribute(self)

    @staticmethod
    def from_dict(data: dict) -> 'DeleteAttribute':
        return DeleteAttribute(data["path"])
