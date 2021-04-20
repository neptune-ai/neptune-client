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
from dataclasses import dataclass
from datetime import datetime
from typing import List, TypeVar, Generic, Optional, Set
from typing import TYPE_CHECKING

from neptune.new.exceptions import InternalClientError

if TYPE_CHECKING:
    from neptune.new.internal.operation_visitor import OperationVisitor

Ret = TypeVar('Ret')
T = TypeVar('T')


def all_subclasses(cls):
    return set(cls.__subclasses__()).union(
        [s for c in cls.__subclasses__() for s in all_subclasses(c)])


@dataclass
class Operation:

    path: List[str]

    @abc.abstractmethod
    def accept(self, visitor: 'OperationVisitor[Ret]') -> Ret:
        pass

    # pylint: disable=unused-argument
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


@dataclass
class AssignFloat(Operation):

    value: float

    def accept(self, visitor: 'OperationVisitor[Ret]') -> Ret:
        return visitor.visit_assign_float(self)

    def to_dict(self) -> dict:
        ret = super().to_dict()
        ret["value"] = self.value
        return ret

    @staticmethod
    def from_dict(data: dict) -> 'AssignFloat':
        return AssignFloat(data["path"], data["value"])


@dataclass
class AssignInt(Operation):

    value: int

    def accept(self, visitor: 'OperationVisitor[Ret]') -> Ret:
        return visitor.visit_assign_int(self)

    def to_dict(self) -> dict:
        ret = super().to_dict()
        ret["value"] = self.value
        return ret

    @staticmethod
    def from_dict(data: dict) -> 'AssignInt':
        return AssignInt(data["path"], data["value"])


@dataclass
class AssignBool(Operation):

    value: bool

    def accept(self, visitor: 'OperationVisitor[Ret]') -> Ret:
        return visitor.visit_assign_bool(self)

    def to_dict(self) -> dict:
        ret = super().to_dict()
        ret["value"] = self.value
        return ret

    @staticmethod
    def from_dict(data: dict) -> 'AssignBool':
        return AssignBool(data["path"], data["value"])


@dataclass
class AssignString(Operation):

    value: str

    def accept(self, visitor: 'OperationVisitor[Ret]') -> Ret:
        return visitor.visit_assign_string(self)

    def to_dict(self) -> dict:
        ret = super().to_dict()
        ret["value"] = self.value
        return ret

    @staticmethod
    def from_dict(data: dict) -> 'AssignString':
        return AssignString(data["path"], data["value"])


@dataclass
class AssignDatetime(Operation):

    value: datetime

    def accept(self, visitor: 'OperationVisitor[Ret]') -> Ret:
        return visitor.visit_assign_datetime(self)

    def to_dict(self) -> dict:
        ret = super().to_dict()
        ret["value"] = int(1000 * self.value.timestamp())
        return ret

    @staticmethod
    def from_dict(data: dict) -> 'AssignDatetime':
        return AssignDatetime(data["path"], datetime.fromtimestamp(data["value"] / 1000))


@dataclass
class UploadFile(Operation):

    ext: str
    file_path: str

    def accept(self, visitor: 'OperationVisitor[Ret]') -> Ret:
        return visitor.visit_upload_file(self)

    def to_dict(self) -> dict:
        ret = super().to_dict()
        ret["ext"] = self.ext
        ret["file_path"] = self.file_path
        return ret

    @staticmethod
    def from_dict(data: dict) -> 'UploadFile':
        return UploadFile(data["path"], data["ext"], data["file_path"])


@dataclass
class UploadFileContent(Operation):

    ext: str
    file_content: str

    def accept(self, visitor: 'OperationVisitor[Ret]') -> Ret:
        return visitor.visit_upload_file_content(self)

    def to_dict(self) -> dict:
        ret = super().to_dict()
        ret["ext"] = self.ext
        ret["file_content"] = self.file_content
        return ret

    @staticmethod
    def from_dict(data: dict) -> 'UploadFileContent':
        return UploadFileContent(data["path"], data["ext"], data["file_content"])


@dataclass
class UploadFileSet(Operation):

    file_globs: List[str]
    reset: bool

    def accept(self, visitor: 'OperationVisitor[Ret]') -> Ret:
        return visitor.visit_upload_file_set(self)

    def to_dict(self) -> dict:
        ret = super().to_dict()
        ret["file_globs"] = self.file_globs
        ret["reset"] = str(self.reset)
        return ret

    @staticmethod
    def from_dict(data: dict) -> 'UploadFileSet':
        return UploadFileSet(data["path"], data["file_globs"], data["reset"] != str(False))


@dataclass
class LogSeriesValue(Generic[T]):

    value: T
    step: Optional[float]
    ts: float

    def to_dict(self, value_serializer=lambda x: x) -> dict:
        return {
            "value": value_serializer(self.value),
            "step": self.step,
            "ts": self.ts
        }

    @staticmethod
    def from_dict(data: dict, value_deserializer=lambda x: x) -> 'LogSeriesValue[T]':
        return LogSeriesValue[T](value_deserializer(data["value"]), data.get("step", None), data["ts"])


@dataclass
class LogFloats(Operation):

    ValueType = LogSeriesValue[float]

    values: List[ValueType]

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


@dataclass
class LogStrings(Operation):

    ValueType = LogSeriesValue[str]

    values: List[ValueType]

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


@dataclass
class ImageValue:
    data: Optional[str]
    name: Optional[str]
    description: Optional[str]

    @staticmethod
    def serializer(obj: 'ImageValue'):
        return dict(data=obj.data, name=obj.name, description=obj.description)

    @staticmethod
    def deserializer(obj) -> 'ImageValue':
        if obj is None:
            return ImageValue(None, None, None)
        if isinstance(obj, str):
            return ImageValue(data=obj, name=None, description=None)
        if isinstance(obj, dict):
            return ImageValue(data=obj["data"], name=obj["name"], description=obj["description"])
        else:
            raise InternalClientError("Run data on disk is malformed or was saved by newer version of Neptune Library")


@dataclass
class LogImages(Operation):

    ValueType = LogSeriesValue[ImageValue]

    values: List[ValueType]

    def accept(self, visitor: 'OperationVisitor[Ret]') -> Ret:
        return visitor.visit_log_images(self)

    def to_dict(self) -> dict:
        ret = super().to_dict()
        ret["values"] = [value.to_dict(ImageValue.serializer) for value in self.values]
        return ret

    @staticmethod
    def from_dict(data: dict) -> 'LogImages':
        return LogImages(
            data["path"],
            [LogImages.ValueType.from_dict(value, ImageValue.deserializer) for value in data["values"]]
        )


@dataclass
class ClearFloatLog(Operation):

    def accept(self, visitor: 'OperationVisitor[Ret]') -> Ret:
        return visitor.visit_clear_float_log(self)

    @staticmethod
    def from_dict(data: dict) -> 'ClearFloatLog':
        return ClearFloatLog(data["path"])


@dataclass
class ClearStringLog(Operation):

    def accept(self, visitor: 'OperationVisitor[Ret]') -> Ret:
        return visitor.visit_clear_string_log(self)

    @staticmethod
    def from_dict(data: dict) -> 'ClearStringLog':
        return ClearStringLog(data["path"])


@dataclass
class ClearImageLog(Operation):

    def accept(self, visitor: 'OperationVisitor[Ret]') -> Ret:
        return visitor.visit_clear_image_log(self)

    @staticmethod
    def from_dict(data: dict) -> 'ClearImageLog':
        return ClearImageLog(data["path"])


@dataclass
class ConfigFloatSeries(Operation):

    min: Optional[float]
    max: Optional[float]
    unit: Optional[str]

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


@dataclass
class AddStrings(Operation):

    values: Set[str]

    def accept(self, visitor: 'OperationVisitor[Ret]') -> Ret:
        return visitor.visit_add_strings(self)

    def to_dict(self) -> dict:
        ret = super().to_dict()
        ret["values"] = list(self.values)
        return ret

    @staticmethod
    def from_dict(data: dict) -> 'AddStrings':
        return AddStrings(data["path"], set(data["values"]))


@dataclass
class RemoveStrings(Operation):

    values: Set[str]

    def accept(self, visitor: 'OperationVisitor[Ret]') -> Ret:
        return visitor.visit_remove_strings(self)

    def to_dict(self) -> dict:
        ret = super().to_dict()
        ret["values"] = list(self.values)
        return ret

    @staticmethod
    def from_dict(data: dict) -> 'RemoveStrings':
        return RemoveStrings(data["path"], set(data["values"]))


@dataclass
class ClearStringSet(Operation):

    def accept(self, visitor: 'OperationVisitor[Ret]') -> Ret:
        return visitor.visit_clear_string_set(self)

    @staticmethod
    def from_dict(data: dict) -> 'ClearStringSet':
        return ClearStringSet(data["path"])


@dataclass
class DeleteFiles(Operation):

    file_paths: Set[str]

    def accept(self, visitor: 'OperationVisitor[Ret]') -> Ret:
        return visitor.visit_delete_files(self)

    def to_dict(self) -> dict:
        ret = super().to_dict()
        ret["file_paths"] = list(self.file_paths)
        return ret

    @staticmethod
    def from_dict(data: dict) -> 'DeleteFiles':
        return DeleteFiles(data["path"], set(data["file_paths"]))


@dataclass
class DeleteAttribute(Operation):

    def accept(self, visitor: 'OperationVisitor[Ret]') -> Ret:
        return visitor.visit_delete_attribute(self)

    @staticmethod
    def from_dict(data: dict) -> 'DeleteAttribute':
        return DeleteAttribute(data["path"])
