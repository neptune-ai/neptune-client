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
from __future__ import annotations

__all__ = (
    "FileEntry",
    "Field",
    "FieldType",
    "LeaderboardEntry",
    "LeaderboardEntriesSearchResult",
    "FieldVisitor",
    "FloatField",
    "IntField",
    "BoolField",
    "StringField",
    "DatetimeField",
    "FileField",
    "FileSetField",
    "FloatSeriesField",
    "StringSeriesField",
    "ImageSeriesField",
    "StringSetField",
    "GitRefField",
    "ObjectStateField",
    "NotebookRefField",
    "ArtifactField",
    "FieldDefinition",
)

import abc
from dataclasses import (
    dataclass,
    field as dataclass_field,
)
from typing import TypeVar, Generic, Dict, Type, ClassVar
from datetime import datetime
from enum import Enum
from typing import (
    Any,
    Optional,
    Set,
    List,
)

Ret = TypeVar("Ret")


@dataclass
class FileEntry:
    name: str
    size: int
    mtime: datetime
    file_type: str

    @classmethod
    def from_dto(cls, file_dto: Any) -> "FileEntry":
        return cls(name=file_dto.name, size=file_dto.size, mtime=file_dto.mtime, file_type=file_dto.fileType)


class FieldType(Enum):
    FLOAT = "float"
    INT = "int"
    BOOL = "bool"
    STRING = "string"
    DATETIME = "datetime"
    FILE = "file"
    FILE_SET = "fileSet"
    FLOAT_SERIES = "floatSeries"
    STRING_SERIES = "stringSeries"
    IMAGE_SERIES = "imageSeries"
    STRING_SET = "stringSet"
    GIT_REF = "gitRef"
    OBJECT_STATE = "experimentState"
    NOTEBOOK_REF = "notebookRef"
    ARTIFACT = "artifact"


@dataclass
class Field(abc.ABC):
    path: str
    type: FieldType = dataclass_field(init=False, default=None)
    _registry: ClassVar[Dict[str, Type[Field]]] = {t.value: {} for t in FieldType}

    def __init_subclass__(cls, **kwargs) -> None:
        super().__init_subclass__(**kwargs)
        field_type: Optional[FieldType] = kwargs.get('type', None)
        if field_type is not None:
            cls.type = field_type
            cls._registry[field_type.value] = cls

    @abc.abstractmethod
    def accept(self, visitor: FieldVisitor[Ret]) -> Ret:
        ...

    @staticmethod
    def from_dict(field: Dict[str, Any]) -> Field:
        field_type = field["type"].value
        return Field._registry[field_type].from_dict(field[f"{field_type}Properties"])


class FieldVisitor(Generic[Ret], abc.ABC):

    def visit(self, field: Field) -> Ret:
        return field.accept(self)

    def visit_float(self, field: FloatField) -> Ret:
        ...

    def visit_int(self, field: IntField) -> Ret:
        ...

    def visit_bool(self, field: BoolField) -> Ret:
        ...

    def visit_string(self, field: StringField) -> Ret:
        ...

    def visit_datetime(self, field: DatetimeField) -> Ret:
        ...

    def visit_file(self, field: FileField) -> Ret:
        ...

    def visit_file_set(self, field: FileSetField) -> Ret:
        ...

    def visit_float_series(self, field: FloatSeriesField) -> Ret:
        ...

    def visit_string_series(self, field: StringSeriesField) -> Ret:
        ...

    def visit_image_series(self, field: ImageSeriesField) -> Ret:
        ...

    def visit_string_set(self, field: StringSetField) -> Ret:
        ...

    def visit_git_ref(self, field: GitRefField) -> Ret:
        ...

    def visit_object_state(self, field: ObjectStateField) -> Ret:
        ...

    def visit_notebook_ref(self, field: NotebookRefField) -> Ret:
        ...

    def visit_artifact(self, field: ArtifactField) -> Ret:
        ...


@dataclass
class FloatField(Field, type=FieldType.FLOAT):
    value: float

    def accept(self, visitor: FieldVisitor[Ret]) -> Ret:
        return visitor.visit_float(self)

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> FloatField:
        # TODO: Map only if not null
        return FloatField(path=data["attributeName"], value=float(data["value"]))


@dataclass
class IntField(Field, type=FieldType.INT):
    value: int

    def accept(self, visitor: FieldVisitor[Ret]) -> Ret:
        return visitor.visit_int(self)

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> IntField:
        # TODO: Map only if not null
        return IntField(path=data["attributeName"], value=int(data["value"]))


@dataclass
class BoolField(Field, type=FieldType.BOOL):
    value: bool

    def accept(self, visitor: FieldVisitor[Ret]) -> Ret:
        return visitor.visit_bool(self)

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> BoolField:
        # TODO: Map only if not null
        return BoolField(path=data["attributeName"], value=bool(data["value"]))


@dataclass
class StringField(Field, type=FieldType.STRING):
    value: str

    def accept(self, visitor: FieldVisitor[Ret]) -> Ret:
        return visitor.visit_string(self)

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> StringField:
        # TODO: Map only if not null
        return StringField(path=data["attributeName"], value=str(data["value"]))


@dataclass
class DatetimeField(Field, type=FieldType.DATETIME):
    value: datetime

    def accept(self, visitor: FieldVisitor[Ret]) -> Ret:
        return visitor.visit_datetime(self)

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> DatetimeField:
        # TODO: parse datetime
        return DatetimeField(path=data["attributeName"], value=data["value"])


@dataclass
class FileField(Field, type=FieldType.FILE):
    name: str
    ext: str
    size: int

    def accept(self, visitor: FieldVisitor[Ret]) -> Ret:
        return visitor.visit_file(self)

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> FileField:
        return FileField(
            path=data["path"],
            name=data["name"],
            ext=data["ext"],
            size=int(data["size"])
        )


@dataclass
class FileSetField(Field, type=FieldType.FILE_SET):
    size: int

    def accept(self, visitor: FieldVisitor[Ret]) -> Ret:
        return visitor.visit_file_set(self)

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> FileSetField:
        return FileSetField(path=data["attributeName"], size=int(data["size"]))


@dataclass
class FloatSeriesField(Field, type=FieldType.FLOAT_SERIES):
    last: Optional[float]

    def accept(self, visitor: "FieldVisitor[Ret]") -> Ret:
        return visitor.visit_float_series(self)

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> FloatSeriesField:
        # TODO: last is optional so map to float if present
        return FloatSeriesField(path=data["attributeName"], last=data["last"])


@dataclass
class StringSeriesField(Field, type=FieldType.STRING_SERIES):
    last: Optional[str]

    def accept(self, visitor: "FieldVisitor[Ret]") -> Ret:
        return visitor.visit_string_series(self)

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> StringSeriesField:
        # TODO: last is optional so map to str if present
        return StringSeriesField(path=data["attributeName"], last=data["last"])


@dataclass
class ImageSeriesField(Field, type=FieldType.IMAGE_SERIES):
    last_step: Optional[float]

    def accept(self, visitor: "FieldVisitor[Ret]") -> Ret:
        return visitor.visit_image_series(self)

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> ImageSeriesField:
        # TODO: last_step is optional so map to float if present
        return ImageSeriesField(path=data["attributeName"], last_step=data["lastStep"])


@dataclass
class StringSetField(Field, type=FieldType.STRING_SET):
    values: Set[str]

    def accept(self, visitor: "FieldVisitor[Ret]") -> Ret:
        return visitor.visit_string_set(self)

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> StringSetField:
        return StringSetField(path=data["attributeName"], values=set(data["values"]))


@dataclass
class GitRefField(Field, type=FieldType.GIT_REF):
    commit_id: Optional[str]

    def accept(self, visitor: FieldVisitor[Ret]) -> Ret:
        return visitor.visit_git_ref(self)

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> GitRefField:
        # TODO: commit and commit_id is optional so map to str if present
        return GitRefField(path=data["attributeName"], commit_id=data["commit"]["commitId"])


@dataclass
class ObjectStateField(Field, type=FieldType.OBJECT_STATE):
    value: str

    def accept(self, visitor: FieldVisitor[Ret]) -> Ret:
        return visitor.visit_object_state(self)

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> ObjectStateField:
        return ObjectStateField(path=data["attributeName"], value=str(data["value"]))


@dataclass
class NotebookRefField(Field, type=FieldType.NOTEBOOK_REF):
    notebook_name: Optional[str]

    def accept(self, visitor: FieldVisitor[Ret]) -> Ret:
        return visitor.visit_notebook_ref(self)

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> NotebookRefField:
        # TODO: notebook_name is optional so map to str if present
        return NotebookRefField(path=data["attributeName"], notebook_name=data["notebookName"])


@dataclass
class ArtifactField(Field, type=FieldType.ARTIFACT):
    hash: str

    def accept(self, visitor: FieldVisitor[Ret]) -> Ret:
        return visitor.visit_artifact(self)


@dataclass
class LeaderboardEntry:
    object_id: str
    fields: List[Field]

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> LeaderboardEntry:
        return LeaderboardEntry(
            object_id=data["experimentId"],
            fields=[Field.from_dict(field) for field in data["attributes"]]
        )


@dataclass
class LeaderboardEntriesSearchResult:
    entries: List[LeaderboardEntry]
    matching_item_count: int

    @staticmethod
    def from_dict(result: Dict[str, Any]) -> LeaderboardEntriesSearchResult:
        return LeaderboardEntriesSearchResult(
            entries=[LeaderboardEntry.from_dict(entry) for entry in result["entries"]],
            matching_item_count=result["matchingItemCount"],
        )


@dataclass
class FieldDefinition:
    path: str
    type: FieldType
