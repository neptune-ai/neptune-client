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
    "GitCommit",
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
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from datetime import datetime
from enum import Enum
from typing import (
    Any,
    ClassVar,
    Dict,
    Generic,
    List,
    Optional,
    Set,
    Type,
    TypeVar,
)

from neptune.internal.utils.iso_dates import parse_iso_date
from neptune.internal.utils.run_state import RunState

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
    type: FieldType = dataclass_field(init=False)
    _registry: ClassVar[Dict[str, Type[Field]]] = {}

    def __init_subclass__(cls, *args: Any, field_type: FieldType, **kwargs: Any) -> None:
        super().__init_subclass__(*args, **kwargs)
        cls.type = field_type
        cls._registry[field_type.value] = cls

    @abc.abstractmethod
    def accept(self, visitor: FieldVisitor[Ret]) -> Ret: ...

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> Field:
        field_type = data["type"]
        return Field._registry[field_type].from_dict(data[f"{field_type}Properties"])

    @staticmethod
    def from_model(model: Any) -> Field:
        field_type = str(model.type)
        return Field._registry[field_type].from_model(model.__getattribute__(f"{field_type}_properties"))


class FieldVisitor(Generic[Ret], abc.ABC):

    def visit(self, field: Field) -> Ret:
        return field.accept(self)

    @abc.abstractmethod
    def visit_float(self, field: FloatField) -> Ret: ...

    @abc.abstractmethod
    def visit_int(self, field: IntField) -> Ret: ...

    @abc.abstractmethod
    def visit_bool(self, field: BoolField) -> Ret: ...

    @abc.abstractmethod
    def visit_string(self, field: StringField) -> Ret: ...

    @abc.abstractmethod
    def visit_datetime(self, field: DatetimeField) -> Ret: ...

    @abc.abstractmethod
    def visit_file(self, field: FileField) -> Ret: ...

    @abc.abstractmethod
    def visit_file_set(self, field: FileSetField) -> Ret: ...

    @abc.abstractmethod
    def visit_float_series(self, field: FloatSeriesField) -> Ret: ...

    @abc.abstractmethod
    def visit_string_series(self, field: StringSeriesField) -> Ret: ...

    @abc.abstractmethod
    def visit_image_series(self, field: ImageSeriesField) -> Ret: ...

    @abc.abstractmethod
    def visit_string_set(self, field: StringSetField) -> Ret: ...

    @abc.abstractmethod
    def visit_git_ref(self, field: GitRefField) -> Ret: ...

    @abc.abstractmethod
    def visit_object_state(self, field: ObjectStateField) -> Ret: ...

    @abc.abstractmethod
    def visit_notebook_ref(self, field: NotebookRefField) -> Ret: ...

    @abc.abstractmethod
    def visit_artifact(self, field: ArtifactField) -> Ret: ...


@dataclass
class FloatField(Field, field_type=FieldType.FLOAT):
    value: float

    def accept(self, visitor: FieldVisitor[Ret]) -> Ret:
        return visitor.visit_float(self)

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> FloatField:
        # TODO: Map only if not null
        return FloatField(path=data["attributeName"], value=float(data["value"]))

    @staticmethod
    def from_model(model: Any) -> FloatField:
        return FloatField(path=model.attributeName, value=model.value)


@dataclass
class IntField(Field, field_type=FieldType.INT):
    value: int

    def accept(self, visitor: FieldVisitor[Ret]) -> Ret:
        return visitor.visit_int(self)

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> IntField:
        # TODO: Map only if not null
        return IntField(path=data["attributeName"], value=int(data["value"]))

    @staticmethod
    def from_model(model: Any) -> IntField:
        return IntField(path=model.attributeName, value=model.value)


@dataclass
class BoolField(Field, field_type=FieldType.BOOL):
    value: bool

    def accept(self, visitor: FieldVisitor[Ret]) -> Ret:
        return visitor.visit_bool(self)

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> BoolField:
        # TODO: Map only if not null
        return BoolField(path=data["attributeName"], value=bool(data["value"]))

    @staticmethod
    def from_model(model: Any) -> BoolField:
        return BoolField(path=model.attributeName, value=model.value)


@dataclass
class StringField(Field, field_type=FieldType.STRING):
    value: str

    def accept(self, visitor: FieldVisitor[Ret]) -> Ret:
        return visitor.visit_string(self)

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> StringField:
        # TODO: Map only if not null
        return StringField(path=data["attributeName"], value=str(data["value"]))

    @staticmethod
    def from_model(model: Any) -> StringField:
        return StringField(path=model.attributeName, value=model.value)


@dataclass
class DatetimeField(Field, field_type=FieldType.DATETIME):
    value: datetime

    def accept(self, visitor: FieldVisitor[Ret]) -> Ret:
        return visitor.visit_datetime(self)

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> DatetimeField:
        # TODO: what if none
        # TODO: Exceptions
        return DatetimeField(path=data["attributeName"], value=parse_iso_date(data["value"]))

    @staticmethod
    def from_model(model: Any) -> DatetimeField:
        return DatetimeField(path=model.attributeName, value=parse_iso_date(model.value))


@dataclass
class FileField(Field, field_type=FieldType.FILE):
    name: str
    ext: str
    size: int

    def accept(self, visitor: FieldVisitor[Ret]) -> Ret:
        return visitor.visit_file(self)

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> FileField:
        # TODO: Map to str if not null name and ext
        return FileField(path=data["attributeName"], name=data["name"], ext=data["ext"], size=int(data["size"]))

    @staticmethod
    def from_model(model: Any) -> FileField:
        return FileField(path=model.attributeName, name=model.name, ext=model.ext, size=model.size)


@dataclass
class FileSetField(Field, field_type=FieldType.FILE_SET):
    size: int

    def accept(self, visitor: FieldVisitor[Ret]) -> Ret:
        return visitor.visit_file_set(self)

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> FileSetField:
        return FileSetField(path=data["attributeName"], size=int(data["size"]))

    @staticmethod
    def from_model(model: Any) -> FileSetField:
        return FileSetField(path=model.attributeName, size=model.size)


@dataclass
class FloatSeriesField(Field, field_type=FieldType.FLOAT_SERIES):
    last: Optional[float]

    def accept(self, visitor: "FieldVisitor[Ret]") -> Ret:
        return visitor.visit_float_series(self)

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> FloatSeriesField:
        # TODO: last is optional so map to float if present
        # TODO: Last may not be present at all
        # TODO: Ensure that it's same as previously (last vs lastStep)
        return FloatSeriesField(path=data["attributeName"], last=data.get("last", None))

    @staticmethod
    def from_model(model: Any) -> FloatSeriesField:
        return FloatSeriesField(path=model.attributeName, last=model.last)


@dataclass
class StringSeriesField(Field, field_type=FieldType.STRING_SERIES):
    last: Optional[str]

    def accept(self, visitor: "FieldVisitor[Ret]") -> Ret:
        return visitor.visit_string_series(self)

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> StringSeriesField:
        # TODO: last is optional so map to str if present
        # TODO: Last may not be present at all
        # TODO: Ensure that it's same as previously (last vs lastStep)
        return StringSeriesField(path=data["attributeName"], last=data.get("last", ""))

    @staticmethod
    def from_model(model: Any) -> StringSeriesField:
        return StringSeriesField(path=model.attributeName, last=model.last)


@dataclass
class ImageSeriesField(Field, field_type=FieldType.IMAGE_SERIES):
    last_step: Optional[float]

    def accept(self, visitor: "FieldVisitor[Ret]") -> Ret:
        return visitor.visit_image_series(self)

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> ImageSeriesField:
        # TODO: last_step is optional so map to float if present
        return ImageSeriesField(path=data["attributeName"], last_step=data["lastStep"])

    @staticmethod
    def from_model(model: Any) -> ImageSeriesField:
        return ImageSeriesField(path=model.attributeName, last_step=model.lastStep)


@dataclass
class StringSetField(Field, field_type=FieldType.STRING_SET):
    values: Set[str]

    def accept(self, visitor: "FieldVisitor[Ret]") -> Ret:
        return visitor.visit_string_set(self)

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> StringSetField:
        return StringSetField(path=data["attributeName"], values=set(map(str, data.get("values", []))))

    @staticmethod
    def from_model(model: Any) -> StringSetField:
        return StringSetField(path=model.attributeName, values=set(model.values))


@dataclass
class GitCommit:
    commit_id: str

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> GitCommit:
        # TODO: commit and commit_id is optional so map to str if present
        return GitCommit(commit_id=str(data["commitId"]))

    @staticmethod
    def from_model(model: Any) -> GitCommit:
        return GitCommit(commit_id=model.commitId)


@dataclass
class GitRefField(Field, field_type=FieldType.GIT_REF):
    commit: Optional[GitCommit]

    def accept(self, visitor: FieldVisitor[Ret]) -> Ret:
        return visitor.visit_git_ref(self)

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> GitRefField:
        commit = GitCommit.from_dict(data["commit"]) if "commit" in data else None
        return GitRefField(path=data["attributeName"], commit=commit)

    @staticmethod
    def from_model(model: Any) -> GitRefField:
        commit = GitCommit.from_model(model.commit) if model.commit is not None else None
        return GitRefField(path=model.attributeName, commit=commit)


@dataclass
class ObjectStateField(Field, field_type=FieldType.OBJECT_STATE):
    value: str

    def accept(self, visitor: FieldVisitor[Ret]) -> Ret:
        return visitor.visit_object_state(self)

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> ObjectStateField:
        # TODO: value is optional so map to str if present
        value = RunState.from_api(str(data["value"])).value
        return ObjectStateField(path=data["attributeName"], value=value)

    @staticmethod
    def from_model(model: Any) -> ObjectStateField:
        value = RunState.from_api(str(model.value)).value
        return ObjectStateField(path=model.attributeName, value=value)


@dataclass
class NotebookRefField(Field, field_type=FieldType.NOTEBOOK_REF):
    notebook_name: Optional[str]

    def accept(self, visitor: FieldVisitor[Ret]) -> Ret:
        return visitor.visit_notebook_ref(self)

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> NotebookRefField:
        # TODO: notebook_name is optional so map to str if present
        return NotebookRefField(path=data["attributeName"], notebook_name=data.get("notebookName", None))

    @staticmethod
    def from_model(model: Any) -> NotebookRefField:
        return NotebookRefField(path=model.attributeName, notebook_name=model.notebookName)


@dataclass
class ArtifactField(Field, field_type=FieldType.ARTIFACT):
    hash: str

    def accept(self, visitor: FieldVisitor[Ret]) -> Ret:
        return visitor.visit_artifact(self)

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> ArtifactField:
        # TODO: hash is optional so map to str if present
        return ArtifactField(path=data["attributeName"], hash=str(data["hash"]))

    @staticmethod
    def from_model(model: Any) -> ArtifactField:
        return ArtifactField(path=model.attributeName, hash=model.hash)


@dataclass
class LeaderboardEntry:
    object_id: str
    fields: List[Field]

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> LeaderboardEntry:
        return LeaderboardEntry(
            object_id=data["experimentId"], fields=[Field.from_dict(field) for field in data["attributes"]]
        )

    @staticmethod
    def from_model(model: Any) -> LeaderboardEntry:
        return LeaderboardEntry(
            object_id=model.experimentId, fields=[Field.from_model(field) for field in model.attributes]
        )


@dataclass
class LeaderboardEntriesSearchResult:
    entries: List[LeaderboardEntry]
    matching_item_count: int

    @staticmethod
    def from_dict(result: Dict[str, Any]) -> LeaderboardEntriesSearchResult:
        return LeaderboardEntriesSearchResult(
            # TODO: Use generator instead
            entries=[LeaderboardEntry.from_dict(entry) for entry in result["entries"]],
            matching_item_count=result["matchingItemCount"],
        )

    @staticmethod
    def from_model(result: Any) -> LeaderboardEntriesSearchResult:
        return LeaderboardEntriesSearchResult(
            entries=[LeaderboardEntry.from_model(entry) for entry in result.entries],
            matching_item_count=result.matchingItemCount,
        )


@dataclass
class FieldDefinition:
    path: str
    type: FieldType

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> FieldDefinition:
        return FieldDefinition(path=data["name"], type=FieldType(data["type"]))

    @staticmethod
    def from_model(model: Any) -> FieldDefinition:
        return FieldDefinition(path=model.name, type=FieldType(model.type))