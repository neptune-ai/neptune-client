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
    "DateTimeField",
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
    "FloatSeriesValues",
    "FloatPointValue",
    "StringSeriesValues",
    "StringPointValue",
    "ImageSeriesValues",
    "QueryFieldDefinitionsResult",
    "NextPage",
    "QueryFieldsResult",
)

import abc
import re
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from datetime import (
    datetime,
    timezone,
)
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

from neptune.api.proto.neptune_pb.api.model.attributes_pb2 import ProtoAttributeDefinitionDTO
from neptune.api.proto.neptune_pb.api.model.leaderboard_entries_pb2 import (
    ProtoAttributeDTO,
    ProtoAttributesDTO,
    ProtoBoolAttributeDTO,
    ProtoDatetimeAttributeDTO,
    ProtoFloatAttributeDTO,
    ProtoFloatSeriesAttributeDTO,
    ProtoIntAttributeDTO,
    ProtoLeaderboardEntriesSearchResultDTO,
    ProtoStringAttributeDTO,
    ProtoStringSetAttributeDTO,
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
    type: ClassVar[FieldType] = dataclass_field(init=False)
    _registry: ClassVar[Dict[str, Type[Field]]] = {}

    def __init_subclass__(cls, *args: Any, field_type: FieldType, **kwargs: Any) -> None:
        super().__init_subclass__(*args, **kwargs)
        cls.type = field_type
        cls._registry[field_type.value] = cls

    @classmethod
    def by_type(cls, field_type: FieldType) -> Type[Field]:
        return cls._registry[field_type.value]

    @abc.abstractmethod
    def accept(self, visitor: FieldVisitor[Ret]) -> Ret: ...

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> Field:
        field_type = data["type"]
        return Field._registry[field_type].from_dict(data[f"{field_type}Properties"])

    @staticmethod
    def from_model(model: Any) -> Field:
        field_type = str(model.type)
        return Field._registry[field_type].from_model(model.__getattr__(f"{field_type}Properties"))

    @staticmethod
    def from_proto(data: Any) -> Field:
        field_type = str(data.type)
        return Field._registry[field_type].from_proto(data.__getattribute__(f"{camel_to_snake(field_type)}_properties"))


def camel_to_snake(name: str) -> str:
    # Insert an underscore before any uppercase letters and convert the string to lowercase
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    # Handle the case where there are uppercase letters in the middle of the name
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


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
    def visit_datetime(self, field: DateTimeField) -> Ret: ...

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
        return FloatField(path=data["attributeName"], value=float(data["value"]))

    @staticmethod
    def from_model(model: Any) -> FloatField:
        return FloatField(path=model.attributeName, value=model.value)

    @staticmethod
    def from_proto(data: ProtoFloatAttributeDTO) -> FloatField:
        return FloatField(path=data.attribute_name, value=data.value)


@dataclass
class IntField(Field, field_type=FieldType.INT):
    value: int

    def accept(self, visitor: FieldVisitor[Ret]) -> Ret:
        return visitor.visit_int(self)

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> IntField:
        return IntField(path=data["attributeName"], value=int(data["value"]))

    @staticmethod
    def from_model(model: Any) -> IntField:
        return IntField(path=model.attributeName, value=model.value)

    @staticmethod
    def from_proto(data: ProtoIntAttributeDTO) -> IntField:
        return IntField(path=data.attribute_name, value=data.value)


@dataclass
class BoolField(Field, field_type=FieldType.BOOL):
    value: bool

    def accept(self, visitor: FieldVisitor[Ret]) -> Ret:
        return visitor.visit_bool(self)

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> BoolField:
        return BoolField(path=data["attributeName"], value=bool(data["value"]))

    @staticmethod
    def from_model(model: Any) -> BoolField:
        return BoolField(path=model.attributeName, value=model.value)

    @staticmethod
    def from_proto(data: ProtoBoolAttributeDTO) -> BoolField:
        return BoolField(path=data.attribute_name, value=data.value)


@dataclass
class StringField(Field, field_type=FieldType.STRING):
    value: str

    def accept(self, visitor: FieldVisitor[Ret]) -> Ret:
        return visitor.visit_string(self)

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> StringField:
        return StringField(path=data["attributeName"], value=str(data["value"]))

    @staticmethod
    def from_model(model: Any) -> StringField:
        return StringField(path=model.attributeName, value=model.value)

    @staticmethod
    def from_proto(data: ProtoStringAttributeDTO) -> StringField:
        return StringField(path=data.attribute_name, value=data.value)


@dataclass
class DateTimeField(Field, field_type=FieldType.DATETIME):
    value: datetime

    def accept(self, visitor: FieldVisitor[Ret]) -> Ret:
        return visitor.visit_datetime(self)

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> DateTimeField:
        return DateTimeField(path=data["attributeName"], value=parse_iso_date(data["value"]))

    @staticmethod
    def from_model(model: Any) -> DateTimeField:
        return DateTimeField(path=model.attributeName, value=parse_iso_date(model.value))

    @staticmethod
    def from_proto(data: ProtoDatetimeAttributeDTO) -> DateTimeField:
        return DateTimeField(
            path=data.attribute_name, value=datetime.fromtimestamp(data.value / 1000.0, tz=timezone.utc)
        )


@dataclass
class FileField(Field, field_type=FieldType.FILE):
    name: str
    ext: str
    size: int

    def accept(self, visitor: FieldVisitor[Ret]) -> Ret:
        return visitor.visit_file(self)

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> FileField:
        return FileField(path=data["attributeName"], name=data["name"], ext=data["ext"], size=int(data["size"]))

    @staticmethod
    def from_model(model: Any) -> FileField:
        return FileField(path=model.attributeName, name=model.name, ext=model.ext, size=model.size)

    @staticmethod
    def from_proto(data: Any) -> FileField:
        raise NotImplementedError()


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

    @staticmethod
    def from_proto(data: Any) -> FileSetField:
        raise NotImplementedError()


@dataclass
class FloatSeriesField(Field, field_type=FieldType.FLOAT_SERIES):
    last: Optional[float]

    def accept(self, visitor: FieldVisitor[Ret]) -> Ret:
        return visitor.visit_float_series(self)

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> FloatSeriesField:
        last = float(data["last"]) if "last" in data else None
        return FloatSeriesField(path=data["attributeName"], last=last)

    @staticmethod
    def from_model(model: Any) -> FloatSeriesField:
        return FloatSeriesField(path=model.attributeName, last=model.last)

    @staticmethod
    def from_proto(data: ProtoFloatSeriesAttributeDTO) -> FloatSeriesField:
        last = data.last if data.HasField("last") else None
        return FloatSeriesField(path=data.attribute_name, last=last)


@dataclass
class StringSeriesField(Field, field_type=FieldType.STRING_SERIES):
    last: Optional[str]

    def accept(self, visitor: FieldVisitor[Ret]) -> Ret:
        return visitor.visit_string_series(self)

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> StringSeriesField:
        last = str(data["last"]) if "last" in data else None
        return StringSeriesField(path=data["attributeName"], last=last)

    @staticmethod
    def from_model(model: Any) -> StringSeriesField:
        return StringSeriesField(path=model.attributeName, last=model.last)

    @staticmethod
    def from_proto(data: Any) -> StringSeriesField:
        raise NotImplementedError()


@dataclass
class ImageSeriesField(Field, field_type=FieldType.IMAGE_SERIES):
    last_step: Optional[float]

    def accept(self, visitor: FieldVisitor[Ret]) -> Ret:
        return visitor.visit_image_series(self)

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> ImageSeriesField:
        last_step = float(data["lastStep"]) if "lastStep" in data else None
        return ImageSeriesField(path=data["attributeName"], last_step=last_step)

    @staticmethod
    def from_model(model: Any) -> ImageSeriesField:
        return ImageSeriesField(path=model.attributeName, last_step=model.lastStep)

    @staticmethod
    def from_proto(data: Any) -> ImageSeriesField:
        raise NotImplementedError()


@dataclass
class StringSetField(Field, field_type=FieldType.STRING_SET):
    values: Set[str]

    def accept(self, visitor: FieldVisitor[Ret]) -> Ret:
        return visitor.visit_string_set(self)

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> StringSetField:
        return StringSetField(path=data["attributeName"], values=set(map(str, data["values"])))

    @staticmethod
    def from_model(model: Any) -> StringSetField:
        return StringSetField(path=model.attributeName, values=set(model.values))

    @staticmethod
    def from_proto(data: ProtoStringSetAttributeDTO) -> StringSetField:
        return StringSetField(path=data.attribute_name, values=set(data.value))


@dataclass
class GitCommit:
    commit_id: Optional[str]

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> GitCommit:
        commit_id = str(data["commitId"]) if "commitId" in data else None
        return GitCommit(commit_id=commit_id)

    @staticmethod
    def from_model(model: Any) -> GitCommit:
        return GitCommit(commit_id=model.commitId)

    @staticmethod
    def from_proto(data: Any) -> GitCommit:
        raise NotImplementedError()


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

    @staticmethod
    def from_proto(data: ProtoAttributeDTO) -> GitRefField:
        raise NotImplementedError()


@dataclass
class ObjectStateField(Field, field_type=FieldType.OBJECT_STATE):
    value: str

    def accept(self, visitor: FieldVisitor[Ret]) -> Ret:
        return visitor.visit_object_state(self)

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> ObjectStateField:
        value = RunState.from_api(str(data["value"])).value
        return ObjectStateField(path=data["attributeName"], value=value)

    @staticmethod
    def from_model(model: Any) -> ObjectStateField:
        value = RunState.from_api(str(model.value)).value
        return ObjectStateField(path=model.attributeName, value=value)

    @staticmethod
    def from_proto(data: Any) -> ObjectStateField:
        raise NotImplementedError()


@dataclass
class NotebookRefField(Field, field_type=FieldType.NOTEBOOK_REF):
    notebook_name: Optional[str]

    def accept(self, visitor: FieldVisitor[Ret]) -> Ret:
        return visitor.visit_notebook_ref(self)

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> NotebookRefField:
        notebook_name = str(data["notebookName"]) if "notebookName" in data else None
        return NotebookRefField(path=data["attributeName"], notebook_name=notebook_name)

    @staticmethod
    def from_model(model: Any) -> NotebookRefField:
        return NotebookRefField(path=model.attributeName, notebook_name=model.notebookName)

    @staticmethod
    def from_proto(data: Any) -> NotebookRefField:
        raise NotImplementedError()


@dataclass
class ArtifactField(Field, field_type=FieldType.ARTIFACT):
    hash: str

    def accept(self, visitor: FieldVisitor[Ret]) -> Ret:
        return visitor.visit_artifact(self)

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> ArtifactField:
        return ArtifactField(path=data["attributeName"], hash=str(data["hash"]))

    @staticmethod
    def from_model(model: Any) -> ArtifactField:
        return ArtifactField(path=model.attributeName, hash=model.hash)

    @staticmethod
    def from_proto(data: Any) -> ArtifactField:
        raise NotImplementedError()


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

    @staticmethod
    def from_proto(data: ProtoAttributesDTO) -> LeaderboardEntry:
        with_proto_support = {
            FieldType.STRING.value,
            FieldType.BOOL.value,
            FieldType.INT.value,
            FieldType.FLOAT.value,
            FieldType.DATETIME.value,
            FieldType.STRING_SET.value,
            FieldType.FLOAT_SERIES.value,
        }

        return LeaderboardEntry(
            object_id=data.experiment_id,
            fields=[Field.from_proto(field) for field in data.attributes if str(field.type) in with_proto_support],
        )


@dataclass
class LeaderboardEntriesSearchResult:
    entries: List[LeaderboardEntry]
    matching_item_count: int

    @staticmethod
    def from_dict(result: Dict[str, Any]) -> LeaderboardEntriesSearchResult:
        return LeaderboardEntriesSearchResult(
            entries=[LeaderboardEntry.from_dict(entry) for entry in result.get("entries", [])],
            matching_item_count=result["matchingItemCount"],
        )

    @staticmethod
    def from_model(result: Any) -> LeaderboardEntriesSearchResult:
        return LeaderboardEntriesSearchResult(
            entries=[LeaderboardEntry.from_model(entry) for entry in result.entries],
            matching_item_count=result.matchingItemCount,
        )

    @staticmethod
    def from_proto(data: ProtoLeaderboardEntriesSearchResultDTO) -> LeaderboardEntriesSearchResult:
        return LeaderboardEntriesSearchResult(
            entries=[LeaderboardEntry.from_proto(entry) for entry in data.entries],
            matching_item_count=data.matching_item_count,
        )


@dataclass
class NextPage:
    limit: Optional[int]
    next_page_token: Optional[str]

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> NextPage:
        return NextPage(limit=data.get("limit"), next_page_token=data.get("nextPageToken"))

    @staticmethod
    def from_model(model: Any) -> NextPage:
        return NextPage(limit=model.limit, next_page_token=model.nextPageToken)

    @staticmethod
    def from_proto(data: Any) -> NextPage:
        return NextPage(limit=data.limit, next_page_token=data.nextPageToken)

    def to_dto(self) -> Dict[str, Any]:
        return {
            "limit": self.limit,
            "nextPageToken": self.next_page_token,
        }


@dataclass
class QueryFieldsExperimentResult:
    object_id: str
    object_key: str
    fields: List[Field]

    # Any field the type of which is not in this set will not be
    # returned to the user. Applies to protobuf calls only.
    PROTO_SUPPORTED_FIELD_TYPES = {
        FieldType.STRING.value,
        FieldType.BOOL.value,
        FieldType.INT.value,
        FieldType.FLOAT.value,
        FieldType.DATETIME.value,
        FieldType.STRING_SET.value,
        FieldType.FLOAT_SERIES.value,
    }

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> QueryFieldsExperimentResult:
        return QueryFieldsExperimentResult(
            object_id=data["experimentId"],
            object_key=data["experimentShortId"],
            fields=[Field.from_dict(field) for field in data["attributes"]],
        )

    @staticmethod
    def from_model(model: Any) -> QueryFieldsExperimentResult:
        return QueryFieldsExperimentResult(
            object_id=model.experimentId,
            object_key=model.experimentShortId,
            fields=[Field.from_model(field) for field in model.attributes],
        )

    @staticmethod
    def from_proto(data: Any) -> QueryFieldsExperimentResult:
        return QueryFieldsExperimentResult(
            object_id=data.experimentId,
            object_key=data.experimentShortId,
            fields=[
                Field.from_proto(field)
                for field in data.attributes
                if field.type in QueryFieldsExperimentResult.PROTO_SUPPORTED_FIELD_TYPES
            ],
        )


@dataclass
class QueryFieldsResult:
    entries: List[QueryFieldsExperimentResult]
    next_page: NextPage

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> QueryFieldsResult:
        return QueryFieldsResult(
            entries=[QueryFieldsExperimentResult.from_dict(entry) for entry in data["entries"]],
            next_page=NextPage.from_dict(data["nextPage"]),
        )

    @staticmethod
    def from_model(model: Any) -> QueryFieldsResult:
        return QueryFieldsResult(
            entries=[QueryFieldsExperimentResult.from_model(entry) for entry in model.entries],
            next_page=NextPage.from_model(model.nextPage),
        )

    @staticmethod
    def from_proto(data: Any) -> QueryFieldsResult:
        return QueryFieldsResult(
            entries=[QueryFieldsExperimentResult.from_proto(entry) for entry in data.entries],
            next_page=NextPage.from_proto(data.nextPage),
        )


@dataclass
class QueryFieldDefinitionsResult:
    entries: List[FieldDefinition]
    next_page: NextPage

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> QueryFieldDefinitionsResult:
        return QueryFieldDefinitionsResult(
            entries=[FieldDefinition.from_dict(entry) for entry in data["entries"]],
            next_page=NextPage.from_dict(data["nextPage"]),
        )

    @staticmethod
    def from_model(model: Any) -> QueryFieldDefinitionsResult:
        return QueryFieldDefinitionsResult(
            entries=[FieldDefinition.from_model(entry) for entry in model.entries],
            next_page=NextPage.from_model(model.nextPage),
        )

    @staticmethod
    def from_proto(data: Any) -> QueryFieldDefinitionsResult:
        raise NotImplementedError()


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

    @staticmethod
    def from_proto(data: ProtoAttributeDefinitionDTO) -> FieldDefinition:
        return FieldDefinition(path=data.name, type=FieldType(data.type))


@dataclass
class FloatSeriesValues:
    total: int
    values: List[FloatPointValue]

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> FloatSeriesValues:
        return FloatSeriesValues(
            total=data["totalItemCount"], values=[FloatPointValue.from_dict(value) for value in data["values"]]
        )

    @staticmethod
    def from_model(model: Any) -> FloatSeriesValues:
        return FloatSeriesValues(
            total=model.totalItemCount, values=[FloatPointValue.from_model(value) for value in model.values]
        )

    @staticmethod
    def from_proto(data: Any) -> FloatSeriesValues:
        return FloatSeriesValues(
            total=data.total_item_count, values=[FloatPointValue.from_proto(value) for value in data.values]
        )


@dataclass
class FloatPointValue:
    timestamp: datetime
    value: float
    step: float

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> FloatPointValue:
        return FloatPointValue(
            timestamp=datetime.fromtimestamp(data["timestampMillis"] / 1000.0, tz=timezone.utc),
            value=float(data["value"]),
            step=float(data["step"]),
        )

    @staticmethod
    def from_model(model: Any) -> FloatPointValue:
        return FloatPointValue(
            timestamp=datetime.fromtimestamp(model.timestampMillis / 1000.0, tz=timezone.utc),
            value=model.value,
            step=model.step,
        )

    @staticmethod
    def from_proto(data: Any) -> FloatPointValue:
        return FloatPointValue(
            timestamp=datetime.fromtimestamp(data.timestamp_millis / 1000.0, tz=timezone.utc),
            value=data.value,
            step=data.step,
        )


@dataclass
class StringSeriesValues:
    total: int
    values: List[StringPointValue]

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> StringSeriesValues:
        return StringSeriesValues(
            total=data["totalItemCount"], values=[StringPointValue.from_dict(value) for value in data["values"]]
        )

    @staticmethod
    def from_model(model: Any) -> StringSeriesValues:
        return StringSeriesValues(
            total=model.totalItemCount, values=[StringPointValue.from_model(value) for value in model.values]
        )

    @staticmethod
    def from_proto(data: Any) -> StringSeriesValues:
        raise NotImplementedError()


@dataclass
class StringPointValue:
    timestamp: datetime
    step: float
    value: str

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> StringPointValue:
        return StringPointValue(
            timestamp=datetime.fromtimestamp(data["timestampMillis"] / 1000.0, tz=timezone.utc),
            value=str(data["value"]),
            step=float(data["step"]),
        )

    @staticmethod
    def from_model(model: Any) -> StringPointValue:
        return StringPointValue(
            timestamp=datetime.fromtimestamp(model.timestampMillis / 1000.0, tz=timezone.utc),
            value=model.value,
            step=model.step,
        )

    @staticmethod
    def from_proto(data: Any) -> StringPointValue:
        raise NotImplementedError()


@dataclass
class ImageSeriesValues:
    total: int

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> ImageSeriesValues:
        return ImageSeriesValues(total=data["totalItemCount"])

    @staticmethod
    def from_model(model: Any) -> ImageSeriesValues:
        return ImageSeriesValues(total=model.totalItemCount)

    @staticmethod
    def from_proto(data: Any) -> ImageSeriesValues:
        raise NotImplementedError()
