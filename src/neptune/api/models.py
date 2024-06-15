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
    "Field",
    "FieldType",
    "LeaderboardEntry",
    "LeaderboardEntriesSearchResult",
    "FieldVisitor",
    "FloatField",
    "IntField",
    "BoolField",
    "StringField",
    "DateTimeField",
    "FloatSeriesField",
    "StringSeriesField",
    "StringSetField",
    "ObjectStateField",
    "NotebookRefField",
    "FieldDefinition",
    "FloatSeriesValues",
    "FloatPointValue",
    "StringSeriesValues",
    "StringPointValue",
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


class FieldType(Enum):
    FLOAT = "float"
    INT = "int"
    BOOL = "bool"
    STRING = "string"
    DATETIME = "datetime"
    FLOAT_SERIES = "floatSeries"
    STRING_SERIES = "stringSeries"
    STRING_SET = "stringSet"
    OBJECT_STATE = "experimentState"
    NOTEBOOK_REF = "notebookRef"


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
    def visit_float_series(self, field: FloatSeriesField) -> Ret: ...

    @abc.abstractmethod
    def visit_string_series(self, field: StringSeriesField) -> Ret: ...

    @abc.abstractmethod
    def visit_string_set(self, field: StringSetField) -> Ret: ...

    @abc.abstractmethod
    def visit_object_state(self, field: ObjectStateField) -> Ret: ...

    @abc.abstractmethod
    def visit_notebook_ref(self, field: NotebookRefField) -> Ret: ...


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
        raise NotImplementedError()

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
        raise NotImplementedError()


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
        raise NotImplementedError()


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
