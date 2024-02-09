#
# Copyright (c) 2022, Neptune Labs Sp. z o.o.
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

__all__ = ["Table"]

from abc import abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import (
    Any,
    List,
    Optional,
    Union,
    Iterator, TypeVar, Generic,
    TYPE_CHECKING,
)

from neptune.exceptions import MetadataInconsistency
from neptune.internal.backends.api_model import (
    AttributeType,
    AttributeWithProperties,
    LeaderboardEntry,
)
from neptune.internal.backends.neptune_backend import NeptuneBackend
from neptune.internal.container_type import ContainerType
from neptune.internal.utils.logger import get_logger
from neptune.internal.utils.paths import (
    join_paths,
    parse_path,
)
from neptune.internal.utils.run_state import RunState as RunStateEnum
from neptune.typing import ProgressBarType


if TYPE_CHECKING:
    import pandas as pd

logger = get_logger()


def get_field_by_path(fields: List[Field], path: str) -> Field:
    for field in fields:
        if field.path == path:
            return field

    raise ValueError("Could not find {} attribute".format(path))


@dataclass
class Field:
    path: str

    @property
    def type(self) -> str:
        return self.__class__.__name__

    @abstractmethod
    def accept(self, visitor: "FieldVisitor[Ret]") -> Ret:
        ...


@dataclass
class Float(Field):
    value: float

    def accept(self, visitor: "FieldVisitor[Ret]") -> Ret:
        return visitor.visit_float(self)


@dataclass
class RunState(Field):
    value: str

    def accept(self, visitor: "FieldVisitor[Ret]") -> Ret:
        return visitor.visit_run_state(self)


@dataclass
class Int(Field):
    value: int

    def accept(self, visitor: "FieldVisitor[Ret]") -> Ret:
        return visitor.visit_int(self)


@dataclass
class Bool(Field):
    value: bool

    def accept(self, visitor: "FieldVisitor[Ret]") -> Ret:
        return visitor.visit_bool(self)


@dataclass
class String(Field):
    value: str

    def accept(self, visitor: "FieldVisitor[Ret]") -> Ret:
        return visitor.visit_string(self)


@dataclass
class Datetime(Field):
    value: datetime

    def accept(self, visitor: "FieldVisitor[Ret]") -> Ret:
        return visitor.visit_datetime(self)


@dataclass
class FloatSeries(Field):
    last: float

    def accept(self, visitor: "FieldVisitor[Ret]") -> Ret:
        return visitor.visit_float_series(self)


@dataclass
class StringSeries(Field):
    last: str

    def accept(self, visitor: "FieldVisitor[Ret]") -> Ret:
        return visitor.visit_string_series(self)


@dataclass
class StringSet(Field):
    values: List[str]

    def accept(self, visitor: "FieldVisitor[Ret]") -> Ret:
        return visitor.visit_string_set(self)


@dataclass
class File(Field):
    def accept(self, visitor: "FieldVisitor[Ret]") -> Ret:
        return visitor.visit_file(self)


@dataclass
class FileSet(Field):
    def accept(self, visitor: "FieldVisitor[Ret]") -> Ret:
        return visitor.visit_file_set(self)


@dataclass
class ImageSeries(Field):
    def accept(self, visitor: "FieldVisitor[Ret]") -> Ret:
        return visitor.visit_image_series(self)


@dataclass
class GitRef(Field):
    commit_id: str

    def accept(self, visitor: "FieldVisitor[Ret]") -> Ret:
        return visitor.visit_git_ref(self)


@dataclass
class NotebookRef(Field):
    notebookName: str

    def accept(self, visitor: "FieldVisitor[Ret]") -> Ret:
        return visitor.visit_notebook_ref(self)


@dataclass
class Artifact(Field):
    hash: str

    def accept(self, visitor: "FieldVisitor[Ret]") -> Ret:
        return visitor.visit_artifact(self)


Ret = TypeVar("Ret")


class FieldVisitor(Generic[Ret]):
    def visit(self, field: Field) -> Ret:
        return field.accept(self)

    @abstractmethod
    def visit_float(self, field: Float) -> Ret:
        ...

    @abstractmethod
    def visit_run_state(self, field: RunState) -> Ret:
        ...

    @abstractmethod
    def visit_int(self, field: Int) -> Ret:
        ...

    @abstractmethod
    def visit_bool(self, field: Bool) -> Ret:
        ...

    @abstractmethod
    def visit_string(self, field: String) -> Ret:
        ...

    @abstractmethod
    def visit_datetime(self, field: Datetime) -> Ret:
        ...

    @abstractmethod
    def visit_float_series(self, field: FloatSeries) -> Ret:
        ...

    @abstractmethod
    def visit_string_series(self, field: StringSeries) -> Ret:
        ...

    @abstractmethod
    def visit_string_set(self, field: StringSet) -> Ret:
        ...

    @abstractmethod
    def visit_file(self, field: File) -> Ret:
        ...

    @abstractmethod
    def visit_file_set(self, field: FileSet) -> Ret:
        ...

    @abstractmethod
    def visit_image_series(self, field: ImageSeries) -> Ret:
        ...

    @abstractmethod
    def visit_git_ref(self, field: GitRef) -> Ret:
        ...

    @abstractmethod
    def visit_notebook_ref(self, field: NotebookRef) -> Ret:
        ...

    @abstractmethod
    def visit_artifact(self, field: Artifact) -> Ret:
        ...


class ToValueVisitor(FieldVisitor[Union[float, str, int, bool, datetime, List[str], None]]):
    def visit_float(self, field: Float) -> float:
        return field.value

    def visit_run_state(self, field: RunState) -> str:
        return field.value

    def visit_int(self, field: Int) -> int:
        return field.value

    def visit_bool(self, field: Bool) -> bool:
        return field.value

    def visit_string(self, field: String) -> str:
        return field.value

    def visit_datetime(self, field: Datetime) -> datetime:
        return field.value

    def visit_float_series(self, field: FloatSeries) -> float:
        return field.last

    def visit_string_series(self, field: StringSeries) -> str:
        return field.last

    def visit_string_set(self, field: StringSet) -> List[str]:
        return field.values

    def visit_file(self, field: File) -> None:
        raise MetadataInconsistency("Cannot get value for file attribute. Use download() instead.")

    def visit_file_set(self, field: FileSet) -> None:
        raise MetadataInconsistency("Cannot get value for file set attribute. Use download() instead.")

    def visit_image_series(self, field: ImageSeries) -> None:
        raise MetadataInconsistency("Cannot get value for image series.")

    def visit_git_ref(self, field: GitRef) -> str:
        return field.commit_id

    def visit_notebook_ref(self, field: NotebookRef) -> str:
        return field.notebookName

    def visit_artifact(self, field: Artifact) -> str:
        return field.hash


def to_field(attr: AttributeWithProperties) -> Optional[Field]:
    if attr.type == AttributeType.RUN_STATE:
        return RunState(path=attr.path, value=str(RunStateEnum.from_api(attr.properties.get("value")).value))
    if attr.type == AttributeType.FLOAT:
        return Float(path=attr.path, value=float(attr.properties.get("value")))
    if attr.type == AttributeType.INT:
        return Int(path=attr.path, value=int(attr.properties.get("value")))
    if attr.type == AttributeType.BOOL:
        return Bool(path=attr.path, value=bool(attr.properties.get("value")))
    if attr.type == AttributeType.STRING:
        return String(path=attr.path, value=str(attr.properties.get("value")))
    if attr.type == AttributeType.DATETIME:
        return Datetime(path=attr.path, value=attr.properties.get("value"))
    if attr.type == AttributeType.FLOAT_SERIES:
        return FloatSeries(path=attr.path, last=float(attr.properties.get("last")))
    if attr.type == AttributeType.STRING_SERIES:
        return StringSeries(path=attr.path, last=str(attr.properties.get("last")))
    if attr.type == AttributeType.STRING_SET:
        return StringSet(path=attr.path, values=attr.properties.get("values"))
    if attr.type == AttributeType.FILE:
        return File(path=attr.path)
    if attr.type == AttributeType.FILE_SET:
        return FileSet(path=attr.path)
    if attr.type == AttributeType.IMAGE_SERIES:
        return ImageSeries(path=attr.path)
    if attr.type == AttributeType.GIT_REF:
        return GitRef(path=attr.path, commit_id=attr.properties.get("commit", {}).get("commitId"))
    if attr.type == AttributeType.NOTEBOOK_REF:
        return NotebookRef(path=attr.path, notebookName=attr.properties.get("notebookName"))
    if attr.type == AttributeType.ARTIFACT:
        return Artifact(path=attr.path, hash=attr.properties.get("hash"))
    logger.error(
        f"Attribute type {attr.type} not supported in this version, yielding None. Recommended client upgrade.",
    )
    return None


class TableEntry:
    def __init__(
        self,
        backend: NeptuneBackend,
        object_type: ContainerType,
        object_id: str,
        fields: List[Field],
    ):
        """Represents a single row in the Table."""
        self._backend: NeptuneBackend = backend
        self._object_type: ContainerType = object_type
        self._object_id: str = object_id
        self._fields: List[Field] = fields

    def __getitem__(self, path: str) -> "FieldHandler":
        return FieldHandler(table_entry=self, path=path)

    def __str__(self) -> str:
        return f"TableEntry(object_type='{self.object_type}', object_id='{self.object_id}') with {self.count} fields"

    @property
    def object_id(self) -> str:
        """Returns the id of the Neptune object."""
        return self._object_id

    @property
    def object_type(self) -> str:
        """Returns the type of the Neptune object."""
        return self._object_type.value.capitalize()

    @property
    def count(self) -> int:
        """Returns the number of fields in the Neptune object."""
        return len(self._fields)

    @property
    def fields(self) -> List[Field]:
        """Returns the list of fields"""
        return self._fields

    @property
    def paths(self) -> List[str]:
        """Returns the list of field paths."""
        return [field.path for field in self._fields]

    def get_field_type(self, path: str) -> str:
        """Returns the type of the field."""
        return get_field_by_path(fields=self._fields, path=path).type

    def get_field_value(self, path: str) -> Optional[Any]:
        """Returns the value of the field."""
        field = get_field_by_path(fields=self._fields, path=path)
        return ToValueVisitor().visit(field)

    def download(self, path: str, destination: Optional[str], progress_bar: Optional[ProgressBarType] = None) -> None:
        """Downloads the file from the specified field."""
        field = get_field_by_path(fields=self._fields, path=path)

        if isinstance(field, File):
            self._backend.download_file(
                container_id=self._object_id,
                container_type=self._object_type,
                path=parse_path(path),
                destination=destination,
                progress_bar=progress_bar,
            )
        elif isinstance(field, FileSet):
            self._backend.download_file_set(
                container_id=self._object_id,
                container_type=self._object_type,
                path=parse_path(path),
                destination=destination,
                progress_bar=progress_bar,
            )

        raise MetadataInconsistency(f"Cannot download file from attribute of type {field.type}")


class FieldHandler:
    def __init__(self, table_entry: TableEntry, path: str) -> None:
        self._table_entry: TableEntry = table_entry
        self._path: str = path

    def __getitem__(self, path: str) -> "FieldHandler":
        return FieldHandler(table_entry=self._table_entry, path=join_paths(self._path, path))

    def __str__(self) -> str:
        return f"FieldHandler(path='{self._path}')"

    def fetch(self) -> Any:
        """Fetches the value of the field."""
        return self._table_entry.get_field_value(path=self._path)

    def download(self, destination: Optional[str], progress_bar: Optional[ProgressBarType] = None) -> None:
        """Downloads the file from the specified field."""
        return self._table_entry.download(path=self._path, destination=destination, progress_bar=progress_bar)


class Table:
    def __init__(
        self,
        backend: NeptuneBackend,
        object_type: ContainerType,
        objects: Iterator[LeaderboardEntry],
    ):
        self._backend = backend
        self._objects = objects
        self._object_type = object_type
        self._iterator = iter(objects if objects else ())

    def __iter__(self) -> "Table":
        return self

    def __next__(self) -> TableEntry:
        entry = next(self._iterator)

        return TableEntry(
            backend=self._backend,
            object_type=self._object_type,
            object_id=entry.id,
            fields=[to_field(attr) for attr in entry.attributes],
        )

    def to_rows(self) -> List[TableEntry]:
        """Get list of entries."""
        return list(self)

    def to_pandas(self) -> "pd.DataFrame":
        """Converts the table to a pandas DataFrame."""
        from neptune.integrations.pandas import to_pandas
        return to_pandas(table=self)
