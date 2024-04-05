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
__all__ = ["Table"]

from typing import (
    TYPE_CHECKING,
    Any,
    Generator,
    List,
    Optional,
)

from neptune.api.field_visitor import FieldToValueVisitor
from neptune.api.models import (
    Field,
    FieldType,
    LeaderboardEntry,
)
from neptune.exceptions import MetadataInconsistency
from neptune.integrations.pandas import to_pandas
from neptune.internal.backends.neptune_backend import NeptuneBackend
from neptune.internal.container_type import ContainerType
from neptune.internal.utils.logger import get_logger
from neptune.internal.utils.paths import (
    join_paths,
    parse_path,
)
from neptune.typing import ProgressBarType

if TYPE_CHECKING:
    import pandas


logger = get_logger()


class TableEntry:
    def __init__(
        self,
        backend: NeptuneBackend,
        container_type: ContainerType,
        _id: str,
        attributes: List[Field],
    ):
        self._backend = backend
        self._container_type = container_type
        self._id = _id
        self._fields = attributes
        self._field_to_value_visitor = FieldToValueVisitor()

    def __getitem__(self, path: str) -> "LeaderboardHandler":
        return LeaderboardHandler(table_entry=self, path=path)

    def get_attribute_type(self, path: str) -> FieldType:
        for field in self._fields:
            if field.path == path:
                return field.type

        raise ValueError(f"Could not find {path} field")

    def get_attribute_value(self, path: str) -> Any:
        for field in self._fields:
            if field.path == path:
                return self._field_to_value_visitor.visit(field)
        raise ValueError("Could not find {} attribute".format(path))

    def download_file_attribute(
        self,
        path: str,
        destination: Optional[str],
        progress_bar: Optional[ProgressBarType] = None,
    ) -> None:
        for attr in self._fields:
            if attr.path == path:
                _type = attr.type
                if _type == FieldType.FILE:
                    self._backend.download_file(
                        container_id=self._id,
                        container_type=self._container_type,
                        path=parse_path(path),
                        destination=destination,
                        progress_bar=progress_bar,
                    )
                    return
                raise MetadataInconsistency("Cannot download file from attribute of type {}".format(_type))
        raise ValueError("Could not find {} attribute".format(path))

    def download_file_set_attribute(
        self,
        path: str,
        destination: Optional[str],
        progress_bar: Optional[ProgressBarType] = None,
    ) -> None:
        for attr in self._fields:
            if attr.path == path:
                _type = attr.type
                if _type == FieldType.FILE_SET:
                    self._backend.download_file_set(
                        container_id=self._id,
                        container_type=self._container_type,
                        path=parse_path(path),
                        destination=destination,
                        progress_bar=progress_bar,
                    )
                    return
                raise MetadataInconsistency("Cannot download ZIP archive from attribute of type {}".format(_type))
        raise ValueError("Could not find {} attribute".format(path))


class LeaderboardHandler:
    def __init__(self, table_entry: TableEntry, path: str) -> None:
        self._table_entry = table_entry
        self._path = path

    def __getitem__(self, path: str) -> "LeaderboardHandler":
        return LeaderboardHandler(table_entry=self._table_entry, path=join_paths(self._path, path))

    def get(self) -> Any:
        return self._table_entry.get_attribute_value(path=self._path)

    def download(self, destination: Optional[str]) -> None:
        attr_type = self._table_entry.get_attribute_type(self._path)
        if attr_type == FieldType.FILE:
            return self._table_entry.download_file_attribute(self._path, destination)
        elif attr_type == FieldType.FILE_SET:
            return self._table_entry.download_file_set_attribute(path=self._path, destination=destination)
        raise MetadataInconsistency("Cannot download file from attribute of type {}".format(attr_type))


class Table:
    def __init__(
        self,
        backend: NeptuneBackend,
        container_type: ContainerType,
        entries: Generator[LeaderboardEntry, None, None],
    ) -> None:
        self._backend = backend
        self._entries = entries
        self._container_type = container_type
        self._iterator = iter(entries if entries else ())

    def to_rows(self) -> List[TableEntry]:
        return list(self)

    def __iter__(self) -> "Table":
        return self

    def __next__(self) -> TableEntry:
        entry = next(self._iterator)

        return TableEntry(
            backend=self._backend,
            container_type=self._container_type,
            _id=entry.object_id,
            attributes=entry.fields,
        )

    def to_pandas(self) -> "pandas.DataFrame":
        return to_pandas(self)
