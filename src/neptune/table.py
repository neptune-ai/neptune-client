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

from neptune.exceptions import MetadataInconsistency
from neptune.integrations.pandas import to_pandas
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
from neptune.internal.utils.run_state import RunState
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
        attributes: List[AttributeWithProperties],
    ):
        self._backend = backend
        self._container_type = container_type
        self._id = _id
        self._attributes = attributes

    def __getitem__(self, path: str) -> "LeaderboardHandler":
        return LeaderboardHandler(table_entry=self, path=path)

    def get_attribute_type(self, path: str) -> AttributeType:
        for attr in self._attributes:
            if attr.path == path:
                return attr.type
        raise ValueError("Could not find {} attribute".format(path))

    def get_attribute_value(self, path: str) -> Any:
        for attr in self._attributes:
            if attr.path == path:
                _type = attr.type
                if _type == AttributeType.RUN_STATE:
                    return RunState.from_api(attr.properties.get("value")).value
                if _type in (
                    AttributeType.FLOAT,
                    AttributeType.INT,
                    AttributeType.BOOL,
                    AttributeType.STRING,
                    AttributeType.DATETIME,
                ):
                    return attr.properties.get("value")
                if _type == AttributeType.FLOAT_SERIES or _type == AttributeType.STRING_SERIES:
                    return attr.properties.get("last")
                if _type == AttributeType.IMAGE_SERIES:
                    raise MetadataInconsistency("Cannot get value for image series.")
                if _type == AttributeType.FILE:
                    raise MetadataInconsistency("Cannot get value for file attribute. Use download() instead.")
                if _type == AttributeType.FILE_SET:
                    raise MetadataInconsistency("Cannot get value for file set attribute. Use download() instead.")
                if _type == AttributeType.STRING_SET:
                    return set(attr.properties.get("values"))
                if _type == AttributeType.GIT_REF:
                    return attr.properties.get("commit", {}).get("commitId")
                if _type == AttributeType.NOTEBOOK_REF:
                    return attr.properties.get("notebookName")
                if _type == AttributeType.ARTIFACT:
                    return attr.properties.get("hash")
                logger.error(
                    "Attribute type %s not supported in this version, yielding None. Recommended client upgrade.",
                    _type,
                )
                return None
        raise ValueError("Could not find {} attribute".format(path))

    def download_file_attribute(
        self,
        path: str,
        destination: Optional[str],
        progress_bar: Optional[ProgressBarType] = None,
    ) -> None:
        for attr in self._attributes:
            if attr.path == path:
                _type = attr.type
                if _type == AttributeType.FILE:
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
        for attr in self._attributes:
            if attr.path == path:
                _type = attr.type
                if _type == AttributeType.FILE_SET:
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
        if attr_type == AttributeType.FILE:
            return self._table_entry.download_file_attribute(self._path, destination)
        elif attr_type == AttributeType.FILE_SET:
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
            _id=entry.id,
            attributes=entry.attributes,
        )

    def to_pandas(self) -> "pandas.DataFrame":
        return to_pandas(self)
