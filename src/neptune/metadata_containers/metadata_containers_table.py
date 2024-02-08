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

from datetime import datetime
from typing import (
    Any,
    Dict,
    Generator,
    List,
    Optional,
    Tuple,
    Union, Iterator,
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
from neptune.internal.utils.run_state import RunState
from neptune.typing import ProgressBarType

logger = get_logger()


def get_attribute_by_path(attributes: List[AttributeWithProperties], path: str) -> AttributeWithProperties:
    for attr in attributes:
        if attr.path == path:
            return attr

    raise ValueError("Could not find {} attribute".format(path))


def to_value(attr: AttributeWithProperties) -> Any:
    attr_type = attr.type
    _properties = attr.properties

    if attr_type == AttributeType.RUN_STATE:
        return str(RunState.from_api(_properties.get("value")).value)
    if attr_type == AttributeType.FLOAT:
        return float(_properties.get("value"))
    if attr_type == AttributeType.INT:
        return int(_properties.get("value"))
    if attr_type == AttributeType.BOOL:
        return bool(_properties.get("value"))
    if attr_type == AttributeType.STRING:
        return str(_properties.get("value"))
    if attr_type == AttributeType.DATETIME:
        # TODO: Think of this
        return _properties.get("value")
    if attr_type == AttributeType.FLOAT_SERIES:
        return float(_properties.get("last"))
    if attr_type == AttributeType.STRING_SERIES:
        return str(_properties.get("last"))
    if attr_type in (AttributeType.FILE, AttributeType.FILE_SET, AttributeType.IMAGE_SERIES):
        return None
    if attr_type == AttributeType.STRING_SET:
        # TODO: What to do here?
        return set(_properties.get("values"))
        # TODO: For pandas we need this
        return ",".join(_properties.get("values"))
    if attr_type == AttributeType.GIT_REF:
        return _properties.get("commit", {}).get("commitId")
    if attr_type == AttributeType.NOTEBOOK_REF:
        return _properties.get("notebookName")
    if attr_type == AttributeType.ARTIFACT:
        return _properties.get("hash")
    logger.error(
        f"Attribute type {attr.type} not supported in this version, yielding None. Recommended client upgrade.",
    )
    return None


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
        return get_attribute_by_path(attributes=self._attributes, path=path).type

    def get_attribute_value(self, path: str) -> Optional[Any]:
        attr = get_attribute_by_path(attributes=self._attributes, path=path)

        if attr.type == AttributeType.IMAGE_SERIES:
            raise MetadataInconsistency("Cannot get value for image series.")
        if attr.type == AttributeType.FILE:
            raise MetadataInconsistency("Cannot get value for file attribute. Use download() instead.")
        if attr.type == AttributeType.FILE_SET:
            raise MetadataInconsistency("Cannot get value for file set attribute. Use download() instead.")

        return to_value(attr=attr)

    def download_file_attribute(
        self,
        path: str,
        destination: Optional[str],
        progress_bar: Optional[ProgressBarType] = None,
    ) -> None:
        attr = get_attribute_by_path(attributes=self._attributes, path=path)
        if attr.type == AttributeType.FILE:
            self._backend.download_file(
                container_id=self._id,
                container_type=self._container_type,
                path=parse_path(path),
                destination=destination,
                progress_bar=progress_bar,
            )
        else:
            raise MetadataInconsistency(f"Cannot download file from attribute of type {attr.type}")

    def download_file_set_attribute(
        self,
        path: str,
        destination: Optional[str],
        progress_bar: Optional[ProgressBarType] = None,
    ) -> None:
        attr = get_attribute_by_path(attributes=self._attributes, path=path)
        if attr.type == AttributeType.FILE_SET:
            self._backend.download_file_set(
                container_id=self._id,
                container_type=self._container_type,
                path=parse_path(path),
                destination=destination,
                progress_bar=progress_bar,
            )
        else:
            raise MetadataInconsistency(f"Cannot download ZIP archive from attribute of type {attr.type}")


class LeaderboardHandler:
    def __init__(self, table_entry: TableEntry, path: str):
        self._table_entry: TableEntry = table_entry
        self._path: str = path

    def __getitem__(self, path: str) -> "LeaderboardHandler":
        return LeaderboardHandler(table_entry=self._table_entry, path=join_paths(self._path, path))

    def get(self) -> Any:
        return self._table_entry.get_attribute_value(path=self._path)

    def download(self, destination: Optional[str], progress_bar: Optional[ProgressBarType] = None) -> None:
        attr_type = self._table_entry.get_attribute_type(self._path)

        if attr_type == AttributeType.FILE:
            return self._table_entry.download_file_attribute(
                path=self._path, destination=destination, progress_bar=progress_bar
            )
        elif attr_type == AttributeType.FILE_SET:
            return self._table_entry.download_file_set_attribute(
                path=self._path, destination=destination, progress_bar=progress_bar
            )

        raise MetadataInconsistency(f"Cannot download file from attribute of type {attr_type}")


class Table:
    def __init__(
        self,
        backend: NeptuneBackend,
        container_type: ContainerType,
        entries: Iterator[LeaderboardEntry],
    ):
        self._backend = backend
        self._entries = entries
        self._container_type = container_type
        self._iterator = iter(entries if entries else ())

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

    def to_rows(self) -> List[TableEntry]:
        """TODO: Proper docstrings. Converts the leaderboard to a list of rows."""
        return list(self)

    def to_pandas(self) -> Any:  # TODO: return type should be pd.DataFrame
        """TODO: Proper docstrings. Converts the leaderboard to a pandas DataFrame."""
        from neptune.integrations.pandas import to_pandas

        return to_pandas(entries=self._entries)
