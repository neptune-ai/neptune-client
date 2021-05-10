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
import logging
import uuid
from datetime import datetime
from typing import List, Dict, Optional, Union

from neptune.new.exceptions import MetadataInconsistency
from neptune.new.internal.backends.api_model import LeaderboardEntry, AttributeWithProperties, AttributeType
from neptune.new.internal.backends.hosted_neptune_backend import HostedNeptuneBackend
from neptune.new.internal.utils.paths import join_paths, parse_path

logger = logging.getLogger(__name__)


class RunsTableEntry:

    def __init__(self, backend: HostedNeptuneBackend, _id: uuid.UUID, attributes: List[AttributeWithProperties]):
        self._backend = backend
        self._id = _id
        self._attributes = attributes

    def __getitem__(self, path: str) -> 'LeaderboardHandler':
        return LeaderboardHandler(self, path)

    def get_attribute_type(self, path: str) -> AttributeType:
        for attr in self._attributes:
            if attr.path == path:
                return attr.type
        raise ValueError("Could not find {} attribute".format(path))

    def get_attribute_value(self, path: str):
        for attr in self._attributes:
            if attr.path == path:
                _type = attr.type
                if _type == AttributeType.RUN_STATE:
                    return attr.properties.value
                if _type in (
                        AttributeType.FLOAT, AttributeType.INT, AttributeType.BOOL,
                        AttributeType.STRING, AttributeType.DATETIME,
                ):
                    return attr.properties.value
                if _type == AttributeType.FLOAT_SERIES or _type == AttributeType.STRING_SERIES:
                    return attr.properties.last
                if _type == AttributeType.IMAGE_SERIES:
                    raise MetadataInconsistency("Cannot get value for image series.")
                if _type == AttributeType.FILE:
                    raise MetadataInconsistency("Cannot get value for file attribute. Use download() instead.")
                if _type == AttributeType.FILE_SET:
                    raise MetadataInconsistency("Cannot get value for file set attribute. Use download() instead.")
                if _type == AttributeType.STRING_SET:
                    return set(attr.properties.values)
                if _type == AttributeType.GIT_REF:
                    return attr.properties.commit.commitId
                if _type == AttributeType.NOTEBOOK_REF:
                    return attr.properties.notebookName
                logger.error(
                    "Attribute type %s not supported in this version, yielding None. Recommended client upgrade.",
                    _type
                )
                return None
        raise ValueError("Could not find {} attribute".format(path))

    def download_file_attribute(self, path: str, destination: Optional[str]):
        for attr in self._attributes:
            if attr.path == path:
                _type = attr.type
                if _type == AttributeType.FILE:
                    self._backend.download_file(self._id, parse_path(path), destination)
                    return
                raise MetadataInconsistency("Cannot download file from attribute of type {}".format(_type))
        raise ValueError("Could not find {} attribute".format(path))

    def download_file_set_attribute(self, path: str, destination: Optional[str]):
        for attr in self._attributes:
            if attr.path == path:
                _type = attr.type
                if _type == AttributeType.FILE_SET:
                    self._backend.download_file_set(self._id, parse_path(path), destination)
                    return
                raise MetadataInconsistency("Cannot download ZIP archive from attribute of type {}".format(_type))
        raise ValueError("Could not find {} attribute".format(path))


class LeaderboardHandler:

    def __init__(self, run: RunsTableEntry, path: str):
        self._run = run
        self._path = path

    def __getitem__(self, path: str) -> 'LeaderboardHandler':
        return LeaderboardHandler(self._run, join_paths(self._path, path))

    def get(self):
        return self._run.get_attribute_value(self._path)

    def download(self, destination: Optional[str]):
        attr_type = self._run.get_attribute_type(self._path)
        if attr_type == AttributeType.FILE:
            return self._run.download_file_attribute(self._path, destination)
        elif attr_type == AttributeType.FILE_SET:
            return self._run.download_file_set_attribute(self._path, destination)
        raise MetadataInconsistency("Cannot download file from attribute of type {}".format(attr_type))


class RunsTable:

    def __init__(self, backend: HostedNeptuneBackend, entries: List[LeaderboardEntry]):
        self._backend = backend
        self._entries = entries

    def to_runs(self) -> List[RunsTableEntry]:
        return [RunsTableEntry(self._backend, e.id, e.attributes) for e in self._entries]

    def to_pandas(self):
        # pylint:disable=import-outside-toplevel
        import pandas as pd

        def make_attribute_value(attribute: AttributeWithProperties) -> Optional[Union[str, float, datetime]]:
            _type = attribute.type
            _properties = attribute.properties
            if _type == AttributeType.RUN_STATE:
                return _properties.value
            if _type in (
                    AttributeType.FLOAT, AttributeType.INT, AttributeType.BOOL,
                    AttributeType.STRING, AttributeType.DATETIME,
            ):
                return _properties.value
            if _type == AttributeType.FLOAT_SERIES or _type == AttributeType.STRING_SERIES:
                return _properties.last
            if _type == AttributeType.IMAGE_SERIES:
                return None
            if _type == AttributeType.FILE or _type == AttributeType.FILE_SET:
                return None
            if _type == AttributeType.STRING_SET:
                return ",".join(_properties.values)
            if _type == AttributeType.GIT_REF:
                return _properties.commit.commitId
            if _type == AttributeType.NOTEBOOK_REF:
                return _properties.notebookName
            logger.error(
                "Attribute type %s not supported in this version, yielding None. Recommended client upgrade.",
                _type
            )
            return None

        def make_row(entry: LeaderboardEntry) -> Dict[str, Optional[Union[str, float, datetime]]]:
            row: Dict[str, Union[str, float, datetime]] = dict()
            for attr in entry.attributes:
                value = make_attribute_value(attr)
                if value is not None:
                    row[attr.path] = value
            return row

        def sort_key(attr):
            domain = attr.split('/')[0]
            if domain == 'sys':
                return 0, attr
            if domain == 'monitoring':
                return 2, attr
            return 1, attr

        rows = dict((n, make_row(entry)) for (n, entry) in enumerate(self._entries))

        df = pd.DataFrame.from_dict(data=rows, orient='index')
        df = df.reindex(sorted(df.columns, key=sort_key), axis='columns')
        return df
