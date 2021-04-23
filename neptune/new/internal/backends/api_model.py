#
# Copyright (c) 2019, Neptune Labs Sp. z o.o.
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
import uuid
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional, List, Set, Any

from packaging.version import Version


class Project:

    def __init__(self, _uuid: uuid.UUID, name: str, workspace: str):
        self.uuid = _uuid
        self.name = name
        self.workspace = workspace


@dataclass
class ApiRun:
    uuid: uuid.UUID
    short_id: str
    workspace: str
    project_name: str
    trashed: bool


class ClientConfig(object):

    def __init__(self,
                 api_url: str,
                 display_url: str,
                 min_recommended_version: Optional[Version],
                 min_compatible_version: Optional[Version],
                 max_compatible_version: Optional[Version]):
        self._api_url = api_url
        self._display_url = display_url
        self._min_recommended_version = min_recommended_version
        self._min_compatible_version = min_compatible_version
        self._max_compatible_version = max_compatible_version

    @property
    def api_url(self) -> str:
        return self._api_url

    @property
    def display_url(self) -> str:
        return self._display_url

    @property
    def min_recommended_version(self) -> Optional[Version]:
        return self._min_recommended_version

    @property
    def min_compatible_version(self) -> Optional[Version]:
        return self._min_compatible_version

    @property
    def max_compatible_version(self) -> Optional[Version]:
        return self._max_compatible_version


class AttributeType(Enum):
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
    RUN_STATE = "experimentState"
    NOTEBOOK_REF = "notebookRef"


@dataclass
class Attribute:
    path: str
    type: AttributeType


@dataclass
class AttributeWithProperties:
    path: str
    type: AttributeType
    properties: Any


@dataclass
class LeaderboardEntry:
    id: uuid.UUID
    attributes: List[AttributeWithProperties]


@dataclass
class StringPointValue:
    timestampMillis: int
    step: float
    value: str


@dataclass
class ImageSeriesValues:
    totalItemCount: int


@dataclass
class StringSeriesValues:
    totalItemCount: int
    values: List[StringPointValue]


@dataclass
class FloatPointValue:
    timestampMillis: int
    step: float
    value: float


@dataclass
class FloatSeriesValues:
    totalItemCount: int
    values: List[FloatPointValue]


@dataclass
class FloatAttribute:
    value: float


@dataclass
class IntAttribute:
    value: int


@dataclass
class BoolAttribute:
    value: bool


@dataclass
class FileAttribute:
    name: str
    ext: str
    size: int


@dataclass
class StringAttribute:
    value: str


@dataclass
class DatetimeAttribute:
    value: datetime


@dataclass
class FloatSeriesAttribute:
    last: Optional[float]


@dataclass
class StringSeriesAttribute:
    last: Optional[str]


@dataclass
class StringSetAttribute:
    values: Set[str]
