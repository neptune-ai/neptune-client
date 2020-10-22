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
from enum import Enum
from typing import Optional

from packaging.version import Version


class Project:

    def __init__(self, _uuid: uuid.UUID, name: str, workspace: str):
        self.uuid = _uuid
        self.name = name
        self.workspace = workspace


class Experiment:

    def __init__(self, _uuid: uuid.UUID, _id: str, project_uuid: uuid.UUID):
        self.uuid = _uuid
        self.id = _id
        self.project_uuid = project_uuid


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
    STRING = "string"
    DATETIME = "datetime"
    FILE = "file"
    FLOAT_SERIES = "floatSeries"
    STRING_SERIES = "stringSeries"
    IMAGE_SERIES = "imageSeries"
    STRING_SET = "stringSet"


class Attribute:

    def __init__(self, _path: str, _type: AttributeType):
        self.path = _path
        self.type = _type
