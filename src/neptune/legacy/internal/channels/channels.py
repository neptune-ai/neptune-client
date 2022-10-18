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
import time
from collections import namedtuple
from enum import Enum
from typing import List

from neptune.legacy.exceptions import NeptuneException

ChannelNameWithTypeAndNamespace = namedtuple(
    "ChannelNameWithType",
    ["channel_id", "channel_name", "channel_type", "channel_namespace"],
)


class ChannelType(Enum):
    TEXT = "text"
    NUMERIC = "numeric"
    IMAGE = "image"


class ChannelValueType(Enum):
    TEXT_VALUE = "text_value"
    NUMERIC_VALUE = "numeric_value"
    IMAGE_VALUE = "image_value"


class ChannelNamespace(Enum):
    USER = "user"
    SYSTEM = "system"


class ChannelValue(object):
    def __init__(self, x, y, ts):
        self._x = x
        self._y = y
        if ts is None:
            ts = time.time()
        self._ts = ts

    @property
    def ts(self):
        return self._ts

    @property
    def x(self):
        return self._x

    @property
    def y(self):
        return self._y

    @property
    def value(self):
        return self.y.get(self.value_type.value)

    @property
    def value_type(self) -> ChannelValueType:
        """We expect that exactly one of `y` values is not None, and according to that we try to determine value type"""
        unique_channel_value_types = set(
            [ch_value_type for ch_value_type in ChannelValueType if self.y.get(ch_value_type.value) is not None]
        )
        if len(unique_channel_value_types) > 1:
            raise NeptuneException(f"There are mixed value types in {self}")
        if not unique_channel_value_types:
            raise NeptuneException(f"Can't determine type of {self}")

        return next(iter(unique_channel_value_types))

    def __str__(self):
        return "ChannelValue(x={},y={},ts={})".format(self.x, self.y, self.ts)

    def __repr__(self):
        return str(self)

    def __eq__(self, o):
        return self.__dict__ == o.__dict__


class ChannelIdWithValues:
    def __init__(self, channel_id, channel_name, channel_type, channel_namespace, channel_values):
        self._channel_id = channel_id
        self._channel_name = channel_name
        self._channel_type = channel_type
        self._channel_namespace = channel_namespace
        self._channel_values = channel_values

    @property
    def channel_id(self) -> str:
        return self._channel_id

    @property
    def channel_name(self) -> str:
        return self._channel_name

    @property
    def channel_values(self) -> List[ChannelValue]:
        return self._channel_values

    @property
    def channel_type(self) -> ChannelValueType:
        if self._channel_type == ChannelType.NUMERIC.value:
            return ChannelValueType.NUMERIC_VALUE
        elif self._channel_type == ChannelType.TEXT.value:
            return ChannelValueType.TEXT_VALUE
        elif self._channel_type == ChannelType.IMAGE.value:
            return ChannelValueType.IMAGE_VALUE
        else:
            raise NeptuneException(f"Unknown channel type: {self._channel_type}")

    @property
    def channel_namespace(self) -> ChannelNamespace:
        return self._channel_namespace

    def __eq__(self, other):
        return self.channel_id == other.channel_id and self.channel_values == other.channel_values

    def __gt__(self, other):
        return hash(self.channel_id) < hash(other.channel_id)
