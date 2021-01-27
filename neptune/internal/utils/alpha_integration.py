#
# Copyright (c) 2021, Neptune Labs Sp. z o.o.
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

from neptune.alpha.internal import operation as alpha_operation
from neptune.alpha.internal.backends.api_model import AttributeType as AlphaAttributeType
from neptune.exceptions import NeptuneException
from neptune.internal.channels.channels import ChannelType, ChannelValueType


class AlphaChannelDTO:
    """It's simple wrapper for `AttributeDTO` objects which uses alpha series attributes to fake channels.

    Alpha leaderboard doesn't have `ChannelDTO` since it doesn't support channels at all,
    so we do need fake `ChannelDTO` class for backward compatibility with old client's code."""

    _allowed_atribute_types = [
        AlphaAttributeType.FLOAT_SERIES.value,
        AlphaAttributeType.STRING_SERIES.value,
        AlphaAttributeType.IMAGE_SERIES.value,
    ]

    def __init__(self, attribute):
        """Expects `AttributeDTO`"""
        if not self.is_valid_attribute_for_channel(attribute):
            raise NeptuneException(f"Invalid channel attribute type: {attribute.type}")

        self._attribute = attribute

    @classmethod
    def is_valid_attribute_for_channel(cls, attribute):
        """Checks if attribute can be used as channel"""
        return attribute.type in cls._allowed_atribute_types

    @property
    def id(self):
        return self._properties.attributeName

    @property
    def name(self):
        return self._properties.attributeName.split('/', 1)[-1]

    @property
    def channelType(self):
        attr_type = self._properties.attributeType
        if attr_type == AlphaAttributeType.FLOAT_SERIES.value:
            return ChannelType.NUMERIC.value
        elif attr_type == AlphaAttributeType.STRING_SERIES.value:
            return ChannelType.TEXT.value
        elif attr_type == AlphaAttributeType.IMAGE_SERIES.value:
            return ChannelType.IMAGE.value

    @property
    def lastX(self):
        if self.channelType == ChannelType.IMAGE.value:
            # We do not store last value for image series
            return None
        return self._properties.last

    @property
    def _properties(self):
        """Returns proper attribute property according to type"""
        return getattr(self._attribute, f'{self._attribute.type}Properties')


class AlphaChannelWithValueDTO:
    """Alpha leaderboard doesn't have `ChannelWithValueDTO` since it doesn't support channels at all,
    so we do need fake `ChannelWithValueDTO` class for backward compatibility with old client's code"""

    def __init__(self, channelId: str, channelName: str, channelType: str, x, y):
        self._ch_id = channelId
        self._ch_name = channelName
        self._ch_type = channelType
        self._x = x
        self._y = y

    @property
    def channelId(self):
        return self._ch_id

    @property
    def channelName(self):
        return self._ch_name

    @property
    def channelType(self):
        return self._ch_type

    @property
    def x(self):
        return self._x

    @x.setter
    def x(self, x):
        self._x = x

    @property
    def y(self):
        return self._y

    @y.setter
    def y(self, y):
        self._y = y


def _map_using_dict(el, el_name, source_dict) -> alpha_operation.Operation:
    try:
        return source_dict[el]
    except KeyError as e:
        raise NeptuneException(f"We're not supporting {el} {el_name}.") from e


def channel_type_to_operation(channel_type: ChannelType) -> alpha_operation.Operation:
    _channel_type_to_operation = {
        ChannelType.TEXT: alpha_operation.LogStrings,
        ChannelType.NUMERIC: alpha_operation.LogFloats,
        ChannelType.IMAGE: alpha_operation.LogImages,
    }
    return _map_using_dict(channel_type, 'channel type', _channel_type_to_operation)


def channel_value_type_to_operation(channel_value_type: ChannelValueType) -> alpha_operation.Operation:
    _channel_value_type_to_operation = {
        ChannelValueType.TEXT_VALUE: alpha_operation.LogStrings,
        ChannelValueType.NUMERIC_VALUE: alpha_operation.LogFloats,
        ChannelValueType.IMAGE_VALUE: alpha_operation.LogImages,
    }
    return _map_using_dict(channel_value_type, 'channel value type', _channel_value_type_to_operation)
