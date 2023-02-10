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
import abc
from collections import namedtuple

from neptune.attributes import constants as alpha_consts
from neptune.internal import operation as alpha_operation
from neptune.internal.backends.api_model import AttributeType as AlphaAttributeType

# Alpha equivalent of old api's `KeyValueProperty` used in `Experiment.properties`
from neptune.internal.operation import ImageValue
from neptune.legacy.exceptions import NeptuneException
from neptune.legacy.internal.channels.channels import (
    ChannelType,
    ChannelValueType,
)

AlphaKeyValueProperty = namedtuple("AlphaKeyValueProperty", ["key", "value"])


class AlphaAttributeWrapper(abc.ABC):
    """It's simple wrapper for `AttributeDTO`."""

    _allowed_atribute_types = list()

    def __init__(self, attribute):
        """Expects `AttributeDTO`"""
        assert self._allowed_atribute_types is not None
        if not self.is_valid_attribute(attribute):
            raise NeptuneException(f"Invalid channel attribute type: {attribute.type}")

        self._attribute = attribute

    @classmethod
    def is_valid_attribute(cls, attribute):
        """Checks if attribute can be wrapped by particular descendant of this class"""
        return attribute.type in cls._allowed_atribute_types

    @property
    def _properties(self):
        """Returns proper attribute property according to type"""
        return getattr(self._attribute, f"{self._attribute.type}Properties")


class AlphaPropertyDTO(AlphaAttributeWrapper):
    """It's simple wrapper for `AttributeDTO` objects which uses alpha variables attributes to fake properties.

    Alpha leaderboard doesn't have `KeyValueProperty` since it doesn't support properties at all,
    so we do need fake `KeyValueProperty` class for backward compatibility with old client's code."""

    _allowed_atribute_types = [
        AlphaAttributeType.STRING.value,
    ]

    @classmethod
    def is_valid_attribute(cls, attribute):
        """Checks if attribute can be used as property"""
        has_valid_type = super().is_valid_attribute(attribute)
        is_in_properties_space = attribute.name.startswith(alpha_consts.PROPERTIES_ATTRIBUTE_SPACE)
        return has_valid_type and is_in_properties_space

    @property
    def key(self):
        return self._properties.attributeName.split("/", 1)[-1]

    @property
    def value(self):
        return self._properties.value


class AlphaParameterDTO(AlphaAttributeWrapper):
    """It's simple wrapper for `AttributeDTO` objects which uses alpha variables attributes to fake properties.

    Alpha leaderboard doesn't have `KeyValueProperty` since it doesn't support properties at all,
    so we do need fake `KeyValueProperty` class for backward compatibility with old client's code."""

    _allowed_atribute_types = [
        AlphaAttributeType.FLOAT.value,
        AlphaAttributeType.STRING.value,
        AlphaAttributeType.DATETIME.value,
    ]

    @classmethod
    def is_valid_attribute(cls, attribute):
        """Checks if attribute can be used as property"""
        has_valid_type = super().is_valid_attribute(attribute)
        is_in_parameters_space = attribute.name.startswith(alpha_consts.PARAMETERS_ATTRIBUTE_SPACE)
        return has_valid_type and is_in_parameters_space

    @property
    def name(self):
        return self._properties.attributeName.split("/", 1)[-1]

    @property
    def value(self):
        return self._properties.value

    @property
    def parameterType(self):
        return "double" if self._properties.attributeType == AlphaAttributeType.FLOAT.value else "string"


class AlphaChannelDTO(AlphaAttributeWrapper):
    """It's simple wrapper for `AttributeDTO` objects which uses alpha series attributes to fake channels.

    Alpha leaderboard doesn't have `ChannelDTO` since it doesn't support channels at all,
    so we do need fake `ChannelDTO` class for backward compatibility with old client's code."""

    _allowed_atribute_types = [
        AlphaAttributeType.FLOAT_SERIES.value,
        AlphaAttributeType.STRING_SERIES.value,
        AlphaAttributeType.IMAGE_SERIES.value,
    ]

    @property
    def id(self):
        return self._properties.attributeName

    @property
    def name(self):
        return self._properties.attributeName.split("/", 1)[-1]

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
    def x(self):
        return self._properties.lastStep

    @property
    def y(self):
        if self.channelType == ChannelType.IMAGE.value:
            # We do not store last value for image series
            return None
        return self._properties.last


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
    return _map_using_dict(channel_type, "channel type", _channel_type_to_operation)


def channel_type_to_clear_operation(
    channel_type: ChannelType,
) -> alpha_operation.Operation:
    _channel_type_to_operation = {
        ChannelType.TEXT: alpha_operation.ClearStringLog,
        ChannelType.NUMERIC: alpha_operation.ClearFloatLog,
        ChannelType.IMAGE: alpha_operation.ClearImageLog,
    }
    return _map_using_dict(channel_type, "channel type", _channel_type_to_operation)


def channel_value_type_to_operation(
    channel_value_type: ChannelValueType,
) -> alpha_operation.Operation:
    _channel_value_type_to_operation = {
        ChannelValueType.TEXT_VALUE: alpha_operation.LogStrings,
        ChannelValueType.NUMERIC_VALUE: alpha_operation.LogFloats,
        ChannelValueType.IMAGE_VALUE: alpha_operation.LogImages,
    }
    return _map_using_dict(channel_value_type, "channel value type", _channel_value_type_to_operation)


def deprecated_img_to_alpha_image(img: dict) -> ImageValue:
    return ImageValue(data=img["data"], name=img["name"], description=img["description"])
