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


class AlphaChannelDTO:
    """Alpha leaderboard doesn't have `ChannelDTO` since it doesn't support channels at all,
    so we do need fake `ChannelDTO` class for backward compatibility with old client's code"""
    def __init__(self, channelId: str, channelName: str, channelType: str):
        self._ch_id = channelId
        self._ch_name = channelName
        self._ch_type = channelType

    @property
    def id(self):
        return self._ch_id

    @property
    def name(self):
        return self._ch_name

    @property
    def type(self):
        return self._ch_type


class AlphaChannelWithValueDTO(AlphaChannelDTO):
    """Alpha leaderboard doesn't have `ChannelWithValueDTO` since it doesn't support channels at all,
    so we do need fake `ChannelWithValueDTO` class for backward compatibility with old client's code"""
    def __init__(self, channelId: str, channelName: str, channelType: str, x, y):
        super().__init__(channelId, channelName, channelType)
        self._x = x
        self._y = y

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
