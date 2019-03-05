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


class ChannelWithValue(object):

    def __init__(self, channel_name, channel_type, x, y, t):
        self._channel_name = channel_name
        self._channel_type = channel_type
        self._x = x
        self._y = y
        if t is None:
            t = time.time()
        self._t = t

    @property
    def channel_name(self):
        return self._channel_name

    @property
    def channel_type(self):
        return self._channel_type

    @property
    def x(self):
        return self._x

    @property
    def y(self):
        return self._y

    @property
    def t(self):
        return self._t


class ChannelWithValues(object):

    def __init__(self, _id, values):
        self._id = _id
        self._values = values

    @property
    def id(self):
        return self._id

    @property
    def values(self):
        return self._values


class ChannelValue(object):

    def __init__(self, t, x, y):
        self._t = t
        self._x = x
        self._y = y

    @property
    def t(self):
        return self._t

    @property
    def x(self):
        return self._x

    @property
    def y(self):
        return self._y
