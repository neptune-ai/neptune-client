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

ChannelIdWithValues = namedtuple('ChannelIdWithValues', ['channel_id', 'channel_values'])


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
