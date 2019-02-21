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
import re
import time


class ChannelWriter(object):
    __SPLIT_PATTERN = re.compile(r'[\n\r]')

    def __init__(self, experiment, channel_name):
        self.time_started = time.time() * 1000
        self._experiment = experiment
        self._channel_name = channel_name
        self._data = b''

    def write(self, data):
        self._data += data
        lines = self.__SPLIT_PATTERN.split(self._data)
        for i in range(0, len(lines) - 1):
            self._experiment.send_text(
                channel_name=self._channel_name,
                x=time.time() * 1000 - self.time_started,
                y=lines[i].encode('utf-8')
            )

        self._data = lines[-1]
