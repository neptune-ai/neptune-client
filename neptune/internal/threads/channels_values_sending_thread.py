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
from itertools import groupby

from future.moves import queue

from neptune.api_exceptions import NeptuneApiException
from neptune.internal.channels.channels import ChannelValue, ChannelWithValues
from neptune.internal.threads.neptune_thread import NeptuneThread


class ChannelsValuesSendingThread(NeptuneThread):
    _SLEEP_TIME = 5
    _MAX_VALUES_BATCH_LENGTH = 100

    def __init__(self, experiment, values_queue):
        super(ChannelsValuesSendingThread, self).__init__(is_daemon=False)
        self._values_queue = values_queue
        self._experiment = experiment
        self._sleep_time = self._SLEEP_TIME
        self._values_batch = []

    def run(self):
        sleep_time = 5
        while not self.is_interrupted():
            sleep_start = time.time()
            try:
                self._values_batch.append(self._values_queue.get(timeout=sleep_time))
                self._values_queue.task_done()
                sleep_time = time.time() - sleep_start
            except queue.Empty:
                sleep_time = 0

            if sleep_time <= 0 or len(self._values_batch) > self._MAX_VALUES_BATCH_LENGTH:
                self._process_batch()
                sleep_time = self._SLEEP_TIME

        self._join()

    def _join(self):
        while not self._values_queue.empty():
            self._values_batch.append(self._values_queue.get())
            self._values_queue.task_done()
        self._process_batch()

    def _process_batch(self):
        send_start = time.time()
        if self._values_batch:
            self._send_values(self._values_batch)
            self._values_batch = []
        self._sleep_time = self._SLEEP_TIME - (time.time() - send_start)

    def _send_values(self, values_with_channel):
        values_grouped_by_channel = {channel: list(values)
                                     for channel, values
                                     in groupby(values_with_channel,
                                                lambda value: (value.channel_name, value.channel_type))}
        channels_with_values = []

        for (channel_name, channel_type) in values_grouped_by_channel:
            # pylint: disable=protected-access
            channel = self._experiment._get_channel(channel_name, channel_type)
            last_x = channel.x if channel.x else 0
            channel_values = []
            for channel_with_value in values_grouped_by_channel[(channel_name, channel_type)]:
                x = channel_with_value.x if channel_with_value.x is not None else last_x + 1
                channel_values.append(ChannelValue(t=channel_with_value.t,
                                                   x=x,
                                                   y=channel_with_value.y))
                last_x = x

            channels_with_values.append(ChannelWithValues(channel.id, channel_values))

        # pylint: disable=protected-access
        try:
            self._experiment._send_channels_values(channels_with_values)
        except NeptuneApiException:
            pass
        except IOError:
            pass
