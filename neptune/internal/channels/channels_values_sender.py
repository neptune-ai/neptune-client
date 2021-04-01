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
import logging
import threading
import time
from collections import namedtuple
from itertools import groupby
from queue import Queue, Empty

from bravado.exception import HTTPUnprocessableEntity

from neptune.exceptions import NeptuneException
from neptune.internal.channels.channels import ChannelIdWithValues, ChannelValue, \
    ChannelType, ChannelNamespace
from neptune.internal.threads.neptune_thread import NeptuneThread

_logger = logging.getLogger(__name__)


class ChannelsValuesSender(object):
    _QUEUED_CHANNEL_VALUE = namedtuple(
        "QueuedChannelValue",
        ['channel_id', 'channel_name', 'channel_type', 'channel_value', 'channel_namespace']
    )

    __LOCK = threading.RLock()

    def __init__(self, experiment):
        self._experiment = experiment
        self._values_queue = None
        self._sending_thread = None
        self._user_channel_name_to_id_map = dict()
        self._system_channel_name_to_id_map = dict()

    # pylint:disable=protected-access
    def send(self, channel_name, channel_type, channel_value, channel_namespace=ChannelNamespace.USER):
        # Before taking the lock here, we need to check if the sending thread is not running yet.
        # Otherwise, the sending thread could call send() while being join()-ed, which would result
        # in a deadlock.
        if not self._is_running():
            with self.__LOCK:
                if not self._is_running():
                    self._start()

        if channel_namespace == ChannelNamespace.USER:
            namespaced_channel_map = self._user_channel_name_to_id_map
        else:
            namespaced_channel_map = self._system_channel_name_to_id_map

        if channel_name in namespaced_channel_map:
            channel_id = namespaced_channel_map[channel_name]
        else:
            response = self._experiment._create_channel(channel_name, channel_type, channel_namespace)
            channel_id = response.id
            namespaced_channel_map[channel_name] = channel_id

        self._values_queue.put(self._QUEUED_CHANNEL_VALUE(
            channel_id=channel_id,
            channel_name=channel_name,
            channel_type=channel_type,
            channel_value=channel_value,
            channel_namespace=channel_namespace
        ))

    def join(self):
        with self.__LOCK:
            if self._is_running():
                self._sending_thread.interrupt()
                self._sending_thread.join()
                self._sending_thread = None
                self._values_queue = None

    def _is_running(self):
        return self._values_queue is not None and self._sending_thread is not None and self._sending_thread.is_alive()

    def _start(self):
        self._values_queue = Queue()
        self._sending_thread = ChannelsValuesSendingThread(self._experiment, self._values_queue)
        self._sending_thread.start()


class ChannelsValuesSendingThread(NeptuneThread):
    _SLEEP_TIME = 5
    _MAX_VALUES_BATCH_LENGTH = 100
    _MAX_IMAGE_VALUES_BATCH_SIZE = 10485760  # 10 MB

    def __init__(self, experiment, values_queue):
        super(ChannelsValuesSendingThread, self).__init__(is_daemon=False)
        self._values_queue = values_queue
        self._experiment = experiment
        self._sleep_time = self._SLEEP_TIME
        self._values_batch = []

    def run(self):
        while self.should_continue_running() or not self._values_queue.empty():
            try:
                sleep_start = time.time()
                self._values_batch.append(self._values_queue.get(timeout=max(self._sleep_time, 0)))
                self._values_queue.task_done()
                self._sleep_time -= time.time() - sleep_start
            except Empty:
                self._sleep_time = 0

            image_values_batch_size = sum([len(v.channel_value.y['image_value']['data'] or [])
                                           for v in self._values_batch
                                           if v.channel_type == ChannelType.IMAGE.value])
            if self._sleep_time <= 0 \
                    or len(self._values_batch) >= self._MAX_VALUES_BATCH_LENGTH \
                    or image_values_batch_size >= self._MAX_IMAGE_VALUES_BATCH_SIZE:  # pylint:disable=line-too-long
                self._process_batch()

        self._process_batch()

    def _process_batch(self):
        send_start = time.time()
        if self._values_batch:
            try:
                self._send_values(self._values_batch)
                self._values_batch = []
            except (NeptuneException, IOError):
                _logger.exception('Failed to send channel value.')
        self._sleep_time = self._SLEEP_TIME - (time.time() - send_start)

    def _send_values(self, queued_channels_values):
        def get_channel_metadata(value):
            return value.channel_id, value.channel_name, value.channel_type, value.channel_namespace

        queued_grouped_by_channel = {channel_metadata: list(values)
                                     for channel_metadata, values
                                     in groupby(sorted(queued_channels_values, key=get_channel_metadata),
                                                get_channel_metadata)}
        channels_with_values = []
        for channel_metadata in queued_grouped_by_channel:
            channel_values = []
            for queued_value in queued_grouped_by_channel[channel_metadata]:
                channel_values.append(ChannelValue(ts=queued_value.channel_value.ts,
                                                   x=queued_value.channel_value.x,
                                                   y=queued_value.channel_value.y))
            channels_with_values.append(
                ChannelIdWithValues(*channel_metadata, channel_values))

        try:
            # pylint:disable=protected-access
            self._experiment._send_channels_values(channels_with_values)
        except HTTPUnprocessableEntity as e:
            message = "Maximum storage limit reached"
            try:
                message = e.response.json()["message"]
            finally:
                _logger.warning('Failed to send channel value: %s', message)
        except (NeptuneException, IOError):
            _logger.exception('Failed to send channel value.')
