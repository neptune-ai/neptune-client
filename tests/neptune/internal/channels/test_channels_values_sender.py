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
from __future__ import unicode_literals

import time
import unittest

import mock

from neptune.internal.channels.channels import ChannelIdWithValues, ChannelValue, ChannelType
from neptune.internal.channels.channels_values_sender import ChannelsValuesSender, ChannelsValuesSendingThread
from tests.neptune.experiments_object_factory import a_channel


class TestChannelsValuesSender(unittest.TestCase):
    _TS = time.time()

    _EXPERIMENT = mock.MagicMock()

    _NUMERIC_CHANNEL = a_channel()
    _NUMERIC_CHANNEL.update(name=u'1numeric', channelType=ChannelType.NUMERIC.value)

    _TEXT_CHANNEL = a_channel()
    _TEXT_CHANNEL.update(name=u'2text', channelType=ChannelType.TEXT.value)

    _IMAGE_CHANNEL = a_channel()
    _IMAGE_CHANNEL.update(name=u'3image', channelType=ChannelType.IMAGE.value)

    _CHANNELS = {c.name: c for c in [_NUMERIC_CHANNEL, _TEXT_CHANNEL, _IMAGE_CHANNEL, a_channel()]}

    # pylint: disable=protected-access
    _BATCH_SIZE = ChannelsValuesSendingThread._MAX_VALUES_BATCH_LENGTH
    # pylint: disable=protected-access
    _IMAGES_BATCH_SIZE = ChannelsValuesSendingThread._MAX_IMAGE_VALUES_BATCH_LENGTH

    def setUp(self):
        # pylint: disable=protected-access
        self._EXPERIMENT._get_channels.return_value = self._CHANNELS

    def tearDown(self):
        self._EXPERIMENT.reset_mock()

    def test_send_values_on_join(self):
        # given
        channel_value = ChannelValue(x=1, y="value", ts=self._TS)
        # and
        channels_values_sender = ChannelsValuesSender(experiment=self._EXPERIMENT)

        # when
        channels_values_sender.send(self._TEXT_CHANNEL.name, self._TEXT_CHANNEL.channelType, channel_value)
        # and
        channels_values_sender.join()

        # then
        # pylint: disable=protected-access
        self._EXPERIMENT._send_channels_values.assert_called_with([ChannelIdWithValues(
            channel_id=self._TEXT_CHANNEL.id,
            channel_values=[channel_value]
        )])

    def test_send_values_in_multiple_batches(self):
        # given
        channels_values = [ChannelValue(x=i, y="value{}".format(i), ts=self._TS + i)
                           for i in range(0, self._BATCH_SIZE * 3)]
        # and
        channels_values_sender = ChannelsValuesSender(experiment=self._EXPERIMENT)

        # when
        for channel_value in channels_values:
            channels_values_sender.send(self._TEXT_CHANNEL.name, self._TEXT_CHANNEL.channelType, channel_value)
        # and
        channels_values_sender.join()

        # then
        # pylint: disable=protected-access
        self.assertEqual(self._EXPERIMENT._send_channels_values.mock_calls, [
            mock.call._send_channels_values([ChannelIdWithValues(
                channel_id=self._TEXT_CHANNEL.id,
                channel_values=channels_values[0:self._BATCH_SIZE]
            )]),
            mock.call._send_channels_values([ChannelIdWithValues(
                channel_id=self._TEXT_CHANNEL.id,
                channel_values=channels_values[self._BATCH_SIZE:self._BATCH_SIZE * 2]
            )]),
            mock.call._send_channels_values([ChannelIdWithValues(
                channel_id=self._TEXT_CHANNEL.id,
                channel_values=channels_values[self._BATCH_SIZE * 2:self._BATCH_SIZE * 3]
            )])
        ])

    def test_send_images_in_smaller_batches(self):
        # and
        channels_values = [ChannelValue(x=i, y="base64Image==", ts=self._TS + i)
                           for i in range(0, self._IMAGES_BATCH_SIZE * 3)]
        # and
        channels_values_sender = ChannelsValuesSender(experiment=self._EXPERIMENT)

        # when
        for channel_value in channels_values:
            channels_values_sender.send(self._IMAGE_CHANNEL.name, self._IMAGE_CHANNEL.channelType, channel_value)
        # and
        channels_values_sender.join()

        # then
        # pylint: disable=protected-access
        self.assertEqual(self._EXPERIMENT._send_channels_values.mock_calls, [
            mock.call._send_channels_values([ChannelIdWithValues(
                channel_id=self._IMAGE_CHANNEL.id,
                channel_values=channels_values[0:self._IMAGES_BATCH_SIZE]
            )]),
            mock.call._send_channels_values([ChannelIdWithValues(
                channel_id=self._IMAGE_CHANNEL.id,
                channel_values=channels_values[self._IMAGES_BATCH_SIZE:self._IMAGES_BATCH_SIZE * 2]
            )]),
            mock.call._send_channels_values([ChannelIdWithValues(
                channel_id=self._IMAGE_CHANNEL.id,
                channel_values=channels_values[self._IMAGES_BATCH_SIZE * 2:self._IMAGES_BATCH_SIZE * 3]
            )])
        ])

    def test_send_values_from_multiple_channels(self):
        # given
        numeric_values = [ChannelValue(x=i, y=i, ts=self._TS + i)
                          for i in range(0, 3)]

        text_values = [ChannelValue(x=i, y="text", ts=self._TS + i)
                       for i in range(0, 3)]

        image_values = [ChannelValue(x=i, y="base64Image==", ts=self._TS + i)
                        for i in range(0, 3)]
        # and
        channels_values_sender = ChannelsValuesSender(experiment=self._EXPERIMENT)

        # when
        for channel_value in numeric_values:
            channels_values_sender.send(self._NUMERIC_CHANNEL.name, self._NUMERIC_CHANNEL.channelType, channel_value)

        for channel_value in text_values:
            channels_values_sender.send(self._TEXT_CHANNEL.name, self._TEXT_CHANNEL.channelType, channel_value)

        for channel_value in image_values:
            channels_values_sender.send(self._IMAGE_CHANNEL.name, self._IMAGE_CHANNEL.channelType, channel_value)

        # and
        channels_values_sender.join()

        # then
        # pylint: disable=protected-access
        (args, _) = self._EXPERIMENT._send_channels_values.call_args
        self.assertEqual(len(args), 1)
        self.assertEqual(sorted(args[0]), sorted([
            ChannelIdWithValues(
                channel_id=self._NUMERIC_CHANNEL.id,
                channel_values=numeric_values
            ),
            ChannelIdWithValues(
                channel_id=self._TEXT_CHANNEL.id,
                channel_values=text_values
            ),
            ChannelIdWithValues(
                channel_id=self._IMAGE_CHANNEL.id,
                channel_values=image_values
            )]))

    __TIMEOUT = 0.1

    @mock.patch('neptune.internal.channels.channels_values_sender.ChannelsValuesSendingThread._SLEEP_TIME', __TIMEOUT)
    def test_send_when_waiting_for_next_value_timed_out(self):
        # given
        numeric_values = [ChannelValue(x=i, y=i, ts=self._TS + i)
                          for i in range(0, 3)]

        # and
        channels_values_sender = ChannelsValuesSender(experiment=self._EXPERIMENT)

        # when
        for channel_value in numeric_values:
            channels_values_sender.send(self._NUMERIC_CHANNEL.name, self._NUMERIC_CHANNEL.channelType, channel_value)

        # and
        time.sleep(self.__TIMEOUT * 2)

        # then
        # pylint: disable=protected-access
        self._EXPERIMENT._send_channels_values.assert_called_with([ChannelIdWithValues(
            channel_id=self._NUMERIC_CHANNEL.id,
            channel_values=numeric_values
        )])

        # and
        self._EXPERIMENT._send_channels_values.reset_mock()
        channels_values_sender.join()
        # and
        self._EXPERIMENT._send_channels_values.assert_not_called()
