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

import unittest
from datetime import datetime

import mock

from neptune.internal.streams.channel_writer import ChannelWriter


class TestChannelWriter(unittest.TestCase):

    def test_write_data_to_channel_writer(self):
        # given
        experiment = mock.MagicMock()
        experiment.get_system_properties.return_value = {"created": datetime.now()}
        channel_name = 'a channel name'
        writer = ChannelWriter(experiment, channel_name)

        # when
        writer.write('some\ndata')

        # then
        # pylint: disable=protected-access
        experiment._channels_values_sender.send.assert_called_once()

    @mock.patch('neptune.internal.streams.channel_writer.datetime')
    def test_write_data_with_low_resolution_datetime_now(self, dt):
        # given
        experiment = mock.MagicMock()
        experiment.get_system_properties.return_value = {"created": datetime(2022, 2, 2, 2, 2, 2, 2)}
        channel_name = 'a channel name'
        writer = ChannelWriter(experiment, channel_name)

        # and
        dt.now.return_value = datetime(2022, 2, 2, 2, 2, 2, 3)

        # when
        writer.write('text1\ntext2\n')

        # then
        # pylint: disable=protected-access
        x_to_text = self._extract_x_to_text_from_calls(experiment._channels_values_sender.send.call_args_list)
        self.assertEqual(x_to_text, {0.001: 'text1', 0.002: 'text2'})

    @mock.patch('neptune.internal.streams.channel_writer.datetime')
    def test_write_data_with_high_resolution_datetime_now(self, dt):
        # given
        experiment = mock.MagicMock()
        experiment.get_system_properties.return_value = {"created": datetime(2022, 2, 2, 2, 2, 2, 2)}
        channel_name = 'a channel name'
        writer = ChannelWriter(experiment, channel_name)

        # when
        dt.now.return_value = datetime(2022, 2, 2, 2, 2, 2, 4)
        writer.write('text1\n')
        dt.now.return_value = datetime(2022, 2, 2, 2, 2, 2, 5)
        writer.write('text2\n')

        # then
        # pylint: disable=protected-access
        x_to_text = self._extract_x_to_text_from_calls(experiment._channels_values_sender.send.call_args_list)
        self.assertEqual(x_to_text, {0.002: 'text1', 0.003: 'text2'})

    @staticmethod
    def _extract_x_to_text_from_calls(calls):
        channel_values = [kwargs['channel_value'] for (_, kwargs) in calls]
        return dict((v.x, v.y['text_value']) for v in channel_values)


if __name__ == '__main__':
    unittest.main()
