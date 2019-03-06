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

from mock import MagicMock

from neptune.internal.streams.channel_writer import ChannelWriter


class TestChannelWriter(unittest.TestCase):

    def test_write_data_to_channel_writer(self):
        # given
        experiment = MagicMock()
        channel_name = 'a channel name'
        writer = ChannelWriter(experiment, channel_name)

        writer.write('some\ndata')

        experiment.send_text.assert_called_once()


if __name__ == '__main__':
    unittest.main()
