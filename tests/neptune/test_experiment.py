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
import base64
import random
import time
import unittest
from io import StringIO

import mock
import pandas as pd
from mock import MagicMock
from munch import Munch
from pandas.util.testing import assert_frame_equal

from neptune.exceptions import NoExperimentContext
from neptune.experiments import Experiment, push_new_experiment, get_current_experiment, pop_stopped_experiment
from neptune.internal.channels.channels import ChannelType, ChannelValue
from tests.neptune.random_utils import sort_df_by_columns, a_string, a_uuid_string, an_experiment_id, \
    a_project_qualified_name


class TestExperiment(unittest.TestCase):

    @mock.patch('neptune.experiments.ChannelsValuesSender', return_value=mock.MagicMock())
    @mock.patch('neptune.experiments.ExecutionContext', new=mock.MagicMock)
    def test_send_metric(self, ChannelsValuesSender):
        # given
        channels_values_sender = ChannelsValuesSender.return_value
        experiment = Experiment(mock.MagicMock(), an_experiment_id(), a_uuid_string(), a_project_qualified_name())
        channel_value = ChannelValue(
            x=random.randint(0, 100),
            y=dict(numeric_value=random.randint(0, 100)),
            ts=time.time()
        )

        # when
        experiment.send_metric('loss', channel_value.x, channel_value.y['numeric_value'], channel_value.ts)

        # then
        channels_values_sender.send.assert_called_with('loss', ChannelType.NUMERIC.value, channel_value)

    @mock.patch('neptune.experiments.ChannelsValuesSender', return_value=mock.MagicMock())
    @mock.patch('neptune.experiments.ExecutionContext', new=mock.MagicMock)
    def test_send_text(self, ChannelsValuesSender):
        # given
        channels_values_sender = ChannelsValuesSender.return_value
        experiment = Experiment(mock.MagicMock(), an_experiment_id(), a_uuid_string(), a_project_qualified_name())
        channel_value = ChannelValue(
            x=random.randint(0, 100),
            y=dict(text_value=a_string()),
            ts=time.time()
        )

        # when
        experiment.send_text('stdout', channel_value.x, channel_value.y['text_value'], channel_value.ts)

        # then
        channels_values_sender.send.assert_called_with('stdout', ChannelType.TEXT.value, channel_value)

    @mock.patch('neptune.experiments.get_image_content', return_value=b'content')
    @mock.patch('neptune.experiments.ChannelsValuesSender', return_value=mock.MagicMock())
    @mock.patch('neptune.experiments.ExecutionContext', new=mock.MagicMock)
    def test_send_image(self, ChannelsValuesSender, content):
        # given
        channels_values_sender = ChannelsValuesSender.return_value
        experiment = Experiment(mock.MagicMock(), an_experiment_id(), a_uuid_string(), a_project_qualified_name())
        image_value = dict(
            name=a_string(),
            description=a_string(),
            data=base64.b64encode(content()).decode('utf-8')
        )
        channel_value = ChannelValue(
            x=random.randint(0, 100),
            y=dict(image_value=image_value),
            ts=time.time()
        )

        # when
        experiment.send_image(
            'errors',
            channel_value.x,
            '/tmp/img.png',
            image_value['name'],
            image_value['description'],
            channel_value.ts
        )

        # then
        channels_values_sender.send.assert_called_with('errors', ChannelType.IMAGE.value, channel_value)

    def test_get_numeric_channels_values(self):
        # when
        client = MagicMock()
        client.get_channel_points_csv.return_value = StringIO(u'\n'.join(['0.3,2.5', '1,2']))

        experiment = MagicMock()
        experiment.id = a_string()
        experiment.internal_id = a_uuid_string()
        experiment.channels = [Munch(id=a_uuid_string(), name='epoch_loss')]
        experiment.channelsLastValues = [Munch(channelName='epoch_loss', x=2.5, y=2)]

        client.get_experiment.return_value = experiment

        # then
        experiment = Experiment(
            client=client,
            _id=a_string(),
            internal_id=a_uuid_string(),
            project_full_id="test/sandbox"
        )
        result = experiment.get_numeric_channels_values('epoch_loss')

        expected_result = pd.DataFrame({'x': [0.3, 1.0],
                                        'epoch_loss': [2.5, 2.0]}, dtype=float)

        expected_result = sort_df_by_columns(expected_result)
        result = sort_df_by_columns(result)

        assert_frame_equal(expected_result, result)

    def test_get_current_experiment_from_stack(self):
        # given
        experiment = Munch(internal_id=a_uuid_string())

        # when
        push_new_experiment(experiment)

        # then
        self.assertEqual(get_current_experiment(), experiment)

    def test_pop_experiment_from_stack(self):
        # given
        first_experiment = Munch(internal_id=a_uuid_string())
        second_experiment = Munch(internal_id=a_uuid_string())
        # and
        push_new_experiment(first_experiment)

        # when
        push_new_experiment(second_experiment)

        # then
        self.assertEqual(get_current_experiment(), second_experiment)
        # and
        self.assertEqual(pop_stopped_experiment(), second_experiment)
        # and
        self.assertEqual(get_current_experiment(), first_experiment)

    def test_empty_stack(self):
        # when
        self.assertIsNone(pop_stopped_experiment())
        # and
        with self.assertRaises(NoExperimentContext):
            get_current_experiment()


if __name__ == '__main__':
    unittest.main()
