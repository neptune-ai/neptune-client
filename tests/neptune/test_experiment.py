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
from mock import call, MagicMock
from munch import Munch
from pandas.util.testing import assert_frame_equal

from neptune.experiments import Experiment
from neptune.internal.channels.channels import ChannelType, ChannelValue
from tests.neptune.random_utils import sort_df_by_columns, a_string, a_uuid_string, an_experiment_id, a_project


class TestExperiment(unittest.TestCase):

    @mock.patch('neptune.experiments.ChannelsValuesSender', return_value=mock.MagicMock())
    @mock.patch('neptune.experiments.ExecutionContext', new=mock.MagicMock)
    def test_send_metric(self, ChannelsValuesSender):
        # given
        channels_values_sender = ChannelsValuesSender.return_value
        experiment = Experiment(
            mock.MagicMock(),
            mock.MagicMock(),
            an_experiment_id(),
            a_uuid_string()
        )
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
        experiment = Experiment(
            mock.MagicMock(),
            a_project(),
            an_experiment_id(),
            a_uuid_string()
        )
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
        experiment = Experiment(
            mock.MagicMock(),
            a_project(),
            an_experiment_id(),
            a_uuid_string()
        )
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

    def test_append_tags(self):
        # given
        backend = mock.MagicMock()
        experiment = Experiment(
            backend,
            a_project(),
            an_experiment_id(),
            a_uuid_string()
        )

        # and
        def build_call(tags_list):
            return call(
                experiment=experiment,
                tags_to_add=tags_list,
                tags_to_delete=[]
            )

        # when
        experiment.append_tag('tag')
        experiment.append_tag(['tag1', 'tag2', 'tag3'])
        experiment.append_tag('tag1', 'tag2', 'tag3')
        experiment.append_tags('tag')
        experiment.append_tags(['tag1', 'tag2', 'tag3'])
        experiment.append_tags('tag1', 'tag2', 'tag3')

        # then
        backend.update_tags.assert_has_calls([
            build_call(['tag']),
            build_call(['tag1', 'tag2', 'tag3']),
            build_call(['tag1', 'tag2', 'tag3']),
            build_call(['tag']),
            build_call(['tag1', 'tag2', 'tag3']),
            build_call(['tag1', 'tag2', 'tag3'])
        ])

    def test_get_numeric_channels_values(self):
        # when
        backend = MagicMock()
        backend.get_channel_points_csv.return_value = StringIO(u'\n'.join(['0.3,2.5', '1,2']))

        experiment = MagicMock()
        experiment.id = a_string()
        experiment.internal_id = a_uuid_string()
        experiment.channels = [Munch(id=a_uuid_string(), name='epoch_loss')]
        experiment.channelsLastValues = [Munch(channelName='epoch_loss', x=2.5, y=2)]

        backend.get_experiment.return_value = experiment

        # then
        experiment = Experiment(
            backend=backend,
            project=a_project(),
            _id=a_string(),
            internal_id=a_uuid_string()
        )
        result = experiment.get_numeric_channels_values('epoch_loss')

        expected_result = pd.DataFrame({'x': [0.3, 1.0],
                                        'epoch_loss': [2.5, 2.0]}, dtype=float)

        expected_result = sort_df_by_columns(expected_result)
        result = sort_df_by_columns(result)

        assert_frame_equal(expected_result, result)


if __name__ == '__main__':
    unittest.main()
