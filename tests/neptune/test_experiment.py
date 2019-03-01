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
from io import StringIO

import pandas as pd
from bunch import Bunch
from mock import MagicMock
from pandas.util.testing import assert_frame_equal

from neptune.experiments import Experiment
from tests.neptune.random_utils import sort_df_by_columns, a_string, a_uuid_string


class TestExperiment(unittest.TestCase):

    def test_get_numeric_channels_values(self):
        # when
        client = MagicMock()
        client.get_channel_points_csv.return_value = StringIO(u'\n'.join(['0.3,2.5', '1,2']))

        experiment = MagicMock()
        experiment.id = a_string()
        experiment.internal_id = a_uuid_string()
        experiment.channels = [Bunch(id=a_uuid_string(), name='epoch_loss')]
        experiment.channelsLastValues = [Bunch(channelName='epoch_loss', x=2.5, y=2)]

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


if __name__ == '__main__':
    unittest.main()
