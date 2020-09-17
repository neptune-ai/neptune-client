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

import numpy as np
import pandas as pd
from pandas.util.testing import assert_frame_equal

from neptune.utils import map_keys, map_values, as_list, align_channels_on_x, get_channel_name_stems, \
    merge_dataframes
from tests.neptune.random_utils import sort_df_by_columns


class TestMapValues(unittest.TestCase):
    def test_empty_map(self):
        # when
        mapped_dict = map_values(times_2, {})

        # then
        self.assertEqual({}, mapped_dict)

    def test_non_empty_map(self):
        # when
        mapped_dict = map_values(times_2, {'a': 2, 'b': 3})

        # then
        self.assertEqual({'a': 4, 'b': 6}, mapped_dict)


class TestMapKeys(unittest.TestCase):
    def test_empty_map(self):
        # when
        mapped_dict = map_keys(times_2, {})

        # then
        self.assertEqual({}, mapped_dict)

    def test_non_empty_map(self):
        # when
        mapped_dict = map_keys(times_2, {2: 'a', 3: 'b'})

        # then
        self.assertEqual({4: 'a', 6: 'b'}, mapped_dict)


class TestAsList(unittest.TestCase):

    def test_none(self):
        # expect
        self.assertEqual(None, as_list(None))

    def test_scalar(self):
        # expect
        self.assertEqual([1], as_list(1))

    def test_list(self):
        # expect
        self.assertEqual([2], as_list([2]))

    def test_dict(self):
        self.assertEqual([{'a': 1}], as_list({'a': 1}))


class TestAlignChannelsOnX(unittest.TestCase):

    def test_ordered_x(self):
        # when
        np.random.seed(1234)
        random_batch = np.random.random(10).tolist()
        random_epoch = np.random.random(5).tolist()
        random_odd = np.random.random(7).tolist()

        df = pd.DataFrame({'x_batch_channel': list(range(10)),
                           'y_batch_channel': random_batch,
                           'x_epoch_channel': list(range(5)) + [np.nan] * 5,
                           'y_epoch_channel': random_epoch + [np.nan] * 5,
                           'x_odd_channel': list(range(7)) + [np.nan] * 3,
                           'y_odd_channel': random_odd + [np.nan] * 3}, dtype=float)

        expected_result = pd.DataFrame({'x': list(range(10)),
                                        'batch_channel': random_batch,
                                        'epoch_channel': random_epoch + [np.nan] * 5,
                                        'odd_channel': random_odd + [np.nan] * 3}, dtype=float)
        expected_result = sort_df_by_columns(expected_result)

        # then
        result = align_channels_on_x(df)
        result = sort_df_by_columns(result)

        assert_frame_equal(result, expected_result)

    def test_shuffled_x(self):
        # when

        batch_x = [4, 2, 10, 28]
        epoch_x = [np.nan] + [1, 2, 21]
        odd_x = [21, 10, 15, 4]
        detached_x = [3, 5, 9] + [np.nan]

        batch_y = [7, 2, 9, 1]
        epoch_y = [np.nan, 3, 5, 9]
        odd_y = [21, 15, 4, 3]
        detached_y = [1, 5, 12, np.nan]

        df = pd.DataFrame({'x_batch_channel': batch_x, 'y_batch_channel': batch_y,
                           'x_epoch_channel': epoch_x, 'y_epoch_channel': epoch_y,
                           'x_odd_channel': odd_x, 'y_odd_channel': odd_y,
                           'x_detached_channel': detached_x, 'y_detached_channel': detached_y}, dtype=float)

        expected_result = pd.DataFrame({'x': [1, 2, 3, 4, 5, 9, 10, 15, 21, 28],
                                        'batch_channel': [np.nan, 2, np.nan, 7, np.nan, np.nan, 9, np.nan, np.nan, 1],
                                        'epoch_channel': [3, 5, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, 9,
                                                          np.nan],
                                        'odd_channel': [np.nan, np.nan, np.nan, 3, np.nan, np.nan, 15, 4, 21, np.nan],
                                        'detached_channel': [np.nan, np.nan, 1, np.nan, 5, 12, np.nan, np.nan, np.nan,
                                                             np.nan]}, dtype=float)
        expected_result = sort_df_by_columns(expected_result)

        # then
        result = align_channels_on_x(df)
        result = sort_df_by_columns(result)

        assert_frame_equal(result, expected_result)

    def test_fraction_x(self):
        # when

        batch_x = [1.2, 0.3, 0.9, 123.4]
        epoch_x = [np.nan] + [1.7, 2.9, 4.5]

        batch_y = [7.3, 2.1, 9.5, 1.2]
        epoch_y = [np.nan, 0.35, 5.4, 0.9]

        df = pd.DataFrame({'x_batch_channel': batch_x, 'y_batch_channel': batch_y,
                           'x_epoch_channel': epoch_x, 'y_epoch_channel': epoch_y}, dtype=float)

        expected_result = pd.DataFrame({'x': [0.3, 0.9, 1.2, 1.7, 2.9, 4.5, 123.4],
                                        'batch_channel': [2.1, 9.5, 7.3, np.nan, np.nan, np.nan, 1.2],
                                        'epoch_channel': [np.nan, np.nan, np.nan, 0.35, 5.4, 0.9, np.nan]}, dtype=float)
        expected_result = sort_df_by_columns(expected_result)

        # then
        result = align_channels_on_x(df)
        result = sort_df_by_columns(result)

        assert_frame_equal(result, expected_result)


class TestGetChannelNameStems(unittest.TestCase):

    def setUp(self):
        np.random.seed(1234)
        self.df = pd.DataFrame({'x_batch_channel': list(range(10)),
                                'y_batch_channel': np.random.random(10),
                                'x_epoch_channel': list(range(5)) + [np.nan] * 5,
                                'y_epoch_channel': np.random.random(10),
                                'x_odd_channel': list(range(7)) + [np.nan] * 3,
                                'y_odd_channel': np.random.random(10)})

    def test_names(self):
        correct_names = set(['epoch_channel', 'batch_channel', 'odd_channel'])
        self.assertEqual(set(get_channel_name_stems(self.df)), correct_names)


class TestMergeDataFrames(unittest.TestCase):

    def setUp(self):
        np.random.seed(1234)
        random_df1 = np.random.random(10).tolist()
        self.df1 = pd.DataFrame({'x': list(range(10)),
                                 'y1': random_df1})

        random_df2 = np.random.random(3).tolist()
        self.df2 = pd.DataFrame({'x': list(range(3)),
                                 'y2': random_df2})

        random_df3 = np.random.random(6).tolist()
        self.df3 = pd.DataFrame({'x': list(range(6)),
                                 'y3': random_df3})

        df_merged_outer = pd.DataFrame({'x': list(range(10)),
                                        'y1': random_df1,
                                        'y2': random_df2 + [np.nan] * 7,
                                        'y3': random_df3 + [np.nan] * 4})

        self.df_merged_outer = sort_df_by_columns(df_merged_outer)

    def test_merge_outer(self):
        result = merge_dataframes([self.df1, self.df2, self.df3], on='x', how='outer')
        result = sort_df_by_columns(result)
        assert_frame_equal(result, self.df_merged_outer)


class TestSortDfByColumns(unittest.TestCase):

    def test_letters_and_numbers(self):
        sorted_df = pd.DataFrame(columns=['1', '2', '3', 'a', 'b', 'c', 'd', ])
        shuffled_df = pd.DataFrame(columns=['c', 'a', '1', 'd', '3', '2', 'b'])

        assert_frame_equal(sort_df_by_columns(shuffled_df), sorted_df)


def times_2(x):
    return x * 2


if __name__ == '__main__':
    unittest.main()
