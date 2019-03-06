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

import functools
import os
import sys

import numpy as np
import pandas as pd

IS_WINDOWS = hasattr(sys, 'getwindowsversion')


def map_values(f_value, dictionary):
    return dict(
        (k, f_value(v)) for k, v in dictionary.items()
    )


def map_keys(f_key, dictionary):
    return dict(
        (f_key(k), v) for k, v in dictionary.items()
    )


def as_list(value):
    if value is None or isinstance(value, list):
        return value
    else:
        return [value]


def align_channels_on_x(dataframe):
    channel_dfs, common_x = _split_df_by_stems(dataframe)
    return merge_dataframes([common_x] + channel_dfs, on='x', how='outer')


def get_channel_name_stems(columns):
    return list(set([col[2:] for col in columns]))


def merge_dataframes(dataframes, on, how='outer'):
    merged_df = functools.reduce(lambda left, right: \
                                     pd.merge(left, right, on=on, how=how), dataframes)
    return merged_df


def is_float(value):
    try:
        _ = float(value)
    except ValueError:
        return False
    else:
        return True


def file_contains(filename, text):
    for line in open(filename):
        if text in line:
            return True
    return False


def in_docker():
    cgroup_file = '/proc/self/cgroup'
    return os.path.exists('./dockerenv') or (os.path.exists(cgroup_file) and file_contains(cgroup_file, text='docker'))

def is_notebook():
    try:
        # pylint: disable=pointless-statement,undefined-variable
        get_ipython
        return True
    except Exception:
        return False

def _split_df_by_stems(df):
    channel_dfs, x_vals = [], []
    for stem in get_channel_name_stems(df.columns):
        channel_df = df[['x_{}'.format(stem), 'y_{}'.format(stem)]]
        channel_df.columns = ['x', stem]
        channel_df = channel_df.dropna()
        channel_dfs.append(channel_df)
        x_vals.extend(channel_df['x'].tolist())
    common_x = pd.DataFrame({'x': np.unique(x_vals)}, dtype=float)
    return channel_dfs, common_x
