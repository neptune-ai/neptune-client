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

from neptune.exceptions import InvalidNotebookPath, FileNotFound, NotAFile
from neptune.git_info import GitInfo

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


def validate_notebook_path(path):
    if not path.endswith(".ipynb"):
        raise InvalidNotebookPath(path)

    if not os.path.exists(path):
        raise FileNotFound(path)

    if not os.path.isfile(path):
        raise NotAFile(path)


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


def is_nan_or_inf(value):
    inf = 1e5000
    return (is_float(value) and value != value) or value == inf or value == -inf


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


def discover_git_repo_location():
    import __main__

    if hasattr(__main__, '__file__'):
        return os.path.dirname(os.path.abspath(__main__.__file__))
    return None


def get_git_info(repo_path=None):
    """Retrieve information about git repository.

    If attempt fails, ``None`` will be returned.

    Args:
        repo_path (:obj:`str`, optional, default is ``None``):

            | Path to the repository from which extract information about git.
            | If ``None`` is passed, calling ``get_git_info`` is equivalent to calling
              ``git.Repo(search_parent_directories=True)``.
              Check `GitPython <https://gitpython.readthedocs.io/en/stable/reference.html#git.repo.base.Repo>`_
              docs for more information.

    Returns:
        :class:`~neptune.git_info.GitInfo` - An object representing information about git repository.

    Examples:

        .. code:: python3

            # Get git info from the current directory
            git_info = get_git_info('.')

    """
    try:
        import git

        repo = git.Repo(repo_path, search_parent_directories=True)

        commit = repo.head.commit

        return GitInfo(
            commit_id=commit.hexsha,
            message=commit.message,
            author_name=commit.author.name,
            author_email=commit.author.email,
            commit_date=commit.committed_datetime,
            repository_dirty=repo.is_dirty()
        )
    except:  # pylint: disable=bare-except
        return None
