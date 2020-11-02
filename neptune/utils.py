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
import glob as globlib
import logging
import os
import sys
import time

import math
import numpy as np
import pandas as pd
import requests
from bravado.exception import BravadoConnectionError, BravadoTimeoutError, HTTPForbidden, \
    HTTPInternalServerError, HTTPServerError, HTTPUnauthorized, HTTPServiceUnavailable, HTTPRequestTimeout, \
    HTTPGatewayTimeout, HTTPBadGateway

from neptune.api_exceptions import ConnectionLost, Forbidden, ServerError, Unauthorized, SSLError
from neptune.exceptions import InvalidNotebookPath, FileNotFound, NotAFile
from neptune.git_info import GitInfo

_logger = logging.getLogger(__name__)

IS_WINDOWS = sys.platform == 'win32'
IS_MACOS = sys.platform == 'darwin'


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
    return math.isnan(value) or math.isinf(value)


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
    # pylint:disable=bad-option-value,import-outside-toplevel
    import __main__

    if hasattr(__main__, '__file__'):
        return os.path.dirname(os.path.abspath(__main__.__file__))
    return None


def update_session_proxies(session, proxies):
    if proxies is not None:
        try:
            session.proxies.update(proxies)
        except (TypeError, ValueError):
            raise ValueError("Wrong proxies format: {}".format(proxies))


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
        # pylint:disable=bad-option-value,import-outside-toplevel
        import git

        repo = git.Repo(repo_path, search_parent_directories=True)

        commit = repo.head.commit

        active_branch = ""

        try:
            active_branch = repo.active_branch.name
        except TypeError as e:
            if str(e.args[0]).startswith("HEAD is a detached symbolic reference as it points to"):
                active_branch = "Detached HEAD"

        remote_urls = [remote.url for remote in repo.remotes]

        return GitInfo(
            commit_id=commit.hexsha,
            message=commit.message,
            author_name=commit.author.name,
            author_email=commit.author.email,
            commit_date=commit.committed_datetime,
            repository_dirty=repo.is_dirty(untracked_files=True),
            active_branch=active_branch,
            remote_urls=remote_urls
        )
    except:  # pylint: disable=bare-except
        return None


def with_api_exceptions_handler(func):
    def wrapper(*args, **kwargs):
        for retry in range(0, 11):
            try:
                return func(*args, **kwargs)
            except requests.exceptions.SSLError:
                raise SSLError()
            except (BravadoConnectionError, BravadoTimeoutError,
                    requests.exceptions.ConnectionError, requests.exceptions.Timeout,
                    HTTPRequestTimeout, HTTPServiceUnavailable, HTTPGatewayTimeout, HTTPBadGateway):
                if retry >= 6:
                    _logger.warning('Experiencing connection interruptions. Reestablishing communication with Neptune.')
                time.sleep(2 ** retry)
                continue
            except HTTPServerError:
                raise ServerError()
            except HTTPUnauthorized:
                raise Unauthorized()
            except HTTPForbidden:
                raise Forbidden()
            except requests.exceptions.RequestException as e:
                if e.response is None:
                    raise
                status_code = e.response.status_code
                if status_code in (
                        HTTPBadGateway.status_code,
                        HTTPServiceUnavailable.status_code,
                        HTTPGatewayTimeout.status_code):
                    if retry >= 6:
                        _logger.warning(
                            'Experiencing connection interruptions. Reestablishing communication with Neptune.')
                    time.sleep(2 ** retry)
                    continue
                elif status_code >= HTTPInternalServerError.status_code:
                    raise ServerError()
                elif status_code == HTTPUnauthorized.status_code:
                    raise Unauthorized()
                elif status_code == HTTPForbidden.status_code:
                    raise Forbidden()
                else:
                    raise
        raise ConnectionLost()

    return wrapper


def glob(pathname):
    # pylint: disable=unexpected-keyword-arg
    if sys.version_info.major < 3 or (sys.version_info.major == 3 and sys.version_info.minor < 5):
        return globlib.glob(pathname)
    else:
        return globlib.glob(pathname, recursive=True)


def is_ipython():
    try:
        # pylint:disable=bad-option-value,import-outside-toplevel
        import IPython
        ipython = IPython.core.getipython.get_ipython()
        return ipython is not None
    except ImportError:
        return False
