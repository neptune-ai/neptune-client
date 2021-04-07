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
import json
import logging
import math
import os
import re
import sys
import time
from json.decoder import JSONDecodeError

import click
import numpy as np
import pandas as pd
import requests
from bravado.exception import (
    BravadoConnectionError, BravadoTimeoutError, HTTPBadGateway, HTTPBadRequest,
    HTTPForbidden, HTTPGatewayTimeout, HTTPInternalServerError, HTTPRequestTimeout,
    HTTPServerError, HTTPServiceUnavailable, HTTPUnauthorized
)

from neptune import envs
from neptune.api_exceptions import ConnectionLost, Forbidden, SSLError, ServerError, Unauthorized
from neptune.exceptions import (
    FileNotFound, InvalidNotebookPath, NeptuneIncorrectProjectQualifiedNameException,
    NeptuneMissingProjectQualifiedNameException,
    NotADirectory, NotAFile, ProjectMigratedToNewStructure
)
from neptune.git_info import GitInfo
from neptune.patterns import PROJECT_QUALIFIED_NAME_PATTERN

_logger = logging.getLogger(__name__)

IS_WINDOWS = sys.platform == 'win32'
IS_MACOS = sys.platform == 'darwin'

MIGRATION_IN_PROGRESS = 'PROJECT_MIGRATION_IN_PROGRESS'
MIGRATION_FINISHED = 'NEW_PROJECT_VERSION'


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


def assure_directory_exists(destination_dir):
    """Check if `destination_dir` DIRECTORY exists, or creates one"""
    if not destination_dir:
        destination_dir = os.getcwd()

    if not os.path.exists(destination_dir):
        os.makedirs(destination_dir)
    elif not os.path.isdir(destination_dir):
        raise NotADirectory(destination_dir)

    return destination_dir


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
    with open(filename) as f:
        for line in f:
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


def parse_error_type(ex):
    try:
        error_data = json.loads(ex.response.text)
        return error_data.get("errorType")
    except JSONDecodeError:
        return None


migration_reported = False


def print_migration_in_progress_message():
    # pylint: disable=global-statement
    global migration_reported
    migration_reported = True
    click.echo(click.style("""
NOTICE: Attention needed
Your Neptune project is currently being migrated to the new structure. 
The execution of the script is paused until migration is finished. 
It will automatically resume once it's finished.
Depending on the number of runs (experiments) in the project
the migration process can take from few minutes to up to few hours.
If you think this operation takes too long please contact Neptune support:
    - https://neptune.ai/?chat-with-us
""", fg='yellow'))


def with_api_exceptions_handler(func):
    def wrapper(*args, **kwargs):
        # pylint: disable=global-statement
        global migration_reported
        retries = 11
        retry = 0
        while retry < retries:
            try:
                return func(*args, **kwargs)
            except requests.exceptions.SSLError:
                raise SSLError()
            except HTTPBadRequest as e:
                if parse_error_type(e) == MIGRATION_FINISHED:
                    raise ProjectMigratedToNewStructure()
                else:
                    raise
            except HTTPServiceUnavailable as e:
                if parse_error_type(e) == MIGRATION_IN_PROGRESS:
                    retry = min(retry + 1, 8)
                    if not migration_reported:
                        print_migration_in_progress_message()
                    time.sleep(2 ** retry)
                    continue
                else:
                    if retry >= 6:
                        _logger.warning(
                            'Experiencing connection interruptions. Reestablishing communication with Neptune.')
                    time.sleep(2 ** retry)
                    retry += 1
                    continue
            except (BravadoConnectionError, BravadoTimeoutError,
                    requests.exceptions.ConnectionError, requests.exceptions.Timeout,
                    HTTPRequestTimeout, HTTPGatewayTimeout, HTTPBadGateway):
                if retry >= 6:
                    _logger.warning('Experiencing connection interruptions. Reestablishing communication with Neptune.')
                time.sleep(2 ** retry)
                retry += 1
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
                if status_code == HTTPBadRequest.status_code and parse_error_type(e) == MIGRATION_FINISHED:
                    raise ProjectMigratedToNewStructure()
                elif status_code == HTTPServiceUnavailable.status_code and parse_error_type(e) == MIGRATION_IN_PROGRESS:
                    retry = min(retry + 1, 8)
                    if not migration_reported:
                        print_migration_in_progress_message()
                    time.sleep(2 ** retry)
                    continue
                elif status_code in (
                        HTTPBadGateway.status_code,
                        HTTPServiceUnavailable.status_code,
                        HTTPGatewayTimeout.status_code):
                    if retry >= 6:
                        _logger.warning(
                            'Experiencing connection interruptions. Reestablishing communication with Neptune.')
                    time.sleep(2 ** retry)
                    retry += 1
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


def assure_project_qualified_name(project_qualified_name):
    project_qualified_name = project_qualified_name or os.getenv(envs.PROJECT_ENV_NAME)

    if not project_qualified_name:
        raise NeptuneMissingProjectQualifiedNameException()
    if not re.match(PROJECT_QUALIFIED_NAME_PATTERN, project_qualified_name):
        raise NeptuneIncorrectProjectQualifiedNameException(project_qualified_name)

    return project_qualified_name


class NoopObject(object):

    def __getattr__(self, name):
        return self

    def __getitem__(self, key):
        return self

    def __call__(self, *args, **kwargs):
        return self

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
