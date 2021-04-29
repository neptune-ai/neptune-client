#
# Copyright (c) 2020, Neptune Labs Sp. z o.o.
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
import logging
import os
import re
from typing import Optional

from neptune.patterns import PROJECT_QUALIFIED_NAME_PATTERN

from neptune.new.envs import PROJECT_ENV_NAME
from neptune.new.exceptions import NeptuneIncorrectProjectNameException, NeptuneMissingProjectNameException
from neptune.new.internal.backends.hosted_neptune_backend import HostedNeptuneBackend
from neptune.new.internal.credentials import Credentials
from neptune.new.internal.utils import verify_type
from neptune.new.project import Project
from neptune.new.version import version as parsed_version

__version__ = str(parsed_version)

_logger = logging.getLogger(__name__)


def get_project(name: Optional[str] = None, api_token: Optional[str] = None) -> Project:
    """Get a project with given `name`.

    Args:
        name(str, optional): Name of a project in a form of namespace/project_name. Defaults to `None`.
            If None, the value of `NEPTUNE_PROJECT` environment variable will be taken.
        api_token(str, optional): User’s API token. Defaults to `None`.
            If None, the value of `NEPTUNE_API_TOKEN` environment variable will be taken.
            .. note::
                It is strongly recommended to use `NEPTUNE_API_TOKEN` environment variable rather than placing your
                API token in plain text in your source code.

    Returns:
        ``Project``: object that can be used to interact with the project as a whole like fetching data from Runs table.

    Examples:

        >>> import neptune.new as neptune

        >>> # Fetch project 'jack/sandbox'
        ... project = neptune.get_project(name='jack/sandbox')

        >>> # Fetch all Runs metadata as Pandas DataFrame
        ... runs_table_df = project.fetch_runs_table().to_pandas()

    You may also want to check `get_project docs page`_.

    .. _get_project docs page:
       https://docs.neptune.ai/api-reference/neptune#get_project
    """
    verify_type("name", name, (str, type(None)))
    verify_type("api_token", api_token, (str, type(None)))

    if not name:
        name = os.getenv(PROJECT_ENV_NAME)
    if not name:
        raise NeptuneMissingProjectNameException()
    if not re.match(PROJECT_QUALIFIED_NAME_PATTERN, name):
        raise NeptuneIncorrectProjectNameException(name)

    backend = HostedNeptuneBackend(Credentials(api_token=api_token))

    project_obj = backend.get_project(name)

    return Project(project_obj.uuid, backend)
