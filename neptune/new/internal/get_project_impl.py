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
