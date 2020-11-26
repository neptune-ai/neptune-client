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
from typing import Optional

from neptune.alpha.envs import PROJECT_ENV_NAME
from neptune.alpha.exceptions import MissingProject
from neptune.alpha.internal.backends.hosted_neptune_backend import HostedNeptuneBackend
from neptune.alpha.internal.credentials import Credentials
from neptune.alpha.internal.utils import verify_type
from neptune.alpha.leaderboard import Leaderboard
from neptune.alpha.project import Project
from neptune.alpha.version import version as parsed_version

__version__ = str(parsed_version)

_logger = logging.getLogger(__name__)


def get_table(page: int = 0, page_size: int = 100) -> Leaderboard:
    return get_project().get_table(page, page_size)


def get_project(name: Optional[str] = None) -> Project:
    verify_type("name", name, (str, type(None)))

    if not name:
        name = os.getenv(PROJECT_ENV_NAME)
    if not name:
        raise MissingProject()

    backend = HostedNeptuneBackend(Credentials())

    project_obj = backend.get_project(name)

    return Project(project_obj.uuid, backend)
