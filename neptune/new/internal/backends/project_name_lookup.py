#
# Copyright (c) 2021, Neptune Labs Sp. z o.o.
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

from neptune.new.envs import PROJECT_ENV_NAME
from neptune.new.exceptions import NeptuneMissingProjectNameException
from neptune.new.internal.backends.neptune_backend import NeptuneBackend
from neptune.new.internal.utils import verify_type
from neptune.new.project import Project
from neptune.new.version import version as parsed_version

__version__ = str(parsed_version)

_logger = logging.getLogger(__name__)


def project_name_lookup(backend: NeptuneBackend, name: Optional[str] = None) -> Project:
    verify_type("name", name, (str, type(None)))

    if not name:
        name = os.getenv(PROJECT_ENV_NAME)
    if not name:
        available_workspaces = backend.get_available_workspaces()
        available_projects = backend.get_available_projects()

        raise NeptuneMissingProjectNameException(available_workspaces=available_workspaces,
                                                 available_projects=available_projects)

    return backend.get_project(name)
