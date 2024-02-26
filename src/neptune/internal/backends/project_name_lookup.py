#
# Copyright (c) 2022, Neptune Labs Sp. z o.o.
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
__all__ = ["project_name_lookup"]

import os
from typing import Optional

from neptune.envs import PROJECT_ENV_NAME
from neptune.exceptions import NeptuneMissingProjectNameException
from neptune.internal.backends.api_model import Project
from neptune.internal.backends.neptune_backend import NeptuneBackend
from neptune.internal.id_formats import QualifiedName
from neptune.internal.utils import verify_type
from neptune.internal.utils.logger import get_logger

_logger = get_logger()


def project_name_lookup(backend: NeptuneBackend, name: Optional[QualifiedName] = None) -> Project:
    verify_type("name", name, (str, type(None)))

    if not name:
        name = os.getenv(PROJECT_ENV_NAME)
    if not name:
        available_workspaces = backend.get_available_workspaces()
        available_projects = backend.get_available_projects()

        raise NeptuneMissingProjectNameException(
            available_workspaces=available_workspaces,
            available_projects=available_projects,
        )

    return backend.get_project(name)
