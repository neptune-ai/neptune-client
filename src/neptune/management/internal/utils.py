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
import re
from typing import Optional

from neptune.common.patterns import PROJECT_QUALIFIED_NAME_PATTERN
from neptune.management.exceptions import (
    ConflictingWorkspaceName,
    InvalidProjectName,
    MissingWorkspaceName,
)
from neptune.new.internal.utils.deprecation import deprecated_parameter


@deprecated_parameter(deprecated_kwarg_name="name", required_kwarg_name="project")
def extract_project_and_workspace(project: str, workspace: Optional[str] = None):
    project_spec = re.search(PROJECT_QUALIFIED_NAME_PATTERN, project)

    if not project_spec:
        raise InvalidProjectName(name=project)

    extracted_workspace, extracted_project_name = (
        project_spec["workspace"],
        project_spec["project"],
    )

    if not workspace and not extracted_workspace:
        raise MissingWorkspaceName(name=project)

    if workspace and extracted_workspace and workspace != extracted_workspace:
        raise ConflictingWorkspaceName(name=project, workspace=workspace)

    final_workspace_name = extracted_workspace or workspace

    return final_workspace_name, extracted_project_name


@deprecated_parameter(deprecated_kwarg_name="name", required_kwarg_name="project")
def normalize_project_name(project: str, workspace: Optional[str] = None):
    extracted_workspace_name, extracted_project_name = extract_project_and_workspace(
        project=project, workspace=workspace
    )

    return f"{extracted_workspace_name}/{extracted_project_name}"
