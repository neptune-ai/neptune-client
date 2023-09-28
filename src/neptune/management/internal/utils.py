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
from enum import Enum
from typing import (
    List,
    Optional,
)

from neptune.common.patterns import PROJECT_QUALIFIED_NAME_PATTERN
from neptune.management.exceptions import (
    ConflictingWorkspaceName,
    InvalidProjectName,
    MissingWorkspaceName,
)


def extract_project_and_workspace(name: str, workspace: Optional[str] = None):
    project_spec = re.search(PROJECT_QUALIFIED_NAME_PATTERN, name)

    if not project_spec:
        raise InvalidProjectName(name=name)

    extracted_workspace, extracted_project_name = (
        project_spec["workspace"],
        project_spec["project"],
    )

    if not workspace and not extracted_workspace:
        raise MissingWorkspaceName(name=name)

    if workspace and extracted_workspace and workspace != extracted_workspace:
        raise ConflictingWorkspaceName(name=name, workspace=workspace)

    final_workspace_name = extracted_workspace or workspace

    return final_workspace_name, extracted_project_name


def normalize_project_name(name: str, workspace: Optional[str] = None):
    extracted_workspace_name, extracted_project_name = extract_project_and_workspace(name=name, workspace=workspace)

    return f"{extracted_workspace_name}/{extracted_project_name}"


class WorkspaceMemberRole(Enum):
    MEMBER = "member"
    ADMIN = "admin"

    def to_api(self) -> str:
        if self.value == "admin":
            return "owner"
        return self.value


def get_trashed_object_ids(project: Optional[str], api_token: Optional[str]) -> List[str]:
    from neptune import init_project  # to avoid circular imports

    with init_project(project=project, api_token=api_token) as project_obj:

        ids = []

        trashed_runs = project_obj.fetch_runs_table(trashed=True).to_rows()
        if trashed_runs:
            ids.extend([run.get_attribute_value("sys/id") for run in trashed_runs])

        trashed_models = project_obj.fetch_models_table(trashed=True).to_rows()
        if trashed_models:
            ids.extend([model.get_attribute_value("sys/id") for model in trashed_models])

    return ids
