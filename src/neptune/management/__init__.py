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
"""The management API lets you perform various neptune.ai administration actions.

- Create and delete projects
- List projects you can access
- Add and remove project members
- List members of projects and workspaces

Most actions can also be performed by service accounts.

Functions:
    get_project_list()
    create_project()
    delete_project()
    get_project_member_list()
    add_project_member()
    remove_project_member()
    get_workspace_member_list()
    add_project_service_account()
    remove_project_service_account()
    get_project_service_account_list()
    get_workspace_service_account_list()
    trash_objects()

Usage examples
--------------

Import management API
>>> from neptune import management

Getting projects in a workspace as a list:
>>> projects = management.get_project_list()

Creating a new project:
>>> management.create_project(
...     name="ml-team/classification",
...     key="CLS",
...     visibility="workspace",
... )

Deleting a project:
>>> management.delete_project(project="ml-team/classification")

Get project members list as dictionary, with usernames as keys and roles as values:
>>> members = management.get_project_member_list(project="ml-team/classification")

Assign a user to a project and specify a role:
>>> management.add_project_member(
...    project="ml-team/classification", username="jackie", role="contributor"
... )

Remove a user from a project:
>>> management.remove_project_member(project="ml-team/classification", username="janus")

Get workspace members list as dictionary, with usernames as keys and roles as values:
>>> management.get_workspace_member_list(workspace="ml-team")

Assign service account to project:
>>> management.add_project_service_account(
...     project="ml-team/classification",
...     service_account_name="cicd@ml-team",
...     role="contributor",
... )

Remove service account from project:
>>> management.remove_project_service_account(
...     project="ml-team/classification", service_account_name="cicd@ml-team"
... )

Get list of project service accounts as dictionary, with usernames as keys and roles as values:
>>> management.get_project_service_account_list(project="ml-team/classification")

Get list of workspace service accounts as dictionary, with usernames as keys and roles as values:
>>> management.get_workspace_service_account_list(workspace="ml-team")

Move one or more Neptune objects to the project trash:
>>> project_name = "ml-team/classification"
>>> # Connect to your project:
... project = neptune.init_project(project=project_name)
>>> # Fetch runs tagged as "trash":
... runs_to_trash_df = project.fetch_runs_table(tag="trash").to_pandas()
>>> # Turn run IDs into a list:
... runs_to_trash = runs_to_trash_df["sys/id"].tolist()
>>> # Move the runs to trash:
... management.trash_objects(project=project_name, ids=runs_to_trash)

Get information about a workspace, including storage usage and limits:
>>> management.get_workspace_status(workspace="ml-team")

---

See also the API reference in the docs: https://docs.neptune.ai/api/management
"""
from .internal.api import (
    WorkspaceMemberRole,
    add_project_member,
    add_project_service_account,
    clear_trash,
    create_project,
    delete_objects_from_trash,
    delete_project,
    get_project_list,
    get_project_member_list,
    get_project_service_account_list,
    get_workspace_member_list,
    get_workspace_service_account_list,
    get_workspace_status,
    invite_to_workspace,
    remove_project_member,
    remove_project_service_account,
    trash_objects,
)
from .internal.types import (
    MemberRole,
    ProjectVisibility,
)

__all__ = [
    "clear_trash",
    "get_project_list",
    "create_project",
    "delete_project",
    "delete_objects_from_trash",
    "get_project_member_list",
    "add_project_member",
    "remove_project_member",
    "get_workspace_member_list",
    "invite_to_workspace",
    "WorkspaceMemberRole",
    "add_project_service_account",
    "remove_project_service_account",
    "get_project_service_account_list",
    "get_workspace_service_account_list",
    "get_workspace_status",
    "trash_objects",
    "MemberRole",
    "ProjectVisibility",
]
