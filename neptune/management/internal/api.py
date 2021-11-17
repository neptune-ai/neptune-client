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
import os
from typing import Optional, List, Dict

from bravado.client import SwaggerClient
from bravado.exception import (
    HTTPNotFound,
    HTTPBadRequest,
    HTTPConflict,
    HTTPForbidden,
    HTTPUnprocessableEntity,
)

from neptune.patterns import PROJECT_QUALIFIED_NAME_PATTERN
from neptune.new.envs import API_TOKEN_ENV_NAME
from neptune.new.internal.utils import verify_type
from neptune.new.internal.credentials import Credentials
from neptune.new.internal.backends.hosted_client import (
    create_backend_client,
    create_http_client_with_auth,
    DEFAULT_REQUEST_KWARGS,
)
from neptune.new.internal.backends.utils import (
    with_api_exceptions_handler,
    ssl_verify,
    parse_validation_errors,
)
from neptune.management.internal.utils import normalize_project_name
from neptune.management.internal.types import *
from neptune.management.exceptions import (
    AccessRevokedOnDeletion,
    AccessRevokedOnMemberRemoval,
    ProjectAlreadyExists,
    ProjectNotFound,
    UserNotExistsOrWithoutAccess,
    WorkspaceNotFound,
    UserAlreadyHasAccess,
    BadRequestException,
    ProjectsLimitReached,
)
from neptune.management.internal.dto import (
    ProjectVisibilityDTO,
    ProjectMemberRoleDTO,
    WorkspaceMemberRoleDTO,
)


def _get_token(api_token: Optional[str] = None) -> str:
    return api_token or os.getenv(API_TOKEN_ENV_NAME)


def _get_backend_client(api_token: Optional[str] = None) -> SwaggerClient:
    credentials = Credentials.from_token(api_token=_get_token(api_token=api_token))
    http_client, client_config = create_http_client_with_auth(
        credentials=credentials, ssl_verify=ssl_verify(), proxies={}
    )
    return create_backend_client(client_config=client_config, http_client=http_client)


@with_api_exceptions_handler
def get_project_list(api_token: Optional[str] = None) -> List[str]:
    """Get a list of projects you have access to.
    Args:
        api_token(str, optional): User’s API token. Defaults to `None`.
            If `None`, the value of `NEPTUNE_API_TOKEN` environment variable will be taken.
            .. note::
                It is strongly recommended to use `NEPTUNE_API_TOKEN` environment variable rather than placing your
                API token in plain text in your source code.
    Returns:
        ``List[str]``: list of project names of a format 'WORKSPACE/PROJECT'
    Examples:
        >>> from neptune import management
        >>> management.get_project_list()
    You may also want to check `management API reference`_.
    .. _management API reference:
       https://docs.neptune.ai/api-reference/management
    """
    verify_type("api_token", api_token, (str, type(None)))

    backend_client = _get_backend_client(api_token=api_token)

    params = {
        "userRelation": "viewerOrHigher",
        "sortBy": ["lastViewed"],
        **DEFAULT_REQUEST_KWARGS,
    }

    projects = backend_client.api.listProjects(**params).response().result.entries

    return [
        normalize_project_name(name=project.name, workspace=project.organizationName)
        for project in projects
    ]


@with_api_exceptions_handler
def create_project(
    name: str,
    key: str,
    workspace: Optional[str] = None,
    visibility: str = ProjectVisibility.PRIVATE,
    description: Optional[str] = None,
    api_token: Optional[str] = None,
) -> str:
    """Creates a new project in your Neptune workspace.
    Args:
        name(str): The name of the project in Neptune in the format 'WORKSPACE/PROJECT'.
            If workspace argument was set, it should only contain 'PROJECT' instead of 'WORKSPACE/PROJECT'.
        key(str): Project identifier. It has to be contain 1-10 upper case letters or numbers.
            For example, 'GOOD5'
        workspace(str, optional): Name of your Neptune workspace.
            If you specify it, change the format of the name argument to 'PROJECT' instead of 'WORKSPACE/PROJECT'.
            If 'None' it will be parsed from the `name` argument.
        visibility(str, optional): level of visibility you want your project to have.
            Can be set to:
             - 'pub' for public projects
             - 'priv' for private projects
            If 'None' it will be set to 'priv'
        description(str, optional): Project description.
            If 'None', it will be left empty.
        api_token(str, optional): User’s API token. Defaults to `None`.
            If `None`, the value of `NEPTUNE_API_TOKEN` environment variable will be taken.
            .. note::
                It is strongly recommended to use `NEPTUNE_API_TOKEN` environment variable rather than placing your
                API token in plain text in your source code.
    Returns:
        ``str``: name of the new project you created.
    Examples:
        >>> from neptune import management
        >>> management.create_project(name="awesome-team/amazing-project",
        ...                           key="AMA",
        ...                           visibility="pub")
    You may also want to check `management API reference`_.
    .. _management API reference:
       https://docs.neptune.ai/api-reference/management
    """
    verify_type("name", name, str)
    verify_type("key", key, str)
    verify_type("workspace", workspace, (str, type(None)))
    verify_type("visibility", visibility, str)
    verify_type("description", description, (str, type(None)))
    verify_type("api_token", api_token, (str, type(None)))

    backend_client = _get_backend_client(api_token=api_token)
    project_identifier = normalize_project_name(name=name, workspace=workspace)

    project_spec = re.search(PROJECT_QUALIFIED_NAME_PATTERN, project_identifier)
    workspace, name = project_spec["workspace"], project_spec["project"]

    try:
        workspaces = (
            backend_client.api.listOrganizations(**DEFAULT_REQUEST_KWARGS)
            .response()
            .result
        )
        workspace_name_to_id = {f"{f.name}": f.id for f in workspaces}
    except HTTPNotFound:
        raise WorkspaceNotFound(workspace=workspace)

    if workspace not in workspace_name_to_id:
        raise WorkspaceNotFound(workspace=workspace)

    params = {
        "projectToCreate": {
            "name": name,
            "description": description,
            "projectKey": key,
            "organizationId": workspace_name_to_id[workspace],
            "visibility": ProjectVisibilityDTO.from_str(visibility).value,
        },
        **DEFAULT_REQUEST_KWARGS,
    }

    try:
        response = backend_client.api.createProject(**params).response()
        return normalize_project_name(
            name=response.result.name, workspace=response.result.organizationName
        )
    except HTTPBadRequest as e:
        validation_errors = parse_validation_errors(error=e)
        if "ERR_NOT_UNIQUE" in validation_errors:
            raise ProjectAlreadyExists(name=project_identifier) from e
        raise BadRequestException(validation_errors=validation_errors)
    except HTTPUnprocessableEntity as e:
        raise ProjectsLimitReached() from e


@with_api_exceptions_handler
def delete_project(
    name: str, workspace: Optional[str] = None, api_token: Optional[str] = None
):
    """Deletes a project from your Neptune workspace.
    Args:
        name(str): The name of the project in Neptune in the format 'WORKSPACE/PROJECT'.
            If workspace argument was set, it should only contain 'PROJECT' instead of 'WORKSPACE/PROJECT'.
        workspace(str, optional): Name of your Neptune workspace.
            If you specify it, change the format of the name argument to 'PROJECT' instead of 'WORKSPACE/PROJECT'.
            If 'None' it will be parsed from the name argument.
        api_token(str, optional): User’s API token. Defaults to `None`.
            If `None`, the value of `NEPTUNE_API_TOKEN` environment variable will be taken.
            .. note::
                It is strongly recommended to use `NEPTUNE_API_TOKEN` environment variable rather than placing your
                API token in plain text in your source code.
    Examples:
        >>> from neptune import management
        >>> management.delete_project(name="awesome-team/amazing-project")
    You may also want to check `management API reference`_.
    .. _management API reference:
       https://docs.neptune.ai/api-reference/management
    """
    verify_type("name", name, str)
    verify_type("workspace", workspace, (str, type(None)))
    verify_type("api_token", api_token, (str, type(None)))

    backend_client = _get_backend_client(api_token=api_token)
    project_identifier = normalize_project_name(name=name, workspace=workspace)

    params = {"projectIdentifier": project_identifier, **DEFAULT_REQUEST_KWARGS}

    try:
        backend_client.api.deleteProject(**params).response()
    except HTTPNotFound as e:
        raise ProjectNotFound(name=project_identifier) from e
    except HTTPForbidden as e:
        raise AccessRevokedOnDeletion(name=project_identifier) from e


@with_api_exceptions_handler
def add_project_member(
    name: str,
    username: str,
    role: str,
    workspace: Optional[str] = None,
    api_token: Optional[str] = None,
):
    """Adds member to the Neptune project.
    Args:
        name(str): The name of the project in Neptune in the format 'WORKSPACE/PROJECT'.
            If workspace argument was set, it should only contain 'PROJECT' instead of 'WORKSPACE/PROJECT'.
        username(str): Name of the user you want to add to the project.
        role(str): level of permissions the user should have in a project.
            Can be set to:
             - 'viewer': can only view project content and members
             - 'contributor': can view and edit project content and only view members
             - 'owner': can view and edit project content and members
            For more information, see `user roles in a project docs`_.
        workspace(str, optional): Name of your Neptune workspace.
            If you specify it, change the format of the name argument to 'PROJECT' instead of 'WORKSPACE/PROJECT'.
            If 'None' it will be parsed from the name argument.
        api_token(str, optional): User’s API token. Defaults to `None`.
            If `None`, the value of `NEPTUNE_API_TOKEN` environment variable will be taken.
            .. note::
                It is strongly recommended to use `NEPTUNE_API_TOKEN` environment variable rather than placing your
                API token in plain text in your source code.
    Examples:
        >>> from neptune import management
        >>> management.add_project_member(name="awesome-team/amazing-project",
        ...                               username="johny",
        ...                               role="contributor")
    You may also want to check `management API reference`_.
    .. _management API reference:
       https://docs.neptune.ai/api-reference/management
    .. _user roles in a project docs:
       https://docs.neptune.ai/administration/user-management#roles-in-a-project
    """
    verify_type("name", name, str)
    verify_type("username", username, str)
    verify_type("role", role, str)
    verify_type("workspace", workspace, (str, type(None)))
    verify_type("api_token", api_token, (str, type(None)))

    backend_client = _get_backend_client(api_token=api_token)
    project_identifier = normalize_project_name(name=name, workspace=workspace)

    params = {
        "projectIdentifier": project_identifier,
        "member": {
            "userId": username,
            "role": ProjectMemberRoleDTO.from_str(role).value,
        },
        **DEFAULT_REQUEST_KWARGS,
    }

    try:
        backend_client.api.addProjectMember(**params).response()
    except HTTPNotFound as e:
        raise ProjectNotFound(name=project_identifier) from e
    except HTTPConflict as e:
        raise UserAlreadyHasAccess(user=username, project=project_identifier) from e


@with_api_exceptions_handler
def get_project_member_list(
    name: str, workspace: Optional[str] = None, api_token: Optional[str] = None
) -> Dict[str, str]:
    """Get a list of members for a project.
    Args:
        name(str): The name of the project in Neptune in the format 'WORKSPACE/PROJECT'.
            If workspace argument was set it should only contain 'PROJECT' instead of 'WORKSPACE/PROJECT'.
        workspace(str, optional): Name of your Neptune workspace.
            If you specify change the format of the name argument to 'PROJECT' instead of 'WORKSPACE/PROJECT'.
            If 'None' it will be parsed from the name argument.
        api_token(str, optional): User’s API token. Defaults to `None`.
            If `None`, the value of `NEPTUNE_API_TOKEN` environment variable will be taken.
            .. note::
                It is strongly recommended to use `NEPTUNE_API_TOKEN` environment variable rather than placing your
                API token in plain text in your source code.
    Returns:
        ``Dict[str, str]``: Dictionary with usernames as keys and ProjectMemberRoles
        ('owner', 'contributor', 'viewer') as values.
    Examples:
        >>> from neptune import management
        >>> management.get_project_member_list(name="awesome-team/amazing-project")
    You may also want to check `management API reference`_.
    .. _management API reference:
       https://docs.neptune.ai/api-reference/management
    """
    verify_type("name", name, str)
    verify_type("workspace", workspace, (str, type(None)))
    verify_type("api_token", api_token, (str, type(None)))

    backend_client = _get_backend_client(api_token=api_token)
    project_identifier = normalize_project_name(name=name, workspace=workspace)

    params = {"projectIdentifier": project_identifier, **DEFAULT_REQUEST_KWARGS}

    try:
        result = backend_client.api.listProjectMembers(**params).response().result
        return {
            f"{m.registeredMemberInfo.username}": ProjectMemberRoleDTO.to_domain(m.role)
            for m in result
        }
    except HTTPNotFound as e:
        raise ProjectNotFound(name=project_identifier) from e


@with_api_exceptions_handler
def remove_project_member(
    name: str,
    username: str,
    workspace: Optional[str] = None,
    api_token: Optional[str] = None,
):
    """Removes member from the Neptune project.
    Args:
        name(str): The name of the project in Neptune in the format 'WORKSPACE/PROJECT'.
            If workspace argument was set, it should only contain 'PROJECT' instead of 'WORKSPACE/PROJECT'.
        username(str): name of the user you want to remove from the project.
        workspace(str, optional): Name of your Neptune workspace.
            If you specify change the format of the name argument to 'PROJECT' instead of 'WORKSPACE/PROJECT'.
            If 'None' it will be parsed from the name argument.
        api_token(str, optional): User’s API token. Defaults to `None`.
            If `None`, the value of `NEPTUNE_API_TOKEN` environment variable will be taken.
            .. note::
                It is strongly recommended to use `NEPTUNE_API_TOKEN` environment variable rather than placing your
                API token in plain text in your source code.
    Examples:
        >>> from neptune import management
        >>> management.remove_project_member(name="awesome-team/amazing-project",
        ...                                  username="johny")
    You may also want to check `management API reference`_.
    .. _management API reference:
       https://docs.neptune.ai/api-reference/management
    """
    verify_type("name", name, str)
    verify_type("username", username, str)
    verify_type("workspace", workspace, (str, type(None)))
    verify_type("api_token", api_token, (str, type(None)))

    backend_client = _get_backend_client(api_token=api_token)
    project_identifier = normalize_project_name(name=name, workspace=workspace)

    params = {
        "projectIdentifier": project_identifier,
        "userId": username,
        **DEFAULT_REQUEST_KWARGS,
    }

    try:
        backend_client.api.deleteProjectMember(**params).response()
    except HTTPNotFound as e:
        raise ProjectNotFound(name=project_identifier) from e
    except HTTPUnprocessableEntity as e:
        raise UserNotExistsOrWithoutAccess(
            user=username, project=project_identifier
        ) from e
    except HTTPForbidden as e:
        raise AccessRevokedOnMemberRemoval(
            user=username, project=project_identifier
        ) from e


@with_api_exceptions_handler
def get_workspace_member_list(
    name: str, api_token: Optional[str] = None
) -> Dict[str, str]:
    """Get a list of members of a workspace.
    Args:
        name(str, optional): Name of your Neptune workspace.
        api_token(str, optional): User’s API token. Defaults to `None`.
            If `None`, the value of `NEPTUNE_API_TOKEN` environment variable will be taken.
            .. note::
                It is strongly recommended to use `NEPTUNE_API_TOKEN` environment variable rather than placing your
                API token in plain text in your source code.
    Returns:
        ``Dict[str, str]``: Dictionary with usernames as keys and `WorkspaceMemberRole` ('member', 'admin') as values.
    Examples:
        >>> from neptune import management
        >>> management.get_workspace_member_list(name="awesome-team")
    You may also want to check `management API reference`_.
    .. _management API reference:
       https://docs.neptune.ai/api-reference/management
    """
    verify_type("name", name, str)
    verify_type("api_token", api_token, (str, type(None)))

    backend_client = _get_backend_client(api_token=api_token)

    params = {"organizationIdentifier": name, **DEFAULT_REQUEST_KWARGS}

    try:
        result = backend_client.api.listOrganizationMembers(**params).response().result
        return {
            f"{m.registeredMemberInfo.username}": WorkspaceMemberRoleDTO.to_domain(
                m.role
            )
            for m in result
        }
    except HTTPNotFound as e:
        raise WorkspaceNotFound(workspace=name) from e
