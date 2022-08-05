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
import os
from typing import Dict, List, Optional

from bravado.exception import (
    HTTPBadRequest,
    HTTPConflict,
    HTTPForbidden,
    HTTPNotFound,
    HTTPUnprocessableEntity,
)

from neptune.management.exceptions import (
    AccessRevokedOnDeletion,
    AccessRevokedOnMemberRemoval,
    AccessRevokedOnServiceAccountRemoval,
    BadRequestException,
    ProjectAlreadyExists,
    ProjectNotFound,
    ProjectsLimitReached,
    ServiceAccountAlreadyHasAccess,
    ServiceAccountNotExistsOrWithoutAccess,
    ServiceAccountNotFound,
    UserAlreadyHasAccess,
    UserNotExistsOrWithoutAccess,
    WorkspaceNotFound,
)
from neptune.management.internal.dto import (
    ProjectMemberRoleDTO,
    ProjectVisibilityDTO,
    ServiceAccountDTO,
    WorkspaceMemberRoleDTO,
)
from neptune.management.internal.types import *
from neptune.management.internal.utils import (
    extract_project_and_workspace,
    normalize_project_name,
)
from neptune.new.envs import API_TOKEN_ENV_NAME
from neptune.new.internal.backends.hosted_client import (
    DEFAULT_REQUEST_KWARGS,
    create_backend_client,
    create_http_client_with_auth,
)
from neptune.new.internal.backends.swagger_client_wrapper import SwaggerClientWrapper
from neptune.new.internal.backends.utils import (
    parse_validation_errors,
    ssl_verify,
    with_api_exceptions_handler,
)
from neptune.new.internal.credentials import Credentials
from neptune.new.internal.utils import verify_type


def _get_token(api_token: Optional[str] = None) -> str:
    return api_token or os.getenv(API_TOKEN_ENV_NAME)


def _get_backend_client(api_token: Optional[str] = None) -> SwaggerClientWrapper:
    credentials = Credentials.from_token(api_token=_get_token(api_token=api_token))
    http_client, client_config = create_http_client_with_auth(
        credentials=credentials, ssl_verify=ssl_verify(), proxies={}
    )
    return create_backend_client(client_config=client_config, http_client=http_client)


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
    projects = _get_projects(backend_client, params)

    return [
        normalize_project_name(name=project.name, workspace=project.organizationName)
        for project in projects
    ]


@with_api_exceptions_handler
def _get_projects(backend_client, params) -> List:
    return backend_client.api.listProjects(**params).response().result.entries


def create_project(
    name: str,
    key: Optional[str] = None,
    workspace: Optional[str] = None,
    visibility: str = ProjectVisibility.PRIVATE,
    description: Optional[str] = None,
    api_token: Optional[str] = None,
) -> str:
    """Creates a new project in your Neptune workspace.
    Args:
        name(str): The name of the project in Neptune in the format 'WORKSPACE/PROJECT'.
            If workspace argument was set, it should only contain 'PROJECT' instead of 'WORKSPACE/PROJECT'.
        key(str, optional): Project identifier. It has to be contain 1-10 upper case letters or numbers.
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
       https://docs.neptune.ai/api-reference/management#.create_project
    """
    verify_type("name", name, str)
    verify_type("key", key, (str, type(None)))
    verify_type("workspace", workspace, (str, type(None)))
    verify_type("visibility", visibility, str)
    verify_type("description", description, (str, type(None)))
    verify_type("api_token", api_token, (str, type(None)))

    backend_client = _get_backend_client(api_token=api_token)
    workspace, name = extract_project_and_workspace(name=name, workspace=workspace)
    project_qualified_name = f"{workspace}/{name}"
    workspace_id = _get_workspace_id(backend_client, workspace)

    params = {
        "projectToCreate": {
            "name": name,
            "description": description,
            "projectKey": key,
            "organizationId": workspace_id,
            "visibility": ProjectVisibilityDTO.from_str(visibility).value,
        },
        **DEFAULT_REQUEST_KWARGS,
    }

    project_response = _create_project(backend_client, project_qualified_name, params)

    return normalize_project_name(
        name=project_response.result.name, workspace=project_response.result.organizationName
    )


def _get_workspace_id(backend_client, workspace) -> str:
    workspaces = _get_workspaces(backend_client)
    workspace_name_to_id = {f"{f.name}": f.id for f in workspaces}
    if workspace not in workspace_name_to_id:
        raise WorkspaceNotFound(workspace=workspace)
    return workspace_name_to_id[workspace]


@with_api_exceptions_handler
def _get_workspaces(backend_client):
    return backend_client.api.listOrganizations(**DEFAULT_REQUEST_KWARGS).response().result


@with_api_exceptions_handler
def _create_project(backend_client, project_qualified_name: str, params: dict):
    try:
        return backend_client.api.createProject(**params).response()
    except HTTPBadRequest as e:
        validation_errors = parse_validation_errors(error=e)
        if "ERR_NOT_UNIQUE" in validation_errors:
            raise ProjectAlreadyExists(name=project_qualified_name) from e
        raise BadRequestException(validation_errors=validation_errors)
    except HTTPUnprocessableEntity as e:
        raise ProjectsLimitReached() from e


@with_api_exceptions_handler
def delete_project(name: str, workspace: Optional[str] = None, api_token: Optional[str] = None):
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
        members = get_project_member_list(name=name, workspace=workspace, api_token=api_token)
        user_role = members.get(username)
        raise UserAlreadyHasAccess(user=username, project=project_identifier, role=user_role) from e


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
        raise UserNotExistsOrWithoutAccess(user=username, project=project_identifier) from e
    except HTTPForbidden as e:
        raise AccessRevokedOnMemberRemoval(user=username, project=project_identifier) from e


@with_api_exceptions_handler
def get_workspace_member_list(name: str, api_token: Optional[str] = None) -> Dict[str, str]:
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
            f"{m.registeredMemberInfo.username}": WorkspaceMemberRoleDTO.to_domain(m.role)
            for m in result
        }
    except HTTPNotFound as e:
        raise WorkspaceNotFound(workspace=name) from e


@with_api_exceptions_handler
def _get_raw_workspace_service_account_list(
    workspace_name: str, api_token: Optional[str] = None
) -> Dict[str, ServiceAccountDTO]:
    verify_type("workspace_name", workspace_name, str)
    verify_type("api_token", api_token, (str, type(None)))

    backend_client = _get_backend_client(api_token=api_token)

    params = {
        "organizationIdentifier": workspace_name,
        "deactivated": False,
        **DEFAULT_REQUEST_KWARGS,
    }

    try:
        result = backend_client.api.listServiceAccounts(**params).response().result
        return {
            f"{sa.displayName}": ServiceAccountDTO(name=sa.displayName, id=sa.id) for sa in result
        }
    except HTTPNotFound as e:
        raise WorkspaceNotFound(workspace=workspace_name) from e


@with_api_exceptions_handler
def get_workspace_service_account_list(
    name: str, api_token: Optional[str] = None
) -> Dict[str, str]:
    service_accounts = _get_raw_workspace_service_account_list(
        workspace_name=name, api_token=api_token
    )

    return {
        service_account_name: WorkspaceMemberRoleDTO.to_domain("member")
        for service_account_name, _ in service_accounts.items()
    }


@with_api_exceptions_handler
def get_project_service_account_list(
    name: str, workspace: Optional[str] = None, api_token: Optional[str] = None
) -> Dict[str, str]:
    verify_type("name", name, str)
    verify_type("workspace", workspace, (str, type(None)))
    verify_type("api_token", api_token, (str, type(None)))

    backend_client = _get_backend_client(api_token=api_token)
    project_identifier = normalize_project_name(name=name, workspace=workspace)

    params = {"projectIdentifier": project_identifier, **DEFAULT_REQUEST_KWARGS}

    try:
        result = backend_client.api.listProjectServiceAccounts(**params).response().result
        return {
            f"{sa.serviceAccountInfo.displayName}": ProjectMemberRoleDTO.to_domain(sa.role)
            for sa in result
        }
    except HTTPNotFound as e:
        raise ProjectNotFound(name=project_identifier) from e


@with_api_exceptions_handler
def add_project_service_account(
    name: str,
    service_account_name: str,
    role: str,
    workspace: Optional[str] = None,
    api_token: Optional[str] = None,
):
    verify_type("name", name, str)
    verify_type("service_account_name", service_account_name, str)
    verify_type("role", role, str)
    verify_type("workspace", workspace, (str, type(None)))
    verify_type("api_token", api_token, (str, type(None)))

    backend_client = _get_backend_client(api_token=api_token)
    workspace, project_name = extract_project_and_workspace(name=name, workspace=workspace)
    project_qualified_name = f"{workspace}/{project_name}"

    try:
        service_account = _get_raw_workspace_service_account_list(
            workspace_name=workspace, api_token=api_token
        )[service_account_name]
    except KeyError as e:
        raise ServiceAccountNotFound(
            service_account_name=service_account_name, workspace=workspace
        ) from e

    params = {
        "projectIdentifier": project_qualified_name,
        "account": {
            "serviceAccountId": service_account.id,
            "role": ProjectMemberRoleDTO.from_str(role).value,
        },
        **DEFAULT_REQUEST_KWARGS,
    }

    try:
        backend_client.api.addProjectServiceAccount(**params).response()
    except HTTPNotFound as e:
        raise ProjectNotFound(name=project_qualified_name) from e
    except HTTPConflict as e:
        service_accounts = get_project_service_account_list(
            name=name, workspace=workspace, api_token=api_token
        )
        service_account_role = service_accounts.get(service_account_name)

        raise ServiceAccountAlreadyHasAccess(
            service_account_name=service_account_name,
            project=project_qualified_name,
            role=service_account_role,
        ) from e


@with_api_exceptions_handler
def remove_project_service_account(
    name: str,
    service_account_name: str,
    workspace: Optional[str] = None,
    api_token: Optional[str] = None,
):
    verify_type("name", name, str)
    verify_type("service_account_name", service_account_name, str)
    verify_type("workspace", workspace, (str, type(None)))
    verify_type("api_token", api_token, (str, type(None)))

    backend_client = _get_backend_client(api_token=api_token)
    workspace, project_name = extract_project_and_workspace(name=name, workspace=workspace)
    project_qualified_name = f"{workspace}/{project_name}"

    try:
        service_account = _get_raw_workspace_service_account_list(
            workspace_name=workspace, api_token=api_token
        )[service_account_name]
    except KeyError as e:
        raise ServiceAccountNotFound(
            service_account_name=service_account_name, workspace=workspace
        ) from e

    params = {
        "projectIdentifier": project_qualified_name,
        "serviceAccountId": service_account.id,
        **DEFAULT_REQUEST_KWARGS,
    }

    try:
        backend_client.api.deleteProjectServiceAccount(**params).response()
    except HTTPNotFound as e:
        raise ProjectNotFound(name=project_qualified_name) from e
    except HTTPUnprocessableEntity as e:
        raise ServiceAccountNotExistsOrWithoutAccess(
            service_account_name=service_account_name, project=project_qualified_name
        ) from e
    except HTTPForbidden as e:
        raise AccessRevokedOnServiceAccountRemoval(
            service_account_name=service_account_name, project=project_qualified_name
        ) from e
