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


@with_api_exceptions_handler
def get_project_list(api_token: Optional[str] = None) -> List[str]:
    """Lists projects that the account has access to.

    Args:
        api_token: Account's API token.
            If None, the value of NEPTUNE_API_TOKEN environment variable will be taken.
            Note: To keep your token secure, use the NEPTUNE_API_TOKEN environment variable rather than placing your
            API token in plain text in your source code.

    Returns:
        List of project names in the form 'workspace-name/project-name'.

    Example:
        >>> from neptune import management
        >>> management.get_project_list()

    You may also want to check the management API reference:
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
    """Creates a new project in a Neptune workspace.

    To create projects, the user or service account must have access to the workspace.

    Args:
        name: The name of the project in Neptune in the form 'workspace-name/project-name'.
            If workspace argument was set, it should only contain 'project' instead of 'workspace-name/project-name'.
        key: Project identifier. It has to be contain 1-10 upper case letters or numbers.
            For example, 'CLS'
        workspace: Name of your Neptune workspace. If you specify it,
            change the format of the name argument to 'project' instead of'workspace-name/project-name'.
            If None, it will be parsed from the name argument.
        visibility: Level of visibility you want your project to have.
            Can be set to:
             - 'pub' for public projects
             - 'priv' for private projects
            If None, it will be set to 'priv'.
        description: Project description.
            If None, it will be left empty.
        api_token: Account's API token.
            If None, the value of NEPTUNE_API_TOKEN environment variable will be taken.
            Note: To keep your token secure, use the NEPTUNE_API_TOKEN environment variable rather than placing your
            API token in plain text in your source code.

    Returns:
        The name of the new project you created.

    Example:
        >>> from neptune import management
        >>> management.create_project(
        ...     workspace="ml-team",
        ...     name="classification",
        ...     key="CLS",
        ...     visibility="pub",
        ... )
        'ml-team/classification'

    You may also want to check the management API reference:
    https://docs.neptune.ai/api-reference/management
    """
    verify_type("name", name, str)
    verify_type("key", key, str)
    verify_type("workspace", workspace, (str, type(None)))
    verify_type("visibility", visibility, str)
    verify_type("description", description, (str, type(None)))
    verify_type("api_token", api_token, (str, type(None)))

    backend_client = _get_backend_client(api_token=api_token)
    workspace, name = extract_project_and_workspace(name=name, workspace=workspace)
    project_qualified_name = f"{workspace}/{name}"

    try:
        workspaces = (
            backend_client.api.listOrganizations(**DEFAULT_REQUEST_KWARGS).response().result
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
            raise ProjectAlreadyExists(name=project_qualified_name) from e
        raise BadRequestException(validation_errors=validation_errors)
    except HTTPUnprocessableEntity as e:
        raise ProjectsLimitReached() from e


@with_api_exceptions_handler
def delete_project(name: str, workspace: Optional[str] = None, api_token: Optional[str] = None):
    """Deletes a project from a Neptune workspace.

    To delete projects, the user must be a workspace admin.

    Args:
        name: The name of the project in Neptune in the form 'workspace-name/project-name'.
            If workspace argument was set, it should only contain 'project' instead of 'workspace-name/project-name'.
        workspace: Name of your Neptune workspace. If you specify it,
            change the format of the name argument to 'project' instead of 'workspace-name/project-name'.
            If None, it will be parsed from the name argument.
        api_token: Account's API token.
            If None, the value of NEPTUNE_API_TOKEN environment variable will be taken.
            Note: To keep your token secure, use the NEPTUNE_API_TOKEN environment variable rather than placing your
            API token in plain text in your source code.

    Example:
        >>> from neptune import management
        >>> management.delete_project(name="ml-team/classification")

    You may also want to check the management API reference:
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
    """Adds a member to a Neptune project.

    To add members, the user or service account must be a project owner.

    Args:
        name: The name of the project in Neptune in the form 'workspace-name/project-name'.
            If workspace argument was set, it should only contain 'project' instead of 'workspace-name/project-name'.
        username: Name of the user to add to the project.
        role: level of permissions the user should have in a project.
            Can be set to:
             - 'viewer': can only view project content and members
             - 'contributor': can view and edit project content and only view members
             - 'owner': can view and edit project content and members
            For more information, see https://docs.neptune.ai/administration/user-management
        workspace: Name of your Neptune workspace. If you specify it,
            change the format of the name argument to 'project' instead of 'workspace-name/project-name'.
            If None, it will be parsed from the name argument.
        api_token: Account's API token.
            If None, the value of NEPTUNE_API_TOKEN environment variable will be taken.
            Note: To keep your token secure, use the NEPTUNE_API_TOKEN environment variable rather than placing your
            API token in plain text in your source code.

    Examples:
        >>> from neptune import management
        >>> management.add_project_member(
        ...     workspace="ml-team",
        ...     name="classification",
        ...     username="johnny",
        ...     role="contributor",
        ... )

    You may also want to check the management API reference:
    https://docs.neptune.ai/api-reference/management
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
    """Lists members of a Neptune project.

    To get the project member list, the user or service account must have access to the project.

    Args:
        name: The name of the project in Neptune in the form 'workspace-name/project-name'.
            If workspace argument was set, it should only contain 'project' instead of 'workspace-name/project-name'.
        workspace: Name of your Neptune workspace. If you specify it,
            change the format of the name argument to 'project' instead of 'workspace-name/project-name'.
            If None, it will be parsed from the name argument.
        api_token: Account's API token.
            If None, the value of NEPTUNE_API_TOKEN environment variable will be taken.
            Note: To keep your token secure, use the NEPTUNE_API_TOKEN environment variable rather than placing your
            API token in plain text in your source code.

    Returns:
        Dictionary with usernames as keys and project member roles
        ('owner', 'contributor', 'viewer') as values.

    Example:
        >>> from neptune import management
        >>> management.get_project_member_list(name="ml-team/classification")

    You may also want to check the management API reference:
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
    """Removes member from a Neptune project.

    To remove members, the user or service account must be a project owner.

    Args:
        name: The name of the project in Neptune in the form 'workspace-name/project-name'.
            If workspace argument was set, it should only contain 'project' instead of 'workspace-name/project-name'.
        username: Name of the user to remove from the project.
        workspace: Name of your Neptune workspace. If you specify it,
            change the format of the name argument to 'project' instead of 'workspace-name/project-name'.
            If None, it will be parsed from the name argument.
        api_token: Account's API token.
            If None, the value of NEPTUNE_API_TOKEN environment variable will be taken.
            Note: To keep your token secure, use the NEPTUNE_API_TOKEN environment variable rather than placing your
            API token in plain text in your source code.

    Example:
        >>> from neptune import management
        >>> management.remove_project_member(
        ...     name="ml-team/classification",
        ...     username="johnny",
        ... )

    You may also want to check the management API reference:
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
    """Lists members of a Neptune workspace.

    To get the workspace member list, the user or service account must be a member of the workspace.

    Args:
        name: Name of the Neptune workspace.
        api_token: Account's API token.
            If None, the value of NEPTUNE_API_TOKEN environment variable will be taken.
            Note: To keep your token secure, use the NEPTUNE_API_TOKEN environment variable rather than placing your
            API token in plain text in your source code.
    Returns:
        Dictionary with usernames as keys and workspace member roles ('admin', 'member') as values.

    Example:
        >>> from neptune import management
        >>> management.get_workspace_member_list(name="ml-team")

    You may also want to check the management API reference:
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
        return {f"{sa.name}": ServiceAccountDTO(name=sa.name, id=sa.id) for sa in result}
    except HTTPNotFound as e:
        raise WorkspaceNotFound(workspace=workspace_name) from e


@with_api_exceptions_handler
def get_workspace_service_account_list(
    name: str, api_token: Optional[str] = None
) -> Dict[str, str]:
    """Lists service accounts of a Neptune workspace.

    To get the service account list, the user or service account must be a member of the workspace.

    Args:
        name: Name of the Neptune workspace.
        api_token: Account's API token.
            If None, the value of NEPTUNE_API_TOKEN environment variable will be taken.
            Note: To keep your token secure, use the NEPTUNE_API_TOKEN environment variable rather than placing your
            API token in plain text in your source code.
    Returns:
        Dictionary with account names as keys and workspace member roles as values.
        Service accounts can only have the 'member' role in workspaces.

    Example:
        >>> from neptune import management
        >>> management.get_workspace_service_account_list(name="ml-team")

    You may also want to check the management API reference:
    https://docs.neptune.ai/api-reference/management
    """
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
    """Lists service accounts of a Neptune workspace.

    To get the service account list, the user or service account must be a member of the workspace.

    Args:
        name: The name of the project in Neptune in the form 'workspace-name/project-name'.
            If workspace argument was set, it should only contain 'project' instead of 'workspace-name/project-name'.
        workspace: Name of your Neptune workspace. If you specify it,
            change the format of the name argument to 'project' instead of 'workspace-name/project-name'.
            If None, it will be parsed from the name argument.
        api_token: Account's API token.
            If None, the value of NEPTUNE_API_TOKEN environment variable will be taken.
            Note: To keep your token secure, use the NEPTUNE_API_TOKEN environment variable rather than placing your
            API token in plain text in your source code.
    Returns:
        Dictionary with account names as keys and project member roles
        ('owner', 'contributor', 'viewer') as values.

    Example:
        >>> from neptune import management
        >>> management.get_project_service_account_list(
        ...     workspace="ml-team",
        ...     name="classification",
        ... )

    You may also want to check the management API reference:
    https://docs.neptune.ai/api-reference/management
    """
    verify_type("name", name, str)
    verify_type("workspace", workspace, (str, type(None)))
    verify_type("api_token", api_token, (str, type(None)))

    backend_client = _get_backend_client(api_token=api_token)
    project_identifier = normalize_project_name(name=name, workspace=workspace)

    params = {"projectIdentifier": project_identifier, **DEFAULT_REQUEST_KWARGS}

    try:
        result = backend_client.api.listProjectServiceAccounts(**params).response().result
        return {
            f"{sa.serviceAccountInfo.name}": ProjectMemberRoleDTO.to_domain(sa.role)
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
    """Adds a service account to a Neptune project.

    To add a service account, the user or service account must be a project owner.

    Args:
        name: The name of the project in Neptune in the form 'workspace-name/project-name'.
            If workspace argument was set, it should only contain 'project' instead of 'workspace-name/project-name'.
        service_account_name: Name of the service account to add to the project (without the workspace tag).
        role: level of permissions the user or service account should have in a project.
            Can be set to:
             - 'viewer': can only view project content and members
             - 'contributor': can view and edit project content and only view members
             - 'owner': can view and edit project content and members
            For more information, see https://docs.neptune.ai/administration/user-management
        workspace: Name of your Neptune workspace. If you specify it,
            change the format of the name argument to 'project' instead of 'workspace-name/project-name'.
            If None, it will be parsed from the name argument.
        api_token: Account's API token.
            If None, the value of NEPTUNE_API_TOKEN environment variable will be taken.
            Note: To keep your token secure, use the NEPTUNE_API_TOKEN environment variable rather than placing your
                API token in plain text in your source code.

    Examples:
        >>> from neptune import management
        >>> management.add_project_service_account(
        ...     workspace="ml-team",
        ...     name="classification",
        ...     service_account_name="automaton",
        ...     role="contributor",
        ... )

    You may also want to check the management API reference:
    https://docs.neptune.ai/api-reference/management
    """
    verify_type("name", name, str)
    verify_type("service_account_name", service_account_name, str)
    verify_type("role", role, str)
    verify_type("workspace", workspace, (str, type(None)))
    verify_type("api_token", api_token, (str, type(None)))

    backend_client = _get_backend_client(api_token=api_token)
    workspace, project_name = extract_project_and_workspace(name=name, workspace=workspace)
    project_qualified_name = f"{workspace}/{project_name}"

    service_account = _get_raw_workspace_service_account_list(
        workspace_name=workspace, api_token=api_token
    ).get(service_account_name)

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
    """Removes a service account from a Neptune project.

    To add a service account, the user or service account must be a project owner.

    Args:
        name: The name of the project in Neptune in the form 'workspace-name/project-name'.
            If workspace argument was set, it should only contain 'project' instead of 'workspace-name/project-name'.
        service_account_name: Name of the service account to add to the project (without the workspace tag).
        workspace: Name of your Neptune workspace. If you specify it,
            change the format of the name argument to 'project' instead of 'workspace-name/project-name'.
            If None, it will be parsed from the name argument.
        api_token: Account's API token.
            If None, the value of NEPTUNE_API_TOKEN environment variable will be taken.
            Note: To keep your token secure, use the NEPTUNE_API_TOKEN environment variable rather than placing your
            API token in plain text in your source code.

    Examples:
        >>> from neptune import management
        >>> management.remove_project_service_account(
        ...     workspace="ml-team",
        ...     name="classification",
        ...     service_account_name="automaton",
        ... )

    You may also want to check the management API reference:
    https://docs.neptune.ai/api-reference/management
    """
    verify_type("name", name, str)
    verify_type("service_account_name", service_account_name, str)
    verify_type("workspace", workspace, (str, type(None)))
    verify_type("api_token", api_token, (str, type(None)))

    backend_client = _get_backend_client(api_token=api_token)
    workspace, project_name = extract_project_and_workspace(name=name, workspace=workspace)
    project_qualified_name = f"{workspace}/{project_name}"

    service_account = _get_raw_workspace_service_account_list(
        workspace_name=workspace, api_token=api_token
    ).get(service_account_name)

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
