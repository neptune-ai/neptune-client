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
    "trash_objects",
]

import os
from typing import (
    Dict,
    Iterable,
    List,
    Optional,
    Union,
)

from bravado.exception import (
    HTTPBadRequest,
    HTTPConflict,
    HTTPForbidden,
    HTTPNotFound,
    HTTPUnprocessableEntity,
)

from neptune.common.backends.utils import with_api_exceptions_handler
from neptune.common.envs import API_TOKEN_ENV_NAME
from neptune.internal.backends.hosted_client import (
    DEFAULT_REQUEST_KWARGS,
    create_backend_client,
    create_http_client_with_auth,
    create_leaderboard_client,
)
from neptune.internal.backends.swagger_client_wrapper import SwaggerClientWrapper
from neptune.internal.backends.utils import (
    parse_validation_errors,
    ssl_verify,
)
from neptune.internal.credentials import Credentials
from neptune.internal.id_formats import QualifiedName
from neptune.internal.utils import (
    verify_collection_type,
    verify_type,
)
from neptune.internal.utils.iteration import get_batches
from neptune.internal.utils.logger import logger
from neptune.management.exceptions import (
    AccessRevokedOnDeletion,
    AccessRevokedOnMemberRemoval,
    AccessRevokedOnServiceAccountRemoval,
    BadRequestException,
    ProjectAlreadyExists,
    ProjectNotFound,
    ServiceAccountAlreadyHasAccess,
    ServiceAccountNotExistsOrWithoutAccess,
    ServiceAccountNotFound,
    UserAlreadyHasAccess,
    UserAlreadyInvited,
    UserNotExistsOrWithoutAccess,
    WorkspaceNotFound,
    WorkspaceOrUserNotFound,
)
from neptune.management.internal.dto import (
    ProjectMemberRoleDTO,
    ProjectVisibilityDTO,
    ServiceAccountDTO,
    WorkspaceMemberRoleDTO,
)
from neptune.management.internal.types import ProjectVisibility
from neptune.management.internal.utils import (
    WorkspaceMemberRole,
    extract_project_and_workspace,
    normalize_project_name,
)

TRASH_BATCH_SIZE = 100


def _get_token(api_token: Optional[str] = None) -> str:
    return api_token or os.getenv(API_TOKEN_ENV_NAME)


def _get_http_client_and_config(api_token: Optional[str] = None):
    credentials = Credentials.from_token(api_token=_get_token(api_token=api_token))
    http_client, client_config = create_http_client_with_auth(
        credentials=credentials, ssl_verify=ssl_verify(), proxies={}
    )
    return http_client, client_config


def _get_backend_client(api_token: Optional[str] = None) -> SwaggerClientWrapper:
    http_client, client_config = _get_http_client_and_config(api_token)
    return create_backend_client(client_config=client_config, http_client=http_client)


def _get_leaderboard_client(api_token: Optional[str] = None) -> SwaggerClientWrapper:
    http_client, client_config = _get_http_client_and_config(api_token)
    return create_leaderboard_client(client_config=client_config, http_client=http_client)


def get_project_list(*, api_token: Optional[str] = None) -> List[str]:
    """Lists projects that the account has access to.

    Args:
        api_token: Account's API token.
            If None, the value of the NEPTUNE_API_TOKEN environment variable is used.
            Note: To keep your token secure, use the NEPTUNE_API_TOKEN environment variable rather than placing your
            API token in plain text in your source code.

    Returns:
        List of project names in the form "workspace-name/project-name".

    Example:
        >>> from neptune import management
        >>> management.get_project_list()

    You may also want to check the management API reference:
    https://docs.neptune.ai/api/management
    """
    verify_type("api_token", api_token, (str, type(None)))
    backend_client = _get_backend_client(api_token=api_token)
    params = {
        "userRelation": "viewerOrHigher",
        "sortBy": ["lastViewed"],
        **DEFAULT_REQUEST_KWARGS,
    }
    projects = _get_projects(backend_client, params)

    return [normalize_project_name(name=project.name, workspace=project.organizationName) for project in projects]


@with_api_exceptions_handler
def _get_projects(backend_client, params) -> List:
    return backend_client.api.listProjects(**params).response().result.entries


def create_project(
    name: str,
    *,
    key: Optional[str] = None,
    workspace: Optional[str] = None,
    visibility: str = ProjectVisibility.PRIVATE,
    description: Optional[str] = None,
    api_token: Optional[str] = None,
) -> str:
    """Creates a new project in a Neptune workspace.

    Args:
        name: The name for the project in Neptune. Can contain letters and hyphens. For example, "classification".
            If you leave out the workspace argument, include the workspace name here,
            in the form "workspace-name/project-name". For example, "ml-team/classification".
        key: Project identifier. Must contain 1-10 upper case letters or numbers (at least one letter).
            For example, "CLS2". If you leave it out, Neptune generates a project key for you.
        workspace: Name of your Neptune workspace.
            If None, it will be parsed from the name argument.
        visibility: Level of privacy for the project. Options:
            - "pub": Public. Anyone on the internet can see it.
            - "priv": Private. Only users specifically assigned to the project can access it. Requires a plan with
                project-level access control.
            - "workspace" (team workspaces only): Accessible to all workspace members.
            If None, it will be set to "priv".
        description: Project description.
            If None, it will be left empty.
        api_token: Account's API token.
            If None, the value of the NEPTUNE_API_TOKEN environment variable is used.
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
    https://docs.neptune.ai/api/management
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
        name=project_response.result.name,
        workspace=project_response.result.organizationName,
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


@with_api_exceptions_handler
def delete_project(project: str, *, workspace: Optional[str] = None, api_token: Optional[str] = None):
    """Deletes a project from a Neptune workspace.

    To delete projects, the user must be a workspace admin.

    Args:
        project: The name of the project in Neptune in the form "workspace-name/project-name".
            If you pass the workspace argument, the "project" argument should only contain "project-name"
            instead of "workspace-name/project-name".
        workspace: Name of your Neptune workspace. If you specify it,
            change the format of the project argument to "project-name" instead of "workspace-name/project-name".
            If None, it will be parsed from the project argument.
        api_token: User's API token.
            If None, the value of the NEPTUNE_API_TOKEN environment variable is used.
            Note: To keep your token secure, use the NEPTUNE_API_TOKEN environment variable rather than placing your
            API token in plain text in your source code.

    Example:
        >>> from neptune import management
        >>> management.delete_project(project="ml-team/classification")

    You may also want to check the management API reference:
    https://docs.neptune.ai/api/management
    """
    verify_type("project", project, str)
    verify_type("workspace", workspace, (str, type(None)))
    verify_type("api_token", api_token, (str, type(None)))

    backend_client = _get_backend_client(api_token=api_token)
    project_identifier = normalize_project_name(name=project, workspace=workspace)

    params = {"projectIdentifier": project_identifier, **DEFAULT_REQUEST_KWARGS}

    try:
        backend_client.api.deleteProject(**params).response()
    except HTTPNotFound as e:
        raise ProjectNotFound(name=project_identifier) from e
    except HTTPForbidden as e:
        raise AccessRevokedOnDeletion(name=project_identifier) from e


@with_api_exceptions_handler
def add_project_member(
    project: str,
    username: str,
    role: str,
    *,
    workspace: Optional[str] = None,
    api_token: Optional[str] = None,
):
    """Adds a member to a Neptune project.

    Only project owners can add members.

    Args:
        project: The name of the project in Neptune in the form "workspace-name/project-name".
            If you pass the workspace argument, the project argument should only contain "project-name"
            instead of "workspace-name/project-name".
        username: Name of the user to add to the project.
        role: level of permissions the user should have in a project. Options:
            - "viewer": Can only view project content and members.
            - "contributor": Can view and edit project content. Can view members.
            - "owner": Can view and edit project content and members.
            For more information, see https://docs.neptune.ai/management/roles/
        workspace: Name of your Neptune workspace. If you specify it,
            change the format of the project argument to "project-name" instead of "workspace-name/project-name".
            If None, it will be parsed from the project argument.
        api_token: Account's API token.
            If None, the value of the NEPTUNE_API_TOKEN environment variable is used.
            Note: To keep your token secure, use the NEPTUNE_API_TOKEN environment variable rather than placing your
            API token in plain text in your source code.

    Examples:
        >>> from neptune import management
        >>> management.add_project_member(
        ...     workspace="ml-team",
        ...     project="classification",
        ...     username="johnny",
        ...     role="contributor",
        ... )

    You may also want to check the management API reference:
    https://docs.neptune.ai/api/management
    """
    verify_type("project", project, str)
    verify_type("username", username, str)
    verify_type("role", role, str)
    verify_type("workspace", workspace, (str, type(None)))
    verify_type("api_token", api_token, (str, type(None)))

    backend_client = _get_backend_client(api_token=api_token)
    project_identifier = normalize_project_name(name=project, workspace=workspace)

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
        members = get_project_member_list(project=project, workspace=workspace, api_token=api_token)
        user_role = members.get(username)
        raise UserAlreadyHasAccess(user=username, project=project_identifier, role=user_role) from e


@with_api_exceptions_handler
def get_project_member_list(
    project: str, *, workspace: Optional[str] = None, api_token: Optional[str] = None
) -> Dict[str, str]:
    """Lists members of a Neptune project.

    Args:
        project: The name of the project in Neptune in the form "workspace-name/project-name".
            If you pass the workspace argument, the project argument should only contain "project-name"
            instead of "workspace-name/project-name".
        workspace: Name of your Neptune workspace. If you specify it,
            change the format of the project argument to "project-name" instead of "workspace-name/project-name".
            If None, it will be parsed from the project argument.
        api_token: Account's API token.
            If None, the value of the NEPTUNE_API_TOKEN environment variable is used.
            Note: To keep your token secure, use the NEPTUNE_API_TOKEN environment variable rather than placing your
            API token in plain text in your source code.

    Returns:
        Dictionary with usernames as keys and project member roles
        ("owner", "contributor", "viewer") as values.

    Example:
        >>> from neptune import management
        >>> management.get_project_member_list(project="ml-team/classification")

    You may also want to check the management API reference:
    https://docs.neptune.ai/api/management
    """
    verify_type("project", project, str)
    verify_type("workspace", workspace, (str, type(None)))
    verify_type("api_token", api_token, (str, type(None)))

    backend_client = _get_backend_client(api_token=api_token)
    project_identifier = normalize_project_name(name=project, workspace=workspace)

    params = {"projectIdentifier": project_identifier, **DEFAULT_REQUEST_KWARGS}

    try:
        result = backend_client.api.listProjectMembers(**params).response().result
        return {f"{m.registeredMemberInfo.username}": ProjectMemberRoleDTO.to_domain(m.role) for m in result}
    except HTTPNotFound as e:
        raise ProjectNotFound(name=project_identifier) from e


@with_api_exceptions_handler
def remove_project_member(
    project: str,
    username: str,
    *,
    workspace: Optional[str] = None,
    api_token: Optional[str] = None,
):
    """Removes member from a Neptune project.

    Only project owners can remove members.

    Args:
        project: The name of the project in Neptune in the form "workspace-name/project-name".
            If you pass the workspace argument, the project argument should only contain "project-name"
            instead of "workspace-name/project-name".
        username: Name of the user to remove from the project.
        workspace: Name of your Neptune workspace. If you specify it,
            change the format of the project argument to "project-name" instead of "workspace-name/project-name".
            If None, it will be parsed from the project argument.
        api_token: Account's API token.
            If None, the value of the NEPTUNE_API_TOKEN environment variable is used.
            Note: To keep your token secure, use the NEPTUNE_API_TOKEN environment variable rather than placing your
            API token in plain text in your source code.

    Example:
        >>> from neptune import management
        >>> management.remove_project_member(
        ...     project="ml-team/classification",
        ...     username="johnny",
        ... )

    You may also want to check the management API reference:
    https://docs.neptune.ai/api/management
    """
    verify_type("project", project, str)
    verify_type("username", username, str)
    verify_type("workspace", workspace, (str, type(None)))
    verify_type("api_token", api_token, (str, type(None)))

    backend_client = _get_backend_client(api_token=api_token)
    project_identifier = normalize_project_name(name=project, workspace=workspace)

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
def get_workspace_member_list(workspace: str, *, api_token: Optional[str] = None) -> Dict[str, str]:
    """Lists members of a Neptune workspace.

    Args:
        workspace: Name of the Neptune workspace.
        api_token: Account's API token.
            If None, the value of the NEPTUNE_API_TOKEN environment variable is used.
            Note: To keep your token secure, use the NEPTUNE_API_TOKEN environment variable rather than placing your
            API token in plain text in your source code.

    Returns:
        Dictionary with usernames as keys and workspace member roles ("admin", "member") as values.

    Example:
        >>> from neptune import management
        >>> management.get_workspace_member_list(workspace="ml-team")

    You may also want to check the management API reference:
    https://docs.neptune.ai/api/management
    """
    verify_type("workspace", workspace, str)
    verify_type("api_token", api_token, (str, type(None)))

    backend_client = _get_backend_client(api_token=api_token)

    params = {"organizationIdentifier": workspace, **DEFAULT_REQUEST_KWARGS}

    try:
        result = backend_client.api.listOrganizationMembers(**params).response().result
        return {f"{m.registeredMemberInfo.username}": WorkspaceMemberRoleDTO.to_domain(m.role) for m in result}
    except HTTPNotFound as e:
        raise WorkspaceNotFound(workspace=workspace) from e


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
        return {f"{sa.displayName}": ServiceAccountDTO(name=sa.displayName, id=sa.id) for sa in result}
    except HTTPNotFound as e:
        raise WorkspaceNotFound(workspace=workspace_name) from e


@with_api_exceptions_handler
def get_workspace_service_account_list(workspace: str, *, api_token: Optional[str] = None) -> Dict[str, str]:
    """Lists service accounts of a Neptune workspace.

    Args:
        workspace: Name of the Neptune workspace.
        api_token: Account's API token.
            If None, the value of the NEPTUNE_API_TOKEN environment variable is used.
            Note: To keep your token secure, use the NEPTUNE_API_TOKEN environment variable rather than placing your
            API token in plain text in your source code.
    Returns:
        Dictionary with account names as keys and workspace member roles as values.
        Service accounts can only have the "member" role in workspaces.

    Example:
        >>> from neptune import management
        >>> management.get_workspace_service_account_list(workspace="ml-team")

    You may also want to check the management API reference:
    https://docs.neptune.ai/api/management
    """
    service_accounts = _get_raw_workspace_service_account_list(workspace_name=workspace, api_token=api_token)

    return {
        service_account_name: WorkspaceMemberRoleDTO.to_domain("member")
        for service_account_name, _ in service_accounts.items()
    }


@with_api_exceptions_handler
def invite_to_workspace(
    *,
    username: Optional[str] = None,
    email: Optional[str] = None,
    workspace: str,
    api_token: Optional[str] = None,
    role: Union[WorkspaceMemberRole, str] = WorkspaceMemberRole.MEMBER,
    add_to_all_projects: bool = False,
) -> None:
    """Creates an invitation to a Neptune workspace.

    Provide either the username or email of the person to invite.

    Args:
        username: username of the user to invite. Note: If you provide this, leave out the email argument.
        email: email of the user to invite. Note: If you provide this, leave out the username argument.
        workspace: Name of your Neptune workspace.
        api_token: User's API token.
            If None, the value of the NEPTUNE_API_TOKEN environment variable is used.
            Note: To keep your token secure, use the NEPTUNE_API_TOKEN environment variable rather than placing your
            API token in plain text in your source code.
        role: The workspace role that is to be granted to the invited user.
            You can choose between the following values: "admin" and "member".
        add_to_all_projects: Whether to add the invited user to all projects in the workspace.

    Example:
        >>> from neptune import management
        >>> management.invite_to_workspace(
        ...     username="user",
        ...     workspace="ml-team",
        ...     role="admin",
        ... )

    Learn more in the docs:
    - https://docs.neptune.ai/api/management/#invite_to_workspace
    - https://docs.neptune.ai/management/inviting_people/
    """
    verify_type("workspace", workspace, str)
    verify_type("role", role, (WorkspaceMemberRole, str))
    verify_type("add_to_all_projects", add_to_all_projects, bool)
    verify_type("username", username, (str, type(None)))
    verify_type("email", email, (str, type(None)))
    verify_type("api_token", api_token, (str, type(None)))

    if username and email:
        raise ValueError("Cannot specify both `username` and `email`.")

    if username:
        invitee = username
        invitation_type = "user"
    elif email:
        invitee = email
        invitation_type = "emailRecipient"
    else:
        raise ValueError("Neither `username` nor `email` arguments filled. At least one needs to be passed")

    if isinstance(role, str):
        role = WorkspaceMemberRole(role)

    params = {
        "newOrganizationInvitations": {
            "invitationsEntries": [
                {
                    "invitee": invitee,
                    "invitationType": invitation_type,
                    "roleGrant": role.to_api(),
                    "addToAllProjects": add_to_all_projects,
                }
            ],
            "organizationIdentifier": workspace,
        },
        **DEFAULT_REQUEST_KWARGS,
    }

    backend_client = _get_backend_client(api_token=api_token)
    try:
        backend_client.api.createOrganizationInvitations(**params)
    except HTTPNotFound:
        raise WorkspaceOrUserNotFound(workspace=workspace, user=invitee)
    except HTTPConflict:
        raise UserAlreadyInvited(user=invitee, workspace=workspace)


@with_api_exceptions_handler
def get_project_service_account_list(
    project: str, *, workspace: Optional[str] = None, api_token: Optional[str] = None
) -> Dict[str, str]:
    """Lists service accounts assigned to a Neptune project.

    Args:
        project: The name of the project in Neptune in the form "workspace-name/project-name".
            If you pass the workspace argument, the project argument should only contain "project-name"
            instead of "workspace-name/project-name".
        workspace: Name of your Neptune workspace. If you specify it,
            change the format of the project argument to "project-name" instead of "workspace-name/project-name".
            If None, it will be parsed from the project argument.
        api_token: Account's API token.
            If None, the value of the NEPTUNE_API_TOKEN environment variable is used.
            Note: To keep your token secure, use the NEPTUNE_API_TOKEN environment variable rather than placing your
            API token in plain text in your source code.
    Returns:
        Dictionary with account names as keys and project member roles
        ("owner", "contributor", "viewer") as values.

    Example:
        >>> from neptune import management
        >>> management.get_project_service_account_list(
        ...     workspace="ml-team",
        ...     project="classification",
        ... )

    You may also want to check the management API reference:
    https://docs.neptune.ai/api/management
    """
    verify_type("project", project, str)
    verify_type("workspace", workspace, (str, type(None)))
    verify_type("api_token", api_token, (str, type(None)))

    backend_client = _get_backend_client(api_token=api_token)
    project_identifier = normalize_project_name(name=project, workspace=workspace)

    params = {"projectIdentifier": project_identifier, **DEFAULT_REQUEST_KWARGS}

    try:
        result = backend_client.api.listProjectServiceAccounts(**params).response().result
        return {f"{sa.serviceAccountInfo.displayName}": ProjectMemberRoleDTO.to_domain(sa.role) for sa in result}
    except HTTPNotFound as e:
        raise ProjectNotFound(name=project_identifier) from e


@with_api_exceptions_handler
def add_project_service_account(
    project: str,
    service_account_name: str,
    role: str,
    *,
    workspace: Optional[str] = None,
    api_token: Optional[str] = None,
):
    """Adds a service account to a Neptune project.

    Only project owners can add accounts as members.

    Args:
        project: The name of the project in Neptune in the form "workspace-name/project-name".
            If you pass the workspace argument, the project argument should only contain "project-name"
            instead of "workspace-name/project-name".
        service_account_name: Name of the service account to add to the project.
        role: level of permissions the user or service account should have in a project.
            Can be set to:
             - "viewer": can only view project content and members
             - "contributor": can view and edit project content and only view members
             - "owner": can view and edit project content and members
            For more information, see https://docs.neptune.ai/management/roles/
        workspace: Name of your Neptune workspace. If you specify it,
            change the format of the project argument to "project-name" instead of "workspace-name/project-name".
            If None, it will be parsed from the project argument.
        api_token: Account's API token.
            If None, the value of the NEPTUNE_API_TOKEN environment variable is used.
            Note: To keep your token secure, use the NEPTUNE_API_TOKEN environment variable rather than placing your
            API token in plain text in your source code.

    Examples:
        >>> from neptune import management
        >>> management.add_project_service_account(
        ...     workspace="ml-team",
        ...     project="classification",
        ...     service_account_name="cicd@ml-team",
        ...     role="contributor",
        ... )

    You may also want to check the management API reference:
    https://docs.neptune.ai/api/management
    """
    verify_type("project", project, str)
    verify_type("service_account_name", service_account_name, str)
    verify_type("role", role, str)
    verify_type("workspace", workspace, (str, type(None)))
    verify_type("api_token", api_token, (str, type(None)))

    backend_client = _get_backend_client(api_token=api_token)
    workspace, project_name = extract_project_and_workspace(name=project, workspace=workspace)
    project_qualified_name = f"{workspace}/{project_name}"

    try:
        service_account = _get_raw_workspace_service_account_list(workspace_name=workspace, api_token=api_token)[
            service_account_name
        ]
    except KeyError as e:
        raise ServiceAccountNotFound(service_account_name=service_account_name, workspace=workspace) from e

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
        service_accounts = get_project_service_account_list(project=project, workspace=workspace, api_token=api_token)
        service_account_role = service_accounts.get(service_account_name)

        raise ServiceAccountAlreadyHasAccess(
            service_account_name=service_account_name,
            project=project_qualified_name,
            role=service_account_role,
        ) from e


@with_api_exceptions_handler
def remove_project_service_account(
    project: str,
    service_account_name: str,
    *,
    workspace: Optional[str] = None,
    api_token: Optional[str] = None,
):
    """Removes a service account from a Neptune project.

    Only project owners can remove accounts.

    Args:
        project: The name of the project in Neptune in the form "workspace-name/project-name".
            If you pass the workspace argument, the project argument should only contain "project-name"
            instead of "workspace-name/project-name".
        service_account_name: Name of the service account to remove from the project.
        workspace: Name of your Neptune workspace. If you specify it,
            change the format of the project argument to "project-name" instead of "workspace-name/project-name".
            If None, it will be parsed from the project argument.
        api_token: Account's API token.
            If None, the value of the NEPTUNE_API_TOKEN environment variable is used.
            Note: To keep your token secure, use the NEPTUNE_API_TOKEN environment variable rather than placing your
            API token in plain text in your source code.

    Examples:
        >>> from neptune import management
        >>> management.remove_project_service_account(
        ...     workspace="ml-team",
        ...     project="classification",
        ...     service_account_name="cicd@ml-team",
        ... )

    You may also want to check the management API reference:
    https://docs.neptune.ai/api/management
    """
    verify_type("project", project, str)
    verify_type("service_account_name", service_account_name, str)
    verify_type("workspace", workspace, (str, type(None)))
    verify_type("api_token", api_token, (str, type(None)))

    backend_client = _get_backend_client(api_token=api_token)
    workspace, project_name = extract_project_and_workspace(name=project, workspace=workspace)
    project_qualified_name = f"{workspace}/{project_name}"

    try:
        service_account = _get_raw_workspace_service_account_list(workspace_name=workspace, api_token=api_token)[
            service_account_name
        ]
    except KeyError as e:
        raise ServiceAccountNotFound(service_account_name=service_account_name, workspace=workspace) from e

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


def trash_objects(
    project: str,
    ids: Union[str, Iterable[str]],
    *,
    workspace: str = None,
    api_token: str = None,
) -> None:
    """Moves the specified Neptune objects to the project trash.

    Args:
        project: The name of the project in Neptune in the form "workspace-name/project-name".
            If you pass the workspace argument, the project argument should only contain "project-name"
            instead of "workspace-name/project-name".
        ids: Neptune ID of object to trash (or list of multiple IDs).
            You can find the ID in the leftmost column of the table view, and in the "sys/id" field of each object.
        workspace: Name of your Neptune workspace. If you specify it,
            change the format of the project argument to "project-name" instead of "workspace-name/project-name".
            If None, it will be parsed from the project argument.
        api_token: Account's API token.
            If None, the value of the NEPTUNE_API_TOKEN environment variable is used.
            Note: To keep your token secure, use the NEPTUNE_API_TOKEN environment variable rather than placing your
            API token in plain text in your source code.

    Examples:

        Trashing a run with the ID "CLS-1":
        >>> from neptune import management
        >>> management.trash_objects(project="ml-team/classification", ids="CLS-1")

        Trashing two runs and a model with the key "PRETRAINED":
        >>> management.trash_objects(
        ...     project="ml-team/classification",
        ...     ids=["CLS-2", "CLS-3", "CLS-PRETRAINED"]
        ... )
        Note: Trashing a model object also trashes all of its versions.

    For more, see the docs: https://docs.neptune.ai/api/management/#trash_objects
    """
    verify_type("project", project, str)
    verify_type("workspace", workspace, (str, type(None)))
    verify_type("api_token", api_token, (str, type(None)))
    if ids is not None:
        if isinstance(ids, str):
            ids = [ids]
        else:
            verify_collection_type("ids", ids, str)

    leaderboard_client = _get_leaderboard_client(api_token=api_token)
    workspace, project_name = extract_project_and_workspace(name=project, workspace=workspace)
    project_qualified_name = f"{workspace}/{project_name}"

    qualified_name_ids = [QualifiedName(f"{workspace}/{project_name}/{container_id}") for container_id in ids]
    errors = list()
    for batch_ids in get_batches(qualified_name_ids, batch_size=TRASH_BATCH_SIZE):
        params = {
            "projectIdentifier": project_qualified_name,
            "experimentIdentifiers": batch_ids,
            **DEFAULT_REQUEST_KWARGS,
        }
        response = leaderboard_client.api.trashExperiments(**params).response()
        errors += response.result.errors

    for error in errors:
        logger.warning(error)


def delete_objects_from_trash(
    project: str,
    ids: Union[str, Iterable[str]],
    *,
    workspace: Optional[str] = None,
    api_token: Optional[str] = None,
) -> None:
    """Permanently deletes the specified objects from the project trash.

    Args:
        project: The name of the project in Neptune in the form "workspace-name/project-name".
            If you pass the workspace argument, the project argument should only contain "project-name"
            instead of "workspace-name/project-name".
        ids: Neptune ID of object to delete from trash (or list of multiple IDs).
            You can find the ID in the leftmost column of the table view, and in the "sys/id" field of each object.
        workspace: Name of your Neptune workspace. If you specify it,
            change the format of the project argument to "project-name" instead of "workspace-name/project-name".
            If None, it will be parsed from the project argument.
        api_token: Account's API token.
            If None, the value of the NEPTUNE_API_TOKEN environment variable is used.
            Note: To keep your token secure, use the NEPTUNE_API_TOKEN environment variable rather than placing your
            API token in plain text in your source code.

    Examples:

        Deleting a run with the ID "CLS-1" from trash:
        >>> from neptune import management
        >>> management.delete_objects_from_trash(project="ml-team/classification", ids="CLS-1")

        Deleting two runs and a model with the key "PRETRAINED" from trash:
        >>> management.delete_objects_from_trash(
        ...     project="ml-team/classification",
        ...     ids=["CLS-2", "CLS-3", "CLS-PRETRAINED"],
        ... )
    """
    verify_type("project", project, str)
    verify_type("workspace", workspace, (str, type(None)))
    verify_type("api_token", api_token, (str, type(None)))

    workspace, project_name = extract_project_and_workspace(name=project, workspace=workspace)
    project_qualified_name = f"{workspace}/{project_name}"

    if isinstance(ids, str):
        ids = [ids]

    verify_collection_type("ids", ids, str)

    leaderboard_client = _get_leaderboard_client(api_token=api_token)

    qualified_name_ids = [QualifiedName(f"{workspace}/{project_name}/{container_id}") for container_id in ids]
    for batch_ids in get_batches(qualified_name_ids, batch_size=TRASH_BATCH_SIZE):
        params = {
            "projectIdentifier": project_qualified_name,
            "experimentIdentifiers": batch_ids,
            **DEFAULT_REQUEST_KWARGS,
        }
        response = leaderboard_client.api.deleteExperiments(**params).response()

        for error in response.result.errors:
            logger.warning(error)


def clear_trash(
    project: str,
    *,
    workspace: Optional[str] = None,
    api_token: Optional[str] = None,
) -> None:
    """Permanently deletes all Neptune objects from the project trash.

    Args:
        project: The name of the project in Neptune in the form "workspace-name/project-name".
            If you pass the workspace argument, the project argument should only contain "project-name"
            instead of "workspace-name/project-name".
        workspace: Name of your Neptune workspace. If you specify it,
            change the format of the project argument to "project-name" instead of "workspace-name/project-name".
            If None, it will be parsed from the project argument.
        api_token: Account's API token.
            If None, the value of the NEPTUNE_API_TOKEN environment variable is used.
            Note: To keep your token secure, use the NEPTUNE_API_TOKEN environment variable rather than placing your
            API token in plain text in your source code.

    Example:
        >>> from neptune import management
        >>> management.clear_trash(project="ml-team/classification")
    """
    verify_type("project", project, str)
    verify_type("workspace", workspace, (str, type(None)))
    verify_type("api_token", api_token, (str, type(None)))

    leaderboard_client = _get_leaderboard_client(api_token=api_token)

    workspace, project_name = extract_project_and_workspace(name=project, workspace=workspace)
    project_qualified_name = f"{workspace}/{project_name}"

    params = {
        "projectIdentifier": project_qualified_name,
        **DEFAULT_REQUEST_KWARGS,
    }

    response = leaderboard_client.api.deleteAllExperiments(**params).response()

    for error in response.result.errors:
        logger.warning(error)
