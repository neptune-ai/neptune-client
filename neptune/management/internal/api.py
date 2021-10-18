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
from bravado.exception import HTTPNotFound, HTTPBadRequest, HTTPConflict, HTTPForbidden, HTTPUnprocessableEntity

from neptune.patterns import PROJECT_QUALIFIED_NAME_PATTERN
from neptune.new.envs import API_TOKEN_ENV_NAME
from neptune.new.internal.utils import verify_type
from neptune.new.internal.credentials import Credentials
from neptune.new.internal.backends.hosted_client import (
    create_backend_client,
    create_http_client_with_auth,
    DEFAULT_REQUEST_KWARGS,
)
from neptune.new.internal.backends.utils import with_api_exceptions_handler, ssl_verify, parse_validation_errors
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


def _get_token(api_token: Optional[str] = None) -> str:
    return api_token or os.getenv(API_TOKEN_ENV_NAME)


def _get_backend_client(api_token: Optional[str] = None) -> SwaggerClient:
    credentials = Credentials.from_token(api_token=_get_token(api_token=api_token))
    http_client, client_config = create_http_client_with_auth(
        credentials=credentials,
        ssl_verify=ssl_verify(),
        proxies={}
    )
    return create_backend_client(
        client_config=client_config,
        http_client=http_client
    )


@with_api_exceptions_handler
def get_project_list(api_token: Optional[str] = None) -> List[str]:
    verify_type('api_token', api_token, (str, type(None)))

    backend_client = _get_backend_client(api_token=api_token)

    params = {
        'userRelation': 'viewerOrHigher',
        'sortBy': ['lastViewed'],
        **DEFAULT_REQUEST_KWARGS
    }

    projects = backend_client.api.listProjects(**params).response().result.entries

    return [normalize_project_name(name=project.name, workspace=project.organizationName) for project in projects]


@with_api_exceptions_handler
def create_project(
        name: str,
        key: str,
        workspace: Optional[str] = None,
        visibility: ProjectVisibility = ProjectVisibility.PRIVATE,
        description: Optional[str] = None,
        api_token: Optional[str] = None
) -> str:
    verify_type('name', name, str)
    verify_type('key', key, str)
    verify_type('workspace', workspace, (str, type(None)))
    verify_type('visibility', visibility, ProjectVisibility)
    verify_type('description', description, (str, type(None)))
    verify_type('api_token', api_token, (str, type(None)))

    backend_client = _get_backend_client(api_token=api_token)
    project_identifier = normalize_project_name(name=name, workspace=workspace)

    project_spec = re.search(PROJECT_QUALIFIED_NAME_PATTERN, project_identifier)
    workspace, name = project_spec['workspace'], project_spec['project']

    try:
        workspaces = backend_client.api.listOrganizations(**DEFAULT_REQUEST_KWARGS).response().result
        workspace_name_to_id = {f'{f.name}': f.id for f in workspaces}
    except HTTPNotFound:
        raise WorkspaceNotFound(workspace=workspace)

    if workspace not in workspace_name_to_id:
        raise WorkspaceNotFound(workspace=workspace)

    params = {
        'projectToCreate': {
            'name': name,
            'description': description,
            'projectKey': key,
            'organizationId': workspace_name_to_id[workspace],
            'visibility': visibility.value
        },
        **DEFAULT_REQUEST_KWARGS
    }

    try:
        response = backend_client.api.createProject(**params).response()
        return normalize_project_name(name=response.result.name, workspace=response.result.organizationName)
    except HTTPBadRequest as e:
        validation_errors = parse_validation_errors(error=e)
        if 'ERR_NOT_UNIQUE' in validation_errors:
            raise ProjectAlreadyExists(name=project_identifier) from e
        raise BadRequestException(validation_errors=validation_errors)
    except HTTPUnprocessableEntity as e:
        raise ProjectsLimitReached() from e


@with_api_exceptions_handler
def delete_project(name: str, workspace: Optional[str] = None, api_token: Optional[str] = None):
    verify_type('name', name, str)
    verify_type('workspace', workspace, (str, type(None)))
    verify_type('api_token', api_token, (str, type(None)))

    backend_client = _get_backend_client(api_token=api_token)
    project_identifier = normalize_project_name(name=name, workspace=workspace)

    params = {
        'projectIdentifier': project_identifier,
        **DEFAULT_REQUEST_KWARGS
    }

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
        role: MemberRole,
        workspace: Optional[str] = None,
        api_token: Optional[str] = None
):
    verify_type('name', name, str)
    verify_type('username', username, str)
    verify_type('role', role, MemberRole)
    verify_type('workspace', workspace, (str, type(None)))
    verify_type('api_token', api_token, (str, type(None)))

    backend_client = _get_backend_client(api_token=api_token)
    project_identifier = normalize_project_name(name=name, workspace=workspace)

    params = {
        'projectIdentifier': project_identifier,
        'member': {
            'userId': username,
            'role': role.value
        },
        **DEFAULT_REQUEST_KWARGS
    }

    try:
        backend_client.api.addProjectMember(**params).response()
    except HTTPNotFound as e:
        raise ProjectNotFound(name=project_identifier) from e
    except HTTPConflict as e:
        raise UserAlreadyHasAccess(user=username, project=project_identifier) from e


@with_api_exceptions_handler
def get_project_member_list(
        name: str,
        workspace: Optional[str] = None,
        api_token: Optional[str] = None
) -> Dict[str, str]:
    verify_type('name', name, str)
    verify_type('workspace', workspace, (str, type(None)))
    verify_type('api_token', api_token, (str, type(None)))

    backend_client = _get_backend_client(api_token=api_token)
    project_identifier = normalize_project_name(name=name, workspace=workspace)

    params = {
        'projectIdentifier': project_identifier,
        **DEFAULT_REQUEST_KWARGS
    }

    try:
        result = backend_client.api.listProjectMembers(**params).response().result
        return {f'{m.registeredMemberInfo.username}': m.role for m in result}
    except HTTPNotFound as e:
        raise ProjectNotFound(name=project_identifier) from e


@with_api_exceptions_handler
def remove_project_member(
        name: str,
        username: str,
        workspace: Optional[str] = None,
        api_token: Optional[str] = None
):
    verify_type('name', name, str)
    verify_type('username', username, str)
    verify_type('workspace', workspace, (str, type(None)))
    verify_type('api_token', api_token, (str, type(None)))

    backend_client = _get_backend_client(api_token=api_token)
    project_identifier = normalize_project_name(name=name, workspace=workspace)

    params = {
        'projectIdentifier': project_identifier,
        'userId': username,
        **DEFAULT_REQUEST_KWARGS
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
    verify_type('name', name, str)
    verify_type('api_token', api_token, (str, type(None)))

    backend_client = _get_backend_client(api_token=api_token)

    params = {
        'organizationIdentifier': name,
        **DEFAULT_REQUEST_KWARGS
    }

    try:
        result = backend_client.api.listOrganizationMembers(**params).response().result
        return {f'{m.registeredMemberInfo.username}': m.role for m in result}
    except HTTPNotFound as e:
        raise WorkspaceNotFound(workspace=name) from e
