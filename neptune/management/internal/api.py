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

import urllib3

from neptune.new.internal.credentials import Credentials
from neptune.new.envs import NEPTUNE_ALLOW_SELF_SIGNED_CERTIFICATE, API_TOKEN_ENV_NAME
from neptune.management.internal.utils import normalize_project_name
from neptune.new.internal.backends.hosted_client import (
    create_backend_client,
    create_http_client_with_auth,
    get_client_config,
    DEFAULT_REQUEST_KWARGS,
)
from neptune.patterns import PROJECT_QUALIFIED_NAME_PATTERN
from neptune.management.internal.types import *


def _get_token(api_token: Optional[str] = None):
    return api_token or os.getenv(API_TOKEN_ENV_NAME)


def _ssl_verify():
    if os.getenv(NEPTUNE_ALLOW_SELF_SIGNED_CERTIFICATE):
        urllib3.disable_warnings()
        return False

    return True


def _get_backend_client(api_token: Optional[str] = None):
    credentials = Credentials(api_token=_get_token(api_token=api_token))
    client_config = get_client_config(
        credentials=credentials,
        ssl_verify=_ssl_verify(),
        proxies={'dupa': 'dupa'}
    )
    http_client = create_http_client_with_auth(
        credentials=credentials,
        ssl_verify=_ssl_verify(),
        proxies={'dupa': 'dupa'}
    )
    return create_backend_client(
        client_config=client_config,
        http_client=http_client
    )


def get_project_list(api_token: Optional[str] = None) -> List[str]:
    backend_client = _get_backend_client(api_token=api_token)

    response = backend_client.api.listProjects(
        userRelation='viewerOrHigher',
        sortBy=['lastViewed'],
        **DEFAULT_REQUEST_KWARGS,
    ).response()

    return list(map(lambda p: f"{p.organizationName}/{p.name}", response.result.entries))


def create_project(
        name: str,
        key: str,
        workspace: Optional[str] = None,
        visibility: Optional[ProjectVisibility] = None,
        description: Optional[str] = None,
        api_token: Optional[str] = None
) -> str:
    backend_client = _get_backend_client(api_token=api_token)
    project_identifier = normalize_project_name(name=name, workspace=workspace)

    project_spec = re.search(PROJECT_QUALIFIED_NAME_PATTERN, project_identifier)
    workspace, name = project_spec['workspace'], project_spec['project']

    response = backend_client.api.listOrganizations(
        **DEFAULT_REQUEST_KWARGS
    ).response().result

    mapper = {
        f'{f.name}': f.id for f in response
    }

    response = backend_client.api.createProject(
        projectToCreate={
            'name': name,
            'description': description,
            'projectKey': key,
            'organizationId': mapper[workspace],
            'visibility': visibility.value
        },
        **DEFAULT_REQUEST_KWARGS,
    ).response()


def delete_project(name, workspace=None, api_token=None):
    backend_client = _get_backend_client(api_token=api_token)
    project_identifier = normalize_project_name(name=name, workspace=workspace)

    response = backend_client.api.deleteProject(
        projectIdentifier=project_identifier,
        **DEFAULT_REQUEST_KWARGS,
    ).response()


def add_project_member(
        name: str,
        username: str,
        role: MemberRole,
        workspace: Optional[str] = None,
        api_token: Optional[str] = None
):
    backend_client = _get_backend_client(api_token=api_token)
    project_identifier = normalize_project_name(name=name, workspace=workspace)

    response = backend_client.api.addProjectMember(
        projectIdentifier=project_identifier,
        member={
            'userId': username,
            'role': role.value
        },
        **DEFAULT_REQUEST_KWARGS,
    ).response()


def get_project_member_list(name, workspace=None, api_token=None) -> Dict[str, str]:
    backend_client = _get_backend_client(api_token=api_token)
    project_identifier = normalize_project_name(name=name, workspace=workspace)

    response = backend_client.api.listProjectMembers(
        projectIdentifier=project_identifier,
        **DEFAULT_REQUEST_KWARGS,
    ).response()


def remove_project_member(name, username, workspace=None, api_token=None):
    backend_client = _get_backend_client(api_token=api_token)
    project_identifier = normalize_project_name(name=name, workspace=workspace)

    response = backend_client.api.deleteProjectMember(
        projectIdentifier=project_identifier,
        userId=username,
        **DEFAULT_REQUEST_KWARGS,
    ).response()


def get_workspace_member_list(name, api_token=None) -> Dict[str, str]:
    backend_client = _get_backend_client(api_token=api_token)

    response = backend_client.api.listOrganizationMembers(
        organizationIdentifier=name,
        **DEFAULT_REQUEST_KWARGS,
    ).response().result

    return {
        f'{m.registeredMemberInfo.username}': m.role for m in response
    }
