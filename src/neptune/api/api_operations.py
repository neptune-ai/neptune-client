#
# Copyright (c) 2023, Neptune Labs Sp. z o.o.
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
from typing import List

from neptune.common.backends.utils import with_api_exceptions_handler
from neptune.internal.backends.hosted_client import DEFAULT_REQUEST_KWARGS
from neptune.internal.backends.swagger_client_wrapper import SwaggerClientWrapper


def add_project_service_account(
    *, client: SwaggerClientWrapper, project_identifier: str, service_account_id: str, role: str
) -> None:
    params = {
        "projectIdentifier": project_identifier,
        "account": {
            "serviceAccountId": service_account_id,
            "role": ProjectMemberRoleDTO.from_str(role).value,
        },
        **DEFAULT_REQUEST_KWARGS,
    }

    try:
        client.api.addProjectServiceAccount(**params).response()
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
def delete_project_service_account(
    *, client: SwaggerClientWrapper, project_identifier: str, service_account_id: str
) -> None:
    params = {
        "projectIdentifier": project_identifier,
        "serviceAccountId": service_account_id,
        **DEFAULT_REQUEST_KWARGS,
    }

    try:
        client.api.deleteProjectServiceAccount(**params).response()
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


@with_api_exceptions_handler
def trash_experiments(*, client: SwaggerClientWrapper, project_identifier: str, batch_ids: List[str]):
    params = {
        "projectIdentifier": project_identifier,
        "experimentIdentifiers": batch_ids,
        **DEFAULT_REQUEST_KWARGS,
    }
    return client.api.trashExperiments(**params).response()
