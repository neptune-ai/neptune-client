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

REGISTERED_CODES = dict()


class ManagementOperationFailure(Exception):
    code = -1
    description = "Unknown error"

    def __init__(self, **kwargs):
        super().__init__()
        self._properties: dict = kwargs or {}

    def __str__(self):
        return f"{self.description.format(**self._properties)} (code: {self.code})"

    def __init_subclass__(cls):
        previous = REGISTERED_CODES.get(cls.code)
        assert previous is None, f"{cls} cannot have code {cls.code} already used by {previous}"
        REGISTERED_CODES[cls.code] = cls

    @property
    def details(self):
        return {
            "code": self.code,
            "description": self.description.format(**self._properties),
        }


class InvalidProjectName(ManagementOperationFailure):
    code = 1
    description = 'Provided project name "{name}" could not be parsed.'


class MissingWorkspaceName(ManagementOperationFailure):
    code = 2
    description = 'Cannot resolve project "{name}", you have to provide a workspace name.'


class ConflictingWorkspaceName(ManagementOperationFailure):
    code = 3
    description = 'Project name "{name}" conflicts with provided workspace "{workspace}".'


class ProjectNotFound(ManagementOperationFailure):
    code = 4
    description = 'Project "{name}" could not be found.'


class WorkspaceNotFound(ManagementOperationFailure):
    code = 5
    description = 'Workspace "{workspace}" could not be found.'


class ProjectAlreadyExists(ManagementOperationFailure):
    code = 6
    description = 'Project "{name}" already exists.'


class AccessRevokedOnDeletion(ManagementOperationFailure):
    code = 7
    description = 'Not enough permissions to delete project "{name}".'


class AccessRevokedOnMemberRemoval(ManagementOperationFailure):
    code = 8
    description = 'Not enough permissions to remove user "{user}" from project "{project}".'


class UserNotExistsOrWithoutAccess(ManagementOperationFailure):
    code = 9
    description = (
        'User "{user}" does not exist or has no access to project "{project}". '
        "If the project visibility is set to 'workspace', a user cannot be added or removed."
    )


class UserAlreadyHasAccess(ManagementOperationFailure):
    code = 10
    description = 'User "{user}" already has access to the project "{project}". Their role is "{role}".'


class ProjectsLimitReached(ManagementOperationFailure):
    code = 11
    description = "Project number limit reached."


class UnsupportedValue(ManagementOperationFailure):
    code = 12
    description = "{enum} cannot have value {value}"


class ServiceAccountAlreadyHasAccess(ManagementOperationFailure):
    code = 13
    description = (
        'The service account "{service_account_name}" already has access to the project "{project}", '
        "either because it was already added or because of the project's visibility setting. "
        'The role of the service account is "{role}".'
    )


class AccessRevokedOnServiceAccountRemoval(ManagementOperationFailure):
    code = 14
    description = (
        'Not enough permissions to remove service account "{service_account_name}" from project "{project}". '
        "The account that performs the removal must be a project owner."
    )


class ServiceAccountNotExistsOrWithoutAccess(ManagementOperationFailure):
    code = 15
    description = (
        'Service account "{service_account_name}" does not exist or did not have access to project "{project}" '
        'in the first place. If the project visibility is set to "workspace", you cannot revoke access for '
        "invididual workspace members."
    )


class ServiceAccountNotFound(ManagementOperationFailure):
    code = 16
    description = 'Service account "{service_account_name}" could not be found in workspace "{workspace}"'


class ProjectKeyCollision(ManagementOperationFailure):
    code = 17
    description = 'Project with key "{key}" already exists.'


class ProjectNameCollision(ManagementOperationFailure):
    code = 18
    description = 'Project with name "{name}" already exists.'


class ProjectKeyInvalid(ManagementOperationFailure):
    code = 19
    description = 'Invalid project key "{key}": {reason}'


class ProjectNameInvalid(ManagementOperationFailure):
    code = 20
    description = 'Invalid project name "{name}": {reason}'


class BadRequestException(ManagementOperationFailure):
    code = 400
    description = "Your request has encountered the following validation errors: {validation_errors}"


class IncorrectIdentifierException(ManagementOperationFailure):
    code = 21
    description = "Can not parse '{identifier}' as identifier."


class ObjectNotFound(ManagementOperationFailure):
    code = 22
    description = "Object not found."


class WorkspaceOrUserNotFound(ManagementOperationFailure):
    code = 23
    description = "Workspace '{workspace}' or user '{user}' could not be found."


class UserAlreadyInvited(ManagementOperationFailure):
    code = 24
    description = "User '{user}' has already been invited to the workspace '{workspace}'."


class ProjectPrivacyRestrictedException(ManagementOperationFailure):
    code = 25
    description = (
        "Cannot set {requested} visibility for project. {followup}This might be caused by workspace "
        "settings or limited by your plan."
    )

    def __init__(self, **kwargs):
        modified_kwargs = {"followup": ""}
        allowed = kwargs.get("allowed")
        if allowed and isinstance(allowed, list):
            modified_kwargs["followup"] = "Allowed values are: {allowed}. ".format(
                allowed=", ".join(['"' + option + '"' for option in allowed])
            )
        modified_kwargs.update(kwargs)
        requested = modified_kwargs.get("requested")
        if not requested:
            modified_kwargs["requested"] = "the selected"
        else:
            modified_kwargs["requested"] = '"' + requested + '"'
        super().__init__(**modified_kwargs)


class ActiveProjectsLimitReachedException(ManagementOperationFailure):
    code = 26
    description = (
        "Limit of active projects reached. You can have up to {currentQuota} active projects simultaneously. "
        "To create a new project, you need to either archive an active project or increase the quota of active "
        "projects in the workspace."
    )
