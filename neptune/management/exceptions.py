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
        "If the project visibility is set to workspace, a user cannot be added or removed."
    )


class UserAlreadyHasAccess(ManagementOperationFailure):
    code = 10
    description = (
        'User "{user}" already has access to the project "{project}". Role already set to "{role}".'
    )


class ProjectsLimitReached(ManagementOperationFailure):
    code = 11
    description = "Project number limit reached."


class UnsupportedValue(ManagementOperationFailure):
    code = 12
    description = "{enum} cannot have value {value}"


class BadRequestException(ManagementOperationFailure):
    code = 400
    description = "Your request has encountered following validation errors: {validation_errors}"
