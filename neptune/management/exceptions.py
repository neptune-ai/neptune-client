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
class ManagementOperationFailure(Exception):
    code = -1
    description = "Unknown error"

    def __init__(self, **kwargs):
        self._properties: dict = kwargs or {}

    def __str__(self):
        return f"{self.description.format(**self._properties)} (code: {self.code})"

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
