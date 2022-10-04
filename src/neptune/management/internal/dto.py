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
from dataclasses import dataclass
from enum import Enum

from neptune.management.exceptions import UnsupportedValue
from neptune.management.internal.types import (
    ProjectMemberRole,
    ProjectVisibility,
    WorkspaceMemberRole,
)
from neptune.new.internal.utils import verify_type


class ProjectVisibilityDTO(Enum):
    PRIVATE = "priv"
    PUBLIC = "pub"
    WORKSPACE = "workspace"

    @classmethod
    def from_str(cls, visibility: str) -> "ProjectVisibilityDTO":
        verify_type("visibility", visibility, str)

        try:
            return {
                ProjectVisibility.PRIVATE: ProjectVisibilityDTO.PRIVATE,
                ProjectVisibility.PUBLIC: ProjectVisibilityDTO.PUBLIC,
                ProjectVisibility.WORKSPACE: ProjectVisibilityDTO.WORKSPACE,
            }[visibility]
        except KeyError as e:
            raise UnsupportedValue(enum=cls.__name__, value=visibility) from e


class ProjectMemberRoleDTO(Enum):
    VIEWER = "viewer"
    MEMBER = "member"
    MANAGER = "manager"

    @classmethod
    def from_str(cls, role: str) -> "ProjectMemberRoleDTO":
        verify_type("role", role, str)

        try:
            return {
                ProjectMemberRole.VIEWER: ProjectMemberRoleDTO.VIEWER,
                ProjectMemberRole.CONTRIBUTOR: ProjectMemberRoleDTO.MEMBER,
                ProjectMemberRole.OWNER: ProjectMemberRoleDTO.MANAGER,
            }[role]
        except KeyError as e:
            raise UnsupportedValue(enum=cls.__name__, value=role) from e

    @staticmethod
    def to_domain(role: str) -> str:
        verify_type("role", role, str)

        return {
            ProjectMemberRoleDTO.VIEWER.value: ProjectMemberRole.VIEWER,
            ProjectMemberRoleDTO.MANAGER.value: ProjectMemberRole.OWNER,
            ProjectMemberRoleDTO.MEMBER.value: ProjectMemberRole.CONTRIBUTOR,
        }.get(role)


class WorkspaceMemberRoleDTO(Enum):
    OWNER = "owner"
    MEMBER = "member"

    @staticmethod
    def to_domain(role: str) -> str:
        return {
            WorkspaceMemberRoleDTO.OWNER.value: WorkspaceMemberRole.ADMIN,
            WorkspaceMemberRoleDTO.MEMBER.value: WorkspaceMemberRole.MEMBER,
        }.get(role)


@dataclass
class ServiceAccountDTO:
    name: str
    id: str
