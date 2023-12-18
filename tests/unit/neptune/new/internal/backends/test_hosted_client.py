#
# Copyright (c) 2019, Neptune Labs Sp. z o.o.
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
import unittest
import uuid
from dataclasses import dataclass

import pytest
from bravado.exception import (
    HTTPBadRequest,
    HTTPConflict,
    HTTPForbidden,
    HTTPNotFound,
    HTTPUnprocessableEntity,
)
from bravado.testing.response_mocks import BravadoResponseMock
from mock import (
    MagicMock,
    Mock,
    patch,
)

from neptune.internal.backends.api_model import AttributeType
from neptune.internal.backends.hosted_client import (
    DEFAULT_REQUEST_KWARGS,
    _get_token_client,
    create_artifacts_client,
    create_backend_client,
    create_http_client_with_auth,
    create_leaderboard_client,
    get_client_config,
)
from neptune.internal.backends.hosted_neptune_backend import _get_column_type_from_entries
from neptune.internal.backends.utils import verify_host_resolution
from neptune.management import (
    MemberRole,
    add_project_member,
    create_project,
    delete_project,
    get_project_list,
    get_project_member_list,
    get_workspace_member_list,
    invite_to_workspace,
    remove_project_member,
)
from neptune.management.exceptions import (
    AccessRevokedOnDeletion,
    AccessRevokedOnMemberRemoval,
    ProjectAlreadyExists,
    ProjectNotFound,
    ProjectPrivacyRestrictedException,
    ProjectsLimitReached,
    UnsupportedValue,
    UserAlreadyHasAccess,
    UserNotExistsOrWithoutAccess,
    WorkspaceNotFound,
)
from tests.unit.neptune.backend_test_mixin import BackendTestMixin
from tests.unit.neptune.new.utils import response_mock

API_TOKEN = (
    "eyJhcGlfYWRkcmVzcyI6Imh0dHBzOi8vYXBwLnN0YWdlLm5lcHR1bmUubWwiLCJ"
    "hcGlfa2V5IjoiOTJhNzhiOWQtZTc3Ni00ODlhLWI5YzEtNzRkYmI1ZGVkMzAyIn0="
)


@patch("neptune.internal.backends.hosted_client.RequestsClient", new=MagicMock())
@patch("neptune.internal.backends.hosted_client.NeptuneAuthenticator", new=MagicMock())
@patch("bravado.client.SwaggerClient.from_url")
@patch("platform.platform", new=lambda: "testPlatform")
@patch("platform.python_version", new=lambda: "3.9.test")
class TestHostedClient(unittest.TestCase, BackendTestMixin):
    def setUp(self) -> None:
        # Clear all LRU storage
        verify_host_resolution.cache_clear()
        _get_token_client.cache_clear()
        get_client_config.cache_clear()
        create_http_client_with_auth.cache_clear()
        create_backend_client.cache_clear()
        create_leaderboard_client.cache_clear()
        create_artifacts_client.cache_clear()

    def test_project_listing_empty(self, swagger_client_factory):
        swagger_client = self._get_swagger_client_mock(swagger_client_factory)

        # given:
        swagger_client.api.listProjects.return_value.response = BravadoResponseMock(result=Mock(entries=[]))

        # when:
        returned_projects = get_project_list(api_token=API_TOKEN)

        # then:
        self.assertEqual([], returned_projects)

    def test_project_listing(self, swagger_client_factory):
        swagger_client = self._get_swagger_client_mock(swagger_client_factory)

        # given:
        project1 = MagicMock(organizationName="org1")
        project1.name = "project1"
        project2 = MagicMock(organizationName="org2")
        project2.name = "project2"
        projects = Mock(entries=[project1, project2])
        swagger_client.api.listProjects.return_value.response = BravadoResponseMock(
            result=projects,
        )

        # when:
        returned_projects = get_project_list(api_token=API_TOKEN)

        # then:
        self.assertEqual(["org1/project1", "org2/project2"], returned_projects)

    def test_invite_to_workspace(self, swagger_client_factory):
        # given:
        swagger_client = self._get_swagger_client_mock(swagger_client_factory)

        # when:
        invite_to_workspace(
            username="tester1",
            workspace="org2",
            api_token=API_TOKEN,
        )

        # then:
        swagger_client.api.createOrganizationInvitations.assert_called_once_with(
            newOrganizationInvitations={
                "invitationsEntries": [
                    {"invitee": "tester1", "invitationType": "user", "roleGrant": "member", "addToAllProjects": False}
                ],
                "organizationIdentifier": "org2",
            },
            **DEFAULT_REQUEST_KWARGS,
        )

    def test_invite_to_workspace_username_email_raises(self, swagger_client_factory):

        # neither specified
        self.assertRaises(ValueError, invite_to_workspace, workspace="org2", api_token=API_TOKEN)

        # both specified
        self.assertRaises(
            ValueError,
            invite_to_workspace,
            workspace="org2",
            api_token=API_TOKEN,
            username="user",
            email="email@email.com",
        )

    def test_invite_to_workspace_invalid_role_raises(self, swagger_client_factory):
        self.assertRaises(
            ValueError,
            invite_to_workspace,
            workspace="org2",
            username="user",
            api_token=API_TOKEN,
            role="non-existent-role",
        )
        self.assertRaises(
            ValueError, invite_to_workspace, workspace="org2", username="user", api_token=API_TOKEN, role="owner"
        )

    def test_workspace_members(self, swagger_client_factory):
        swagger_client = self._get_swagger_client_mock(swagger_client_factory)

        # given:
        members = [
            Mock(role="member", registeredMemberInfo=Mock(username="tester1")),
            Mock(role="owner", registeredMemberInfo=Mock(username="tester2")),
        ]
        swagger_client.api.listOrganizationMembers.return_value.response = BravadoResponseMock(
            result=members,
        )

        # when:
        returned_members = get_workspace_member_list(workspace="org2", api_token=API_TOKEN)

        # then:
        self.assertEqual({"tester1": "member", "tester2": "admin"}, returned_members)

    def test_workspace_members_empty(self, swagger_client_factory):
        swagger_client = self._get_swagger_client_mock(swagger_client_factory)

        # given:
        members = []
        swagger_client.api.listOrganizationMembers.return_value.response = BravadoResponseMock(
            result=members,
        )

        # when:
        returned_members = get_workspace_member_list(workspace="org2", api_token=API_TOKEN)

        # then:
        self.assertEqual({}, returned_members)

    def test_workspace_members_not_exists(self, swagger_client_factory):
        swagger_client = self._get_swagger_client_mock(swagger_client_factory)

        # when:
        swagger_client.api.listOrganizationMembers.side_effect = HTTPNotFound(response=response_mock())

        # then:
        with self.assertRaises(WorkspaceNotFound):
            get_workspace_member_list(workspace="org2", api_token=API_TOKEN)

    def test_project_members(self, swagger_client_factory):
        swagger_client = self._get_swagger_client_mock(swagger_client_factory)

        # given:
        members = [
            Mock(role="member", registeredMemberInfo=Mock(username="tester1")),
            Mock(role="manager", registeredMemberInfo=Mock(username="tester2")),
            Mock(role="viewer", registeredMemberInfo=Mock(username="tester3")),
        ]
        swagger_client.api.listProjectMembers.return_value.response = BravadoResponseMock(
            result=members,
        )

        # when:
        returned_members = get_project_member_list(project="org/proj", api_token=API_TOKEN)

        # then:
        self.assertEqual(
            {"tester1": "contributor", "tester2": "owner", "tester3": "viewer"},
            returned_members,
        )

    def test_project_members_empty(self, swagger_client_factory):
        swagger_client = self._get_swagger_client_mock(swagger_client_factory)

        # given:
        members = []
        swagger_client.api.listProjectMembers.return_value.response = BravadoResponseMock(
            result=members,
        )

        # when:
        returned_members = get_project_member_list(project="org/proj", api_token=API_TOKEN)

        # then:
        self.assertEqual({}, returned_members)

    def test_project_members_not_exists(self, swagger_client_factory):
        swagger_client = self._get_swagger_client_mock(swagger_client_factory)

        # when:
        swagger_client.api.listProjectMembers.side_effect = HTTPNotFound(response=response_mock())

        # then:
        with self.assertRaises(ProjectNotFound):
            get_project_member_list(project="org/proj", api_token=API_TOKEN)

    def test_delete_project_not_exists(self, swagger_client_factory):
        swagger_client = self._get_swagger_client_mock(swagger_client_factory)

        # when:
        swagger_client.api.deleteProject.side_effect = HTTPNotFound(response=response_mock())

        # then:
        with self.assertRaises(ProjectNotFound):
            delete_project(project="org/proj", api_token=API_TOKEN)

    def test_delete_project_permissions(self, swagger_client_factory):
        swagger_client = self._get_swagger_client_mock(swagger_client_factory)

        # when:
        swagger_client.api.deleteProject.side_effect = HTTPForbidden(response=response_mock())

        # then:
        with self.assertRaises(AccessRevokedOnDeletion):
            delete_project(project="org/proj", api_token=API_TOKEN)

    def test_create_project_already_exists(self, swagger_client_factory):
        swagger_client = self._get_swagger_client_mock(swagger_client_factory)

        # given:
        organization = Mock(id=str(uuid.uuid4()))
        organization.name = "org"
        organizations = [organization]

        # when:
        swagger_client.api.listOrganizations.return_value.response = BravadoResponseMock(
            result=organizations,
        )
        swagger_client.api.createProject.side_effect = HTTPBadRequest(
            response=response_mock(),
            swagger_result=MagicMock(
                code=None,
                errorType={"name": "validationError"},
                message=None,
                title="Validation Errors",
                type=None,
                validationErrors=[
                    {
                        "path": ["name"],
                        "errors": [{"errorCode": {"name": "ERR_NOT_UNIQUE"}}],
                    }
                ],
            ),
        )

        # then:
        with self.assertRaises(ProjectAlreadyExists):
            create_project(name="org/proj", key="PRJ", api_token=API_TOKEN)

    def test_create_project_unknown_visibility(self, swagger_client_factory):
        swagger_client = self._get_swagger_client_mock(swagger_client_factory)

        # given:
        organization = Mock(id=str(uuid.uuid4()))
        organization.name = "org"
        organizations = [organization]

        # when:
        swagger_client.api.listOrganizations.return_value.response = BravadoResponseMock(
            result=organizations,
        )

        with self.assertRaises(UnsupportedValue):
            create_project(
                name="org/proj",
                key="PRJ",
                visibility="unknown_value",
                api_token=API_TOKEN,
            )

    def test_create_project_no_workspace(self, swagger_client_factory):
        swagger_client = self._get_swagger_client_mock(swagger_client_factory)

        # given:
        organization = Mock(id=str(uuid.uuid4()))
        organization.name = "org"
        organizations = [organization]

        # when:
        swagger_client.api.listOrganizations.return_value.response = BravadoResponseMock(
            result=organizations,
        )

        # then:
        with self.assertRaises(WorkspaceNotFound):
            create_project(name="not_an_org/proj", key="PRJ", api_token=API_TOKEN)

    def test_create_project_limit_reached(self, swagger_client_factory):
        swagger_client = self._get_swagger_client_mock(swagger_client_factory)

        # given:
        organization = Mock(id=str(uuid.uuid4()))
        organization.name = "org"
        organizations = [organization]

        # when:
        swagger_client.api.listOrganizations.return_value.response = BravadoResponseMock(
            result=organizations,
        )
        response = response_mock()
        response.json.return_value = {
            "errorCode": 422,
            "errorType": "LIMIT_OF_PROJECTS_REACHED",
            "message": "Maximum number of projects (1000) reached",
        }
        swagger_client.api.createProject.side_effect = HTTPUnprocessableEntity(
            response=response,
        )

        # then:
        with self.assertRaises(ProjectsLimitReached):
            create_project(name="org/proj", key="PRJ", api_token=API_TOKEN)

    def test_create_project_private_not_allowed(self, swagger_client_factory):
        swagger_client = self._get_swagger_client_mock(swagger_client_factory)

        # given:
        organization = Mock(id=str(uuid.uuid4()))
        organization.name = "org"
        organizations = [organization]

        # when:
        swagger_client.api.listOrganizations.return_value.response = BravadoResponseMock(
            result=organizations,
        )
        response = response_mock()
        response.json.return_value = {
            "errorType": "VISIBILITY_RESTRICTED",
            "message": "Cannot set visibility priv for project. You are limited to: pub, workspace",
            "requestedValue": "priv",
            "allowedValues": ["pub", "workspace"],
        }
        swagger_client.api.createProject.side_effect = HTTPUnprocessableEntity(
            response=response,
        )

        # then:
        with self.assertRaisesRegex(ProjectPrivacyRestrictedException, '.*"priv" visibility.*'):
            create_project(name="org/proj", key="PRJ", visibility="priv", api_token=API_TOKEN)

    def test_create_project_private_not_allowed_no_details(self, swagger_client_factory):
        swagger_client = self._get_swagger_client_mock(swagger_client_factory)

        # given:
        organization = Mock(id=str(uuid.uuid4()))
        organization.name = "org"
        organizations = [organization]

        # when:
        swagger_client.api.listOrganizations.return_value.response = BravadoResponseMock(
            result=organizations,
        )
        response = response_mock()
        response.json.return_value = {
            "errorType": "VISIBILITY_RESTRICTED",
            "message": "Cannot set visibility priv for project. You are limited to: pub, workspace",
        }
        swagger_client.api.createProject.side_effect = HTTPUnprocessableEntity(
            response=response,
        )

        # then:
        with self.assertRaisesRegex(ProjectPrivacyRestrictedException, ".*selected visibility.*"):
            create_project(name="org/proj", key="PRJ", visibility="priv", api_token=API_TOKEN)

    def test_create_project(self, swagger_client_factory):
        swagger_client = self._get_swagger_client_mock(swagger_client_factory)

        # given:
        organization = Mock(id=str(uuid.uuid4()))
        organization.name = "org"
        organizations = [organization]

        # and:
        project = Mock(organizationName="org")
        project.name = "proj"

        # when:
        swagger_client.api.listOrganizations.return_value.response = BravadoResponseMock(
            result=organizations,
        )
        swagger_client.api.createProject.return_value.response = BravadoResponseMock(
            result=project,
        )

        # then:
        self.assertEqual("org/proj", create_project(name="org/proj", key="PRJ", api_token=API_TOKEN))

    def test_add_project_member_project_not_found(self, swagger_client_factory):
        swagger_client = self._get_swagger_client_mock(swagger_client_factory)

        # when:
        swagger_client.api.addProjectMember.side_effect = HTTPNotFound(
            response=response_mock(),
        )

        # then:
        with self.assertRaises(ProjectNotFound):
            add_project_member(
                project="org/proj",
                username="tester",
                role=MemberRole.VIEWER,
                api_token=API_TOKEN,
            )

    def test_add_project_member_unknown_role(self, swagger_client_factory):
        _ = self._get_swagger_client_mock(swagger_client_factory)

        # then:
        with self.assertRaises(UnsupportedValue):
            add_project_member(
                project="org/proj",
                username="tester",
                role="unknown_role",
                api_token=API_TOKEN,
            )

    def test_add_project_member_member_without_access(self, swagger_client_factory):
        swagger_client = self._get_swagger_client_mock(swagger_client_factory)

        # when:
        swagger_client.api.addProjectMember.side_effect = HTTPConflict(response=response_mock())

        # then:
        with self.assertRaises(UserAlreadyHasAccess):
            add_project_member(
                project="org/proj",
                username="tester",
                role=MemberRole.VIEWER,
                api_token=API_TOKEN,
            )

    def test_remove_project_member_project_not_found(self, swagger_client_factory):
        swagger_client = self._get_swagger_client_mock(swagger_client_factory)

        # when:
        swagger_client.api.deleteProjectMember.side_effect = HTTPNotFound(
            response=response_mock(),
        )

        # then:
        with self.assertRaises(ProjectNotFound):
            remove_project_member(project="org/proj", username="tester", api_token=API_TOKEN)

    def test_remove_project_member_no_user(self, swagger_client_factory):
        swagger_client = self._get_swagger_client_mock(swagger_client_factory)

        # when:
        swagger_client.api.deleteProjectMember.side_effect = HTTPUnprocessableEntity(
            response=response_mock(),
        )

        # then:
        with self.assertRaises(UserNotExistsOrWithoutAccess):
            remove_project_member(project="org/proj", username="tester", api_token=API_TOKEN)

    def test_remove_project_member_permissions(self, swagger_client_factory):
        swagger_client = self._get_swagger_client_mock(swagger_client_factory)

        # when:
        swagger_client.api.deleteProjectMember.side_effect = HTTPForbidden(
            response=response_mock(),
        )

        # then:
        with self.assertRaises(AccessRevokedOnMemberRemoval):
            remove_project_member(project="org/proj", username="tester", api_token=API_TOKEN)


def test__get_column_type_from_entries():
    @dataclass
    class DTO:
        type: str
        name: str = "test_column"

    # when
    test_cases = [
        {"entries": [], "exc": ValueError},
        {"entries": [DTO(type="float")], "result": AttributeType.FLOAT.value},
        {"entries": [DTO(type="string")], "result": AttributeType.STRING.value},
        {"entries": [DTO(type="float"), DTO(type="floatSeries")], "exc": ValueError},
        {"entries": [DTO(type="float"), DTO(type="int")], "result": AttributeType.FLOAT.value},
        {"entries": [DTO(type="float"), DTO(type="int"), DTO(type="datetime")], "result": AttributeType.STRING.value},
        {"entries": [DTO(type="float"), DTO(type="int"), DTO(type="string")], "result": AttributeType.STRING.value},
        {
            "entries": [DTO(type="float"), DTO(type="int"), DTO(type="string", name="test_column_different")],
            "result": AttributeType.FLOAT.value,
        },
    ]

    # then
    for tc in test_cases:
        exc = tc.get("exc", None)
        if exc is not None:
            with pytest.raises(exc):
                _get_column_type_from_entries(tc["entries"], column="test_column")
        else:
            result = _get_column_type_from_entries(tc["entries"], column="test_column")
            assert result == tc["result"]
