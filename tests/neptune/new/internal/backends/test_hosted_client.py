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
import uuid
import unittest

from mock import patch, MagicMock, Mock
from bravado.exception import (
    HTTPNotFound,
    HTTPForbidden,
    HTTPConflict,
    HTTPBadRequest,
    HTTPUnprocessableEntity,
)
from bravado.testing.response_mocks import BravadoResponseMock

from neptune.management import (
    get_project_list,
    get_workspace_member_list,
    get_project_member_list,
    delete_project,
    create_project,
    add_project_member,
    remove_project_member,
    MemberRole,
)
from neptune.management.exceptions import (
    WorkspaceNotFound,
    ProjectNotFound,
    AccessRevokedOnDeletion,
    ProjectAlreadyExists,
    UserNotExistsOrWithoutAccess,
    UserAlreadyHasAccess,
    AccessRevokedOnMemberRemoval,
    UnsupportedValue,
)
from neptune.new.internal.backends.utils import verify_host_resolution
from neptune.new.internal.backends.hosted_client import (
    _get_token_client,  # pylint:disable=protected-access
    get_client_config,
    create_http_client_with_auth,
    create_backend_client,
    create_leaderboard_client,
    create_artifacts_client,
)
from tests.neptune.new.backend_test_mixin import BackendTestMixin

API_TOKEN = (
    "eyJhcGlfYWRkcmVzcyI6Imh0dHBzOi8vYXBwLnN0YWdlLm5lcHR1bmUubWwiLCJ"
    "hcGlfa2V5IjoiOTJhNzhiOWQtZTc3Ni00ODlhLWI5YzEtNzRkYmI1ZGVkMzAyIn0="
)


@patch("neptune.new.internal.backends.hosted_client.RequestsClient", new=MagicMock())
@patch(
    "neptune.new.internal.backends.hosted_client.NeptuneAuthenticator", new=MagicMock()
)
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
        swagger_client.api.listProjects.return_value.response = BravadoResponseMock(
            result=Mock(entries=[])
        )

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

    def test_workspace_members(self, swagger_client_factory):
        swagger_client = self._get_swagger_client_mock(swagger_client_factory)

        # given:
        members = [
            Mock(role="member", registeredMemberInfo=Mock(username="tester1")),
            Mock(role="owner", registeredMemberInfo=Mock(username="tester2")),
        ]
        swagger_client.api.listOrganizationMembers.return_value.response = (
            BravadoResponseMock(
                result=members,
            )
        )

        # when:
        returned_members = get_workspace_member_list(name="org2", api_token=API_TOKEN)

        # then:
        self.assertEqual({"tester1": "member", "tester2": "admin"}, returned_members)

    def test_workspace_members_empty(self, swagger_client_factory):
        swagger_client = self._get_swagger_client_mock(swagger_client_factory)

        # given:
        members = []
        swagger_client.api.listOrganizationMembers.return_value.response = (
            BravadoResponseMock(
                result=members,
            )
        )

        # when:
        returned_members = get_workspace_member_list(name="org2", api_token=API_TOKEN)

        # then:
        self.assertEqual({}, returned_members)

    def test_workspace_members_not_exists(self, swagger_client_factory):
        swagger_client = self._get_swagger_client_mock(swagger_client_factory)

        # when:
        swagger_client.api.listOrganizationMembers.side_effect = HTTPNotFound(
            response=MagicMock()
        )

        # then:
        with self.assertRaises(WorkspaceNotFound):
            get_workspace_member_list(name="org2", api_token=API_TOKEN)

    def test_project_members(self, swagger_client_factory):
        swagger_client = self._get_swagger_client_mock(swagger_client_factory)

        # given:
        members = [
            Mock(role="member", registeredMemberInfo=Mock(username="tester1")),
            Mock(role="manager", registeredMemberInfo=Mock(username="tester2")),
            Mock(role="viewer", registeredMemberInfo=Mock(username="tester3")),
        ]
        swagger_client.api.listProjectMembers.return_value.response = (
            BravadoResponseMock(
                result=members,
            )
        )

        # when:
        returned_members = get_project_member_list(name="org/proj", api_token=API_TOKEN)

        # then:
        self.assertEqual(
            {"tester1": "contributor", "tester2": "owner", "tester3": "viewer"},
            returned_members,
        )

    def test_project_members_empty(self, swagger_client_factory):
        swagger_client = self._get_swagger_client_mock(swagger_client_factory)

        # given:
        members = []
        swagger_client.api.listProjectMembers.return_value.response = (
            BravadoResponseMock(
                result=members,
            )
        )

        # when:
        returned_members = get_project_member_list(name="org/proj", api_token=API_TOKEN)

        # then:
        self.assertEqual({}, returned_members)

    def test_project_members_not_exists(self, swagger_client_factory):
        swagger_client = self._get_swagger_client_mock(swagger_client_factory)

        # when:
        swagger_client.api.listProjectMembers.side_effect = HTTPNotFound(
            response=MagicMock()
        )

        # then:
        with self.assertRaises(ProjectNotFound):
            get_project_member_list(name="org/proj", api_token=API_TOKEN)

    def test_delete_project_not_exists(self, swagger_client_factory):
        swagger_client = self._get_swagger_client_mock(swagger_client_factory)

        # when:
        swagger_client.api.deleteProject.side_effect = HTTPNotFound(
            response=MagicMock()
        )

        # then:
        with self.assertRaises(ProjectNotFound):
            delete_project(name="org/proj", api_token=API_TOKEN)

    def test_delete_project_permissions(self, swagger_client_factory):
        swagger_client = self._get_swagger_client_mock(swagger_client_factory)

        # when:
        swagger_client.api.deleteProject.side_effect = HTTPForbidden(
            response=MagicMock()
        )

        # then:
        with self.assertRaises(AccessRevokedOnDeletion):
            delete_project(name="org/proj", api_token=API_TOKEN)

    def test_create_project_already_exists(self, swagger_client_factory):
        swagger_client = self._get_swagger_client_mock(swagger_client_factory)

        # given:
        organization = Mock(id=str(uuid.uuid4()))
        organization.name = "org"
        organizations = [organization]

        # when:
        swagger_client.api.listOrganizations.return_value.response = (
            BravadoResponseMock(
                result=organizations,
            )
        )
        swagger_client.api.createProject.side_effect = HTTPBadRequest(
            response=MagicMock(),
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
        swagger_client.api.listOrganizations.return_value.response = (
            BravadoResponseMock(
                result=organizations,
            )
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
        swagger_client.api.listOrganizations.return_value.response = (
            BravadoResponseMock(
                result=organizations,
            )
        )

        # then:
        with self.assertRaises(WorkspaceNotFound):
            create_project(name="not_an_org/proj", key="PRJ", api_token=API_TOKEN)

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
        swagger_client.api.listOrganizations.return_value.response = (
            BravadoResponseMock(
                result=organizations,
            )
        )
        swagger_client.api.createProject.return_value.response = BravadoResponseMock(
            result=project,
        )

        # then:
        self.assertEqual(
            "org/proj", create_project(name="org/proj", key="PRJ", api_token=API_TOKEN)
        )

    def test_add_project_member_project_not_found(self, swagger_client_factory):
        swagger_client = self._get_swagger_client_mock(swagger_client_factory)

        # when:
        swagger_client.api.addProjectMember.side_effect = HTTPNotFound(
            response=MagicMock()
        )

        # then:
        with self.assertRaises(ProjectNotFound):
            add_project_member(
                name="org/proj",
                username="tester",
                role=MemberRole.VIEWER,
                api_token=API_TOKEN,
            )

    def test_add_project_member_unknown_role(self, swagger_client_factory):
        _ = self._get_swagger_client_mock(swagger_client_factory)

        # then:
        with self.assertRaises(UnsupportedValue):
            add_project_member(
                name="org/proj",
                username="tester",
                role="unknown_role",
                api_token=API_TOKEN,
            )

    def test_add_project_member_member_without_access(self, swagger_client_factory):
        swagger_client = self._get_swagger_client_mock(swagger_client_factory)

        # when:
        swagger_client.api.addProjectMember.side_effect = HTTPConflict(
            response=MagicMock()
        )

        # then:
        with self.assertRaises(UserAlreadyHasAccess):
            add_project_member(
                name="org/proj",
                username="tester",
                role=MemberRole.VIEWER,
                api_token=API_TOKEN,
            )

    def test_remove_project_member_project_not_found(self, swagger_client_factory):
        swagger_client = self._get_swagger_client_mock(swagger_client_factory)

        # when:
        swagger_client.api.deleteProjectMember.side_effect = HTTPNotFound(
            response=MagicMock()
        )

        # then:
        with self.assertRaises(ProjectNotFound):
            remove_project_member(
                name="org/proj", username="tester", api_token=API_TOKEN
            )

    def test_remove_project_member_no_user(self, swagger_client_factory):
        swagger_client = self._get_swagger_client_mock(swagger_client_factory)

        # when:
        swagger_client.api.deleteProjectMember.side_effect = HTTPUnprocessableEntity(
            response=MagicMock()
        )

        # then:
        with self.assertRaises(UserNotExistsOrWithoutAccess):
            remove_project_member(
                name="org/proj", username="tester", api_token=API_TOKEN
            )

    def test_remove_project_member_permissions(self, swagger_client_factory):
        swagger_client = self._get_swagger_client_mock(swagger_client_factory)

        # when:
        swagger_client.api.deleteProjectMember.side_effect = HTTPForbidden(
            response=MagicMock()
        )

        # then:
        with self.assertRaises(AccessRevokedOnMemberRemoval):
            remove_project_member(
                name="org/proj", username="tester", api_token=API_TOKEN
            )
