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
import time
from functools import partial
from typing import (
    Callable,
    Dict,
    List,
)

import backoff
import pytest

from neptune import (
    Project,
    init_model_version,
)
from neptune.internal.container_type import ContainerType
from neptune.management import (
    ProjectVisibility,
    add_project_member,
    add_project_service_account,
    clear_trash,
    create_project,
    delete_project,
    get_project_list,
    get_project_member_list,
    get_project_service_account_list,
    get_workspace_member_list,
    get_workspace_service_account_list,
    get_workspace_status,
    invite_to_workspace,
    remove_project_member,
    remove_project_service_account,
    trash_objects,
)
from neptune.management.exceptions import (
    ProjectNotFound,
    ProjectPrivacyRestrictedException,
    UserAlreadyInvited,
    UserNotExistsOrWithoutAccess,
    WorkspaceOrUserNotFound,
)
from neptune.management.internal.utils import normalize_project_name
from neptune.table import Table
from tests.e2e.base import (
    BaseE2ETest,
    fake,
)
from tests.e2e.utils import (
    Environment,
    a_project_name,
    initialize_container,
)


@pytest.mark.management
class TestManagement(BaseE2ETest):
    @staticmethod
    def _assure_presence_and_role(*, username: str, expected_role: str, member_list: Dict[str, str]):
        assert username in member_list
        assert member_list.get(username) == expected_role

    def test_standard_scenario(self, environment: Environment):
        project_name = a_project_name(project_slug=f"{fake.slug()}-mgmt")
        project_identifier = normalize_project_name(name=project_name, workspace=environment.workspace)

        assert project_identifier not in get_project_list(api_token=environment.admin_token)
        assert project_identifier not in get_project_list(api_token=environment.user_token)

        self._assure_presence_and_role(
            username=environment.user,
            expected_role="member",
            member_list=get_workspace_member_list(workspace=environment.workspace, api_token=environment.admin_token),
        )
        self._assure_presence_and_role(
            username=environment.user,
            expected_role="member",
            member_list=get_workspace_member_list(workspace=environment.workspace, api_token=environment.user_token),
        )
        self._assure_presence_and_role(
            username=environment.service_account,
            expected_role="member",
            member_list=get_workspace_service_account_list(
                workspace=environment.workspace, api_token=environment.user_token
            ),
        )

        created_project_identifier = create_project(
            name=project_name,
            visibility="priv",
            workspace=environment.workspace,
            api_token=environment.admin_token,
        )

        assert created_project_identifier == project_identifier
        assert created_project_identifier in get_project_list(api_token=environment.admin_token)
        assert created_project_identifier not in get_project_list(api_token=environment.user_token)

        assert environment.user not in get_project_member_list(
            project=created_project_identifier, api_token=environment.admin_token
        )
        assert environment.service_account not in get_project_service_account_list(
            project=created_project_identifier, api_token=environment.admin_token
        )

        add_project_service_account(
            project=created_project_identifier,
            service_account_name=environment.service_account,
            role="contributor",
            api_token=environment.admin_token,
        )
        add_project_member(
            project=created_project_identifier,
            username=environment.user,
            role="contributor",
            api_token=environment.admin_token,
        )

        project_members = get_project_member_list(project=created_project_identifier, api_token=environment.admin_token)
        assert environment.user in project_members
        assert project_members.get(environment.user) == "contributor"

        project_members = get_project_member_list(project=created_project_identifier, api_token=environment.user_token)
        assert environment.user in project_members
        assert project_members.get(environment.user) == "contributor"
        assert environment.service_account not in project_members

        assert created_project_identifier in get_project_list(api_token=environment.user_token)

        remove_project_member(
            project=created_project_identifier,
            username=environment.user,
            api_token=environment.admin_token,
        )
        remove_project_service_account(
            project=created_project_identifier,
            service_account_name=environment.service_account,
            api_token=environment.admin_token,
        )

        assert created_project_identifier not in get_project_list(api_token=environment.user_token)
        assert environment.user not in get_project_member_list(
            project=created_project_identifier, api_token=environment.admin_token
        )
        assert environment.service_account not in get_project_service_account_list(
            project=created_project_identifier, api_token=environment.admin_token
        )

        delete_project(project=created_project_identifier, api_token=environment.admin_token)

        assert created_project_identifier not in get_project_list(api_token=environment.admin_token)

    def test_visibility_workspace(self, environment: "Environment"):
        project_name = a_project_name(project_slug=f"{fake.slug()}-workspace")
        project_identifier = normalize_project_name(name=project_name, workspace=environment.workspace)

        assert project_identifier not in get_project_list(api_token=environment.admin_token)
        assert project_identifier not in get_project_list(api_token=environment.user_token)

        self._assure_presence_and_role(
            username=environment.user,
            expected_role="member",
            member_list=get_workspace_member_list(workspace=environment.workspace, api_token=environment.admin_token),
        )

        created_project_identifier = create_project(
            name=project_name,
            visibility="workspace",
            workspace=environment.workspace,
            api_token=environment.admin_token,
        )

        assert created_project_identifier == project_identifier
        assert created_project_identifier in get_project_list(api_token=environment.admin_token)

        self._assure_presence_and_role(
            username=environment.user,
            expected_role="owner",
            member_list=get_project_member_list(project=created_project_identifier, api_token=environment.admin_token),
        )
        assert environment.service_account not in get_project_service_account_list(
            project=created_project_identifier, api_token=environment.admin_token
        )

        add_project_service_account(
            project=created_project_identifier,
            service_account_name=environment.service_account,
            role="contributor",
            api_token=environment.admin_token,
        )

        self._assure_presence_and_role(
            username=environment.service_account,
            expected_role="contributor",
            member_list=get_project_service_account_list(
                project=created_project_identifier, api_token=environment.admin_token
            ),
        )

        with pytest.raises(UserNotExistsOrWithoutAccess):
            remove_project_member(
                project=created_project_identifier,
                username=environment.user,
                api_token=environment.admin_token,
            )

        remove_project_service_account(
            project=created_project_identifier,
            service_account_name=environment.service_account,
            api_token=environment.admin_token,
        )

        self._assure_presence_and_role(
            username=environment.user,
            expected_role="owner",
            member_list=get_project_member_list(project=created_project_identifier, api_token=environment.admin_token),
        )
        assert environment.service_account not in get_project_service_account_list(
            project=created_project_identifier, api_token=environment.admin_token
        )

        delete_project(project=created_project_identifier, api_token=environment.admin_token)

        assert project_identifier not in get_project_list(api_token=environment.admin_token)

    def test_create_project(self, environment: "Environment"):
        project_name = a_project_name(project_slug=f"{fake.slug()}-create")
        project_identifier = normalize_project_name(name=project_name, workspace=environment.workspace)

        assert project_identifier not in get_project_list(api_token=environment.user_token)
        self._assure_presence_and_role(
            username=environment.user,
            expected_role="member",
            member_list=get_workspace_member_list(workspace=environment.workspace, api_token=environment.user_token),
        )

        created_project_identifier = create_project(
            name=project_name,
            workspace=environment.workspace,
            api_token=environment.user_token,
        )

        assert created_project_identifier == project_identifier
        assert created_project_identifier in get_project_list(api_token=environment.user_token)

        delete_project(project=created_project_identifier, api_token=environment.admin_token)

        assert project_identifier not in get_project_list(api_token=environment.user_token)

    def test_invalid_visibility(self, environment: "Environment"):
        project_name = a_project_name(project_slug=f"{fake.slug()}-create")
        project_identifier = normalize_project_name(name=project_name, workspace=environment.workspace)

        assert project_identifier not in get_project_list(api_token=environment.user_token)
        self._assure_presence_and_role(
            username=environment.user,
            expected_role="member",
            member_list=get_workspace_member_list(workspace=environment.workspace, api_token=environment.user_token),
        )

        with pytest.raises(ProjectPrivacyRestrictedException):
            create_project(
                name=project_name,
                workspace=environment.workspace,
                api_token=environment.user_token,
                # TODO(bartosz.prusak): this is an invalid setting because workspaces have "public" setting banned as
                #  default. The test should check if the workspace used has this ban set (and if not - skip this test).
                visibility=ProjectVisibility.PUBLIC,
            )

        assert project_identifier not in get_project_list(api_token=environment.user_token)

    def _test_add_sa_to_project_as_owner(self, created_project_identifier: str, environment: "Environment"):
        self._assure_presence_and_role(
            username=environment.user,
            expected_role="owner",
            member_list=get_project_member_list(project=created_project_identifier, api_token=environment.user_token),
        )

        assert environment.service_account not in get_project_service_account_list(
            project=created_project_identifier, api_token=environment.user_token
        )

        add_project_service_account(
            project=created_project_identifier,
            service_account_name=environment.service_account,
            role="contributor",
            api_token=environment.user_token,
        )
        self._assure_presence_and_role(
            username=environment.service_account,
            expected_role="contributor",
            member_list=get_project_service_account_list(
                project=created_project_identifier, api_token=environment.user_token
            ),
        )

        remove_project_service_account(
            project=created_project_identifier,
            service_account_name=environment.service_account,
            api_token=environment.user_token,
        )
        assert environment.service_account not in get_project_service_account_list(
            project=created_project_identifier, api_token=environment.admin_token
        )

    def _test_add_user_to_project_as_sa(self, created_project_identifier: str, environment: "Environment"):
        self._assure_presence_and_role(
            username=environment.service_account,
            expected_role="owner",
            member_list=get_project_service_account_list(
                project=created_project_identifier, api_token=environment.user_token
            ),
        )

        assert environment.user not in get_project_member_list(
            project=created_project_identifier, api_token=environment.user_token
        )

        add_project_member(
            project=created_project_identifier,
            username=environment.user,
            role="contributor",
            api_token=environment.admin_token,
        )
        self._assure_presence_and_role(
            username=environment.user,
            expected_role="contributor",
            member_list=get_project_member_list(project=created_project_identifier, api_token=environment.user_token),
        )

        remove_project_member(
            project=created_project_identifier,
            username=environment.user,
            api_token=environment.admin_token,
        )
        assert environment.user not in get_project_member_list(
            project=created_project_identifier, api_token=environment.user_token
        )

    def test_invite_as_non_admin(self, environment: "Environment"):
        project_name = a_project_name(project_slug=f"{fake.slug()}-invitation")
        project_identifier = normalize_project_name(name=project_name, workspace=environment.workspace)

        created_project_identifier = create_project(
            name=project_name,
            workspace=environment.workspace,
            api_token=environment.user_token,
        )

        assert created_project_identifier == project_identifier
        assert created_project_identifier in get_project_list(api_token=environment.user_token)

        # user who created a project (`user_token` owner) will be automatically project owner
        sa_is_project_owner = (
            get_project_service_account_list(project=created_project_identifier, api_token=environment.user_token).get(
                environment.service_account
            )
            == "owner"
        )
        user_is_project_owner = (
            get_project_member_list(project=created_project_identifier, api_token=environment.user_token).get(
                environment.user
            )
            == "owner"
        )
        if sa_is_project_owner and not user_is_project_owner:
            # SA has access to project, so tests are run as SA
            self._test_add_user_to_project_as_sa(created_project_identifier, environment)
        elif user_is_project_owner and not sa_is_project_owner:
            # SA doesn't have access to project, so tests are run as user
            self._test_add_sa_to_project_as_owner(created_project_identifier, environment)
        else:
            raise AssertionError("Expected to only SA or user to be owner of newly created project.")

        delete_project(project=created_project_identifier, api_token=environment.admin_token)

        assert project_identifier not in get_project_list(api_token=environment.user_token)

    def test_invite_to_workspace(self, environment: "Environment"):
        with pytest.raises(UserAlreadyInvited):
            invite_to_workspace(
                username=environment.user, workspace=environment.workspace, api_token=environment.admin_token
            )

        with pytest.raises(UserAlreadyInvited):
            invite_to_workspace(
                username=environment.user,
                workspace=environment.workspace,
                api_token=environment.admin_token,
                role="admin",
            )

        with pytest.raises(WorkspaceOrUserNotFound):
            invite_to_workspace(
                username="non-existent-user", workspace=environment.workspace, api_token=environment.admin_token
            )

        with pytest.raises(WorkspaceOrUserNotFound):
            invite_to_workspace(
                username=environment.user, workspace="non-existent-workspace", api_token=environment.admin_token
            )

    def test_workspace_status(self, environment: "Environment"):
        status = get_workspace_status(workspace=environment.workspace, api_token=environment.admin_token)

        assert "storageBytesAvailable" in status
        assert "storageBytesLimit" in status
        assert "storageBytesUsed" in status
        assert status["storageBytesAvailable"] >= 0
        assert status["storageBytesLimit"] >= 0
        assert status["storageBytesUsed"] >= 0


@pytest.mark.management
class TestTrashObjects(BaseE2ETest):
    """
    Test trash_objects
    """

    def test_trash_objects_wrong_project(self):
        with pytest.raises(ProjectNotFound):
            trash_objects("org/non-existent-project", ["RUN-1", "RUN-2", "RUN-3"])

    def test_trash_runs_and_models(self, project, environment):
        # WITH runs and models
        run1_id = initialize_container(ContainerType.RUN, project=environment.project)["sys/id"].fetch()
        run2_id = initialize_container(ContainerType.RUN, project=environment.project)["sys/id"].fetch()
        model1_id = initialize_container(ContainerType.MODEL, project=environment.project)["sys/id"].fetch()
        model2_id = initialize_container(ContainerType.MODEL, project=environment.project)["sys/id"].fetch()
        # wait for elastic index to refresh
        self.wait_for_containers([run1_id, run2_id], project.fetch_runs_table)
        self.wait_for_containers([model1_id, model2_id], project.fetch_models_table)

        # WHEN trash one run and one model
        trash_objects(environment.project, [run1_id, model1_id])

        # THEN trashed runs are not fetched
        self.wait_for_containers([run2_id], project.fetch_runs_table)
        # AND trashed models are not fetched
        self.wait_for_containers([model2_id], project.fetch_models_table)

    def test_trash_model_version(self, environment):
        # WITH model
        model = initialize_container(ContainerType.MODEL, project=environment.project)
        model_id = model["sys/id"].fetch()
        # AND model's model versions
        model_version1_id = init_model_version(model=model_id, project=environment.project)["sys/id"].fetch()
        model_version2_id = init_model_version(model=model_id, project=environment.project)["sys/id"].fetch()
        self.wait_for_containers([model_version1_id, model_version2_id], model.fetch_model_versions_table)

        # WHEN model version is trashed
        trash_objects(environment.project, model_version1_id)

        # THEN expect this version to not be fetched anymore
        self.wait_for_containers([model_version2_id], model.fetch_model_versions_table)

        # WHEN whole model is trashed
        trash_objects(environment.project, model_id)

        # THEN expect none of its versions to be fetched anymore
        self.wait_for_containers([], model.fetch_model_versions_table)

    @backoff.on_exception(partial(backoff.expo, base=4), Exception, max_time=5)
    def wait_for_containers(self, ids: List[str], container_provider: Callable[[], Table]):
        fetched_entries = container_provider().to_pandas()
        actual_ids = fetched_entries["sys/id"].tolist() if len(fetched_entries) > 0 else []
        assert sorted(actual_ids) == sorted(ids)


@pytest.mark.management
class TestDeleteFromTrash:
    def test_delete_from_trash(self, environment):
        # given
        run1 = initialize_container(ContainerType.RUN, project=environment.project)
        run2 = initialize_container(ContainerType.RUN, project=environment.project)
        model = initialize_container(ContainerType.MODEL, project=environment.project)
        run_id_1 = run1["sys/id"].fetch()
        run_id_2 = run2["sys/id"].fetch()
        model_id = model["sys/id"].fetch()
        time.sleep(5)

        with initialize_container(ContainerType.PROJECT, project=environment.project) as project:
            trash_objects(environment.project, [run_id_1, run_id_2, model_id])
            time.sleep(10)

            # when
            clear_trash(environment.project)

            # then
            self.wait_for_containers_in_trash(0, 0, project)

    @backoff.on_exception(backoff.expo, Exception, max_time=30)
    def wait_for_containers_in_trash(self, expected_run_count: int, expected_model_count: int, project: Project):
        trashed_runs = project.fetch_runs_table(trashed=True).to_rows()
        trashed_models = project.fetch_models_table(trashed=True).to_rows()
        assert len(trashed_models) == expected_model_count
        assert len(trashed_runs) == expected_run_count
