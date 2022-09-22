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
from time import sleep
from typing import Dict

import pytest

from e2e_tests.base import BaseE2ETest, fake
from e2e_tests.utils import Environment, a_project_name, initialize_container
from neptune.management import (
    add_project_member,
    add_project_service_account,
    create_project,
    delete_project,
    get_project_list,
    get_project_member_list,
    get_project_service_account_list,
    get_workspace_member_list,
    get_workspace_service_account_list,
    remove_project_member,
    remove_project_service_account,
    trash_objects,
)
from neptune.management.exceptions import UserNotExistsOrWithoutAccess
from neptune.management.internal.utils import normalize_project_name
from neptune.new import init_model_version
from neptune.new.internal.container_type import ContainerType


@pytest.mark.management
class TestManagement(BaseE2ETest):
    @staticmethod
    def _assure_presence_and_role(
        *, username: str, expected_role: str, member_list: Dict[str, str]
    ):
        assert username in member_list
        assert member_list.get(username) == expected_role

    def test_standard_scenario(self, environment: Environment):
        project_name = a_project_name(project_slug=f"{fake.slug()}-mgmt")
        project_identifier = normalize_project_name(
            name=project_name, workspace=environment.workspace
        )

        assert project_identifier not in get_project_list(api_token=environment.admin_token)
        assert project_identifier not in get_project_list(api_token=environment.user_token)

        self._assure_presence_and_role(
            username=environment.user,
            expected_role="member",
            member_list=get_workspace_member_list(
                name=environment.workspace, api_token=environment.admin_token
            ),
        )
        self._assure_presence_and_role(
            username=environment.user,
            expected_role="member",
            member_list=get_workspace_member_list(
                name=environment.workspace, api_token=environment.user_token
            ),
        )
        self._assure_presence_and_role(
            username=environment.service_account,
            expected_role="member",
            member_list=get_workspace_service_account_list(
                name=environment.workspace, api_token=environment.user_token
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
            name=created_project_identifier, api_token=environment.admin_token
        )
        assert environment.service_account not in get_project_service_account_list(
            name=created_project_identifier, api_token=environment.admin_token
        )

        add_project_service_account(
            name=created_project_identifier,
            service_account_name=environment.service_account,
            role="contributor",
            api_token=environment.admin_token,
        )
        add_project_member(
            name=created_project_identifier,
            username=environment.user,
            role="contributor",
            api_token=environment.admin_token,
        )

        project_members = get_project_member_list(
            name=created_project_identifier, api_token=environment.admin_token
        )
        assert environment.user in project_members
        assert project_members.get(environment.user) == "contributor"

        project_members = get_project_member_list(
            name=created_project_identifier, api_token=environment.user_token
        )
        assert environment.user in project_members
        assert project_members.get(environment.user) == "contributor"
        assert environment.service_account not in project_members

        assert created_project_identifier in get_project_list(api_token=environment.user_token)

        remove_project_member(
            name=created_project_identifier,
            username=environment.user,
            api_token=environment.admin_token,
        )
        remove_project_service_account(
            name=created_project_identifier,
            service_account_name=environment.service_account,
            api_token=environment.admin_token,
        )

        assert created_project_identifier not in get_project_list(api_token=environment.user_token)
        assert environment.user not in get_project_member_list(
            name=created_project_identifier, api_token=environment.admin_token
        )
        assert environment.service_account not in get_project_service_account_list(
            name=created_project_identifier, api_token=environment.admin_token
        )

        delete_project(name=created_project_identifier, api_token=environment.admin_token)

        assert created_project_identifier not in get_project_list(api_token=environment.admin_token)

    def test_visibility_workspace(self, environment: "Environment"):
        project_name = a_project_name(project_slug=f"{fake.slug()}-workspace")
        project_identifier = normalize_project_name(
            name=project_name, workspace=environment.workspace
        )

        assert project_identifier not in get_project_list(api_token=environment.admin_token)
        assert project_identifier not in get_project_list(api_token=environment.user_token)

        self._assure_presence_and_role(
            username=environment.user,
            expected_role="member",
            member_list=get_workspace_member_list(
                name=environment.workspace, api_token=environment.admin_token
            ),
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
            member_list=get_project_member_list(
                name=created_project_identifier, api_token=environment.admin_token
            ),
        )
        assert environment.service_account not in get_project_service_account_list(
            name=created_project_identifier, api_token=environment.admin_token
        )

        add_project_service_account(
            name=created_project_identifier,
            service_account_name=environment.service_account,
            role="contributor",
            api_token=environment.admin_token,
        )

        self._assure_presence_and_role(
            username=environment.service_account,
            expected_role="contributor",
            member_list=get_project_service_account_list(
                name=created_project_identifier, api_token=environment.admin_token
            ),
        )

        with pytest.raises(UserNotExistsOrWithoutAccess):
            remove_project_member(
                name=created_project_identifier,
                username=environment.user,
                api_token=environment.admin_token,
            )

        remove_project_service_account(
            name=created_project_identifier,
            service_account_name=environment.service_account,
            api_token=environment.admin_token,
        )

        self._assure_presence_and_role(
            username=environment.user,
            expected_role="owner",
            member_list=get_project_member_list(
                name=created_project_identifier, api_token=environment.admin_token
            ),
        )
        assert environment.service_account not in get_project_service_account_list(
            name=created_project_identifier, api_token=environment.admin_token
        )

        delete_project(name=created_project_identifier, api_token=environment.admin_token)

        assert project_identifier not in get_project_list(api_token=environment.admin_token)

    def test_create_project(self, environment: "Environment"):
        project_name = a_project_name(project_slug=f"{fake.slug()}-create")
        project_identifier = normalize_project_name(
            name=project_name, workspace=environment.workspace
        )

        assert project_identifier not in get_project_list(api_token=environment.user_token)
        self._assure_presence_and_role(
            username=environment.user,
            expected_role="member",
            member_list=get_workspace_member_list(
                name=environment.workspace, api_token=environment.user_token
            ),
        )

        created_project_identifier = create_project(
            name=project_name,
            workspace=environment.workspace,
            api_token=environment.user_token,
        )

        assert created_project_identifier == project_identifier
        assert created_project_identifier in get_project_list(api_token=environment.user_token)

        delete_project(name=created_project_identifier, api_token=environment.admin_token)

        assert project_identifier not in get_project_list(api_token=environment.user_token)

    def _test_add_sa_to_project_as_owner(
        self, created_project_identifier: str, environment: "Environment"
    ):
        self._assure_presence_and_role(
            username=environment.user,
            expected_role="owner",
            member_list=get_project_member_list(
                name=created_project_identifier, api_token=environment.user_token
            ),
        )

        assert environment.service_account not in get_project_service_account_list(
            name=created_project_identifier, api_token=environment.user_token
        )

        add_project_service_account(
            name=created_project_identifier,
            service_account_name=environment.service_account,
            role="contributor",
            api_token=environment.user_token,
        )
        self._assure_presence_and_role(
            username=environment.service_account,
            expected_role="contributor",
            member_list=get_project_service_account_list(
                name=created_project_identifier, api_token=environment.user_token
            ),
        )

        remove_project_service_account(
            name=created_project_identifier,
            service_account_name=environment.service_account,
            api_token=environment.user_token,
        )
        assert environment.service_account not in get_project_service_account_list(
            name=created_project_identifier, api_token=environment.admin_token
        )

    def _test_add_user_to_project_as_sa(
        self, created_project_identifier: str, environment: "Environment"
    ):
        self._assure_presence_and_role(
            username=environment.service_account,
            expected_role="owner",
            member_list=get_project_service_account_list(
                name=created_project_identifier, api_token=environment.user_token
            ),
        )

        assert environment.user not in get_project_member_list(
            name=created_project_identifier, api_token=environment.user_token
        )

        add_project_member(
            name=created_project_identifier,
            username=environment.user,
            role="contributor",
            api_token=environment.admin_token,
        )
        self._assure_presence_and_role(
            username=environment.user,
            expected_role="contributor",
            member_list=get_project_member_list(
                name=created_project_identifier, api_token=environment.user_token
            ),
        )

        remove_project_member(
            name=created_project_identifier,
            username=environment.user,
            api_token=environment.admin_token,
        )
        assert environment.user not in get_project_member_list(
            name=created_project_identifier, api_token=environment.user_token
        )

    def test_invite_as_non_admin(self, environment: "Environment"):
        project_name = a_project_name(project_slug=f"{fake.slug()}-invitation")
        project_identifier = normalize_project_name(
            name=project_name, workspace=environment.workspace
        )

        created_project_identifier = create_project(
            name=project_name,
            workspace=environment.workspace,
            api_token=environment.user_token,
        )

        assert created_project_identifier == project_identifier
        assert created_project_identifier in get_project_list(api_token=environment.user_token)

        # user who created a project (`user_token` owner) will be automatically project owner
        sa_is_project_owner = (
            get_project_service_account_list(
                name=created_project_identifier, api_token=environment.user_token
            ).get(environment.service_account)
            == "owner"
        )
        user_is_project_owner = (
            get_project_member_list(
                name=created_project_identifier, api_token=environment.user_token
            ).get(environment.user)
            == "owner"
        )
        if sa_is_project_owner and not user_is_project_owner:
            # SA has access to project, so tests are run as SA
            self._test_add_user_to_project_as_sa(created_project_identifier, environment)
        elif user_is_project_owner and not sa_is_project_owner:
            # SA doesn't have access to project, so tests are run as user
            self._test_add_sa_to_project_as_owner(created_project_identifier, environment)
        else:
            raise AssertionError(
                "Expected to only SA or user to be owner of newly created project."
            )

        delete_project(name=created_project_identifier, api_token=environment.admin_token)

        assert project_identifier not in get_project_list(api_token=environment.user_token)


@pytest.mark.management
class TestTrashObjects(BaseE2ETest):
    """
    Test trash_objects
    """

    # pylint: disable=singleton-comparison

    def test_trash_runs_and_models(self, project, environment):
        # WITH runs and models
        run1_id = initialize_container(ContainerType.RUN, project=environment.project)[
            "sys/id"
        ].fetch()
        run2_id = initialize_container(ContainerType.RUN, project=environment.project)[
            "sys/id"
        ].fetch()
        model1_id = initialize_container(ContainerType.MODEL, project=environment.project)[
            "sys/id"
        ].fetch()
        model2_id = initialize_container(ContainerType.MODEL, project=environment.project)[
            "sys/id"
        ].fetch()

        # WHEN trash one run and one model
        trash_objects(environment.project, [run1_id, model1_id])
        # wait for the elasticsearch cache to fill
        sleep(5)

        # THEN trashed runs are marked as trashed
        runs = project.fetch_runs_table().to_pandas()
        assert run1_id in runs[runs["sys/trashed"] == True]["sys/id"].tolist()
        assert run2_id in runs[runs["sys/trashed"] == False]["sys/id"].tolist()

        # AND trashed models are marked as trashed
        models = project.fetch_models_table().to_pandas()
        assert model1_id in models[models["sys/trashed"] == True]["sys/id"].tolist()
        assert model2_id in models[models["sys/trashed"] == False]["sys/id"].tolist()

    def test_trash_model_version(self, environment):
        # WITH model
        model = initialize_container(ContainerType.MODEL, project=environment.project)
        model_id = model["sys/id"].fetch()

        # AND model's model versions
        model_version1 = init_model_version(model=model_id, project=environment.project)[
            "sys/id"
        ].fetch()
        model_version2 = init_model_version(model=model_id, project=environment.project)[
            "sys/id"
        ].fetch()

        # WHEN model version is trashed
        trash_objects(environment.project, model_version1)
        # wait for the elasticsearch cache to fill
        sleep(5)

        # THEN expect this version to be trashed
        model_versions = model.fetch_model_versions_table().to_pandas()
        assert (
            model_version1
            in model_versions[model_versions["sys/trashed"] == True]["sys/id"].tolist()
        )
        assert (
            model_version2
            in model_versions[model_versions["sys/trashed"] == False]["sys/id"].tolist()
        )

        # WHEN whole model is trashed
        trash_objects(environment.project, model_id)
        # wait for the elasticsearch cache to fill
        sleep(5)

        # THEN expect all its versions to be trashed
        model_versions = model.fetch_model_versions_table().to_pandas()
        assert (
            model_version1
            in model_versions[model_versions["sys/trashed"] == True]["sys/id"].tolist()
        )
        assert (
            model_version2
            in model_versions[model_versions["sys/trashed"] == True]["sys/id"].tolist()
        )
