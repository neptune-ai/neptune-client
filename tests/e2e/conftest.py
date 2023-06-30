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
import os
import time
import uuid
from datetime import datetime

import pytest
from faker import Faker
from git import Repo

from neptune import init_project
from neptune.internal.utils.s3 import get_boto_s3_client
from neptune.management import (
    add_project_member,
    add_project_service_account,
    create_project,
)
from neptune.management.internal.utils import normalize_project_name
from tests.e2e.utils import (
    Environment,
    RawEnvironment,
    a_project_name,
    initialize_container,
)

fake = Faker()


@pytest.fixture(scope="session")
def environment():
    raw_env = RawEnvironment()
    workspace = raw_env.workspace_name
    admin_token = raw_env.admin_neptune_api_token
    user = raw_env.user_username
    service_account_name = raw_env.service_account_name

    project_name = a_project_name(project_slug=fake.slug())
    project_identifier = normalize_project_name(name=project_name, workspace=workspace)

    created_project_identifier = create_project(
        name=project_name,
        visibility="priv",
        workspace=workspace,
        api_token=admin_token,
    )

    time.sleep(10)

    add_project_member(
        project=created_project_identifier,
        username=user,
        role="contributor",
        api_token=admin_token,
    )

    add_project_service_account(
        project=created_project_identifier,
        service_account_name=service_account_name,
        role="contributor",
        api_token=admin_token,
    )

    yield Environment(
        workspace=workspace,
        project=project_identifier,
        user_token=raw_env.neptune_api_token,
        admin_token=admin_token,
        admin=raw_env.admin_username,
        user=user,
        service_account=raw_env.service_account_name,
    )

    project = init_project(project=created_project_identifier, api_token=admin_token)
    project["finished"] = datetime.now()
    project.stop()


@pytest.fixture(scope="session")
def container(request, environment):
    exp = initialize_container(container_type=request.param, project=environment.project)
    yield exp
    exp.stop()


@pytest.fixture(scope="session")
def containers_pair(request, environment):
    container_a_type, container_b_type = request.param.split("-")
    container_a = initialize_container(container_type=container_a_type, project=environment.project)
    container_b = initialize_container(container_type=container_b_type, project=environment.project)

    yield container_a, container_b

    container_b.stop()
    container_a.stop()


@pytest.fixture(scope="session")
def bucket():
    bucket_name = os.environ.get("BUCKET_NAME")

    s3_client = get_boto_s3_client()

    yield bucket_name, s3_client


@pytest.fixture()
def common_tag():
    yield fake.nic_handle()


@pytest.fixture(scope="session")
def project(environment):
    yield init_project(mode="read-only", project=environment.project, api_token=environment.user_token)


@pytest.fixture(scope="session")
def repo(tmp_path_factory) -> Repo:
    path = tmp_path_factory.mktemp("git_repo")
    repo = Repo.init(path)
    file = path / f"{uuid.uuid4()}.txt"
    with open(file, "w") as fp:
        fp.write(str(uuid.uuid4()))

    repo.git.add(all=True)
    repo.index.commit("Init commit")
    return repo
