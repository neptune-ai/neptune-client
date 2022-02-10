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
# pylint: disable=redefined-outer-name
import os
import time

from faker import Faker
import boto3
import pytest

from neptune.management.internal.utils import normalize_project_name
from neptune.management import create_project, add_project_member
import neptune.new as neptune

from e2e_tests.utils import a_project_name, a_key, Environment

fake = Faker()


@pytest.fixture(scope="session")
def environment():
    workspace = os.getenv("WORKSPACE_NAME")
    admin_token = os.getenv("ADMIN_NEPTUNE_API_TOKEN")
    user = os.getenv("USER_USERNAME")

    project_name, project_key = a_project_name(project_slug=fake.slug())
    project_identifier = normalize_project_name(name=project_name, workspace=workspace)
    created_project_identifier = create_project(
        name=project_name,
        key=project_key,
        visibility="priv",
        workspace=workspace,
        api_token=admin_token,
    )

    time.sleep(10)

    add_project_member(
        name=created_project_identifier,
        username=user,
        # pylint: disable=no-member
        role="contributor",
        api_token=admin_token,
    )

    yield Environment(
        workspace=workspace,
        project=project_identifier,
        user_token=os.getenv("NEPTUNE_API_TOKEN"),
        admin_token=admin_token,
        admin=os.getenv("ADMIN_USERNAME"),
        user=user,
    )


def initialize_container(container_type, project):
    if container_type == "project":
        return neptune.init_project(name=project)

    if container_type == "run":
        return neptune.init_run(project=project)

    if container_type == "model":
        return neptune.init_model(key=a_key(fake.slug()), project=project)

    if container_type == "model_version":
        model = neptune.init_model(key=a_key(fake.slug()), project=project)
        model_sys_id = model["sys/id"].fetch()
        model.stop()

        return neptune.init_model_version(model=model_sys_id, project=project)


@pytest.fixture(scope="session")
def container(request, environment):
    exp = initialize_container(
        container_type=request.param, project=environment.project
    )
    yield exp
    exp.stop()


@pytest.fixture(scope="session")
def containers_pair(request, environment):
    container_a_type, container_b_type = request.param.split("-")
    container_a = initialize_container(
        container_type=container_a_type, project=environment.project
    )
    container_b = initialize_container(
        container_type=container_b_type, project=environment.project
    )

    yield container_a, container_b

    container_b.stop()
    container_a.stop()


@pytest.fixture(scope="session")
def bucket(environment):
    bucket_name = os.environ.get("BUCKET_NAME")

    s3_client = boto3.resource("s3")
    s3_bucket = s3_client.Bucket(bucket_name)

    yield bucket_name, s3_client

    s3_bucket.objects.filter(Prefix=environment.project).delete()
