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

from faker import Faker
import boto3
import pytest

from neptune.management.internal.utils import normalize_project_name
from neptune.management import create_project, add_project_member
import neptune.new as neptune

from e2e_tests.utils import a_project_name, Environment

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


@pytest.fixture(scope="session")
def container(request, environment):
    if request.param == "project":
        project = neptune.init_project(name=environment.project)
        yield project
        project.stop()

    if request.param == "run":
        exp = neptune.init_run(project=environment.project)
        yield exp
        exp.stop()


@pytest.fixture(scope="session")
def bucket(environment):
    bucket_name = os.environ.get("BUCKET_NAME")

    s3_client = boto3.resource("s3")
    s3_bucket = s3_client.Bucket(bucket_name)

    yield bucket_name, s3_client

    s3_bucket.objects.filter(Prefix=environment.project).delete()
