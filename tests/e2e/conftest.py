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
import pytest
from faker import Faker

from neptune import init_project
from tests.e2e.utils import (
    Environment,
    RawEnvironment,
    initialize_container,
)

fake = Faker()


@pytest.fixture(scope="session")
def environment():
    raw_env = RawEnvironment()

    yield Environment(
        project=raw_env.project_name,
        user_token=raw_env.neptune_api_token,
    )


@pytest.fixture(scope="session")
def container(request, environment):
    exp = initialize_container(
        container_type=request.param, project=environment.project, api_token=environment.user_token
    )
    yield exp
    exp.stop()


@pytest.fixture(scope="function")
def container_fn_scope(request, environment):
    exp = initialize_container(
        container_type=request.param, project=environment.project, api_token=environment.user_token
    )
    yield exp
    exp.stop()


@pytest.fixture(scope="session")
def containers_pair(request, environment):
    container_a_type, container_b_type = request.param
    container_a = initialize_container(
        container_type=container_a_type, project=environment.project, api_token=environment.user_token
    )
    container_b = initialize_container(
        container_type=container_b_type, project=environment.project, api_token=environment.user_token
    )

    yield container_a, container_b

    container_b.stop()
    container_a.stop()


@pytest.fixture()
def common_tag():
    yield fake.nic_handle()


@pytest.fixture(scope="session")
def project(environment):
    yield init_project(mode="read-only", project=environment.project, api_token=environment.user_token)
