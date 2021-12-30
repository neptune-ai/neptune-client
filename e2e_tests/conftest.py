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
from collections import namedtuple

import boto3
import pytest

import neptune.new as neptune


@pytest.fixture(scope='session')
def container(request):
    if request.param == 'project':
        project = neptune.init_project()
        yield project
        project.stop()

    if request.param == 'run':
        exp = neptune.init_run(
            name='E2e main run'
        )
        yield exp
        exp.stop()


@pytest.fixture()
def bucket():
    bucket_name = os.environ.get('BUCKET_NAME')

    s3_client = boto3.resource('s3')
    s3_bucket = s3_client.Bucket(bucket_name)

    yield bucket_name, s3_client

    s3_bucket.objects.all().delete()


Environment = namedtuple(
    'Environment',
    ['workspace', 'project', 'user_token', 'admin_token', 'admin', 'user']
)


@pytest.fixture()
def environment():
    yield Environment(
        workspace=os.getenv('WORKSPACE_NAME'),
        project=os.getenv('NEPTUNE_PROJECT'),
        user_token=os.getenv('NEPTUNE_API_TOKEN'),
        admin_token=os.getenv('ADMIN_NEPTUNE_API_TOKEN'),
        admin=os.getenv('ADMIN_USERNAME'),
        user=os.getenv('USER_USERNAME'),
    )
