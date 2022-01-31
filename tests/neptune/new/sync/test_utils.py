#
# Copyright (c) 2022, Neptune Labs Sp. z o.o.
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
from unittest.mock import MagicMock

import pytest

from neptune.new.exceptions import ProjectNotFound
from neptune.new.sync.utils import get_project


@pytest.fixture(name="backend")
def backend_fixture():
    return MagicMock()


def test_get_project_no_name_set(mocker, backend):
    # given
    mocker.patch.object(os, "getenv")
    os.getenv.return_value = None

    # expect
    assert get_project(None, backend=backend) is None


def test_get_project_project_not_found(backend):
    # given
    backend.get_project.side_effect = ProjectNotFound("foo")

    # expect
    assert get_project("foo", backend=backend) is None
