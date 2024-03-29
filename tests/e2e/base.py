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
__all__ = ["BaseE2ETest", "AVAILABLE_CONTAINERS", "fake"]

import inspect

import pytest
from faker import Faker

fake = Faker()

AVAILABLE_CONTAINERS = ["project", "run"]
AVAILABLE_CONTAINERS = [
    pytest.param("run"),
    pytest.param("project", marks=pytest.mark.skip(reason="Project not supported")),
    pytest.param(
        "model",
        marks=pytest.mark.skip(reason="Model not implemented"),
    ),
    pytest.param(
        "model_version",
        marks=pytest.mark.skip(reason="Model not implemented"),
    ),
]


class BaseE2ETest:
    def gen_key(self):
        # Get test name
        caller_name = inspect.stack()[1][3]
        return f"{self.__class__.__name__}/{caller_name}/{fake.unique.slug()}"
