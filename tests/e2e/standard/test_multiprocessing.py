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
import random

import pytest

from neptune.metadata_containers import MetadataContainer
from tests.e2e.base import (
    AVAILABLE_CONTAINERS,
    BaseE2ETest,
    fake,
)


class TestStageTransitions(BaseE2ETest):
    @pytest.mark.parametrize("container", AVAILABLE_CONTAINERS, indirect=True)
    @pytest.mark.parametrize("value", [random.randint(0, 100), random.random(), fake.boolean(), fake.word()])
    def test_simple_assign_and_fetch(self, container: MetadataContainer, value):
        child_pid = os.fork()

        key = self.gen_key()

        container[key] = value
        container.sync()
        assert container[key].fetch() == value

        if child_pid != 0:
            os.waitpid(child_pid, 0)
