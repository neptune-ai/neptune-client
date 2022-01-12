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
import random
import pytest
from faker import Faker

import neptune.new as neptune
from neptune.new.run import Run
from neptune.new.project import Project

from e2e_tests.base import BaseE2ETest

fake = Faker()


class TestCopying(BaseE2ETest):
    @pytest.mark.parametrize("container", ["run", "project"], indirect=True)
    @pytest.mark.parametrize(
        "value", [random.randint(0, 100), random.random(), fake.boolean(), fake.word()]
    )
    def test_copy_project_to_container(self, container: Run, value, environment):
        project = neptune.init_project(name=environment.project)

        src, destination, destination2 = self.gen_key(), self.gen_key(), self.gen_key()

        project[src] = value
        project.sync()

        container[destination] = project[src]
        container[destination2] = container[destination]
        container.sync()

        assert project[src].fetch() == value
        assert container[destination].fetch() == value
        assert container[destination2].fetch() == value

    @pytest.mark.parametrize("container", ["project", "run"], indirect=True)
    @pytest.mark.parametrize(
        "value", [random.randint(0, 100), random.random(), fake.boolean(), fake.word()]
    )
    def test_copy_run_to_container(self, container: Project, value, environment):
        run = neptune.init_run(project=environment.project)
        src, destination, destination2 = self.gen_key(), self.gen_key(), self.gen_key()

        container[src] = value
        container.sync()

        run[destination] = container[src]
        run[destination2] = run[destination]
        run.sync()

        assert container[src].fetch() == value
        assert run[destination].fetch() == value
        assert run[destination2].fetch() == value
