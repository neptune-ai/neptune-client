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
from faker import Faker

import neptune.new as neptune
from neptune.new.project import Project

from e2e_tests.base import BaseE2ETest
from e2e_tests.utils import with_check_if_file_appears

fake = Faker()


class TestInitRun(BaseE2ETest):
    # TODO: test all remaining init parameters
    def test_resuming_run(self, environment):
        exp = neptune.init(project=environment.project)

        key = self.gen_key()
        val = fake.word()
        exp[key] = val
        exp.sync()

        exp.stop()

        # pylint: disable=protected-access
        exp2 = neptune.init(run=exp._short_id, project=environment.project)
        assert exp2[key].fetch() == val

    def test_custom_run_id(self, environment):
        custom_run_id = "-".join((fake.word() for _ in range(3)))
        run = neptune.init(custom_run_id=custom_run_id, project=environment.project)

        key = self.gen_key()
        val = fake.word()
        run[key] = val
        run.sync()

        run.stop()

        exp2 = neptune.init(custom_run_id=custom_run_id, project=environment.project)
        assert exp2[key].fetch() == val

    def test_send_source_code(self, environment):
        exp = neptune.init(
            source_files="**/*.py",
            name="E2e init source code",
            project=environment.project,
        )

        # download sources
        exp.sync()
        with with_check_if_file_appears("files.zip"):
            exp["source_code/files"].download()


class TestInitProject(BaseE2ETest):
    def test_resuming_project(self, environment):
        exp = neptune.init_project(name=environment.project)

        key = self.gen_key()
        val = fake.word()
        exp[key] = val
        exp.sync()

        exp.stop()

        exp2 = neptune.init_project(name=environment.project)
        assert exp2[key].fetch() == val

    def test_init_and_readonly(self, environment):
        project: Project = neptune.init_project(name=environment.project)

        key = f"{self.gen_key()}-" + "-".join((fake.word() for _ in range(4)))
        val = fake.word()
        project[key] = val
        project.sync()
        project.stop()

        read_only_project = neptune.get_project(name=environment.project)
        read_only_project.sync()

        assert set(read_only_project.get_structure()["sys"]) == {
            "creation_time",
            "id",
            "modification_time",
            "monitoring_time",
            "name",
            "ping_time",
            "running_time",
            "size",
            "state",
            "tags",
            "visibility",
        }
        assert read_only_project[key].fetch() == val
