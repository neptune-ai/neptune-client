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

import neptune
from neptune.exceptions import NeptuneModelKeyAlreadyExistsError
from neptune.metadata_containers import (
    Model,
    Project,
)
from neptune.types import GitRef
from tests.e2e.base import (
    AVAILABLE_CONTAINERS,
    BaseE2ETest,
    fake,
)
from tests.e2e.utils import (
    initialize_container,
    reinitialize_container,
    with_check_if_file_appears,
)


class TestInitRun(BaseE2ETest):
    def test_custom_run_id(self, environment):
        custom_run_id = "-".join((fake.word() for _ in range(3)))
        with neptune.init_run(custom_run_id=custom_run_id, project=environment.project) as run:
            key = self.gen_key()
            val = fake.word()
            run[key] = val
            run.sync()

        with neptune.init_run(custom_run_id=custom_run_id, project=environment.project) as exp2:
            assert exp2[key].fetch() == val

    def test_send_source_code(self, environment):
        with neptune.init_run(
            source_files="**/*.py",
            name="E2e init source code",
            project=environment.project,
        ) as exp:
            # download sources
            exp.sync()
            with with_check_if_file_appears("files.zip"):
                exp["source_code/files"].download()

    def test_git_client_repository(self, environment):
        with neptune.init_run(
            git_ref=GitRef(repository_path="."),
            project=environment.project,
        ) as exp:
            # download sources
            exp.sync()
            assert exp.exists("source_code/git")

        with neptune.init_run(
            git_ref=True,
            project=environment.project,
        ) as exp:
            # download sources
            exp.sync()
            assert exp.exists("source_code/git")

    def test_git_disabled(self, environment, recwarn):
        with neptune.init_run(
            git_ref=GitRef.DISABLED,
            project=environment.project,
        ) as exp:
            # download sources
            exp.sync()
            assert not exp.exists("source_code/git")

        with neptune.init_run(
            git_ref=False,
            project=environment.project,
        ) as exp:
            # download sources
            exp.sync()
            assert not exp.exists("source_code/git")

        assert len(recwarn) == 0  # upload was not skipped due to an exception that would raise a warning

    def test_infer_dependencies(self, environment):
        with neptune.init_run(
            project=environment.project,
            dependencies="infer",
        ) as exp:
            exp.sync()

            assert exp.exists("source_code/requirements")

    def test_upload_dependency_file(self, environment):
        filename = fake.file_name(extension="txt")
        with open(filename, "w") as file:
            file.write("some-dependency==1.0.0")

        with neptune.init_run(
            project=environment.project,
            dependencies=filename,
        ) as exp:
            exp.sync()

            exp["source_code/requirements"].download("requirements.txt")

        with open("requirements.txt", "r") as file:
            assert file.read() == "some-dependency==1.0.0"

    def test_warning_raised_if_dependency_file_non_existent(self, capsys, environment):
        with neptune.init_run(dependencies="some_non_existent_file", project=environment.project):
            ...

        captured = capsys.readouterr()
        assert "'some_non_existent_file' does not exist" in captured.out
        assert "ERROR" in captured.out

    def test_tracking_uncommitted_changes(self, repo, environment):
        file = repo.working_dir + "/some_file.txt"
        with open(file, "w") as fp:
            fp.write("some-content\n")

        repo.git.add(file)

        with neptune.init_run(project=environment.project, git_ref=GitRef(repository_path=repo.working_dir)) as run:
            run.sync()
            assert run.exists("source_code/diff")
            run["source_code/diff"].download()
            with open("diff.patch") as fp:
                assert "some-content" in fp.read()


class TestInitProject(BaseE2ETest):
    def test_resuming_project(self, environment):
        exp = neptune.init_project(project=environment.project)

        key = self.gen_key()
        val = fake.word()
        exp[key] = val
        exp.sync()

        exp.stop()

        exp2 = neptune.init_project(project=environment.project)
        assert exp2[key].fetch() == val

    def test_init_and_readonly(self, environment):
        project: Project = neptune.init_project(project=environment.project)

        key = f"{self.gen_key()}-" + "-".join((fake.word() for _ in range(4)))
        val = fake.word()
        project[key] = val
        project.sync()
        project.stop()

        read_only_project = neptune.init_project(mode="read-only", project=environment.project)
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


class TestInitModel(BaseE2ETest):
    @pytest.mark.parametrize("container", ["model"], indirect=True)
    def test_fail_reused_model_key(self, container: Model, environment):
        with pytest.raises(NeptuneModelKeyAlreadyExistsError):
            model_key = container["sys/id"].fetch().split("-")[1]
            neptune.init_model(key=model_key, project=environment.project)


class TestReinitialization(BaseE2ETest):
    @pytest.mark.parametrize("container_type", AVAILABLE_CONTAINERS)
    def test_resuming_container(self, container_type, environment):
        container = initialize_container(container_type=container_type, project=environment.project)
        sys_id = container["sys/id"].fetch()

        key = self.gen_key()
        val = fake.word()
        container[key] = val
        container.sync()
        container.stop()

        reinitialized = reinitialize_container(
            sys_id=sys_id,
            container_type=container.container_type.value,
            project=environment.project,
        )
        assert reinitialized[key].fetch() == val

        reinitialized.stop()
