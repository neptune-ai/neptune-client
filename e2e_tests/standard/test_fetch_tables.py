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
import random
import time
import uuid

import pytest

import neptune.new as neptune
from e2e_tests.base import BaseE2ETest, fake
from e2e_tests.utils import a_key
from neptune.new.metadata_containers import Model


class TestFetchTable(BaseE2ETest):
    def test_fetch_runs_by_tag(self, environment, project):
        tag1, tag2 = str(uuid.uuid4()), str(uuid.uuid4())

        with neptune.init_run(project=environment.project) as run:
            run_id1 = run["sys/id"].fetch()
            run["sys/tags"].add(tag1)
            run["sys/tags"].add(tag2)

        with neptune.init_run(project=environment.project) as run:
            run["sys/tags"].add(tag2)

        # wait for the cache to fill
        time.sleep(5)

        runs_table = project.fetch_runs_table(tag=[tag1, tag2]).to_rows()
        assert len(runs_table) == 1
        assert runs_table[0].get_attribute_value("sys/id") == run_id1

    @pytest.mark.parametrize("container", ["model"], indirect=True)
    def test_fetch_model_versions_with_correct_ids(self, container: Model, environment):
        model_sys_id = container["sys/id"].fetch()
        versions_to_initialize = 5

        for _ in range(versions_to_initialize):
            with neptune.init_model_version(model=model_sys_id, project=environment.project):
                pass

        # wait for the elasticsearch cache to fill
        time.sleep(5)

        versions_table = sorted(
            container.fetch_model_versions_table().to_rows(),
            key=lambda r: r.get_attribute_value("sys/id"),
        )
        assert len(versions_table) == versions_to_initialize
        for index in range(versions_to_initialize):
            assert (
                versions_table[index].get_attribute_value("sys/id") == f"{model_sys_id}-{index + 1}"
            )

    def _test_fetch_from_container(self, init_container, get_containers_as_rows):
        container_id1, container_id2 = None, None
        key1 = self.gen_key()
        key2 = f"{self.gen_key()}/{self.gen_key()}"
        value1 = random.randint(1, 100)
        value2 = fake.name()

        with init_container() as container:
            container_id1 = container["sys/id"].fetch()
            container[key1] = value1
            container[key2] = value2
            container.sync()

        with init_container() as container:
            container_id2 = container["sys/id"].fetch()
            container[key1] = value1
            container.sync()

        # wait for the cache to fill
        time.sleep(5)

        containers_as_rows = get_containers_as_rows()
        container1 = next(
            filter(lambda m: m.get_attribute_value("sys/id") == container_id1, containers_as_rows)
        )
        container2 = next(
            filter(lambda m: m.get_attribute_value("sys/id") == container_id2, containers_as_rows)
        )

        assert container1.get_attribute_value(key1) == value1
        assert container1.get_attribute_value(key2) == value2
        assert container2.get_attribute_value(key1) == value1
        with pytest.raises(ValueError):
            container2.get_attribute_value(key2)

        def get_container1(**kwargs):
            containers_as_rows = get_containers_as_rows(**kwargs)
            return next(
                filter(
                    lambda m: m.get_attribute_value("sys/id") == container_id1, containers_as_rows
                )
            )

        non_filtered = get_container1()
        assert non_filtered.get_attribute_value(key1) == value1
        assert non_filtered.get_attribute_value(key2) == value2

        columns_none = get_container1(columns=None)
        assert columns_none.get_attribute_value(key1) == value1
        assert columns_none.get_attribute_value(key2) == value2

        columns_empty = get_container1(columns=[])
        with pytest.raises(ValueError):
            columns_empty.get_attribute_value(key1)
        with pytest.raises(ValueError):
            columns_empty.get_attribute_value(key2)

        columns_with_one_key = get_container1(columns=[key1])
        assert columns_with_one_key.get_attribute_value(key1) == value1
        with pytest.raises(ValueError):
            columns_with_one_key.get_attribute_value(key2)

        columns_with_one_key = get_container1(columns=[key2])
        with pytest.raises(ValueError):
            columns_with_one_key.get_attribute_value(key1)
        assert columns_with_one_key.get_attribute_value(key2) == value2

    def test_fetch_runs_table(self, environment, project):
        def init_run():
            return neptune.init_run(project=environment.project)

        def get_runs_as_rows(**kwargs):
            return project.fetch_runs_table(**kwargs).to_rows()

        self._test_fetch_from_container(init_run, get_runs_as_rows)

    def test_fetch_models_table(self, environment, project):
        def init_run():
            return neptune.init_model(project=environment.project, key=a_key())

        def get_models_as_rows(**kwargs):
            return project.fetch_models_table(**kwargs).to_rows()

        self._test_fetch_from_container(init_run, get_models_as_rows)

    @pytest.mark.parametrize("container", ["model"], indirect=True)
    def test_fetch_model_versions_table(self, container: Model, environment):
        model_sys_id = container["sys/id"].fetch()

        def init_run():
            return neptune.init_model_version(model=model_sys_id, project=environment.project)

        def get_model_versions_as_rows(**kwargs):
            return container.fetch_model_versions_table(**kwargs).to_rows()

        self._test_fetch_from_container(init_run, get_model_versions_as_rows)
