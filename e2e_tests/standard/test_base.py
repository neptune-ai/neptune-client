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
import time
import uuid
from datetime import datetime, timezone

import pytest

import neptune.new as neptune
from e2e_tests.base import AVAILABLE_CONTAINERS, BaseE2ETest, fake
from e2e_tests.utils import a_key
from neptune.new.metadata_containers import MetadataContainer, Model


class TestAtoms(BaseE2ETest):
    @pytest.mark.parametrize("container", AVAILABLE_CONTAINERS, indirect=True)
    @pytest.mark.parametrize(
        "value", [random.randint(0, 100), random.random(), fake.boolean(), fake.word()]
    )
    def test_simple_assign_and_fetch(self, container: MetadataContainer, value):
        key = self.gen_key()

        container[key] = value
        container.sync()
        assert container[key].fetch() == value

    @pytest.mark.parametrize("container", AVAILABLE_CONTAINERS, indirect=True)
    def test_simple_assign_datetime(self, container: MetadataContainer):
        key = self.gen_key()
        now = datetime.now()

        container[key] = now
        container.sync()

        # expect truncate to milliseconds and add UTC timezone
        expected_now = now.astimezone(timezone.utc).replace(
            microsecond=int(now.microsecond / 1000) * 1000
        )
        assert container[key].fetch() == expected_now

    @pytest.mark.parametrize("container", AVAILABLE_CONTAINERS, indirect=True)
    def test_fetch_non_existing_key(self, container: MetadataContainer):
        key = self.gen_key()
        with pytest.raises(AttributeError):
            container[key].fetch()

    @pytest.mark.parametrize("container", AVAILABLE_CONTAINERS, indirect=True)
    def test_delete_atom(self, container: MetadataContainer):
        key = self.gen_key()
        value = fake.name()

        container[key] = value
        container.sync()

        assert container[key].fetch() == value

        del container[key]
        with pytest.raises(AttributeError):
            container[key].fetch()


class TestNamespace(BaseE2ETest):
    @pytest.mark.parametrize("container", AVAILABLE_CONTAINERS, indirect=True)
    def test_reassigning(self, container: MetadataContainer):
        namespace = self.gen_key()
        key = f"{fake.unique.word()}/{fake.unique.word()}"
        value = fake.name()

        # Assign a namespace
        container[namespace] = {f"{key}": value}
        container.sync()

        assert container[f"{namespace}/{key}"].fetch() == value

        # Direct reassign internal value
        value = fake.name()
        container[f"{namespace}/{key}"] = value
        container.sync()

        assert container[f"{namespace}/{key}"].fetch() == value

        # Reassigning by namespace
        value = fake.name()
        container[namespace] = {f"{key}": value}
        container.sync()

        assert container[f"{namespace}/{key}"].fetch() == value

    @pytest.mark.parametrize("container", AVAILABLE_CONTAINERS, indirect=True)
    def test_distinct_types(self, container: MetadataContainer):
        namespace = self.gen_key()
        key = f"{fake.unique.word()}/{fake.unique.word()}"
        value = random.randint(0, 100)

        container[namespace] = {f"{key}": value}
        container.sync()

        assert container[f"{namespace}/{key}"].fetch() == value

        new_value = fake.name()

        with pytest.raises(ValueError):
            container[namespace] = {f"{key}": new_value}
            container.sync()

    @pytest.mark.parametrize("container", AVAILABLE_CONTAINERS, indirect=True)
    def test_delete_namespace(self, container: MetadataContainer):
        namespace = fake.unique.word()
        key1 = fake.unique.word()
        key2 = fake.unique.word()
        value1 = fake.name()
        value2 = fake.name()

        container[namespace][key1] = value1
        container[namespace][key2] = value2
        container.sync()

        assert container[namespace][key1].fetch() == value1
        assert container[namespace][key2].fetch() == value2

        del container[namespace]
        with pytest.raises(AttributeError):
            container[namespace][key1].fetch()
        with pytest.raises(AttributeError):
            container[namespace][key2].fetch()


class TestStringSet(BaseE2ETest):
    neptune_tags_path = "sys/tags"

    @pytest.mark.parametrize("container", AVAILABLE_CONTAINERS, indirect=True)
    def test_do_not_accept_non_tag_path(self, container: MetadataContainer):
        random_path = "some/path"
        container[random_path].add(fake.unique.word())
        container.sync()

        with pytest.raises(AttributeError):
            # backends accepts `'sys/tags'` only
            container[random_path].fetch()

    @pytest.mark.parametrize("container", AVAILABLE_CONTAINERS, indirect=True)
    def test_add_and_remove_tags(self, container: MetadataContainer):
        remaining_tag1 = fake.unique.word()
        remaining_tag2 = fake.unique.word()
        to_remove_tag1 = fake.unique.word()
        to_remove_tag2 = fake.unique.word()

        container.sync()
        if container.exists(self.neptune_tags_path):
            container[self.neptune_tags_path].clear()
        container[self.neptune_tags_path].add(remaining_tag1)
        container[self.neptune_tags_path].add([to_remove_tag1, remaining_tag2])
        container[self.neptune_tags_path].remove(to_remove_tag1)
        container[self.neptune_tags_path].remove(to_remove_tag2)  # remove non existing tag
        container.sync()

        assert container[self.neptune_tags_path].fetch() == {
            remaining_tag1,
            remaining_tag2,
        }


class TestFetchTable(BaseE2ETest):
    def test_fetch_runs_by_tag(self, environment):
        tag1, tag2 = str(uuid.uuid4()), str(uuid.uuid4())

        with neptune.init_run(project=environment.project) as run:
            run_id1 = run["sys/id"].fetch()
            run["sys/tags"].add(tag1)
            run["sys/tags"].add(tag2)

        with neptune.init_run(project=environment.project) as run:
            run["sys/tags"].add(tag2)

        # wait for the cache to fill
        time.sleep(5)

        project = neptune.get_project(name=environment.project)

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

    def test_fetch_runs_table(self, environment):
        def init_run():
            return neptune.init_run(project=environment.project)

        def get_runs_as_rows(**kwargs):
            project = neptune.get_project(name=environment.project)
            return project.fetch_runs_table(**kwargs).to_rows()

        self._test_fetch_from_container(init_run, get_runs_as_rows)

    def test_fetch_models_table(self, environment):
        def init_run():
            return neptune.init_model(project=environment.project, key=a_key())

        def get_models_as_rows(**kwargs):
            project = neptune.get_project(name=environment.project)
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
