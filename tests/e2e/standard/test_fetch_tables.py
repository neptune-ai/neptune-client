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
import datetime
import random
import time
import uuid

import pytest

import neptune
from neptune.common.utils import IS_MACOS
from neptune.exceptions import NeptuneInvalidQueryException
from neptune.metadata_containers import Model
from tests.e2e.base import (
    BaseE2ETest,
    are_group_tags_enabled,
    fake,
)
from tests.e2e.utils import a_key

WAIT_DURATION = 60


class TestFetchTable(BaseE2ETest):
    @pytest.mark.parametrize("with_query", [True, False])
    def test_fetch_runs_by_tag(self, environment, project, with_query):
        tag1, tag2 = str(uuid.uuid4()), str(uuid.uuid4())

        with neptune.init_run(project=environment.project) as run:
            run_id1 = run["sys/id"].fetch()
            run["sys/tags"].add(tag1)
            run["sys/tags"].add(tag2)

        with neptune.init_run(project=environment.project) as run:
            run["sys/tags"].add(tag2)

        # wait for the cache to fill
        time.sleep(WAIT_DURATION)

        if with_query:
            kwargs = {"query": f"(sys/tags: stringSet CONTAINS '{tag1}')"}
        else:
            kwargs = {"tag": [tag1, tag2]}

        runs = project.fetch_runs_table(progress_bar=False, **kwargs).to_rows()

        assert len(runs) == 1
        assert runs[0].get_attribute_value("sys/id") == run_id1

    @pytest.mark.skipif(not are_group_tags_enabled(), reason="Group tags are not enabled")
    def test_fetch_runs_by_group_tag(self, environment, project):
        tag1, tag2 = str(uuid.uuid4()), str(uuid.uuid4())

        with neptune.init_run(project=environment.project) as run:
            run_id1 = run["sys/id"].fetch()
            run["sys/group_tags"].add(tag1)
            run["sys/group_tags"].add(tag2)

        with neptune.init_run(project=environment.project) as run:
            run["sys/group_tags"].add(tag2)

        # wait for the cache to fill
        time.sleep(WAIT_DURATION)

        runs = project.fetch_runs_table(
            progress_bar=False,
            query=f"(sys/group_tags: stringSet CONTAINS '{tag1}')",
        ).to_rows()

        assert len(runs) == 1
        assert runs[0].get_attribute_value("sys/id") == run_id1

    @pytest.mark.parametrize("with_query", [True, False])
    @pytest.mark.parametrize("container_fn_scope", ["model"], indirect=True)
    def test_fetch_model_versions_with_correct_ids(self, container_fn_scope: Model, environment, with_query: bool):
        model_sys_id = container_fn_scope["sys/id"].fetch()
        versions_to_initialize = 5

        for _ in range(versions_to_initialize):
            with neptune.init_model_version(model=model_sys_id, project=environment.project):
                pass

        # wait for the elasticsearch cache to fill
        time.sleep(WAIT_DURATION)

        query = "" if with_query else None
        versions_table = sorted(
            container_fn_scope.fetch_model_versions_table(query=query, progress_bar=False).to_rows(),
            key=lambda r: r.get_attribute_value("sys/id"),
        )
        assert len(versions_table) == versions_to_initialize
        for index in range(versions_to_initialize):
            assert versions_table[index].get_attribute_value("sys/id") == f"{model_sys_id}-{index + 1}"

        versions_table_gen = container_fn_scope.fetch_model_versions_table(
            query=query, ascending=True, progress_bar=False
        )
        for te1, te2 in zip(list(versions_table_gen), versions_table):
            assert te1._id == te2._id
            assert te1._container_type == te2._container_type

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
        time.sleep(WAIT_DURATION)

        containers_as_rows = get_containers_as_rows()
        container1 = next(filter(lambda m: m.get_attribute_value("sys/id") == container_id1, containers_as_rows))
        container2 = next(filter(lambda m: m.get_attribute_value("sys/id") == container_id2, containers_as_rows))

        assert container1.get_attribute_value(key1) == value1
        assert container1.get_attribute_value(key2) == value2
        assert container2.get_attribute_value(key1) == value1
        with pytest.raises(ValueError):
            container2.get_attribute_value(key2)

        def get_container1(**kwargs):
            containers_as_rows = get_containers_as_rows(**kwargs)
            return next(filter(lambda m: m.get_attribute_value("sys/id") == container_id1, containers_as_rows))

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
            return project.fetch_runs_table(**kwargs, progress_bar=False).to_rows()

        self._test_fetch_from_container(init_run, get_runs_as_rows)

    def test_fetch_models_table(self, environment, project):
        def init_run():
            return neptune.init_model(project=environment.project, key=a_key())

        def get_models_as_rows(**kwargs):
            return project.fetch_models_table(**kwargs, progress_bar=False).to_rows()

        self._test_fetch_from_container(init_run, get_models_as_rows)

    @pytest.mark.parametrize("container", ["model"], indirect=True)
    def test_fetch_model_versions_table(self, container: Model, environment):
        model_sys_id = container["sys/id"].fetch()

        def init_run():
            return neptune.init_model_version(model=model_sys_id, project=environment.project)

        def get_model_versions_as_rows(**kwargs):
            return container.fetch_model_versions_table(**kwargs, progress_bar=False).to_rows()

        self._test_fetch_from_container(init_run, get_model_versions_as_rows)

    @pytest.mark.parametrize("container", ["model"], indirect=True)
    def test_fetch_model_versions_table_by_query(self, container, environment):
        model_sys_id = container["sys/id"].fetch()
        key = "some_key"
        vals = ["some_val_1", "some_val_2"]
        names = ["name_1", "name_2"]

        for name, val in zip(names, vals):
            with neptune.init_model_version(model=model_sys_id, name=name, project=environment.project) as mv:
                mv[key] = val

        time.sleep(WAIT_DURATION)

        for val, expected_names in zip(vals + ["non_existent_val"], [[names[0]], [names[1]], []]):
            model_versions = container.fetch_model_versions_table(
                query=f"({key}: string = '{val}')",
                sort_by="name",
            ).to_rows()
            assert sorted([mv.get_attribute_value("sys/name") for mv in model_versions]) == sorted(expected_names)

    @pytest.mark.parametrize("with_query", [True, False])
    def test_fetch_runs_table_by_state(self, environment, project, with_query):
        tag = str(uuid.uuid4())
        random_val = random.random()
        with neptune.init_run(project=environment.project, tags=tag) as run:
            run["some_random_val"] = random_val

            time.sleep(WAIT_DURATION)

            if with_query:
                kwargs = {"query": "(sys/state: experimentState = running)"}
            else:
                kwargs = {"state": "active"}

            runs = project.fetch_runs_table(**kwargs, progress_bar=False).to_pandas()

            assert not runs.empty
            assert tag in runs["sys/tags"].values
            assert random_val in runs["some_random_val"].values

        time.sleep(WAIT_DURATION)

        if with_query:
            kwargs = {"query": "(sys/state: experimentState = idle)"}
        else:
            kwargs = {"state": "inactive"}

        runs = project.fetch_runs_table(**kwargs, progress_bar=False).to_pandas()

        assert not runs.empty
        assert tag in runs["sys/tags"].values
        assert random_val in runs["some_random_val"].values

    @pytest.mark.parametrize("ascending", [True, False])
    @pytest.mark.parametrize("with_query", [True, False])
    def test_fetch_runs_table_sorting(self, environment, project, ascending, with_query):
        # given
        with neptune.init_run(project=environment.project, custom_run_id="run1") as run:
            run["metrics/accuracy"] = 0.95
            run["some_val"] = "b"

        with neptune.init_run(project=environment.project, custom_run_id="run2") as run:
            run["metrics/accuracy"] = 0.90
            run["some_val"] = "a"

        time.sleep(WAIT_DURATION)
        query = "" if with_query else None

        # when
        runs = project.fetch_runs_table(
            query=query, sort_by="sys/creation_time", ascending=ascending, progress_bar=False
        ).to_pandas()

        # then
        # runs are correctly sorted by creation time -> run1 was first
        assert not runs.empty
        run_list = runs["sys/custom_run_id"].dropna().to_list()
        if ascending:
            assert run_list == ["run1", "run2"]
        else:
            assert run_list == ["run2", "run1"]

        # when
        runs = project.fetch_runs_table(
            query=query, sort_by="metrics/accuracy", ascending=ascending, progress_bar=False
        ).to_pandas()

        # then
        assert not runs.empty
        run_list = runs["sys/custom_run_id"].dropna().to_list()

        if ascending:
            assert run_list == ["run2", "run1"]
        else:
            assert run_list == ["run1", "run2"]

        # when
        runs = project.fetch_runs_table(
            query=query, sort_by="some_val", ascending=ascending, progress_bar=False
        ).to_pandas()

        # then
        assert not runs.empty
        run_list = runs["sys/custom_run_id"].dropna().to_list()

        if ascending:
            assert run_list == ["run2", "run1"]
        else:
            assert run_list == ["run1", "run2"]

    @pytest.mark.parametrize("with_query", [True, False])
    def test_fetch_runs_table_non_atomic_type(self, environment, project, with_query):
        # test if now it fails when we add a non-atomic type to that field

        query = "" if with_query else None
        # given
        with neptune.init_run(project=environment.project, custom_run_id="run3") as run:
            run["metrics/accuracy"] = 0.9

        with neptune.init_run(project=environment.project, custom_run_id="run4") as run:
            for i in range(5):
                run["metrics/accuracy"].log(0.95)

        time.sleep(WAIT_DURATION)

        # then
        with pytest.raises(ValueError):
            project.fetch_runs_table(query=query, sort_by="metrics/accuracy", progress_bar=False)

    @pytest.mark.parametrize("with_query", [True, False])
    def test_fetch_runs_table_datetime_parsed(self, environment, project, with_query):
        # given
        with neptune.init_run(project=environment.project) as run:
            run["some_timestamp"] = datetime.datetime.now()

        time.sleep(WAIT_DURATION)

        # when
        query = "" if with_query else None
        runs = project.fetch_runs_table(
            query=query, columns=["sys/creation_time", "some_timestamp"], progress_bar=False
        ).to_pandas()

        # then
        assert isinstance(runs["sys/creation_time"].iloc[0], datetime.datetime)
        assert isinstance(runs["some_timestamp"].iloc[0], datetime.datetime)

    @pytest.mark.parametrize("with_query", [True, False])
    def test_fetch_runs_table_limit(self, environment, project, with_query):
        # given
        with neptune.init_run(project=environment.project) as run:
            run["some_val"] = "a"

        with neptune.init_run(project=environment.project) as run:
            run["some_val"] = "b"

        time.sleep(WAIT_DURATION)

        # when
        query = "" if with_query else None
        runs = project.fetch_runs_table(query=query, limit=1, progress_bar=False).to_pandas()

        # then
        assert len(runs) == 1

    @pytest.mark.skipif(IS_MACOS, reason="MacOS behaves strangely on github actions")
    def test_fetch_runs_table_raw_query_trashed(self, environment, project):
        # given
        val: float = 2.2
        with neptune.init_run(project=environment.project, custom_run_id="run1") as run:
            run["key"] = val

        with neptune.init_run(project=environment.project, custom_run_id="run2") as run:
            run["key"] = val

        time.sleep(WAIT_DURATION)

        # when
        runs = project.fetch_runs_table(query=f"(key: float = {val})", progress_bar=False, trashed=False).to_pandas()

        # then
        run_list = runs["sys/custom_run_id"].dropna().to_list()
        assert ["run1", "run2"] == sorted(run_list)

        # when
        neptune.management.trash_objects(
            project=environment.project, ids=runs[runs["sys/custom_run_id"] == "run2"]["sys/id"].item()
        )

        time.sleep(WAIT_DURATION)

        runs = project.fetch_runs_table(query=f"(key: float = {val})", progress_bar=False, trashed=True).to_pandas()

        # then
        run_list = runs["sys/custom_run_id"].dropna().to_list()
        assert ["run2"] == run_list

        # when
        runs = project.fetch_runs_table(query=f"(key: float = {val})", progress_bar=False, trashed=None).to_pandas()

        # then
        run_list = runs["sys/custom_run_id"].dropna().to_list()
        assert ["run1", "run2"] == sorted(run_list)

    def test_fetch_runs_invalid_query_handling(self, project):
        # given
        runs_table = project.fetch_runs_table(query="key: float = (-_-)", progress_bar=False)

        # then
        with pytest.raises(NeptuneInvalidQueryException):
            next(iter(runs_table))

    @pytest.mark.skipif(IS_MACOS, reason="MacOS behaves strangely on github actions")
    def test_fetch_models_raw_query_trashed(self, environment, project):
        # given
        val: float = 2.2
        with neptune.init_model(project=environment.project, key=a_key(), name="name-1") as model:
            model["key"] = val

        with neptune.init_model(project=environment.project, key=a_key(), name="name-2") as model:
            model["key"] = val

        time.sleep(WAIT_DURATION)

        # when
        models = project.fetch_models_table(
            query=f"(key: float = {val})", progress_bar=False, trashed=False
        ).to_pandas()

        # then
        model_list = models["sys/name"].dropna().to_list()
        assert sorted(model_list) == sorted(["name-1", "name-2"])

        # when
        neptune.management.trash_objects(
            project=environment.project, ids=models[models["sys/name"] == "name-1"]["sys/id"].item()
        )

        time.sleep(WAIT_DURATION)

        trashed_vals = [True, False, None]
        expected_model_names = [["name-1"], ["name-2"], ["name-1", "name-2"]]

        for trashed, model_names in zip(trashed_vals, expected_model_names):
            # when
            models = project.fetch_models_table(
                query=f"(key: float = {val})", progress_bar=False, trashed=trashed
            ).to_pandas()

            # then
            model_list = models["sys/name"].dropna().to_list()
            assert sorted(model_list) == sorted(model_names)

    def test_fetch_models_invalid_query_handling(self, project):
        # given
        runs_table = project.fetch_models_table(query="key: float = (-_-)", progress_bar=False)

        # then
        with pytest.raises(NeptuneInvalidQueryException):
            next(iter(runs_table))
