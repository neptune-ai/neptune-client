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
from time import sleep

import pytest

import neptune.new as neptune
from e2e_tests.base import BaseE2ETest
from e2e_tests.utils import initialize_container
from neptune.new.internal.container_type import ContainerType


class TestTrashObjects(BaseE2ETest):
    """
    Test Project specific functions
    """

    @pytest.mark.parametrize("container_type", ["model", "run"])
    def test_trash_runs_and_models(self, environment, container_type):
        # WITH project
        project = neptune.init_project(name=environment.project)

        # AND its runs and models
        run1_id = initialize_container(ContainerType.RUN, project=environment.project)[
            "sys/id"
        ].fetch()
        run2_id = initialize_container(ContainerType.RUN, project=environment.project)[
            "sys/id"
        ].fetch()
        model1_id = initialize_container(ContainerType.MODEL, project=environment.project)[
            "sys/id"
        ].fetch()
        model2_id = initialize_container(ContainerType.MODEL, project=environment.project)[
            "sys/id"
        ].fetch()

        # WHEN trash one run and one model
        project.trash_objects([run1_id, model1_id])
        # wait for the elasticsearch cache to fill
        sleep(5)

        # THEN trashed runs are marked as trashed
        runs = project.fetch_runs_table().to_pandas()
        assert run1_id in runs[runs["sys/trashed"] == True]["sys/id"].tolist()
        assert run2_id in runs[runs["sys/trashed"] == False]["sys/id"].tolist()

        # AND trashed models are marked as trashed
        models = project.fetch_models_table().to_pandas()
        assert model1_id in models[models["sys/trashed"] == True]["sys/id"].tolist()
        assert model2_id in models[models["sys/trashed"] == False]["sys/id"].tolist()

    def test_trash_model_version(self, environment):
        # WITH project
        project = neptune.init_project(name=environment.project)

        # AND its model
        model = initialize_container(ContainerType.MODEL, project=environment.project)
        model_id = model["sys/id"].fetch()

        # AND model's model versions
        model_version1 = neptune.init_model_version(model=model_id, project=environment.project)[
            "sys/id"
        ].fetch()
        model_version2 = neptune.init_model_version(model=model_id, project=environment.project)[
            "sys/id"
        ].fetch()

        # WHEN model version is trashed
        project.trash_objects(model_version1)
        # wait for the elasticsearch cache to fill
        sleep(5)

        # THEN expect this version to be trashed
        model_versions = model.fetch_model_versions_table().to_pandas()
        assert (
            model_version1
            in model_versions[model_versions["sys/trashed"] == True]["sys/id"].tolist()
        )
        assert (
            model_version2
            in model_versions[model_versions["sys/trashed"] == False]["sys/id"].tolist()
        )

        # WHEN whole model is trashed
        project.trash_objects(model_id)
        # wait for the elasticsearch cache to fill
        sleep(5)

        # THEN expect all its versions to be trashed
        model_versions = model.fetch_model_versions_table().to_pandas()
        assert (
            model_version1
            in model_versions[model_versions["sys/trashed"] == True]["sys/id"].tolist()
        )
        assert (
            model_version2
            in model_versions[model_versions["sys/trashed"] == True]["sys/id"].tolist()
        )
