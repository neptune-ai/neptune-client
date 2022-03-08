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
import concurrent.futures

import pytest
import neptune.new as neptune

from e2e_tests.base import BaseE2ETest, fake
from e2e_tests.utils import reinitialize_container


def store_in_container(
    sys_id: str, project: str, container_type: str, destination: str
):
    container = reinitialize_container(
        sys_id=sys_id, container_type=container_type, project=project
    )
    container[destination] = fake.color()
    container.sync()


class TestMultiple(BaseE2ETest):
    @pytest.mark.parametrize(
        "container", ["run", "model", "model_version"], indirect=True
    )
    def test_single_thread(
        self, container: neptune.metadata_containers.MetadataContainer, environment
    ):
        sys_id = container["sys/id"].fetch()
        number_of_reinitialized = 5
        namespace = self.gen_key()

        reinitialized = [
            reinitialize_container(
                sys_id=sys_id,
                container_type=container.container_type.value,
                project=environment.project,
            )
            for _ in range(number_of_reinitialized)
        ]

        container[f"{namespace}/{fake.unique.word()}"] = fake.color()
        container.sync()

        random.shuffle(reinitialized)
        for reinitialized_container in reinitialized:
            reinitialized_container[f"{namespace}/{fake.unique.word()}"] = fake.color()

        random.shuffle(reinitialized)
        for reinitialized_container in reinitialized:
            reinitialized_container.sync()

        container.sync()

        assert len(container[namespace].fetch()) == number_of_reinitialized + 1

        for r in reinitialized:
            r.stop()

    @pytest.mark.skip(reason="no way of currently testing this")
    @pytest.mark.parametrize(
        "container", ["run", "model", "model_version"], indirect=True
    )
    def test_multiple_processes(self, container: neptune.Run, environment):
        number_of_reinitialized = 10
        namespace = self.gen_key()

        container[f"{namespace}/{fake.unique.word()}"] = fake.color()

        with concurrent.futures.ProcessPoolExecutor(max_workers=3) as executor:
            futures = [
                executor.submit(
                    store_in_container,
                    sys_id=container["sys/id"].fetch(),
                    container_type=container.container_type.value,
                    project=environment.project,
                    destination=f"{namespace}/{fake.unique.word()}",
                )
                for _ in range(number_of_reinitialized)
            ]
            for future in concurrent.futures.as_completed(futures):
                _ = future.result()

        container.sync()

        assert len(container[namespace].fetch()) == number_of_reinitialized + 1

    @pytest.mark.parametrize(
        "container", ["run", "model", "model_version"], indirect=True
    )
    def test_multiple_threads(self, container: neptune.Run, environment):
        number_of_reinitialized = 10
        namespace = self.gen_key()

        container[f"{namespace}/{fake.unique.word()}"] = fake.color()

        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            futures = [
                executor.submit(
                    store_in_container,
                    sys_id=container["sys/id"].fetch(),
                    container_type=container.container_type.value,
                    project=environment.project,
                    destination=f"{namespace}/{fake.unique.word()}",
                )
                for _ in range(number_of_reinitialized)
            ]
            for future in concurrent.futures.as_completed(futures):
                _ = future.result()

        container.sync()

        assert len(container[namespace].fetch()) == number_of_reinitialized + 1
