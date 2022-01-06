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
from faker import Faker
import neptune.new as neptune

from e2e_tests.base import BaseE2ETest

fake = Faker()


def store_in_run(run_short_id: str, project: str, destination: str):
    reinitialized_run = neptune.init(run=run_short_id, project=project)
    reinitialized_run[destination] = fake.color()
    reinitialized_run.sync()


class TestMultipleRuns(BaseE2ETest):
    @pytest.mark.parametrize("container", ["run"], indirect=True)
    def test_multiple_runs_single(self, container: neptune.Run, environment):
        # pylint: disable=protected-access,undefined-loop-variable

        number_of_reinitialized = 5
        namespace = fake.unique.word()

        reinitialized_runs = [
            neptune.init(run=container._short_id, project=environment.project)
            for _ in range(number_of_reinitialized)
        ]

        container[f"{namespace}/{fake.unique.word()}"] = fake.color()
        container.sync()

        random.shuffle(reinitialized_runs)
        for run in reinitialized_runs:
            run[f"{namespace}/{fake.unique.word()}"] = fake.color()

        random.shuffle(reinitialized_runs)
        for run in reinitialized_runs:
            run.sync()

        container.sync()

        assert len(container[namespace].fetch()) == number_of_reinitialized + 1

    @pytest.mark.skip(reason="no way of currently testing this")
    @pytest.mark.parametrize("container", ["run"], indirect=True)
    def test_multiple_runs_processes(self, container: neptune.Run, environment):
        # pylint: disable=protected-access

        number_of_reinitialized = 10
        namespace = fake.unique.word()

        container[f"{namespace}/{fake.unique.word()}"] = fake.color()

        with concurrent.futures.ProcessPoolExecutor(max_workers=3) as executor:
            futures = [
                executor.submit(
                    store_in_run,
                    container._short_id,
                    environment.project,
                    f"{namespace}/{fake.unique.word()}",
                )
                for _ in range(number_of_reinitialized)
            ]
            for future in concurrent.futures.as_completed(futures):
                _ = future.result()

        container.sync()

        assert len(container[namespace].fetch()) == number_of_reinitialized + 1

    @pytest.mark.parametrize("container", ["run"], indirect=True)
    def test_multiple_runs_thread(self, container: neptune.Run, environment):
        # pylint: disable=protected-access

        number_of_reinitialized = 10
        namespace = fake.unique.word()

        container[f"{namespace}/{fake.unique.word()}"] = fake.color()

        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            futures = [
                executor.submit(
                    store_in_run,
                    container._short_id,
                    environment.project,
                    f"{namespace}/{fake.unique.word()}",
                )
                for _ in range(number_of_reinitialized)
            ]
            for future in concurrent.futures.as_completed(futures):
                _ = future.result()

        container.sync()

        assert len(container[namespace].fetch()) == number_of_reinitialized + 1
