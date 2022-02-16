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
import itertools

import pytest

from neptune.new.metadata_containers import MetadataContainer

from e2e_tests.base import BaseE2ETest, AVAILABLE_CONTAINERS, fake

# List of every possible container type pair for instance: "run-run, run-model, model-model_version, ..."
ALL_CONTAINERS_PAIRS = list(
    map("-".join, itertools.product(AVAILABLE_CONTAINERS, AVAILABLE_CONTAINERS))
)


class TestCopying(BaseE2ETest):
    @pytest.mark.parametrize("containers_pair", ALL_CONTAINERS_PAIRS, indirect=True)
    @pytest.mark.parametrize(
        "value", [random.randint(0, 100), random.random(), fake.boolean(), fake.word()]
    )
    def test_copy(self, containers_pair: (MetadataContainer, MetadataContainer), value):
        container_a, container_b = containers_pair

        src, destination, destination2 = self.gen_key(), self.gen_key(), self.gen_key()

        container_a[src] = value
        container_a.sync()

        container_b[destination] = container_a[src]
        container_b[destination2] = container_b[destination]
        container_b.sync()

        assert container_a[src].fetch() == value
        assert container_b[destination].fetch() == value
        assert container_b[destination2].fetch() == value
