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
from contextlib import contextmanager

import pytest
from PIL import Image

from neptune.new.metadata_containers import MetadataContainer
from tests.e2e.base import (
    AVAILABLE_CONTAINERS,
    BaseE2ETest,
    fake,
)
from tests.e2e.utils import (
    generate_image,
    image_to_png,
    tmp_context,
)

BASIC_SERIES_TYPES = ["strings", "floats", "images"]


class TestSeries(BaseE2ETest):
    @pytest.mark.parametrize("series_type", BASIC_SERIES_TYPES)
    @pytest.mark.parametrize("container", AVAILABLE_CONTAINERS, indirect=True)
    def test_log(self, container: MetadataContainer, series_type: str):
        with self.run_operations_then_assert(container=container, series_type=series_type) as (namespace, values):
            namespace.log(values[0])
            namespace.log(values[1:])

    @pytest.mark.parametrize("series_type", BASIC_SERIES_TYPES)
    @pytest.mark.parametrize("container", AVAILABLE_CONTAINERS, indirect=True)
    def test_append(self, container: MetadataContainer, series_type: str):
        with self.run_operations_then_assert(container=container, series_type=series_type) as (namespace, values):
            for value in values:
                namespace.append(value)

    @pytest.mark.parametrize("series_type", BASIC_SERIES_TYPES)
    @pytest.mark.parametrize("container", AVAILABLE_CONTAINERS, indirect=True)
    def test_extend(self, container: MetadataContainer, series_type: str):
        with self.run_operations_then_assert(container=container, series_type=series_type) as (namespace, values):
            namespace.extend([values[0]])
            namespace.extend(values[1:])

    @contextmanager
    def run_operations_then_assert(self, container: MetadataContainer, series_type: str):
        key = self.gen_key()

        if series_type == "floats":
            # given
            values = list(random.random() for _ in range(50))

            # when
            yield container[key], values
            container.sync()

            # then
            assert container[key].fetch_last() == values[-1]
            assert list(container[key].fetch_values()["value"]) == values

        elif series_type == "strings":
            # given
            values = list(fake.word() for _ in range(50))

            # when
            yield container[key], values
            container.sync()

            # then
            assert container[key].fetch_last() == values[-1]
            assert list(container[key].fetch_values()["value"]) == values

        elif series_type == "images":
            # given
            images = list(generate_image(size=2**n) for n in range(8, 12))

            # when
            yield container[key], images
            container.sync()

            # then
            with tmp_context():
                container[key].download_last("last")
                container[key].download("all")

                with Image.open("last/3.png") as img:
                    assert img == image_to_png(image=images[-1])

                for i in range(4):
                    with Image.open(f"all/{i}.png") as img:
                        assert img == image_to_png(image=images[i])
