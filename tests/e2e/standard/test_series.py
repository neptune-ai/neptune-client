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
from typing import (
    Any,
    List,
)

import pytest
from PIL import Image

from neptune.new.metadata_containers import MetadataContainer
from tests.e2e.base import (
    AVAILABLE_CONTAINERS,
    BaseE2ETest,
    fake,
)
from tests.e2e.utils import (
    SIZE_1KB,
    generate_image,
    image_to_png,
    tmp_context,
)

SERIES_VALUES = ["strings", "floats"]


class TestSeries(BaseE2ETest):
    @pytest.mark.parametrize("values", SERIES_VALUES, indirect=True)
    @pytest.mark.parametrize("container", AVAILABLE_CONTAINERS, indirect=True)
    def test_log(self, container: MetadataContainer, values: List[Any], key: str):
        # when
        container[key].log(values[0])
        container[key].log(values[1:])
        container.sync()

        # then
        assert container[key].fetch_last() == values[-1]
        assert list(container[key].fetch_values()["value"]) == values

    @pytest.mark.parametrize("container", AVAILABLE_CONTAINERS, indirect=True)
    def test_log_images(self, container: MetadataContainer, key: str):
        # given
        images = [generate_image(size=32 * SIZE_1KB) for _ in range(4)]

        # when
        container[key].log(images[0])
        container[key].log(images[1:])
        container.sync()

        # then
        assert_images(container=container, key=key, images=images)

    @pytest.mark.parametrize("values", SERIES_VALUES, indirect=True)
    @pytest.mark.parametrize("container", AVAILABLE_CONTAINERS, indirect=True)
    def test_append(self, container: MetadataContainer, values: List[Any], key: str):
        # when
        for value in values:
            container[key].append(value)
        container.sync()

        # then
        assert container[key].fetch_last() == values[-1]
        assert list(container[key].fetch_values()["value"]) == values

    @pytest.mark.parametrize("container", AVAILABLE_CONTAINERS, indirect=True)
    def test_append_images(self, container: MetadataContainer, key: str):
        # given
        # images with size between 200KB - 12MB
        images = list(generate_image(size=2**n) for n in range(8, 12))

        # when
        for value in images:
            container[key].append(value)
        container.sync()

        # then
        assert_images(container=container, key=key, images=images)

    @pytest.mark.parametrize("values", SERIES_VALUES, indirect=True)
    @pytest.mark.parametrize("container", AVAILABLE_CONTAINERS, indirect=True)
    def test_extend(self, container: MetadataContainer, values: List[Any], key: str):
        # when
        container[key].extend([values[0]])
        container[key].extend(values[1:])
        container.sync()

        # then
        assert container[key].fetch_last() == values[-1]
        assert list(container[key].fetch_values()["value"]) == values

    @pytest.mark.parametrize("container", AVAILABLE_CONTAINERS, indirect=True)
    def test_extend_images(self, container: MetadataContainer, key: str):
        # given
        # images with size between 200KB - 12MB
        images = list(generate_image(size=2**n) for n in range(8, 12))

        # when
        container[key].extend([images[0]])
        container[key].extend(images[1:])
        container.sync()

        # then
        assert_images(container=container, key=key, images=images)

    @pytest.fixture()
    def key(self):
        yield self.gen_key()


@pytest.fixture()
def values(request):
    if request.param == "floats":
        yield list(random.random() for _ in range(50))

    elif request.param == "strings":
        yield list(fake.word() for _ in range(50))

    elif request.param == "images":
        # images with size between 200KB - 12MB
        yield list(generate_image(size=2**n) for n in range(8, 12))


def assert_images(container: MetadataContainer, key: str, images: List["Image"]):
    with tmp_context():
        container[key].download_last("last")
        container[key].download("all")

        with Image.open("last/3.png") as img:
            assert img == image_to_png(image=images[-1])

        for i in range(4):
            with Image.open(f"all/{i}.png") as img:
                assert img == image_to_png(image=images[i])
