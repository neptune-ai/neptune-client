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

import pytest
from PIL import Image
from faker import Faker

from neptune.new.attribute_container import AttributeContainer

from e2e_tests.base import BaseE2ETest
from e2e_tests.utils import generate_image, image_to_png, tmp_context

fake = Faker()


class TestSeries(BaseE2ETest):
    @pytest.mark.parametrize("container", ["project", "run"], indirect=True)
    def test_log_numbers(self, container: AttributeContainer):
        key = self.gen_key()
        values = [random.random() for _ in range(50)]

        container[key].log(values[0])
        container[key].log(values[1:])
        container.sync()

        assert container[key].fetch_last() == values[-1]

        fetched_values = container[key].fetch_values()
        assert list(fetched_values["value"]) == values

    @pytest.mark.parametrize("container", ["project", "run"], indirect=True)
    def test_log_strings(self, container: AttributeContainer):
        key = self.gen_key()
        values = [fake.word() for _ in range(50)]

        container[key].log(values[0])
        container[key].log(values[1:])
        container.sync()

        assert container[key].fetch_last() == values[-1]

        fetched_values = container[key].fetch_values()
        assert list(fetched_values["value"]) == values

    @pytest.mark.parametrize("container", ["project", "run"], indirect=True)
    def test_log_images(self, container: AttributeContainer):
        key = self.gen_key()
        # images with size between 200KB - 12MB
        images = list(generate_image(size=2 ** n) for n in range(8, 12))

        container[key].log(images[0])
        container[key].log(images[1:])
        container.sync()

        with tmp_context():
            container[key].download_last("last")
            container[key].download("all")

            with Image.open("last/3.png") as img:
                assert img == image_to_png(image=images[-1])

            for i in range(4):
                with Image.open(f"all/{i}.png") as img:
                    assert img == image_to_png(image=images[i])
