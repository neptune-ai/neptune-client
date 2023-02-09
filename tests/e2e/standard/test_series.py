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
from neptune.new.types import (
    FileSeries,
    FloatSeries,
    StringSeries,
)
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

BASIC_SERIES_TYPES = ["strings"]  # , "floats", "files"]


class TestSeries(BaseE2ETest):
    @pytest.mark.parametrize("series_type", BASIC_SERIES_TYPES)
    @pytest.mark.parametrize("container", AVAILABLE_CONTAINERS, indirect=True)
    def test_log(self, container: MetadataContainer, series_type: str):
        with self.run_then_assert(container, series_type) as (
            namespace,
            values,
            steps,
            timestamps,
        ):
            for value, step, timestamp in zip(values, steps, timestamps):
                namespace.log(value, step=step, timestamp=timestamp)

    @pytest.mark.parametrize("series_type", BASIC_SERIES_TYPES)
    @pytest.mark.parametrize("container", AVAILABLE_CONTAINERS, indirect=True)
    def test_append(self, container: MetadataContainer, series_type: str):
        with self.run_then_assert(container, series_type) as (namespace, values, steps, timestamps):
            for value, step, timestamp in zip(values, steps, timestamps):
                namespace.append(value, step=step, timestamp=timestamp)

    @pytest.mark.parametrize("series_type", BASIC_SERIES_TYPES)
    @pytest.mark.parametrize("container", AVAILABLE_CONTAINERS, indirect=True)
    def test_extend(self, container: MetadataContainer, series_type: str):
        with self.run_then_assert(container, series_type) as (namespace, values, steps, timestamps):
            namespace.extend([values[0]], steps=[steps[0]], timestamps=[timestamps[0]])
            namespace.extend(values[1:], steps=steps[1:], timestamps=timestamps[1:])

    @pytest.mark.parametrize("container", AVAILABLE_CONTAINERS, indirect=True)
    def test_float_series_type_assign(self, container: MetadataContainer):
        with self.run_then_assert(container, "floats") as (namespace, values, steps, timestamps):
            namespace.assign(FloatSeries(values=values, steps=steps, timestamps=timestamps))

    @pytest.mark.parametrize("container", AVAILABLE_CONTAINERS, indirect=True)
    def test_string_series_type_assign(self, container: MetadataContainer):
        with self.run_then_assert(container, "strings") as (namespace, values, steps, timestamps):
            namespace.assign(StringSeries(values=values, steps=steps, timestamps=timestamps))

    @pytest.mark.parametrize("container", AVAILABLE_CONTAINERS, indirect=True)
    def test_file_series_type_assign(self, container: MetadataContainer):
        with self.run_then_assert(container, "files") as (namespace, values, steps, timestamps):
            namespace.assign(FileSeries(values=values, steps=steps, timestamps=timestamps))

    @contextmanager
    def run_then_assert(self, container: MetadataContainer, series_type: str):
        key = self.gen_key()

        if series_type == "floats":
            # given
            n = 5
            values = list(random.random() for _ in range(n))
            steps = sorted(random.sample(range(1, 100), n))
            timestamps = [
                1675876469,
                1675876470,
                1675876471,
                1675876472,
                1675876473,
            ]

            # datetime(2023, 2, 8)

            # when
            yield container[key], values, steps, timestamps  # 1675811871.0 vs 1675815350.0
            container.sync()

            # then
            assert container[key].fetch_last() == values[-1]
            assert list(container[key].fetch_values()["value"]) == values
            assert list(container[key].fetch_values()["step"]) == steps
            assert (
                list(map(lambda timestamp: timestamp.timestamp(), container[key].fetch_values()["timestamp"]))
                == timestamps
            )

        elif series_type == "strings":
            # given
            n = 5
            values = list(fake.word() for _ in range(n))
            steps = sorted(random.sample(range(1, 100), n))
            timestamps = [
                1675876469,
                1675876470,
                1675876471,
                1675876472,
                1675876473,
            ]
            # random.random() * timedelta(days=1)).timetuple()

            # when
            yield container[key], values, steps, timestamps
            container.sync()

            # then
            assert container[key].fetch_last() == values[-1]
            assert list(container[key].fetch_values()["value"]) == values
            assert list(container[key].fetch_values()["step"]) == steps
            assert (
                list(map(lambda timestamp: timestamp.timestamp(), container[key].fetch_values()["timestamp"]))
                == timestamps
            )

        elif series_type == "files":
            # given
            images = list(generate_image(size=2**n) for n in range(8, 12))
            steps = sorted(random.sample(range(1, 100), 4))
            timestamps = [
                1675876469,
                1675876470,
                1675876471,
                1675876472,
            ]

            # when
            yield container[key], images, steps, timestamps
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
