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
from contextlib import contextmanager
from typing import (
    Any,
    Dict,
    List,
)

import pytest
from PIL import Image

from neptune.metadata_containers import MetadataContainer
from neptune.types import (
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

BASIC_SERIES_TYPES = ["strings", "floats", "files"]
NON_LAST_FETCH_CASES = [  # offset, limit, total
    (0, 0, 3),  # no values to fetch
    (1, 3, 5),  # proper subset of values
    (0, 5, 3),  # limit is too big
]


class TestSeries(BaseE2ETest):
    @pytest.mark.parametrize("series_type", BASIC_SERIES_TYPES)
    @pytest.mark.parametrize("container", AVAILABLE_CONTAINERS, indirect=True)
    def test_log(self, container: MetadataContainer, series_type: str):
        with self.run_then_assert_default(container, series_type) as (
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
        with self.run_then_assert_default(container, series_type) as (namespace, values, steps, timestamps):
            for value, step, timestamp in zip(values, steps, timestamps):
                namespace.append(value, step=step, timestamp=timestamp)

    @pytest.mark.parametrize("series_type", BASIC_SERIES_TYPES)
    @pytest.mark.parametrize("container", AVAILABLE_CONTAINERS, indirect=True)
    def test_extend(self, container: MetadataContainer, series_type: str):
        with self.run_then_assert_default(container, series_type) as (namespace, values, steps, timestamps):
            namespace.extend([values[0]], steps=[steps[0]], timestamps=[timestamps[0]])
            namespace.extend(values[1:], steps=steps[1:], timestamps=timestamps[1:])

    @pytest.mark.parametrize("container", AVAILABLE_CONTAINERS, indirect=True)
    def test_float_series_type_assign(self, container: MetadataContainer):
        with self.run_then_assert_default(container, "floats") as (namespace, values, steps, timestamps):
            namespace.assign(FloatSeries(values=values, steps=steps, timestamps=timestamps))

    @pytest.mark.parametrize("container", AVAILABLE_CONTAINERS, indirect=True)
    def test_string_series_type_assign(self, container: MetadataContainer):
        with self.run_then_assert_default(container, "strings") as (namespace, values, steps, timestamps):
            namespace.assign(StringSeries(values=values, steps=steps, timestamps=timestamps))

    @pytest.mark.parametrize("container", AVAILABLE_CONTAINERS, indirect=True)
    def test_file_series_type_assign(self, container: MetadataContainer):
        with self.run_then_assert_default(container, "files") as (namespace, values, steps, timestamps):
            namespace.assign(FileSeries(values=values, steps=steps, timestamps=timestamps))

    @pytest.mark.parametrize("offset,limit,total", NON_LAST_FETCH_CASES)
    @pytest.mark.parametrize("series_type", ["strings", "floats"])
    @pytest.mark.parametrize("container", AVAILABLE_CONTAINERS, indirect=True)
    def test_series_non_last_fetch(
        self, container: MetadataContainer, series_type: str, offset: int, limit: int, total: int
    ):
        fetch_kwargs = {"offset": offset, "limit": limit}
        steps = list(sorted(random.sample(range(1, 100), total)))
        timestamps = list(sorted([time.mktime(fake.date_time_this_month().utctimetuple()) for _ in range(total)]))

        if series_type == "floats":
            values = [random.random() for _ in range(total)]
            series_init = FloatSeries
        elif series_type == "strings":
            values = [fake.word() for _ in range(total)]
            series_init = StringSeries
        elif series_type == "files":
            values = [generate_image(size=2**n) for n in range(7, 7 + total)]
            series_init = FileSeries
        else:
            raise ValueError(f"Unknown series type: {series_type}")

        expected_steps = steps[offset : offset + limit]
        expected_timestamps = timestamps[offset : offset + limit]
        expected_values = values[offset : offset + limit]

        with self.run_then_assert_with_expected(
            container,
            series_type,
            expected_steps,
            expected_timestamps,
            expected_values,
            last_value=values[-1],
            fetch_kwargs=fetch_kwargs,
        ) as (namespace, _, _, _):
            namespace.assign(series_init(values=values, steps=steps, timestamps=timestamps))

    @contextmanager
    def run_then_assert_default(
        self, container: MetadataContainer, series_type: str, fetch_kwargs: Dict[str, Any] = {}
    ):
        steps = sorted(random.sample(range(1, 100), 5))
        timestamps = [
            1675876469.0,
            1675876470.0,
            1675876471.0,
            1675876472.0,
            1675876473.0,
        ]

        if series_type == "floats":
            values = list(random.random() for _ in range(5))
        elif series_type == "strings":
            values = list(fake.word() for _ in range(5))
        elif series_type == "files":
            values = list(generate_image(size=2**n) for n in range(7, 12))
        else:
            raise ValueError(f"Unknown series type: {series_type}")

        with self.run_then_assert_with_expected(
            container, series_type, steps, timestamps, values, last_value=values[-1], fetch_kwargs=fetch_kwargs
        ) as result:
            yield result

    @contextmanager
    def run_then_assert_with_expected(
        self,
        container: MetadataContainer,
        series_type: str,
        steps: List[int],
        timestamps: List[float],
        values: List[Any],
        last_value: Any,
        fetch_kwargs: Dict[str, Any] = {},
    ):
        key = self.gen_key()
        assert len(steps) == len(timestamps) == len(values)

        if series_type == "floats":
            assert all(isinstance(v, float) for v in values)

            # when
            yield container[key], values, steps, timestamps
            container.sync()

            # then
            assert container[key].fetch_last() == last_value
            assert list(container[key].fetch_values(**fetch_kwargs).get("value", [])) == values
            assert list(container[key].fetch_values(**fetch_kwargs).get("step", [])) == steps
            assert (
                list(
                    map(
                        lambda t: time.mktime(t.utctimetuple()),
                        container[key].fetch_values(**fetch_kwargs).get("timestamp", []),
                    )
                )
                == timestamps
            )

        elif series_type == "strings":
            assert all(isinstance(v, str) for v in values)

            # when
            yield container[key], values, steps, timestamps

            container.sync()

            # then
            assert container[key].fetch_last() == last_value
            assert list(container[key].fetch_values(**fetch_kwargs).get("value", [])) == values
            assert list(container[key].fetch_values(**fetch_kwargs).get("step", [])) == steps
            assert (
                list(
                    map(
                        lambda t: time.mktime(t.utctimetuple()),
                        container[key].fetch_values(**fetch_kwargs).get("timestamp", []),
                    )
                )
                == timestamps
            )

        elif series_type == "files":
            assert all(isinstance(v, Image.Image) for v in values)

            # when
            yield container[key], values, steps, timestamps

            container.sync()

            # then
            with tmp_context():
                n_values = len(values)
                container[key].download_last("last")
                container[key].download("all")

                with Image.open(f"last/{n_values-1}.png") as img:
                    assert img == image_to_png(image=last_value)

                for i in range(n_values):
                    with Image.open(f"all/{i}.png") as img:
                        assert img == image_to_png(image=values[i])
