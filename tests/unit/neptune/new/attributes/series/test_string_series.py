#
# Copyright (c) 2020, Neptune Labs Sp. z o.o.
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
from faker import Faker
from mock import (
    MagicMock,
    patch,
)

from neptune.attributes.series.string_series import StringSeries
from neptune.internal.backends.api_model import (
    StringPointValue,
    StringSeriesValues,
)
from tests.unit.neptune.new.attributes.test_attribute_base import TestAttributeBase

fake = Faker()


@patch("time.time", new=TestAttributeBase._now)
class TestStringSeries(TestAttributeBase):
    def _get_string_series_values_dummy_impl(self, series: StringSeriesValues):
        def _wrapper(self, _container_id, _container_type, _path, offset, limit) -> StringSeriesValues:
            if offset < 0:
                raise ValueError("Offset must be non-negative")
            if limit < 0:
                raise ValueError("Limit must be non-negative")
            return StringSeriesValues(
                values=series.values[offset : offset + limit], totalItemCount=series.totalItemCount
            )

        return _wrapper

    def test_assign_type_error(self):
        values = [55, "string", None]
        for value in values:
            with self.assertRaises(Exception):
                StringSeries(MagicMock(), MagicMock()).assign(value)

    def test_get(self):
        with self._exp() as exp:
            var = StringSeries(exp, self._random_path())
            var.log("asdfhadh")
            var.log("hej!")
            self.assertEqual("hej!", var.fetch_last())

    def test_log(self):
        with self._exp() as exp:
            var = StringSeries(exp, self._random_path())
            var.log([str(val) for val in range(0, 5000)])
            self.assertEqual("4999", var.fetch_last())
            values = list(var.fetch_values()["value"].array)
            expected = list(range(0, 5000))
            self.assertEqual(len(set(expected)), len(set(values)))

    @patch("neptune.attributes.series.fetchable_series.MAX_FETCH_LIMIT", 7)
    def test_custom_offset_limit(self):
        total = 100
        offset = 27
        limit = 42
        words = [fake.word() for _ in range(total)]
        series = StringSeriesValues(
            values=[StringPointValue(step=i, value=words[i], timestampMillis=i) for i in range(total)],
            totalItemCount=total,
        )
        with self._exp() as exp:
            with patch(
                "neptune.internal.backends.neptune_backend_mock.NeptuneBackendMock.get_string_series_values",
                new=self._get_string_series_values_dummy_impl(series),
            ):
                var = StringSeries(exp, self._random_path())
                values = list(var.fetch_values(offset=offset, limit=limit)["value"].array)
                expected = words[offset : offset + limit]
                self.assertEqual(expected, values)
