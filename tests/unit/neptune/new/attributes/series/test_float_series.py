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
from mock import (
    MagicMock,
    patch,
)

from neptune.attributes.series.float_series import FloatSeries
from neptune.utils import stringify_unsupported
from tests.unit.neptune.new.attributes.test_attribute_base import TestAttributeBase


@patch("time.time", new=TestAttributeBase._now)
class TestFloatSeries(TestAttributeBase):
    def test_assign_type_error(self):
        values = [["text"], 55, "string", None]
        for value in values:
            with self.assertRaises(Exception):
                FloatSeries(MagicMock(), MagicMock()).assign(value)

    def test_log_type_error(self):
        values = [["text"], [[]], [5, ""], "string", None]
        for value in values:
            with self.assertRaises(Exception):
                FloatSeries(MagicMock(), MagicMock()).log(value)

    def test_get(self):
        with self._exp() as exp:
            var = FloatSeries(exp, self._random_path())
            var.log(5)
            var.log(34)
            self.assertEqual(34, var.fetch_last())

    def test_log(self):
        with self._exp() as exp:
            var = FloatSeries(exp, self._random_path())
            var.log([val for val in range(0, 5000)])
            self.assertEqual(4999, var.fetch_last())
            values = list(var.fetch_values()["value"].array)
            expected = list(range(0, 5000))
            self.assertEqual(len(set(expected)), len(set(values)))

    def test_handle_stringified_value(self):
        val = FloatSeries(MagicMock(), MagicMock())._handle_stringified_value(stringify_unsupported(420.2137))
        assert val == 420.2137
