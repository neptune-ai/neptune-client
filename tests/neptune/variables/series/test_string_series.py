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

# pylint: disable=protected-access
from mock import patch, MagicMock, call
from neptune.variables.series.string_series import StringSeries, StringSeriesVal

from neptune.internal.operation import LogSeriesValue, ClearStringLog, LogStrings
from tests.neptune.variables.test_variable_base import TestVariableBase


@patch("time.time", new=TestVariableBase._now)
class TestStringSeries(TestVariableBase):

    def test_assign(self):
        value = StringSeriesVal(["aaaa", "bbc"])
        expected = [LogSeriesValue[str]("aaaa", None, self._now()), LogSeriesValue[str]("bbc", None, self._now())]

        backend, processor = MagicMock(), MagicMock()
        exp, path, wait = self._create_experiment(backend, processor), self._random_path(), self._random_wait()
        var = StringSeries(exp, path)
        var.assign(value, wait=wait)
        self.assertEqual(2, processor.enqueue_operation.call_count)
        processor.enqueue_operation.assert_has_calls([
            call(ClearStringLog(exp._uuid, path), False),
            call(LogStrings(exp._uuid, path, expected), wait)
        ])

    def test_assign_empty(self):
        backend, processor = MagicMock(), MagicMock()
        exp, path, wait = self._create_experiment(backend, processor), self._random_path(), self._random_wait()
        var = StringSeries(exp, path)
        var.assign(StringSeriesVal([]), wait=wait)
        processor.enqueue_operation.assert_called_once_with(ClearStringLog(exp._uuid, path), wait)

    def test_assign_type_error(self):
        values = [[5.], ["text"], [], 55, "string", None]
        for value in values:
            with self.assertRaises(TypeError):
                StringSeries(MagicMock(), MagicMock()).assign(value)

    def test_log(self):
        value_and_expected = [
            ("xyz", LogSeriesValue[str]("xyz", None, self._now())),
            ("abc", LogSeriesValue[str]("abc", None, self._now()))
        ]

        for value, expected in value_and_expected:
            backend, processor = MagicMock(), MagicMock()
            exp, path, wait = self._create_experiment(backend, processor), self._random_path(), self._random_wait()
            var = StringSeries(exp, path)
            var.log(value, wait=wait)
            processor.enqueue_operation.assert_called_once_with(LogStrings(exp._uuid, path, [expected]), wait)

    def test_log_with_step(self):
        value_step_and_expected = [
            ("xyz", 5.3, LogSeriesValue[str]("xyz", 5.3, self._now())),
            ("abc", 10, LogSeriesValue[str]("abc", 10, self._now()))
        ]

        for value, step, expected in value_step_and_expected:
            backend, processor = MagicMock(), MagicMock()
            exp, path, wait = self._create_experiment(backend, processor), self._random_path(), self._random_wait()
            var = StringSeries(exp, path)
            var.log(value, step=step, wait=wait)
            processor.enqueue_operation.assert_called_once_with(LogStrings(exp._uuid, path, [expected]), wait)

    def test_log_with_timestamp(self):
        value_step_and_expected = [
            ("xyz", 5.3, LogSeriesValue[str]("xyz", None, 5.3)),
            ("abc", 10, LogSeriesValue[str]("abc", None, 10))
        ]

        for value, ts, expected in value_step_and_expected:
            backend, processor = MagicMock(), MagicMock()
            exp, path, wait = self._create_experiment(backend, processor), self._random_path(), self._random_wait()
            var = StringSeries(exp, path)
            var.log(value, timestamp=ts, wait=wait)
            processor.enqueue_operation.assert_called_once_with(LogStrings(exp._uuid, path, [expected]), wait)

    def test_clear(self):
        backend, processor = MagicMock(), MagicMock()
        exp, path, wait = self._create_experiment(backend, processor), self._random_path(), self._random_wait()
        var = StringSeries(exp, path)
        var.clear(wait=wait)
        processor.enqueue_operation.assert_called_once_with(ClearStringLog(exp._uuid, path), wait)
