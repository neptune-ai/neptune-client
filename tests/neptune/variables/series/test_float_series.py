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

from mock import MagicMock, call, patch
from neptune.internal.operation import ClearFloatLog, LogFloats

from neptune.variables.series.float_series import FloatSeries, FloatSeriesVal

from tests.neptune.variables.test_variable_base import TestVariableBase


@patch("time.time", new=TestVariableBase._now)
class TestFloatSeries(TestVariableBase):

    def test_assign(self):
        value = FloatSeriesVal([17, 3.6])
        expected = [LogFloats.ValueType(17, None, self._now()), LogFloats.ValueType(3.6, None, self._now())]

        backend, processor = MagicMock(), MagicMock()
        exp, path, wait = self._create_experiment(backend, processor), self._random_path(), self._random_wait()
        var = FloatSeries(exp, path)
        var.assign(value, wait=wait)
        self.assertEqual(2, processor.enqueue_operation.call_count)
        processor.enqueue_operation.assert_has_calls([
            call(ClearFloatLog(exp._uuid, path), False),
            call(LogFloats(exp._uuid, path, expected), wait)
        ])

    def test_assign_empty(self):
        backend, processor = MagicMock(), MagicMock()
        exp, path, wait = self._create_experiment(backend, processor), self._random_path(), self._random_wait()
        var = FloatSeries(exp, path)
        var.assign(FloatSeriesVal([]), wait=wait)
        processor.enqueue_operation.assert_called_once_with(ClearFloatLog(exp._uuid, path), wait)

    def test_assign_type_error(self):
        values = [[5.], ["text"], [], 55, "string", None]
        for value in values:
            with self.assertRaises(TypeError):
                FloatSeries(MagicMock(), MagicMock()).assign(value)

    def test_log(self):
        value_and_expected = [
            (13, LogFloats.ValueType(13, None, self._now())),
            (15.3, LogFloats.ValueType(15.3, None, self._now()))
        ]

        for value, expected in value_and_expected:
            backend, processor = MagicMock(), MagicMock()
            exp, path, wait = self._create_experiment(backend, processor), self._random_path(), self._random_wait()
            var = FloatSeries(exp, path)
            var.log(value, wait=wait)
            processor.enqueue_operation.assert_called_once_with(LogFloats(exp._uuid, path, [expected]), wait)

    def test_log_with_step(self):
        value_step_and_expected = [
            (13, 5.3, LogFloats.ValueType(13, 5.3, self._now())),
            (15.3, 10, LogFloats.ValueType(15.3, 10, self._now()))
        ]

        for value, step, expected in value_step_and_expected:
            backend, processor = MagicMock(), MagicMock()
            exp, path, wait = self._create_experiment(backend, processor), self._random_path(), self._random_wait()
            var = FloatSeries(exp, path)
            var.log(value, step=step, wait=wait)
            processor.enqueue_operation.assert_called_once_with(LogFloats(exp._uuid, path, [expected]), wait)

    def test_log_with_timestamp(self):
        value_step_and_expected = [
            (13, 5.3, LogFloats.ValueType(13, None, 5.3)),
            (15.3, 10, LogFloats.ValueType(15.3, None, 10))
        ]

        for value, ts, expected in value_step_and_expected:
            backend, processor = MagicMock(), MagicMock()
            exp, path, wait = self._create_experiment(backend, processor), self._random_path(), self._random_wait()
            var = FloatSeries(exp, path)
            var.log(value, timestamp=ts, wait=wait)
            processor.enqueue_operation.assert_called_once_with(LogFloats(exp._uuid, path, [expected]), wait)

    def test_clear(self):
        backend, processor = MagicMock(), MagicMock()
        exp, path, wait = self._create_experiment(backend, processor), self._random_path(), self._random_wait()
        var = FloatSeries(exp, path)
        var.clear(wait=wait)
        processor.enqueue_operation.assert_called_once_with(ClearFloatLog(exp._uuid, path), wait)
