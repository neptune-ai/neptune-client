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
    call,
    patch,
)

from neptune.attributes.series.float_series import (
    FloatSeries,
    FloatSeriesVal,
)
from neptune.attributes.series.string_series import (
    StringSeries,
    StringSeriesVal,
)
from neptune.internal.operation import (
    ClearFloatLog,
    ClearStringLog,
    ConfigFloatSeries,
    LogFloats,
)
from tests.unit.neptune.new.attributes.test_attribute_base import TestAttributeBase


@patch("time.time", new=TestAttributeBase._now)
class TestSeries(TestAttributeBase):
    @patch("neptune.metadata_containers.metadata_container.get_operation_processor")
    def test_assign(self, get_operation_processor):
        value = FloatSeriesVal([17, 3.6], min=0, max=100, unit="%")
        expected = [
            LogFloats.ValueType(17, None, self._now()),
            LogFloats.ValueType(3.6, None, self._now()),
        ]

        processor = MagicMock()
        get_operation_processor.return_value = processor
        path, wait = (
            self._random_path(),
            self._random_wait(),
        )
        with self._exp() as exp:
            var = FloatSeries(exp, path)
            var.assign(value, wait=wait)
            processor.enqueue_operation.assert_has_calls(
                [
                    call(ConfigFloatSeries(path, min=0, max=100, unit="%"), wait=False),
                    call(ClearFloatLog(path), wait=False),
                    call(LogFloats(path, expected), wait=wait),
                ]
            )

    @patch("neptune.metadata_containers.metadata_container.get_operation_processor")
    def test_assign_empty(self, get_operation_processor):
        processor = MagicMock()
        get_operation_processor.return_value = processor

        with self._exp() as exp:
            path, wait = (
                self._random_path(),
                self._random_wait(),
            )
            var = StringSeries(exp, path)
            var.assign(StringSeriesVal([]), wait=wait)
            processor.enqueue_operation.assert_called_with(ClearStringLog(path), wait=wait)

    @patch("neptune.metadata_containers.metadata_container.get_operation_processor")
    def test_log(self, get_operation_processor):
        value_and_expected = [
            (13, [LogFloats.ValueType(13, None, self._now())]),
            (15.3, [LogFloats.ValueType(15.3, None, self._now())]),
            (
                [1, 9, 7],
                [
                    LogFloats.ValueType(1, None, self._now()),
                    LogFloats.ValueType(9, None, self._now()),
                    LogFloats.ValueType(7, None, self._now()),
                ],
            ),
            (
                (1, 9, 7),
                [
                    LogFloats.ValueType(1, None, self._now()),
                    LogFloats.ValueType(9, None, self._now()),
                    LogFloats.ValueType(7, None, self._now()),
                ],
            ),
            (
                {1, 9, 7},
                [
                    LogFloats.ValueType(1, None, self._now()),
                    LogFloats.ValueType(9, None, self._now()),
                    LogFloats.ValueType(7, None, self._now()),
                ],
            ),
        ]

        for value, expected in value_and_expected:
            processor = MagicMock()
            get_operation_processor.return_value = processor

            with self._exp() as exp:
                path, wait = (
                    self._random_path(),
                    self._random_wait(),
                )
                var = FloatSeries(exp, path)
                var.log(value, wait=wait)
                processor.enqueue_operation.assert_called_with(LogFloats(path, expected), wait=wait)

    @patch("neptune.metadata_containers.metadata_container.get_operation_processor")
    def test_log_with_step(self, get_operation_processor):
        value_step_and_expected = [
            (13, 5.3, LogFloats.ValueType(13, 5.3, self._now())),
            (15.3, 10, LogFloats.ValueType(15.3, 10, self._now())),
            ([13], 5.3, LogFloats.ValueType(13, 5.3, self._now())),
            ((13,), 5.3, LogFloats.ValueType(13, 5.3, self._now())),
            ({13}, 5.3, LogFloats.ValueType(13, 5.3, self._now())),
        ]

        for value, step, expected in value_step_and_expected:
            processor = MagicMock()
            get_operation_processor.return_value = processor

            with self._exp() as exp:
                path, wait = (
                    self._random_path(),
                    self._random_wait(),
                )
                var = FloatSeries(exp, path)
                var.log(value, step=step, wait=wait)
                processor.enqueue_operation.assert_called_with(LogFloats(path, [expected]), wait=wait)

    @patch("neptune.metadata_containers.metadata_container.get_operation_processor")
    def test_log_with_timestamp(self, get_operation_processor):
        value_step_and_expected = [
            (13, 5.3, LogFloats.ValueType(13, None, 5.3)),
            (15.3, 10, LogFloats.ValueType(15.3, None, 10)),
        ]

        for value, ts, expected in value_step_and_expected:
            processor = MagicMock()
            get_operation_processor.return_value = processor

            with self._exp() as exp:
                path, wait = (
                    self._random_path(),
                    self._random_wait(),
                )
                var = FloatSeries(exp, path)
                var.log(value, timestamp=ts, wait=wait)
                processor.enqueue_operation.assert_called_with(LogFloats(path, [expected]), wait=wait)

    @patch("neptune.metadata_containers.metadata_container.get_operation_processor")
    def test_log_value_errors(self, get_operation_processor):
        processor = MagicMock()
        get_operation_processor.return_value = processor

        with self._exp() as exp:
            attr = FloatSeries(exp, self._random_path())

            with self.assertRaises(ValueError):
                attr.log(["str", 5])
            with self.assertRaises(ValueError):
                attr.log([5, 10], step=10)
            with self.assertRaises(TypeError):
                attr.log(5, step="str")
            with self.assertRaises(TypeError):
                attr.log(5, timestamp="str")

    @patch("neptune.metadata_containers.metadata_container.get_operation_processor")
    def test_clear(self, get_operation_processor):
        processor = MagicMock()
        get_operation_processor.return_value = processor

        with self._exp() as exp:
            path, wait = (
                self._random_path(),
                self._random_wait(),
            )
            var = FloatSeries(exp, path)
            var.clear(wait=wait)
            processor.enqueue_operation.assert_called_with(ClearFloatLog(path), wait=wait)
