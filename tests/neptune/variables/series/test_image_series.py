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
import numpy
from PIL import Image
from mock import patch, MagicMock, call

from neptune.internal.utils.images import get_image_content
from neptune.variables.series.image_series import ImageSeries, ImageSeriesVal

from neptune.internal.operation import ClearImageLog, LogImages
from tests.neptune.variables.test_variable_base import TestVariableBase


@patch("time.time", new=TestVariableBase._now)
class TestStringSeries(TestVariableBase):

    def test_assign(self):
        value = ImageSeriesVal([self._random_image_array(), self._random_image_array()])
        expected = [
            LogImages.ValueType(value.values[0], None, self._now()),
            LogImages.ValueType(value.values[1], None, self._now())
        ]

        backend, processor = MagicMock(), MagicMock()
        exp, path, wait = self._create_experiment(backend, processor), self._random_path(), self._random_wait()
        var = ImageSeries(exp, path)
        var.assign(value, wait=wait)
        self.assertEqual(2, processor.enqueue_operation.call_count)
        processor.enqueue_operation.assert_has_calls([
            call(ClearImageLog(exp._uuid, path), False),
            call(LogImages(exp._uuid, path, expected), wait)
        ])

    def test_assign_empty(self):
        backend, processor = MagicMock(), MagicMock()
        exp, path, wait = self._create_experiment(backend, processor), self._random_path(), self._random_wait()
        var = ImageSeries(exp, path)
        var.assign(ImageSeriesVal([]), wait=wait)
        processor.enqueue_operation.assert_called_once_with(ClearImageLog(exp._uuid, path), wait)

    def test_assign_type_error(self):
        values = [[5.], ["text"], [], 55, "string", None]
        for value in values:
            with self.assertRaises(TypeError):
                ImageSeries(MagicMock(), MagicMock()).assign(value)

    def test_log(self):
        values = [self._random_image_array(), self._random_image_array()]
        value_and_expected = [
            (values[0], LogImages.ValueType(get_image_content(values[0]), None, self._now())),
            (values[1], LogImages.ValueType(get_image_content(values[1]), None, self._now()))
        ]

        for value, expected in value_and_expected:
            backend, processor = MagicMock(), MagicMock()
            exp, path, wait = self._create_experiment(backend, processor), self._random_path(), self._random_wait()
            var = ImageSeries(exp, path)
            var.log(value, wait=wait)
            processor.enqueue_operation.assert_called_once_with(LogImages(exp._uuid, path, [expected]), wait)

    def test_log_with_step(self):
        values = [self._random_image_array(), self._random_image_array()]
        value_and_expected = [
            (values[0], 5.3, LogImages.ValueType(get_image_content(values[0]), 5.3, self._now())),
            (values[1], 100, LogImages.ValueType(get_image_content(values[1]), 100, self._now()))
        ]

        for value, step, expected in value_and_expected:
            backend, processor = MagicMock(), MagicMock()
            exp, path, wait = self._create_experiment(backend, processor), self._random_path(), self._random_wait()
            var = ImageSeries(exp, path)
            var.log(value, step=step, wait=wait)
            processor.enqueue_operation.assert_called_once_with(LogImages(exp._uuid, path, [expected]), wait)

    def test_log_with_timestamp(self):
        values = [self._random_image_array(), self._random_image_array()]
        value_and_expected = [
            (values[0], 5.3, LogImages.ValueType(get_image_content(values[0]), None, 5.3)),
            (values[1], 100, LogImages.ValueType(get_image_content(values[1]), None, 100))
        ]

        for value, ts, expected in value_and_expected:
            backend, processor = MagicMock(), MagicMock()
            exp, path, wait = self._create_experiment(backend, processor), self._random_path(), self._random_wait()
            var = ImageSeries(exp, path)
            var.log(value, timestamp=ts, wait=wait)
            processor.enqueue_operation.assert_called_once_with(LogImages(exp._uuid, path, [expected]), wait)

    def test_clear(self):
        backend, processor = MagicMock(), MagicMock()
        exp, path, wait = self._create_experiment(backend, processor), self._random_path(), self._random_wait()
        var = ImageSeries(exp, path)
        var.clear(wait=wait)
        processor.enqueue_operation.assert_called_once_with(ClearImageLog(exp._uuid, path), wait)

    @staticmethod
    def _random_image_array(w=20, h=10):
        image_array = numpy.random.rand(w, h, 3) * 255
        return Image.fromarray(image_array.astype(numpy.uint8))
