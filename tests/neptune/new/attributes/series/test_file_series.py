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
from tempfile import NamedTemporaryFile

import numpy
from mock import patch, MagicMock, call
from neptune.new.exceptions import OperationNotSupported

from neptune.new.internal.utils import base64_encode

from neptune.new.internal.operation import ImageValue, LogImages, ClearImageLog

from neptune.new.types import File

from neptune.new.attributes.series.file_series import FileSeries

from tests.neptune.new.attributes.test_attribute_base import TestAttributeBase


@patch("time.time", new=TestAttributeBase._now)
class TestFileSeries(TestAttributeBase):

    def test_assign_type_error(self):
        values = [[5.], ["text"], 55, "string", None]
        for value in values:
            with self.assertRaises(Exception):
                FileSeries(MagicMock(), MagicMock()).assign(value)

    def test_log_type_error(self):
        values = [[5.], [[]], 55, None]
        for value in values:
            with self.assertRaises(TypeError):
                FileSeries(MagicMock(), MagicMock()).log(value)

    def test_log_content(self):
        # given
        wait = self._random_wait()
        path = self._random_path()
        op_processor = MagicMock()
        exp = self._create_run(processor=op_processor)
        attr = FileSeries(exp, path)

        file = File.as_image(numpy.random.rand(10, 10) * 255)

        # when
        attr.log(file, step=3, timestamp=self._now(), wait=wait, name="nazwa", description="opis")

        # then
        op_processor.enqueue_operation.assert_called_once_with(
            LogImages(path, [LogImages.ValueType(
                ImageValue(base64_encode(file.content), "nazwa", "opis"),
                3,
                self._now())]), wait
        )

    def test_assign_content(self):
        # given
        wait = self._random_wait()
        path = self._random_path()
        op_processor = MagicMock()
        exp = self._create_run(processor=op_processor)
        attr = FileSeries(exp, path)

        file = File.as_image(numpy.random.rand(10, 10) * 255)

        # when
        attr.assign([file], wait=wait)

        # then
        op_processor.enqueue_operation.assert_has_calls([
            call(ClearImageLog(path), False),
            call(LogImages(path, [LogImages.ValueType(
                ImageValue(base64_encode(file.content), None, None),
                None,
                self._now())]), wait),
        ])

    def test_log_path(self):
        # given
        wait = self._random_wait()
        path = self._random_path()
        op_processor = MagicMock()
        exp = self._create_run(processor=op_processor)
        attr = FileSeries(exp, path)

        file = File.as_image(numpy.random.rand(10, 10) * 255)
        with self._create_image_file(file.content) as tmp_file:
            saved_file = File(tmp_file.name)

            # when
            attr.log(saved_file, step=3, timestamp=self._now(), wait=wait, description="something")

            # then
            op_processor.enqueue_operation.assert_called_once_with(
                LogImages(path, [LogImages.ValueType(
                    ImageValue(base64_encode(file.content), None, "something"),
                    3,
                    self._now())]), wait
            )

    def test_log_raise_not_image(self):
        # given
        path = self._random_path()
        op_processor = MagicMock()
        exp = self._create_run(processor=op_processor)
        attr = FileSeries(exp, path)

        file = File.from_content("some text")
        with self._create_image_file(file.content) as tmp_file:
            saved_file = File(tmp_file.name)

            # when
            with self.assertRaises(OperationNotSupported):
                attr.log(file)
            with self.assertRaises(OperationNotSupported):
                attr.log(saved_file)

    def test_assign_raise_not_image(self):
        # given
        path = self._random_path()
        op_processor = MagicMock()
        exp = self._create_run(processor=op_processor)
        attr = FileSeries(exp, path)

        file = File.from_content("some text")
        with self._create_image_file(file.content) as tmp_file:
            saved_file = File(tmp_file.name)

            # when
            with self.assertRaises(OperationNotSupported):
                attr.assign([file])
            with self.assertRaises(OperationNotSupported):
                attr.assign([saved_file])

    @staticmethod
    def _create_image_file(content):
        file = NamedTemporaryFile("wb")
        file.write(content)
        file.flush()
        return file
