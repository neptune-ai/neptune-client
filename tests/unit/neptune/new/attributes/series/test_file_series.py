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
import io
from unittest import mock

import numpy
import pytest
from mock import (
    MagicMock,
    call,
    patch,
)

from neptune.attributes.series.file_series import FileSeries
from neptune.exceptions import OperationNotSupported
from neptune.internal.operation import (
    ClearImageLog,
    ImageValue,
    LogImages,
)
from neptune.internal.utils import base64_encode
from neptune.types import File
from tests.unit.neptune.new.attributes.test_attribute_base import TestAttributeBase
from tests.unit.neptune.new.utils.file_helpers import create_file


@patch("time.time", new=TestAttributeBase._now)
class TestFileSeries(TestAttributeBase):
    def test_assign_type_error(self):
        values = [[5.0], ["text"], 55, "string", None]
        for value in values:
            with self.assertRaises(Exception):
                FileSeries(MagicMock(), MagicMock()).assign(value)

    def test_log_type_error(self):
        values = [[5.0], [[]], 55, None]
        for value in values:
            with self.assertRaises(TypeError):
                FileSeries(MagicMock(), MagicMock()).log(value)

    @patch("neptune.metadata_containers.metadata_container.get_operation_processor")
    def test_log_content(self, get_operation_processor):
        # given
        wait = self._random_wait()
        path = self._random_path()
        processor = MagicMock()
        get_operation_processor.return_value = processor

        with self._exp() as exp:
            attr = FileSeries(exp, path)

            file = File.as_image(numpy.random.rand(10, 10) * 255)

            # when
            attr.log(
                file,
                step=3,
                timestamp=self._now(),
                wait=wait,
                name="nazwa",
                description="opis",
            )

            # then
            processor.enqueue_operation.assert_called_with(
                LogImages(
                    path,
                    [
                        LogImages.ValueType(
                            ImageValue(base64_encode(file.content), "nazwa", "opis"),
                            3,
                            self._now(),
                        )
                    ],
                ),
                wait=wait,
            )

    @patch("neptune.metadata_containers.metadata_container.get_operation_processor")
    def test_assign_content(self, get_operation_processor):
        # given
        wait = self._random_wait()
        path = self._random_path()
        processor = MagicMock()
        get_operation_processor.return_value = processor

        with self._exp() as exp:
            attr = FileSeries(exp, path)

            file = File.as_image(numpy.random.rand(10, 10) * 255)

            # when
            attr.assign([file], wait=wait)

            # then
            processor.enqueue_operation.assert_has_calls(
                [
                    call(ClearImageLog(path), wait=False),
                    call(
                        LogImages(
                            path,
                            [
                                LogImages.ValueType(
                                    ImageValue(base64_encode(file.content), None, None),
                                    None,
                                    self._now(),
                                )
                            ],
                        ),
                        wait=wait,
                    ),
                ]
            )

    @patch("neptune.metadata_containers.metadata_container.get_operation_processor")
    def test_log_path(self, get_operation_processor):
        # given
        wait = self._random_wait()
        path = self._random_path()
        processor = MagicMock()
        get_operation_processor.return_value = processor

        with self._exp() as exp:
            attr = FileSeries(exp, path)

            file = File.as_image(numpy.random.rand(10, 10) * 255)
            stream = File.from_stream(io.BytesIO(file.content))
            with create_file(file.content, binary_mode=True) as tmp_filename:
                saved_file = File(tmp_filename)

                # when
                attr.log(
                    file,
                    step=3,
                    timestamp=self._now(),
                    wait=wait,
                    description="something",
                )
                attr.log(
                    [stream, saved_file],
                    timestamp=self._now(),
                    wait=wait,
                    description="something",
                )

                # then
                def generate_expected_call(wait, step):
                    log_operation = LogImages(
                        path=path,
                        values=[
                            LogImages.ValueType(
                                value=ImageValue(base64_encode(file.content), None, "something"),
                                step=step,
                                ts=self._now(),
                            )
                        ],
                    )
                    return call(
                        log_operation,
                        wait=wait,
                    )

                processor.enqueue_operation.assert_has_calls(
                    [
                        generate_expected_call(wait, step=3),
                        generate_expected_call(wait, step=None),
                        generate_expected_call(wait, step=None),
                    ]
                )

    def test_log_raise_not_image(self):
        # given
        path = self._random_path()

        with self._exp() as exp:
            attr = FileSeries(exp, path)

            file = File.from_content("some text")
            stream = File.from_stream(io.BytesIO(file.content))
            with create_file(file.content, binary_mode=True) as tmp_filename:
                saved_file = File(tmp_filename)

                # when
                with self.assertRaises(OperationNotSupported):
                    attr.log(file)
                with self.assertRaises(OperationNotSupported):
                    attr.log(saved_file)
                with self.assertRaises(OperationNotSupported):
                    attr.log(stream)

    def test_assign_raise_not_image(self):
        # given
        path = self._random_path()

        with self._exp() as exp:
            attr = FileSeries(exp, path)

            file = File.from_content("some text")
            stream = File.from_stream(io.BytesIO(file.content))
            with create_file(file.content, binary_mode=True) as tmp_filename:
                saved_file = File(tmp_filename)

                # when
                with self.assertRaises(OperationNotSupported):
                    attr.assign([file])
                with self.assertRaises(OperationNotSupported):
                    attr.assign([saved_file])
                with self.assertRaises(OperationNotSupported):
                    attr.assign([stream])

    @mock.patch("neptune.internal.utils.limits._LOGGED_IMAGE_SIZE_LIMIT_MB", (10**-3))
    def test_image_limit(self):
        """Test if we prohibit logging images greater than mocked 1KB limit size"""
        # given
        path = self._random_path()

        with self._exp() as exp:
            attr = FileSeries(exp, path)

            file = File.as_image(numpy.random.rand(100, 100) * 255)
            with create_file(file.content, binary_mode=True) as tmp_filename:
                saved_file = File(tmp_filename)

                # when
                with pytest.warns(
                    expected_warning=UserWarning, match=".* Neptune supports logging images smaller than .*"
                ):
                    attr.assign([file])
                with pytest.warns(
                    expected_warning=UserWarning, match=".* Neptune supports logging images smaller than .*"
                ):
                    attr.assign([saved_file])
