#
# Copyright (c) 2024, Neptune Labs Sp. z o.o.
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
import threading
import unittest
from pathlib import Path
from unittest.mock import (
    Mock,
    patch,
)

from neptune.constants import ASYNC_DIRECTORY
from neptune.core.components.abstract import WithResources
from neptune.core.operation_processors.async_operation_processor import (
    AsyncOperationProcessor,
    _queue_has_enough_space,
)
from neptune.core.operation_processors.operation_processor import OperationProcessor
from neptune.core.typing.container_type import ContainerType
from neptune.core.typing.id_formats import UniqueId
from neptune.internal.warnings import NeptuneWarning


@patch("neptune.core.operation_processors.async_operation_processor.MetadataFile", new=Mock)
class TestAsyncOperationProcessorInit(unittest.TestCase):
    def test_is_with_resources_and_operation_processor(self):
        # given
        processor = AsyncOperationProcessor(
            container_id=UniqueId("test_id"),
            container_type=random.choice(list(ContainerType)),
            lock=threading.RLock(),
            signal_queue=Mock(),
        )

        # then
        assert isinstance(processor, WithResources)
        assert isinstance(processor, OperationProcessor)

    def test_resources(self):
        # given
        processor = AsyncOperationProcessor(
            container_id=UniqueId("test_id"),
            container_type=random.choice(list(ContainerType)),
            lock=threading.RLock(),
            signal_queue=Mock(),
        )

        # then
        assert processor.resources == processor.processing_resources.resources

    @patch(
        "neptune.core.operation_processors.async_operation_processor.get_container_full_path",
        return_value=Path("mock_path"),
    )
    # these patches prevent the side effect of creating test directories
    @patch("neptune.core.components.operation_storage.os.makedirs", new=lambda *_, **__: None)
    @patch("neptune.core.operation_processors.async_operation_processor.DiskQueue", new=Mock)
    def test_data_path(self, mock_get_container_full_path):
        # given
        test_cases = [
            {"data_path": Path("test_data_path"), "expected": Path("test_data_path")},
            {"data_path": None, "expected": Path("mock_path")},
        ]

        for test_case in test_cases:
            container_type = random.choice(list(ContainerType))
            processor = AsyncOperationProcessor(
                container_id=UniqueId("test_id"),
                container_type=container_type,
                lock=threading.RLock(),
                signal_queue=Mock(),
                data_path=test_case["data_path"],
            )
            # then
            assert processor.data_path == test_case["expected"]

            if test_case["data_path"] is None:
                mock_get_container_full_path.assert_called_once_with(
                    ASYNC_DIRECTORY,
                    UniqueId("test_id"),
                    container_type,
                )


@patch("neptune.core.operation_processors.async_operation_processor.MetadataFile", new=Mock)
class TestAsyncOperationProcessorEnqueueOperation(unittest.TestCase):
    def test_check_queue_size(self):
        assert not _queue_has_enough_space(queue_size=1, batch_size=10)
        assert not _queue_has_enough_space(queue_size=5, batch_size=10)
        assert _queue_has_enough_space(queue_size=6, batch_size=10)

    @patch("neptune.core.operation_processors.async_operation_processor.DiskQueue", new=Mock)
    def test_enqueue_operation(self):
        # given
        processor = AsyncOperationProcessor(
            container_id=UniqueId("test_id"),
            container_type=random.choice(list(ContainerType)),
            lock=threading.RLock(),
            signal_queue=Mock(),
        )

        processor.processing_resources.disk_queue.put = Mock(return_value=1)
        processor.processing_resources.disk_queue.size.return_value = 100

        op = Mock()
        mock_wait = Mock()
        processor.wait = mock_wait

        # when
        processor.enqueue_operation(op, wait=False)

        # then
        processor.processing_resources.disk_queue.put.assert_called_once_with(op)
        mock_wait.assert_not_called()

        # when
        processor.enqueue_operation(op, wait=True)

        # then
        processor.processing_resources.disk_queue.put.assert_called_with(op)
        mock_wait.assert_called_once()

        # when
        processor._accepts_operations = False

        # then
        with self.assertWarnsRegex(NeptuneWarning, "Not accepting operations"):
            processor.enqueue_operation(op, wait=False)
        processor.processing_resources.disk_queue.put.assert_called_with(op)
