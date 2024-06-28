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
    MagicMock,
    Mock,
    patch,
)

from neptune.constants import ASYNC_DIRECTORY
from neptune.core.components.abstract import WithResources
from neptune.core.operation_processors.async_operation_processor import AsyncOperationProcessor
from neptune.core.operation_processors.async_operation_processor.async_operation_processor import (
    _queue_has_enough_space,
)
from neptune.core.operation_processors.operation_processor import OperationProcessor
from neptune.core.typing.container_type import ContainerType
from neptune.core.typing.id_formats import (
    CustomId,
    UniqueId,
)
from neptune.exceptions import NeptuneSynchronizationAlreadyStoppedException
from neptune.internal.warnings import NeptuneWarning


@patch("neptune.core.operation_processors.async_operation_processor.processing_resources.MetadataFile", new=Mock)
class TestAsyncOperationProcessorInit(unittest.TestCase):
    def test_is_with_resources_and_operation_processor(self):
        # given
        processor = AsyncOperationProcessor(
            custom_id=CustomId("test_id"),
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
            custom_id=CustomId("test_id"),
            container_type=random.choice(list(ContainerType)),
            lock=threading.RLock(),
            signal_queue=Mock(),
        )

        # then
        assert processor.resources == processor.processing_resources.resources

    @patch(
        "neptune.core.operation_processors.async_operation_processor.processing_resources.get_container_full_path",
        return_value=Path("mock_path"),
    )
    # these patches prevent the side effect of creating test directories
    @patch("neptune.core.components.operation_storage.os.makedirs", new=lambda *_, **__: None)
    @patch("neptune.core.operation_processors.async_operation_processor.processing_resources.DiskQueue", new=Mock)
    def test_data_path(self, mock_get_container_full_path):
        # given
        test_cases = [
            {"data_path": Path("test_data_path"), "expected": Path("test_data_path")},
            {"data_path": None, "expected": Path("mock_path")},
        ]

        for test_case in test_cases:
            container_type = random.choice(list(ContainerType))
            processor = AsyncOperationProcessor(
                custom_id=CustomId("test_id"),
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


@patch("neptune.core.operation_processors.async_operation_processor.processing_resources.MetadataFile", new=Mock)
@patch("neptune.core.operation_processors.async_operation_processor.processing_resources.DiskQueue", new=Mock)
class TestAsyncOperationProcessorEnqueueOperation(unittest.TestCase):
    def test_check_queue_size(self):
        assert not _queue_has_enough_space(queue_size=1, batch_size=10)
        assert not _queue_has_enough_space(queue_size=5, batch_size=10)
        assert _queue_has_enough_space(queue_size=6, batch_size=10)

    def test_enqueue_operation_without_wait(self):
        # given
        processor = AsyncOperationProcessor(
            custom_id=CustomId("test_id"),
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

    def test_enqueue_operation_with_wait(self):
        # given
        processor = AsyncOperationProcessor(
            custom_id=CustomId("test_id"),
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
        processor.enqueue_operation(op, wait=True)

        # then
        processor.processing_resources.disk_queue.put.assert_called_once_with(op)
        mock_wait.assert_called_once()

    def test_enqueue_operation_not_accepting_operations_raises_warning_and_doesnt_put_to_queue(self):
        # given
        processor = AsyncOperationProcessor(
            custom_id=CustomId("test_id"),
            container_type=random.choice(list(ContainerType)),
            lock=threading.RLock(),
            signal_queue=Mock(),
        )

        processor.processing_resources.disk_queue.put = Mock(return_value=1)
        processor.processing_resources.disk_queue.size.return_value = 100

        # when
        processor._accepts_operations = False

        # then
        with self.assertWarnsRegex(NeptuneWarning, "Not accepting operations"):
            processor.enqueue_operation(Mock(), wait=False)
        processor.processing_resources.disk_queue.put.assert_not_called()


@patch("neptune.core.operation_processors.async_operation_processor.processing_resources.MetadataFile", new=Mock)
@patch("neptune.core.operation_processors.async_operation_processor.processing_resources.DiskQueue", new=Mock)
class TestAsyncOperationProcessorWait(unittest.TestCase):
    def test_async_operation_processor_wait(self):
        # given
        processor = AsyncOperationProcessor(
            custom_id=CustomId("test_id"),
            container_type=random.choice(list(ContainerType)),
            lock=threading.RLock(),
            signal_queue=Mock(),
        )

        processor._consumer = Mock()
        processor._consumer.is_running.return_value = True

        processor._processing_resources.waiting_cond = MagicMock()

        # when
        processor.wait()

        # then
        processor._consumer.wake_up.assert_called_once()
        processor._processing_resources.waiting_cond.wait_for.assert_called_once()

    def test_async_operation_processor_wait_consumer_not_running_raises_sync_stopped(self):
        # given
        processor = AsyncOperationProcessor(
            custom_id=CustomId("test_id"),
            container_type=random.choice(list(ContainerType)),
            lock=threading.RLock(),
            signal_queue=Mock(),
        )

        processor._consumer = Mock()
        processor._consumer.is_running.return_value = False

        processor._processing_resources.waiting_cond = MagicMock()

        # then
        with self.assertRaises(expected_exception=NeptuneSynchronizationAlreadyStoppedException):
            processor.wait()

        processor._consumer.wake_up.assert_called_once()


@patch("neptune.core.operation_processors.async_operation_processor.processing_resources.MetadataFile", new=Mock)
class TestAsyncOperationProcessorStartPauseResume(unittest.TestCase):
    def test_start(self):
        # given
        processor = AsyncOperationProcessor(
            custom_id=CustomId("test_id"),
            container_type=random.choice(list(ContainerType)),
            lock=threading.RLock(),
            signal_queue=Mock(),
        )

        processor._consumer = Mock()

        # when
        processor.start()

        # then
        processor._consumer.start.assert_called_once()

    def test_pause(self):
        # given
        processor = AsyncOperationProcessor(
            custom_id=CustomId("test_id"),
            container_type=random.choice(list(ContainerType)),
            lock=threading.RLock(),
            signal_queue=Mock(),
        )

        processor._consumer = Mock()
        processor.flush = Mock()

        # when
        processor.pause()

        # then
        processor._consumer.pause.assert_called_once()
        processor.flush.assert_called_once()

    def test_resume(self):
        # given
        processor = AsyncOperationProcessor(
            custom_id=CustomId("test_id"),
            container_type=random.choice(list(ContainerType)),
            lock=threading.RLock(),
            signal_queue=Mock(),
        )

        processor._consumer = Mock()

        # when
        processor.resume()

        # then
        processor._consumer.resume.assert_called_once()


@patch("neptune.core.operation_processors.async_operation_processor.processing_resources.MetadataFile", new=Mock)
class TestAsyncOperationProcessorStopAndClose(unittest.TestCase):
    def test_stop(self):
        # given
        processor = AsyncOperationProcessor(
            custom_id=CustomId("test_id"),
            container_type=random.choice(list(ContainerType)),
            lock=threading.RLock(),
            signal_queue=Mock(),
        )

        processor._consumer = Mock()
        processor._consumer.is_running.return_value = True

        processor.flush = Mock()
        processor.close = Mock()
        processor.cleanup = Mock()

        processor._queue_observer = Mock()
        processor._queue_observer.wait_for_queue_empty.return_value = True

        mock_signal_queue = Mock()

        # when
        processor.stop(seconds=10, processor_stop_signal_queue=mock_signal_queue)

        # then
        processor.flush.assert_called_once()

        processor._consumer.is_running.assert_called_once()
        processor._consumer.wake_up.assert_called_once()
        processor._consumer.interrupt.assert_called_once()
        processor._consumer.join.assert_called_once()

        processor._queue_observer.wait_for_queue_empty.assert_called_once_with(
            seconds=10,
            processor_stop_signal_queue=mock_signal_queue,
        )

        processor.close.assert_called_once()

        processor._queue_observer.is_queue_empty.assert_called_once()
        processor.cleanup.assert_called_once()

    def test_close(self):
        # given
        processor = AsyncOperationProcessor(
            custom_id=CustomId("test_id"),
            container_type=random.choice(list(ContainerType)),
            lock=threading.RLock(),
            signal_queue=Mock(),
        )

        processor._consumer = Mock()

        assert processor._accepts_operations

        # when
        processor.stop()

        # then
        assert not processor._accepts_operations
        processor._consumer.join.assert_called_once()

    def test_cleanup_triggers_processing_resources_cleanup(self):
        # given
        processor = AsyncOperationProcessor(
            custom_id=CustomId("test_id"),
            container_type=random.choice(list(ContainerType)),
            lock=threading.RLock(),
            signal_queue=Mock(),
        )

        mock_processing_resources = Mock(wraps=processor._processing_resources)

        processor._processing_resources = mock_processing_resources
        processor._processing_resources.data_path.rmdir = Mock()

        # when
        processor.cleanup()

        # then
        processor._processing_resources.cleanup.assert_called_once()
