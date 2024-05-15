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

import unittest
from unittest.mock import (
    Mock,
    patch,
)

from neptune.core.operation_processors.async_operation_processor.constants import (
    STOP_QUEUE_MAX_TIME_NO_CONNECTION_SECONDS,
)
from neptune.core.operation_processors.async_operation_processor.queue_observer import (
    QueueObserver,
    QueueWaitCycleResults,
)


class TestQueueObserver(unittest.TestCase):
    def test_wait_for_queue_empty_no_backoff_time(self):
        # given
        queue_observer = QueueObserver(
            disk_queue=Mock(),
            consumer=Mock(),
            should_print_logs=True,
            stop_queue_max_time_no_connection_seconds=STOP_QUEUE_MAX_TIME_NO_CONNECTION_SECONDS,
        )

        queue_observer._disk_queue.size.return_value = 10
        queue_observer._consumer.last_backoff_time = 0
        queue_observer._processor_stop_logger = Mock()
        queue_observer._wait_single_cycle = Mock(
            side_effect=[
                QueueWaitCycleResults(
                    size_remaining=100,
                    already_synced=70,
                    already_synced_proc=70,
                ),
                None,
            ]
        )

        signal_queue = Mock()

        # when
        queue_observer.wait_for_queue_empty(seconds=30, processor_stop_signal_queue=signal_queue)

        # then
        queue_observer._processor_stop_logger.log_remaining_operations.assert_called_once_with(size_remaining=10)
        queue_observer._processor_stop_logger.log_connection_interruption.assert_not_called()

        assert queue_observer._wait_single_cycle.call_count == 2
        queue_observer._processor_stop_logger.log_still_waiting.assert_called_once_with(
            size_remaining=100,
            already_synced=70,
            already_synced_proc=70,
        )
        queue_observer._processor_stop_logger.set_processor_stop_signal_queue.assert_called_once_with(signal_queue)

    def test_wait_for_queue_empty_with_backoff_time_logs_connection_interruption(self):
        # given
        queue_observer = QueueObserver(
            disk_queue=Mock(),
            consumer=Mock(),
            should_print_logs=True,
            stop_queue_max_time_no_connection_seconds=STOP_QUEUE_MAX_TIME_NO_CONNECTION_SECONDS,
        )

        queue_observer._disk_queue.size.return_value = 10
        queue_observer._consumer.last_backoff_time = 10
        queue_observer._wait_single_cycle = Mock(return_value=None)
        queue_observer._processor_stop_logger = Mock()

        # when
        queue_observer.wait_for_queue_empty(seconds=30, processor_stop_signal_queue=Mock())

        # then
        queue_observer._processor_stop_logger.log_connection_interruption.assert_called_once_with(30)

    def test_wait_single_cycle_reconnect_failure_triggers_logging(self):
        # given
        queue_observer = QueueObserver(
            disk_queue=Mock(),
            consumer=Mock(),
            should_print_logs=True,
            stop_queue_max_time_no_connection_seconds=STOP_QUEUE_MAX_TIME_NO_CONNECTION_SECONDS,
        )

        queue_observer._consumer.last_backoff_time = 10
        queue_observer._disk_queue.size.return_value = 10
        op_logger = Mock()

        # when
        queue_observer._wait_single_cycle(
            seconds=30,
            op_logger=op_logger,
            initial_queue_size=10,
            waiting_start=0,
            time_elapsed=0,
            max_reconnect_wait_time=0,
        )

        # then
        op_logger.log_reconnect_failure.assert_called_once_with(max_reconnect_wait_time=0, size_remaining=10)

    def test_wait_single_cycle_sync_failure_triggers_logging(self):
        # given
        queue_observer = QueueObserver(
            disk_queue=Mock(),
            consumer=Mock(),
            should_print_logs=True,
            stop_queue_max_time_no_connection_seconds=0,
        )

        queue_observer._consumer.last_backoff_time = 0
        queue_observer._disk_queue.size.return_value = 10
        op_logger = Mock()

        # when
        queue_observer._wait_single_cycle(
            seconds=30,
            op_logger=op_logger,
            initial_queue_size=10,
            waiting_start=0,
            time_elapsed=0,
            max_reconnect_wait_time=0,
        )

        # then
        op_logger.log_sync_failure.assert_called_once_with(seconds=30, size_remaining=10)

    @patch(
        "neptune.core.operation_processors.async_operation_processor.queue_observer"
        ".NeptuneSynchronizationAlreadyStoppedException"
    )
    def test_wait_single_cycle_sync_already_stopped(self, synchronization_stopped_exception):
        # given
        queue_observer = QueueObserver(
            disk_queue=Mock(),
            consumer=Mock(),
            should_print_logs=True,
            stop_queue_max_time_no_connection_seconds=STOP_QUEUE_MAX_TIME_NO_CONNECTION_SECONDS,
        )

        queue_observer._consumer.last_backoff_time = 0
        queue_observer._disk_queue.size.return_value = 10
        queue_observer._consumer.is_running.return_value = False

        # when
        queue_observer._wait_single_cycle(
            seconds=None,
            op_logger=Mock(),
            initial_queue_size=10,
            waiting_start=0,
            time_elapsed=0,
            max_reconnect_wait_time=0,
        )

        # then
        synchronization_stopped_exception.assert_called_once()

    @patch(
        "neptune.core.operation_processors.async_operation_processor.queue_observer._calculate_wait_cycle_results",
        new=Mock(return_value=QueueWaitCycleResults(200, 30, 15)),
    )
    def test_wait_single_cycle_returns_cycle_result(self):
        # given
        queue_observer = QueueObserver(
            disk_queue=Mock(),
            consumer=Mock(),
            should_print_logs=True,
            stop_queue_max_time_no_connection_seconds=STOP_QUEUE_MAX_TIME_NO_CONNECTION_SECONDS,
        )

        queue_observer._consumer.last_backoff_time = 0
        queue_observer._disk_queue.size.return_value = 10
        queue_observer._consumer.is_running.return_value = True

        # when
        result = queue_observer._wait_single_cycle(
            seconds=None,
            op_logger=Mock(),
            initial_queue_size=10,
            waiting_start=0,
            time_elapsed=0,
            max_reconnect_wait_time=0,
        )

        # then
        assert result == QueueWaitCycleResults(200, 30, 15)
