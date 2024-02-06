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
from unittest.mock import Mock

from neptune.internal.operation_processors.operation_logger import (
    CONNECTION_INTERRUPTED_MSG,
    RECONNECT_FAILURE_MSG,
    STILL_WAITING_MSG,
    SUCCESS_MSG,
    SYNC_FAILURE_MSG,
    WAITING_FOR_OPERATIONS_MSG,
    ProcessorStopLogger,
    ProcessorStopSignal,
    ProcessorStopSignalData,
    ProcessorStopSignalType,
)


class TestOperationLoggerNoQueue(unittest.TestCase):
    def setUp(self):
        self.logger = ProcessorStopLogger(processor_id=0, signal_queue=None, logger=Mock())

    def test_log_connection_interruption(self):
        self.logger.log_connection_interruption(10)

        self.logger._logger.warning.assert_called_once_with(CONNECTION_INTERRUPTED_MSG, 10)

    def test_log_remaining_operations(self):
        self.logger.log_remaining_operations(10)

        self.logger._logger.info.assert_called_once_with(WAITING_FOR_OPERATIONS_MSG, 10)

        self.logger._logger.warning.reset_mock()

        self.logger.log_remaining_operations(0)

        self.logger._logger.warning.assert_not_called()

    def test_log_success(self):
        self.logger.log_success(10)

        self.logger._logger.info.assert_called_once_with(SUCCESS_MSG, 10)

    def test_log_sync_failure(self):
        self.logger.log_sync_failure(10, 20)

        self.logger._logger.warning.assert_called_once_with(SYNC_FAILURE_MSG, 10, 20)

    def test_log_reconnect_failure(self):
        self.logger.log_reconnect_failure(10, 20)

        self.logger._logger.warning.assert_called_once_with(RECONNECT_FAILURE_MSG, 10, 20)

    def test_log_still_waiting(self):
        self.logger.log_still_waiting(10, 10, 20)

        self.logger._logger.info.assert_called_once_with(STILL_WAITING_MSG, 10, 20)


class TestOperationLoggerWithQueue(unittest.TestCase):
    def setUp(self):
        self.logger = ProcessorStopLogger(processor_id=0, signal_queue=Mock(), logger=Mock())

    def test_log_connection_interruption(self):
        self.logger.log_connection_interruption(10)

        self.logger._logger.warning.assert_not_called()
        self.logger._signal_queue.put.assert_called_once_with(
            ProcessorStopSignal(
                data=ProcessorStopSignalData(max_reconnect_wait_time=10),
                signal_type=ProcessorStopSignalType.CONNECTION_INTERRUPTED,
            )
        )

    def test_log_remaining_operations(self):
        self.logger.log_remaining_operations(10)

        self.logger._logger.warning.assert_not_called()
        self.logger._signal_queue.put.assert_called_once_with(
            ProcessorStopSignal(
                data=ProcessorStopSignalData(size_remaining=10),
                signal_type=ProcessorStopSignalType.WAITING_FOR_OPERATIONS,
            )
        )

    def test_log_success(self):
        self.logger.log_success(10)

        self.logger._logger.info.assert_not_called()

    def test_log_sync_failure(self):
        self.logger.log_sync_failure(10, 20)

        self.logger._logger.warning.assert_not_called()

    def test_log_reconnect_failure(self):
        self.logger.log_reconnect_failure(10, 20)

        self.logger._logger.warning.assert_not_called()

    def test_log_still_waiting(self):
        self.logger.log_still_waiting(10, 10, 20)

        self.logger._logger.warning.assert_not_called()
        self.logger._signal_queue.put.assert_called_once_with(
            ProcessorStopSignal(
                data=ProcessorStopSignalData(size_remaining=10, already_synced=10, already_synced_proc=20),
                signal_type=ProcessorStopSignalType.STILL_WAITING,
            ),
        )
