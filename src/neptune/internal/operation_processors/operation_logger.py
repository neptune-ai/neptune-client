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
__all__ = [
    "CONNECTION_INTERRUPTED_MSG",
    "WAITING_FOR_OPERATIONS_MSG",
    "SUCCESS_MSG",
    "SYNC_FAILURE_MSG",
    "RECONNECT_FAILURE_MSG",
    "STILL_WAITING_MSG",
    "QueueSignal",
    "OperationLogger",
]

import logging
from dataclasses import dataclass
from queue import Queue
from typing import Optional

CONNECTION_INTERRUPTED_MSG = (
    "We have been experiencing connection interruptions during your run."
    " Neptune client will now try to resume connection and sync data for the next"
    " %s seconds."
    " You can also kill this process and synchronize your data manually later"
    " using `neptune sync` command."
)

WAITING_FOR_OPERATIONS_MSG = (
    "Waiting for the remaining %s operations to synchronize with Neptune." " Do not kill this process."
)

SUCCESS_MSG = "All %s operations synced, thanks for waiting!"

SYNC_FAILURE_MSG = (
    "Failed to sync all operations in %s seconds."
    " You have %s operations saved on disk that can be manually synced"
    " using `neptune sync` command."
)

RECONNECT_FAILURE_MSG = (
    "Failed to reconnect with Neptune in %s seconds."
    " You have %s operations saved on disk that can be manually synced"
    " using `neptune sync` command."
)

STILL_WAITING_MSG = "Still waiting for the remaining %s operations" " (%.2f%% done). Please wait."


@dataclass
class QueueSignal:
    size_remaining: int = 0
    already_synced_proc: float = 0.0
    should_block_logging: bool = False


class OperationLogger:
    def __init__(self, signal_queue: Optional["Queue[QueueSignal]"], logger: logging.Logger) -> None:
        self._signal_queue = signal_queue
        self._logger = logger

    def log_connection_interruption(self, max_reconnect_wait_time: float) -> None:
        if self._signal_queue is not None:
            self._signal_queue.put(QueueSignal(should_block_logging=True))
        else:
            self._logger.warning(
                CONNECTION_INTERRUPTED_MSG,
                max_reconnect_wait_time,
            )

    def log_remaining_operations(self, size_remaining: int) -> None:
        if self._signal_queue is not None:
            self._signal_queue.put(QueueSignal(size_remaining=size_remaining))
        else:
            if size_remaining:
                self._logger.warning(
                    WAITING_FOR_OPERATIONS_MSG,
                    size_remaining,
                )

    def log_success(self, ops_synced: int) -> None:
        self._logger.info(SUCCESS_MSG, ops_synced)

    def log_sync_failure(self, seconds: float, size_remaining: int) -> None:
        self._logger.warning(
            SYNC_FAILURE_MSG,
            seconds,
            size_remaining,
        )

    def log_reconnect_failure(self, max_reconnect_wait_time: float, size_remaining: int) -> None:
        self._logger.warning(
            RECONNECT_FAILURE_MSG,
            max_reconnect_wait_time,
            size_remaining,
        )

    def log_still_waiting(self, size_remaining: int, already_synced_proc: float) -> None:
        if self._signal_queue is not None:
            self._signal_queue.put(QueueSignal(size_remaining=size_remaining, already_synced_proc=already_synced_proc))
        else:
            self._logger.warning(
                STILL_WAITING_MSG,
                size_remaining,
                already_synced_proc,
            )
