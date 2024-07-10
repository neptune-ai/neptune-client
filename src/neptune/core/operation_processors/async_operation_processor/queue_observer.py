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
from __future__ import annotations

__all__ = ["QueueObserver"]

from dataclasses import dataclass
from queue import Queue
from time import monotonic
from typing import (
    Optional,
    Union,
)

from neptune.core.components.queue.aggregating_disk_queue import AggregatingDiskQueue
from neptune.core.components.queue.disk_queue import DiskQueue
from neptune.core.operation_processors.async_operation_processor.consumer_thread import ConsumerThread
from neptune.core.operation_processors.async_operation_processor.operation_logger import (
    ProcessorStopLogger,
    ProcessorStopSignal,
)
from neptune.exceptions import NeptuneSynchronizationAlreadyStoppedException
from neptune.internal.utils.logger import get_logger

logger = get_logger()


@dataclass
class QueueWaitCycleResults:
    size_remaining: int
    already_synced: int
    already_synced_proc: float


class QueueObserver:
    def __init__(
        self,
        disk_queue: Union[DiskQueue, AggregatingDiskQueue],
        consumer: ConsumerThread,
        should_print_logs: bool,
        stop_queue_max_time_no_connection_seconds: float,
    ):
        self._disk_queue = disk_queue
        self._consumer = consumer
        self._stop_queue_max_time_no_connection_seconds = stop_queue_max_time_no_connection_seconds

        self._processor_stop_logger = ProcessorStopLogger(
            processor_id=id(self),
            signal_queue=None,
            logger=logger,
            should_print_logs=should_print_logs,
        )

    def is_queue_empty(self) -> bool:
        return self._disk_queue.is_empty()

    def wait_for_queue_empty(
        self,
        seconds: Optional[float],
        processor_stop_signal_queue: Optional[Queue[ProcessorStopSignal]] = None,
    ) -> None:
        initial_queue_size = self._disk_queue.size()
        waiting_start: float = monotonic()
        time_elapsed: float = 0.0
        max_reconnect_wait_time: float = self._stop_queue_max_time_no_connection_seconds if seconds is None else seconds

        self._processor_stop_logger.set_processor_stop_signal_queue(processor_stop_signal_queue)

        if initial_queue_size > 0:
            if self._consumer.last_backoff_time > 0:
                self._processor_stop_logger.log_connection_interruption(max_reconnect_wait_time)
            else:
                self._processor_stop_logger.log_remaining_operations(size_remaining=initial_queue_size)

        while True:
            wait_cycle_results = self._wait_single_cycle(
                seconds,
                self._processor_stop_logger,
                initial_queue_size,
                waiting_start,
                time_elapsed,
                max_reconnect_wait_time,
            )
            if wait_cycle_results is None:
                # either there are no more operations to process or there is a synchronization failure
                return
            self._processor_stop_logger.log_still_waiting(
                size_remaining=wait_cycle_results.size_remaining,
                already_synced=wait_cycle_results.already_synced,
                already_synced_proc=wait_cycle_results.already_synced_proc,
            )

    def _wait_single_cycle(
        self,
        seconds: Optional[float],
        op_logger: ProcessorStopLogger,
        initial_queue_size: int,
        waiting_start: float,
        time_elapsed: float,
        max_reconnect_wait_time: float,
    ) -> Optional[QueueWaitCycleResults]:
        if seconds is None:
            if self._consumer.last_backoff_time == 0:
                # reset `waiting_start` on successful action
                waiting_start = monotonic()
            wait_time = self._stop_queue_max_time_no_connection_seconds
        else:
            wait_time = max(
                min(
                    seconds - time_elapsed,
                    self._stop_queue_max_time_no_connection_seconds,
                ),
                0.0,
            )
        self._disk_queue.wait_for_empty(wait_time)

        cycle_results = _calculate_wait_cycle_results(self._disk_queue, initial_queue_size)

        if cycle_results.size_remaining == 0:
            op_logger.log_success(ops_synced=initial_queue_size)
            return None

        time_elapsed = monotonic() - waiting_start
        if self._consumer.last_backoff_time > 0 and time_elapsed >= max_reconnect_wait_time:
            op_logger.log_reconnect_failure(
                max_reconnect_wait_time=max_reconnect_wait_time,
                size_remaining=cycle_results.size_remaining,
            )
            return None

        if seconds is not None and wait_time == 0:
            op_logger.log_sync_failure(seconds=seconds, size_remaining=cycle_results.size_remaining)
            return None

        if not self._consumer.is_running():
            exception = NeptuneSynchronizationAlreadyStoppedException()
            logger.warning(str(exception))
            return None

        return cycle_results


def _calculate_wait_cycle_results(
    disk_queue: Union[DiskQueue, AggregatingDiskQueue], initial_queue_size: int
) -> QueueWaitCycleResults:
    size_remaining = disk_queue.size()
    already_synced = initial_queue_size - size_remaining
    already_synced_proc = (already_synced / initial_queue_size) * 100 if initial_queue_size else 100

    return QueueWaitCycleResults(size_remaining, already_synced, already_synced_proc)
