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

import os
import threading
from dataclasses import dataclass
from pathlib import Path
from queue import Queue
from time import (
    monotonic,
    time,
)
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
)

from neptune.constants import ASYNC_DIRECTORY
from neptune.core.components.abstract import (
    Resource,
    WithResources,
)
from neptune.core.components.metadata_file import MetadataFile
from neptune.core.components.operation_storage import OperationStorage
from neptune.core.components.queue.disk_queue import DiskQueue
from neptune.core.operation_processors.operation_processor import OperationProcessor
from neptune.core.operation_processors.utils import (
    common_metadata,
    get_container_full_path,
)
from neptune.core.operations.operation import Operation
from neptune.core.typing.container_type import ContainerType
from neptune.core.typing.id_formats import UniqueId
from neptune.envs import NEPTUNE_SYNC_AFTER_STOP_TIMEOUT
from neptune.exceptions import NeptuneSynchronizationAlreadyStoppedException
from neptune.internal.init.parameters import DEFAULT_STOP_TIMEOUT
from neptune.internal.operation_processors.operation_logger import (
    ProcessorStopLogger,
    ProcessorStopSignal,
)
from neptune.internal.signals_processing.signals import Signal
from neptune.internal.signals_processing.utils import (
    signal_batch_lag,
    signal_batch_processed,
    signal_batch_started,
)
from neptune.internal.threading.daemon import Daemon
from neptune.internal.utils.disk_utilization import ensure_disk_not_overutilize
from neptune.internal.utils.logger import get_logger
from neptune.internal.warnings import (
    NeptuneWarning,
    warn_once,
)

logger = get_logger()


@dataclass(slots=True)
class QueueWaitCycleResults:
    size_remaining: int
    already_synced: int
    already_synced_proc: float


class ProcessingResources(WithResources):
    def __init__(
        self,
        container_id: UniqueId,
        container_type: ContainerType,
        lock: threading.RLock,
        signal_queue: "Queue[Signal]",
        batch_size: int = 1,
        data_path: Optional[Path] = None,
        serializer: Callable[[Operation], Dict[str, Any]] = lambda op: op.to_dict(),
    ) -> None:
        self.batch_size: int = batch_size
        self._data_path = (
            data_path if data_path else get_container_full_path(ASYNC_DIRECTORY, container_id, container_type)
        )

        self.metadata_file = MetadataFile(
            data_path=self._data_path,
            metadata=common_metadata(mode="async", container_id=container_id, container_type=container_type),
        )
        self.operation_storage = OperationStorage(data_path=self._data_path)
        self.disk_queue = DiskQueue(
            data_path=self._data_path,
            to_dict=serializer,
            from_dict=Operation.from_dict,
            lock=lock,
        )

        self.waiting_cond = threading.Condition()

        self.signals_queue = signal_queue

        self.consumed_version: int = 0

    @property
    def resources(self) -> Tuple[Resource, ...]:
        return self.metadata_file, self.operation_storage, self.disk_queue

    @property
    def data_path(self) -> Path:
        return self._data_path


class AsyncOperationProcessor(WithResources, OperationProcessor):
    STOP_QUEUE_STATUS_UPDATE_FREQ_SECONDS = 30.0
    STOP_QUEUE_MAX_TIME_NO_CONNECTION_SECONDS = float(os.getenv(NEPTUNE_SYNC_AFTER_STOP_TIMEOUT, DEFAULT_STOP_TIMEOUT))

    def __init__(
        self,
        container_id: UniqueId,
        container_type: ContainerType,
        lock: threading.RLock,
        signal_queue: "Queue[Signal]",
        batch_size: int = 1,
        data_path: Optional[Path] = None,
        serializer: Callable[[Operation], Dict[str, Any]] = lambda op: op.to_dict(),
        should_print_logs: bool = True,
    ) -> None:
        self._should_print_logs = should_print_logs
        self._accepts_operations: bool = True
        self._last_version: int = 0

        self._processing_resources = ProcessingResources(
            batch_size=batch_size,
            container_id=container_id,
            container_type=container_type,
            lock=lock,
            signal_queue=signal_queue,
            data_path=data_path,
            serializer=serializer,
        )

        self._consumer = ConsumerThread(
            sleep_time=5,
            processing_resources=self._processing_resources,
        )

        self._queue_observer = QueueObserver(
            disk_queue=self._processing_resources.disk_queue,
            consumer=self._consumer,
            should_print_logs=self._should_print_logs,
            stop_queue_max_time_no_connection_seconds=self.STOP_QUEUE_MAX_TIME_NO_CONNECTION_SECONDS,
        )

    @property
    def resources(self) -> Tuple[Resource, ...]:
        return self._processing_resources.resources

    @property
    def data_path(self) -> Path:
        return self._processing_resources.data_path

    @property
    def processing_resources(self) -> ProcessingResources:
        return self._processing_resources

    @ensure_disk_not_overutilize
    def enqueue_operation(self, op: Operation, *, wait: bool) -> None:
        if not self._accepts_operations:
            warn_once("Not accepting operations", exception=NeptuneWarning)
            return

        self._last_version = self.processing_resources.disk_queue.put(op)

        if _queue_has_enough_space(self.processing_resources.disk_queue.size(), self._processing_resources.batch_size):
            self._consumer.wake_up()
        if wait:
            self.wait()

    def start(self) -> None:
        self._consumer.start()

    def pause(self) -> None:
        self._consumer.pause()
        self.flush()

    def resume(self) -> None:
        self._consumer.resume()

    def wait(self) -> None:
        self.flush()
        waiting_for_version = self._last_version
        self._consumer.wake_up()

        # Probably reentering lock just for sure
        with self._processing_resources.waiting_cond:
            self._processing_resources.waiting_cond.wait_for(
                lambda: self._processing_resources.consumed_version >= waiting_for_version
                or not self._consumer.is_running()
            )
        if not self._consumer.is_running():
            raise NeptuneSynchronizationAlreadyStoppedException()

    def stop(
        self, seconds: Optional[float] = None, signal_queue: Optional["Queue[ProcessorStopSignal]"] = None
    ) -> None:
        ts = time()
        self.flush()
        if self._consumer.is_running():
            self._consumer.disable_sleep()
            self._consumer.wake_up()
            self._queue_observer.wait_for_queue_empty(
                seconds=seconds,
                signal_queue=signal_queue,
            )
            self._consumer.interrupt()
        sec_left = None if seconds is None else seconds - (time() - ts)
        self._consumer.join(sec_left)

        # Close resources
        self.close()

        # Remove local files
        if self._queue_observer.is_queue_empty():
            self.cleanup()

    def cleanup(self) -> None:
        super().cleanup()
        try:
            self._processing_resources.data_path.rmdir()
        except OSError:
            pass

    def close(self) -> None:
        self._accepts_operations = False
        super().close()


def _queue_has_enough_space(queue_size: int, batch_size: int) -> bool:
    return queue_size > batch_size / 2


class ConsumerThread(Daemon):
    def __init__(
        self,
        sleep_time: float,
        processing_resources: ProcessingResources,
    ):
        super().__init__(sleep_time=sleep_time, name="NeptuneAsyncOpProcessor")
        self._processing_resources = processing_resources
        self._last_flush: float = 0.0

    def run(self) -> None:
        try:
            super().run()
        except Exception as e:
            with self._processing_resources.waiting_cond:
                self._processing_resources.waiting_cond.notify_all()
            raise Exception from e

    def work(self) -> None:
        ts = time()
        if ts - self._last_flush >= self._sleep_time:
            self._last_flush = ts
            self._processing_resources.disk_queue.flush()

        while True:
            batch = self._processing_resources.disk_queue.get_batch(self._processing_resources.batch_size)
            if not batch:
                return

            signal_batch_started(queue=self._processing_resources.signals_queue)
            self.process_batch([element.obj for element in batch], batch[-1].ver, batch[-1].at)

    @Daemon.ConnectionRetryWrapper(
        kill_message=(
            "Killing Neptune asynchronous thread. All data is safe on disk and can be later"
            " synced manually using `neptune sync` command."
        )
    )
    def process_batch(self, batch: List[Operation], version: int, occurred_at: Optional[float] = None) -> None:
        if occurred_at is not None:
            signal_batch_lag(queue=self._processing_resources.signals_queue, lag=time() - occurred_at)

        expected_count = len(batch)
        version_to_ack = version - expected_count
        while True:

            signal_batch_processed(queue=self._processing_resources.signals_queue)
            version_to_ack += len(batch)

            with self._processing_resources.waiting_cond:
                self._processing_resources.disk_queue.ack(version_to_ack)

                self._processing_resources.consumed_version = version_to_ack

                if version_to_ack == version:
                    self._processing_resources.waiting_cond.notify_all()
                    return


class QueueObserver:
    def __init__(
        self,
        disk_queue: DiskQueue,
        consumer: ConsumerThread,
        should_print_logs: bool,
        stop_queue_max_time_no_connection_seconds: float,
    ):
        self._disk_queue = disk_queue
        self._consumer = consumer
        self._should_print_logs = should_print_logs
        self._stop_queue_max_time_no_connection_seconds = stop_queue_max_time_no_connection_seconds

    def is_queue_empty(self) -> bool:
        return self._disk_queue.is_empty()

    def wait_for_queue_empty(
        self,
        seconds: Optional[float],
        signal_queue: Optional["Queue[ProcessorStopSignal]"] = None,
    ) -> None:
        initial_queue_size = self._disk_queue.size()
        waiting_start: float = monotonic()
        time_elapsed: float = 0.0
        max_reconnect_wait_time: float = self._stop_queue_max_time_no_connection_seconds if seconds is None else seconds
        op_logger = ProcessorStopLogger(
            processor_id=id(self),
            signal_queue=signal_queue,
            logger=logger,
            should_print_logs=self._should_print_logs,
        )
        if initial_queue_size > 0:
            if self._consumer.last_backoff_time > 0:
                op_logger.log_connection_interruption(max_reconnect_wait_time)
            else:
                op_logger.log_remaining_operations(size_remaining=initial_queue_size)

        while True:
            wait_cycle_results = self._wait_single_cycle(
                seconds,
                op_logger,
                initial_queue_size,
                waiting_start,
                time_elapsed,
                max_reconnect_wait_time,
            )
            if wait_cycle_results is None:
                # either there are no more operations to process or there is a synchronization failure
                return
            op_logger.log_still_waiting(
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


def _calculate_wait_cycle_results(disk_queue: DiskQueue, initial_queue_size: int) -> QueueWaitCycleResults:
    size_remaining = disk_queue.size()
    already_synced = initial_queue_size - size_remaining
    already_synced_proc = (already_synced / initial_queue_size) * 100 if initial_queue_size else 100

    return QueueWaitCycleResults(size_remaining, already_synced, already_synced_proc)
