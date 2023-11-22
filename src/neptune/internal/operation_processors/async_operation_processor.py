#
# Copyright (c) 2022, Neptune Labs Sp. z o.o.
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
__all__ = ("AsyncOperationProcessor",)

import os
import threading
from datetime import datetime
from pathlib import Path
from queue import Queue
from time import (
    monotonic,
    time,
)
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    Optional,
)

from neptune.common.exceptions import NeptuneException
from neptune.common.warnings import (
    NeptuneWarning,
    warn_once,
)
from neptune.constants import ASYNC_DIRECTORY
from neptune.envs import NEPTUNE_SYNC_AFTER_STOP_TIMEOUT
from neptune.exceptions import NeptuneSynchronizationAlreadyStoppedException
from neptune.internal.disk_queue import DiskQueue
from neptune.internal.init.parameters import DEFAULT_STOP_TIMEOUT
from neptune.internal.metadata_file import MetadataFile
from neptune.internal.operation import Operation
from neptune.internal.operation_processors.operation_processor import OperationProcessor
from neptune.internal.operation_processors.operation_storage import (
    OperationStorage,
    get_container_dir,
)
from neptune.internal.operation_processors.utils import common_metadata
from neptune.internal.signals_processing.utils import (
    signal_batch_lag,
    signal_batch_processed,
    signal_batch_started,
)
from neptune.internal.threading.daemon import Daemon
from neptune.internal.utils.disk_utilization import ensure_disk_not_overutilize
from neptune.internal.utils.logger import logger

if TYPE_CHECKING:
    from neptune.internal.backends.neptune_backend import NeptuneBackend
    from neptune.internal.container_type import ContainerType
    from neptune.internal.id_formats import UniqueId
    from neptune.internal.signals_processing.signals import Signal


class AsyncOperationProcessor(OperationProcessor):
    STOP_QUEUE_STATUS_UPDATE_FREQ_SECONDS = 30.0
    STOP_QUEUE_MAX_TIME_NO_CONNECTION_SECONDS = float(os.getenv(NEPTUNE_SYNC_AFTER_STOP_TIMEOUT, DEFAULT_STOP_TIMEOUT))

    def __init__(
        self,
        container_id: "UniqueId",
        container_type: "ContainerType",
        backend: "NeptuneBackend",
        lock: threading.RLock,
        queue: "Queue[Signal]",
        sleep_time: float = 5,
        batch_size: int = 1000,
        data_path: Optional[Path] = None,
        should_print_logs: bool = True,
    ):
        self._should_print_logs: bool = should_print_logs
        self._data_path = data_path if data_path else self._init_data_path(container_id, container_type)
        self._metadata_file = MetadataFile(
            data_path=self._data_path,
            metadata=common_metadata(mode="async", container_id=container_id, container_type=container_type),
        )
        self._operation_storage = OperationStorage(data_path=self._data_path)

        serializer: Callable[[Operation], Dict[str, Any]] = lambda op: op.to_dict()
        self._queue = DiskQueue(
            dir_path=self._operation_storage.data_path,
            to_dict=serializer,
            from_dict=Operation.from_dict,
            lock=lock,
        )

        self._container_id: "UniqueId" = container_id
        self._container_type: "ContainerType" = container_type
        self._backend: "NeptuneBackend" = backend
        self._batch_size: int = batch_size
        self._last_version: int = 0
        self._consumed_version: int = 0
        self._consumer: Daemon = self.ConsumerThread(self, sleep_time, batch_size)
        self._lock: threading.RLock = lock
        self._signals_queue: "Queue[Signal]" = queue
        self._accepts_operations: bool = True

        # Caller is responsible for taking this lock
        self._waiting_cond = threading.Condition(lock=lock)

    @staticmethod
    def _init_data_path(container_id: "UniqueId", container_type: "ContainerType") -> Path:
        now = datetime.now()
        path_suffix = f"exec-{now.timestamp()}-{now.strftime('%Y-%m-%d_%H.%M.%S.%f')}-{os.getpid()}"
        return get_container_dir(ASYNC_DIRECTORY, container_id, container_type, path_suffix)

    @ensure_disk_not_overutilize
    def enqueue_operation(self, op: Operation, *, wait: bool) -> None:
        if not self._accepts_operations:
            warn_once("Not accepting operations", exception=NeptuneWarning)
            return

        self._last_version = self._queue.put(op)

        if self._check_queue_size():
            self._consumer.wake_up()
        if wait:
            self.wait()

    def start(self) -> None:
        self._consumer.start()

    def pause(self) -> None:
        self._consumer.pause()
        self._queue.flush()

    def resume(self) -> None:
        self._consumer.resume()

    def flush(self) -> None:
        self._queue.flush()

    def wait(self) -> None:
        self.flush()
        waiting_for_version = self._last_version
        self._consumer.wake_up()

        # Probably reentering lock just for sure
        with self._waiting_cond:
            self._waiting_cond.wait_for(
                lambda: self._consumed_version >= waiting_for_version or not self._consumer.is_running()
            )
        if not self._consumer.is_running():
            raise NeptuneSynchronizationAlreadyStoppedException()

    def _check_queue_size(self) -> bool:
        return self._queue.size() > self._batch_size / 2

    def _wait_for_queue_empty(self, initial_queue_size: int, seconds: Optional[float]) -> None:
        waiting_start: float = monotonic()
        time_elapsed: float = 0.0
        max_reconnect_wait_time: float = self.STOP_QUEUE_MAX_TIME_NO_CONNECTION_SECONDS if seconds is None else seconds

        if initial_queue_size > 0:
            if self._consumer.last_backoff_time > 0:
                logger.warning(
                    "We have been experiencing connection interruptions during your run."
                    " Neptune client will now try to resume connection and sync data for the next"
                    " %s seconds."
                    " You can also kill this process and synchronize your data manually later"
                    " using `neptune sync` command.",
                    max_reconnect_wait_time,
                )
            else:
                logger.warning(
                    "Waiting for the remaining %s operations to synchronize with Neptune." " Do not kill this process.",
                    initial_queue_size,
                )

        while True:
            if seconds is None:
                if self._consumer.last_backoff_time == 0:
                    # reset `waiting_start` on successful action
                    waiting_start = monotonic()
                wait_time = self.STOP_QUEUE_STATUS_UPDATE_FREQ_SECONDS
            else:
                wait_time = max(
                    min(
                        seconds - time_elapsed,
                        self.STOP_QUEUE_STATUS_UPDATE_FREQ_SECONDS,
                    ),
                    0.0,
                )
            self._queue.wait_for_empty(wait_time)
            size_remaining = self._queue.size()
            already_synced = initial_queue_size - size_remaining
            already_synced_proc = (already_synced / initial_queue_size) * 100 if initial_queue_size else 100
            if size_remaining == 0:
                if self._should_print_logs:
                    logger.info("All %s operations synced, thanks for waiting!", initial_queue_size)
                return

            time_elapsed = monotonic() - waiting_start
            if self._consumer.last_backoff_time > 0 and time_elapsed >= max_reconnect_wait_time:
                logger.warning(
                    "Failed to reconnect with Neptune in %s seconds."
                    " You have %s operations saved on disk that can be manually synced"
                    " using `neptune sync` command.",
                    max_reconnect_wait_time,
                    size_remaining,
                )
                return

            if seconds is not None and wait_time == 0:
                logger.warning(
                    "Failed to sync all operations in %s seconds."
                    " You have %s operations saved on disk that can be manually synced"
                    " using `neptune sync` command.",
                    seconds,
                    size_remaining,
                )
                return

            if not self._consumer.is_running():
                exception = NeptuneSynchronizationAlreadyStoppedException()
                logger.warning(str(exception))
                return

            logger.warning(
                "Still waiting for the remaining %s operations" " (%.2f%% done). Please wait.",
                size_remaining,
                already_synced_proc,
            )

    def stop(self, seconds: Optional[float] = None) -> None:
        ts = time()
        self._queue.flush()
        if self._consumer.is_running():
            self._consumer.disable_sleep()
            self._consumer.wake_up()
            self._wait_for_queue_empty(initial_queue_size=self._queue.size(), seconds=seconds)
            self._consumer.interrupt()
        sec_left = None if seconds is None else seconds - (time() - ts)
        self._consumer.join(sec_left)

        # Close resources
        self.close()

        # Remove local files
        if self._queue.is_empty():
            # TODO: Will be refactored
            self._metadata_file.cleanup()
            self._queue.cleanup_if_empty()

    def close(self) -> None:
        self._accepts_operations = False
        self._queue.close()
        self._metadata_file.close()

    class ConsumerThread(Daemon):
        def __init__(
            self,
            processor: "AsyncOperationProcessor",
            sleep_time: float,
            batch_size: int,
        ):
            super().__init__(sleep_time=sleep_time, name="NeptuneAsyncOpProcessor")
            self._processor: "AsyncOperationProcessor" = processor
            self._batch_size: int = batch_size
            self._last_flush: float = 0.0

        def run(self) -> None:
            try:
                super().run()
            except Exception:
                with self._processor._waiting_cond:
                    self._processor._waiting_cond.notify_all()
                raise

        def work(self) -> None:
            ts = time()
            if ts - self._last_flush >= self._sleep_time:
                self._last_flush = ts
                self._processor._queue.flush()

            while True:
                batch = self._processor._queue.get_batch(self._batch_size)
                if not batch:
                    return

                signal_batch_started(queue=self._processor._signals_queue)
                self.process_batch([element.obj for element in batch], batch[-1].ver, batch[-1].at)

        # WARNING: Be careful when changing this function. It is used in the experimental package
        def _handle_errors(self, errors: List[NeptuneException]) -> None:
            for error in errors:
                logger.error(
                    "Error occurred during asynchronous operation processing: %s",
                    error,
                )

        @Daemon.ConnectionRetryWrapper(
            kill_message=(
                "Killing Neptune asynchronous thread. All data is safe on disk and can be later"
                " synced manually using `neptune sync` command."
            )
        )
        def process_batch(self, batch: List[Operation], version: int, occurred_at: Optional[float] = None) -> None:
            if occurred_at is not None:
                signal_batch_lag(queue=self._processor._signals_queue, lag=time() - occurred_at)

            expected_count = len(batch)
            version_to_ack = version - expected_count
            while True:
                # TODO: Handle Metadata errors
                processed_count, errors = self._processor._backend.execute_operations(
                    container_id=self._processor._container_id,
                    container_type=self._processor._container_type,
                    operations=batch,
                    operation_storage=self._processor._operation_storage,
                )

                signal_batch_processed(queue=self._processor._signals_queue)
                version_to_ack += processed_count
                batch = batch[processed_count:]

                with self._processor._waiting_cond:
                    self._processor._queue.ack(version_to_ack)

                    self._handle_errors(errors)

                    self._processor._consumed_version = version_to_ack

                    if version_to_ack == version:
                        self._processor._waiting_cond.notify_all()
                        return
