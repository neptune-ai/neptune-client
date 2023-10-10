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

import logging
import os
import threading
from datetime import datetime
from pathlib import Path
from time import (
    monotonic,
    time,
)
from typing import (
    TYPE_CHECKING,
    Callable,
    ClassVar,
    Optional,
)

from neptune.constants import ASYNC_DIRECTORY
from neptune.envs import NEPTUNE_SYNC_AFTER_STOP_TIMEOUT
from neptune.exceptions import NeptuneSynchronizationAlreadyStoppedException
from neptune.internal.backends.neptune_backend import NeptuneBackend
from neptune.internal.container_type import ContainerType
from neptune.internal.id_formats import UniqueId
from neptune.internal.init.parameters import (
    ASYNC_LAG_THRESHOLD,
    ASYNC_NO_PROGRESS_THRESHOLD,
    DEFAULT_STOP_TIMEOUT,
)
from neptune.internal.operation import Operation
from neptune.internal.operation_processors.batcher import Batcher
from neptune.internal.operation_processors.operation_processor import OperationProcessor
from neptune.internal.operation_processors.operation_storage import (
    OperationStorage,
    get_container_dir,
)
from neptune.internal.queue.disk_queue import DiskQueue
from neptune.internal.threading.daemon import Daemon
from neptune.internal.utils.disk_full import ensure_disk_not_full
from neptune.internal.utils.logger import logger

if TYPE_CHECKING:
    from neptune.internal.preprocessor.accumulated_operations import AccumulatedOperations

_logger = logging.getLogger(__name__)


class AsyncOperationProcessor(OperationProcessor):
    STOP_QUEUE_STATUS_UPDATE_FREQ_SECONDS = 30
    STOP_QUEUE_MAX_TIME_NO_CONNECTION_SECONDS = int(os.getenv(NEPTUNE_SYNC_AFTER_STOP_TIMEOUT, DEFAULT_STOP_TIMEOUT))

    def __init__(
        self,
        container_id: UniqueId,
        container_type: ContainerType,
        backend: NeptuneBackend,
        lock: threading.RLock,
        sleep_time: float = 5,
        batch_size: int = 1000,
        async_lag_callback: Optional[Callable[[], None]] = None,
        async_lag_threshold: float = ASYNC_LAG_THRESHOLD,
        async_no_progress_callback: Optional[Callable[[], None]] = None,
        async_no_progress_threshold: float = ASYNC_NO_PROGRESS_THRESHOLD,
    ):
        self._operation_storage = OperationStorage(self._init_data_path(container_id, container_type))

        self._queue = DiskQueue(
            dir_path=self._operation_storage.data_path,
            to_dict=lambda x: x.to_dict(),
            from_dict=Operation.from_dict,
            lock=lock,
        )

        self._container_id = container_id
        self._container_type = container_type
        self._backend = backend
        self._batch_size = batch_size
        self._async_lag_callback = async_lag_callback or (lambda: None)
        self._async_lag_threshold = async_lag_threshold
        self._async_no_progress_callback = async_no_progress_callback or (lambda: None)
        self._async_no_progress_threshold = async_no_progress_threshold
        self._last_version = 0
        self._consumed_version = 0
        self._consumer = self.ConsumerThread(self, sleep_time, batch_size)
        self._lock = lock
        self._last_ack = None
        self._lag_exceeded = False
        self._should_call_no_progress_callback = False

        # Caller is responsible for taking this lock
        self._waiting_cond = threading.Condition(lock=lock)

    @staticmethod
    def _init_data_path(container_id: UniqueId, container_type: ContainerType) -> Path:
        now = datetime.now()
        process_path = f"exec-{now.timestamp()}-{now.strftime('%Y-%m-%d_%H.%M.%S.%f')}-{os.getpid()}"
        return get_container_dir(ASYNC_DIRECTORY, container_id, container_type, process_path)

    @ensure_disk_not_full
    def enqueue_operation(self, op: Operation, *, wait: bool) -> None:
        self._last_version = self._queue.put(op)

        self._check_lag()
        self._check_no_progress()

        if self._queue.size() > self._batch_size / 2:
            self._consumer.wake_up()
        if wait:
            self.wait()

    def wait(self):
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

    def _check_lag(self):
        if self._lag_exceeded or not self._last_ack or monotonic() - self._last_ack <= self._async_lag_threshold:
            return

        with self._lock:
            if not self._lag_exceeded:
                self._async_lag_callback()
                self._lag_exceeded = True

    def _check_no_progress(self):
        if not self._should_call_no_progress_callback:
            return

        with self._lock:
            if self._should_call_no_progress_callback:
                self._async_no_progress_callback()
                self._should_call_no_progress_callback = False

    def flush(self):
        self._queue.flush()

    def start(self):
        self._consumer.start()

    def pause(self):
        self._consumer.pause()
        self._queue.flush()

    def resume(self):
        self._consumer.resume()

    def _wait_for_queue_empty(self, initial_queue_size: int, seconds: Optional[float]):
        waiting_start = monotonic()
        time_elapsed = 0
        max_reconnect_wait_time = self.STOP_QUEUE_MAX_TIME_NO_CONNECTION_SECONDS if seconds is None else seconds
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
                    0,
                )
            self._queue.wait_for_empty(wait_time)
            size_remaining = self._queue.size()
            already_synced = initial_queue_size - size_remaining
            already_synced_proc = (already_synced / initial_queue_size) * 100 if initial_queue_size else 100
            if size_remaining == 0:
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
        self._queue.cleanup_if_empty()

    def close(self):
        self._queue.close()

    class ConsumerThread(Daemon):
        MAX_POINTS_PER_BATCH: ClassVar[int] = 100000
        MAX_ATTRIBUTES_IN_BATCH: ClassVar[int] = 1000
        MAX_POINTS_PER_ATTRIBUTE: ClassVar[int] = 10000

        def __init__(
            self,
            processor: "AsyncOperationProcessor",
            sleep_time: float,
            batch_size: int,
        ):
            super().__init__(sleep_time=sleep_time, name="NeptuneAsyncOpProcessor")
            self._processor = processor
            self._batch_size = batch_size
            self._last_flush = 0
            self._no_progress_exceeded = False
            self._batcher = Batcher(
                queue=self._processor._queue,
                backend=self._processor._backend,
                max_points_per_batch=self.MAX_POINTS_PER_BATCH,
                max_attributes_in_batch=self.MAX_ATTRIBUTES_IN_BATCH,
                max_points_per_attribute=self.MAX_POINTS_PER_ATTRIBUTE,
            )

        def run(self):
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

            while self.collect_and_process_batch():
                pass

        @Daemon.ConnectionRetryWrapper(
            kill_message=(
                "Killing Neptune asynchronous thread. All data is safe on disk and can be later"
                " synced manually using `neptune sync` command."
            )
        )
        def collect_and_process_batch(self) -> bool:
            batch = self._batcher.collect_batch()

            if batch:
                operations, dropped_operations_count, version = batch
                self.process_batch(operations, dropped_operations_count, version)

            return batch is not None

        def _check_no_progress(self):
            if not self._no_progress_exceeded:
                if monotonic() - self._processor._last_ack > self._processor._async_no_progress_threshold:
                    self._no_progress_exceeded = True
                    self._processor._should_call_no_progress_callback = True

        def process_batch(self, batch: "AccumulatedOperations", dropped_operations_count: int, version: int) -> None:
            expected_count = batch.operations_count
            version_to_ack = version - expected_count

            while True:
                # TODO: Handle Metadata errors
                try:
                    processed_count, errors = self._processor._backend.execute_operations_from_accumulator(
                        container_id=self._processor._container_id,
                        container_type=self._processor._container_type,
                        accumulated_operations=batch,
                        operation_storage=self._processor._operation_storage,
                    )
                except Exception as e:
                    self._check_no_progress()
                    # Let default retry logic handle this
                    raise e from e

                self._no_progress_exceeded = False

                version_to_ack += processed_count + dropped_operations_count

                with self._processor._waiting_cond:
                    self._processor._queue.ack(version_to_ack)
                    self._processor._last_ack = monotonic()
                    self._processor._lag_exceeded = False

                    for error in errors:
                        _logger.error(
                            "Error occurred during asynchronous operation processing: %s",
                            error,
                        )

                    self._processor._consumed_version = version_to_ack

                    if version_to_ack == version:
                        self._processor._waiting_cond.notify_all()
                        return
