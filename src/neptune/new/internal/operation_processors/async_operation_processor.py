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
import sys
import threading
from datetime import datetime
from time import (
    monotonic,
    time,
)
from typing import (
    List,
    Optional,
)

from neptune.new.constants import (
    ASYNC_DIRECTORY,
    NEPTUNE_DATA_DIRECTORY,
)
from neptune.new.exceptions import NeptuneSynchronizationAlreadyStoppedException
from neptune.new.internal.backends.neptune_backend import NeptuneBackend
from neptune.new.internal.container_type import ContainerType
from neptune.new.internal.disk_queue import DiskQueue
from neptune.new.internal.id_formats import UniqueId
from neptune.new.internal.operation import Operation
from neptune.new.internal.operation_processors.operation_processor import OperationProcessor
from neptune.new.internal.operation_processors.operation_storage import OperationStorage
from neptune.new.internal.threading.daemon import Daemon
from neptune.new.internal.utils.logger import logger

_logger = logging.getLogger(__name__)


class AsyncOperationProcessor(OperationProcessor):
    STOP_QUEUE_STATUS_UPDATE_FREQ_SECONDS = 30
    STOP_QUEUE_MAX_TIME_NO_CONNECTION_SECONDS = 300

    def __init__(
        self,
        container_id: UniqueId,
        container_type: ContainerType,
        backend: NeptuneBackend,
        lock: threading.RLock,
        sleep_time: float = 5,
        batch_size: int = 1000,
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
        self._last_version = 0
        self._consumed_version = 0
        self._consumer = self.ConsumerThread(self, sleep_time, batch_size)
        self._drop_operations = False

        # Caller is responsible for taking this lock
        self._waiting_cond = threading.Condition(lock=lock)

        if sys.version_info >= (3, 7):
            try:
                os.register_at_fork(after_in_child=self._handle_fork_in_child)
            except AttributeError:
                pass

    @staticmethod
    def _init_data_path(container_id: UniqueId, container_type: ContainerType):
        now = datetime.now()
        container_dir = f"{NEPTUNE_DATA_DIRECTORY}/{ASYNC_DIRECTORY}/{container_type.create_dir_name(container_id)}"
        data_path = f"{container_dir}/exec-{now.timestamp()}-{now.strftime('%Y-%m-%d_%H.%M.%S.%f')}"
        data_path = data_path.replace(" ", "_").replace(":", ".")
        return data_path

    def _handle_fork_in_child(self):
        self._drop_operations = True

    def enqueue_operation(self, op: Operation, wait: bool) -> None:
        if self._drop_operations:
            return
        self._last_version = self._queue.put(op)
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

    def flush(self):
        self._queue.flush()

    def start(self):
        self._consumer.start()

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

    def stop(self, seconds: Optional[float] = None):
        ts = time()
        self._queue.flush()
        if self._consumer.is_running():
            self._consumer.disable_sleep()
            self._consumer.wake_up()
            self._wait_for_queue_empty(initial_queue_size=self._queue.size(), seconds=seconds)
            self._consumer.interrupt()
        sec_left = None if seconds is None else seconds - (time() - ts)
        self._consumer.join(sec_left)
        self._queue.close()

    class ConsumerThread(Daemon):
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

            while True:
                batch = self._processor._queue.get_batch(self._batch_size)
                if not batch:
                    return
                self.process_batch([element.obj for element in batch], batch[-1].ver)

        @Daemon.ConnectionRetryWrapper(
            kill_message=(
                "Killing Neptune asynchronous thread. All data is safe on disk and can be later"
                " synced manually using `neptune sync` command."
            )
        )
        def process_batch(self, batch: List[Operation], version: int) -> None:
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
                version_to_ack += processed_count
                batch = batch[processed_count:]
                with self._processor._waiting_cond:
                    self._processor._queue.ack(version_to_ack)

                    for error in errors:
                        _logger.error(
                            "Error occurred during asynchronous operation processing: %s",
                            error,
                        )

                    self._processor._consumed_version = version_to_ack

                    if version_to_ack == version:
                        self._processor._waiting_cond.notify_all()
                        return
