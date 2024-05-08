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
from pathlib import Path
from queue import Queue
from time import time
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
from neptune.internal.init.parameters import DEFAULT_STOP_TIMEOUT
from neptune.internal.signals_processing.signals import Signal
from neptune.internal.signals_processing.utils import (
    signal_batch_lag,
    signal_batch_processed,
    signal_batch_started,
)
from neptune.internal.threading.daemon import Daemon
from neptune.internal.utils import get_logger
from neptune.internal.utils.disk_utilization import ensure_disk_not_overutilize
from neptune.internal.warnings import (
    NeptuneWarning,
    warn_once,
)

logger = get_logger()


class AsyncOperationProcessor(WithResources, OperationProcessor):
    STOP_QUEUE_STATUS_UPDATE_FREQ_SECONDS = 30.0
    STOP_QUEUE_MAX_TIME_NO_CONNECTION_SECONDS = float(os.getenv(NEPTUNE_SYNC_AFTER_STOP_TIMEOUT, DEFAULT_STOP_TIMEOUT))

    def __init__(
        self,
        container_id: UniqueId,
        container_type: ContainerType,
        lock: threading.RLock,
        batch_size: int = 1,
        data_path: Optional[Path] = None,
        serializer: Callable[[Operation], Dict[str, Any]] = lambda op: op.to_dict(),
    ) -> None:
        self._data_path = (
            data_path if data_path else get_container_full_path(ASYNC_DIRECTORY, container_id, container_type)
        )

        self._metadata_file = MetadataFile(
            data_path=self._data_path,
            metadata=common_metadata(mode="async", container_id=container_id, container_type=container_type),
        )
        self._operation_storage = OperationStorage(data_path=self._data_path)
        self._disk_queue = DiskQueue(
            data_path=self._data_path,
            to_dict=serializer,
            from_dict=Operation.from_dict,
            lock=lock,
        )

        self._accepts_operations: bool = True
        self._last_version: int = 0
        self._batch_size: int = batch_size

    @property
    def resources(self) -> Tuple[Resource, ...]:
        return self._metadata_file, self._operation_storage, self._disk_queue

    @property
    def data_path(self) -> Path:
        return self._data_path

    @ensure_disk_not_overutilize
    def enqueue_operation(self, op: Operation, *, wait: bool) -> None:
        if not self._accepts_operations:
            warn_once("Not accepting operations", exception=NeptuneWarning)
            return

        self._last_version = self._disk_queue.put(op)

        if _queue_has_enough_space(self._disk_queue.size(), self._batch_size):
            # self._consumer.wake_up()
            pass
        if wait:
            self.wait()


def _queue_has_enough_space(queue_size: int, batch_size: int) -> bool:
    return queue_size > batch_size / 2


class ConsumerThread(Daemon):
    def __init__(
        self,
        sleep_time: float,
        batch_size: int,
        waiting_cond: threading.Condition,
        disk_queue: DiskQueue,
        signals_queue: "Queue[Signal]",
        consumed_version: int,
    ):
        super().__init__(sleep_time=sleep_time, name="NeptuneAsyncOpProcessor")
        self._batch_size: int = batch_size
        self._last_flush: float = 0.0
        self._waiting_cond: threading.Condition = waiting_cond
        self._disk_queue = disk_queue
        self._signals_queue = signals_queue
        self._consumed_version = consumed_version

    def run(self) -> None:
        try:
            super().run()
        except Exception:
            with self._waiting_cond:
                self._waiting_cond.notify_all()
            raise

    def work(self) -> None:
        ts = time()
        if ts - self._last_flush >= self._sleep_time:
            self._last_flush = ts
            self._disk_queue.flush()

        while True:
            batch = self._disk_queue.get_batch(self._batch_size)
            if not batch:
                return

            signal_batch_started(queue=self._signals_queue)
            self.process_batch([element.obj for element in batch], batch[-1].ver, batch[-1].at)

    @Daemon.ConnectionRetryWrapper(
        kill_message=(
            "Killing Neptune asynchronous thread. All data is safe on disk and can be later"
            " synced manually using `neptune sync` command."
        )
    )
    def process_batch(self, batch: List[Operation], version: int, occurred_at: Optional[float] = None) -> None:
        if occurred_at is not None:
            signal_batch_lag(queue=self._signals_queue, lag=time() - occurred_at)

        expected_count = len(batch)
        version_to_ack = version - expected_count
        while True:

            signal_batch_processed(queue=self._signals_queue)
            processed_count = len(batch)
            version_to_ack += processed_count
            batch = batch[processed_count:]

            with self._waiting_cond:
                self._disk_queue.ack(version_to_ack)

                self._consumed_version = version_to_ack

                if version_to_ack == version:
                    self._waiting_cond.notify_all()
                    return
