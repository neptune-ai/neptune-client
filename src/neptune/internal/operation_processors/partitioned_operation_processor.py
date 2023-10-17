#
# Copyright (c) 2023, Neptune Labs Sp. z o.o.
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
#
# Copyright (c) 2023, Neptune Labs Sp. z o.o.
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
import logging
import os
import threading
from datetime import datetime
from typing import (
    Callable,
    List,
    Optional,
)

from neptune.internal.backends.neptune_backend import NeptuneBackend
from neptune.internal.container_type import ContainerType
from neptune.internal.id_formats import UniqueId
from neptune.internal.init.parameters import (
    ASYNC_LAG_THRESHOLD,
    ASYNC_NO_PROGRESS_THRESHOLD,
)
from neptune.internal.operation import Operation
from neptune.internal.operation_processors.async_operation_processor import AsyncOperationProcessor
from neptune.internal.operation_processors.operation_processor import OperationProcessor

_logger = logging.getLogger(__name__)


class PartitionedOperationProcessor(OperationProcessor):
    def __init__(
        self,
        container_id: UniqueId,
        container_type: ContainerType,
        backend: NeptuneBackend,
        lock: threading.RLock,
        batch_size: int,
        sleep_time: float = 5,
        async_lag_callback: Optional[Callable[[], None]] = None,
        async_lag_threshold: float = ASYNC_LAG_THRESHOLD,
        async_no_progress_callback: Optional[Callable[[], None]] = None,
        async_no_progress_threshold: float = ASYNC_NO_PROGRESS_THRESHOLD,
        partitions: int = 5,
    ):
        now = datetime.now()
        exec_path = f"exec-{now.timestamp()}-{now.strftime('%Y-%m-%d_%H.%M.%S.%f')}-{os.getpid()}"

        self._partitions = partitions
        self._processors = [
            AsyncOperationProcessor(
                container_id=container_id,
                container_type=container_type,
                backend=backend,
                lock=lock,
                batch_size=batch_size,
                sleep_time=sleep_time,
                async_lag_callback=async_lag_callback,
                async_lag_threshold=async_lag_threshold,
                async_no_progress_callback=async_no_progress_callback,
                async_no_progress_threshold=async_no_progress_threshold,
                path_suffix=f"{exec_path}/partition-{partition_id}",
            )
            for partition_id in range(partitions)
        ]

    def enqueue_operation(self, op: Operation, *, wait: bool) -> None:
        processor = self._get_operation_processor(op.path)
        processor.enqueue_operation(op, wait=wait)

    def _get_operation_processor(self, path: List[str]) -> OperationProcessor:
        path_hash = hash(tuple(path))
        return self._processors[path_hash % self._partitions]

    def pause(self) -> None:
        for processor in self._processors:
            processor.pause()

    def resume(self) -> None:
        for processor in self._processors:
            processor.resume()

    def wait(self) -> None:
        for processor in self._processors:
            processor.wait()

    def flush(self) -> None:
        for processor in self._processors:
            processor.flush()

    def start(self) -> None:
        # TODO: Handle exceptions
        for processor in self._processors:
            processor.start()

    def stop(self, seconds: Optional[float] = None) -> None:
        # TODO: Handle exceptions
        # TODO: Better stop (async?)
        if seconds is not None:
            seconds /= self._partitions

        for processor in self._processors:
            processor.stop(seconds=seconds)

    def close(self) -> None:
        for processor in self._processors:
            processor.close()
