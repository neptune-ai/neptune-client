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
__all__ = ("OfflineOperationProcessor",)

import threading
from typing import Optional

from neptune.constants import OFFLINE_DIRECTORY
from neptune.internal.container_type import ContainerType
from neptune.internal.id_formats import UniqueId
from neptune.internal.operation import Operation
from neptune.internal.operation_processors.operation_processor import OperationProcessor
from neptune.internal.operation_processors.operation_storage import OperationStorage
from neptune.internal.operation_processors.utils import get_container_dir
from neptune.internal.queue.disk_queue import DiskQueue
from neptune.internal.utils.disk_full import ensure_disk_not_full


class OfflineOperationProcessor(OperationProcessor):
    def __init__(self, container_id: UniqueId, container_type: ContainerType, lock: threading.RLock):
        self._container_id: UniqueId = container_id
        self._container_type: ContainerType = container_type

        data_path = get_container_dir(
            type_dir=OFFLINE_DIRECTORY, container_id=container_id, container_type=container_type
        )

        self._operation_storage = OperationStorage(data_path=data_path)
        self._queue = DiskQueue(dir_path=data_path, to_dict=Operation.to_dict, from_dict=Operation.from_dict, lock=lock)

    @ensure_disk_not_full
    def enqueue_operation(self, op: Operation, *, wait: bool) -> None:
        self._queue.put(op)

    def stop(self, seconds: Optional[float] = None) -> None:
        self.close()
        # Remove local files
        # TODO: Cleanup operation storage as well
        self._queue.cleanup_if_empty()

    def wait(self) -> None:
        self.flush()

    def flush(self) -> None:
        self._queue.flush()

    def close(self) -> None:
        self._queue.close()
