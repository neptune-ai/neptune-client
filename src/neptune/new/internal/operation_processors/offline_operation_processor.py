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

from neptune.new.constants import (
    NEPTUNE_DATA_DIRECTORY,
    OFFLINE_DIRECTORY,
)
from neptune.new.internal.container_type import ContainerType
from neptune.new.internal.disk_queue import DiskQueue
from neptune.new.internal.id_formats import UniqueId
from neptune.new.internal.operation import Operation
from neptune.new.internal.operation_processors.operation_processor import OperationProcessor
from neptune.new.internal.operation_processors.operation_storage import OperationStorage


class OfflineOperationProcessor(OperationProcessor):
    def __init__(self, container_id: UniqueId, container_type: ContainerType, lock: threading.RLock):
        self._operation_storage = OperationStorage(self._init_data_path(container_id, container_type))

        self._queue = DiskQueue(
            dir_path=self._operation_storage.data_path,
            to_dict=lambda x: x.to_dict(),
            from_dict=Operation.from_dict,
            lock=lock,
        )

    @staticmethod
    def _init_data_path(container_id: UniqueId, container_type: ContainerType):
        return f"{NEPTUNE_DATA_DIRECTORY}/{OFFLINE_DIRECTORY}/{container_type.create_dir_name(container_id)}"

    def enqueue_operation(self, op: Operation, wait: bool) -> None:
        self._queue.put(op)

    def wait(self):
        self.flush()

    def flush(self):
        self._queue.flush()

    def start(self):
        pass

    def stop(self, seconds: Optional[float] = None):
        self._queue.close()
