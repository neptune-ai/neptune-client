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
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Optional,
    Tuple,
)

from neptune.constants import OFFLINE_DIRECTORY
from neptune.core.components.abstract import WithResources
from neptune.core.components.metadata_file import MetadataFile
from neptune.core.components.operation_storage import OperationStorage
from neptune.core.components.queue.disk_queue import DiskQueue
from neptune.internal.operation import Operation
from neptune.internal.operation_processors.operation_processor import OperationProcessor
from neptune.internal.operation_processors.utils import (
    common_metadata,
    get_container_full_path,
)
from neptune.internal.utils.disk_utilization import ensure_disk_not_overutilize

if TYPE_CHECKING:
    from neptune.core.components.abstract import Resource
    from neptune.internal.container_type import ContainerType
    from neptune.internal.id_formats import UniqueId


serializer: Callable[[Operation], Dict[str, Any]] = lambda op: op.to_dict()


class OfflineOperationProcessor(WithResources, OperationProcessor):
    def __init__(self, container_id: "UniqueId", container_type: "ContainerType", lock: "threading.RLock"):
        self._data_path = get_container_full_path(OFFLINE_DIRECTORY, container_id, container_type)

        # Initialize directory
        self._data_path.mkdir(parents=True, exist_ok=True)

        self._metadata_file = MetadataFile(
            data_path=self._data_path,
            metadata=common_metadata(mode="offline", container_id=container_id, container_type=container_type),
        )
        self._operation_storage = OperationStorage(data_path=self._data_path)
        self._queue = DiskQueue(data_path=self._data_path, to_dict=serializer, from_dict=Operation.from_dict, lock=lock)

    @property
    def operation_storage(self) -> "OperationStorage":
        return self._operation_storage

    @property
    def data_path(self) -> Path:
        return self._data_path

    @property
    def resources(self) -> Tuple["Resource", ...]:
        return self._metadata_file, self._operation_storage, self._queue

    @ensure_disk_not_overutilize
    def enqueue_operation(self, op: Operation, *, wait: bool) -> None:
        self._queue.put(op)

    def wait(self) -> None:
        self.flush()

    def stop(self, seconds: Optional[float] = None) -> None:
        self.flush()
        self.close()
