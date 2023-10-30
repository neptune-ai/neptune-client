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

from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Optional,
)

from neptune.constants import OFFLINE_DIRECTORY
from neptune.internal.disk_queue import DiskQueue
from neptune.internal.metadata_file import MetadataFile
from neptune.internal.operation import Operation
from neptune.internal.operation_processors.operation_processor import OperationProcessor
from neptune.internal.operation_processors.operation_storage import (
    OperationStorage,
    get_container_dir,
)
from neptune.internal.operation_processors.utils import common_metadata
from neptune.internal.utils.disk_utilization import ensure_disk_not_overutilize

if TYPE_CHECKING:
    import threading
    from pathlib import Path

    from neptune.internal.container_type import ContainerType
    from neptune.internal.id_formats import UniqueId


class OfflineOperationProcessor(OperationProcessor):
    def __init__(self, container_id: "UniqueId", container_type: "ContainerType", lock: "threading.RLock"):
        data_path = self._init_data_path(container_id, container_type)

        self._metadata_file = MetadataFile(
            data_path=data_path,
            metadata=common_metadata(mode="offline", container_id=container_id, container_type=container_type),
        )
        self._operation_storage = OperationStorage(data_path=data_path)

        serializer: Callable[[Operation], Dict[str, Any]] = lambda op: op.to_dict()
        self._queue = DiskQueue(dir_path=data_path, to_dict=serializer, from_dict=Operation.from_dict, lock=lock)

    @staticmethod
    def _init_data_path(container_id: "UniqueId", container_type: "ContainerType") -> "Path":
        return get_container_dir(OFFLINE_DIRECTORY, container_id, container_type)

    @ensure_disk_not_overutilize
    def enqueue_operation(self, op: Operation, *, wait: bool) -> None:
        self._queue.put(op)

    def flush(self) -> None:
        self._queue.flush()

    def wait(self) -> None:
        self.flush()

    def stop(self, seconds: Optional[float] = None) -> None:
        self.close()

    def close(self) -> None:
        self._queue.close()
        self._metadata_file.close()
