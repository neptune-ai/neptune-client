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
from typing import (
    Any,
    Callable,
    Dict,
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


class AsyncOperationProcessor(WithResources, OperationProcessor):
    STOP_QUEUE_STATUS_UPDATE_FREQ_SECONDS = 30.0
    STOP_QUEUE_MAX_TIME_NO_CONNECTION_SECONDS = float(os.getenv(NEPTUNE_SYNC_AFTER_STOP_TIMEOUT, DEFAULT_STOP_TIMEOUT))

    def __init__(
        self,
        container_id: UniqueId,
        container_type: ContainerType,
        lock: threading.RLock,
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
        self._queue = DiskQueue(
            data_path=self._data_path,
            to_dict=serializer,
            from_dict=Operation.from_dict,
            lock=lock,
        )

    @property
    def resources(self) -> Tuple[Resource, ...]:
        return self._metadata_file, self._operation_storage, self._queue

    @property
    def data_path(self) -> Path:
        return self._data_path

    def enqueue_operation(self, op: Operation, *, wait: bool) -> None:
        pass
