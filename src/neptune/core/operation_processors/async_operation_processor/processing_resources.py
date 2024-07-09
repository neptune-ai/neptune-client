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

from __future__ import annotations

__all__ = ["ProcessingResources"]

import threading
from pathlib import Path
from queue import Queue
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
from neptune.core.components.queue.aggregating_disk_queue import AggregatingDiskQueue
from neptune.core.operation_processors.utils import (
    common_metadata,
    get_container_full_path,
)
from neptune.core.operations.operation import Operation
from neptune.core.typing.container_type import ContainerType
from neptune.core.typing.id_formats import CustomId
from neptune.internal.signals_processing.signals import Signal


class ProcessingResources(WithResources):
    def __init__(
        self,
        custom_id: CustomId,
        container_type: ContainerType,
        lock: threading.RLock,
        signal_queue: Queue[Signal],
        batch_size: int = 1,
        data_path: Optional[Path] = None,
        serializer: Callable[[Operation], Dict[str, Any]] = lambda op: op.to_dict(),
    ) -> None:
        self.batch_size: int = batch_size
        self._data_path = (
            data_path if data_path else get_container_full_path(ASYNC_DIRECTORY, custom_id, container_type)
        )

        self.metadata_file = MetadataFile(
            data_path=self._data_path,
            metadata=common_metadata(mode="async", custom_id=custom_id, container_type=container_type),
        )
        self.operation_storage = OperationStorage(data_path=self._data_path)
        self.disk_queue = AggregatingDiskQueue[Operation, float](
            data_path=self._data_path,
            to_dict=serializer,
            from_dict=Operation.from_dict,
            lock=lock,
        )

        self.waiting_cond = threading.Condition()

        self.signals_queue = signal_queue

        self.consumed_version: int = 0

    @property
    def resources(self) -> Tuple[Resource, ...]:
        return self.metadata_file, self.operation_storage, self.disk_queue

    @property
    def data_path(self) -> Path:
        return self._data_path

    def cleanup(self) -> None:
        super().cleanup()
        try:
            self.data_path.rmdir()
        except OSError:
            pass
