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
__all__ = ("SyncOperationProcessor",)

from pathlib import Path
from typing import (
    Optional,
    Tuple,
)

from neptune.constants import SYNC_DIRECTORY
from neptune.core.components.abstract import (
    Resource,
    WithResources,
)
from neptune.core.components.metadata_file import MetadataFile
from neptune.core.components.operation_storage import OperationStorage
from neptune.core.operation_processors.operation_processor import OperationProcessor
from neptune.core.operation_processors.utils import (
    common_metadata,
    get_container_full_path,
)
from neptune.core.operations.operation import Operation
from neptune.core.typing.container_type import ContainerType
from neptune.core.typing.id_formats import CustomId
from neptune.internal.utils.disk_utilization import ensure_disk_not_overutilize


class SyncOperationProcessor(WithResources, OperationProcessor):
    def __init__(self, custom_id: "CustomId", container_type: "ContainerType"):
        self._container_id: "CustomId" = custom_id
        self._container_type: "ContainerType" = container_type

        self._data_path = get_container_full_path(SYNC_DIRECTORY, custom_id, container_type)

        # Initialize directory
        self._data_path.mkdir(parents=True, exist_ok=True)

        self._metadata_file = MetadataFile(
            data_path=self._data_path,
            metadata=common_metadata(mode="sync", custom_id=custom_id, container_type=container_type),
        )
        self._operation_storage = OperationStorage(data_path=self._data_path)

    @property
    def operation_storage(self) -> "OperationStorage":
        return self._operation_storage

    @property
    def data_path(self) -> Path:
        return self._data_path

    @property
    def resources(self) -> Tuple["Resource", ...]:
        return self._metadata_file, self._operation_storage

    @ensure_disk_not_overutilize
    def enqueue_operation(self, op: "Operation", *, wait: bool) -> None: ...

    def stop(self, seconds: Optional[float] = None) -> None:
        self.flush()
        self.close()
        self.cleanup()

    def cleanup(self) -> None:
        super().cleanup()
        try:
            self._data_path.rmdir()
        except OSError:
            pass
