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
__all__ = ("SyncOperationProcessor",)

from typing import (
    TYPE_CHECKING,
    Optional,
)

from neptune.constants import SYNC_DIRECTORY
from neptune.core.components.metadata_file import MetadataFile
from neptune.core.components.operation_storage import OperationStorage
from neptune.internal.operation_processors.operation_processor import OperationProcessor
from neptune.internal.operation_processors.utils import (
    common_metadata,
    get_container_dir,
)
from neptune.internal.utils.disk_utilization import ensure_disk_not_overutilize
from neptune.internal.utils.files import should_clean_internal_data

if TYPE_CHECKING:
    from neptune.internal.backends.neptune_backend import NeptuneBackend
    from neptune.internal.container_type import ContainerType
    from neptune.internal.id_formats import UniqueId
    from neptune.internal.operation import Operation


class SyncOperationProcessor(OperationProcessor):
    def __init__(self, container_id: "UniqueId", container_type: "ContainerType", backend: "NeptuneBackend"):
        self._container_id: "UniqueId" = container_id
        self._container_type: "ContainerType" = container_type
        self._backend: "NeptuneBackend" = backend

        self._data_path = get_container_dir(SYNC_DIRECTORY, container_id, container_type)

        self._metadata_file = MetadataFile(
            data_path=self._data_path,
            metadata=common_metadata(mode="sync", container_id=container_id, container_type=container_type),
        )
        self._operation_storage = OperationStorage(data_path=self._data_path)

    @property
    def operation_storage(self) -> "OperationStorage":
        return self._operation_storage

    @ensure_disk_not_overutilize
    def enqueue_operation(self, op: "Operation", *, wait: bool) -> None:
        _, errors = self._backend.execute_operations(
            container_id=self._container_id,
            container_type=self._container_type,
            operations=[op],
            operation_storage=self._operation_storage,
        )
        if errors:
            raise errors[0]

    def stop(self, seconds: Optional[float] = None) -> None:
        if should_clean_internal_data():
            self._remove_local_files()

    def _remove_local_files(self) -> None:
        self._metadata_file.cleanup()
        self._operation_storage.cleanup()

    def close(self) -> None:
        self._metadata_file.close()
