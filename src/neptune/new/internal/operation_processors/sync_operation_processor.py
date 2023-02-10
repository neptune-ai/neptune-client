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

from datetime import datetime
from typing import Optional

from neptune.new.constants import (
    NEPTUNE_DATA_DIRECTORY,
    SYNC_DIRECTORY,
)
from neptune.new.internal.backends.neptune_backend import NeptuneBackend
from neptune.new.internal.container_type import ContainerType
from neptune.new.internal.id_formats import UniqueId
from neptune.new.internal.operation import Operation
from neptune.new.internal.operation_processors.operation_processor import OperationProcessor
from neptune.new.internal.operation_processors.operation_storage import OperationStorage


class SyncOperationProcessor(OperationProcessor):
    def __init__(self, container_id: UniqueId, container_type: ContainerType, backend: NeptuneBackend):
        self._container_id = container_id
        self._container_type = container_type
        self._backend = backend
        self._operation_storage = OperationStorage(self._init_data_path(container_id, container_type))

    @staticmethod
    def _init_data_path(container_id: UniqueId, container_type: ContainerType):
        now = datetime.now()
        container_dir = f"{NEPTUNE_DATA_DIRECTORY}/{SYNC_DIRECTORY}/{container_type.create_dir_name(container_id)}"
        data_path = f"{container_dir}/exec-{now.timestamp()}-{now.strftime('%Y-%m-%d_%H.%M.%S.%f')}"
        return data_path

    def enqueue_operation(self, op: Operation, wait: bool) -> None:
        _, errors = self._backend.execute_operations(
            container_id=self._container_id,
            container_type=self._container_type,
            operations=[op],
            operation_storage=self._operation_storage,
        )
        if errors:
            raise errors[0]

    def wait(self):
        pass

    def flush(self):
        pass

    def start(self):
        pass

    def stop(self, seconds: Optional[float] = None):
        self._operation_storage.close()
