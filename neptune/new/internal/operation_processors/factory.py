#
# Copyright (c) 2021, Neptune Labs Sp. z o.o.
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
from datetime import datetime
from pathlib import Path

from neptune.new.constants import (
    ASYNC_DIRECTORY,
    NEPTUNE_DATA_DIRECTORY,
    OFFLINE_DIRECTORY,
)
from neptune.new.internal.backends.neptune_backend import NeptuneBackend
from neptune.new.internal.containers.disk_queue import DiskQueue
from neptune.new.internal.operation import Operation
from neptune.new.types.mode import Mode
from .async_operation_processor import AsyncOperationProcessor
from .offline_operation_processor import OfflineOperationProcessor
from .operation_processor import OperationProcessor
from .read_only_operation_processor import ReadOnlyOperationProcessor
from .sync_operation_processor import SyncOperationProcessor
from ..container_type import ContainerType


def get_operation_processor(
    mode: Mode,
    container_id: str,
    container_type: ContainerType,
    backend: NeptuneBackend,
    lock: threading.RLock,
    flush_period: float,
) -> OperationProcessor:

    if mode == Mode.ASYNC:
        data_path = "{}/{}/{}".format(
            NEPTUNE_DATA_DIRECTORY, ASYNC_DIRECTORY, container_id
        )
        try:
            execution_id = len(os.listdir(data_path))
        except FileNotFoundError:
            execution_id = 0
        execution_path = "{}/exec-{}-{}".format(data_path, execution_id, datetime.now())
        execution_path = execution_path.replace(" ", "_").replace(":", ".")
        return AsyncOperationProcessor(
            container_id,
            container_type,
            DiskQueue(
                Path(execution_path),
                lambda x: x.to_dict(),
                Operation.from_dict,
                lock,
                container_type,
            ),
            backend,
            lock,
            sleep_time=flush_period,
        )
    elif mode == Mode.SYNC:
        return SyncOperationProcessor(container_id, container_type, backend)
    elif mode == Mode.DEBUG:
        return SyncOperationProcessor(container_id, container_type, backend)
    elif mode == Mode.OFFLINE:
        # the object was returned by mocked backend and has some random ID.
        data_path = "{}/{}/{}".format(
            NEPTUNE_DATA_DIRECTORY, OFFLINE_DIRECTORY, container_id
        )
        storage_queue = DiskQueue(
            Path(data_path),
            lambda x: x.to_dict(),
            Operation.from_dict,
            lock,
            container_type,
        )
        return OfflineOperationProcessor(storage_queue)
    elif mode == Mode.READ_ONLY:
        return ReadOnlyOperationProcessor(container_id, backend)
    else:
        raise ValueError(f"mode should be one of {[m for m in Mode]}")
