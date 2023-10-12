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

__all__ = ["get_operation_processor"]

import os
import threading
from typing import (
    Callable,
    Optional,
)

from neptune.envs import NEPTUNE_ASYNC_BATCH_SIZE
from neptune.internal.backends.neptune_backend import NeptuneBackend
from neptune.internal.container_type import ContainerType
from neptune.internal.id_formats import UniqueId
from neptune.internal.init.parameters import (
    ASYNC_LAG_THRESHOLD,
    ASYNC_NO_PROGRESS_THRESHOLD,
)
from neptune.types.mode import Mode

from .async_operation_processor import AsyncOperationProcessor
from .offline_operation_processor import OfflineOperationProcessor
from .operation_processor import OperationProcessor
from .read_only_operation_processor import ReadOnlyOperationProcessor
from .sync_operation_processor import SyncOperationProcessor


def get_operation_processor(
    mode: Mode,
    container_id: UniqueId,
    container_type: ContainerType,
    backend: NeptuneBackend,
    lock: threading.RLock,
    flush_period: float,
    async_lag_callback: Optional[Callable[[], None]] = None,
    async_lag_threshold: float = ASYNC_LAG_THRESHOLD,
    async_no_progress_callback: Optional[Callable[[], None]] = None,
    async_no_progress_threshold: float = ASYNC_NO_PROGRESS_THRESHOLD,
) -> OperationProcessor:
    if mode == Mode.ASYNC:
        return AsyncOperationProcessor(
            container_id=container_id,
            container_type=container_type,
            backend=backend,
            lock=lock,
            sleep_time=flush_period,
            batch_size=int(os.environ.get(NEPTUNE_ASYNC_BATCH_SIZE) or "1000"),
            async_lag_callback=async_lag_callback,
            async_lag_threshold=async_lag_threshold,
            async_no_progress_callback=async_no_progress_callback,
            async_no_progress_threshold=async_no_progress_threshold,
        )
    elif mode == Mode.SYNC:
        return SyncOperationProcessor(container_id, container_type, backend)
    elif mode == Mode.DEBUG:
        return SyncOperationProcessor(container_id, container_type, backend)
    elif mode == Mode.OFFLINE:
        # the object was returned by mocked backend and has some random ID.
        return OfflineOperationProcessor(container_id, container_type, lock)
    elif mode == Mode.READ_ONLY:
        return ReadOnlyOperationProcessor()
    else:
        raise ValueError(f"mode should be one of {[m for m in Mode]}")
