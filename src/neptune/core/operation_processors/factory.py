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

__all__ = ["get_operation_processor"]

import os
import threading
from queue import Queue
from typing import TYPE_CHECKING

from neptune.core.operation_processors.async_operation_processor import AsyncOperationProcessor
from neptune.core.operation_processors.offline_operation_processor import OfflineOperationProcessor
from neptune.core.operation_processors.operation_processor import OperationProcessor
from neptune.core.operation_processors.read_only_operation_processor import ReadOnlyOperationProcessor
from neptune.core.operation_processors.sync_operation_processor import SyncOperationProcessor
from neptune.core.typing.container_type import ContainerType
from neptune.core.typing.id_formats import CustomId
from neptune.envs import NEPTUNE_ASYNC_BATCH_SIZE
from neptune.objects.mode import Mode

if TYPE_CHECKING:
    from neptune.internal.signals_processing.signals import Signal


def get_operation_processor(
    mode: Mode,
    custom_id: CustomId,
    container_type: ContainerType,
    lock: threading.RLock,
    flush_period: float,
    queue: "Queue[Signal]",
) -> OperationProcessor:
    if mode == Mode.ASYNC:
        return AsyncOperationProcessor(
            custom_id=custom_id,
            container_type=container_type,
            lock=lock,
            sleep_time=flush_period,
            batch_size=int(os.environ.get(NEPTUNE_ASYNC_BATCH_SIZE) or "1000"),
            signal_queue=queue,
        )
    elif mode in {Mode.SYNC, Mode.DEBUG}:
        return SyncOperationProcessor(custom_id=custom_id, container_type=container_type)
    elif mode == Mode.OFFLINE:
        return OfflineOperationProcessor(custom_id=custom_id, container_type=container_type, lock=lock)
    elif mode == Mode.READ_ONLY:
        return ReadOnlyOperationProcessor()
    else:
        raise ValueError(f"mode should be one of {[m for m in Mode]}")
