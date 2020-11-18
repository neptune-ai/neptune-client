#
# Copyright (c) 2020, Neptune Labs Sp. z o.o.
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
from typing import Optional

from neptune.alpha.internal.containers.storage_queue import StorageQueue
from neptune.alpha.internal.operation import Operation, VersionedOperation
from neptune.alpha.internal.operation_processors.operation_processor import OperationProcessor


class OfflineOperationProcessor(OperationProcessor):

    def __init__(self, queue: StorageQueue[VersionedOperation]):
        self._queue = queue
        self._last_version = 0

    def enqueue_operation(self, op: Operation, wait: bool) -> None:
        # pylint: disable=unused-argument
        self._last_version += 1
        self._queue.put(VersionedOperation(op, self._last_version))

    def wait(self):
        pass

    def start(self):
        pass

    def stop(self, seconds: Optional[float] = None):
        pass
