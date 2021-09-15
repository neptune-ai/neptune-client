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

from neptune.new.internal.backends.neptune_backend import NeptuneBackend
from neptune.new.internal.operation import Operation
from neptune.new.internal.operation_processors.operation_processor import OperationProcessor


class SyncOperationProcessor(OperationProcessor):

    def __init__(self, run_id: str, backend: NeptuneBackend):
        self._run_id = run_id
        self._backend = backend

    def enqueue_operation(self, op: Operation, wait: bool) -> None:
        # pylint: disable=unused-argument
        errors = self._backend.execute_operations(self._run_id, [op])
        if errors:
            raise errors[0]

    def wait(self):
        pass

    def flush(self):
        pass

    def start(self):
        pass

    def stop(self, seconds: Optional[float] = None):
        pass
