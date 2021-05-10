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
import logging
import uuid
from typing import Optional

from neptune.new.internal.backends.neptune_backend import NeptuneBackend
from neptune.new.internal.operation import Operation
from neptune.new.internal.operation_processors.operation_processor import OperationProcessor

_logger = logging.getLogger(__name__)


class ReadOnlyOperationProcessor(OperationProcessor):

    def __init__(self, run_uuid: uuid.UUID, backend: NeptuneBackend):
        self._run_uuid = run_uuid
        self._backend = backend
        self._warning_emitted = False

    def enqueue_operation(self, op: Operation, wait: bool) -> None:
        if not self._warning_emitted:
            self._warning_emitted = True
            _logger.warning("Client in read-only mode, nothing will be saved to server.")

    def wait(self):
        pass

    def flush(self):
        pass

    def start(self):
        pass

    def stop(self, seconds: Optional[float] = None):
        pass
