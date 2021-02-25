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
import uuid
from typing import Optional

from neptune.alpha.internal.api_clients.neptune_api_client import NeptuneApiClient
from neptune.alpha.internal.operation import Operation
from neptune.alpha.internal.operation_processors.operation_processor import OperationProcessor


class SyncOperationProcessor(OperationProcessor):

    def __init__(self, experiment_uuid: uuid.UUID, api_client: NeptuneApiClient):
        self._experiment_uuid = experiment_uuid
        self._api_client = api_client

    def enqueue_operation(self, op: Operation, wait: bool) -> None:
        # pylint: disable=unused-argument
        errors = self._api_client.execute_operations(self._experiment_uuid, [op])
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
