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
import warnings
from typing import List, Optional, Dict

from neptune.new.exceptions import NeptuneException
from neptune.new.internal.backends.api_model import ApiRun
from neptune.new.internal.backends.hosted_neptune_backend import HostedNeptuneBackend
from neptune.new.internal.backends.utils import with_api_exceptions_handler
from neptune.new.internal.credentials import Credentials
from neptune.new.internal.operation import Operation
from neptune.new.internal.websockets.websockets_factory import WebsocketsFactory
from neptune.new.types.atoms import GitRef

_logger = logging.getLogger(__name__)


class ReadOnlyNeptuneBackend(HostedNeptuneBackend):
    def __init__(self, credentials: Credentials, proxies: Optional[Dict[str, str]] = None):
        super().__init__(credentials, proxies)
        self._warning_emitted = False

    def websockets_factory(self, project_uuid: uuid.UUID, run_uuid: uuid.UUID) -> Optional[WebsocketsFactory]:
        return None

    @with_api_exceptions_handler
    def create_run(self,
                   project_uuid: uuid.UUID,
                   git_ref: Optional[GitRef] = None,
                   custom_run_id: Optional[str] = None,
                   notebook_id: Optional[uuid.UUID] = None,
                   checkpoint_id: Optional[uuid.UUID] = None
                   ) -> ApiRun:
        raise NotImplementedError("You can't create new runs in read-only mode")

    def create_checkpoint(self, notebook_id: uuid.UUID, jupyter_path: str) -> Optional[uuid.UUID]:
        ...

    def ping_run(self, run_uuid: uuid.UUID):
        ...

    def execute_operations(self, run_uuid: uuid.UUID, operations: List[Operation]) -> List[NeptuneException]:
        if not self._warning_emitted:
            self._warning_emitted = True
            _logger.warning("Client in read-only mode, nothing will be saved to server.")
        return []
