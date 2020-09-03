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

import logging
import threading
import uuid
from typing import Dict, Any, Optional


from neptune.internal.backends.neptune_backend import NeptuneBackend
from neptune_old.utils import is_ipython

_logger = logging.getLogger(__name__)

_checkpoints_lock = threading.Lock()
_checkpoints = dict()


class Notebook:

    def __init__(self,
                 backend: NeptuneBackend,
                 notebook_id: uuid.UUID,
                 notebook_path: str
                 ):
        self._notebook_id = notebook_id
        self._notebook_path = notebook_path
        self._backend = backend

    def create_checkpoint(self) -> uuid.UUID:
        if is_ipython():
            # pylint:disable=bad-option-value,import-outside-toplevel,import-error
            import IPython
            ipython = IPython.core.getipython.get_ipython()
            execution_count = -1
            if ipython.kernel is not None:
                execution_count = ipython.kernel.execution_count
            with _checkpoints_lock:

                if execution_count in _checkpoints:
                    return _checkpoints[execution_count]

                checkpoint_id = self._backend.create_checkpoint(self._notebook_id, self._notebook_path)
                if ipython is not None and ipython.kernel is not None:
                    self._send_checkpoint_created(checkpoint_id=checkpoint_id)
                    _checkpoints[execution_count] = checkpoint_id
                return checkpoint_id

    def _send_checkpoint_created(self, checkpoint_id: uuid.UUID) -> None:
        neptune_comm = self._get_comm()
        neptune_comm.send(data=dict(
            message_type="CHECKPOINT_CREATED",
            data=dict(checkpoint_id=str(checkpoint_id),
                      notebook_id=str(self._notebook_id),
                      notebook_path=str(self._notebook_path))))

    def _get_comm(self) -> Any:
        # pylint: disable=import-error
        from ipykernel.comm import Comm
        return Comm(target_name='neptune_comm')
