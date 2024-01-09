#
# Copyright (c) 2019, Neptune Labs Sp. z o.o.
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

from neptune.common.utils import is_ipython
from neptune.legacy.internal.notebooks.comm import send_checkpoint_created

_logger = logging.getLogger(__name__)

_checkpoints_lock = threading.Lock()
_checkpoints = dict()


def create_checkpoint(backend, notebook_id, notebook_path):
    if is_ipython():
        import IPython

        ipython = IPython.core.getipython.get_ipython()
        execution_count = -1
        if ipython.kernel is not None:
            execution_count = ipython.kernel.execution_count
        with _checkpoints_lock:

            if execution_count in _checkpoints:
                return _checkpoints[execution_count]

            checkpoint = backend.create_checkpoint(notebook_id, notebook_path)
            if ipython is not None and ipython.kernel is not None:
                send_checkpoint_created(
                    notebook_id=notebook_id,
                    notebook_path=notebook_path,
                    checkpoint_id=checkpoint.id,
                )
                _checkpoints[execution_count] = checkpoint
            return checkpoint
