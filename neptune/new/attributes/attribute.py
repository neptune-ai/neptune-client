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
from typing import List, TYPE_CHECKING

from neptune.new.internal.backends.neptune_backend import NeptuneBackend

from neptune.new.internal.operation import Operation

if TYPE_CHECKING:
    from neptune.new.experiment import Experiment


class Attribute:

    def __init__(self, _experiment: 'Experiment', path: List[str]):
        super().__init__()
        self._experiment = _experiment
        self._path = path

    def __getattr__(self, attr):
        raise AttributeError("{} has no attribute {}.".format(type(self), attr))

    def _enqueue_operation(self, operation: Operation, wait: bool):
        # pylint: disable=protected-access
        self._experiment._op_processor.enqueue_operation(operation, wait)

    @property
    def _backend(self) -> NeptuneBackend:
        # pylint: disable=protected-access
        return self._experiment._backend

    @property
    def _experiment_uuid(self) -> uuid.UUID:
        # pylint: disable=protected-access
        return self._experiment._uuid
