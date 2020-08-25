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

import time
from typing import List, TYPE_CHECKING

from neptune.internal.operation import ClearFloatLog, LogFloats
from neptune.variables.series.series import Series

if TYPE_CHECKING:
    from neptune.experiment import Experiment

# pylint: disable=protected-access


class FloatSeries(Series):

    def __init__(self, _experiment: 'Experiment', path: List[str]):
        super().__init__(_experiment, path)
        self._next_step = 0

    def log(self, value: float, step: float = None, timestamp: float = None, wait: bool = False):
        # TODO: Support steps and timestamps
        if not step:
            step = self._next_step
        if not timestamp:
            timestamp = time.time()
        self._next_step = step + 1

        self._experiment._op_processor.enqueue_operation(LogFloats(self._experiment._uuid, self._path, [value]), wait)

    def clear(self, wait: bool = False):
        self._experiment.queue_operation(ClearFloatLog(self._experiment._uuid, self._path), wait)
