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
from typing import Union

from neptune.types.series.float_series import FloatSeries as FloatSeriesVal

from neptune.internal.utils import verify_type

from neptune.internal.operation import ClearFloatLog, LogFloats
from neptune.variables.series.series import Series

# pylint: disable=protected-access


class FloatSeries(Series):

    def assign(self, value: FloatSeriesVal, wait: bool = False):
        verify_type("value", value, FloatSeriesVal)
        with self._experiment.lock():
            self.clear()
            # TODO: Avoid loop
            for val in value.values[:-1]:
                self.log(val)
            self.log(value.values[-1], wait)

    def log(self, value: Union[float, int], step: float = None, timestamp: float = None, wait: bool = False):
        # pylint: disable=unused-argument
        verify_type("value", value, (float, int))
        with self._experiment.lock():
            # TODO: Support steps and timestamps
            if not timestamp:
                timestamp = time.time()
            self._experiment._op_processor.enqueue_operation(
                LogFloats(self._experiment._uuid, self._path, [value]), wait)

    def clear(self, wait: bool = False):
        with self._experiment.lock():
            self._experiment._op_processor.enqueue_operation(ClearFloatLog(self._experiment._uuid, self._path), wait)
