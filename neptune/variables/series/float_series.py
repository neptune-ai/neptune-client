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
from typing import Union, Optional

from neptune.types.series.float_series import FloatSeries as FloatSeriesVal

from neptune.internal.utils import verify_type

from neptune.internal.operation import ClearFloatLog, LogFloats, LogSeriesValue
from neptune.variables.series.series import Series


class FloatSeries(Series):

    def assign(self, value: FloatSeriesVal, wait: bool = False):
        verify_type("value", value, FloatSeriesVal)

        with self._experiment.lock():
            clear_op = ClearFloatLog(self._experiment_uuid, self._path)
            if not value.values:
                self._enqueue_operation(clear_op, wait=wait)
            else:
                self._enqueue_operation(clear_op, wait=False)
                ts = time.time()
                values = [LogSeriesValue[float](val, step=None, ts=ts) for val in value.values]
                self._enqueue_operation(LogFloats(self._experiment_uuid, self._path, values), wait=wait)

    def log(self,
            value: Union[float, int],
            step: Optional[float] = None,
            timestamp: Optional[float] = None,
            wait: bool = False):
        verify_type("value", value, (float, int))

        if not timestamp:
            timestamp = time.time()

        with self._experiment.lock():
            self._enqueue_operation(
                LogFloats(self._experiment_uuid, self._path, [LogSeriesValue[float](value, step, timestamp)]),
                wait
            )

    def clear(self, wait: bool = False):
        with self._experiment.lock():
            self._enqueue_operation(ClearFloatLog(self._experiment_uuid, self._path), wait)
