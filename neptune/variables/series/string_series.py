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

from neptune.internal.utils import verify_type

from neptune.types.series.string_series import StringSeries as StringSeriesVal

from neptune.internal.operation import LogStrings, ClearStringLog
from neptune.variables.series.series import Series

# pylint: disable=protected-access


class StringSeries(Series):

    def assign(self, value: StringSeriesVal, wait: bool = False):
        verify_type("value", value, StringSeriesVal)
        with self._experiment.lock():
            self.clear()
            # TODO: Avoid loop
            for val in value.values[:-1]:
                self.log(val)
            self.log(value.values[-1], wait)

    def log(self, value: str, step: float = None, timestamp: float = None, wait: bool = False):
        # pylint: disable=unused-argument
        verify_type("value", value, str)
        with self._experiment.lock():
            if not timestamp:
                timestamp = time.time()
            self._experiment._op_processor.enqueue_operation(
                LogStrings(self._experiment._uuid, self._path, [value]), wait)

    def clear(self, wait: bool = False):
        with self._experiment.lock():
            self._experiment._op_processor.enqueue_operation(ClearStringLog(self._experiment._uuid, self._path), wait)
