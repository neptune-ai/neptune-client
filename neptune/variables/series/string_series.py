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

from typing import Optional

from neptune.internal.utils import verify_type

from neptune.types.series.string_series import StringSeries as StringSeriesVal

from neptune.internal.operation import LogStrings, ClearStringLog, Operation
from neptune.variables.series.series import Series

Val = StringSeriesVal
Data = str


class StringSeries(Series[Val, Data]):

    def _get_log_operation_from_value(self, value: Val, step: Optional[float], timestamp: float) -> Operation:
        values = [LogStrings.ValueType(val, step=step, ts=timestamp) for val in value.values]
        return LogStrings(self._path, values)

    def _get_log_operation_from_data(self, data: Data, step: Optional[float], timestamp: float) -> Operation:
        return LogStrings(self._path, [LogStrings.ValueType(data, step, timestamp)])

    def _get_clear_operation(self) -> Operation:
        return ClearStringLog(self._path)

    def _verify_value_type(self, value) -> None:
        verify_type("value", value, Val)

    def _verify_data_type(self, data) -> None:
        verify_type("data", data, Data)
