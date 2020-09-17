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

from typing import Union, Optional

from neptune.alpha.types.series.float_series import FloatSeries as FloatSeriesVal

from neptune.alpha.internal.utils import verify_type

from neptune.alpha.internal.operation import ClearFloatLog, LogFloats, Operation
from neptune.alpha.attributes.series.series import Series

Val = FloatSeriesVal
Data = Union[float, int]


class FloatSeries(Series[Val, Data]):

    def _get_log_operation_from_value(self, value: Val, step: Optional[float], timestamp: float) -> Operation:
        values = [LogFloats.ValueType(val, step=step, ts=timestamp) for val in value.values]
        return LogFloats(self._path, values)

    def _get_log_operation_from_data(self, data: Data, step: Optional[float], timestamp: float) -> Operation:
        return LogFloats(self._path, [LogFloats.ValueType(data, step, timestamp)])

    def _get_clear_operation(self) -> Operation:
        return ClearFloatLog(self._path)

    def _verify_value_type(self, value) -> None:
        verify_type("value", value, Val)

    def _verify_data_type(self, data) -> None:
        verify_type("data", data, (float, int))
