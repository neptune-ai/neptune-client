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
from datetime import datetime
from typing import Optional, Iterable, Dict, Union

from neptune.new.internal.backends.api_model import StringPointValue
from neptune.new.types.series.string_series import StringSeries as StringSeriesVal

from neptune.new.internal.operation import LogStrings, ClearStringLog, Operation
from neptune.new.attributes.series.series import Series

Val = StringSeriesVal
Data = str


class StringSeries(Series[Val, Data]):

    def _get_log_operation_from_value(self, value: Val, step: Optional[float], timestamp: float) -> Operation:
        values = [LogStrings.ValueType(val, step=step, ts=timestamp) for val in value.values]
        return LogStrings(self._path, values)

    def _get_log_operation_from_data(self,
                                     data_list: Iterable[Data],
                                     step: Optional[float],
                                     timestamp: float) -> Operation:
        return LogStrings(self._path, [LogStrings.ValueType(data, step, timestamp) for data in data_list])

    def _get_clear_operation(self) -> Operation:
        return ClearStringLog(self._path)

    def _data_to_value(self, value: Iterable) -> Val:
        return StringSeriesVal(value)

    def _is_value_type(self, value) -> bool:
        return isinstance(value, StringSeriesVal)

    def get_last(self, wait=True) -> str:
        # pylint: disable=protected-access
        if wait:
            self._experiment.wait()
        val = self._backend.get_string_series_attribute(self._experiment_uuid, self._path)
        return val.last

    def fetch_values(self, include_timestamp=True):
        # pylint: disable=import-outside-toplevel
        import pandas as pd
        limit = 1000
        val = self._backend.get_string_series_values(self._experiment_uuid, self._path, 0, limit)
        data = val.values
        offset = limit

        def make_row(entry: StringPointValue) -> Dict[str, Optional[Union[str, float, datetime]]]:
            row: Dict[str, Union[str, float, datetime]] = dict()
            row["step"] = entry.step
            row["value"] = entry.value
            if include_timestamp:
                row["timestamp"] = datetime.fromtimestamp(entry.timestampMillis / 1000)
            return row

        while offset < val.totalItemCount:
            batch = self._backend.get_string_series_values(self._experiment_uuid, self._path, offset, limit)
            data.extend(batch.values)
            offset += limit

        rows = dict((n, make_row(entry)) for (n, entry) in enumerate(data))

        df = pd.DataFrame.from_dict(data=rows, orient='index')
        return df
