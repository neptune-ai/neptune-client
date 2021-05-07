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
from typing import Optional, Iterable, List, TYPE_CHECKING

import click

from neptune.new.attributes.series.fetchable_series import FetchableSeries
from neptune.new.internal.backends.api_model import StringSeriesValues
from neptune.new.internal.utils.paths import path_to_str
from neptune.new.types.series.string_series import StringSeries as StringSeriesVal

from neptune.new.internal.operation import LogStrings, ClearStringLog, Operation
from neptune.new.attributes.series.series import Series

if TYPE_CHECKING:
    from neptune.new.run import Run

Val = StringSeriesVal
Data = str

MAX_STRING_SERIES_VALUE_LENGTH = 1000


class StringSeries(Series[Val, Data], FetchableSeries[StringSeriesValues]):

    def __init__(self, run: 'Run', path: List[str]):
        super().__init__(run, path)
        self._value_truncation_occurred = False

    def _get_log_operation_from_value(self, value: Val, step: Optional[float], timestamp: float) -> Operation:
        values = [v[:MAX_STRING_SERIES_VALUE_LENGTH] for v in value.values]
        if not self._value_truncation_occurred \
                and any([len(v) > MAX_STRING_SERIES_VALUE_LENGTH for v in value.values]):
            # the first truncation
            self._value_truncation_occurred = True
            click.echo(f"Warning: string series '{ path_to_str(self._path)}' value was "
                       f"longer than {MAX_STRING_SERIES_VALUE_LENGTH} characters and was truncated. "
                       f"This warning is printed only once per series.", err=True)

        values = [LogStrings.ValueType(val, step=step, ts=timestamp) for val in values]
        return LogStrings(self._path, values)

    def _get_clear_operation(self) -> Operation:
        return ClearStringLog(self._path)

    def _data_to_value(self, values: Iterable, **kwargs) -> Val:
        if kwargs:
            click.echo("Warning: unexpected arguments ({kwargs}) in StringSeries".format(kwargs=kwargs), err=True)
        return StringSeriesVal(values)

    def _is_value_type(self, value) -> bool:
        return isinstance(value, StringSeriesVal)

    def fetch_last(self) -> str:
        # pylint: disable=protected-access
        val = self._backend.get_string_series_attribute(self._run_uuid, self._path)
        return val.last

    def _fetch_values_from_backend(self, offset, limit) -> StringSeriesValues:
        return self._backend.get_string_series_values(self._run_uuid, self._path, offset, limit)
