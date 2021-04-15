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
from typing import Union, Optional, Iterable

import click

from neptune.new.attributes.series.fetchable_series import FetchableSeries
from neptune.new.internal.backends.api_model import FloatSeriesValues
from neptune.new.types.series.float_series import FloatSeries as FloatSeriesVal

from neptune.new.internal.utils import verify_type

from neptune.new.internal.operation import ClearFloatLog, LogFloats, Operation, ConfigFloatSeries
from neptune.new.attributes.series.series import Series

Val = FloatSeriesVal
Data = Union[float, int]


class FloatSeries(Series[Val, Data], FetchableSeries[FloatSeriesValues]):

    # pylint: disable=redefined-builtin
    def configure(self,
                  min: Optional[Union[float, int]] = None,
                  max: Optional[Union[float, int]] = None,
                  unit: Optional[str] = None,
                  wait: bool = False) -> None:
        verify_type("min", min, (float, int))
        verify_type("max", max, (float, int))
        verify_type("unit", unit, str)
        with self._run.lock():
            self._enqueue_operation(ConfigFloatSeries(self._path, min, max, unit), wait)

    def _get_log_operation_from_value(self, value: Val, step: Optional[float], timestamp: float) -> Operation:
        values = [LogFloats.ValueType(val, step=step, ts=timestamp) for val in value.values]
        return LogFloats(self._path, values)

    def _get_clear_operation(self) -> Operation:
        return ClearFloatLog(self._path)

    def _get_config_operation_from_value(self, value: Val) -> Optional[Operation]:
        return ConfigFloatSeries(self._path, value.min, value.max, value.unit)

    def _data_to_value(self, values: Iterable, **kwargs) -> Val:
        if kwargs:
            click.echo("Warning: unexpected arguments ({kwargs}) in FloatSeries".format(kwargs=kwargs), err=True)
        return FloatSeriesVal(values)

    def _is_value_type(self, value) -> bool:
        return isinstance(value, FloatSeriesVal)

    def fetch_last(self) -> float:
        # pylint: disable=protected-access
        val = self._backend.get_float_series_attribute(self._run_uuid, self._path)
        return val.last

    def _fetch_values_from_backend(self, offset, limit) -> FloatSeriesValues:
        return self._backend.get_float_series_values(self._run_uuid, self._path, offset, limit)
