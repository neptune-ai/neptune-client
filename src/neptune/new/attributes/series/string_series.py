#
# Copyright (c) 2022, Neptune Labs Sp. z o.o.
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
from typing import TYPE_CHECKING, Iterable, List, Optional

from neptune.new.attributes.series.fetchable_series import FetchableSeries
from neptune.new.attributes.series.series import Series
from neptune.new.internal.backends.api_model import StringSeriesValues
from neptune.new.internal.operation import ClearStringLog, LogStrings, Operation
from neptune.new.internal.utils.iteration import get_batches
from neptune.new.internal.utils.logger import logger
from neptune.new.internal.utils.paths import path_to_str
from neptune.new.types.series.string_series import StringSeries as StringSeriesVal

if TYPE_CHECKING:
    from neptune.new.metadata_containers import MetadataContainer

Val = StringSeriesVal
Data = str

MAX_STRING_SERIES_VALUE_LENGTH = 1000


class StringSeries(Series[Val, Data], FetchableSeries[StringSeriesValues]):
    def __init__(self, container: "MetadataContainer", path: List[str]):
        super().__init__(container, path)
        self._value_truncation_occurred = False

    def _get_log_operations_from_value(self, value: Val, step: Optional[float], timestamp: float) -> List[Operation]:
        values = [v[:MAX_STRING_SERIES_VALUE_LENGTH] for v in value.values]
        if not self._value_truncation_occurred and any([len(v) > MAX_STRING_SERIES_VALUE_LENGTH for v in value.values]):
            # the first truncation
            self._value_truncation_occurred = True
            logger.warning(
                "Warning: string series '%s' value was"
                " longer than %s characters and was truncated."
                " This warning is printed only once per series.",
                path_to_str(self._path),
                MAX_STRING_SERIES_VALUE_LENGTH,
            )

        values = [LogStrings.ValueType(val, step=step, ts=timestamp) for val in values]
        return [LogStrings(self._path, chunk) for chunk in get_batches(values, batch_size=10)]

    def _get_clear_operation(self) -> Operation:
        return ClearStringLog(self._path)

    def _data_to_value(self, values: Iterable, **kwargs) -> Val:
        if kwargs:
            logger.warning("Warning: unexpected arguments (%s) in StringSeries", kwargs)
        return StringSeriesVal(values)

    def _is_value_type(self, value) -> bool:
        return isinstance(value, StringSeriesVal)

    def fetch_last(self) -> str:
        # pylint: disable=protected-access
        val = self._backend.get_string_series_attribute(self._container_id, self._container_type, self._path)
        return val.last

    def _fetch_values_from_backend(self, offset, limit) -> StringSeriesValues:
        return self._backend.get_string_series_values(
            self._container_id, self._container_type, self._path, offset, limit
        )
