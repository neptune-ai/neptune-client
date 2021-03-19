#
# Copyright (c) 2021, Neptune Labs Sp. z o.o.
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
import abc
from datetime import datetime
from typing import TypeVar, Generic, Union, Dict

from neptune.new.internal.backends.api_model import FloatSeriesValues, StringSeriesValues

Row = TypeVar('Row', StringSeriesValues, FloatSeriesValues)


class FetchableSeries(Generic[Row]):

    @abc.abstractmethod
    def _fetch_values_from_backend(self, offset, limit) -> Row:
        pass

    def fetch_values(self, include_timestamp=True):
        # pylint: disable=import-outside-toplevel
        import pandas as pd
        limit = 1000
        val = self._fetch_values_from_backend(0, limit)
        data = val.values
        offset = limit

        def make_row(entry: Row) -> Dict[str, Union[str, float, datetime]]:
            row: Dict[str, Union[str, float, datetime]] = dict()
            row["step"] = entry.step
            row["value"] = entry.value
            if include_timestamp:
                row["timestamp"] = datetime.fromtimestamp(entry.timestampMillis / 1000)
            return row

        while offset < val.totalItemCount:
            batch = self._fetch_values_from_backend(offset, limit)
            data.extend(batch.values)
            offset += limit

        rows = dict((n, make_row(entry)) for (n, entry) in enumerate(data))

        df = pd.DataFrame.from_dict(data=rows, orient='index')
        return df
