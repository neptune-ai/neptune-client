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
__all__ = ["FetchableSeries"]

import abc
from datetime import datetime
from typing import (
    Dict,
    Generic,
    Optional,
    TypeVar,
    Union,
)

from neptune.internal.backends.api_model import (
    FloatSeriesValues,
    StringSeriesValues,
)
from neptune.internal.backends.utils import construct_progress_bar
from neptune.internal.utils.paths import path_to_str
from neptune.typing import ProgressBarType

Row = TypeVar("Row", StringSeriesValues, FloatSeriesValues)


class FetchableSeries(Generic[Row]):
    @abc.abstractmethod
    def _fetch_values_from_backend(self, offset, limit) -> Row:
        pass

    def fetch_values(self, *, include_timestamp: bool = True, progress_bar: Optional[ProgressBarType] = None):
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

        progress_bar = False if len(data) < limit else progress_bar

        path = path_to_str(self._path) if hasattr(self, "_path") else ""
        with construct_progress_bar(progress_bar, f"Fetching {path} values") as bar:
            bar.update(by=len(data), total=val.totalItemCount)  # first fetch before the loop
            while offset < val.totalItemCount:
                batch = self._fetch_values_from_backend(offset, limit)
                data.extend(batch.values)
                offset += limit
                bar.update(by=len(batch.values), total=val.totalItemCount)

        rows = dict((n, make_row(entry)) for (n, entry) in enumerate(data))

        df = pd.DataFrame.from_dict(data=rows, orient="index")
        return df
