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

MAX_FETCH_LIMIT = 1000


class FetchableSeries(Generic[Row]):
    @abc.abstractmethod
    def _fetch_values_from_backend(self, offset, limit) -> Row:
        pass

    def fetch_values(
        self,
        *,
        include_timestamp: bool = True,
        progress_bar: Optional[ProgressBarType] = None,
        offset: Optional[int] = None,
        limit: Optional[int] = None,
    ):
        import pandas as pd

        def make_row(entry: Row) -> Dict[str, Union[str, float, datetime]]:
            row: Dict[str, Union[str, float, datetime]] = dict()
            row["step"] = entry.step
            row["value"] = entry.value
            if include_timestamp:
                row["timestamp"] = datetime.fromtimestamp(entry.timestampMillis / 1000)
            return row

        if offset is None:
            offset = 0

        if limit is not None:
            fetch_chunk_size = min(limit, MAX_FETCH_LIMIT)
        else:
            fetch_chunk_size = MAX_FETCH_LIMIT

        val = self._fetch_values_from_backend(offset=offset, limit=fetch_chunk_size)

        if limit is None:
            to_load = val.totalItemCount - offset
        else:
            to_load = min(val.totalItemCount - offset, limit)

        data = val.values
        offset += len(data)

        # dont display progress bar if all values are fetched in one go
        left_to_fetch = to_load - len(data)
        if left_to_fetch == 0:
            progress_bar = False

        path = path_to_str(self._path) if hasattr(self, "_path") else ""
        with construct_progress_bar(progress_bar, f"Fetching {path} values") as bar:
            bar.update(by=len(data), total=to_load)  # first fetch before the loop
            while left_to_fetch != 0:
                fetch_chunk_size = min(left_to_fetch, MAX_FETCH_LIMIT)
                batch = self._fetch_values_from_backend(offset, fetch_chunk_size)

                data.extend(batch.values)
                offset += len(batch.values)
                left_to_fetch -= len(batch.values)
                bar.update(by=len(batch.values), total=to_load)

        rows = dict((n, make_row(entry)) for (n, entry) in enumerate(data))

        df = pd.DataFrame.from_dict(data=rows, orient="index")
        return df
