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
    TYPE_CHECKING,
    Dict,
    Generator,
    Generic,
    TypeVar,
    Union,
)

from tqdm import tqdm

from neptune.internal.backends.api_model import (
    FloatSeriesValues,
    StringSeriesValues,
)

if TYPE_CHECKING:
    from pandas import DataFrame

Row = TypeVar("Row", StringSeriesValues, FloatSeriesValues)


def make_row(entry: Row, include_timestamp: bool) -> Dict[str, Union[str, float, datetime]]:
    row: Dict[str, Union[str, float, datetime]] = {
        "step": entry.step,
        "value": entry.value,
    }

    if include_timestamp:
        row["timestamp"] = datetime.fromtimestamp(entry.timestampMillis / 1000)

    return row


class FetchableSeries(Generic[Row]):
    @abc.abstractmethod
    def _fetch_values_from_backend(self, offset, limit) -> Row:
        pass

    # Limit needs to be set to 10000 for float series, for string series it needs to be set to 5k
    def _fetch_row_values(
        self, limit: int = 200_000, include_timestamp: bool = True
    ) -> Generator[Dict[str, Union[str, float, datetime]], None, None]:
        offset = 0
        while True:
            batch = self._fetch_values_from_backend(offset, limit)
            yield from map(lambda entry: make_row(entry, include_timestamp=include_timestamp), batch["values"])
            offset += limit
            if offset > batch["totalItemCount"]:
                break

    def fetch_values(self, *, include_timestamp: bool = True) -> "DataFrame":
        try:
            import pandas as pd
        except ImportError:
            raise ImportError(
                "Fetching series values requires pandas to be installed. "
                "Please install it with `pip install pandas`."
            )

        data = {
            index: entry
            for index, entry in enumerate(
                tqdm(self._fetch_row_values(include_timestamp=include_timestamp), total=10_000_000)
            )
        }

        return pd.DataFrame.from_dict(data, orient="index")
