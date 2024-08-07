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
from functools import partial
from typing import (
    Dict,
    Generic,
    Optional,
    TypeVar,
    Union,
)

from neptune.api.fetching_series_values import fetch_series_values
from neptune.api.models import (
    FloatPointValue,
    StringPointValue,
)
from neptune.internal.utils.paths import path_to_str
from neptune.typing import ProgressBarType

Row = TypeVar("Row", StringPointValue, FloatPointValue)


def make_row(entry: Row, include_timestamp: bool = True) -> Dict[str, Union[str, float, datetime]]:
    row: Dict[str, Union[str, float, datetime]] = {
        "step": entry.step,
        "value": entry.value,
    }

    if include_timestamp:
        row["timestamp"] = entry.timestamp

    return row


class FetchableSeries(Generic[Row]):
    @abc.abstractmethod
    def _fetch_values_from_backend(
        self, limit: int, from_step: Optional[float] = None, include_inherited: bool = True
    ) -> Row: ...

    def fetch_values(
        self,
        *,
        include_timestamp: bool = True,
        progress_bar: Optional[ProgressBarType] = None,
        include_inherited: bool = True,
    ):
        import pandas as pd

        path = path_to_str(self._path) if hasattr(self, "_path") else ""
        data = fetch_series_values(
            getter=partial(self._fetch_values_from_backend, include_inherited=include_inherited),
            path=path,
            progress_bar=progress_bar,
        )

        rows = dict((n, make_row(entry=entry, include_timestamp=include_timestamp)) for (n, entry) in enumerate(data))

        df = pd.DataFrame.from_dict(data=rows, orient="index")
        return df
