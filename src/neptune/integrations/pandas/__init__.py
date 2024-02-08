#
# Copyright (c) 2024, Neptune Labs Sp. z o.o.
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
__all__ = [
    'to_pandas'
]

from typing import (
    Iterator,
    TYPE_CHECKING, Tuple, Dict, Optional, Union,
)
from datetime import datetime

if TYPE_CHECKING:
    from neptune.internal.backends.api_model import LeaderboardEntry


try:
    import pandas as pd
except ModuleNotFoundError as e:
    if e.name == "neptune_optuna":
        from neptune.exceptions import NeptuneIntegrationNotInstalledException

        raise NeptuneIntegrationNotInstalledException(
            integration_package_name="pandas", framework_name="pandas"
        ) from None
    else:
        raise


def make_attribute_value(attr):
    # TODO: Implement this
    return None


def make_row(entry: LeaderboardEntry, ) -> Dict[str, Optional[Union[str, float, datetime]]]:
    row: Dict[str, Optional[Union[str, float, datetime]]] = dict()
    for attr in entry.attributes:
        value = make_attribute_value(attr)
        if value is not None:
            row[attr.path] = value
    return row


def sort_key(attr: str) -> Tuple[int, str]:
    domain = attr.split("/")[0]
    if domain == "sys":
        return 0, attr
    if domain == "monitoring":
        return 2, attr
    return 1, attr


def to_pandas(entries: Iterator["LeaderboardEntry"]) -> pd.DataFrame:
    rows = dict((n, make_row(entry)) for (n, entry) in enumerate(entries))

    df = pd.DataFrame.from_dict(data=rows, orient="index")
    df = df.reindex(sorted(df.columns, key=sort_key), axis="columns")

    return df
