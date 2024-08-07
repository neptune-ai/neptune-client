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
__all__ = ("fetch_series_values",)

from typing import (
    Any,
    Callable,
    Iterator,
    Optional,
    TypeVar,
)

from neptune.api.models import (
    FloatPointValue,
    StringPointValue,
)
from neptune.internal.backends.utils import construct_progress_bar
from neptune.typing import ProgressBarType

PointValue = TypeVar("PointValue", StringPointValue, FloatPointValue)


def fetch_series_values(
    getter: Callable[..., Any], path: str, step_size: int = 1000, progress_bar: Optional[ProgressBarType] = None
) -> Iterator[PointValue]:
    first_batch = getter(from_step=None, limit=1)
    data_count = 0
    total = first_batch.total
    last_step_value = (first_batch.values[-1].step - 1) if first_batch.values else None
    progress_bar = False if total < step_size else progress_bar

    if total <= 1:
        yield from first_batch.values
        return

    with construct_progress_bar(progress_bar, f"Fetching {path} values") as bar:
        bar.update(by=data_count, total=total)

        while data_count < first_batch.total:
            batch = getter(from_step=last_step_value, limit=step_size)

            bar.update(by=len(batch.values), total=total)

            yield from batch.values

            last_step_value = batch.values[-1].step if batch.values else None
            data_count += len(batch.values)
