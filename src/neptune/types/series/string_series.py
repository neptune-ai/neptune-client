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
__all__ = ["StringSeries"]

import time
from itertools import cycle
from typing import (
    TYPE_CHECKING,
    Iterable,
    Optional,
    Sequence,
    TypeVar,
    Union,
)

from neptune.internal.types.stringify_value import StringifyValue
from neptune.internal.utils import (
    is_collection,
    is_stringify_value,
)
from neptune.types.series.series import Series

if TYPE_CHECKING:
    from neptune.types.value_visitor import ValueVisitor

Ret = TypeVar("Ret")

MAX_STRING_SERIES_VALUE_LENGTH = 1000


class StringSeries(Series):
    def __init__(
        self,
        values: Union[Iterable[str], StringifyValue],
        timestamps: Optional[Sequence[float]] = None,
        steps: Optional[Sequence[float]] = None,
    ):
        if is_stringify_value(values):
            values = list(map(str, values.value))

        if not is_collection(values):
            raise TypeError("`values` is not a collection")

        self._truncated = any([len(value) > MAX_STRING_SERIES_VALUE_LENGTH for value in values])
        self._values = [value[:MAX_STRING_SERIES_VALUE_LENGTH] for value in values]

        if steps is None:
            self._steps = cycle([None])
        else:
            assert len(values) == len(steps)
            self._steps = steps

        if timestamps is None:
            self._timestamps = cycle([time.time()])
        else:
            assert len(values) == len(timestamps)
            self._timestamps = timestamps

    @property
    def steps(self):
        return self._steps

    @property
    def timestamps(self):
        return self._timestamps

    def accept(self, visitor: "ValueVisitor[Ret]") -> Ret:
        return visitor.visit_string_series(self)

    @property
    def values(self):
        return self._values

    @property
    def truncated(self):
        """True if any value had to be truncated to `MAX_STRING_SERIES_VALUE_LENGTH`"""
        return self._truncated

    def __str__(self):
        return "StringSeries({})".format(str(self.values))
