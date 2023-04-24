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
__all__ = ["FloatSeries"]

import time
from itertools import cycle
from typing import (
    TYPE_CHECKING,
    Optional,
    Sequence,
    TypeVar,
    Union,
)

from neptune.internal.types.stringify_value import extract_if_stringify_value
from neptune.internal.utils import is_collection
from neptune.types.series.series import Series

if TYPE_CHECKING:
    from neptune.types.value_visitor import ValueVisitor

Ret = TypeVar("Ret")


class FloatSeries(Series):
    def __init__(
        self,
        values,
        min: Optional[Union[float, int]] = None,
        max: Optional[Union[float, int]] = None,
        unit: Optional[str] = None,
        timestamps: Optional[Sequence[float]] = None,
        steps: Optional[Sequence[float]] = None,
    ):
        values = extract_if_stringify_value(values)

        if not is_collection(values):
            raise TypeError("`values` is not a collection")

        self._values = [float(value) for value in values]
        self._min = min
        self._max = max
        self._unit = unit

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
        return visitor.visit_float_series(self)

    @property
    def values(self):
        return self._values

    @property
    def min(self):
        return self._min

    @property
    def max(self):
        return self._max

    @property
    def unit(self):
        return self._unit

    def __str__(self):
        return "FloatSeries({})".format(str(self.values))
