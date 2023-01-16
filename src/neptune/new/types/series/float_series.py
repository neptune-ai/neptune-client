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

from typing import (
    TYPE_CHECKING,
    Optional,
    TypeVar,
    Union,
)

from neptune.new.internal.utils import is_collection
from neptune.new.internal.utils.stringify_value import extract_if_stringify_value
from neptune.new.types.series.series import Series

if TYPE_CHECKING:
    from neptune.new.types.value_visitor import ValueVisitor

Ret = TypeVar("Ret")


class FloatSeries(Series):
    def __init__(
        self,
        values,
        min: Optional[Union[float, int]] = None,
        max: Optional[Union[float, int]] = None,
        unit: Optional[str] = None,
    ):
        if not is_collection(values):
            raise TypeError("`values` is not a collection")
        self._values = [float(extract_if_stringify_value(value)) for value in values]
        self._min = min
        self._max = max
        self._unit = unit

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
