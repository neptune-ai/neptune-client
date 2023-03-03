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
    Iterable,
    List,
    Optional,
    SupportsFloat,
    TypeVar,
    Union,
)

from neptune.internal.types.stringify_value import StringifyValue
from neptune.internal.utils import is_collection
from neptune.types.series.series import Series

if TYPE_CHECKING:
    from neptune.types.value_visitor import ValueVisitor

Ret = TypeVar("Ret")


class FloatSeries(Series):
    def __init__(
        self,
        values: Union[Iterable[SupportsFloat], StringifyValue[Iterable[SupportsFloat]]],
        min: Optional[Union[float, int]] = None,
        max: Optional[Union[float, int]] = None,
        unit: Optional[str] = None,
    ):
        if isinstance(values, StringifyValue):
            values = values.value

        if not is_collection(values):
            raise TypeError("`values` is not a collection")

        self._values = [float(value) for value in values]
        self._min = float(min) if min is not None else None
        self._max = float(max) if max is not None else None
        self._unit = unit

    def accept(self, visitor: "ValueVisitor[Ret]") -> Ret:
        return visitor.visit_float_series(self)

    @property
    def values(self) -> List[float]:
        return self._values

    @property
    def min(self) -> Optional[float]:
        return self._min

    @property
    def max(self) -> Optional[float]:
        return self._max

    @property
    def unit(self) -> Optional[str]:
        return self._unit

    def __str__(self) -> str:
        return "FloatSeries({})".format(str(self.values))
