#
# Copyright (c) 2020, Neptune Labs Sp. z o.o.
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
from typing import List, TYPE_CHECKING

from neptune.types.atoms.float import Float
from neptune.types.atoms.string import String
from neptune.types.series.float_series import FloatSeries
from neptune.types.series.string_series import StringSeries
from neptune.types.sets.string_set import StringSet
from neptune.types.value_visitor import ValueVisitor
from neptune.variable import Variable, FloatVariable, StringVariable, FloatSeriesVariable, StringSeriesVariable, \
    StringSetVariable

if TYPE_CHECKING:
    from neptune import Experiment


class VariableSetterValueVisitor(ValueVisitor[Variable]):

    def __init__(self, _experiment: 'Experiment', path: List[str], wait: bool = False):
        self._experiment = _experiment
        self._path = path
        self._wait = wait

    def visit_float(self, value: Float) -> Variable:
        var = FloatVariable(self._experiment, self._path)
        var.assign(value.value, self._wait)
        return var

    def visit_string(self, value: String) -> Variable:
        var = StringVariable(self._experiment, self._path)
        var.assign(value.value, self._wait)
        return var

    def visit_float_series(self, value: FloatSeries) -> Variable:
        var = FloatSeriesVariable(self._experiment, self._path)
        var.clear()
        # TODO: Avoid loop
        for val in value.values[:-1]:
            var.log(val)
        var.log(value.values[-1], self._wait)
        return var

    def visit_string_series(self, value: StringSeries) -> Variable:
        var = StringSeriesVariable(self._experiment, self._path)
        var.clear()
        # TODO: Avoid loop
        for val in value.values[:-1]:
            var.log(val)
        var.log(value.values[-1], self._wait)
        return var

    def visit_string_set(self, value: StringSet) -> Variable:
        var = StringSetVariable(self._experiment, self._path)
        var.clear()
        var.insert(value.values, self._wait)
        return var
