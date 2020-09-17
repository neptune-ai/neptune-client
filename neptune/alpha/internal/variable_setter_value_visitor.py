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

from neptune.alpha.types.atoms.float import Float
from neptune.alpha.types.atoms.string import String
from neptune.alpha.types.atoms.file import File
from neptune.alpha.types.series.float_series import FloatSeries
from neptune.alpha.types.series.image_series import ImageSeries
from neptune.alpha.types.series.string_series import StringSeries
from neptune.alpha.types.sets.string_set import StringSet
from neptune.alpha.types.value_visitor import ValueVisitor
from neptune.alpha.variables.atoms.float import Float as FloatVar
from neptune.alpha.variables.atoms.string import String as StringVar
from neptune.alpha.variables.atoms.file import File as FileVar
from neptune.alpha.variables.series.float_series import FloatSeries as FloatSeriesVar
from neptune.alpha.variables.series.string_series import StringSeries as StringSeriesVar
from neptune.alpha.variables.series.image_series import ImageSeries as ImageSeriesVar
from neptune.alpha.variables.sets.string_set import StringSet as StringSetVar
from neptune.alpha.variables.variable import Variable

if TYPE_CHECKING:
    from neptune.alpha import Experiment


class VariableSetterValueVisitor(ValueVisitor[Variable]):

    def __init__(self, _experiment: 'Experiment', path: List[str], wait: bool = False):
        self._experiment = _experiment
        self._path = path
        self._wait = wait

    def visit_float(self, value: Float) -> Variable:
        var = FloatVar(self._experiment, self._path)
        var.assign(value, self._wait)
        return var

    def visit_string(self, value: String) -> Variable:
        var = StringVar(self._experiment, self._path)
        var.assign(value, self._wait)
        return var

    def visit_file(self, value: File) -> Variable:
        var = FileVar(self._experiment, self._path)
        var.assign(value, self._wait)
        return var

    def visit_float_series(self, value: FloatSeries) -> Variable:
        var = FloatSeriesVar(self._experiment, self._path)
        var.assign(value, self._wait)
        return var

    def visit_string_series(self, value: StringSeries) -> Variable:
        var = StringSeriesVar(self._experiment, self._path)
        var.assign(value, self._wait)
        return var

    def visit_image_series(self, value: ImageSeries) -> Variable:
        var = ImageSeriesVar(self._experiment, self._path)
        var.assign(value, self._wait)
        return var

    def visit_string_set(self, value: StringSet) -> Variable:
        var = StringSetVar(self._experiment, self._path)
        var.assign(value, self._wait)
        return var
