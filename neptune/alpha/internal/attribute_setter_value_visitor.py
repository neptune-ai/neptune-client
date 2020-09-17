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
from neptune.alpha.attributes.atoms.float import Float as FloatAttr
from neptune.alpha.attributes.atoms.string import String as StringAttr
from neptune.alpha.attributes.atoms.file import File as FileAttr
from neptune.alpha.attributes.series.float_series import FloatSeries as FloatSeriesAttr
from neptune.alpha.attributes.series.string_series import StringSeries as StringSeriesAttr
from neptune.alpha.attributes.series.image_series import ImageSeries as ImageSeriesAttr
from neptune.alpha.attributes.sets.string_set import StringSet as StringSetAttr
from neptune.alpha.attributes.attribute import Attribute

if TYPE_CHECKING:
    from neptune.alpha import Experiment


class AttributeSetterValueVisitor(ValueVisitor[Attribute]):

    def __init__(self, _experiment: 'Experiment', path: List[str], wait: bool = False):
        self._experiment = _experiment
        self._path = path
        self._wait = wait

    def visit_float(self, value: Float) -> Attribute:
        attr = FloatAttr(self._experiment, self._path)
        attr.assign(value, self._wait)
        return attr

    def visit_string(self, value: String) -> Attribute:
        attr = StringAttr(self._experiment, self._path)
        attr.assign(value, self._wait)
        return attr

    def visit_file(self, value: File) -> Attribute:
        attr = FileAttr(self._experiment, self._path)
        attr.assign(value, self._wait)
        return attr

    def visit_float_series(self, value: FloatSeries) -> Attribute:
        attr = FloatSeriesAttr(self._experiment, self._path)
        attr.assign(value, self._wait)
        return attr

    def visit_string_series(self, value: StringSeries) -> Attribute:
        attr = StringSeriesAttr(self._experiment, self._path)
        attr.assign(value, self._wait)
        return attr

    def visit_image_series(self, value: ImageSeries) -> Attribute:
        attr = ImageSeriesAttr(self._experiment, self._path)
        attr.assign(value, self._wait)
        return attr

    def visit_string_set(self, value: StringSet) -> Attribute:
        attr = StringSetAttr(self._experiment, self._path)
        attr.assign(value, self._wait)
        return attr
