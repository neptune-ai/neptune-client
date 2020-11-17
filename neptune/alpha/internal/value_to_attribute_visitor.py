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

from neptune.alpha.attributes.atoms.datetime import Datetime as DatetimeAttr
from neptune.alpha.attributes.atoms.file import File as FileAttr
from neptune.alpha.attributes.file_set import FileSet as FileSetAttr
from neptune.alpha.attributes.atoms.float import Float as FloatAttr
from neptune.alpha.attributes.atoms.string import String as StringAttr
from neptune.alpha.attributes.attribute import Attribute
from neptune.alpha.attributes.series.float_series import FloatSeries as FloatSeriesAttr
from neptune.alpha.attributes.series.image_series import ImageSeries as ImageSeriesAttr
from neptune.alpha.attributes.series.string_series import StringSeries as StringSeriesAttr
from neptune.alpha.attributes.sets.string_set import StringSet as StringSetAttr
from neptune.alpha.exceptions import OperationNotSupported
from neptune.alpha.types.atoms import GitRef
from neptune.alpha.types.atoms.datetime import Datetime
from neptune.alpha.types.atoms.file import File
from neptune.alpha.types.atoms.float import Float
from neptune.alpha.types.atoms.string import String
from neptune.alpha.types.file_set import FileSet
from neptune.alpha.types.series.float_series import FloatSeries
from neptune.alpha.types.series.image_series import ImageSeries
from neptune.alpha.types.series.string_series import StringSeries
from neptune.alpha.types.sets.string_set import StringSet
from neptune.alpha.types.value_visitor import ValueVisitor, Ret

if TYPE_CHECKING:
    from neptune.alpha import Experiment


class ValueToAttributeVisitor(ValueVisitor[Attribute]):

    def __init__(self, _experiment: 'Experiment', path: List[str]):
        self._experiment = _experiment
        self._path = path

    def visit_float(self, _: Float) -> Attribute:
        return FloatAttr(self._experiment, self._path)

    def visit_string(self, _: String) -> Attribute:
        return StringAttr(self._experiment, self._path)

    def visit_datetime(self, _: Datetime) -> Attribute:
        return DatetimeAttr(self._experiment, self._path)

    def visit_file(self, _: File) -> Attribute:
        return FileAttr(self._experiment, self._path)

    def visit_file_set(self, _: FileSet) -> Attribute:
        return FileSetAttr(self._experiment, self._path)

    def visit_float_series(self, _: FloatSeries) -> Attribute:
        return FloatSeriesAttr(self._experiment, self._path)

    def visit_string_series(self, _: StringSeries) -> Attribute:
        return StringSeriesAttr(self._experiment, self._path)

    def visit_image_series(self, _: ImageSeries) -> Attribute:
        return ImageSeriesAttr(self._experiment, self._path)

    def visit_string_set(self, _: StringSet) -> Attribute:
        return StringSetAttr(self._experiment, self._path)

    def visit_git_ref(self, _: GitRef) -> Attribute:
        raise OperationNotSupported("Cannot create custom attribute of type GitRef")
