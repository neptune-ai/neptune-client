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
__all__ = ["ValueToAttributeVisitor"]

from typing import (
    TYPE_CHECKING,
    List,
    Type,
)

from neptune.new.attributes.atoms.artifact import Artifact as ArtifactAttr
from neptune.new.attributes.atoms.boolean import Boolean as BooleanAttr
from neptune.new.attributes.atoms.datetime import Datetime as DatetimeAttr
from neptune.new.attributes.atoms.file import File as FileAttr
from neptune.new.attributes.atoms.float import Float as FloatAttr
from neptune.new.attributes.atoms.integer import Integer as IntegerAttr
from neptune.new.attributes.atoms.string import String as StringAttr
from neptune.new.attributes.attribute import Attribute
from neptune.new.attributes.file_set import FileSet as FileSetAttr
from neptune.new.attributes.namespace import Namespace as NamespaceAttr
from neptune.new.attributes.series.file_series import FileSeries as ImageSeriesAttr
from neptune.new.attributes.series.float_series import FloatSeries as FloatSeriesAttr
from neptune.new.attributes.series.string_series import StringSeries as StringSeriesAttr
from neptune.new.attributes.sets.string_set import StringSet as StringSetAttr
from neptune.new.exceptions import OperationNotSupported
from neptune.new.types import (
    Boolean,
    Integer,
)
from neptune.new.types.atoms import GitRef
from neptune.new.types.atoms.artifact import Artifact
from neptune.new.types.atoms.datetime import Datetime
from neptune.new.types.atoms.file import File
from neptune.new.types.atoms.float import Float
from neptune.new.types.atoms.string import String
from neptune.new.types.file_set import FileSet
from neptune.new.types.namespace import Namespace
from neptune.new.types.series.file_series import FileSeries
from neptune.new.types.series.float_series import FloatSeries
from neptune.new.types.series.string_series import StringSeries
from neptune.new.types.sets.string_set import StringSet
from neptune.new.types.value_visitor import ValueVisitor

if TYPE_CHECKING:
    from neptune.new.metadata_containers import MetadataContainer


class ValueToAttributeVisitor(ValueVisitor[Attribute]):
    def __init__(self, container: "MetadataContainer", path: List[str]):
        self._container = container
        self._path = path

    def visit_float(self, _: Float) -> Attribute:
        return FloatAttr(self._container, self._path)

    def visit_integer(self, _: Integer) -> Attribute:
        return IntegerAttr(self._container, self._path)

    def visit_boolean(self, _: Boolean) -> Attribute:
        return BooleanAttr(self._container, self._path)

    def visit_string(self, _: String) -> Attribute:
        return StringAttr(self._container, self._path)

    def visit_datetime(self, _: Datetime) -> Attribute:
        return DatetimeAttr(self._container, self._path)

    def visit_artifact(self, _: Artifact) -> Attribute:
        return ArtifactAttr(self._container, self._path)

    def visit_file(self, _: File) -> Attribute:
        return FileAttr(self._container, self._path)

    def visit_file_set(self, _: FileSet) -> Attribute:
        return FileSetAttr(self._container, self._path)

    def visit_float_series(self, _: FloatSeries) -> Attribute:
        return FloatSeriesAttr(self._container, self._path)

    def visit_string_series(self, _: StringSeries) -> Attribute:
        return StringSeriesAttr(self._container, self._path)

    def visit_image_series(self, _: FileSeries) -> Attribute:
        return ImageSeriesAttr(self._container, self._path)

    def visit_string_set(self, _: StringSet) -> Attribute:
        return StringSetAttr(self._container, self._path)

    def visit_git_ref(self, _: GitRef) -> Attribute:
        raise OperationNotSupported("Cannot create custom attribute of type GitRef")

    def visit_namespace(self, _: Namespace) -> Attribute:
        return NamespaceAttr(self._container, self._path)

    def copy_value(self, source_type: Type[Attribute], source_path: List[str]) -> Attribute:
        return source_type(self._container, self._path)
