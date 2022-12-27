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
__all__ = ["ValueVisitor"]

import abc
import typing
from typing import (
    Generic,
    TypeVar,
)

from neptune.new.attributes.attribute import Attribute
from neptune.new.types.atoms import GitRef
from neptune.new.types.atoms.artifact import Artifact
from neptune.new.types.atoms.boolean import Boolean
from neptune.new.types.atoms.datetime import Datetime
from neptune.new.types.atoms.file import File
from neptune.new.types.atoms.float import Float
from neptune.new.types.atoms.integer import Integer
from neptune.new.types.atoms.string import String
from neptune.new.types.file_set import FileSet
from neptune.new.types.namespace import Namespace
from neptune.new.types.series.file_series import FileSeries
from neptune.new.types.series.float_series import FloatSeries
from neptune.new.types.series.string_series import StringSeries
from neptune.new.types.sets.string_set import StringSet
from neptune.new.types.value import Value

Ret = TypeVar("Ret")


class ValueVisitor(Generic[Ret]):
    def visit(self, value: Value) -> Ret:
        return value.accept(self)

    @abc.abstractmethod
    def visit_float(self, value: Float) -> Ret:
        pass

    @abc.abstractmethod
    def visit_integer(self, value: Integer) -> Ret:
        pass

    @abc.abstractmethod
    def visit_boolean(self, value: Boolean) -> Ret:
        pass

    @abc.abstractmethod
    def visit_string(self, value: String) -> Ret:
        pass

    @abc.abstractmethod
    def visit_datetime(self, value: Datetime) -> Ret:
        pass

    @abc.abstractmethod
    def visit_artifact(self, value: Artifact) -> Ret:
        pass

    @abc.abstractmethod
    def visit_file(self, value: File) -> Ret:
        pass

    @abc.abstractmethod
    def visit_file_set(self, value: FileSet) -> Ret:
        pass

    @abc.abstractmethod
    def visit_float_series(self, value: FloatSeries) -> Ret:
        pass

    @abc.abstractmethod
    def visit_string_series(self, value: StringSeries) -> Ret:
        pass

    @abc.abstractmethod
    def visit_image_series(self, value: FileSeries) -> Ret:
        pass

    @abc.abstractmethod
    def visit_string_set(self, value: StringSet) -> Ret:
        pass

    @abc.abstractmethod
    def visit_git_ref(self, value: GitRef) -> Ret:
        pass

    @abc.abstractmethod
    def visit_namespace(self, value: Namespace) -> Ret:
        pass

    @abc.abstractmethod
    def copy_value(self, source_type: typing.Type[Attribute], source_path: typing.List[str]) -> Ret:
        pass
