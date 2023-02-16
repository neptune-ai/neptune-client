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
from typing import (
    Generic,
    List,
    Type,
    TypeVar,
)

from neptune.attributes.attribute import Attribute
from neptune.types import (
    Artifact,
    Boolean,
    Datetime,
    File,
    FileSeries,
    FileSet,
    Float,
    FloatSeries,
    GitRef,
    Integer,
    String,
    StringSeries,
    StringSet,
)
from neptune.types.namespace import Namespace
from neptune.types.value import Value

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
    def copy_value(self, source_type: Type[Attribute], source_path: List[str]) -> Ret:
        pass
