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
__all__ = ["FileSeries"]

from typing import (
    TYPE_CHECKING,
    List,
    TypeVar,
)

from neptune.new.internal.utils import is_collection
from neptune.new.internal.utils.logger import logger
from neptune.new.internal.utils.stringify_value import extract_if_stringify_value
from neptune.new.types import File
from neptune.new.types.series.series import Series

if TYPE_CHECKING:
    from neptune.new.types.value_visitor import ValueVisitor

Ret = TypeVar("Ret")


class FileSeries(Series):
    def __init__(self, values, **kwargs):
        if not is_collection(values):
            raise TypeError("`values` is not a collection")
        self._values = [File.create_from(extract_if_stringify_value(value)) for value in values]

        self.name = kwargs.pop("name", None)
        self.description = kwargs.pop("description", None)
        if kwargs:
            logger.error("Warning: unexpected arguments (%s) in FileSeries", kwargs)

    def accept(self, visitor: "ValueVisitor[Ret]") -> Ret:
        return visitor.visit_image_series(self)

    @property
    def values(self) -> List[File]:
        return self._values

    def __str__(self):
        return f"FileSeries({self.values})"
