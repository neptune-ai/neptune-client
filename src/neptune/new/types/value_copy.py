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
__all__ = ["ValueCopy"]

from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    TypeVar,
)

from neptune.new.internal.utils.paths import parse_path
from neptune.new.types.value import Value

if TYPE_CHECKING:
    from neptune.new.metadata_containers import Handler
    from neptune.new.types.value_visitor import ValueVisitor

Ret = TypeVar("Ret")


@dataclass
class ValueCopy(Value):

    source_handler: "Handler"

    def __init__(self, source_handler: "Handler"):
        self.source_handler = source_handler

    def accept(self, visitor: "ValueVisitor[Ret]") -> Ret:
        source_path = self.source_handler._path
        source_attr = self.source_handler._container.get_attribute(source_path)
        if source_attr and source_attr.supports_copy:
            return visitor.copy_value(source_type=type(source_attr), source_path=parse_path(source_path))
        else:
            raise Exception(f"{type(source_attr).__name__} doesn't support copying")

    def __str__(self):
        return "Copy({})".format(str(self.source_handler))
