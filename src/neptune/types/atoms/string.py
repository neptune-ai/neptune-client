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
__all__ = ["String"]

from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Optional,
    TypeVar,
    Union,
)

from neptune.internal.types.stringify_value import StringifyValue
from neptune.internal.utils import (
    is_stringify_value,
    verify_type,
)
from neptune.types.atoms.atom import Atom

if TYPE_CHECKING:
    from neptune.types.value_visitor import ValueVisitor

Ret = TypeVar("Ret")


@dataclass
class String(Atom):

    value: str

    def __init__(self, value: Optional[Union[str, StringifyValue]]):
        verify_type("value", value, (str, type(None), StringifyValue))

        self.value = str(value.value) if is_stringify_value(value) else value

    def accept(self, visitor: "ValueVisitor[Ret]") -> Ret:
        return visitor.visit_string(self)

    def __str__(self) -> str:
        return "String({})".format(str(self.value))
