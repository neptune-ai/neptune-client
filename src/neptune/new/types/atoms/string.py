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
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    TypeVar,
)

from neptune.common.deprecation import warn_once
from neptune.new.internal.utils import is_string
from neptune.new.types.atoms.atom import Atom

if TYPE_CHECKING:
    from neptune.new.types.value_visitor import ValueVisitor

Ret = TypeVar("Ret")


@dataclass
class String(Atom):

    value: str

    def __init__(self, value):
        if not is_string(value):
            warn_once(
                message="The object you're logging will be implicitly cast to a string."
                " We'll end support of this behavior in `neptune-client==1.0.0`."
                " To log the object as a string, use `String(str(object))` instead.",
                stack_level=2,
            )

        self.value = str(value)

    def accept(self, visitor: "ValueVisitor[Ret]") -> Ret:
        return visitor.visit_string(self)

    def __str__(self):
        return "String({})".format(str(self.value))
