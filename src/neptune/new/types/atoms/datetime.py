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
__all__ = ["Datetime"]

from dataclasses import dataclass
from datetime import datetime
from typing import (
    TYPE_CHECKING,
    TypeVar,
)

from neptune.new.internal.utils.stringify_value import extract_if_stringify_value
from neptune.new.types.atoms.atom import Atom

if TYPE_CHECKING:
    from neptune.new.types.value_visitor import ValueVisitor

Ret = TypeVar("Ret")


@dataclass
class Datetime(Atom):
    value: datetime

    def __init__(self, value: datetime):
        value = extract_if_stringify_value(value)
        self.value = value.replace(microsecond=1000 * int(value.microsecond / 1000))

    def accept(self, visitor: "ValueVisitor[Ret]") -> Ret:
        return visitor.visit_datetime(self)

    def __str__(self):
        return "Datetime({})".format(str(self.value))
