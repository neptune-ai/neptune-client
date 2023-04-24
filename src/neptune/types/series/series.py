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
__all__ = ["Series"]

import abc
from typing import (
    TYPE_CHECKING,
    TypeVar,
)

from neptune.types.value import Value

if TYPE_CHECKING:
    from neptune.types.value_visitor import ValueVisitor

Ret = TypeVar("Ret")


class Series(Value):
    @abc.abstractmethod
    def accept(self, visitor: "ValueVisitor[Ret]") -> Ret:
        pass

    @property
    @abc.abstractmethod
    def values(self):
        pass

    @property
    @abc.abstractmethod
    def steps(self):
        pass

    @property
    @abc.abstractmethod
    def timestamps(self):
        pass

    def __len__(self):
        return len(self.values)
