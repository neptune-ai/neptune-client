#
# Copyright (c) 2024, Neptune Labs Sp. z o.o.
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
__all__ = ["OperationVisitor"]

import abc
from typing import (
    Generic,
    TypeVar,
)

from neptune.core.operations.operation import (
    AssignBool,
    AssignDatetime,
    AssignFloat,
    AssignInt,
    AssignString,
    LogFloats,
    Operation,
    RunCreation,
)

Ret = TypeVar("Ret")


class OperationVisitor(Generic[Ret]):
    def visit(self, op: Operation) -> Ret:
        return op.accept(self)

    @abc.abstractmethod
    def visit_assign_float(self, op: AssignFloat) -> Ret:
        pass

    @abc.abstractmethod
    def visit_assign_int(self, op: AssignInt) -> Ret:
        pass

    @abc.abstractmethod
    def visit_assign_bool(self, op: AssignBool) -> Ret:
        pass

    @abc.abstractmethod
    def visit_assign_string(self, op: AssignString) -> Ret:
        pass

    @abc.abstractmethod
    def visit_assign_datetime(self, op: AssignDatetime) -> Ret:
        pass

    @abc.abstractmethod
    def visit_log_floats(self, op: LogFloats) -> Ret:
        pass

    @abc.abstractmethod
    def visit_run_creation(self, op: RunCreation) -> Ret:
        pass
