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

import neptune.api.operations as api_operations
from neptune.api.operations import Serializable
from neptune.core.operations import (
    AssignBool,
    AssignDatetime,
    AssignFloat,
    AssignInt,
    AssignString,
    LogFloats,
)
from neptune.core.operations.operation import RunCreation
from neptune.core.operations.operation_visitor import OperationVisitor
from neptune.internal.utils.paths import path_to_str


class OperationToApiVisitor(OperationVisitor[Serializable]):
    def visit_assign_float(self, op: AssignFloat) -> Serializable:
        return api_operations.AssignFloat(path_to_str(op.path), op.value)

    def visit_assign_int(self, op: AssignInt) -> Serializable:
        return api_operations.AssignInteger(path_to_str(op.path), op.value)

    def visit_assign_bool(self, op: AssignBool) -> Serializable:
        return api_operations.AssignBool(path_to_str(op.path), op.value)

    def visit_assign_datetime(self, op: AssignDatetime) -> Serializable:
        return api_operations.AssignDatetime(path_to_str(op.path), op.value)

    def visit_assign_string(self, op: AssignString) -> Serializable:
        return api_operations.AssignString(path_to_str(op.path), op.value)

    def visit_log_floats(self, op: LogFloats) -> Serializable:
        return api_operations.LogFloats(
            path_to_str(op.path),
            [api_operations.FloatValue(val.ts, val.value, val.step) for val in op.values],
        )

    def visit_run_creation(self, op: RunCreation) -> Serializable:
        return api_operations.Run(op.created_at, op.custom_id)
