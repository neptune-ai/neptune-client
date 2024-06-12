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
__all__ = ["OperationApiObjectConverter"]

from neptune.internal.operation import (
    AddStrings,
    AssignBool,
    AssignDatetime,
    AssignFloat,
    AssignInt,
    AssignString,
    ClearFloatLog,
    ClearStringLog,
    ClearStringSet,
    ConfigFloatSeries,
    CopyAttribute,
    DeleteAttribute,
    LogFloats,
    LogStrings,
    Operation,
    RemoveStrings,
)
from neptune.internal.operation_visitor import (
    OperationVisitor,
    Ret,
)


class OperationApiObjectConverter(OperationVisitor[dict]):
    def convert(self, op: Operation) -> dict:
        return op.accept(self)

    def visit_assign_float(self, op: AssignFloat) -> dict:
        return {"value": op.value}

    def visit_assign_int(self, op: AssignInt) -> dict:
        return {"value": op.value}

    def visit_assign_bool(self, op: AssignBool) -> dict:
        return {"value": op.value}

    def visit_assign_string(self, op: AssignString) -> dict:
        return {"value": op.value}

    def visit_assign_datetime(self, op: AssignDatetime) -> Ret:
        return {"valueMilliseconds": int(1000 * op.value.timestamp())}

    def visit_log_floats(self, op: LogFloats) -> dict:
        return {
            "entries": [
                {
                    "value": value.value,
                    "step": value.step,
                    "timestampMilliseconds": int(value.ts * 1000),
                }
                for value in op.values
            ]
        }

    def visit_log_strings(self, op: LogStrings) -> dict:
        return {
            "entries": [
                {
                    "value": value.value,
                    "step": value.step,
                    "timestampMilliseconds": int(value.ts * 1000),
                }
                for value in op.values
            ]
        }

    def visit_clear_float_log(self, _: ClearFloatLog) -> dict:
        return {}

    def visit_clear_string_log(self, _: ClearStringLog) -> dict:
        return {}

    def visit_config_float_series(self, op: ConfigFloatSeries) -> dict:
        return {"min": op.min, "max": op.max, "unit": op.unit}

    def visit_add_strings(self, op: AddStrings) -> dict:
        return {"values": list(op.values)}

    def visit_remove_strings(self, op: RemoveStrings) -> dict:
        return {"values": list(op.values)}

    def visit_delete_attribute(self, _: DeleteAttribute) -> dict:
        return {}

    def visit_clear_string_set(self, _: ClearStringSet) -> dict:
        return {}

    def visit_copy_attribute(self, _: CopyAttribute) -> Ret:
        raise NotImplementedError("This operation is client-side only")
