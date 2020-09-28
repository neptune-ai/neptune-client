#
# Copyright (c) 2020, Neptune Labs Sp. z o.o.
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
import abc

from neptune.alpha.exceptions import InternalClientError
from neptune.alpha.internal.operation import Operation, AssignFloat, AssignString, LogFloats, LogStrings, \
    ClearFloatLog, ClearStringLog, AddStrings, RemoveStrings, DeleteAttribute, ClearStringSet, LogImages, \
    ClearImageLog, UploadFile
from neptune.alpha.internal.operation_visitor import OperationVisitor


class OperationApiNameVisitor(OperationVisitor[str]):

    def visit(self, op: Operation) -> str:
        return op.accept(self)

    def visit_assign_float(self, op: AssignFloat) -> str:
        return "assignFloat"

    def visit_assign_string(self, op: AssignString) -> str:
        return "assignString"

    def visit_upload_file(self, op: UploadFile) -> str:
        raise InternalClientError("Specialized endpoint should be used to upload file attribute")

    def visit_log_floats(self, op: LogFloats) -> str:
        return "logFloats"

    def visit_log_strings(self, op: LogStrings) -> str:
        return "logStrings"

    def visit_log_images(self, op: LogImages) -> str:
        return "logImages"

    def visit_clear_float_log(self, op: ClearFloatLog) -> str:
        return "clearFloatSeries"

    def visit_clear_string_log(self, op: ClearStringLog) -> str:
        return "clearStringSeries"

    def visit_clear_image_log(self, op: ClearImageLog) -> str:
        return "clearImageSeries"

    def visit_add_strings(self, op: AddStrings) -> str:
        return "insertStrings"

    def visit_remove_strings(self, op: RemoveStrings) -> str:
        return "removeStrings"

    def visit_delete_attribute(self, op: DeleteAttribute) -> str:
        return "deleteAttribute"

    def visit_clear_string_set(self, op: ClearStringSet) -> str:
        return "clearStringSet"
