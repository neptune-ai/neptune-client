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

from neptune.new.exceptions import InternalClientError
from neptune.new.internal.operation import AssignBool, AssignInt, Operation, AssignFloat, AssignString, LogFloats, \
    LogStrings, \
    ClearFloatLog, ClearStringLog, AddStrings, RemoveStrings, DeleteAttribute, ClearStringSet, LogImages, \
    ClearImageLog, UploadFile, AssignDatetime, ConfigFloatSeries, UploadFileSet, UploadFileContent, DeleteFiles
from neptune.new.internal.operation_visitor import OperationVisitor, Ret


class OperationApiObjectConverter(OperationVisitor[dict]):

    def convert(self, op: Operation) -> dict:
        return op.accept(self)

    def visit_assign_float(self, op: AssignFloat) -> dict:
        return {
            'value': op.value
        }

    def visit_assign_int(self, op: AssignInt) -> dict:
        return {
            'value': op.value
        }

    def visit_assign_bool(self, op: AssignBool) -> dict:
        return {
            'value': op.value
        }

    def visit_assign_string(self, op: AssignString) -> dict:
        return {
            'value': op.value
        }

    def visit_assign_datetime(self, op: AssignDatetime) -> Ret:
        return {
            'valueMilliseconds': int(1000 * op.value.timestamp())
        }

    def visit_upload_file(self, _: UploadFile) -> dict:
        raise InternalClientError("Specialized endpoint should be used to upload file attribute")

    def visit_upload_file_content(self, _: UploadFileContent) -> dict:
        raise InternalClientError("Specialized endpoint should be used to upload file attribute")

    def visit_upload_file_set(self, op: UploadFileSet) -> Ret:
        raise InternalClientError("Specialized endpoints should be used to upload file set attribute")

    def visit_log_floats(self, op: LogFloats) -> dict:
        return {
            'entries': [{
                'value': value.value,
                'step': value.step,
                'timestampMilliseconds': int(value.ts * 1000)
            } for value in op.values]
        }

    def visit_log_strings(self, op: LogStrings) -> dict:
        return {
            'entries': [{
                'value': value.value,
                'step': value.step,
                'timestampMilliseconds': int(value.ts * 1000)
            } for value in op.values]
        }

    def visit_log_images(self, op: LogImages) -> dict:
        return {
            'entries': [{
                'value': {
                    'data': value.value.data,
                    'name': value.value.name,
                    'description': value.value.description
                },
                'step': value.step,
                'timestampMilliseconds': int(value.ts * 1000)
            } for value in op.values]
        }

    def visit_clear_float_log(self, _: ClearFloatLog) -> dict:
        return {}

    def visit_clear_string_log(self, _: ClearStringLog) -> dict:
        return {}

    def visit_clear_image_log(self, _: ClearImageLog) -> dict:
        return {}

    def visit_config_float_series(self, op: ConfigFloatSeries) -> dict:
        return {
            'min': op.min,
            'max': op.max,
            'unit': op.unit
        }

    def visit_add_strings(self, op: AddStrings) -> dict:
        return {
            'values': list(op.values)
        }

    def visit_remove_strings(self, op: RemoveStrings) -> dict:
        return {
            'values': list(op.values)
        }

    def visit_delete_attribute(self, _: DeleteAttribute) -> dict:
        return {}

    def visit_clear_string_set(self, _: ClearStringSet) -> dict:
        return {}

    def visit_delete_files(self, op: DeleteFiles) -> Ret:
        return {
            'filePaths': list(op.file_paths)
        }
