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
__all__ = ["OperationApiNameVisitor"]

from neptune.common.exceptions import InternalClientError
from neptune.new.internal.operation import (
    AddStrings,
    AssignArtifact,
    AssignBool,
    AssignDatetime,
    AssignFloat,
    AssignInt,
    AssignString,
    ClearArtifact,
    ClearFloatLog,
    ClearImageLog,
    ClearStringLog,
    ClearStringSet,
    ConfigFloatSeries,
    CopyAttribute,
    DeleteAttribute,
    DeleteFiles,
    LogFloats,
    LogImages,
    LogStrings,
    Operation,
    RemoveStrings,
    TrackFilesToArtifact,
    UploadFile,
    UploadFileContent,
    UploadFileSet,
)
from neptune.new.internal.operation_visitor import (
    OperationVisitor,
    Ret,
)


class OperationApiNameVisitor(OperationVisitor[str]):
    def visit(self, op: Operation) -> str:
        return op.accept(self)

    def visit_assign_float(self, _: AssignFloat) -> str:
        return "assignFloat"

    def visit_assign_int(self, _: AssignInt) -> str:
        return "assignInt"

    def visit_assign_bool(self, _: AssignBool) -> str:
        return "assignBool"

    def visit_assign_string(self, _: AssignString) -> str:
        return "assignString"

    def visit_assign_datetime(self, _: AssignDatetime) -> Ret:
        return "assignDatetime"

    def visit_upload_file(self, _: UploadFile) -> str:
        raise InternalClientError("Specialized endpoint should be used to upload file attribute")

    def visit_upload_file_content(self, _: UploadFileContent) -> str:
        raise InternalClientError("Specialized endpoint should be used to upload file attribute")

    def visit_upload_file_set(self, op: UploadFileSet) -> Ret:
        raise InternalClientError("Specialized endpoints should be used to upload file set attribute")

    def visit_log_floats(self, _: LogFloats) -> str:
        return "logFloats"

    def visit_log_strings(self, _: LogStrings) -> str:
        return "logStrings"

    def visit_log_images(self, _: LogImages) -> str:
        return "logImages"

    def visit_clear_float_log(self, _: ClearFloatLog) -> str:
        return "clearFloatSeries"

    def visit_clear_string_log(self, _: ClearStringLog) -> str:
        return "clearStringSeries"

    def visit_clear_image_log(self, _: ClearImageLog) -> str:
        return "clearImageSeries"

    def visit_config_float_series(self, _: ConfigFloatSeries) -> str:
        return "configFloatSeries"

    def visit_add_strings(self, _: AddStrings) -> str:
        return "insertStrings"

    def visit_remove_strings(self, _: RemoveStrings) -> str:
        return "removeStrings"

    def visit_delete_attribute(self, _: DeleteAttribute) -> str:
        return "deleteAttribute"

    def visit_clear_string_set(self, _: ClearStringSet) -> str:
        return "clearStringSet"

    def visit_delete_files(self, _: DeleteFiles) -> Ret:
        return "deleteFiles"

    def visit_assign_artifact(self, _: AssignArtifact) -> Ret:
        return "assignArtifact"

    def visit_track_files_to_artifact(self, _: TrackFilesToArtifact) -> Ret:
        raise InternalClientError("Specialized endpoint should be used to track artifact files")

    def visit_clear_artifact(self, _: ClearArtifact) -> Ret:
        return "clearArtifact"

    def visit_copy_attribute(self, _: CopyAttribute) -> Ret:
        raise NotImplementedError("This operation is client-side only")
