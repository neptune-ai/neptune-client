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
from typing import TypeVar, Generic

from neptune.new.internal.operation import AssignBool, AssignInt, Operation, AssignFloat, AssignString, LogFloats, \
    LogStrings, \
    ClearFloatLog, ClearStringLog, AddStrings, RemoveStrings, DeleteAttribute, ClearStringSet, LogImages, \
    ClearImageLog, UploadFile, AssignDatetime, ConfigFloatSeries, UploadFileSet, UploadFileContent, DeleteFiles

Ret = TypeVar('Ret')


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
    def visit_upload_file(self, op: UploadFile) -> Ret:
        pass

    @abc.abstractmethod
    def visit_upload_file_content(self, op: UploadFileContent) -> Ret:
        pass

    @abc.abstractmethod
    def visit_upload_file_set(self, op: UploadFileSet) -> Ret:
        pass

    @abc.abstractmethod
    def visit_log_floats(self, op: LogFloats) -> Ret:
        pass

    @abc.abstractmethod
    def visit_log_strings(self, op: LogStrings) -> Ret:
        pass

    @abc.abstractmethod
    def visit_log_images(self, op: LogImages) -> Ret:
        pass

    @abc.abstractmethod
    def visit_clear_float_log(self, op: ClearFloatLog) -> Ret:
        pass

    @abc.abstractmethod
    def visit_clear_string_log(self, op: ClearStringLog) -> Ret:
        pass

    @abc.abstractmethod
    def visit_clear_image_log(self, op: ClearImageLog) -> Ret:
        pass

    @abc.abstractmethod
    def visit_config_float_series(self, op: ConfigFloatSeries) -> Ret:
        pass

    @abc.abstractmethod
    def visit_add_strings(self, op: AddStrings) -> Ret:
        pass

    @abc.abstractmethod
    def visit_remove_strings(self, op: RemoveStrings) -> Ret:
        pass

    @abc.abstractmethod
    def visit_delete_attribute(self, op: DeleteAttribute) -> Ret:
        pass

    @abc.abstractmethod
    def visit_clear_string_set(self, op: ClearStringSet) -> Ret:
        pass

    @abc.abstractmethod
    def visit_delete_files(self, op: DeleteFiles) -> Ret:
        pass
