#
# Copyright (c) 2023, Neptune Labs Sp. z o.o.
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
__all__ = ["OperationsAccumulator"]

from enum import Enum
from typing import (
    Callable,
    List,
    Optional,
    Type,
    TypeVar,
)

from neptune.common.exceptions import InternalClientError
from neptune.exceptions import MetadataInconsistency
from neptune.internal.operation import (
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
    LogOperation,
    LogStrings,
    Operation,
    RemoveStrings,
    TrackFilesToArtifact,
    UploadFile,
    UploadFileContent,
    UploadFileSet,
)
from neptune.internal.operation_visitor import OperationVisitor
from neptune.internal.preprocessor.exceptions import RequiresPreviousCompleted
from neptune.internal.utils.paths import path_to_str

T = TypeVar("T")


class DataType(Enum):
    FLOAT = "Float"
    INT = "Int"
    BOOL = "Bool"
    STRING = "String"
    FILE = "File"
    DATETIME = "Datetime"
    FILE_SET = "File Set"
    FLOAT_SERIES = "Float Series"
    STRING_SERIES = "String Series"
    IMAGE_SERIES = "Image Series"
    STRING_SET = "String Set"
    ARTIFACT = "Artifact"


class OperationsAccumulator(OperationVisitor[None]):
    def __init__(self, path: List[str]):
        self._path: List[str] = path
        self._type: Optional[DataType] = None
        self._delete_ops: List[Operation] = []
        self._modify_ops: List[Operation] = []
        self._config_ops: List[Operation] = []
        self._errors: List[MetadataInconsistency] = []
        self._ops_count: int = 0
        self._append_count: int = 0

    def get_operations(self) -> List[Operation]:
        return self._delete_ops + self._modify_ops + self._config_ops

    def get_errors(self) -> List[MetadataInconsistency]:
        return self._errors

    def get_op_count(self) -> int:
        return self._ops_count

    def get_append_count(self) -> int:
        return self._append_count

    def _check_prerequisites(self, op: Operation) -> None:
        if (is_file_op(op) or is_artifact_op(op)) and len(self._delete_ops) > 0:
            raise RequiresPreviousCompleted()

    def _process_modify_op(
        self,
        expected_type: DataType,
        op: Operation,
        modifier: Callable[[List[Operation], Operation], List[Operation]],
    ) -> None:

        if self._type and self._type != expected_type:
            # This case should never happen since inconsistencies on data types are verified on user api.
            # So such operations should not appear in the queue without delete operation between them.
            # Still we want to support this case to avoid some unclear dependencies and assumptions.
            self._errors.append(
                MetadataInconsistency(
                    "Cannot perform {} operation on {}: Attribute is not a {}".format(
                        op.__class__.__name__,
                        path_to_str(self._path),
                        expected_type.value,
                    )
                )
            )
        else:
            self._check_prerequisites(op)
            self._type = expected_type
            old_op_count = len(self._modify_ops)
            self._modify_ops = modifier(self._modify_ops, op)
            self._ops_count += len(self._modify_ops) - old_op_count

    def _process_config_op(self, expected_type: DataType, op: Operation) -> None:

        if self._type and self._type != expected_type:
            # This case should never happen since inconsistencies on data types are verified on user api.
            # So such operations should not appear in the queue without delete operation between them.
            # Still we want to support this case to avoid some unclear dependencies and assumptions.
            self._errors.append(
                MetadataInconsistency(
                    "Cannot perform {} operation on {}: Attribute is not a {}".format(
                        op.__class__.__name__,
                        path_to_str(self._path),
                        expected_type.value,
                    )
                )
            )
        else:
            self._check_prerequisites(op)
            self._type = expected_type
            old_op_count = len(self._config_ops)
            self._config_ops = [op]
            self._ops_count += len(self._config_ops) - old_op_count

    def visit_assign_float(self, op: AssignFloat) -> None:
        self._process_modify_op(DataType.FLOAT, op, assign_modifier)

    def visit_assign_int(self, op: AssignInt) -> None:
        self._process_modify_op(DataType.INT, op, assign_modifier)

    def visit_assign_bool(self, op: AssignBool) -> None:
        self._process_modify_op(DataType.BOOL, op, assign_modifier)

    def visit_assign_string(self, op: AssignString) -> None:
        self._process_modify_op(DataType.STRING, op, assign_modifier)

    def visit_assign_datetime(self, op: AssignDatetime) -> None:
        self._process_modify_op(DataType.DATETIME, op, assign_modifier)

    def visit_upload_file(self, op: UploadFile) -> None:
        self._process_modify_op(DataType.FILE, op, assign_modifier)

    def visit_upload_file_content(self, op: UploadFileContent) -> None:
        self._process_modify_op(DataType.FILE, op, assign_modifier)

    def visit_assign_artifact(self, op: AssignArtifact) -> None:
        self._process_modify_op(DataType.ARTIFACT, op, assign_modifier)

    def visit_upload_file_set(self, op: UploadFileSet) -> None:
        if op.reset:
            self._process_modify_op(DataType.FILE_SET, op, assign_modifier)
        else:
            self._process_modify_op(DataType.FILE_SET, op, add_modifier)

    def visit_log_floats(self, op: LogFloats) -> None:
        self._process_modify_op(
            DataType.FLOAT_SERIES,
            op,
            log_modifier(
                LogFloats,
                ClearFloatLog,
                lambda op1, op2: LogFloats(op1.path, op1.get_values() + op2.get_values()),
            ),
        )

    def visit_log_strings(self, op: LogStrings) -> None:
        self._process_modify_op(
            DataType.STRING_SERIES,
            op,
            log_modifier(
                LogStrings,
                ClearStringLog,
                lambda op1, op2: LogStrings(op1.path, op1.get_values() + op2.get_values()),
            ),
        )

    def visit_log_images(self, op: LogImages) -> None:
        self._process_modify_op(
            DataType.IMAGE_SERIES,
            op,
            log_modifier(
                LogImages,
                ClearImageLog,
                lambda op1, op2: LogImages(op1.path, op1.get_values() + op2.get_values()),
            ),
        )

    def visit_clear_float_log(self, op: ClearFloatLog) -> None:
        self._process_modify_op(DataType.FLOAT_SERIES, op, clear_modifier)

    def visit_clear_string_log(self, op: ClearStringLog) -> None:
        self._process_modify_op(DataType.STRING_SERIES, op, clear_modifier)

    def visit_clear_image_log(self, op: ClearImageLog) -> None:
        self._process_modify_op(DataType.IMAGE_SERIES, op, clear_modifier)

    def visit_add_strings(self, op: AddStrings) -> None:
        self._process_modify_op(DataType.STRING_SET, op, add_modifier)

    def visit_clear_string_set(self, op: ClearStringSet) -> None:
        self._process_modify_op(DataType.STRING_SET, op, clear_modifier)

    def visit_remove_strings(self, op: RemoveStrings) -> None:
        self._process_modify_op(DataType.STRING_SET, op, remove_modifier)

    def visit_config_float_series(self, op: ConfigFloatSeries) -> None:
        self._process_config_op(DataType.FLOAT_SERIES, op)

    def visit_delete_files(self, op: DeleteFiles) -> None:
        self._process_modify_op(DataType.FILE_SET, op, add_modifier)

    def visit_delete_attribute(self, op: DeleteAttribute) -> None:
        if self._type:
            if self._delete_ops:
                # Keep existing delete operation and simply clear all modification operations after it
                self._modify_ops = []
                self._config_ops = []
                self._type = None
                self._ops_count = len(self._delete_ops)
                self._append_count = 0
            else:
                # This case is tricky. There was no delete operation, but some modifications was performed.
                # We do not know if this attribute exists on server side and we do not want a delete op to fail.
                # So we need to send a single modification before delete to be sure a delete op is valid.
                self._delete_ops = [self._modify_ops[0], op]
                self._modify_ops = []
                self._config_ops = []
                self._type = None
                self._ops_count = len(self._delete_ops)
                self._append_count = 0
        else:
            if self._delete_ops:
                # Do nothing if there already is a delete operation
                # and no other operations was performed after it
                return
            else:
                # If value has not been set locally yet and no delete operation was performed,
                # simply perform single delete operation.
                self._delete_ops.append(op)
                self._ops_count = len(self._delete_ops)

    def visit_track_files_to_artifact(self, op: TrackFilesToArtifact) -> None:
        self._process_modify_op(DataType.ARTIFACT, op, artifact_log_modifier)

    def visit_clear_artifact(self, op: ClearArtifact) -> None:
        self._process_modify_op(DataType.ARTIFACT, op, clear_modifier)

    def visit_copy_attribute(self, op: CopyAttribute) -> None:
        raise MetadataInconsistency("No CopyAttribute should reach accumulator")


def artifact_log_modifier(ops: List[Operation], new_op: Operation) -> List[Operation]:
    assert isinstance(new_op, TrackFilesToArtifact)

    if len(ops) == 0:
        return [new_op]

    # There should be exactly 1 operation, merge it with new_op
    assert len(ops) == 1
    op_old = ops[0]
    assert isinstance(op_old, TrackFilesToArtifact)
    assert op_old.path == new_op.path
    assert op_old.project_id == new_op.project_id
    return [TrackFilesToArtifact(op_old.path, op_old.project_id, op_old.entries + new_op.entries)]


def log_modifier(
    log_op_class: Type[LogOperation],
    clear_op_class: Type[Operation],
    log_combine: Callable[[LogOperation, LogOperation], LogOperation],
) -> Callable[[List[Operation], Operation], List[Operation]]:
    def modifier(ops: List[Operation], new_op: Operation) -> List[Operation]:
        assert isinstance(new_op, log_op_class)

        if len(ops) == 0:
            return [new_op]

        if len(ops) == 1:
            first_operation = ops[0]

            if isinstance(first_operation, log_op_class):
                return [log_combine(first_operation, new_op)]
            elif isinstance(first_operation, clear_op_class):
                return [first_operation, new_op]

        if len(ops) == 2:
            first_operation = ops[0]
            second_operation = ops[1]

            if isinstance(second_operation, log_op_class):
                return [first_operation, log_combine(second_operation, new_op)]

        raise InternalClientError(f"Preprocessing operations failed: len(ops) == {len(ops)}")
        # TODO: Restore
        # if isinstance(new_op, log_op_class):  # Check just so that static typing doesn't complain
        #     self._append_count += new_op.value_count()

    return modifier


def is_file_op(op: Operation) -> bool:
    return isinstance(op, (UploadFile, UploadFileContent, UploadFileSet))


def is_artifact_op(op: Operation) -> bool:
    return isinstance(op, TrackFilesToArtifact)


# TODO
# for op in ops:
#     if isinstance(op, LogOperation):
#         self._append_count -= op.value_count()
clear_modifier: Callable[[List[Operation], Operation], List[Operation]] = lambda ops, new_op: [new_op]


assign_modifier: Callable[[List[Operation], Operation], List[Operation]] = lambda ops, new_op: [new_op]


# We do not optimize it on client side for now. It should not be often operation.
add_modifier: Callable[[List[Operation], Operation], List[Operation]] = lambda ops, op: ops + [op]


# We do not optimize it on client side for now. It should not be often operation.
remove_modifier: Callable[[List[Operation], Operation], List[Operation]] = lambda ops, op: ops + [op]
