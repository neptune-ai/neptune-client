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
__all__ = ["OperationsPreprocessor"]

from typing import (
    Dict,
    List,
    TypeVar,
)

from neptune.internal.operation import (
    Operation,
    TrackFilesToArtifact,
    UploadFile,
    UploadFileContent,
    UploadFileSet,
)
from neptune.internal.preprocessor.accumulated_operations import AccumulatedOperations
from neptune.internal.preprocessor.exceptions import RequiresPreviousCompleted
from neptune.internal.preprocessor.operations_accumulator import OperationsAccumulator
from neptune.internal.utils.paths import path_to_str

T = TypeVar("T")


class OperationsPreprocessor:
    def __init__(self) -> None:
        self._accumulators: Dict[str, "OperationsAccumulator"] = dict()
        self.processed_ops_count: int = 0
        self.final_ops_count: int = 0
        self.final_append_count: int = 0

    def process(self, operation: Operation) -> bool:
        """Adds a single operation to its processed list.
        Returns `False` iff the new operation can't be in queue until one of already enqueued operations gets
        synchronized with server first.
        """
        try:
            self._process_op(operation)
            self.processed_ops_count += 1
            return True
        except RequiresPreviousCompleted:
            return False

    def process_batch(self, operations: List[Operation]) -> None:
        for op in operations:
            if not self.process(op):
                return

    def _process_op(self, op: Operation) -> "OperationsAccumulator":
        path_str = path_to_str(op.path)
        target_acc = self._accumulators.setdefault(path_str, OperationsAccumulator(op.path))
        old_ops_count, old_append_count = target_acc.get_op_count(), target_acc.get_append_count()
        target_acc.visit(op)
        self.final_ops_count += target_acc.get_op_count() - old_ops_count
        self.final_append_count += target_acc.get_append_count() - old_append_count
        return target_acc

    def accumulate_operations(self) -> AccumulatedOperations:
        result = AccumulatedOperations()
        for _, acc in sorted(self._accumulators.items()):
            for op in acc.get_operations():
                if isinstance(op, TrackFilesToArtifact):
                    result.artifact_operations.append(op)
                elif isinstance(op, (UploadFile, UploadFileContent, UploadFileSet)):
                    result.upload_operations.append(op)
                else:
                    result.other_operations.append(op)
            result.errors.extend(acc.get_errors())

        return result
