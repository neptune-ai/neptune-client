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
from typing import (
    TYPE_CHECKING,
    List,
    Optional,
    Tuple,
)

from neptune.exceptions import MetadataInconsistency
from neptune.internal.operation import CopyAttribute
from neptune.internal.preprocessor.operations_preprocessor import OperationsPreprocessor
from neptune.internal.queue.disk_queue import QueueElement

if TYPE_CHECKING:
    from neptune.exceptions import NeptuneException
    from neptune.internal.backends.neptune_backend import NeptuneBackend
    from neptune.internal.operation import Operation
    from neptune.internal.preprocessor.accumulated_operations import AccumulatedOperations
    from neptune.internal.queue.disk_queue import DiskQueue


class Batcher:
    def __init__(
        self,
        queue: "DiskQueue",
        backend: "NeptuneBackend",
        max_points_per_batch: int,
        max_attributes_in_batch: int,
        max_points_per_attribute: int,
    ):
        self._backend: "NeptuneBackend" = backend
        self._queue: "DiskQueue" = queue
        self._max_points_per_batch = max_points_per_batch
        self._max_attributes_in_batch = max_attributes_in_batch
        self._max_points_per_attribute = max_points_per_attribute

        self._last_disk_record: Optional[QueueElement["Operation"]] = None

    def collect_batch(self) -> Optional[Tuple["AccumulatedOperations", int, int]]:
        preprocessor = OperationsPreprocessor()
        version: Optional[int] = None
        copy_ops: List["CopyAttribute"] = []
        errors: List["NeptuneException"] = []
        dropped_operations_count = 0

        while (
            preprocessor.points_count < self._max_points_per_batch
            and preprocessor.accumulators_count < self._max_attributes_in_batch
            and preprocessor.max_points_per_accumulator < self._max_points_per_attribute
        ):
            record: Optional[QueueElement[Operation]] = self._last_disk_record or self._queue.get()
            self._last_disk_record = None

            if not record:
                break

            operation, operation_version = record.obj, record.ver

            if isinstance(operation, CopyAttribute):
                # CopyAttribute can be only at the start of a batch.
                # TODO: This doesn't work as expected
                if copy_ops or preprocessor.operations_count:
                    self._last_disk_record = record
                    break
                else:
                    try:
                        operation = operation.resolve(self._backend)
                    except MetadataInconsistency as e:
                        errors.append(e)
                        dropped_operations_count += 1

                    version = operation_version

            if preprocessor.process(operation):
                version = operation_version
            else:
                self._last_disk_record = record
                break

        return (
            (
                preprocessor.accumulate_operations(
                    initial_errors=errors, source_operations_count=preprocessor.operations_count
                ),
                dropped_operations_count,
                version,
            )
            if version is not None
            else None
        )
