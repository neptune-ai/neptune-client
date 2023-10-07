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
__all__ = ["AccumulatedOperations"]

from dataclasses import (
    dataclass,
    field,
)
from typing import List

from neptune.exceptions import MetadataInconsistency
from neptune.internal.operation import (
    Operation,
    TrackFilesToArtifact,
)


@dataclass
class AccumulatedOperations:
    upload_operations: List[Operation] = field(default_factory=list)
    artifact_operations: List[TrackFilesToArtifact] = field(default_factory=list)
    other_operations: List[Operation] = field(default_factory=list)
    errors: List[MetadataInconsistency] = field(default_factory=list)
    final_ops_count: int = 0

    def all_operations(self) -> List[Operation]:
        return self.upload_operations + self.artifact_operations + self.other_operations

    @property
    def operations_count(self) -> int:
        return len(self.all_operations())
