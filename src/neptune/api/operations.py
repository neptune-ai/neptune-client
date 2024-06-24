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

from __future__ import annotations

from dataclasses import dataclass

from google.protobuf import timestamp_pb2

import neptune.api.proto.neptune_pb.ingest.v1.pub.ingest_pb2 as ingest_pb2
import neptune.api.proto.neptune_pb.ingest.v1.common_pb2 as common_pb2
from neptune.api.models import FloatSeriesValues
from neptune.core.operations.operation import RunCreation


@dataclass
class RunOperation:
    project: str
    run_id: str
    create_missing_project: bool
    api_key: bytes
    operation: common_pb2.Run | common_pb2.UpdateRunSnapshot

    def to_proto(self) -> ingest_pb2.RunOperation:
        create = self.operation if isinstance(self.operation, common_pb2.Run) else None
        update = self.operation if isinstance(self.operation, common_pb2.UpdateRunSnapshot) else None

        if create is not None and update is not None:
            raise ValueError("Operation must be either a Run or an UpdateRunSnapshot")

        return ingest_pb2.RunOperation(
            project=self.project,
            run_id=self.run_id,
            create_missing_project=self.create_missing_project,
            create=create,
            update=update,
            api_key=self.api_key,
        )


@dataclass
class Run:
    operation: RunCreation

    def to_proto(self) -> common_pb2.Run:
        return common_pb2.Run(
            run_id=self.operation.custom_id,
            creation_time=timestamp_pb2.Timestamp(seconds=int(self.operation.created_at.timestamp())),
        )


@dataclass
class LogFloats:
    path: str
    items: FloatSeriesValues

    def to_proto(self) -> common_pb2.UpdateRunSnapshot:
        first_item = self.items.values[0]

        return common_pb2.UpdateRunSnapshot(
            step=common_pb2.Step(whole=int(first_item.step), micro=int((first_item.step - int(first_item.step)) * 1e6)),
            append={
                "path": common_pb2.Value(string=self.path),
                "value": common_pb2.Value(float64=first_item.value),
            },
            timestamp=timestamp_pb2.Timestamp(seconds=int(first_item.timestamp.timestamp())),
        )
