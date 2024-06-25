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
__all__ = [
    "Serializable",
    "Run",
    "FloatValue",
    "LogFloats",
    "AssignInteger",
    "AssignFloat",
    "AssignBool",
    "AssignString",
    "AssignDatetime",
    "RunOperation",
]


import abc
from dataclasses import dataclass
from datetime import datetime
from typing import (
    List,
    Optional,
)

from google.protobuf import timestamp_pb2

import neptune.api.proto.neptune_pb.ingest.v1.common_pb2 as common_pb2
import neptune.api.proto.neptune_pb.ingest.v1.pub.ingest_pb2 as ingest_pb2


class Serializable(abc.ABC):
    @abc.abstractmethod
    def to_proto(self) -> ingest_pb2.RunOperation:
        pass


@dataclass
class Run(Serializable):
    created_at: datetime
    custom_id: str

    def to_proto(self) -> ingest_pb2.RunOperation:
        return ingest_pb2.RunOperation(
            create=common_pb2.Run(
                creation_time=timestamp_pb2.Timestamp(seconds=int(self.created_at.timestamp())),
                run_id=self.custom_id,
            ),
            update=None,
            api_key=b"",
        )


@dataclass
class FloatValue:
    timestamp: float
    value: float
    step: Optional[float] = None


@dataclass
class LogFloats(Serializable):
    path: str
    items: List[FloatValue]

    def to_proto(self) -> ingest_pb2.RunOperation:
        first_item = self.items[0]

        step = (
            common_pb2.Step(whole=int(first_item.step), micro=int((first_item.step - int(first_item.step)) * 1e6))
            if first_item.step is not None
            else None
        )

        return ingest_pb2.RunOperation(
            update=common_pb2.UpdateRunSnapshot(
                step=step,
                append={
                    "path": common_pb2.Value(string=self.path),
                    "value": common_pb2.Value(float64=first_item.value),
                },
                timestamp=timestamp_pb2.Timestamp(seconds=int(first_item.timestamp)),
            ),
            api_key=b"",
        )

    @property
    def update(self) -> common_pb2.UpdateRunSnapshot:
        first_item = self.items[0]

        step = (
            common_pb2.Step(whole=int(first_item.step), micro=int((first_item.step - int(first_item.step)) * 1e6))
            if first_item.step is not None
            else None
        )

        return common_pb2.UpdateRunSnapshot(
            step=step,
            append={
                "path": common_pb2.Value(string=self.path),
                "value": common_pb2.Value(float64=first_item.value),
            },
            timestamp=timestamp_pb2.Timestamp(seconds=int(first_item.timestamp)),
        )


@dataclass
class AssignInteger(Serializable):
    path: str
    value: int

    def to_proto(self) -> ingest_pb2.RunOperation:
        return ingest_pb2.RunOperation(
            update=common_pb2.UpdateRunSnapshot(
                assign={
                    "path": common_pb2.Value(string=self.path),
                    "value": common_pb2.Value(int64=self.value),
                },
            ),
            api_key=b"",
        )


@dataclass
class AssignFloat(Serializable):
    path: str
    value: float

    def to_proto(self) -> ingest_pb2.RunOperation:
        return ingest_pb2.RunOperation(
            update=common_pb2.UpdateRunSnapshot(
                assign={
                    "path": common_pb2.Value(string=self.path),
                    "value": common_pb2.Value(float64=self.value),
                },
            ),
            api_key=b"",
        )


@dataclass
class AssignBool(Serializable):
    path: str
    value: bool

    def to_proto(self) -> ingest_pb2.RunOperation:
        return ingest_pb2.RunOperation(
            update=common_pb2.UpdateRunSnapshot(
                assign={
                    "path": common_pb2.Value(string=self.path),
                    "value": common_pb2.Value(bool=self.value),
                },
            ),
            api_key=b"",
        )


@dataclass
class AssignString(Serializable):
    path: str
    value: str

    def to_proto(self) -> ingest_pb2.RunOperation:
        return ingest_pb2.RunOperation(
            update=common_pb2.UpdateRunSnapshot(
                assign={
                    "path": common_pb2.Value(string=self.path),
                    "value": common_pb2.Value(string=self.value),
                },
            ),
            api_key=b"",
        )


@dataclass
class AssignDatetime(Serializable):
    path: str
    value: datetime

    def to_proto(self) -> ingest_pb2.RunOperation:
        return ingest_pb2.RunOperation(
            update=common_pb2.UpdateRunSnapshot(
                assign={
                    "path": common_pb2.Value(string=self.path),
                    "value": common_pb2.Value(timestamp=timestamp_pb2.Timestamp(seconds=int(self.value.timestamp()))),
                },
            ),
            api_key=b"",
        )


@dataclass
class RunOperation:
    project: str
    run_id: str
    operation: Serializable

    def to_proto(self) -> ingest_pb2.RunOperation:
        serialized_op = self.operation.to_proto()

        return ingest_pb2.RunOperation(
            project=self.project,
            run_id=self.run_id,
            create_missing_project=False,
            create=serialized_op.create if serialized_op.create.ListFields() else None,
            update=serialized_op.update if serialized_op.update.ListFields() else None,
            api_key=b"",
        )
