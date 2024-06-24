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
    "ProtoSerializable",
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


class ProtoSerializable(abc.ABC):
    @property
    @abc.abstractmethod
    def create(self) -> Optional[common_pb2.Run]: ...

    @property
    @abc.abstractmethod
    def update(self) -> Optional[common_pb2.UpdateRunSnapshot]: ...


class SerializableCreation(ProtoSerializable, abc.ABC):
    @property
    def update(self) -> Optional[common_pb2.UpdateRunSnapshot]:
        return None


class SerializableUpdate(ProtoSerializable, abc.ABC):
    @property
    def create(self) -> Optional[common_pb2.Run]:
        return None


@dataclass
class Run(SerializableCreation):
    created_at: datetime
    custom_id: str

    @property
    def create(self) -> common_pb2.Run:
        return common_pb2.Run(
            run_id=self.custom_id,
            creation_time=timestamp_pb2.Timestamp(seconds=int(self.created_at.timestamp())),
        )


@dataclass
class FloatValue:
    timestamp: float
    value: float
    step: Optional[float] = None


@dataclass
class LogFloats(SerializableUpdate):
    path: str
    items: List[FloatValue]

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
class AssignInteger(SerializableUpdate):
    path: str
    value: int

    @property
    def update(self) -> common_pb2.UpdateRunSnapshot:
        return common_pb2.UpdateRunSnapshot(
            assign={
                "path": common_pb2.Value(string=self.path),
                "value": common_pb2.Value(int64=self.value),
            },
        )


@dataclass
class AssignFloat(SerializableUpdate):
    path: str
    value: float

    @property
    def update(self) -> common_pb2.UpdateRunSnapshot:
        return common_pb2.UpdateRunSnapshot(
            assign={
                "path": common_pb2.Value(string=self.path),
                "value": common_pb2.Value(float64=self.value),
            },
        )


@dataclass
class AssignBool(SerializableUpdate):
    path: str
    value: bool

    @property
    def update(self) -> common_pb2.UpdateRunSnapshot:
        return common_pb2.UpdateRunSnapshot(
            assign={
                "path": common_pb2.Value(string=self.path),
                "value": common_pb2.Value(bool=self.value),
            },
        )


@dataclass
class AssignString(SerializableUpdate):
    path: str
    value: str

    @property
    def update(self) -> common_pb2.UpdateRunSnapshot:
        return common_pb2.UpdateRunSnapshot(
            assign={
                "path": common_pb2.Value(string=self.path),
                "value": common_pb2.Value(string=self.value),
            },
        )


@dataclass
class AssignDatetime(SerializableUpdate):
    path: str
    value: datetime

    @property
    def update(self) -> common_pb2.UpdateRunSnapshot:
        return common_pb2.UpdateRunSnapshot(
            assign={
                "path": common_pb2.Value(string=self.path),
                "value": common_pb2.Value(timestamp=timestamp_pb2.Timestamp(seconds=int(self.value.timestamp()))),
            },
        )


@dataclass
class RunOperation:
    project: str
    run_id: str
    operation: ProtoSerializable

    def to_proto(self) -> ingest_pb2.RunOperation:
        return ingest_pb2.RunOperation(
            project=self.project,
            run_id=self.run_id,
            create_missing_project=False,
            create=self.operation.create,
            update=self.operation.update,
            api_key=b"",
        )
