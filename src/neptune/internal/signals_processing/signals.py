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
__all__ = ["Signal", "SignalsVisitor"]

from enum import StrEnum
from queue import (
    Queue,
    Full,
)
from abc import abstractmethod
from time import monotonic
from dataclasses import dataclass
from typing import Optional
from neptune.common.warnings import (
    warn_once,
    NeptuneWarning,
)


class SignalType(StrEnum):
    OPERATION_QUEUED = "OperationQueued"


@dataclass
class Signal:
    occured_at: float
    type: SignalType

    def accept(self, visitor: "SignalsVisitor") -> None:
        return visitor.visit(self)


class SignalsVisitor:
    def visit(self, signal: Signal) -> None:
        return signal.accept(self)

    @abstractmethod
    def visit_operation_queued(self, signal: Signal) -> None:
        pass


def signal_operation_queued(queue: Queue["Signal"], occured_at: Optional[float] = None) -> None:
    try:
        queue.put_nowait(
            item=Signal(
                occured_at=occured_at if occured_at is not None else monotonic(),
                type=SignalType.OPERATION_QUEUED
            )
        )
    except Full:
        warn_once("Signal queue is full. Some signals will be lost.", exception=NeptuneWarning)
