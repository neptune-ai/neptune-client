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
__all__ = ["Signal", "SignalsVisitor", "SignalType"]

from abc import abstractmethod
from dataclasses import dataclass
from enum import Enum


class SignalType(str, Enum):
    BATCH_STARTED = "BatchStarted"
    BATCH_ACK = "BatchAck"


@dataclass
class Signal:
    occured_at: float
    type: SignalType

    def accept(self, visitor: "SignalsVisitor") -> None:
        if type == SignalType.BATCH_STARTED:
            visitor.visit_batch_started(signal=self)
        if type == SignalType.BATCH_ACK:
            visitor.visit_batch_ack(signal=self)


class SignalsVisitor:
    @abstractmethod
    def visit_batch_started(self, signal: Signal) -> None:
        ...

    @abstractmethod
    def visit_batch_ack(self, signal: Signal) -> None:
        ...
