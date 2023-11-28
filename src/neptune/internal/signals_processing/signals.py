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
__all__ = [
    "Signal",
    "SignalsVisitor",
    "BatchStartedSignal",
    "BatchProcessedSignal",
    "BatchLagSignal",
]

from abc import abstractmethod
from dataclasses import dataclass


@dataclass
class Signal:
    occured_at: float

    @abstractmethod
    def accept(self, visitor: "SignalsVisitor") -> None:
        ...


@dataclass
class BatchStartedSignal(Signal):
    def accept(self, visitor: "SignalsVisitor") -> None:
        visitor.visit_batch_started(signal=self)


@dataclass
class BatchProcessedSignal(Signal):
    def accept(self, visitor: "SignalsVisitor") -> None:
        visitor.visit_batch_processed(signal=self)


@dataclass
class BatchLagSignal(Signal):
    lag: float

    def accept(self, visitor: "SignalsVisitor") -> None:
        visitor.visit_batch_lag(signal=self)


class SignalsVisitor:
    @abstractmethod
    def visit_batch_started(self, signal: Signal) -> None:
        ...

    @abstractmethod
    def visit_batch_processed(self, signal: Signal) -> None:
        ...

    @abstractmethod
    def visit_batch_lag(self, signal: Signal) -> None:
        ...
