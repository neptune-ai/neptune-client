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
__all__ = ("OperationProcessor",)

import abc
from typing import Optional

from neptune.internal.operation import Operation


class OperationProcessor(abc.ABC):
    def pause(self):
        pass

    def resume(self):
        pass

    @abc.abstractmethod
    def enqueue_operation(self, op: Operation, *, wait: bool) -> None:
        ...

    @abc.abstractmethod
    def wait(self) -> None:
        ...

    @abc.abstractmethod
    def flush(self):
        ...

    @abc.abstractmethod
    def start(self):
        ...

    @abc.abstractmethod
    def stop(self, seconds: Optional[float] = None) -> None:
        ...

    @abc.abstractmethod
    def close(self) -> None:
        ...
