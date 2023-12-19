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
from typing import (
    TYPE_CHECKING,
    Optional,
)

if TYPE_CHECKING:
    from neptune.core.components.operation_storage import OperationStorage
    from neptune.internal.operation import Operation


class OperationProcessor(abc.ABC):
    @abc.abstractmethod
    def enqueue_operation(self, op: "Operation", *, wait: bool) -> None:
        ...

    @property
    def operation_storage(self) -> "OperationStorage":
        raise NotImplementedError()

    def start(self) -> None:
        pass

    def pause(self) -> None:
        pass

    def resume(self) -> None:
        pass

    def flush(self) -> None:
        pass

    def wait(self) -> None:
        pass

    def stop(self, seconds: Optional[float] = None) -> None:
        pass

    def close(self) -> None:
        pass
