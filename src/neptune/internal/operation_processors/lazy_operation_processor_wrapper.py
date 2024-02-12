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

__all__ = ("LazyOperationProcessorWrapper",)

from typing import (
    Any,
    Callable,
    Optional,
    TypeVar,
)

from neptune.core.components.operation_storage import OperationStorage
from neptune.internal.operation import Operation
from neptune.internal.operation_processors.operation_processor import OperationProcessor

RT = TypeVar("RT")


def trigger_op_processor(method: Callable[..., RT]) -> Callable[..., RT]:
    def _wrapper(self: LazyOperationProcessorWrapper, *args: Any, **kwargs: Any) -> RT:
        if self._operation_processor is None:
            self._operation_processor = self._operation_processor_getter()
        return method(self, *args, **kwargs)

    return _wrapper


def exec_if_triggered(method: Callable[..., RT]) -> Callable[..., RT]:
    def _wrapper(self: LazyOperationProcessorWrapper, *args: Any, **kwargs: Any) -> RT:
        if self._operation_processor is not None:
            return method(self, *args, **kwargs)

    return _wrapper


class LazyOperationProcessorWrapper(OperationProcessor):
    def __init__(self, operation_processor_getter: Callable[[], OperationProcessor]):
        self._operation_processor_getter = operation_processor_getter
        self._operation_processor: OperationProcessor = None  # type: ignore

    def evaluated(self) -> bool:
        return self._operation_processor is not None

    @trigger_op_processor
    def enqueue_operation(self, op: Operation, *, wait: bool) -> None:
        self._operation_processor.enqueue_operation(op, wait=wait)

    @property
    @trigger_op_processor
    def operation_storage(self) -> OperationStorage:
        return self._operation_processor.operation_storage

    @exec_if_triggered
    def start(self) -> None:
        self._operation_processor.start()

    @exec_if_triggered
    def pause(self) -> None:
        self._operation_processor.pause()

    @exec_if_triggered
    def resume(self) -> None:
        self._operation_processor.resume()

    @exec_if_triggered
    def flush(self) -> None:
        self._operation_processor.flush()

    @exec_if_triggered
    def wait(self) -> None:
        self._operation_processor.wait()

    @exec_if_triggered
    def stop(self, seconds: Optional[float] = None) -> None:
        self._operation_processor.stop(seconds=seconds)

    @exec_if_triggered
    def close(self) -> None:
        self._operation_processor.close()
