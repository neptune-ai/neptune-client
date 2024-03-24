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

from pathlib import Path
from typing import (
    Any,
    Optional,
)

from neptune.core.components.abstract import Resource
from neptune.core.components.operation_storage import OperationStorage
from neptune.internal.operation import Operation
from neptune.internal.operation_processors.factory import get_operation_processor
from neptune.internal.operation_processors.operation_processor import OperationProcessor
from neptune.internal.utils.evaluable import (
    Evaluable,
    noop_if_not_triggered,
    trigger_evaluation,
)


class LazyOperationProcessorWrapper(Evaluable, OperationProcessor):
    def __init__(self, **kwargs: Any):
        self._operation_processor_kwargs = kwargs
        self._operation_processor: Optional[OperationProcessor] = None

    def evaluate(self) -> None:
        self._operation_processor = get_operation_processor(**self._operation_processor_kwargs)
        self._operation_processor.start()

    @property
    def evaluated(self) -> bool:
        return self._operation_processor is not None

    @trigger_evaluation
    def enqueue_operation(self, op: Operation, *, wait: bool) -> None:
        if self._operation_processor is not None:
            self._operation_processor.enqueue_operation(op, wait=wait)

    @property
    @trigger_evaluation
    def operation_storage(self) -> OperationStorage:
        if self._operation_processor is None:
            raise NotImplementedError
        return self._operation_processor.operation_storage

    @property
    @trigger_evaluation
    def data_path(self) -> Path:
        if isinstance(self._operation_processor, Resource):
            return self._operation_processor.data_path
        else:
            raise NotImplementedError

    @trigger_evaluation
    def start(self) -> None:
        if self._operation_processor is not None:
            self._operation_processor.start()

    @noop_if_not_triggered
    def pause(self) -> None:
        if self._operation_processor is not None:
            self._operation_processor.pause()

    @noop_if_not_triggered
    def resume(self) -> None:
        if self._operation_processor is not None:
            self._operation_processor.resume()

    @noop_if_not_triggered
    def flush(self) -> None:
        if self._operation_processor is not None:
            self._operation_processor.flush()

    @noop_if_not_triggered
    def wait(self) -> None:
        if self._operation_processor is not None:
            self._operation_processor.wait()

    @noop_if_not_triggered
    def stop(self, seconds: Optional[float] = None) -> None:
        if self._operation_processor is not None:
            self._operation_processor.stop(seconds=seconds)

    @noop_if_not_triggered
    def close(self) -> None:
        if self._operation_processor is not None:
            self._operation_processor.close()
