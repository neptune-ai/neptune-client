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
__all__ = ["Series"]

import abc
from typing import (
    Collection,
    Generic,
    Iterable,
    List,
    Optional,
    TypeVar,
    Union,
)

from neptune.attributes.attribute import Attribute
from neptune.internal.operation import LogOperation
from neptune.internal.types.stringify_value import StringifyValue
from neptune.internal.utils import (
    is_collection,
    is_stringify_value,
    verify_collection_type,
    verify_type,
)
from neptune.internal.utils.iteration import get_batches
from neptune.types.series.series import Series as SeriesVal

ValTV = TypeVar("ValTV", bound=SeriesVal)
DataTV = TypeVar("DataTV")
LogOperationTV = TypeVar("LogOperationTV", bound=LogOperation)


class Series(Attribute, Generic[ValTV, DataTV, LogOperationTV]):
    def __init_subclass__(cls, max_batch_size: int, operation_cls: type(LogOperationTV)):
        cls.max_batch_size = max_batch_size
        cls.operation_cls = operation_cls

    def clear(self, *, wait: bool = False) -> None:
        self._clear_impl(wait)

    def _get_log_operations_from_value(self, value: ValTV) -> List[LogOperationTV]:
        mapped_values = self._map_series_val(value)
        values_with_step_and_ts = zip(mapped_values, value.steps, value.timestamps)
        log_values = [self.operation_cls.ValueType(val, step=step, ts=ts) for val, step, ts in values_with_step_and_ts]
        return [
            self.operation_cls(self._path, chunk) for chunk in get_batches(log_values, batch_size=self.max_batch_size)
        ]

    @classmethod
    def _map_series_val(cls, value: ValTV) -> List[DataTV]:
        return value.values

    def _get_config_operation_from_value(self, value: ValTV) -> Optional[LogOperationTV]:
        return None

    @abc.abstractmethod
    def _get_clear_operation(self) -> LogOperationTV:
        pass

    @abc.abstractmethod
    def _data_to_value(self, values: Iterable, **kwargs) -> ValTV:
        pass

    @abc.abstractmethod
    def _is_value_type(self, value) -> bool:
        pass

    def _handle_stringified_value(self, value):
        return value.value

    def assign(self, value, wait: bool = False) -> None:
        if not self._is_value_type(value):
            value = self._data_to_value(value)
        clear_op = self._get_clear_operation()
        config_op = self._get_config_operation_from_value(value)
        with self._container.lock():
            if config_op:
                self._enqueue_operation(config_op, wait=False)
            if not value.values:
                self._enqueue_operation(clear_op, wait=wait)
            else:
                self._enqueue_operation(clear_op, wait=False)
                ops = self._get_log_operations_from_value(value)
                for op in ops:
                    self._enqueue_operation(op, wait=wait)

    def log(
        self,
        value: Union[DataTV, Iterable[DataTV]],
        step: Optional[float] = None,
        timestamp: Optional[float] = None,
        wait: bool = False,
        **kwargs,
    ) -> None:
        """log is a deprecated method, this code should be removed in future"""
        if step is not None:
            verify_type("step", step, (float, int))
        if timestamp is not None:
            verify_type("timestamp", timestamp, (float, int))

        if is_stringify_value(value):
            value = self._handle_stringified_value(value)

        if is_collection(value):
            if step is not None and len(value) > 1:
                raise ValueError("Collection of values is not supported for explicitly defined 'step'.")
            steps = None if step is None else [step] * len(value)
            timestamps = None if timestamp is None else [timestamp] * len(value)
            value = self._data_to_value(value, steps=steps, timestamps=timestamps, **kwargs)
        else:
            steps = None if step is None else [step]
            timestamps = None if timestamp is None else [timestamp]
            value = self._data_to_value([value], steps=steps, timestamps=timestamps, **kwargs)

        ops = self._get_log_operations_from_value(value)

        with self._container.lock():
            for op in ops:
                self._enqueue_operation(op, wait=wait)

    def extend(
        self,
        values: Union[Collection[DataTV], StringifyValue],
        steps: Optional[Collection[float]] = None,
        timestamps: Optional[Collection[float]] = None,
        wait: bool = False,
        **kwargs,
    ) -> None:
        if is_stringify_value(values):
            values = self._handle_stringified_value(values)

        if steps is not None:
            verify_collection_type("steps", steps, (float, int))
            if len(steps) != len(values):
                raise ValueError(f"Number of steps must be equal to number of values ({len(steps)} != {len(values)}")

        if timestamps is not None:
            verify_collection_type("timestamps", timestamps, (float, int))
            if len(timestamps) != len(values):
                raise ValueError(
                    f"Number of timestamps must be equal to number of values ({len(timestamps)} != {len(values)}"
                )

        value = self._data_to_value(values, steps=steps, timestamps=timestamps, **kwargs)
        ops = self._get_log_operations_from_value(value)

        with self._container.lock():
            for op in ops:
                self._enqueue_operation(op, wait=wait)

    def _clear_impl(self, wait: bool = False) -> None:
        op = self._get_clear_operation()
        with self._container.lock():
            self._enqueue_operation(op, wait=wait)
