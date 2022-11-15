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
import abc
import time
from itertools import cycle
from typing import (
    Collection,
    Generic,
    Iterable,
    List,
    Optional,
    TypeVar,
    Union,
)

from neptune.new.attributes.attribute import Attribute
from neptune.new.internal.operation import Operation
from neptune.new.internal.utils import (
    is_collection,
    verify_type,
)
from neptune.new.internal.utils.iteration import get_batches
from neptune.new.types.series.series import Series as SeriesVal

Val = TypeVar("Val", bound=SeriesVal)
Data = TypeVar("Data")
LogOperation = TypeVar("LogOperation", bound=Operation)


class Series(Attribute, Generic[Val, Data, LogOperation]):
    MAX_BATCH_SIZE = None
    operation_cls: type(LogOperation) = None

    def clear(self, wait: bool = False) -> None:
        self._clear_impl(wait)

    def _get_log_operations_from_value(
        self, value: Val, *, steps: Union[None, Collection[float]], timestamps: Union[None, Collection[float]]
    ) -> List[LogOperation]:
        if steps is None:
            steps = cycle([None])
        if timestamps is None:
            timestamps = cycle([time.time()])

        mapped_values = self._map_series_val(value)
        values_with_step_and_ts = zip(mapped_values, steps, timestamps)
        log_values = [self.operation_cls.ValueType(val, step=step, ts=ts) for val, step, ts in values_with_step_and_ts]
        return [
            self.operation_cls(self._path, chunk) for chunk in get_batches(log_values, batch_size=self.MAX_BATCH_SIZE)
        ]

    @classmethod
    def _map_series_val(cls, value: Val) -> List[Data]:
        return [value for value in value.values]

    def _get_config_operation_from_value(self, value: Val) -> Optional[LogOperation]:
        return None

    @abc.abstractmethod
    def _get_clear_operation(self) -> LogOperation:
        pass

    @abc.abstractmethod
    def _data_to_value(self, values: Iterable, **kwargs) -> Val:
        pass

    @abc.abstractmethod
    def _is_value_type(self, value) -> bool:
        pass

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
                ops = self._get_log_operations_from_value(value, steps=None, timestamps=None)
                for op in ops:
                    self._enqueue_operation(op, wait=wait)

    def log(
        self,
        value: Union[Data, Iterable[Data]],
        step: Optional[float] = None,
        timestamp: Optional[float] = None,
        wait: bool = False,
        **kwargs,
    ) -> None:
        """log is a deprecated method, this code should be removed in future"""
        if is_collection(value):
            if step is not None and len(value) > 1:
                raise ValueError("Collection of values are not supported for explicitly defined 'step'.")
            value = self._data_to_value(value, **kwargs)
        else:
            value = self._data_to_value([value], **kwargs)

        if step is not None:
            verify_type("step", step, (float, int))
        if timestamp is not None:
            verify_type("timestamp", timestamp, (float, int))

        steps = None if step is None else [step]
        timestamps = None if timestamp is None else [timestamp] * len(value)

        ops = self._get_log_operations_from_value(value, steps=steps, timestamps=timestamps)

        with self._container.lock():
            for op in ops:
                self._enqueue_operation(op, wait)

    def extend(
        self,
        values: Collection[Data],
        steps: Optional[Collection[float]] = None,
        timestamps: Optional[Collection[float]] = None,
        wait: bool = False,
        **kwargs,
    ) -> None:
        value = self._data_to_value(values, **kwargs)

        if steps is not None:
            verify_type("step", steps, (float, int))
            if len(steps) != len(values):
                raise ValueError("Mismatch in len")
        if timestamps is not None:
            verify_type("timestamp", timestamps, (float, int))
            if len(timestamps) != len(values):
                raise ValueError("Mismatch in len")

        ops = self._get_log_operations_from_value(value, steps=steps, timestamps=timestamps)

        with self._container.lock():
            for op in ops:
                self._enqueue_operation(op, wait)

    def _clear_impl(self, wait: bool = False) -> None:
        op = self._get_clear_operation()
        with self._container.lock():
            self._enqueue_operation(op, wait)
