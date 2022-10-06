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
from typing import (
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
from neptune.new.types.series.series import Series as SeriesVal

Val = TypeVar("Val", bound=SeriesVal)
Data = TypeVar("Data")


class Series(Attribute, Generic[Val, Data]):
    def clear(self, wait: bool = False) -> None:
        self._clear_impl(wait)

    @abc.abstractmethod
    def _get_log_operations_from_value(self, value: Val, step: Optional[float], timestamp: float) -> List[Operation]:
        pass

    # pylint: disable=unused-argument
    # pylint: disable=assignment-from-none
    def _get_config_operation_from_value(self, value: Val) -> Optional[Operation]:
        return None

    @abc.abstractmethod
    def _get_clear_operation(self) -> Operation:
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
                ts = time.time()
                ops = self._get_log_operations_from_value(value, None, ts)
                for op in ops:
                    self._enqueue_operation(op, wait=wait)

    def log(
        self,
        value: Union[Data, Iterable[Data]],
        step: Optional[float] = None,
        timestamp: Optional[float] = None,
        wait: bool = False,
        **kwargs
    ) -> None:
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

        if not timestamp:
            timestamp = time.time()

        ops = self._get_log_operations_from_value(value, step, timestamp)

        with self._container.lock():
            for op in ops:
                self._enqueue_operation(op, wait)

    def _clear_impl(self, wait: bool = False) -> None:
        op = self._get_clear_operation()
        with self._container.lock():
            self._enqueue_operation(op, wait)
