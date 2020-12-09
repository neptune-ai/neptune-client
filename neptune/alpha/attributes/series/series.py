#
# Copyright (c) 2020, Neptune Labs Sp. z o.o.
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
from typing import Optional, TypeVar, Generic, Union, Iterable

from neptune.alpha.internal.operation import Operation

from neptune.alpha.internal.utils import verify_type, is_collection

from neptune.alpha.types.series.series import Series as SeriesVal

from neptune.alpha.attributes.attribute import Attribute

Val = TypeVar('Val', bound=SeriesVal)
Data = TypeVar('Data')


class Series(Attribute, Generic[Val, Data]):

    def assign(self, value: Val, wait: bool = False) -> None:
        self._verify_value_type(value)
        self._assign_impl(value, wait)

    def clear(self, wait: bool = False) -> None:
        self._clear_impl(wait)

    @abc.abstractmethod
    def _get_log_operation_from_value(self, value: Val, step: Optional[float], timestamp: float) -> Operation:
        pass

    @abc.abstractmethod
    def _get_log_operation_from_data(self,
                                     data_list: Iterable[Data],
                                     step: Optional[float],
                                     timestamp: float) -> Operation:
        pass

    # pylint: disable=unused-argument
    # pylint: disable=assignment-from-none
    def _get_config_operation_from_value(self, value: Val) -> Optional[Operation]:
        return None

    @abc.abstractmethod
    def _get_clear_operation(self) -> Operation:
        pass

    def _verify_value_type(self, value) -> None:
        pass

    def _verify_data_type(self, data) -> None:
        pass

    def _assign_impl(self, value: Val, wait: bool = False) -> None:
        clear_op = self._get_clear_operation()
        config_op = self._get_config_operation_from_value(value)
        with self._experiment.lock():
            if config_op:
                self._enqueue_operation(config_op, wait=False)
            if not value.values:
                self._enqueue_operation(clear_op, wait=wait)
            else:
                self._enqueue_operation(clear_op, wait=False)
                ts = time.time()
                self._enqueue_operation(self._get_log_operation_from_value(value, None, ts), wait=wait)

    def log(self,
            value: Union[Data, Iterable],
            step: Optional[float] = None,
            timestamp: Optional[float] = None,
            wait: bool = False) -> None:
        if is_collection(value):
            if step is not None and len(value) > 1:
                raise ValueError("Collection of values are not supported for explicitly defined 'step'.")
            value = list(value)
        else:
            value = [value]

        self._verify_data_type(value)

        if step is not None:
            verify_type("step", step, (float, int))
        if timestamp is not None:
            verify_type("timestamp", timestamp, (float, int))

        if not timestamp:
            timestamp = time.time()

        op = self._get_log_operation_from_data(value, step, timestamp)

        with self._experiment.lock():
            self._enqueue_operation(op, wait)

    def _clear_impl(self, wait: bool = False) -> None:
        op = self._get_clear_operation()
        with self._experiment.lock():
            self._enqueue_operation(op, wait)
