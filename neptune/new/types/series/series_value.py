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

from typing import TypeVar, Generic

T = TypeVar('T')


class SeriesValue(Generic[T]):

    def __init__(self, step: float, value: T, timestamp: float):
        self._step = step
        self._value = value
        self._timestamp = timestamp

    @property
    def step(self) -> float:
        return self._step

    @step.setter
    def step(self, step: float):
        self._step = step

    @property
    def value(self) -> T:
        return self._value

    @value.setter
    def value(self, value: T):
        self._value = value

    @property
    def timestamp(self) -> float:
        return self._timestamp

    @timestamp.setter
    def timestamp(self, timestamp: float):
        self._timestamp = timestamp
