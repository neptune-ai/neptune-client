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
from typing import Generic, TypeVar, List

T = TypeVar('T')


class StorageQueue(Generic[T]):

    # NOTICE: All implementations should be thread-safe as long as there is only one consumer and one producer.

    @abc.abstractmethod
    def put(self, obj: T) -> None:
        pass

    @abc.abstractmethod
    def get(self) -> T:
        pass

    @abc.abstractmethod
    def get_batch(self, size: int) -> List[T]:
        pass

    @abc.abstractmethod
    def flush(self) -> None:
        pass

    @abc.abstractmethod
    def is_overflowing(self) -> bool:
        pass

    @abc.abstractmethod
    def close(self):
        pass
