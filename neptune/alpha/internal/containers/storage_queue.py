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
from typing import Generic, TypeVar, List, Optional, Tuple

T = TypeVar('T')


class StorageQueue(Generic[T]):

    # NOTICE: All implementations should be thread-safe as long as there is only one consumer and one producer.

    @abc.abstractmethod
    def put(self, obj: T) -> int:
        pass

    @abc.abstractmethod
    def get(self) -> Tuple[Optional[T], int]:
        pass

    @abc.abstractmethod
    def get_batch(self, size: int) -> Tuple[Optional[List[T]], int]:
        pass

    @abc.abstractmethod
    def flush(self) -> None:
        pass

    @abc.abstractmethod
    def close(self):
        pass

    @abc.abstractmethod
    def wait_for_empty(self, seconds: Optional[float] = None) -> None:
        pass

    @abc.abstractmethod
    def ack(self, version: int) -> None:
        pass

    @abc.abstractmethod
    def is_empty(self) -> bool:
        pass

    @abc.abstractmethod
    def size(self) -> int:
        pass
