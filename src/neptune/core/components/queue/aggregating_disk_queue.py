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

import threading
from dataclasses import dataclass
from pathlib import Path
from types import TracebackType
from typing import (
    Callable,
    Generic,
    List,
    Optional,
    Type,
    TypeVar,
)

from neptune.core.components.queue.disk_queue import (
    DiskQueue,
    QueueElement,
)

T = TypeVar("T")
Timestamp = float


@dataclass
class CategoryQueueElement(Generic[T]):
    obj: T
    category: Optional[int] = None


def to_dict_factory(to_dict: Callable[[T], dict]) -> Callable[[CategoryQueueElement[T]], dict]:
    def _to_dict(obj: CategoryQueueElement[T]) -> dict:
        return {"obj": to_dict(obj.obj), "cat": obj.category}

    return _to_dict


def from_dict_factory(from_dict: Callable[[dict], T]) -> Callable[[dict], CategoryQueueElement[T]]:
    def _from_dict(data: dict) -> CategoryQueueElement[T]:
        return CategoryQueueElement(from_dict(data["obj"]), data["cat"])

    return _from_dict


class AggregatingDiskQueue(Generic[T]):
    def __init__(
        self,
        data_path: Path,
        to_dict: Callable[[T], dict],
        from_dict: Callable[[dict], T],
        lock: threading.RLock,
        max_file_size: int = 64 * 1024**2,
        max_batch_size_bytes: int | None = None,
        extension: str = "log",
    ) -> None:
        self._disk_queue = DiskQueue[CategoryQueueElement[T]](
            data_path=data_path,
            to_dict=to_dict_factory(to_dict),
            from_dict=from_dict_factory(from_dict),
            lock=lock,
            max_file_size=max_file_size,
            max_batch_size_bytes=max_batch_size_bytes,
            extension=extension,
        )
        self._stored_element: Optional[QueueElement[CategoryQueueElement[T]]] = None

    def put(self, obj: T, category: Optional[int] = None) -> int:
        return self._disk_queue.put(CategoryQueueElement(obj, category))

    def get(self) -> Optional[QueueElement[CategoryQueueElement[T]]]:
        return self._disk_queue.get()

    def get_batch(self, size: int) -> List[QueueElement[CategoryQueueElement[T]]]:
        if self._stored_element is not None:
            first: QueueElement[CategoryQueueElement[T]] = self._stored_element
            self._stored_element = None
        else:
            possible_first = self._disk_queue.get()
            if not possible_first:
                return []
            else:
                first = possible_first

        ret = [first]
        category = first.obj.category
        cur_batch_size = first.size
        for _ in range(0, size - 1):
            if cur_batch_size >= self._disk_queue._max_batch_size_bytes:
                break
            next_obj = self._disk_queue._get()
            if not next_obj:
                break

            if next_obj.obj.category is not None:
                if category is None:
                    category = next_obj.obj.category
                elif category != next_obj.obj.category:
                    self._stored_element = next_obj
                    break

            cur_batch_size += next_obj.size
            ret.append(next_obj)
        return ret

    def __enter__(self) -> "AggregatingDiskQueue[T]":
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        self._disk_queue.__exit__(exc_type, exc_value, traceback)

    def close(self) -> None:
        self._disk_queue.close()

    def flush(self) -> None:
        self._disk_queue.flush()

    def ack(self, version: int) -> None:
        self._disk_queue.ack(version)

    def size(self) -> int:
        return self._disk_queue.size()

    def is_empty(self) -> bool:
        return self._disk_queue.is_empty()
