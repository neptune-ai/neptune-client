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
__all__ = ["QueueElement", "DiskQueue"]

import json
import os
import threading
from collections import deque
from dataclasses import dataclass
from glob import glob
from pathlib import Path
from time import time
from types import TracebackType
from typing import (
    TYPE_CHECKING,
    Callable,
    Deque,
    Generic,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
)

from neptune.core.components.abstract import WithResources
from neptune.core.components.queue.json_file_splitter import JsonFileSplitter
from neptune.core.components.queue.log_file import LogFile
from neptune.core.components.queue.sync_offset_file import SyncOffsetFile
from neptune.exceptions import MalformedOperation
from neptune.internal.utils.logger import get_logger

if TYPE_CHECKING:
    from neptune.core.components.abstract import Resource


T = TypeVar("T")
Timestamp = float

_logger = get_logger()


DEFAULT_MAX_BATCH_SIZE_BYTES = 100 * 1024**2


@dataclass
class QueueElement(Generic[T]):
    obj: T
    ver: int
    size: int
    at: Optional[Timestamp] = None


# NOTICE: This class is thread-safe as long as there is only one consumer and one producer.
class DiskQueue(WithResources, Generic[T]):
    def __init__(
        self,
        data_path: Path,
        to_dict: Callable[[T], dict],
        from_dict: Callable[[dict], T],
        lock: threading.RLock,
        max_file_size: int = 64 * 1024**2,
        max_batch_size_bytes: Optional[int] = None,
        extension: str = "log",
    ) -> None:
        self._data_path: Path = data_path.resolve()
        self._to_dict: Callable[[T], dict] = to_dict
        self._from_dict: Callable[[dict], T] = from_dict
        self._max_file_size: int = max_file_size
        self._max_batch_size_bytes: int = max_batch_size_bytes or int(
            os.environ.get("NEPTUNE_MAX_BATCH_SIZE_BYTES") or str(DEFAULT_MAX_BATCH_SIZE_BYTES)
        )
        self._extension: str = extension

        self._last_ack_file = SyncOffsetFile(data_path / "last_ack_version", default=0)
        self._last_put_file = SyncOffsetFile(data_path / "last_put_version", default=0)

        self._log_files: Deque[LogFile] = get_all_log_files(data_path, extension)
        self._write_file_version: int = self._log_files[-1].min_version
        self._writer = self._log_files[-1]
        self._read_file_version: int = self._log_files[0].min_version
        self._reader = JsonFileSplitter(self._log_files[0].file_path)

        self._should_skip_to_ack = True

        self._empty_cond = threading.Condition(lock)

    @property
    def data_path(self) -> Path:
        return self._data_path

    @property
    def resources(self) -> Tuple["Resource", ...]:
        return (
            self._last_put_file,
            self._last_ack_file,
        ) + tuple(self._log_files)

    def put(self, obj: T) -> int:
        version = self._last_put_file.read_local() + 1
        serialized_obj = json.dumps(self._serialize(obj=obj, version=version, at=time()))

        self._create_new_writer_if_file_size_exceeded(len(serialized_obj), version)

        self._writer.write(serialized_obj)
        self._last_put_file.write(version)

        return version

    def get(self) -> Optional[QueueElement[T]]:
        if self._should_skip_to_ack:
            return self._skip_and_get()
        else:
            return self._get()

    def _skip_and_get(self) -> Optional[QueueElement[T]]:
        ack_version = self._last_ack_file.read_local()
        while True:
            top_element = self._get()
            if top_element is None:
                return None
            if top_element.ver > ack_version:
                self._should_skip_to_ack = False
                if top_element.ver > ack_version + 1:
                    _logger.warning(
                        "Possible data loss. Last acknowledged operation version: %d, next: %d",
                        ack_version,
                        top_element.ver,
                    )
                return top_element

    def _get(self) -> Optional[QueueElement[T]]:
        _json, size = self._reader.get_with_size()
        if not _json:
            if self._read_file_version >= self._write_file_version:
                return None

            self._reader.close()
            for log_file in self._log_files:
                if log_file.min_version > self._read_file_version:
                    self._read_file_version = log_file.min_version
                    self._reader = JsonFileSplitter(log_file.file_path)
                    break

            # It is safe. Max recursion level is 2.
            return self._get()
        try:
            obj, ver, at = self._deserialize(_json)
            return QueueElement[T](obj, ver, size, at)
        except Exception as e:
            raise MalformedOperation from e

    def get_batch(self, size: int) -> List[QueueElement[T]]:
        if self._should_skip_to_ack:
            first = self._skip_and_get()
        else:
            first = self._get()
        if not first:
            return []

        ret = [first]
        cur_batch_size = first.size
        for _ in range(0, size - 1):
            if cur_batch_size >= self._max_batch_size_bytes:
                break
            next_obj = self._get()
            if not next_obj:
                break

            cur_batch_size += next_obj.size
            ret.append(next_obj)
        return ret

    def wait_for_empty(self, seconds: Optional[float] = None) -> bool:
        with self._empty_cond:
            return self._empty_cond.wait_for(self.is_empty, timeout=seconds)

    def ack(self, version: int) -> None:
        self._last_ack_file.write(version)
        self._clean_log_files_up_to(version)

        with self._empty_cond:
            if self.is_empty():
                self._empty_cond.notify_all()

    def _create_new_writer_if_file_size_exceeded(self, size: int, version: int) -> None:
        if self._writer.file_size + size > self._max_file_size:
            old_writer = self._writer
            self._writer = LogFile(self._data_path, version, extension=self._extension)
            old_writer.flush()
            old_writer.close()
            self._write_file_version = version
            self._log_files.append(self._writer)

    def _clean_log_files_up_to(self, version: int) -> None:
        log_versions = [log.min_version for log in self._log_files]

        for current_min_version, next_min_version in zip(log_versions, log_versions[1:]):
            if next_min_version <= version:
                self._log_files.popleft().cleanup()

    def is_empty(self) -> bool:
        return self.size() == 0

    def size(self) -> int:
        return self._last_put_file.read_local() - self._last_ack_file.read_local()

    def _serialize(self, obj: T, version: int, at: Optional[Timestamp] = None) -> dict:
        return {"obj": self._to_dict(obj), "version": version, "at": at}

    def _deserialize(self, data: dict) -> Tuple[T, int, Optional[Timestamp]]:
        return self._from_dict(data["obj"]), data["version"], data.get("at")

    def close(self) -> None:
        self._reader.close()
        super().close()

    def __enter__(self) -> "DiskQueue[T]":
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        self.flush()
        self.close()
        if self.is_empty():
            self.cleanup()


def get_all_log_files(data_path: Path, extension: str) -> Deque[LogFile]:
    local_data_files = glob(f"{data_path}/data-*.{extension}")

    if not local_data_files:
        return deque([LogFile(data_path, 1, extension=extension)])

    sorted_local_data_files = sorted(
        local_data_files, key=lambda file_path: extract_version_from_file_name(Path(file_path), extension)
    )

    return deque(
        [
            LogFile(data_path, extract_version_from_file_name(Path(file_path), extension), extension=extension)
            for file_path in sorted_local_data_files
        ]
    )


def extract_version_from_file_name(file_path: Path, extension: str) -> int:
    return int(file_path.name.split("-")[-1][: -len(extension) - 1])
