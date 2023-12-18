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
__all__ = ["DiskQueue", "QueueElement"]

import json
import logging
import os
import threading
from dataclasses import dataclass
from glob import glob
from pathlib import Path
from time import time
from types import TracebackType
from typing import (
    Callable,
    Generic,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
    TYPE_CHECKING,
)

from neptune.core.components.abstract import WithResources
from neptune.core.components.queue.json_file_splitter import JsonFileSplitter
from neptune.core.components.queue.sync_offset_file import SyncOffsetFile
from neptune.exceptions import MalformedOperation

if TYPE_CHECKING:
    from neptune.core.components.abstract import Resource

T = TypeVar("T")
Timestamp = float

_logger = logging.getLogger(__name__)


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
    ):
        self._data_path: Path = data_path.resolve(strict=False)
        self._to_dict: Callable[[T], dict] = to_dict
        self._from_dict: Callable[[dict], T] = from_dict
        self._max_file_size: int = max_file_size
        self._max_batch_size_bytes: int = max_batch_size_bytes or int(
            os.environ.get("NEPTUNE_MAX_BATCH_SIZE_BYTES") or str(DEFAULT_MAX_BATCH_SIZE_BYTES)
        )

        self._last_ack_file = SyncOffsetFile(self._data_path / "last_ack_version")
        self._last_put_file = SyncOffsetFile(self._data_path / "last_put_version")

        (
            self._read_file_version,
            self._write_file_version,
        ) = self._get_first_and_last_log_file_version()
        self._writer = open(self._get_log_file(self._write_file_version), "a")
        self._reader = JsonFileSplitter(self._get_log_file(self._read_file_version))
        self._file_size = 0
        self._should_skip_to_ack = True

        self._empty_cond = threading.Condition(lock)

    @property
    def resources(self) -> Tuple["Resource", ...]:
        return (
            self._last_ack_file,
            self._last_put_file,
        )

    def put(self, obj: T) -> int:
        version = self._last_put_file.read_local() + 1
        _json = json.dumps(self._serialize(obj=obj, version=version, at=time()))
        if self._file_size + len(_json) > self._max_file_size:
            old_writer = self._writer
            self._writer = open(self._get_log_file(version), "a")
            old_writer.flush()
            old_writer.close()
            self._file_size = 0
            self._write_file_version = version
        self._writer.write(_json + "\n")
        self._last_put_file.write(version)
        self._file_size += len(_json) + 1
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

    def close(self) -> None:
        super().close()
        self._writer.flush()
        self._writer.close()
        self._reader.close()

    def _get(self) -> Optional[QueueElement[T]]:
        _json, size = self._reader.get_with_size()
        if not _json:
            if self._read_file_version >= self._write_file_version:
                return None
            self._reader.close()
            self._read_file_version = self._next_log_file_version(self._read_file_version)
            # TODO: Track new resource
            self._reader = JsonFileSplitter(self._get_log_file(self._read_file_version))
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

        log_versions = self._get_all_log_file_versions()
        for i in range(0, len(log_versions) - 1):
            if log_versions[i + 1] <= version:
                filename = self._get_log_file(log_versions[i])
                try:
                    os.remove(filename)
                except FileNotFoundError:
                    # not really a problem
                    pass
                except Exception:
                    _logger.exception("Cannot remove queue file %s", filename)
            else:
                break

        with self._empty_cond:
            if self.is_empty():
                self._empty_cond.notify_all()

    def is_empty(self) -> bool:
        return self.size() == 0

    def size(self) -> int:
        return self._last_put_file.read_local() - self._last_ack_file.read_local()

    def _get_log_file(self, index: int) -> str:
        return "{}/data-{}.log".format(self._data_path, index)

    def _get_all_log_file_versions(self) -> Union[Tuple[int, int], List[int]]:
        log_files = glob("{}/data-*.log".format(self._data_path))
        if not log_files:
            return 1, 1
        return sorted([int(file[len(str(self._data_path)) + 6 : -4]) for file in log_files])

    def _get_first_and_last_log_file_version(self) -> Tuple[int, int]:
        log_versions = self._get_all_log_file_versions()
        return min(log_versions), max(log_versions)

    def _next_log_file_version(self, version: int) -> int:
        log_versions = self._get_all_log_file_versions()
        for i, val in enumerate(log_versions):
            if val == version:
                return log_versions[i + 1]
        raise ValueError("Missing log file with version > {}".format(version))

    def _serialize(self, obj: T, version: int, at: Optional[Timestamp] = None) -> dict:
        return {"obj": self._to_dict(obj), "version": version, "at": at}

    def _deserialize(self, data: dict) -> Tuple[T, int, Optional[Timestamp]]:
        return self._from_dict(data["obj"]), data["version"], data.get("at")

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        self.close()
        if self.is_empty():
            self.clean()
