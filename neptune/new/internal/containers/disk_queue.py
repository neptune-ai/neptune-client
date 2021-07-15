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
import json
import logging
import os
from glob import glob
from pathlib import Path
from threading import Event
from typing import TypeVar, List, Callable, Optional, Tuple

from neptune.new.exceptions import MalformedOperation
from neptune.new.internal.containers.storage_queue import StorageQueue
from neptune.new.internal.utils.json_file_splitter import JsonFileSplitter
from neptune.new.internal.utils.sync_offset_file import SyncOffsetFile

T = TypeVar('T')

_logger = logging.getLogger(__name__)


class DiskQueue(StorageQueue[T]):

    # NOTICE: This class is thread-safe as long as there is only one consumer and one producer.

    def __init__(
            self,
            dir_path: Path,
            to_dict: Callable[[T], dict],
            from_dict: Callable[[dict], T],
            max_file_size: int = 64 * 1024**2):
        self._dir_path = dir_path.resolve()
        self._to_dict = to_dict
        self._from_dict = from_dict
        self._max_file_size = max_file_size

        try:
            os.makedirs(self._dir_path)
        except FileExistsError:
            pass

        self._last_ack_file = SyncOffsetFile(dir_path / "last_ack_version", default=0)
        self._last_put_file = SyncOffsetFile(dir_path / "last_put_version", default=0)

        self._read_file_version, self._write_file_version = self._get_first_and_last_log_file_version()
        self._writer = open(self._get_log_file(self._write_file_version), "a")
        self._reader = JsonFileSplitter(self._get_log_file(self._read_file_version))
        self._file_size = 0
        self._should_skip_to_ack = True

        self._event_empty = Event()
        if self.is_empty():
            self._event_empty.set()
        else:
            self._event_empty.clear()

    def put(self, obj: T) -> int:
        version = self._last_put_file.read_local() + 1
        self._event_empty.clear()
        _json = json.dumps(self._serialize(obj, version))
        if self._file_size + len(_json) > self._max_file_size:
            self._writer.flush()
            self._writer.close()
            self._writer = open(self._get_log_file(version), "a")
            self._file_size = 0
            self._write_file_version = version
        self._writer.write(_json + "\n")
        self._last_put_file.write(version)
        self._file_size += len(_json) + 1
        return version

    def get(self) -> Tuple[Optional[T], int]:
        if self._should_skip_to_ack:
            return self._skip_and_get()
        else:
            return self._get()

    def _skip_and_get(self) -> Tuple[Optional[T], int]:
        ack_version = self._last_ack_file.read_local()
        ver = -1
        while True:
            obj, next_ver = self._get()
            if obj is None:
                return None, ver
            ver = next_ver
            if ver > ack_version:
                self._should_skip_to_ack = False
                if ver > ack_version + 1:
                    _logger.warning("Possible data loss. Last acknowledged operation version: %d, next: %d",
                                    ack_version, ver)
                return obj, ver

    def _get(self) -> Tuple[Optional[T], int]:
        _json = self._reader.get()
        if not _json:
            if self._read_file_version >= self._write_file_version:
                return None, -1
            self._reader.close()
            self._read_file_version = self._next_log_file_version(self._read_file_version)
            self._reader = JsonFileSplitter(self._get_log_file(self._read_file_version))
            # It is safe. Max recursion level is 2.
            return self._get()
        try:
            return self._deserialize(_json)
        except Exception as e:
            raise MalformedOperation from e

    def get_batch(self, size: int) -> Tuple[List[T], int]:
        first, ver = self.get()
        if not first:
            return [], ver
        ret = [first]
        for _ in range(0, size - 1):
            obj, next_ver = self._get()
            if not obj:
                break
            ver = next_ver
            ret.append(obj)
        return ret, ver

    def flush(self):
        self._writer.flush()
        self._last_ack_file.flush()
        self._last_put_file.flush()

    def close(self):
        self._reader.close()
        self._writer.close()
        self._last_ack_file.close()
        self._last_put_file.close()

    def wait_for_empty(self, seconds: Optional[float] = None) -> None:
        self._event_empty.wait(seconds)

    def ack(self, version: int) -> None:
        self._last_ack_file.write(version)
        if self.is_empty():
            self._event_empty.set()

        log_versions = self._get_all_log_file_versions()
        for i in range(0, len(log_versions) - 1):
            if log_versions[i + 1] <= version:
                os.remove(self._get_log_file(log_versions[i]))
            else:
                break

    def is_empty(self) -> bool:
        return self.size() == 0

    def size(self) -> int:
        return self._last_put_file.read_local() - self._last_ack_file.read_local()

    def _get_log_file(self, index: int) -> str:
        return "{}/data-{}.log".format(self._dir_path, index)

    def _get_all_log_file_versions(self):
        log_files = glob("{}/data-*.log".format(self._dir_path))
        if not log_files:
            return 1, 1
        return sorted([int(file[len(str(self._dir_path)) + 6:-4]) for file in log_files])

    def _get_first_and_last_log_file_version(self) -> (int, int):
        log_versions = self._get_all_log_file_versions()
        return min(log_versions), max(log_versions)

    def _next_log_file_version(self, version: int) -> int:
        log_versions = self._get_all_log_file_versions()
        for i, val in enumerate(log_versions):
            if val == version:
                return log_versions[i + 1]
        raise ValueError("Missing log file with version > {}".format(version))

    def _serialize(self, obj: T, version: int) -> dict:
        return {
            "obj": self._to_dict(obj),
            "version": version
        }

    def _deserialize(self, data: dict) -> Tuple[T, int]:
        return self._from_dict(data["obj"]), data["version"]
