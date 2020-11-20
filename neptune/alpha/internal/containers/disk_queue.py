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
from threading import Event
from typing import TypeVar, List, Callable, Optional

from neptune.alpha.exceptions import MalformedOperation
from neptune.alpha.internal.containers.storage_queue import StorageQueue
from neptune.alpha.internal.utils.json_file_splitter import JsonFileSplitter
from neptune.alpha.internal.utils.sync_offset_file import SyncOffsetFile

T = TypeVar('T')

_logger = logging.getLogger(__name__)


class DiskQueue(StorageQueue[T]):

    # NOTICE: This class is thread-safe as long as there is only one consumer and one producer.

    def __init__(
            self,
            dir_path: str,
            log_files_name: str,
            to_dict: Callable[[T], dict],
            from_dict: Callable[[dict], T],
            version_getter: [Callable[[T], int]],
            max_file_size: int = 64 * 1024**2):
        self._dir_path = dir_path
        self._log_files_name = log_files_name
        self._to_dict = to_dict
        self._from_dict = from_dict
        self._version_getter = version_getter
        self._max_file_size = max_file_size
        self._event_empty = Event()
        self._event_empty.set()
        self._last_ack_file = SyncOffsetFile("last_ack_version")
        self._last_put_file = SyncOffsetFile("last_put_version")

        try:
            os.makedirs(self._dir_path)
        except FileExistsError:
            pass

        self._read_file_idx, self._write_file_idx = self._get_first_and_last_log_file_index()

        self._writer = open(self._get_log_file(self._write_file_idx), "a")
        self._reader = JsonFileSplitter(self._get_log_file(self._read_file_idx))
        self._file_size = 0

    def put(self, obj: T) -> None:
        self._event_empty.clear()
        _json = json.dumps(self._to_dict(obj))
        if self._file_size + len(_json) > self._max_file_size:
            self._writer.close()
            self._write_file_idx += 1
            self._writer = open(self._get_log_file(self._write_file_idx), "a")
            self._file_size = 0
        self._writer.write(_json + "\n")
        self._last_put_file.write(self._version_getter(obj))
        self._file_size += len(_json) + 1

    def get(self) -> Optional[T]:
        last_ack_version = self._last_ack_file.read_local()
        while True:
            obj = self._get()
            if obj is None:
                return None
            current_version = self._version_getter(obj)
            if current_version > last_ack_version:
                if current_version > last_ack_version + 1:
                    _logger.warning("Possible data lost. Last acknowledged operation version: {}, next: {}",
                                    last_ack_version, current_version)
                return obj

    def _get(self) -> Optional[T]:
        _json = self._reader.get()
        if not _json:
            if self._read_file_idx >= self._write_file_idx:
                self._event_empty.set()
                return None
            self._reader.close()
            os.remove(self._get_log_file(self._read_file_idx))
            self._read_file_idx += 1
            self._reader = JsonFileSplitter(self._get_log_file(self._read_file_idx))
            return self.get()
        try:
            return self._from_dict(_json)
        except Exception as e:
            raise MalformedOperation from e

    def get_batch(self, size: int) -> List[T]:
        ret = []
        for _ in range(0, size):
            obj = self.get()
            if not obj:
                return ret
            ret.append(obj)
        return ret

    def flush(self):
        self._writer.flush()

    def is_overflowing(self) -> bool:
        # Some heuristic.
        return self._write_file_idx > self._read_file_idx + 1 or (
            self._write_file_idx > self._read_file_idx and self._writer.tell() >= self._max_file_size / 2)

    def close(self):
        self._reader.close()
        self._writer.close()

    def wait_for_empty(self, seconds: Optional[float] = None) -> None:
        self._event_empty.wait(seconds)

    def ack(self, version: int) -> None:
        self._last_ack_file.write(version)

    def is_empty(self) -> bool:
        return self.size() == 0

    def size(self) -> int:
        return self._last_put_file.read_local() - self._last_ack_file.read_local()

    def _get_log_file(self, index: int) -> str:
        return "{}/{}-{}.log".format(self._dir_path, self._log_files_name, index)

    def _get_first_and_last_log_file_index(self) -> (int, int):
        log_files = glob("{}/{}-*.log".format(self._dir_path, self._log_files_name))
        if not log_files:
            return 0, 0
        log_indices = [int(file[len(self._dir_path) + 1:-4]) for file in log_files]
        return min(log_indices), max(log_indices)
