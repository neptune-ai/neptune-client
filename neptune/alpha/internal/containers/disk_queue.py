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
import os
from typing import TypeVar, List, Callable, Optional

from neptune.alpha.exceptions import MalformedOperation
from neptune.alpha.internal.containers.storage_queue import StorageQueue
from neptune.alpha.internal.utils.json_file_splitter import JsonFileSplitter

T = TypeVar('T')


class DiskQueue(StorageQueue[T]):

    # NOTICE: This class is thread-safe as long as there is only one consumer and one producer.

    def __init__(
            self,
            dir_path: str,
            log_files_name: str,
            to_dict: Callable[[T], dict],
            from_dict: Callable[[dict], T],
            max_file_size: int = 64 * 1024**2):
        self._dir_path = dir_path
        self._log_files_name = log_files_name
        self._to_dict = to_dict
        self._from_dict = from_dict
        self._max_file_size = max_file_size

        try:
            os.makedirs(self._dir_path)
        except FileExistsError:
            pass

        self._read_file_idx = 0
        self._write_file_idx = 0
        self._writer = open(self._current_write_log_file(), "a")
        self._reader = JsonFileSplitter(self._current_read_log_file())
        self._file_size = 0

    def put(self, obj: T) -> None:
        _json = json.dumps(self._to_dict(obj))
        if self._file_size + len(_json) > self._max_file_size:
            self._writer.close()
            self._write_file_idx += 1
            self._writer = open(self._current_write_log_file(), "a")
            self._file_size = 0
        self._writer.write(_json + "\n")
        self._file_size += len(_json) + 1

    def get(self, dry_run=False) -> Optional[T]:
        _json = self._reader.get()
        if not _json:
            if self._read_file_idx >= self._write_file_idx:
                return None
            self._reader.close()
            if not dry_run:
                os.remove(self._current_read_log_file())
            self._read_file_idx += 1
            self._reader = JsonFileSplitter(self._current_read_log_file())
            return self.get()
        try:
            return self._from_dict(_json)
        except Exception as e:
            raise MalformedOperation from e

    def get_batch(self, size: int, dry_run=False) -> List[T]:
        ret = []
        for _ in range(0, size):
            obj = self.get(dry_run=dry_run)
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

    def _current_read_log_file(self) -> str:
        return "{}/{}-{}.log".format(self._dir_path, self._log_files_name, self._read_file_idx)

    def _current_write_log_file(self) -> str:
        return "{}/{}-{}.log".format(self._dir_path, self._log_files_name, self._write_file_idx)
