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
import logging
import os
import shutil
import threading
from dataclasses import dataclass
from glob import glob
from pathlib import Path
from typing import (
    Callable,
    Generic,
    List,
    Optional,
    Tuple,
    TypeVar,
)

from neptune.new.exceptions import MalformedOperation
from neptune.new.internal.utils.json_file_splitter import JsonFileSplitter
from neptune.new.internal.utils.sync_offset_file import SyncOffsetFile

T = TypeVar("T")

_logger = logging.getLogger(__name__)


@dataclass
class QueueElement(Generic[T]):
    obj: T
    ver: int
    size: int


class DiskQueue(Generic[T]):
    # NOTICE: This class is thread-safe as long as there is only one consumer and one producer.
    DEFAULT_MAX_BATCH_SIZE_BYTES = 100 * 1024**2

    def __init__(
        self,
        dir_path: Path,
        to_dict: Callable[[T], dict],
        from_dict: Callable[[dict], T],
        lock: threading.RLock,
        max_file_size: int = 64 * 1024**2,
        max_batch_size_bytes: int = None,
    ):
        self._dir_path = dir_path.resolve()
        self._to_dict = to_dict
        self._from_dict = from_dict
        self._max_file_size = max_file_size
        self._max_batch_size_bytes = max_batch_size_bytes or int(
            os.environ.get("NEPTUNE_MAX_BATCH_SIZE_BYTES") or str(self.DEFAULT_MAX_BATCH_SIZE_BYTES)
        )

        try:
            os.makedirs(self._dir_path)
        except FileExistsError:
            pass

        self._last_ack_file = SyncOffsetFile(dir_path / "last_ack_version", default=0)
        self._last_put_file = SyncOffsetFile(dir_path / "last_put_version", default=0)

        (
            self._read_file_version,
            self._write_file_version,
        ) = self._get_first_and_last_log_file_version()
        self._writer = open(self._get_log_file(self._write_file_version), "a")
        self._reader = JsonFileSplitter(self._get_log_file(self._read_file_version))
        self._file_size = 0
        self._should_skip_to_ack = True

        self._empty_cond = threading.Condition(lock)

    def put(self, obj: T) -> int:
        version = self._last_put_file.read_local() + 1
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
            self._read_file_version = self._next_log_file_version(self._read_file_version)
            self._reader = JsonFileSplitter(self._get_log_file(self._read_file_version))
            # It is safe. Max recursion level is 2.
            return self._get()
        try:
            obj, ver = self._deserialize(_json)
            return QueueElement[T](obj, ver, size)
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

    def flush(self):
        self._writer.flush()
        self._last_ack_file.flush()
        self._last_put_file.flush()

    def close(self):
        """
        Close and remove underlying files if queue is empty
        """
        self._reader.close()
        self._writer.close()
        self._last_ack_file.close()
        self._last_put_file.close()

        if self.is_empty():
            self._remove_data()

    def _remove_data(self):
        path = self._dir_path
        shutil.rmtree(path, ignore_errors=True)

        parent = path.parent

        files = os.listdir(parent)
        if len(files) == 0:
            try:
                os.rmdir(parent)
            except OSError:
                _logger.info(f"Cannot remove directory: {parent}")

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
        return "{}/data-{}.log".format(self._dir_path, index)

    def _get_all_log_file_versions(self):
        log_files = glob("{}/data-*.log".format(self._dir_path))
        if not log_files:
            return 1, 1
        return sorted([int(file[len(str(self._dir_path)) + 6 : -4]) for file in log_files])

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
        return {"obj": self._to_dict(obj), "version": version}

    def _deserialize(self, data: dict) -> Tuple[T, int]:
        return self._from_dict(data["obj"]), data["version"]

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.flush()
        self.close()
