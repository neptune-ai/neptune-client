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
from pathlib import Path

from neptune.core.components.abstract import Resource
from neptune.internal.utils.logger import get_logger

logger = get_logger()


class LogFile(Resource):
    def __init__(self, data_path: Path, min_version: int, extension: str = "log") -> None:
        self._data_path: Path = data_path
        self._min_version: int = min_version
        self._extension: str = extension

        self._file_size: int = 0
        if (data_path / f"data-{min_version}.{extension}").exists():
            self._file_size = self.file_path.stat().st_size

        self._writer = open(self.file_path, "a")

    @property
    def data_path(self) -> Path:
        return self._data_path

    @property
    def min_version(self) -> int:
        return self._min_version

    @property
    def file_size(self) -> int:
        return self._file_size

    @property
    def file_name(self) -> str:
        return f"data-{self._min_version}.{self._extension}"

    @property
    def file_path(self) -> Path:
        return self._data_path / self.file_name

    def write(self, data: str) -> None:
        self._writer.write(data + "\n")
        self._file_size += len(data) + 1

    def cleanup(self) -> None:
        self.close()
        try:
            self.file_path.unlink()
        except FileNotFoundError:
            pass
        except Exception:
            logger.exception("Cannot remove queue file %s", self.file_name)

    def flush(self) -> None:
        if not self._writer.closed:
            self._writer.flush()

    def close(self) -> None:
        if not self._writer.closed:
            self._writer.close()
