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
__all__ = ["SyncOffsetFile"]

import os
from pathlib import Path
from typing import IO

from neptune.core.components.abstract import Resource


class SyncOffsetFile(Resource):
    def __init__(self, path: Path, default: int = 0):
        self._path = path
        mode = "r+" if path.exists() else "w+"
        self._file: IO = open(self._path, mode)
        self._default: int = default
        self._last: int = self.read()

    @property
    def data_path(self) -> Path:
        return self._path.parent

    def write(self, offset: int) -> None:
        self._file.seek(0)
        self._file.write(str(offset))
        self._file.truncate()
        self._file.flush()
        self._last = offset

    def read(self) -> int:
        self._file.seek(0)
        content = self._file.read()
        if not content:
            return self._default
        return int(content)

    def read_local(self) -> int:
        return self._last

    def flush(self) -> None:
        self._file.flush()

    def close(self) -> None:
        self._file.close()

    def cleanup(self) -> None:
        try:
            os.remove(self._path)
        except OSError:
            pass
