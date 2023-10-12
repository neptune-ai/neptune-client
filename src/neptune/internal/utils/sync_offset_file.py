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

from pathlib import Path
from typing import Optional


class SyncOffsetFile:
    def __init__(self, path: Path, default: int = None):
        mode = "r+" if path.exists() else "w+"
        self._file = open(path, mode)
        self._default = default
        self._last = self.read()

    def write(self, offset: int) -> None:
        self._file.seek(0)
        self._file.write(str(offset))
        self._file.truncate()
        self._file.flush()
        self._last = offset

    def read(self) -> Optional[int]:
        self._file.seek(0)
        content = self._file.read()
        if not content:
            return self._default
        return int(content)

    def read_local(self) -> Optional[int]:
        return self._last

    def flush(self):
        self._file.flush()

    def close(self):
        self._file.close()
