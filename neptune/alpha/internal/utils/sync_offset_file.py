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

from pathlib import Path
from typing import Optional


class SyncOffsetFile:
    def __init__(self, experiment_path: Path):
        offset_file_path = experiment_path / 'offset'
        mode = 'r+' if offset_file_path.exists() else 'w+'
        self._file = open(offset_file_path, mode)

    def write(self, offset: int) -> None:
        self._file.seek(0)
        self._file.write(str(offset))
        self._file.truncate()
        self._file.flush()

    def read(self) -> Optional[int]:
        self._file.seek(0)
        content = self._file.read()
        if not content:
            return None
        return int(content)
