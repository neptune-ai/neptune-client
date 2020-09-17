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
from json import JSONDecodeError
from typing import Optional


class JsonFileSplitter:

    # TODO: experiment with larger buffer sizes
    BUFFER_SIZE = 4096

    def __init__(self, file_path: str):
        self._file = open(file_path, "r")
        self._decoder = json.JSONDecoder(strict=False)
        self._buffer = ""
        self._start_pos = 0

    def close(self) -> None:
        self._file.close()

    def get(self) -> Optional[dict]:
        try:
            return self._decode()
        except (JSONDecodeError, ValueError):
            self._read_data()
            try:
                return self._decode()
            except (JSONDecodeError, ValueError):
                return None

    def _read_data(self):
        data = self._file.read(self.BUFFER_SIZE)
        if not data:
            return
        self._buffer = self._buffer[self._start_pos:] + data
        self._start_pos = 0

    def _decode(self):
        self._start_pos = self._buffer.index("{", self._start_pos)
        data, end = self._decoder.raw_decode(self._buffer, self._start_pos)
        self._start_pos = end
        return data
