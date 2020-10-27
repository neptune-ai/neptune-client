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

class ExperimentOffset:
    def __init__(self, experiment_path: Path):
        self._file = open(experiment_path / 'offset', 'a+b')

    def write(self, offset):
        self._file.seek(0)
        self._file.write(offset.to_bytes(4, byteorder='big'))
        self._file.flush()

    def read(self):
        self._file.seek(0)
        return int.from_bytes(self._file.read(), byteorder='big')
