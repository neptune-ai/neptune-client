#
# Copyright (c) 2021, Neptune Labs Sp. z o.o.
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

from neptune.new.internal.container_type import ContainerType
from neptune.new.internal.exceptions import NeptuneInternalException


class ContainerTypeFile:
    FILE_NAME = "container_type"

    def __init__(self, dir_path: Path, default_container_type: ContainerType = None):
        self._file = dir_path / self.FILE_NAME
        self._container_type = self.check_container_type(default_container_type)

    @property
    def container_type(self) -> ContainerType:
        return self._container_type

    def check_container_type(
        self, default_container_type: ContainerType
    ) -> ContainerType:
        """Make sure that queue will serve requested `default_container_type`
        or analyze container_type based on information stored on disk."""
        if self._file.exists():
            with open(self._file, "r") as f:
                # Information about type is stored on disk
                return ContainerType(f.read())

        # No information about type stored on disk
        return default_container_type

    def save(self):
        """Saves information regarding container_type in queue directory"""
        with open(self._file, "w") as f:
            f.write(self._container_type.value)
