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
import os
from typing import Union

from neptune.alpha.internal.utils import verify_type

from neptune.alpha.internal.operation import UploadFile
from neptune.alpha.types.atoms.file import File as FileVal
from neptune.alpha.variables.atoms.atom import Atom

# pylint: disable=protected-access


class File(Atom):

    def assign(self, value: Union[FileVal, str], wait: bool = False) -> None:
        verify_type("value", value, (FileVal, str))
        if isinstance(value, FileVal):
            value = value.file_path
        with self._experiment.lock():
            self._enqueue_operation(UploadFile(self._path, os.path.abspath(value)), wait)

    def save(self, path: str, wait: bool = False) -> None:
        verify_type("path", path, str)
        self.assign(FileVal(path), wait)
