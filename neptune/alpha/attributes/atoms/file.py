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
from io import IOBase, TextIOBase
from typing import Union, Optional

from neptune.alpha.internal.utils import verify_type

from neptune.alpha.internal.operation import UploadFile
from neptune.alpha.types.atoms.file import File as FileVal
from neptune.alpha.attributes.atoms.atom import Atom

# pylint: disable=protected-access


class File(Atom):

    def assign(self, value: Union[FileVal, str, IOBase], wait: bool = False) -> None:
        verify_type("value", value, (FileVal, str, IOBase))
        if isinstance(value, FileVal):
            if value.file_path is not None:
                operation = UploadFile(self._path,
                                       file_name=os.path.basename(value.file_path),
                                       file_path=os.path.abspath(value.file_path))
            else:
                operation = UploadFile(self._path,
                                       file_name="stream.txt" if isinstance(value.stream, TextIOBase) else "stream.bin",
                                       stream=value.stream)
        elif isinstance(value, IOBase):
            operation = UploadFile(self._path,
                                   file_name="stream.txt" if isinstance(value, TextIOBase) else "stream.bin",
                                   stream=value)
        else:
            operation = UploadFile(self._path, file_name=os.path.basename(value), file_path=os.path.abspath(value))

        with self._experiment.lock():
            self._enqueue_operation(operation, wait)

    def save(self, path: str, wait: bool = False) -> None:
        verify_type("path", path, str)
        self.assign(FileVal(file_path=path), wait)

    def download(self, destination: Optional[str] = None) -> None:
        verify_type("destination", destination, (str, type(None)))
        self._backend.download_file(self._experiment_uuid, self._path, destination)
