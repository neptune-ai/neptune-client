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
from io import IOBase
from typing import Optional

from neptune.alpha.internal.utils import verify_type, base64_encode

from neptune.alpha.internal.operation import UploadFile, UploadFileContent
from neptune.alpha.types.atoms.file import File as FileVal
from neptune.alpha.attributes.atoms.atom import Atom

# pylint: disable=protected-access


class File(Atom):

    def assign(self, value: FileVal, wait: bool = False) -> None:
        verify_type("value", value, (FileVal, IOBase))

        if value.path is not None:
            operation = UploadFile(self._path, ext=value.extension, file_path=os.path.abspath(value.path))
        elif value.content is not None:
            operation = UploadFileContent(self._path, ext=value.extension, file_content=base64_encode(value.content))
        else:
            raise ValueError("File path and content are None")

        with self._experiment.lock():
            self._enqueue_operation(operation, wait)

    def upload(self, path: str, wait: bool = False) -> None:
        verify_type("path", path, str)
        self.assign(FileVal(path=path), wait)

    def download(self, destination: Optional[str] = None, wait=True) -> None:
        verify_type("destination", destination, (str, type(None)))
        if wait:
            self._experiment.wait()
        self._backend.download_file(self._experiment_uuid, self._path, destination)
