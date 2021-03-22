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
from typing import Union, Iterable, Optional

from neptune.new.attributes.attribute import Attribute
from neptune.new.internal.operation import UploadFileSet, DeleteFiles
from neptune.new.internal.utils import verify_type, verify_collection_type
from neptune.new.types.file_set import FileSet as FileSetVal


# pylint: disable=protected-access


class FileSet(Attribute):

    def assign(self, value: Union[FileSetVal, str, Iterable[str]], wait: bool = False) -> None:
        verify_type("value", value, (FileSetVal, str, Iterable))
        if isinstance(value, FileSetVal):
            value = value.file_globs
        elif isinstance(value, str):
            value = [value]
        else:
            verify_collection_type("value", value, str)
        self._enqueue_upload_operation(value, reset=True, wait=wait)

    def upload_files(self, globs: Union[str, Iterable[str]], wait: bool = False) -> None:
        if isinstance(globs, str):
            globs = [globs]
        else:
            verify_collection_type("globs", globs, str)
        self._enqueue_upload_operation(globs, reset=False, wait=wait)

    def delete_files(self, paths: Union[str, Iterable[str]], wait: bool = False) -> None:
        if isinstance(paths, str):
            paths = [paths]
        else:
            verify_collection_type("paths", paths, str)
        with self._run.lock():
            self._enqueue_operation(DeleteFiles(self._path, set(paths)), wait)

    def _enqueue_upload_operation(self, globs: Iterable[str], reset: bool, wait: bool):
        with self._run.lock():
            abs_file_globs = list(os.path.abspath(file_glob) for file_glob in globs)
            self._enqueue_operation(UploadFileSet(self._path, abs_file_globs, reset=reset), wait)

    def download(self, destination: Optional[str] = None) -> None:
        verify_type("destination", destination, (str, type(None)))
        self._backend.download_file_set(self._run_uuid, self._path, destination)
