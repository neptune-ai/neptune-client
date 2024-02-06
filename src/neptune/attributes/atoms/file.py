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
__all__ = ["File"]

from typing import Optional

from neptune.attributes.atoms.atom import Atom
from neptune.internal.operation import UploadFile
from neptune.internal.utils import verify_type
from neptune.types.atoms.file import File as FileVal
from neptune.typing import ProgressBarType


class File(Atom):
    def assign(self, value: FileVal, *, wait: bool = False) -> None:
        verify_type("value", value, FileVal)

        operation = UploadFile.of_file(
            value=value,
            attribute_path=self._path,
            operation_storage=self._container._op_processor.operation_storage,
        )

        with self._container.lock():
            self._enqueue_operation(operation, wait=wait)

    def upload(self, value, *, wait: bool = False) -> None:
        self.assign(FileVal.create_from(value), wait=wait)

    def download(
        self,
        destination: Optional[str] = None,
        progress_bar: Optional[ProgressBarType] = None,
    ) -> None:
        verify_type("destination", destination, (str, type(None)))
        self._backend.download_file(self._container_id, self._container_type, self._path, destination, progress_bar)

    def fetch_extension(self) -> str:
        val = self._backend.get_file_attribute(self._container_id, self._container_type, self._path)
        return val.ext
