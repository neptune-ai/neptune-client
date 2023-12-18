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
__all__ = ["OperationStorage"]

import os
import shutil
from pathlib import Path

from neptune.internal.utils.files import remove_parent_folder_if_allowed

UPLOAD_PATH: str = "upload_path"


class OperationStorage:
    def __init__(self, data_path: Path):
        self._data_path = data_path.resolve()

        # initialize directory
        os.makedirs(data_path / UPLOAD_PATH, exist_ok=True)

    @property
    def data_path(self) -> Path:
        return self._data_path

    @property
    def upload_path(self) -> Path:
        return self.data_path / "upload_path"

    def cleanup(self) -> None:
        shutil.rmtree(self.data_path, ignore_errors=True)
        remove_parent_folder_if_allowed(self.data_path)
