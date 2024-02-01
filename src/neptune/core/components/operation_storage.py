#
# Copyright (c) 2024, Neptune Labs Sp. z o.o.
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

from neptune.core.components.abstract import Resource

UPLOAD_PATH: str = "upload_path"


class OperationStorage(Resource):
    def __init__(self, data_path: Path):
        self._data_path = data_path

        # initialize upload directory
        os.makedirs(self.upload_path, exist_ok=True)

    @property
    def data_path(self) -> Path:
        return self._data_path

    @property
    def upload_path(self) -> Path:
        return self._data_path / UPLOAD_PATH

    def cleanup(self) -> None:
        shutil.rmtree(self.upload_path, ignore_errors=True)
