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

from neptune.core.components.abstract import Resource

UPLOAD_DIRECTORY_NAME: str = "upload_path"


class OperationStorage(Resource):
    def __init__(self, data_path: Path) -> None:
        self._upload_path = (data_path / UPLOAD_DIRECTORY_NAME).resolve(strict=False)
        os.makedirs(self._upload_path, exist_ok=True)

    @property
    def upload_path(self) -> Path:
        return self._upload_path

    def clean(self) -> None:
        shutil.rmtree(self.upload_path, ignore_errors=True)
