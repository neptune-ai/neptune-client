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
__all__ = [
    "OperationStorage",
]

import os
import shutil
from pathlib import Path
from typing import Union

from neptune.constants import NEPTUNE_DATA_DIRECTORY
from neptune.internal.container_type import ContainerType
from neptune.internal.id_formats import UniqueId
from neptune.internal.utils.logger import logger


class OperationStorage:
    def __init__(self, data_path: Union[str, Path]):
        if isinstance(data_path, Path):
            self._data_path = data_path
        else:
            self._data_path = Path(data_path)
        # initialize directories
        os.makedirs(self.data_path, exist_ok=True)
        os.makedirs(self.upload_path, exist_ok=True)

        cwd = Path(os.getcwd())
        self._data_path_abs = cwd / self._data_path

    @property
    def data_path(self) -> Path:
        return self._data_path

    @property
    def data_path_abs(self) -> Path:
        return self._data_path_abs

    @property
    def upload_path(self) -> Path:
        return self.data_path / "upload_path"

    @property
    def upload_path_abs(self) -> Path:
        return self.data_path_abs / "upload_path"

    @staticmethod
    def _get_container_dir(type_dir: str, container_id: UniqueId, container_type: ContainerType):
        return f"{NEPTUNE_DATA_DIRECTORY}/{type_dir}/{container_type.create_dir_name(container_id)}"

    def close(self):
        shutil.rmtree(self.data_path, ignore_errors=True)

        try:
            parent = self.data_path.parent

            files = os.listdir(parent)
        except FileNotFoundError:
            # cwd is not the same as the script directory
            parent = self.data_path_abs.parent

            files = os.listdir(parent)

        if len(files) == 0:
            try:
                os.rmdir(parent)
            except OSError:
                logger.debug(f"Cannot remove directory: {parent}")
