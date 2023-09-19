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
__all__ = ["OperationStorage", "get_container_dir"]

import os
import shutil
from pathlib import Path
from datetime import datetime

from neptune.constants import NEPTUNE_DATA_DIRECTORY
from neptune.internal.container_type import ContainerType
from neptune.internal.id_formats import UniqueId
from neptune.internal.utils.logger import logger


def get_container_dir(
    type_dir: str, container_id: UniqueId, container_type: ContainerType
) -> Path:
    now = datetime.now()
    process_path = f"exec-{now.timestamp()}-{now.strftime('%Y-%m-%d_%H.%M.%S.%f')}-{os.getpid()}"
    neptune_data_dir = os.getenv("NEPTUNE_DATA_DIRECTORY", NEPTUNE_DATA_DIRECTORY)
    container_dir = Path(
        f"{neptune_data_dir}/{type_dir}/{container_type.create_dir_name(container_id)}/{process_path}"
    )

    return container_dir


class OperationStorage:
    UPLOAD_PATH: str = "upload_path"

    def __init__(self, data_path: Path):
        self._data_path = data_path.resolve()

        # initialize directory
        os.makedirs(data_path / OperationStorage.UPLOAD_PATH, exist_ok=True)

    @property
    def data_path(self) -> Path:
        return self._data_path

    @property
    def upload_path(self) -> Path:
        return self.data_path / "upload_path"

    def cleanup(self) -> None:
        shutil.rmtree(self.data_path, ignore_errors=True)

        parent = self.data_path.parent
        files = os.listdir(parent)

        if len(files) == 0:
            try:
                os.rmdir(parent)
            except OSError:
                logger.debug(f"Cannot remove directory: {parent}")
