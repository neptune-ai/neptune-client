#
# Copyright (c) 2023, Neptune Labs Sp. z o.o.
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
__all__ = ["common_metadata", "get_container_dir"]

import datetime
import os
import platform
import sys
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Optional,
)

from neptune.constants import NEPTUNE_DATA_DIRECTORY

if TYPE_CHECKING:
    from neptune.internal.container_type import ContainerType
    from neptune.internal.id_formats import UniqueId


def get_neptune_version() -> str:
    from neptune.version import __version__ as neptune_version

    return neptune_version


def common_metadata(mode: str, container_id: "UniqueId", container_type: "ContainerType") -> Dict[str, Any]:
    return {
        "mode": mode,
        "containerId": container_id,
        "containerType": container_type,
        "structureVersion": 2,
        "os": platform.platform(),
        "pythonVersion": sys.version,
        "neptuneClientVersion": get_neptune_version(),
        "createdAt": datetime.datetime.utcnow().isoformat(),
    }


def get_container_dir(
    type_dir: str, container_id: "UniqueId", container_type: "ContainerType", process_path: Optional[str] = None
) -> Path:
    neptune_data_dir = os.getenv("NEPTUNE_DATA_DIRECTORY", NEPTUNE_DATA_DIRECTORY)
    container_dir = Path(f"{neptune_data_dir}/{type_dir}/{container_type.create_dir_name(container_id)}")
    if process_path:
        container_dir /= Path(process_path)

    return container_dir
