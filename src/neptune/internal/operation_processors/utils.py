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
__all__ = ["common_metadata", "get_container_full_path", "get_container_dir"]

import os
import platform
import random
import string
import sys
from datetime import datetime
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
)

from neptune.constants import NEPTUNE_DATA_DIRECTORY
from neptune.metadata_containers.structure_version import StructureVersion

if TYPE_CHECKING:
    from neptune.internal.container_type import ContainerType
    from neptune.internal.id_formats import UniqueId


RANDOM_KEY_LENGTH = 8


def get_neptune_version() -> str:
    from neptune.version import __version__ as neptune_version

    return neptune_version


def common_metadata(mode: str, container_id: "UniqueId", container_type: "ContainerType") -> Dict[str, Any]:
    return {
        "mode": mode,
        "containerId": container_id,
        "containerType": container_type,
        "structureVersion": StructureVersion.DIRECT_DIRECTORY.value,
        "os": platform.platform(),
        "pythonVersion": sys.version,
        "neptuneClientVersion": get_neptune_version(),
        "createdAt": datetime.utcnow().isoformat(),
    }


def get_container_dir(container_id: "UniqueId", container_type: "ContainerType") -> str:
    return f"{container_type.value}__{container_id}__{os.getpid()}__{random_key(RANDOM_KEY_LENGTH)}"


def get_container_full_path(type_dir: str, container_id: "UniqueId", container_type: "ContainerType") -> Path:
    neptune_data_dir = Path(os.getenv("NEPTUNE_DATA_DIRECTORY", NEPTUNE_DATA_DIRECTORY))
    return neptune_data_dir / type_dir / get_container_dir(container_id=container_id, container_type=container_type)


def random_key(length: int) -> str:
    characters = string.ascii_lowercase + string.digits
    return "".join(random.choice(characters) for _ in range(length))
