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

__all__ = ["common_metadata", "get_container_full_path", "get_container_dir"]

import os
import platform
import random
import string
import sys
from datetime import (
    datetime,
    timezone,
)
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
)

from neptune.constants import NEPTUNE_DATA_DIRECTORY
from neptune.objects.structure_version import StructureVersion

if TYPE_CHECKING:
    from neptune.core.typing.container_type import ContainerType
    from neptune.core.typing.id_formats import CustomId

RANDOM_KEY_LENGTH = 8


def get_neptune_version() -> str:
    from neptune.version import __version__

    return __version__


def random_key(length: int) -> str:
    characters = string.ascii_lowercase + string.digits
    return "".join(random.choice(characters) for _ in range(length))


def get_container_dir(custom_id: "CustomId", container_type: "ContainerType") -> str:
    return f"{container_type.value}__{custom_id}__{os.getpid()}__{random_key(RANDOM_KEY_LENGTH)}"


def get_container_full_path(type_dir: str, custom_id: "CustomId", container_type: "ContainerType") -> Path:
    neptune_data_dir = Path(os.getenv("NEPTUNE_DATA_DIRECTORY", NEPTUNE_DATA_DIRECTORY))
    return neptune_data_dir / type_dir / get_container_dir(custom_id=custom_id, container_type=container_type)


def common_metadata(mode: str, custom_id: "CustomId", container_type: "ContainerType") -> Dict[str, Any]:
    return {
        "mode": mode,
        "customId": custom_id,
        "containerType": container_type,
        "structureVersion": StructureVersion.DIRECT_DIRECTORY.value,
        "os": platform.platform(),
        "pythonVersion": sys.version,
        "neptuneClientVersion": get_neptune_version(),
        "createdAt": datetime.now(timezone.utc).isoformat(),
    }
