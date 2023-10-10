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
__all__ = ["get_container_dir"]

import os
from pathlib import Path

from neptune.constants import NEPTUNE_DATA_DIRECTORY
from neptune.internal.container_type import ContainerType
from neptune.internal.id_formats import UniqueId


def get_container_dir(type_dir: str, container_id: UniqueId, container_type: ContainerType) -> Path:
    neptune_data_dir = Path(os.getenv("NEPTUNE_DATA_DIRECTORY", NEPTUNE_DATA_DIRECTORY))
    return neptune_data_dir / type_dir / f"{container_type.create_dir_name(container_id)}__{os.getpid()}"
