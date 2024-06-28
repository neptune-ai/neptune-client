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
import random
import string
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from neptune.internal.container_type import ContainerType
    from neptune.internal.id_formats import UniqueId


RANDOM_KEY_LENGTH = 8


def get_container_dir(container_id: "UniqueId", container_type: "ContainerType") -> str:
    return f"{container_type.value}__{container_id}__{os.getpid()}__{random_key(RANDOM_KEY_LENGTH)}"


def random_key(length: int) -> str:
    characters = string.ascii_lowercase + string.digits
    return "".join(random.choice(characters) for _ in range(length))
