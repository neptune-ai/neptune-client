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
__all__ = ["remove_parent_folder_if_allowed"]

import os
from pathlib import Path

from neptune.envs import NEPTUNE_DISABLE_PARENT_DIR_DELETION
from neptune.internal.utils.logger import logger


def remove_parent_folder_if_allowed(path: Path) -> None:
    deletion_disabled = os.getenv(NEPTUNE_DISABLE_PARENT_DIR_DELETION, "false").lower() in ("true", "1", "t")

    if not deletion_disabled:
        parent = path.parent

        try:
            files = os.listdir(parent)
        except FileNotFoundError:
            files = []

        if len(files) == 0:
            try:
                os.rmdir(parent)
            except OSError:
                logger.debug(f"Cannot remove directory: {parent}")
