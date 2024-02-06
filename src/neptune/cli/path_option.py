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

__all__ = ["path_option"]

from pathlib import Path
from typing import Any

import click

from neptune.constants import NEPTUNE_DATA_DIRECTORY


def get_neptune_path(ctx: Any, param: Any, path: str) -> Path:
    # check if path exists and contains a '.neptune' folder
    local_path = Path(path)

    if (local_path / NEPTUNE_DATA_DIRECTORY).is_dir():
        return local_path / NEPTUNE_DATA_DIRECTORY
    elif local_path.name == NEPTUNE_DATA_DIRECTORY and local_path.is_dir():
        return local_path
    else:
        raise click.BadParameter("Path {} does not contain a '{}' folder.".format(local_path, NEPTUNE_DATA_DIRECTORY))


path_option = click.option(
    "--path",
    type=click.Path(exists=True, file_okay=False, resolve_path=True),
    default=Path.cwd(),
    callback=get_neptune_path,
    metavar="<location>",
    help="path to a directory containing a '.neptune' folder with stored objects",
)
