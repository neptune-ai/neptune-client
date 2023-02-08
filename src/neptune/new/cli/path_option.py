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

import click

from neptune.common.exceptions import NeptuneException  # noqa: F401
from neptune.new.constants import NEPTUNE_DATA_DIRECTORY
from neptune.new.exceptions import (  # noqa: F401
    CannotSynchronizeOfflineRunsWithoutProject,
    ProjectNotFound,
    RunNotFound,
)
from neptune.new.internal.backends.api_model import (  # noqa: F401
    ApiExperiment,
    Project,
)
from neptune.new.internal.backends.neptune_backend import NeptuneBackend  # noqa: F401
from neptune.new.internal.disk_queue import DiskQueue  # noqa: F401
from neptune.new.internal.operation import Operation  # noqa: F401


def get_neptune_path(ctx, param, path: str) -> Path:
    # check if path exists and contains a '.neptune' folder
    path = Path(path)
    if (path / NEPTUNE_DATA_DIRECTORY).is_dir():
        return path / NEPTUNE_DATA_DIRECTORY
    elif path.name == NEPTUNE_DATA_DIRECTORY and path.is_dir():
        return path
    else:
        raise click.BadParameter("Path {} does not contain a '{}' folder.".format(path, NEPTUNE_DATA_DIRECTORY))


path_option = click.option(
    "--path",
    type=click.Path(exists=True, file_okay=False, resolve_path=True),
    default=Path.cwd(),
    callback=get_neptune_path,
    metavar="<location>",
    help="path to a directory containing a '.neptune' folder with stored objects",
)
