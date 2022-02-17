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

__all__ = []

from pathlib import Path
from typing import List, Optional

import click

from neptune.new.constants import (
    NEPTUNE_DATA_DIRECTORY,
)
from neptune.new.internal.backends.hosted_neptune_backend import HostedNeptuneBackend
from neptune.new.internal.credentials import Credentials

from neptune.new.sync.status import StatusRunner
from neptune.new.sync.sync import SyncRunner


# backwards compatibility
# pylint: disable=unused-import,wrong-import-order

from neptune.new.internal.backends.neptune_backend import NeptuneBackend
from neptune.new.internal.disk_queue import DiskQueue
from neptune.new.internal.operation import Operation
from neptune.new.internal.backends.api_model import ApiExperiment, Project
from neptune.new.exceptions import (
    CannotSynchronizeOfflineRunsWithoutProject,
    NeptuneConnectionLostException,
    NeptuneException,
    ProjectNotFound,
    RunNotFound,
)


# pylint: disable=unused-argument
def get_neptune_path(ctx, param, path: str) -> Path:
    # check if path exists and contains a '.neptune' folder
    path = Path(path)
    if (path / NEPTUNE_DATA_DIRECTORY).is_dir():
        return path / NEPTUNE_DATA_DIRECTORY
    elif path.name == NEPTUNE_DATA_DIRECTORY and path.is_dir():
        return path
    else:
        raise click.BadParameter(
            "Path {} does not contain a '{}' folder.".format(
                path, NEPTUNE_DATA_DIRECTORY
            )
        )


path_option = click.option(
    "--path",
    type=click.Path(exists=True, file_okay=False, resolve_path=True),
    default=Path.cwd(),
    callback=get_neptune_path,
    metavar="<location>",
    help="path to a directory containing a '.neptune' folder with stored objects",
)


@click.command()
@path_option
def status(path: Path) -> None:
    """List synchronized and unsynchronized objects in the given directory. Trashed objects are not listed.

    Neptune stores object data on disk in '.neptune' directories. In case an object executes offline
    or network is unavailable as the object executes, object data can be synchronized
    with the server with this command line utility.

    Examples:

    \b
    # List synchronized and unsynchronized objects in the current directory
    neptune status

    \b
    # List synchronized and unsynchronized objects in directory "foo/bar" without actually syncing
    neptune status --path foo/bar
    """

    status_runner = StatusRunner(backend=HostedNeptuneBackend(Credentials.from_token()))

    status_runner.synchronization_status(path)


@click.command()
@path_option
@click.option(
    "--run",
    "runs_names",
    multiple=True,
    metavar="<run-name>",
    help="[deprecated] run name (workspace/project/short-id or UUID for offline runs) to synchronize.",
)
@click.option(
    "--object",
    "object_names",
    multiple=True,
    metavar="<object-name>",
    help="object name (workspace/project/short-id or UUID for offline runs) to synchronize.",
)
@click.option(
    "-p",
    "--project",
    "project_name",
    multiple=False,
    metavar="project-name",
    help="project name (workspace/project) where offline runs will be sent",
)
def sync(
    path: Path,
    runs_names: List[str],
    object_names: List[str],
    project_name: Optional[str],
):
    """Synchronizes objects with unsent data with the server.

    Neptune stores object data on disk in '.neptune' directories. In case a object executes offline
    or network is unavailable as the run executes, object data can be synchronized
    with the server with this command line utility.

    You can list unsynchronized runs with `neptune status`

    Examples:

    \b
    # Synchronize all objects in the current directory
    neptune sync

    \b
    # Synchronize all objects in the given path
    neptune sync --path foo/bar

    \b
    # Synchronize only runs "NPT-42" and "NPT-43" in "workspace/project" in the current directory
    neptune sync --object workspace/project/NPT-42 --object workspace/project/NPT-43

    \b
    # Synchronise all objects in the current directory, sending offline runs to project "workspace/project"
    neptune sync --project workspace/project

    \b
    # Synchronize only the offline run with UUID offline/a1561719-b425-4000-a65a-b5efb044d6bb
    # to project "workspace/project"
    neptune sync --project workspace/project --object offline/a1561719-b425-4000-a65a-b5efb044d6bb
    """

    sync_runner = SyncRunner(backend=HostedNeptuneBackend(Credentials.from_token()))

    if runs_names:
        click.echo(
            "WARNING: --run parameter is deprecated and will be removed in the future, please start using --object"
        )
        # prefer object_names, obviously
        object_names = set(object_names)
        object_names.update(runs_names)

    if object_names:
        sync_runner.sync_selected_containers(path, project_name, object_names)
    else:
        sync_runner.sync_all_containers(path, project_name)
