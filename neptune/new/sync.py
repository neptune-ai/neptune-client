#
# Copyright (c) 2020, Neptune Labs Sp. z o.o.
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

import logging
import os
import sys
import textwrap
import time
import uuid
from pathlib import Path
from typing import Sequence, Iterable, List, Optional, Tuple, Any

import click

from neptune.new.constants import (
    ASYNC_DIRECTORY,
    NEPTUNE_RUNS_DIRECTORY,
    OFFLINE_DIRECTORY,
    OFFLINE_NAME_PREFIX,
)
from neptune.new.envs import NEPTUNE_SYNC_BATCH_TIMEOUT_ENV, PROJECT_ENV_NAME
from neptune.new.exceptions import (
    CannotSynchronizeOfflineRunsWithoutProject,
    NeptuneConnectionLostException, NeptuneException,
    ProjectNotFound,
    RunNotFound,
)
from neptune.new.internal.backends.api_model import Project, ApiRun
from neptune.new.internal.backends.hosted_neptune_backend import HostedNeptuneBackend
from neptune.new.internal.backends.neptune_backend import NeptuneBackend
from neptune.new.internal.containers.disk_queue import DiskQueue
from neptune.new.internal.credentials import Credentials
from neptune.new.internal.operation import Operation

#######################################################################################################################
# Run and Project utilities
#######################################################################################################################


# Set in CLI entry points block, patched in tests
backend: NeptuneBackend = None

retries_timeout = int(os.getenv(NEPTUNE_SYNC_BATCH_TIMEOUT_ENV, "3600"))


def get_run(run_id: str) -> Optional[ApiRun]:
    try:
        return backend.get_run(run_id)
    except RunNotFound:
        return None
    except NeptuneException as e:
        click.echo('Exception while fetching run {}. Skipping run.'.format(run_id), err=True)
        logging.exception(e)
        return None


project_name_missing_message = (
    'Project name not provided. Could not synchronize offline runs. '
    'To synchronize offline run, specify the project name with the --project flag '
    'or by setting the {} environment variable.'.format(PROJECT_ENV_NAME))


def project_not_found_message(project_name: str) -> str:
    return ('Project {} not found. Could not synchronize offline runs. '.format(project_name) +
            'Please ensure you specified the correct project name with the --project flag ' +
            'or with the {} environment variable, or contact Neptune for support.'.format(PROJECT_ENV_NAME))


def get_project(project_name_flag: Optional[str]) -> Optional[Project]:
    project_name = project_name_flag or os.getenv(PROJECT_ENV_NAME)
    if not project_name:
        click.echo(textwrap.fill(project_name_missing_message), file=sys.stderr)
        return None
    try:
        return backend.get_project(project_name)
    except ProjectNotFound:
        click.echo(textwrap.fill(project_not_found_message(project_name)), file=sys.stderr)
        return None


def get_qualified_name(run: ApiRun) -> str:
    return "{}/{}/{}".format(run.workspace, run.project_name, run.short_id)


def is_valid_uuid(val: Any) -> bool:
    try:
        uuid.UUID(val)
        return True
    except ValueError:
        return False


#######################################################################################################################
# Listing runs to be synchronized
#######################################################################################################################

def is_run_synced(run_path: Path) -> bool:
    return all(is_execution_synced(execution_path) for execution_path in run_path.iterdir())


def is_execution_synced(execution_path: Path) -> bool:
    disk_queue = DiskQueue(execution_path, lambda x: x.to_dict(), Operation.from_dict)
    return disk_queue.is_empty()


def get_offline_runs_ids(base_path: Path) -> List[str]:
    result = []
    if not (base_path / OFFLINE_DIRECTORY).is_dir():
        return []
    for run_path in (base_path / OFFLINE_DIRECTORY).iterdir():
        if is_valid_uuid(run_path.name):
            result.append(run_path.name)
    return result


def partition_runs(base_path: Path) -> Tuple[List[ApiRun], List[ApiRun], int]:
    synced_runs_uuids = []
    unsynced_runs_uuids = []
    for run_path in (base_path / ASYNC_DIRECTORY).iterdir():
        if is_valid_uuid(run_path.name):
            run_uuid = run_path.name
            if is_run_synced(run_path):
                synced_runs_uuids.append(run_uuid)
            else:
                unsynced_runs_uuids.append(run_uuid)
    synced_runs = [run for run in map(get_run, synced_runs_uuids)]
    unsynced_runs = [run for run in map(get_run, unsynced_runs_uuids)]
    not_found = len([exp for exp in synced_runs + unsynced_runs if not exp or exp.trashed])
    synced_runs = [exp for exp in synced_runs if exp and not exp.trashed]
    unsynced_runs = [exp for exp in unsynced_runs if exp and not exp.trashed]

    return synced_runs, unsynced_runs, not_found


offline_run_explainer = '''
Runs which execute offline are not created on the server and they are not assigned to projects;
instead, they are identified by UUIDs like the ones above.
When synchronizing offline runs, please specify the workspace and project using the "--project"
flag. Alternatively, you can set the environment variable
{} to the target workspace/project. See the examples below.
'''.format(PROJECT_ENV_NAME)


def list_runs(base_path: Path, synced_runs: Sequence[ApiRun],
              unsynced_runs: Sequence[ApiRun], offline_runs_ids: Sequence[str]) \
        -> None:
    if not synced_runs and not unsynced_runs and not offline_runs_ids:
        click.echo('There are no Neptune runs in {}'.format(base_path))
        sys.exit(1)

    if unsynced_runs:
        click.echo('Unsynchronized runs:')
        for run in unsynced_runs:
            click.echo('- {}'.format(get_qualified_name(run)))

    if synced_runs:
        click.echo('Synchronized runs:')
        for run in synced_runs:
            click.echo('- {}'.format(get_qualified_name(run)))

    if offline_runs_ids:
        click.echo('Unsynchronized offline runs:')
        for run_id in offline_runs_ids:
            click.echo('- {}{}'.format(OFFLINE_NAME_PREFIX, run_id))
        click.echo()
        click.echo(textwrap.fill(offline_run_explainer, width=90))

    if not unsynced_runs:
        click.echo()
        click.echo('There are no unsynchronized runs in {}'.format(base_path))

    if not synced_runs:
        click.echo()
        click.echo('There are no synchronized runs in {}'.format(base_path))

    click.echo()
    click.echo('Please run with the `neptune sync --help` to see example commands.')


def synchronization_status(base_path: Path) -> None:
    synced_runs, unsynced_runs, not_found = partition_runs(base_path)
    if not_found > 0:
        click.echo(
            "WARNING: {} runs was skipped because they are in trash or do not exist anymore.".format(not_found),
            sys.stderr)
    offline_runs_ids = get_offline_runs_ids(base_path)
    list_runs(base_path, synced_runs, unsynced_runs, offline_runs_ids)


#######################################################################################################################
# Run synchronization
#######################################################################################################################


def sync_run(run_path: Path, qualified_run_name: str) -> None:
    run_uuid = uuid.UUID(run_path.name)
    click.echo('Synchronising {}'.format(qualified_run_name))
    for execution_path in run_path.iterdir():
        sync_execution(execution_path, run_uuid)
    click.echo('Synchronization of run {} completed.'.format(qualified_run_name))


def sync_execution(execution_path: Path, run_uuid: uuid.UUID) -> None:
    disk_queue = DiskQueue(execution_path, lambda x: x.to_dict(), Operation.from_dict)
    while True:
        batch, version = disk_queue.get_batch(1000)
        if not batch:
            break

        start_time = time.monotonic()
        while True:
            try:
                backend.execute_operations(run_uuid, batch)
                break
            except NeptuneConnectionLostException as ex:
                if time.monotonic() - start_time > retries_timeout:
                    raise ex
                click.echo(f"Experiencing connection interruptions. "
                           f"Will try to reestablish communication with Neptune.",
                           sys.stderr)

        disk_queue.ack(version)


def sync_all_registered_runs(base_path: Path) -> None:
    for run_path in (base_path / ASYNC_DIRECTORY).iterdir():
        if is_valid_uuid(run_path.name) and not is_run_synced(run_path):
            run_uuid = run_path.name
            run = get_run(run_uuid)
            if run:
                sync_run(run_path, get_qualified_name(run))


def sync_selected_registered_runs(base_path: Path, qualified_runs_names: Sequence[str]) -> None:
    for name in qualified_runs_names:
        run = get_run(name)
        if run:
            run_path = base_path / ASYNC_DIRECTORY / str(run.uuid)
            if run_path.exists():
                sync_run(run_path, name)
            else:
                click.echo("Warning: Run '{}' does not exist in location {}".format(name, base_path),
                           file=sys.stderr)


def register_offline_run(project: Project) -> Optional[ApiRun]:
    try:
        return backend.create_run(project.uuid)
    except Exception as e:
        click.echo('Exception occurred while trying to create a run '
                   'on the Neptune server. Please try again later',
                   file=sys.stderr)
        logging.exception(e)
        return None


def move_offline_run(base_path: Path, offline_uuid: str, server_uuid: str) -> None:
    (base_path / ASYNC_DIRECTORY / server_uuid).mkdir(parents=True)
    (base_path / OFFLINE_DIRECTORY / offline_uuid).rename(base_path / ASYNC_DIRECTORY / server_uuid / "exec-0-offline")


def register_offline_runs(base_path: Path, project: Project,
                          offline_runs_ids: Iterable[str]) -> List[ApiRun]:
    result = []
    for run_uuid in offline_runs_ids:
        if (base_path / OFFLINE_DIRECTORY / run_uuid).is_dir():
            run = register_offline_run(project)
            if run:
                move_offline_run(base_path, offline_uuid=run_uuid, server_uuid=str(run.uuid))
                click.echo('Offline run {} registered as {}'
                           .format(run_uuid, get_qualified_name(run)))
                result.append(run)
        else:
            click.echo('Offline run with UUID {} not found on disk.'.format(run_uuid), err=True)
    return result


def is_offline_run_name(name: str) -> bool:
    return name.startswith(OFFLINE_NAME_PREFIX) and is_valid_uuid(name[len(OFFLINE_NAME_PREFIX):])


def sync_offline_runs(base_path: Path, project_name: Optional[str], offline_run_ids: Sequence[str]):
    if offline_run_ids:
        project = get_project(project_name)
        if not project:
            raise CannotSynchronizeOfflineRunsWithoutProject
        registered_runs = register_offline_runs(base_path, project, offline_run_ids)
        offline_runs_names = [get_qualified_name(exp) for exp in registered_runs]
        sync_selected_registered_runs(base_path, offline_runs_names)


def sync_selected_runs(base_path: Path, project_name: Optional[str],
                       runs_names: Sequence[str]) -> None:
    other_runs_names = [name for name in runs_names if not is_offline_run_name(name)]
    sync_selected_registered_runs(base_path, other_runs_names)

    offline_runs_ids = [name[len(OFFLINE_NAME_PREFIX):] for name in runs_names
                        if is_offline_run_name(name)]
    sync_offline_runs(base_path, project_name, offline_runs_ids)


def sync_all_runs(base_path: Path, project_name: Optional[str]) -> None:
    sync_all_registered_runs(base_path)

    offline_runs_ids = get_offline_runs_ids(base_path)
    sync_offline_runs(base_path, project_name, offline_runs_ids)


#######################################################################################################################
# Entrypoint for the CLI utility
#######################################################################################################################


# pylint: disable=unused-argument
def get_neptune_path(ctx, param, path: str) -> Path:
    # check if path exists and contains a '.neptune' folder
    path = Path(path)
    if (path / NEPTUNE_RUNS_DIRECTORY).is_dir():
        return path / NEPTUNE_RUNS_DIRECTORY
    elif path.name == NEPTUNE_RUNS_DIRECTORY and path.is_dir():
        return path
    else:
        raise click.BadParameter("Path {} does not contain a '{}' folder.".format(path, NEPTUNE_RUNS_DIRECTORY))


path_option = click.option('--path', type=click.Path(exists=True, file_okay=False, resolve_path=True),
                           default=Path.cwd(), callback=get_neptune_path, metavar='<location>',
                           help="path to a directory containing a '.neptune' folder with stored runs")


@click.command()
@path_option
def status(path: Path) -> None:
    """List synchronized and unsynchronized runs in the given directory. Trashed runs are not listed.

    Neptune stores run data on disk in '.neptune' directories. In case a run executes offline
    or network is unavailable as the run executes, run data can be synchronized
    with the server with this command line utility.

    Examples:

    \b
    # List synchronized and unsynchronized runs in the current directory
    neptune status

    \b
    # List synchronized and unsynchronized runs in directory "foo/bar" without actually syncing
    neptune status --path foo/bar
    """

    # pylint: disable=global-statement
    global backend
    backend = HostedNeptuneBackend(Credentials())

    synchronization_status(path)


@click.command()
@path_option
@click.option('--run', 'runs_names', multiple=True, metavar='<run-name>',
              help="run name (workspace/project/short-id or UUID for offline runs) to synchronize.")
@click.option('-p', '--project', 'project_name', multiple=False, metavar='project-name',
              help="project name (workspace/project) where offline runs will be sent")
def sync(path: Path, runs_names: List[str], project_name: Optional[str]):
    """Synchronizes runs with unsent data with the server.

    Neptune stores run data on disk in '.neptune' directories. In case a run executes offline
    or network is unavailable as the run executes, run data can be synchronized
    with the server with this command line utility.

    You can list unsynchronized runs with `neptune status`

    Examples:

    \b
    # Synchronize all runs in the current directory
    neptune sync

    \b
    # Synchronize all runs in the given path
    neptune sync --path foo/bar

    \b
    # Synchronize only runs "NPT-42" and "NPT-43" in "workspace/project" in the current directory
    neptune sync --run workspace/project/NPT-42 --run workspace/project/NPT-43

    \b
    # Synchronise all runs in the current directory, sending offline runs to project "workspace/project"
    neptune sync --project workspace/project

    \b
    # Synchronize only the offline run with UUID offline/a1561719-b425-4000-a65a-b5efb044d6bb
    # to project "workspace/project"
    neptune sync --project workspace/project --run offline/a1561719-b425-4000-a65a-b5efb044d6bb
    """

    # pylint: disable=global-statement
    global backend
    backend = HostedNeptuneBackend(Credentials())

    if runs_names:
        sync_selected_runs(path, project_name, runs_names)
    else:
        sync_all_runs(path, project_name)
