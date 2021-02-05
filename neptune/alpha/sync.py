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
import uuid
from pathlib import Path
from typing import Sequence, Iterable, List, Optional, Tuple, Any

import click

from neptune.alpha.constants import NEPTUNE_EXPERIMENT_DIRECTORY, OFFLINE_DIRECTORY, \
    OFFLINE_NAME_PREFIX, ASYNC_DIRECTORY
from neptune.alpha.envs import PROJECT_ENV_NAME
from neptune.alpha.exceptions import ProjectNotFound, NeptuneException, \
    CannotSynchronizeOfflineExperimentsWithoutProject
from neptune.alpha.internal.backends.api_model import Project, Experiment
from neptune.alpha.internal.backends.hosted_neptune_backend import HostedNeptuneBackend
from neptune.alpha.internal.backends.neptune_backend import NeptuneBackend
from neptune.alpha.internal.containers.disk_queue import DiskQueue
from neptune.alpha.internal.credentials import Credentials
from neptune.alpha.internal.operation import Operation


#######################################################################################################################
# Experiment and Project utilities
#######################################################################################################################


# Set in CLI entry points block, patched in tests
backend: NeptuneBackend = None


def report_get_experiment_error(experiment_id: str, status_code: int, skipping: bool) -> None:
    comment = "Skipping experiment." if skipping else "Please try again later or contact Neptune team."
    click.echo("Warning: Getting experiment {}: server responded with status code {}. {}"
               .format(experiment_id, status_code, comment), file=sys.stderr)


def get_experiment(experiment_id: str) -> Optional[Experiment]:
    try:
        return backend.get_experiment(experiment_id)
    except NeptuneException as e:
        click.echo('Exception while fetching experiment {}. Skipping experiment.'.format(experiment_id), err=True)
        logging.exception(e)


project_name_missing_message = (
    'Project name not provided. Could not synchronize offline experiments. '
    'To synchronize offline experiment, specify the project name with the --project flag '
    'or by setting the {} environment variable.'.format(PROJECT_ENV_NAME))


def project_not_found_message(project_name: str) -> str:
    return ('Project {} not found. Could not synchronize offline experiments. '.format(project_name) +
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


def get_qualified_name(experiment: Experiment) -> str:
    return "{}/{}/{}".format(experiment.workspace, experiment.project_name, experiment.short_id)


def is_valid_uuid(val: Any) -> bool:
    try:
        uuid.UUID(val)
        return True
    except ValueError:
        return False


#######################################################################################################################
# Listing experiments to be synchronized
#######################################################################################################################

def is_experiment_synced(experiment_path: Path) -> bool:
    return all(is_execution_synced(execution_path) for execution_path in experiment_path.iterdir())


def is_execution_synced(execution_path: Path) -> bool:
    disk_queue = DiskQueue(execution_path, lambda x: x.to_dict(), Operation.from_dict)
    return disk_queue.is_empty()


def get_offline_experiments_ids(base_path: Path) -> List[str]:
    result = []
    if not (base_path / OFFLINE_DIRECTORY).is_dir():
        return []
    for experiment_path in (base_path / OFFLINE_DIRECTORY).iterdir():
        if is_valid_uuid(experiment_path.name):
            result.append(experiment_path.name)
    return result


def partition_experiments(base_path: Path) -> Tuple[List[Experiment], List[Experiment]]:
    synced_experiment_uuids = []
    unsynced_experiment_uuids = []
    for experiment_path in (base_path / ASYNC_DIRECTORY).iterdir():
        if is_valid_uuid(experiment_path.name):
            experiment_uuid = experiment_path.name
            if is_experiment_synced(experiment_path):
                synced_experiment_uuids.append(experiment_uuid)
            else:
                unsynced_experiment_uuids.append(experiment_uuid)
    synced_experiments = [experiment for experiment in map(get_experiment, synced_experiment_uuids)
                          if experiment and not experiment.trashed]
    unsynced_experiments = [experiment for experiment in map(get_experiment, unsynced_experiment_uuids)
                            if experiment and not experiment.trashed]
    return synced_experiments, unsynced_experiments


offline_experiment_explainer = '''
Experiments which run offline are not created on the server and they are not assigned to projects;
instead, they are identified by UUIDs like the ones above.
When synchronizing offline experiments, please specify the workspace and project using the "--project"
flag. Alternatively, you can set the environment variable
{} to the target workspace/project. See the examples below.
'''.format(PROJECT_ENV_NAME)


def list_experiments(base_path: Path, synced_experiments: Sequence[Experiment],
                     unsynced_experiments: Sequence[Experiment], offline_experiments_ids: Sequence[str])\
                     -> None:

    if not synced_experiments and not unsynced_experiments and not offline_experiments_ids:
        click.echo('There are no Neptune experiments in {}'.format(base_path))
        sys.exit(1)

    if unsynced_experiments:
        click.echo('Unsynchronized experiments:')
        for experiment in unsynced_experiments:
            click.echo('- {}'.format(get_qualified_name(experiment)))

    if synced_experiments:
        click.echo('Synchronized experiments:')
        for experiment in synced_experiments:
            click.echo('- {}'.format(get_qualified_name(experiment)))

    if offline_experiments_ids:
        click.echo('Unsynchronized offline experiments:')
        for experiment_id in offline_experiments_ids:
            click.echo('- {}{}'.format(OFFLINE_NAME_PREFIX, experiment_id))
        click.echo()
        click.echo(textwrap.fill(offline_experiment_explainer, width=90))

    if not unsynced_experiments:
        click.echo()
        click.echo('There are no unsynchronized experiments in {}'.format(base_path))

    if not synced_experiments:
        click.echo()
        click.echo('There are no synchronized experiments in {}'.format(base_path))

    click.echo()
    click.echo('Please run with the `neptune sync --help` to see example commands.')


def synchronization_status(base_path: Path) -> None:
    synced_experiments, unsynced_experiments = partition_experiments(base_path)
    offline_experiments_ids = get_offline_experiments_ids(base_path)
    list_experiments(base_path, synced_experiments, unsynced_experiments, offline_experiments_ids)


#######################################################################################################################
# Experiment synchronization
#######################################################################################################################


def sync_experiment(experiment_path: Path, qualified_experiment_name: str) -> None:
    experiment_uuid = uuid.UUID(experiment_path.name)
    click.echo('Synchronising {}'.format(qualified_experiment_name))
    for execution_path in experiment_path.iterdir():
        sync_execution(execution_path, experiment_uuid)
    click.echo('Synchronization of experiment {} completed.'.format(qualified_experiment_name))


def sync_execution(execution_path: Path, experiment_uuid: uuid.UUID) -> None:
    disk_queue = DiskQueue(execution_path, lambda x: x.to_dict(), Operation.from_dict)
    while True:
        batch, version = disk_queue.get_batch(1000)
        if not batch:
            break
        backend.execute_operations(experiment_uuid, batch)
        disk_queue.ack(version)


def sync_all_registered_experiments(base_path: Path) -> None:
    for experiment_path in (base_path / ASYNC_DIRECTORY).iterdir():
        if is_valid_uuid(experiment_path.name) and not is_experiment_synced(experiment_path):
            experiment_uuid = experiment_path.name
            experiment = get_experiment(experiment_uuid)
            if experiment:
                sync_experiment(experiment_path, get_qualified_name(experiment))


def sync_selected_registered_experiments(base_path: Path, qualified_experiment_names: Sequence[str]) -> None:
    for name in qualified_experiment_names:
        experiment = get_experiment(name)
        if experiment:
            experiment_path = base_path / ASYNC_DIRECTORY / str(experiment.uuid)
            if experiment_path.exists():
                sync_experiment(experiment_path, name)
            else:
                click.echo("Warning: Experiment '{}' does not exist in location {}".format(name, base_path),
                           file=sys.stderr)


def register_offline_experiment(project: Project) -> Optional[Experiment]:
    try:
        return backend.create_experiment(project.uuid)
    except Exception as e:
        click.echo('Exception occurred while trying to create an experiment '
                   'on the Neptune server. Please try again later',
                   file=sys.stderr)
        logging.exception(e)
        return None


def move_offline_experiment(base_path: Path, offline_uuid: str, server_uuid: str) -> None:
    (base_path / ASYNC_DIRECTORY / server_uuid).mkdir(parents=True)
    (base_path / OFFLINE_DIRECTORY / offline_uuid).rename(base_path / ASYNC_DIRECTORY / server_uuid / "exec-0-offline")


def register_offline_experiments(base_path: Path, project: Project,
                                 offline_experiments_ids: Iterable[str]) -> List[Experiment]:
    result = []
    for experiment_uuid in offline_experiments_ids:
        if (base_path / OFFLINE_DIRECTORY / experiment_uuid).is_dir():
            experiment = register_offline_experiment(project)
            if experiment:
                move_offline_experiment(base_path, offline_uuid=experiment_uuid, server_uuid=str(experiment.uuid))
                click.echo('Offline experiment {} registered as {}'
                           .format(experiment_uuid, get_qualified_name(experiment)))
                result.append(experiment)
        else:
            click.echo('Offline experiment with UUID {} not found on disk.'.format(experiment_uuid), err=True)
    return result


def is_offline_experiment_name(name: str) -> bool:
    return name.startswith(OFFLINE_NAME_PREFIX) and is_valid_uuid(name[len(OFFLINE_NAME_PREFIX):])


def sync_offline_experiments(base_path: Path, project_name: Optional[str], offline_experiment_ids: Sequence[str]):
    if offline_experiment_ids:
        project = get_project(project_name)
        if not project:
            raise CannotSynchronizeOfflineExperimentsWithoutProject
        registered_experiments = register_offline_experiments(base_path, project, offline_experiment_ids)
        offline_experiment_names = [get_qualified_name(exp) for exp in registered_experiments]
        sync_selected_registered_experiments(base_path, offline_experiment_names)


def sync_selected_experiments(base_path: Path, project_name: Optional[str],
                              experiment_names: Sequence[str]) -> None:
    other_experiment_names = [name for name in experiment_names if not is_offline_experiment_name(name)]
    sync_selected_registered_experiments(base_path, other_experiment_names)

    offline_experiment_ids = [name[len(OFFLINE_NAME_PREFIX):] for name in experiment_names
                              if is_offline_experiment_name(name)]
    sync_offline_experiments(base_path, project_name, offline_experiment_ids)


def sync_all_experiments(base_path: Path, project_name: Optional[str]) -> None:
    sync_all_registered_experiments(base_path)

    offline_experiment_ids = get_offline_experiments_ids(base_path)
    sync_offline_experiments(base_path, project_name, offline_experiment_ids)


#######################################################################################################################
# Entrypoint for the CLI utility
#######################################################################################################################


# pylint: disable=unused-argument
def get_neptune_path(ctx, param, path: str) -> Path:
    # check if path exists and contains a '.neptune' folder
    path = Path(path)
    if (path / NEPTUNE_EXPERIMENT_DIRECTORY).is_dir():
        return path / NEPTUNE_EXPERIMENT_DIRECTORY
    elif path.name == NEPTUNE_EXPERIMENT_DIRECTORY and path.is_dir():
        return path
    else:
        raise click.BadParameter("Path {} does not contain a '{}' folder.".format(path, NEPTUNE_EXPERIMENT_DIRECTORY))


path_option = click.option('--path', type=click.Path(exists=True, file_okay=False, resolve_path=True),
                           default=Path.cwd(), callback=get_neptune_path, metavar='<location>',
                           help="path to a directory containing a '.neptune' folder with stored experiments")


@click.command()
@path_option
def status(path: Path) -> None:
    """List synchronized and unsynchronized experiments in the given directory. Trashed experiments are not listed.

    Neptune stores experiment data on disk in '.neptune' directories. In case an experiment runs offline
    or network is unavailable as the experiment runs, experiment data can be synchronized
    with the server with this command line utility.

    Examples:

    \b
    # List synchronized and unsynchronized experiments in the current directory
    neptune status

    \b
    # List synchronized and unsynchronized experiments in directory "foo/bar" without actually syncing
    neptune status --path foo/bar
    """

    # pylint: disable=global-statement
    global backend
    backend = HostedNeptuneBackend(Credentials())

    synchronization_status(path)


@click.command()
@path_option
@click.option('-e', '--experiment', 'experiment_names', multiple=True, metavar='<experiment-name>',
              help="experiment name (workspace/project/short-id or UUID for offline experiments) to synchronize.")
@click.option('-p', '--project', 'project_name', multiple=False, metavar='project-name',
              help="project name (workspace/project) where offline experiments will be sent")
def sync(path: Path, experiment_names: List[str], project_name: Optional[str]):
    """Synchronizes experiments with unsent data with the server.

    Neptune stores experiment data on disk in '.neptune' directories. In case an experiment runs offline
    or network is unavailable as the experiment runs, experiment data can be synchronized
    with the server with this command line utility.

    You can list unsynchronized experiments with `neptune status`

    Examples:

    \b
    # Synchronize all experiments in the current directory
    neptune sync # TODO while in alpha: python -m neptune.alpha.cli sync

    \b
    # Synchronize all experiments in the given path
    neptune sync --path foo/bar

    \b
    # Synchronize only experiments "NPT-42" and "NPT-43" in "workspace/project" in the current directory
    neptune sync --experiment workspace/project/NPT-42 --experiment workspace/project/NPT-43

    \b
    # Synchronise all experiment in the current directory, sending offline experiments to project "workspace/project"
    neptune sync --project workspace/project

    \b
    # Synchronize only the offline experiment with UUID offline/a1561719-b425-4000-a65a-b5efb044d6bb
    # to project "workspace/project"
    neptune sync --project workspace/project --experiment offline/a1561719-b425-4000-a65a-b5efb044d6bb
    """

    # pylint: disable=global-statement
    global backend
    backend = HostedNeptuneBackend(Credentials())

    if experiment_names:
        sync_selected_experiments(path, project_name, experiment_names)
    else:
        sync_all_experiments(path, project_name)
