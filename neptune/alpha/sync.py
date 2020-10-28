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

import argparse
import uuid
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Tuple, Any

from bravado.exception import HTTPError
from neptune.alpha.constants import NEPTUNE_EXPERIMENT_FOLDER, OPERATIONS_DISK_QUEUE_PREFIX
from neptune.alpha.internal.backends.hosted_neptune_backend import HostedNeptuneBackend
from neptune.alpha.internal.backends.neptune_backend import NeptuneBackend
from neptune.alpha.internal.containers.disk_queue import DiskQueue
from neptune.alpha.internal.credentials import Credentials
from neptune.alpha.internal.operation import VersionedOperation
from neptune.alpha.internal.utils.sync_offset_file import SyncOffsetFile


#######################################################################################################################
# Argument parser
#######################################################################################################################


epilog = """
Neptune stores experiment data on disk. In case an experiment is running offline
or network is unavailable as the experiment runs, experiment data can be synchronized
with the server with this utility.

If you run experiments from directory D, then Neptune stores experiment data in
'D/.neptune' folder. You can specify any directory which contains a '.neptune' folder
for synchronization.

Examples:
  
  # List unsynchronized experiments in the current directory
  python -m neptune.alpha.sync list

  # List unsynchronized experiments in the given directory
  python -m neptune.alpha.sync list --location foo/bar

  # Synchronize experiments with unsent data in the current directory
  python -m neptune.alpha.sync sync

  # Synchronize experiments with unsent data in the given location
  python -m neptune.alpha.sync sync --location foo/bar

  # Synchronize given experiments in the current directory if they have unsent data
  python -m neptune.alpha.sync sync --experiment workspace/project/NPT-42 --experiment workspace/project/NPT-43
"""


parser = argparse.ArgumentParser(
    description='Synchronizes experiments with unsent data with the server',
    epilog=epilog, formatter_class=argparse.RawDescriptionHelpFormatter)
parser.add_argument('action', choices=['list', 'sync'],
                    help="Action: 'list' to list unsynchronized experiments in the chosen directory; "
                    "'sync' to synchronize experiments in the chosen directory")
parser.add_argument('-l', '--location', default='.', metavar='experiment-directory',
                    help="Path to the directory containing a '.neptune' folder with stored experiments")
parser.add_argument('-e', '--experiment', action='append', metavar='qualified-experiment-name',
                    help="Qualified name ('workspace/project/short-id') of an experiment to synchronize.")


#######################################################################################################################
# Stubs of dynamically-generated classes for type checking and mocking in tests
#######################################################################################################################


@dataclass
class Experiment:
    uuid: str
    shortId: str
    organizationName: str
    projectName: str


#######################################################################################################################
# Experiment utilities
#######################################################################################################################


# Set in the __main__ block, patched in tests
backend: NeptuneBackend = None


def report_get_experiment_error(experimentId: str, status_code: int, skipping: bool) -> None:
    comment = "Skipping experiment." if skipping else "Please try again later or contact Neptune team."
    print("Warning: Getting experiment {}: server responded with status code {}. {}"
          .format(experimentId, status_code, comment), file=sys.stderr)


def get_experiment(experimentId: str) -> Optional[Experiment]:
    try:
        response = backend.leaderboard_client.api.getExperiment(experimentId=experimentId).response()
        return response.result
    except HTTPError as e:
        if e.status_code in (401, 403, 404):
            report_get_experiment_error(experimentId, e.status_code, skipping=True)
        else:
            report_get_experiment_error(experimentId, e.status_code, skipping=False)


def get_qualified_name(experiment: Experiment) -> str:
    return "{}/{}/{}".format(experiment.organizationName, experiment.projectName, experiment.shortId)


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
    sync_offset_file = SyncOffsetFile(experiment_path)
    sync_offset = sync_offset_file.read()

    disk_queue = DiskQueue(str(experiment_path), OPERATIONS_DISK_QUEUE_PREFIX,
                           VersionedOperation.to_dict, VersionedOperation.from_dict)
    previous_operation = None
    while True:
        operation = disk_queue.get()
        if not operation:
            break
        previous_operation = operation
    if not previous_operation:
        return True

    return sync_offset >= previous_operation.version


def partition_experiments(base_path: Path) -> Tuple[List[Experiment], List[Experiment]]:
    synced_experiment_uuids = []
    unsynced_experiment_uuids = []
    for experiment_path in base_path.iterdir():
        if is_valid_uuid(experiment_path.name):
            experiment_uuid = experiment_path.name
            if is_experiment_synced(experiment_path):
                synced_experiment_uuids.append(experiment_uuid)
            else:
                unsynced_experiment_uuids.append(experiment_uuid)
    synced_experiments = [experiment for experiment in map(get_experiment, synced_experiment_uuids) if experiment]
    unsynced_experiments = [experiment for experiment in map(get_experiment, unsynced_experiment_uuids) if experiment]
    return (synced_experiments, unsynced_experiments)


def list_experiments(path: Path, synced_experiments: List[Experiment], unsynced_experiments: List[Experiment]) -> None:
    if not synced_experiments and not unsynced_experiments:
        print('There are no Neptune experiments in ', path)
        sys.exit(1)

    if unsynced_experiments:
        print('Unsynchronised experiments:')
        for experiment in unsynced_experiments:
            print('-', get_qualified_name(experiment))

    if synced_experiments:
        print('Synchronised experiments:')
        for experiment in synced_experiments:
            print('-', get_qualified_name(experiment))

    if not unsynced_experiments:
        print()
        print('There are no unsynchronized experiments in ', path)

    if not synced_experiments:
        print()
        print('There are no synchronized experiments in ', path)

    print()
    print(list_experiments_follow_up_prompt)


#######################################################################################################################
# Follow-up prompt when listing experiments
#######################################################################################################################


list_experiments_follow_up_prompt = '''You can run:

$ python -m neptune.alpha.sync sync

to synchronise all experiment in the current directory, or

$ python -m neptune.alpha.sync sync --experiment org/proj/PRJ-XXX

to synchronise the given experiment.'''


#######################################################################################################################
# Experiment synchronization
#######################################################################################################################


def sync_experiment(path: Path, qualified_experiment_name: str) -> None:
    experiment_uuid = path.name
    print('Synchronising', qualified_experiment_name)

    disk_queue = DiskQueue(str(path), OPERATIONS_DISK_QUEUE_PREFIX,
                           VersionedOperation.to_dict, VersionedOperation.from_dict)
    sync_offset_file = SyncOffsetFile(path)
    sync_offset = sync_offset_file.read()

    while True:
        batch = disk_queue.get_batch(1000)
        if not batch:
            print('Synchronization of experiment {} completed.'.format(qualified_experiment_name))
            return
        if batch[0].version > sync_offset:
            pass
        elif batch[-1].version <= sync_offset:
            continue
        else:
            for i, operation in enumerate(batch):
                if operation.version > sync_offset:
                    batch = batch[i:]
                    break
        backend.execute_operations(experiment_uuid, [op.op for op in batch])
        sync_offset_file.write(batch[-1].version)
        sync_offset = batch[-1].version


def sync_all_experiments(path: Path) -> None:
    for experiment_path in path.iterdir():
        if is_valid_uuid(experiment_path.name) and not is_experiment_synced(experiment_path):
            experiment_uuid = experiment_path.name
            experiment = get_experiment(experiment_uuid)
            if experiment:
                sync_experiment(experiment_path, get_qualified_name(experiment))


def sync_selected_experiments(path: Path, qualified_experiment_names: List[str]) -> None:
    for name in qualified_experiment_names:
        experiment = get_experiment(name)
        if experiment:
            experiment_path = path / experiment.uuid
            if experiment_path.exists():
                sync_experiment(experiment_path, name)
            else:
                print("Warning: Experiment '{}' does not exist in location {}".format(name, path), file=sys.stderr)


#######################################################################################################################
# Entrypoint for the CLI utility
#######################################################################################################################


def main():
    args = parser.parse_args()

    # get base path
    if args.location:
        path = Path.cwd() / Path(args.location)
    else:
        path = Path.cwd()

    # check if path exists and contains a '.neptune' folder
    if (path / NEPTUNE_EXPERIMENT_FOLDER).is_dir():
        path = path / NEPTUNE_EXPERIMENT_FOLDER
    elif path.name == NEPTUNE_EXPERIMENT_FOLDER and path.is_dir():
        pass
    else:
        error_message = \
            ("Path {} does not contain a '{}' folder. Please specify a path to a folder with Neptune experiments."
             .format(path, NEPTUNE_EXPERIMENT_FOLDER))
        print(error_message, file=sys.stderr)
        sys.exit(1)

    qualified_experiment_names = args.experiment

    if args.action == 'list':
        list_experiments(path, *partition_experiments(path))
    elif args.action == 'sync' and qualified_experiment_names:
        sync_selected_experiments(path, qualified_experiment_names)
    elif args.action == 'sync' and not qualified_experiment_names:
        sync_all_experiments(path)

if __name__ == '__main__':
    backend = HostedNeptuneBackend(Credentials())
    main()
