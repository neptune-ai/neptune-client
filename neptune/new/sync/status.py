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

__all__ = ["StatusRunner"]

import sys
import textwrap
from pathlib import Path
from typing import List, Sequence, Tuple

import click

from neptune.new.constants import (
    ASYNC_DIRECTORY,
    OFFLINE_NAME_PREFIX,
)
from neptune.new.envs import PROJECT_ENV_NAME
from neptune.new.internal.backends.api_model import ApiExperiment
from neptune.new.sync.abstract_backend_runner import AbstractBackendRunner
from neptune.new.sync.utils import (
    get_qualified_name,
    is_run_synced,
    iterate_containers,
    get_metadata_container,
    get_offline_dirs,
)

offline_run_explainer = """
Runs which execute offline are not created on the server and they are not assigned to projects;
instead, they are identified by UUIDs like the ones above.
When synchronizing offline runs, please specify the workspace and project using the "--project"
flag. Alternatively, you can set the environment variable
{} to the target workspace/project. See the examples below.
""".format(
    PROJECT_ENV_NAME
)


class StatusRunner(AbstractBackendRunner):
    def partition_containers(
        self,
        base_path: Path,
    ) -> Tuple[List[ApiExperiment], List[ApiExperiment], int]:
        synced_runs = []
        unsynced_runs = []
        async_path = base_path / ASYNC_DIRECTORY
        for container_type, container_id, path in iterate_containers(async_path):
            metadata_container = get_metadata_container(
                container_id=container_id,
                container_type=container_type,
                backend=self._backend,
            )

            if is_run_synced(path):
                synced_runs.append(metadata_container)
            else:
                unsynced_runs.append(metadata_container)

        not_found = len(
            [exp for exp in synced_runs + unsynced_runs if not exp or exp.trashed]
        )
        synced_runs = [exp for exp in synced_runs if exp and not exp.trashed]
        unsynced_runs = [exp for exp in unsynced_runs if exp and not exp.trashed]

        return synced_runs, unsynced_runs, not_found

    @staticmethod
    def list_runs(
        base_path: Path,
        synced_runs: Sequence[ApiExperiment],
        unsynced_runs: Sequence[ApiExperiment],
        offline_dirs: Sequence[str],
    ) -> None:
        if not synced_runs and not unsynced_runs and not offline_dirs:
            click.echo("There are no Neptune runs in {}".format(base_path))
            sys.exit(1)

        if unsynced_runs:
            click.echo("Unsynchronized runs:")
            for run in unsynced_runs:
                click.echo("- {}".format(get_qualified_name(run)))

        if synced_runs:
            click.echo("Synchronized runs:")
            for run in synced_runs:
                click.echo("- {}".format(get_qualified_name(run)))

        if offline_dirs:
            click.echo("Unsynchronized offline runs:")
            for run_id in offline_dirs:
                click.echo("- {}{}".format(OFFLINE_NAME_PREFIX, run_id))
            click.echo()
            click.echo(textwrap.fill(offline_run_explainer, width=90))

        if not unsynced_runs:
            click.echo()
            click.echo("There are no unsynchronized runs in {}".format(base_path))

        if not synced_runs:
            click.echo()
            click.echo("There are no synchronized runs in {}".format(base_path))

        click.echo()
        click.echo("Please run with the `neptune sync --help` to see example commands.")

    def synchronization_status(self, base_path: Path) -> None:
        synced_runs, unsynced_runs, not_found = self.partition_containers(base_path)
        if not_found > 0:
            click.echo(
                "WARNING: {} runs was skipped because they are in trash or do not exist anymore.".format(
                    not_found
                ),
                sys.stderr,
            )
        offline_dirs = get_offline_dirs(base_path)
        self.list_runs(base_path, synced_runs, unsynced_runs, offline_dirs)
