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
    is_container_synced,
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
        synced_containers = []
        unsynced_containers = []
        async_path = base_path / ASYNC_DIRECTORY
        for container_type, container_id, path in iterate_containers(async_path):
            metadata_container = get_metadata_container(
                backend=self._backend,
                container_id=container_id,
                container_type=container_type,
            )

            if is_container_synced(path):
                synced_containers.append(metadata_container)
            else:
                unsynced_containers.append(metadata_container)

        not_found = len(
            [
                exp
                for exp in synced_containers + unsynced_containers
                if not exp or exp.trashed
            ]
        )
        synced_containers = [
            obj for obj in synced_containers if obj and not obj.trashed
        ]
        unsynced_containers = [
            obj for obj in unsynced_containers if obj and not obj.trashed
        ]

        return synced_containers, unsynced_containers, not_found

    @staticmethod
    def list_containers(
        base_path: Path,
        synced_containers: Sequence[ApiExperiment],
        unsynced_containers: Sequence[ApiExperiment],
        offline_dirs: Sequence[str],
    ) -> None:
        if not synced_containers and not unsynced_containers and not offline_dirs:
            click.echo("There are no Neptune objects in {}".format(base_path))
            sys.exit(1)

        if unsynced_containers:
            click.echo("Unsynchronized objects:")
            for container in unsynced_containers:
                click.echo("- {}".format(get_qualified_name(container)))

        if synced_containers:
            click.echo("Synchronized objects:")
            for container in synced_containers:
                click.echo("- {}".format(get_qualified_name(container)))

        if offline_dirs:
            click.echo("Unsynchronized offline objects:")
            for run_id in offline_dirs:
                click.echo("- {}{}".format(OFFLINE_NAME_PREFIX, run_id))
            click.echo()
            click.echo(textwrap.fill(offline_run_explainer, width=90))

        if not unsynced_containers:
            click.echo()
            click.echo("There are no unsynchronized objects in {}".format(base_path))

        if not synced_containers:
            click.echo()
            click.echo("There are no synchronized objects in {}".format(base_path))

        click.echo()
        click.echo("Please run with the `neptune sync --help` to see example commands.")

    def synchronization_status(self, base_path: Path) -> None:
        synced_containers, unsynced_containers, not_found = self.partition_containers(
            base_path
        )
        if not_found > 0:
            click.echo(
                f"WARNING: {not_found} objects was skipped because they are in trash or do not exist anymore.",
                sys.stderr,
            )
        offline_dirs = get_offline_dirs(base_path)
        self.list_containers(
            base_path, synced_containers, unsynced_containers, offline_dirs
        )
