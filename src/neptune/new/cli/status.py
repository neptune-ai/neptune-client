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
from typing import (
    List,
    Sequence,
    Tuple,
)

from neptune.new.cli.abstract_backend_runner import AbstractBackendRunner
from neptune.new.cli.utils import (
    get_metadata_container,
    get_offline_dirs,
    get_qualified_name,
    is_container_synced,
    iterate_containers,
)
from neptune.new.constants import (
    ASYNC_DIRECTORY,
    OFFLINE_NAME_PREFIX,
)
from neptune.new.envs import PROJECT_ENV_NAME
from neptune.new.internal.backends.api_model import ApiExperiment
from neptune.new.internal.utils.logger import logger

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

        not_found = len([obj for obj in synced_containers + unsynced_containers if not obj])
        synced_containers = [obj for obj in synced_containers if obj]
        unsynced_containers = [obj for obj in unsynced_containers if obj]

        return synced_containers, unsynced_containers, not_found

    @staticmethod
    def list_containers(
        base_path: Path,
        synced_containers: Sequence[ApiExperiment],
        unsynced_containers: Sequence[ApiExperiment],
        offline_dirs: Sequence[str],
    ) -> None:
        def trashed(cont: ApiExperiment):
            return " (Trashed)" if cont.trashed else ""

        if not synced_containers and not unsynced_containers and not offline_dirs:
            logger.info("There are no Neptune objects in %s", base_path)
            sys.exit(1)

        if unsynced_containers:
            logger.info("Unsynchronized objects:")
            for container in unsynced_containers:
                logger.info("- %s%s", get_qualified_name(container), trashed(container))

        if synced_containers:
            logger.info("Synchronized objects:")
            for container in synced_containers:
                logger.info("- %s%s", get_qualified_name(container), trashed(container))

        if offline_dirs:
            logger.info("Unsynchronized offline objects:")
            for run_id in offline_dirs:
                logger.info("- %s", f"{OFFLINE_NAME_PREFIX}{run_id}")
            logger.info("\n%s", textwrap.fill(offline_run_explainer, width=90))

        if not unsynced_containers:
            logger.info("\nThere are no unsynchronized objects in %s", base_path)

        if not synced_containers:
            logger.info("\nThere are no synchronized objects in %s", base_path)

        logger.info("\nPlease run with the `neptune sync --help` to see example commands.")

    def synchronization_status(self, base_path: Path) -> None:
        synced_containers, unsynced_containers, not_found = self.partition_containers(base_path)
        if not_found > 0:
            logger.warning(
                "WARNING: %s objects was skipped because they do not exist anymore.",
                not_found,
            )
        offline_dirs = get_offline_dirs(base_path)
        self.list_containers(base_path, synced_containers, unsynced_containers, offline_dirs)
