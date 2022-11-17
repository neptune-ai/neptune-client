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
    Optional,
    Sequence,
)

from neptune.new.cli.abstract_backend_runner import AbstractBackendRunner
from neptune.new.cli.container_manager import ContainersManager
from neptune.new.cli.utils import (
    get_offline_dirs,
    get_qualified_name,
)
from neptune.new.constants import OFFLINE_NAME_PREFIX
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
    @staticmethod
    def list_containers(
        base_path: Path,
        synced_containers: Optional[Sequence[ApiExperiment]],
        unsynced_containers: Sequence[ApiExperiment],
        offline_dirs: Sequence[str],
    ) -> None:

        if not synced_containers and not unsynced_containers and not offline_dirs:
            logger.info("There are no Neptune objects in %s", base_path)
            sys.exit(1)

        StatusRunner.log_unsync_objects(unsynced_containers)

        StatusRunner.log_offline_objects(offline_dirs)

        if not unsynced_containers:
            logger.info("\nThere are no unsynchronized objects in %s", base_path)

        if not synced_containers:
            logger.info("\nThere are no synchronized objects in %s", base_path)

        logger.info("\nPlease run with the `neptune sync --help` to see example commands.")

    @staticmethod
    def trashed(cont: ApiExperiment):
        return " (Trashed)" if cont.trashed else ""

    @staticmethod
    def log_offline_objects(offline_dirs, info=True):
        if offline_dirs:
            logger.info("Unsynchronized offline objects:")
            for container_id in offline_dirs:
                logger.info("- %s", f"{OFFLINE_NAME_PREFIX}{container_id}")
            if info:
                logger.info("\n%s", textwrap.fill(offline_run_explainer, width=90))

    @staticmethod
    def log_unsync_objects(unsynced_containers):
        if unsynced_containers:
            logger.info("Unsynchronized objects:")
            for container in unsynced_containers:
                logger.info("- %s%s", get_qualified_name(container), StatusRunner.trashed(container))

    def synchronization_status(self, base_path: Path) -> None:
        container_manager = ContainersManager(self._backend, base_path)
        synced_containers, unsynced_containers, not_found = container_manager.partition_containers_and_clean_junk(
            base_path
        )
        if len(not_found) > 0:
            logger.warning(
                "\nWARNING: %s objects was skipped because they do not exist anymore.",
                len(not_found),
            )
        offline_dirs = get_offline_dirs(base_path)
        self.list_containers(base_path, synced_containers, unsynced_containers, offline_dirs)
