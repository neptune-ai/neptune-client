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
    TYPE_CHECKING,
    Sequence,
)

from neptune.cli.collect import collect_containers
from neptune.cli.containers import (
    AsyncContainer,
    OfflineContainer,
)
from neptune.cli.utils import get_qualified_name
from neptune.constants import OFFLINE_NAME_PREFIX
from neptune.envs import PROJECT_ENV_NAME
from neptune.internal.backends.api_model import ApiExperiment
from neptune.internal.utils.logger import get_logger

logger = get_logger(with_prefix=False)

if TYPE_CHECKING:
    from neptune.internal.backends.neptune_backend import NeptuneBackend


offline_run_explainer = """
Runs that execute offline are not created on the server and are not assigned to projects;
instead, they are identified by UUIDs like the ones above.
When synchronizing offline runs, specify the workspace and project using the "--project"
flag. Alternatively, you can set the environment variable
{} to the target workspace/project. See the examples below.
""".format(
    PROJECT_ENV_NAME
)


class StatusRunner:
    @staticmethod
    def status(*, backend: "NeptuneBackend", path: Path) -> None:
        containers = collect_containers(path=path, backend=backend)

        if len(containers.not_found_containers) > 0:
            logger.warning(
                "\nWARNING: %s objects was skipped because they do not exist anymore.",
                len(containers.not_found_containers),
            )
        if not any([containers.synced_containers, containers.unsynced_containers, containers.offline_containers]):
            logger.info("There are no Neptune objects in %s", path)
            sys.exit(1)

        StatusRunner.log_unsync_objects(unsynced_containers=containers.unsynced_containers)
        StatusRunner.log_offline_objects(offline_containers=containers.offline_containers)

        if not containers.unsynced_containers:
            logger.info("\nThere are no unsynchronized objects in %s", path)

        if not containers.synced_containers:
            logger.info("\nThere are no synchronized objects in %s", path)

        logger.info("\nPlease run with the `neptune sync --help` to see example commands.")

    @staticmethod
    def log_offline_objects(*, offline_containers: Sequence["OfflineContainer"], info: bool = True) -> None:
        if offline_containers:
            logger.info("Unsynchronized offline objects:")
            for container in offline_containers:
                logger.info("- %s", f"{OFFLINE_NAME_PREFIX}{container.container_id}")

            if info:
                logger.info("\n%s", textwrap.fill(offline_run_explainer, width=90))

    @staticmethod
    def log_unsync_objects(*, unsynced_containers: Sequence["AsyncContainer"]) -> None:
        if unsynced_containers:
            logger.info("Unsynchronized objects:")
            for container in unsynced_containers:
                experiment = container.experiment

                assert experiment is not None  # mypy fix as experiment is present for async containers

                logger.info("- %s%s", get_qualified_name(experiment), trashed(experiment))


def trashed(cont: ApiExperiment) -> str:
    return " (Trashed)" if cont.trashed else ""
