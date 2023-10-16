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

__all__ = ["ClearRunner"]

import shutil
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    List,
    Sequence,
)

import click

from neptune.cli.abstract_backend_runner import AbstractBackendRunner
from neptune.cli.container_manager import ContainersManager
from neptune.cli.status import StatusRunner
from neptune.cli.utils import get_offline_dirs
from neptune.constants import SYNC_DIRECTORY
from neptune.internal.utils.logger import logger

if TYPE_CHECKING:
    from neptune.internal.backends.api_model import ApiExperiment
    from neptune.internal.id_formats import UniqueId


class ClearRunner(AbstractBackendRunner):
    def clear(self, path: Path, force: bool = False, clear_eventual: bool = True) -> None:
        container_manager = ContainersManager(self._backend, path)
        synced_containers, unsynced_containers, not_found = container_manager.partition_containers_and_clean_junk(path)

        ClearRunner.remove_containers(not_found)

        ClearRunner.remove_sync_containers(path)

        ClearRunner.remove_synced_data(container_manager=container_manager, synced_containers=synced_containers)

        offline_containers = get_offline_dirs(path)
        if clear_eventual and (offline_containers or unsynced_containers):
            self.log_junk_metadata(offline_containers, unsynced_containers)

            if force or click.confirm("\nDo you want to delete the listed metadata?"):
                self.remove_data(container_manager, offline_containers, unsynced_containers)

    @staticmethod
    def remove_sync_containers(path: Path) -> None:
        """
        Function can remove SYNC_DIRECTORY safely, Neptune client only stores files to upload in this location.
        """
        shutil.rmtree(path / SYNC_DIRECTORY, ignore_errors=True)

    @staticmethod
    def log_junk_metadata(
        offline_containers: Sequence["UniqueId"], unsynced_containers: Sequence["ApiExperiment"]
    ) -> None:
        if unsynced_containers:
            logger.info("")
            StatusRunner.log_unsync_objects(unsynced_containers=unsynced_containers)
        if offline_containers:
            logger.info("")
            StatusRunner.log_offline_objects(offline_dirs=offline_containers, info=False)

    @staticmethod
    def remove_data(
        container_manager: ContainersManager,
        offline_containers: Sequence["UniqueId"],
        unsynced_containers: Sequence["ApiExperiment"],
    ) -> None:

        offline_containers_paths = [container_manager.resolve_offline_container_dir(x) for x in offline_containers]
        unsynced_containers_paths = [
            container_manager.resolve_async_path(container) for container in unsynced_containers
        ]

        ClearRunner.remove_containers(offline_containers_paths)
        ClearRunner.remove_containers(unsynced_containers_paths)

    @staticmethod
    def remove_synced_data(container_manager: ContainersManager, synced_containers: Sequence["ApiExperiment"]) -> None:
        synced_containers_paths = [container_manager.resolve_async_path(container) for container in synced_containers]
        ClearRunner.remove_containers(synced_containers_paths)

    @staticmethod
    def remove_containers(paths: List[Path]) -> None:
        for path in paths:
            if path.exists():
                try:
                    shutil.rmtree(path)
                    logger.info(f"Deleted: {path}")
                except OSError:
                    logger.warn(f"Cannot remove directory: {path}")
