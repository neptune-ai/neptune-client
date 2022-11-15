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
from typing import Sequence

import click

from neptune.new.cli.abstract_backend_runner import AbstractBackendRunner
from neptune.new.cli.container_manager import ContainersManager
from neptune.new.cli.status import StatusRunner
from neptune.new.cli.utils import get_offline_dirs
from neptune.new.internal.backends.api_model import ApiExperiment
from neptune.new.internal.id_formats import UniqueId
from neptune.new.internal.utils.logger import logger


class ClearRunner(AbstractBackendRunner):
    def clear(self, path: Path, force: bool = False):
        container_manager = ContainersManager(self._backend, path)
        synced_containers, unsynced_containers, not_found = container_manager.partition_containers_and_clean_junk(path)

        offline_containers = get_offline_dirs(path)

        ClearRunner.remove_containers(not_found)

        if offline_containers or unsynced_containers:
            self.log_junk_metadata(offline_containers, unsynced_containers)

            if force or click.confirm("\nDo you want to delete listed metadata?"):  # TODO NPT-12295
                self.remove_data(container_manager, offline_containers, unsynced_containers)

    @staticmethod
    def log_junk_metadata(offline_containers, unsynced_containers):
        if unsynced_containers:
            logger.info("")
            StatusRunner.log_unsync_objects(unsynced_containers=unsynced_containers)
        if offline_containers:
            logger.info("")
            StatusRunner.log_offline_objects(offline_dirs=offline_containers, info=False)

    @staticmethod
    def remove_data(
        container_manager: ContainersManager,
        offline_containers: Sequence[UniqueId],
        unsynced_containers: Sequence[ApiExperiment],
    ):

        offline_containers_paths = [container_manager.resolve_offline_container_dir(x) for x in offline_containers]
        unsynced_containers_paths = [
            container_manager.resolve_async_path(container) for container in unsynced_containers
        ]

        ClearRunner.remove_containers(offline_containers_paths)
        ClearRunner.remove_containers(unsynced_containers_paths)

    @staticmethod
    def remove_containers(paths):
        for path in paths:
            try:
                shutil.rmtree(path)
                logger.info(f"Deleted: {path}")
            except OSError:
                logger.warn(f"Cannot remove directory: {path}")
