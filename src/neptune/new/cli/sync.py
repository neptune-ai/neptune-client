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

__all__ = ["SyncRunner"]

import logging
import os
import threading
import time
from pathlib import Path
from typing import (
    Iterable,
    List,
    Optional,
    Sequence,
)

from neptune.common.exceptions import NeptuneConnectionLostException
from neptune.new.cli.abstract_backend_runner import AbstractBackendRunner
from neptune.new.cli.utils import (
    get_metadata_container,
    get_offline_dirs,
    get_project,
    get_qualified_name,
    is_container_synced_and_remove_junk,
    iterate_containers,
    split_dir_name,
)
from neptune.new.constants import (
    ASYNC_DIRECTORY,
    OFFLINE_DIRECTORY,
    OFFLINE_NAME_PREFIX,
)
from neptune.new.envs import NEPTUNE_SYNC_BATCH_TIMEOUT_ENV
from neptune.new.exceptions import CannotSynchronizeOfflineRunsWithoutProject
from neptune.new.internal.backends.api_model import (
    ApiExperiment,
    Project,
)
from neptune.new.internal.container_type import ContainerType
from neptune.new.internal.disk_queue import DiskQueue
from neptune.new.internal.id_formats import (
    QualifiedName,
    UniqueId,
)
from neptune.new.internal.operation import Operation
from neptune.new.internal.operation_processors.operation_storage import OperationStorage
from neptune.new.internal.utils.logger import logger

retries_timeout = int(os.getenv(NEPTUNE_SYNC_BATCH_TIMEOUT_ENV, "3600"))


class SyncRunner(AbstractBackendRunner):
    def sync_container(self, container_path: Path, experiment: ApiExperiment) -> None:
        qualified_container_name = get_qualified_name(experiment)
        logger.info("Synchronising %s", qualified_container_name)
        for execution_path in container_path.iterdir():
            self.sync_execution(
                execution_path=execution_path,
                container_id=experiment.id,
                container_type=experiment.type,
            )
        logger.info("Synchronization of %s %s completed.", experiment.type.value, qualified_container_name)

    def sync_execution(
        self,
        execution_path: Path,
        container_id: UniqueId,
        container_type: ContainerType,
    ) -> None:
        operation_storage = OperationStorage(execution_path)
        with DiskQueue(
            dir_path=operation_storage.data_path,
            to_dict=lambda x: x.to_dict(),
            from_dict=Operation.from_dict,
            lock=threading.RLock(),
        ) as disk_queue:
            while True:
                batch = disk_queue.get_batch(1000)
                if not batch:
                    break
                version = batch[-1].ver
                batch = [element.obj for element in batch]

                start_time = time.monotonic()
                expected_count = len(batch)
                version_to_ack = version - expected_count
                while True:
                    try:
                        processed_count, _ = self._backend.execute_operations(
                            container_id=container_id,
                            container_type=container_type,
                            operations=batch,
                            operation_storage=operation_storage,
                        )
                        version_to_ack += processed_count
                        batch = batch[processed_count:]
                        disk_queue.ack(version)
                        if version_to_ack == version:
                            break
                    except NeptuneConnectionLostException as ex:
                        if time.monotonic() - start_time > retries_timeout:
                            raise ex
                        logger.warning(
                            "Experiencing connection interruptions."
                            " Will try to reestablish communication with Neptune."
                            " Internal exception was: %s",
                            ex.cause.__class__.__name__,
                        )

    def sync_all_registered_containers(self, base_path: Path) -> None:
        async_path = base_path / ASYNC_DIRECTORY
        for container_type, unique_id, path in iterate_containers(async_path):
            if not is_container_synced_and_remove_junk(path):
                container = get_metadata_container(
                    backend=self._backend,
                    container_id=unique_id,
                    container_type=container_type,
                )
                if container:
                    self.sync_container(container_path=path, experiment=container)

    def sync_selected_registered_containers(
        self, base_path: Path, qualified_container_names: Sequence[QualifiedName]
    ) -> None:
        for name in qualified_container_names:
            container = get_metadata_container(
                backend=self._backend,
                container_id=name,
            )
            if container:
                container_path = base_path / ASYNC_DIRECTORY / f"{container.type.create_dir_name(container.id)}"
                container_path_deprecated = base_path / ASYNC_DIRECTORY / f"{container.id}"
                if container_path.exists():
                    self.sync_container(container_path=container_path, experiment=container)
                elif container_path_deprecated.exists():
                    self.sync_container(container_path=container_path_deprecated, experiment=container)
                else:
                    logger.warning("Warning: Run '%s' does not exist in location %s", name, base_path)

    def _register_offline_container(self, project: Project, container_type: ContainerType) -> Optional[ApiExperiment]:
        try:
            if container_type == ContainerType.RUN:
                return self._backend.create_run(project.id)
            else:
                raise ValueError("Only runs are supported in offline mode")
        except Exception as e:
            logger.warning(
                "Exception occurred while trying to create a run" " on the Neptune server. Please try again later",
            )
            logging.exception(e)
            return None

    @staticmethod
    def _move_offline_container(
        base_path: Path,
        offline_dir: str,
        server_id: UniqueId,
        server_type: ContainerType,
    ) -> None:
        online_dir = server_type.create_dir_name(container_id=server_id)
        # create async directory for run
        (base_path / ASYNC_DIRECTORY / online_dir).mkdir(parents=True)
        # mv offline directory inside async one
        (base_path / OFFLINE_DIRECTORY / offline_dir).rename(
            base_path / ASYNC_DIRECTORY / online_dir / "exec-0-offline"
        )

    def register_offline_containers(
        self, base_path: Path, project: Project, offline_dirs: Iterable[str]
    ) -> List[ApiExperiment]:
        result = []
        for offline_dir in offline_dirs:
            offline_path = base_path / OFFLINE_DIRECTORY / offline_dir
            if offline_path.is_dir():
                container_type, _ = split_dir_name(dir_name=offline_dir)
                container = self._register_offline_container(project, container_type=container_type)
                if container:
                    self._move_offline_container(
                        base_path=base_path,
                        offline_dir=offline_dir,
                        server_id=container.id,
                        server_type=container.type,
                    )
                    logger.info("Offline container %s registered as %s", offline_dir, get_qualified_name(container))
                    result.append(container)
            else:
                logger.warning("Offline container %s not found on disk.", offline_dir)
        return result

    def sync_offline_containers(
        self,
        base_path: Path,
        project_name: Optional[QualifiedName],
        offline_dirs: Sequence[UniqueId],
    ):
        if offline_dirs:
            project = get_project(project_name, backend=self._backend)
            if not project:
                raise CannotSynchronizeOfflineRunsWithoutProject
            registered_containers = self.register_offline_containers(base_path, project, offline_dirs)
            offline_containers_names = [get_qualified_name(exp) for exp in registered_containers]
            self.sync_selected_registered_containers(base_path, offline_containers_names)

    def sync_all_offline_containers(self, base_path: Path, project_name: QualifiedName) -> None:

        offline_dirs = get_offline_dirs(base_path)
        self.sync_offline_containers(base_path, project_name, offline_dirs)

    def sync_selected_containers(
        self,
        base_path: Path,
        project_name: Optional[str],
        container_names: Sequence[str],
    ) -> None:
        non_offline_container_names = [
            QualifiedName(name) for name in container_names if not name.startswith(OFFLINE_NAME_PREFIX)
        ]
        self.sync_selected_registered_containers(base_path, non_offline_container_names)

        offline_dirs = [
            UniqueId(name[len(OFFLINE_NAME_PREFIX) :])
            for name in container_names
            if name.startswith(OFFLINE_NAME_PREFIX)
        ]
        self.sync_offline_containers(base_path, project_name, offline_dirs)

    def sync_all_containers(self, base_path: Path, project_name: Optional[str]) -> None:
        self.sync_all_registered_containers(base_path)
        self.sync_all_offline_containers(base_path, project_name)
