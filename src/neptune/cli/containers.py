#
# Copyright (c) 2024, Neptune Labs Sp. z o.o.
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
__all__ = [
    "Container",
    "ExecutionDirectory",
    "OfflineContainer",
    "AsyncContainer",
]

import os
import shutil
import threading
import time
from abc import (
    ABC,
    abstractmethod,
)
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    Optional,
)

from neptune.cli.utils import get_qualified_name
from neptune.common.exceptions import NeptuneConnectionLostException
from neptune.constants import ASYNC_DIRECTORY
from neptune.core.components.operation_storage import OperationStorage
from neptune.core.components.queue.disk_queue import DiskQueue
from neptune.envs import NEPTUNE_SYNC_BATCH_TIMEOUT_ENV
from neptune.internal.container_type import ContainerType
from neptune.internal.id_formats import UniqueId
from neptune.internal.operation import Operation
from neptune.internal.operation_processors.utils import get_container_dir
from neptune.internal.utils.logger import get_logger
from neptune.metadata_containers.structure_version import StructureVersion

if TYPE_CHECKING:
    from neptune.internal.backends.api_model import (
        ApiExperiment,
        Project,
    )
    from neptune.internal.backends.neptune_backend import NeptuneBackend


logger = get_logger(with_prefix=False)
retries_timeout = int(os.getenv(NEPTUNE_SYNC_BATCH_TIMEOUT_ENV, "3600"))


class ExecutionDirectory:
    def __init__(self, path: Path, synced: bool, structure_version: StructureVersion, parent: Optional[Path] = None):
        self._path = path
        self._synced = synced
        self._structure_version = structure_version
        self._parent = parent

    @property
    def path(self) -> Path:
        return self._path

    @property
    def synced(self) -> bool:
        return self._synced

    @property
    def structure_version(self) -> StructureVersion:
        return self._structure_version

    @property
    def parent(self) -> Optional[Path]:
        return self._parent

    def clear(self) -> None:
        if self.path.exists():
            remove_directory_structure(self.path)

    def sync(self, *, backend: "NeptuneBackend", container_id: UniqueId, container_type: ContainerType) -> None:
        operation_storage = OperationStorage(self.path)
        serializer: Callable[[Operation], Dict[str, Any]] = lambda op: op.to_dict()

        with DiskQueue(
            data_path=self.path,
            to_dict=serializer,
            from_dict=Operation.from_dict,
            lock=threading.RLock(),
        ) as disk_queue:
            while True:
                raw_batch = disk_queue.get_batch(1000)
                if not raw_batch:
                    break
                version = raw_batch[-1].ver
                batch = [element.obj for element in raw_batch]

                start_time = time.monotonic()
                expected_count = len(batch)
                version_to_ack = version - expected_count
                while True:
                    try:
                        processed_count, _ = backend.execute_operations(
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

    def move(self, *, base_path: Path, target_container_id: UniqueId, container_type: ContainerType) -> None:
        new_online_dir = get_container_dir(container_id=target_container_id, container_type=container_type)
        try:
            (base_path / ASYNC_DIRECTORY).mkdir(parents=True, exist_ok=True)
        except OSError:
            logger.warning(f"Cannot create directory: {base_path / ASYNC_DIRECTORY}")
            return

        self.path.rename(base_path / ASYNC_DIRECTORY / new_online_dir)
        self._path = base_path / ASYNC_DIRECTORY / new_online_dir
        self._structure_version = StructureVersion.DIRECT_DIRECTORY


class Container(ABC):
    @property
    @abstractmethod
    def container_id(self) -> UniqueId:
        ...

    @property
    @abstractmethod
    def execution_dirs(self) -> List[ExecutionDirectory]:
        ...

    @property
    def synced(self) -> bool:
        return all(map(lambda execution_dir: execution_dir.synced, self.execution_dirs))

    @abstractmethod
    def sync(self, *, base_path: Path, backend: "NeptuneBackend", project: Optional["Project"] = None) -> None:
        ...

    def clear(self) -> None:
        for execution_dir in self.execution_dirs:
            execution_dir.clear()

        for execution_dir in self.execution_dirs:
            if execution_dir.parent is not None:
                remove_directory(execution_dir.parent)
                break


class AsyncContainer(Container):
    def __init__(
        self,
        container_id: UniqueId,
        container_type: ContainerType,
        execution_dirs: List[ExecutionDirectory],
        found: bool,
        experiment: Optional["ApiExperiment"] = None,
    ):
        self._container_id = container_id
        self._container_type = container_type
        self._execution_dirs = execution_dirs
        self._found = found
        self._experiment = experiment

    @property
    def container_id(self) -> UniqueId:
        return self._container_id

    @property
    def container_type(self) -> ContainerType:
        return self._container_type

    @property
    def execution_dirs(self) -> List[ExecutionDirectory]:
        return self._execution_dirs

    @property
    def found(self) -> bool:
        return self._found

    @property
    def experiment(self) -> Optional["ApiExperiment"]:
        return self._experiment

    def sync(self, *, base_path: Path, backend: "NeptuneBackend", project: Optional["Project"] = None) -> None:
        assert self.experiment is not None  # mypy fix

        qualified_container_name = get_qualified_name(self.experiment)
        logger.info("Synchronising %s", qualified_container_name)

        for execution_dir in self.execution_dirs:
            if not execution_dir.synced:
                execution_dir.sync(
                    backend=backend,
                    container_id=self.container_id,
                    container_type=self.container_type,
                )

        self.clear()
        logger.info("Synchronization of %s %s completed.", self.experiment.type.value, qualified_container_name)


class OfflineContainer(Container):
    def __init__(
        self,
        container_id: UniqueId,
        container_type: ContainerType,
        execution_dirs: List[ExecutionDirectory],
        found: bool,
    ):
        self._container_id = container_id
        self._container_type = container_type
        self._execution_dirs = execution_dirs
        self._found = found

    @property
    def container_id(self) -> UniqueId:
        return self._container_id

    @property
    def container_type(self) -> ContainerType:
        return self._container_type

    @property
    def execution_dirs(self) -> List[ExecutionDirectory]:
        return self._execution_dirs

    @property
    def found(self) -> bool:
        return self._found

    def sync(self, *, base_path: Path, backend: "NeptuneBackend", project: Optional["Project"] = None) -> None:
        assert project is not None  # mypy fix

        experiment = register_offline_container(
            backend=backend,
            project=project,
            container_type=self.container_type,
        )

        if experiment:
            self._container_id = experiment.id
            for execution_dir in self.execution_dirs:
                execution_dir.move(
                    base_path=base_path,
                    target_container_id=self.container_id,
                    container_type=self.container_type,
                )
        else:
            logger.warning("Cannot register offline container %s", self.container_id)
            return

        qualified_container_name = get_qualified_name(experiment)
        logger.info("Offline container %s registered as %s", self.container_id, qualified_container_name)
        logger.info("Synchronising %s", qualified_container_name)

        for execution_dir in self.execution_dirs:
            execution_dir.sync(
                backend=backend,
                container_id=self.container_id,
                container_type=self.container_type,
            )

        self.clear()
        logger.info("Synchronization of %s %s completed.", experiment.type.value, qualified_container_name)


def remove_directory(path: Path) -> None:
    try:
        path.rmdir()
        logger.info(f"Deleted: {path}")
    except OSError:
        logger.warning(f"Cannot remove directory: {path}")


def remove_directory_structure(path: Path) -> None:
    try:
        shutil.rmtree(path)
        logger.info(f"Deleted: {path}")
    except OSError:
        logger.warning(f"Cannot remove directory: {path}")


def register_offline_container(
    *, backend: "NeptuneBackend", project: "Project", container_type: ContainerType
) -> Optional["ApiExperiment"]:
    try:
        if container_type == ContainerType.RUN:
            return backend.create_run(project.id)
        else:
            raise ValueError("Only runs are supported in offline mode")
    except Exception as e:
        logger.warning(
            "Exception occurred while trying to create a run" " on the Neptune server. Please try again later",
        )
        logger.exception(e)
        return None
