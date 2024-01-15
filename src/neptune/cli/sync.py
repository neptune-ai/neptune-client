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

from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Optional,
    Sequence,
)

from neptune.cli.collect import collect_containers
from neptune.cli.utils import (
    get_metadata_container,
    get_project,
)
from neptune.constants import OFFLINE_NAME_PREFIX
from neptune.exceptions import CannotSynchronizeOfflineRunsWithoutProject
from neptune.internal.id_formats import (
    QualifiedName,
    UniqueId,
)
from neptune.internal.utils.logger import logger

if TYPE_CHECKING:
    from neptune.internal.backends.neptune_backend import NeptuneBackend


class SyncRunner:
    @staticmethod
    def sync_all_offline(*, backend: "NeptuneBackend", base_path: Path, project_name: Optional["str"]) -> None:
        containers = collect_containers(path=base_path, backend=backend)

        project = get_project(project_name_flag=QualifiedName(project_name) if project_name else None, backend=backend)
        if not project:
            raise CannotSynchronizeOfflineRunsWithoutProject

        for container in containers.offline_containers:
            container.sync(base_path=base_path, backend=backend, project=project)

    @staticmethod
    def sync_all(*, backend: "NeptuneBackend", base_path: Path, project_name: Optional[str]) -> None:
        containers = collect_containers(path=base_path, backend=backend)

        if containers.unsynced_containers:
            for container in containers.unsynced_containers:
                container.sync(base_path=base_path, backend=backend, project=None)

        if containers.offline_containers:
            project = get_project(
                project_name_flag=QualifiedName(project_name) if project_name else None, backend=backend
            )
            if not project:
                raise CannotSynchronizeOfflineRunsWithoutProject

            for container in containers.offline_containers:
                container.sync(base_path=base_path, backend=backend, project=project)

    @staticmethod
    def sync_selected(
        *, backend: "NeptuneBackend", base_path: Path, project_name: Optional[str], object_names: Sequence[str]
    ) -> None:
        containers = collect_containers(path=base_path, backend=backend)

        non_offline_container_names = [
            QualifiedName(name) for name in object_names if not name.startswith(OFFLINE_NAME_PREFIX)
        ]
        non_offline_containers_ids = []
        for container_name in non_offline_container_names:
            experiment = get_metadata_container(
                backend=backend,
                container_id=container_name,
            )
            if experiment:
                non_offline_containers_ids.append(experiment.id)
            else:
                logger.error(f"Container {container_name} not found")

        selected_async_containers = list(
            filter(lambda x: x.container_id in non_offline_containers_ids, containers.async_containers)
        )
        for container in selected_async_containers:
            container.sync(base_path=base_path, backend=backend, project=None)

        selected_offline_container_ids = [
            UniqueId(name[len(OFFLINE_NAME_PREFIX) :]) for name in object_names if name.startswith(OFFLINE_NAME_PREFIX)
        ]
        if selected_offline_container_ids:
            project = get_project(
                project_name_flag=QualifiedName(project_name) if project_name else None, backend=backend
            )
            if not project:
                raise CannotSynchronizeOfflineRunsWithoutProject

            selected_offline_containers = []
            for container_id in selected_offline_container_ids:
                container = list(filter(lambda x: x.container_id == container_id, containers.offline_containers))
                if container:
                    selected_offline_containers.append(container[0])
                else:
                    logger.warning("Offline container %s not found on disk.", container_id)

            for container in selected_offline_containers:
                container.sync(base_path=base_path, backend=backend, project=project)
