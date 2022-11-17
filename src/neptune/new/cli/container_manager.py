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

__all__ = ["ContainersManager"]

import abc
from pathlib import Path
from typing import (
    List,
    Tuple,
)

from neptune.new.cli.utils import (
    get_metadata_container,
    is_container_synced_and_remove_junk,
    iterate_containers,
)
from neptune.new.constants import (
    ASYNC_DIRECTORY,
    OFFLINE_DIRECTORY,
    SYNC_DIRECTORY,
)
from neptune.new.internal.backends.api_model import ApiExperiment
from neptune.new.internal.backends.neptune_backend import NeptuneBackend
from neptune.new.internal.id_formats import UniqueId


class ContainersManager(abc.ABC):
    _backend: NeptuneBackend

    def __init__(self, backend: NeptuneBackend, base_path: Path):
        self._backend = backend
        self._base_path = base_path

    def partition_containers_and_clean_junk(
        self,
        base_path: Path,
    ) -> Tuple[List[ApiExperiment], List[ApiExperiment], List[Path]]:
        synced_containers = []
        unsynced_containers = []
        not_found = []
        async_path = base_path / ASYNC_DIRECTORY
        for container_type, container_id, path in iterate_containers(async_path):
            metadata_container = get_metadata_container(
                backend=self._backend,
                container_id=container_id,
                container_type=container_type,
            )
            if metadata_container:
                if is_container_synced_and_remove_junk(path):
                    synced_containers.append(metadata_container)
                else:

                    unsynced_containers.append(metadata_container)
            else:
                not_found.append(path)

        synced_containers = [obj for obj in synced_containers if obj]
        unsynced_containers = [obj for obj in unsynced_containers if obj]

        return synced_containers, unsynced_containers, not_found

    def resolve_async_path(self, container: ApiExperiment) -> Path:
        return self._base_path / ASYNC_DIRECTORY / container.type.create_dir_name(container.id)

    def resolve_offline_container_dir(self, offline_id: UniqueId):
        return self._base_path / OFFLINE_DIRECTORY / offline_id

    def iterate_sync_containers(self):
        return iterate_containers(self._base_path / SYNC_DIRECTORY)
