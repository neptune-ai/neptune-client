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
from collections import defaultdict
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Callable,
    Dict,
    Iterable,
    List,
    NamedTuple,
    Tuple,
)

from neptune.cli.containers import (
    AsyncContainer,
    ExecutionDirectory,
    OfflineContainer,
)
from neptune.cli.utils import (
    detect_async_dir,
    detect_offline_dir,
    get_metadata_container,
    is_single_execution_dir_synced,
)
from neptune.constants import (
    ASYNC_DIRECTORY,
    OFFLINE_DIRECTORY,
)
from neptune.internal.container_type import ContainerType
from neptune.internal.id_formats import UniqueId
from neptune.metadata_containers.structure_version import StructureVersion

if TYPE_CHECKING:
    from neptune.internal.backends.neptune_backend import NeptuneBackend


class CollectedContainers(NamedTuple):
    async_containers: List[AsyncContainer]
    offline_containers: List[OfflineContainer]
    synced_containers: List[AsyncContainer]
    unsynced_containers: List[AsyncContainer]
    not_found_containers: List[AsyncContainer]


def collect_containers(*, path: Path, backend: "NeptuneBackend") -> CollectedContainers:
    if not path.is_dir():
        return CollectedContainers(
            async_containers=[],
            offline_containers=[],
            synced_containers=[],
            unsynced_containers=[],
            not_found_containers=[],
        )

    async_containers: List[AsyncContainer] = []
    if (path / ASYNC_DIRECTORY).exists():
        async_containers = list(collect_async_containers(path=path, backend=backend))

    offline_containers = []
    if (path / OFFLINE_DIRECTORY).exists():
        offline_containers = list(collect_offline_containers(path=path))

    return CollectedContainers(
        async_containers=async_containers,
        offline_containers=offline_containers,
        synced_containers=[x for x in async_containers if x.synced],
        unsynced_containers=[x for x in async_containers if not x.synced and x.found is True],
        not_found_containers=[x for x in async_containers if x.found is False],
    )


def collect_async_containers(*, path: Path, backend: "NeptuneBackend") -> Iterable[AsyncContainer]:
    container_to_execution_dirs = collect_by_container(base_path=path / ASYNC_DIRECTORY, detect_by=detect_async_dir)

    for (container_type, container_id), execution_dirs in container_to_execution_dirs.items():
        experiment = get_metadata_container(backend=backend, container_type=container_type, container_id=container_id)
        found = experiment is not None

        yield AsyncContainer(
            container_id=container_id,
            container_type=container_type,
            found=found,
            experiment=experiment,
            execution_dirs=execution_dirs,
        )


def collect_offline_containers(*, path: Path) -> Iterable[OfflineContainer]:
    container_to_execution_dirs = collect_by_container(base_path=path / OFFLINE_DIRECTORY, detect_by=detect_offline_dir)

    for (container_type, container_id), execution_dirs in container_to_execution_dirs.items():
        yield OfflineContainer(
            container_id=container_id,
            container_type=container_type,
            execution_dirs=execution_dirs,
            found=False,
        )


def collect_child_directories(base_path: Path, structure_version: StructureVersion) -> List[Path]:
    if structure_version in {StructureVersion.CHILD_EXECUTION_DIRECTORIES, StructureVersion.LEGACY}:
        return list(map(lambda r: base_path / r.name, base_path.iterdir()))
    elif structure_version == StructureVersion.DIRECT_DIRECTORY:
        return [base_path]
    else:
        raise ValueError(f"Unknown structure version {structure_version}")


def collect_by_container(
    *, base_path: Path, detect_by: Callable[[str], Tuple[ContainerType, UniqueId, StructureVersion]]
) -> Dict[Tuple[ContainerType, UniqueId], List["ExecutionDirectory"]]:
    container_to_execution_dirs: Dict[Tuple[ContainerType, UniqueId], List["ExecutionDirectory"]] = defaultdict(list)

    for child_path in base_path.iterdir():
        container_type, unique_id, structure_version = detect_by(child_path.name)
        execution_dirs = collect_child_directories(child_path, structure_version)
        for execution_dir in execution_dirs:
            parent = execution_dir.parent if structure_version == StructureVersion.CHILD_EXECUTION_DIRECTORIES else None
            container_to_execution_dirs[(container_type, unique_id)].append(
                ExecutionDirectory(
                    path=execution_dir,
                    synced=is_single_execution_dir_synced(execution_dir),
                    structure_version=structure_version,
                    parent=parent,
                )
            )

    return container_to_execution_dirs
