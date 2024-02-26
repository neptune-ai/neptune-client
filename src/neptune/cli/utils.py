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

__all__ = [
    "get_metadata_container",
    "get_project",
    "get_qualified_name",
    "is_single_execution_dir_synced",
    "detect_offline_dir",
    "detect_async_dir",
]

import os
import textwrap
import threading
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    Optional,
    Tuple,
    Union,
)

from neptune.common.exceptions import NeptuneException
from neptune.core.components.queue.disk_queue import DiskQueue
from neptune.envs import PROJECT_ENV_NAME
from neptune.exceptions import (
    MetadataContainerNotFound,
    ProjectNotFound,
)
from neptune.internal.backends.api_model import (
    ApiExperiment,
    Project,
)
from neptune.internal.backends.neptune_backend import NeptuneBackend
from neptune.internal.container_type import ContainerType
from neptune.internal.id_formats import (
    QualifiedName,
    UniqueId,
)
from neptune.internal.operation import Operation
from neptune.internal.utils.logger import get_logger
from neptune.metadata_containers.structure_version import StructureVersion

logger = get_logger(with_prefix=False)


def get_metadata_container(
    backend: NeptuneBackend,
    container_id: Union[UniqueId, QualifiedName],
    container_type: Optional[ContainerType] = None,
) -> Optional[ApiExperiment]:
    public_container_type = container_type or "object"
    try:
        return backend.get_metadata_container(container_id=container_id, expected_container_type=container_type)
    except MetadataContainerNotFound:
        logger.warning("Can't fetch %s %s. Skipping.", public_container_type, container_id)
    except NeptuneException as e:
        logger.warning("Exception while fetching %s %s. Skipping.", public_container_type, container_id)
        logger.exception(e)

    return None


_project_name_missing_message = (
    "Project name not provided. Could not synchronize offline runs."
    " To synchronize an offline run, specify the project name with the --project flag"
    f" or by setting the {PROJECT_ENV_NAME} environment variable."
)


def _project_not_found_message(project_name: QualifiedName) -> str:
    return (
        f"Project {project_name} not found. Could not synchronize offline runs."
        " Please ensure you specified the correct project name with the --project flag"
        f" or with the {PROJECT_ENV_NAME} environment variable, or contact Neptune for support."
    )


def get_project(backend: NeptuneBackend, project_name_flag: Optional[QualifiedName] = None) -> Optional[Project]:
    project_name: Optional[QualifiedName] = project_name_flag
    if project_name_flag is None:
        project_name_from_env = os.getenv(PROJECT_ENV_NAME)
        if project_name_from_env is not None:
            project_name = QualifiedName(project_name_from_env)

    if not project_name:
        logger.warning(textwrap.fill(_project_name_missing_message))
        return None
    try:
        return backend.get_project(project_name)
    except ProjectNotFound:
        logger.warning(textwrap.fill(_project_not_found_message(project_name)))
        return None


def get_qualified_name(experiment: ApiExperiment) -> QualifiedName:
    return QualifiedName(f"{experiment.workspace}/{experiment.project_name}/{experiment.sys_id}")


def is_single_execution_dir_synced(execution_path: Path) -> bool:
    serializer: Callable[[Operation], Dict[str, Any]] = lambda op: op.to_dict()

    with DiskQueue(execution_path, serializer, Operation.from_dict, threading.RLock()) as disk_queue:
        is_queue_empty: bool = disk_queue.is_empty()

    return is_queue_empty


def detect_async_dir(dir_name: str) -> Tuple[ContainerType, UniqueId, StructureVersion]:
    parts = dir_name.split("__")
    if len(parts) == 1:
        return ContainerType.RUN, UniqueId(dir_name), StructureVersion.LEGACY
    elif len(parts) == 2:
        return ContainerType(parts[0]), UniqueId(parts[1]), StructureVersion.CHILD_EXECUTION_DIRECTORIES
    elif len(parts) == 4 or len(parts) == 5:
        return ContainerType(parts[0]), UniqueId(parts[1]), StructureVersion.DIRECT_DIRECTORY
    else:
        raise ValueError(f"Wrong dir format: {dir_name}")


def detect_offline_dir(dir_name: str) -> Tuple[ContainerType, UniqueId, StructureVersion]:
    parts = dir_name.split("__")
    if len(parts) == 1:
        return ContainerType.RUN, UniqueId(dir_name), StructureVersion.DIRECT_DIRECTORY
    elif len(parts) == 2 or len(parts) == 4:
        return ContainerType(parts[0]), UniqueId(parts[1]), StructureVersion.DIRECT_DIRECTORY
    else:
        raise ValueError(f"Wrong dir format: {dir_name}")
