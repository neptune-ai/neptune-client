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
    "is_container_synced_and_remove_junk",
    "get_offline_dirs",
    "iterate_containers",
    "split_dir_name",
]

import logging
import os
import textwrap
import threading
from pathlib import Path
from typing import (
    Iterator,
    List,
    Optional,
    Tuple,
    Union,
)

from neptune.common.exceptions import NeptuneException
from neptune.new.constants import OFFLINE_DIRECTORY
from neptune.new.envs import PROJECT_ENV_NAME
from neptune.new.exceptions import (
    MetadataContainerNotFound,
    ProjectNotFound,
)
from neptune.new.internal.backends.api_model import (
    ApiExperiment,
    Project,
)
from neptune.new.internal.backends.neptune_backend import NeptuneBackend
from neptune.new.internal.container_type import ContainerType
from neptune.new.internal.disk_queue import DiskQueue
from neptune.new.internal.id_formats import (
    QualifiedName,
    UniqueId,
)
from neptune.new.internal.operation import Operation
from neptune.new.internal.utils.logger import logger


def get_metadata_container(
    backend: NeptuneBackend,
    container_id: Union[UniqueId, QualifiedName],
    container_type: Optional[ContainerType] = None,
) -> Optional[ApiExperiment]:
    public_container_type = container_type or "object"
    try:
        return backend.get_metadata_container(container_id, expected_container_type=container_type)
    except MetadataContainerNotFound:
        logger.warning("Can't fetch %s %s. Skipping.", public_container_type, container_id)
    except NeptuneException as e:
        logger.warning("Exception while fetching %s %s. Skipping.", public_container_type, container_id)
        logging.exception(e)

    return None


_project_name_missing_message = (
    "Project name not provided. Could not synchronize offline runs."
    " To synchronize offline run, specify the project name with the --project flag"
    f" or by setting the {PROJECT_ENV_NAME} environment variable."
)


def _project_not_found_message(project_name: QualifiedName) -> str:
    return (
        f"Project {project_name} not found. Could not synchronize offline runs."
        " Please ensure you specified the correct project name with the --project flag"
        f" or with the {PROJECT_ENV_NAME} environment variable, or contact Neptune for support."
    )


def get_project(project_name_flag: Optional[QualifiedName], backend: NeptuneBackend) -> Optional[Project]:
    project_name = project_name_flag or QualifiedName(os.getenv(PROJECT_ENV_NAME))
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


def is_container_synced_and_remove_junk(experiment_path: Path) -> bool:
    return all(_is_execution_synced_and_remove_junk(execution_path) for execution_path in experiment_path.iterdir())


def _is_execution_synced_and_remove_junk(execution_path: Path) -> bool:
    """
    The DiskQueue.close() method remove junk metadata from disk when queue is empty.
    """
    with DiskQueue(execution_path, lambda x: x.to_dict(), Operation.from_dict, threading.RLock()) as disk_queue:
        return disk_queue.is_empty()


def split_dir_name(dir_name: str) -> Tuple[ContainerType, UniqueId]:
    parts = dir_name.split("__")
    if len(parts) == 2:
        return ContainerType(parts[0]), UniqueId(parts[1])
    elif len(parts) == 1:
        return ContainerType.RUN, UniqueId(dir_name)
    else:
        raise ValueError(f"Wrong dir format: {dir_name}")


def iterate_containers(
    base_path: Path,
) -> Iterator[Tuple[ContainerType, UniqueId, Path]]:
    if not base_path.is_dir():
        return

    for path in base_path.iterdir():
        container_type, unique_id = split_dir_name(dir_name=path.name)

        yield container_type, unique_id, path


def get_offline_dirs(base_path: Path) -> List[UniqueId]:
    result = []
    if not (base_path / OFFLINE_DIRECTORY).is_dir():
        return []
    for path_ in (base_path / OFFLINE_DIRECTORY).iterdir():
        dir_name = path_.name
        result.append(UniqueId(dir_name))
    return result
